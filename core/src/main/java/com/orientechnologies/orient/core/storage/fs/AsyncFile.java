package com.orientechnologies.orient.core.storage.fs;

import com.orientechnologies.common.concur.lock.OInterruptedException;
import com.orientechnologies.common.concur.lock.ScalableRWLock;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.orient.core.exception.OStorageException;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

public final class AsyncFile implements OFile {
  private final ScalableRWLock lock = new ScalableRWLock();
  private volatile Path osFile;

  private final AtomicLong dirtyCounter = new AtomicLong();
  private final Object flushSemaphore = new Object();

  private final AtomicLong size = new AtomicLong(-1);
  private AsynchronousFileChannel fileChannel;

  private final int pageSize;

  public AsyncFile(final Path osFile, final int pageSize) {
    this.osFile = osFile;
    this.pageSize = pageSize;
  }

  @Override
  public void create() throws IOException {
    lock.exclusiveLock();
    try {
      if (fileChannel != null) {
        throw new OStorageException("File " + osFile + " is already opened.");
      }

      Files.createFile(osFile);

      doOpen();
    } finally {
      lock.exclusiveUnlock();
    }
  }

  private void initSize() throws IOException {
    if (fileChannel.size() < HEADER_SIZE) {
      final ByteBuffer buffer = ByteBuffer.allocate(HEADER_SIZE);

      int written = 0;
      do {
        buffer.position(written);
        final Future<Integer> writeFuture = fileChannel.write(buffer, written);
        try {
          written += writeFuture.get();
        } catch (InterruptedException e) {
          throw OException.wrapException(
              new OInterruptedException("File write was interrupted"), e);
        } catch (ExecutionException e) {
          throw OException.wrapException(
              new OStorageException("Error during write operation to the file " + osFile), e);
        }
      } while (written < HEADER_SIZE);

      dirtyCounter.incrementAndGet();
    }

    long currentSize = fileChannel.size() - HEADER_SIZE;

    if (currentSize % pageSize != 0) {
      final long initialSize = currentSize;

      currentSize = (currentSize / pageSize) * pageSize;
      fileChannel.truncate(currentSize + HEADER_SIZE);

      OLogManager.instance()
          .warnNoDb(
              this,
              "Data page in file {} was partially written and will be truncated, "
                  + "initial size {}, truncated size {}",
              osFile,
              initialSize,
              currentSize);
    }

    if (size.get() < 0) {
      size.set(currentSize);
    } else {
      if (fileChannel.size() - HEADER_SIZE > size.get()) {
        throw new IllegalStateException(
            "Physical size of the file "
                + (fileChannel.size() - HEADER_SIZE)
                + " but logical size is "
                + size.get());
      }
    }
  }

  @Override
  public void open() {
    lock.exclusiveLock();
    try {
      doOpen();
    } catch (IOException e) {
      throw OException.wrapException(new OStorageException("Can not open file " + osFile), e);
    } finally {
      lock.exclusiveUnlock();
    }
  }

  private void doOpen() throws IOException {
    if (fileChannel != null) {
      throw new OStorageException("File " + osFile + " is already opened.");
    }

    fileChannel =
        AsynchronousFileChannel.open(osFile, StandardOpenOption.READ, StandardOpenOption.WRITE);

    initSize();
  }

  @Override
  public long getFileSize() {
    return size.get();
  }

  @Override
  public String getName() {
    return osFile.getFileName().toString();
  }

  @Override
  public boolean isOpen() {
    lock.sharedLock();
    try {
      return fileChannel != null;
    } finally {
      lock.sharedUnlock();
    }
  }

  @Override
  public boolean exists() {
    return Files.exists(osFile);
  }

  @Override
  public void write(long offset, ByteBuffer buffer) {
    lock.sharedLock();
    try {
      buffer.rewind();

      checkForClose();
      checkPosition(offset);
      checkPosition(offset + buffer.limit() - 1);

      int written = 0;
      do {
        buffer.position(written);
        final Future<Integer> writeFuture =
            fileChannel.write(buffer, offset + HEADER_SIZE + written);
        try {
          written += writeFuture.get();
        } catch (InterruptedException e) {
          throw OException.wrapException(
              new OInterruptedException("File write was interrupted"), e);
        } catch (ExecutionException e) {
          throw OException.wrapException(
              new OStorageException("Error during write operation to the file " + osFile), e);
        }
      } while (written < buffer.limit());

      dirtyCounter.incrementAndGet();
      assert written == buffer.limit();
    } finally {
      lock.sharedUnlock();
    }
  }

  @Override
  public IOResult write(List<ORawPair<Long, ByteBuffer>> buffers) {
    final CountDownLatch latch = new CountDownLatch(buffers.size());
    final AsyncIOResult asyncIOResult = new AsyncIOResult(latch);

    for (final ORawPair<Long, ByteBuffer> pair : buffers) {
      final ByteBuffer byteBuffer = pair.second;
      byteBuffer.rewind();
      lock.sharedLock();
      try {
        checkForClose();
        checkPosition(pair.first);
        checkPosition(pair.first + pair.second.limit() - 1);

        final long position = pair.first + HEADER_SIZE;
        fileChannel.write(
            byteBuffer, position, latch, new WriteHandler(byteBuffer, asyncIOResult, position));
      } finally {
        lock.sharedUnlock();
      }
    }

    return asyncIOResult;
  }

  @Override
  public void read(long offset, ByteBuffer buffer, boolean throwOnEof) throws IOException {
    lock.sharedLock();
    try {
      checkForClose();
      checkPosition(offset);

      int read = 0;
      do {
        buffer.position(read);
        final Future<Integer> readFuture = fileChannel.read(buffer, offset + HEADER_SIZE + read);
        final int bytesRead;
        try {
          bytesRead = readFuture.get();
        } catch (InterruptedException e) {
          throw OException.wrapException(
              new OInterruptedException("File write was interrupted"), e);
        } catch (ExecutionException e) {
          throw OException.wrapException(
              new OStorageException("Error during read operation from the file " + osFile), e);
        }

        if (bytesRead == -1) {
          if (throwOnEof) {
            throw new EOFException("End of file " + osFile + " is reached.");
          }

          break;
        }

        read += bytesRead;
      } while (read < buffer.limit());
    } finally {
      lock.sharedUnlock();
    }
  }

  @Override
  public long allocateSpace(int size) {
    return this.size.getAndAdd(size);
  }

  @Override
  public void shrink(long size) throws IOException {
    lock.exclusiveLock();
    try {
      checkForClose();

      this.size.set(0);
      fileChannel.truncate(size + HEADER_SIZE);
    } finally {
      lock.exclusiveUnlock();
    }
  }

  @Override
  public void synch() {
    lock.sharedLock();
    try {
      doSynch();
    } finally {
      lock.sharedUnlock();
    }
  }

  private void doSynch() {
    synchronized (flushSemaphore) {
      long dirtyCounterValue = dirtyCounter.get();
      if (dirtyCounterValue > 0) {
        try {
          fileChannel.force(false);
        } catch (final IOException e) {
          OLogManager.instance()
              .warn(
                  this,
                  "Error during flush of file %s. Data may be lost in case of power failure",
                  e,
                  getName());
        }

        dirtyCounter.addAndGet(-dirtyCounterValue);
      }
    }
  }

  @Override
  public void close() {
    lock.exclusiveLock();
    try {
      doSynch();
      doClose();
    } catch (IOException e) {
      throw OException.wrapException(
          new OStorageException("Error during closing the file " + osFile), e);
    } finally {
      lock.exclusiveUnlock();
    }
  }

  private void doClose() throws IOException {
    // ignore if closed
    if (fileChannel != null) {
      fileChannel.close();
      fileChannel = null;
    }
  }

  @Override
  public void delete() throws IOException {
    lock.exclusiveLock();
    try {
      doClose();

      Files.delete(osFile);
    } finally {
      lock.exclusiveUnlock();
    }
  }

  @Override
  public void renameTo(Path newFile) throws IOException {
    lock.exclusiveLock();
    try {
      doClose();

      //noinspection NonAtomicOperationOnVolatileField
      osFile = Files.move(osFile, newFile);

      doOpen();
    } finally {
      lock.exclusiveUnlock();
    }
  }

  @Override
  public void replaceContentWith(final Path newContentFile) throws IOException {
    lock.exclusiveLock();
    try {
      doClose();

      Files.copy(newContentFile, osFile, StandardCopyOption.REPLACE_EXISTING);

      doOpen();
    } finally {
      lock.exclusiveUnlock();
    }
  }

  private void checkPosition(long offset) {
    final long fileSize = size.get();
    if (offset < 0 || offset >= fileSize) {
      throw new OStorageException(
          "You are going to access region outside of allocated file position. File size = "
              + fileSize
              + ", requested position "
              + offset);
    }
  }

  private void checkForClose() {
    if (fileChannel == null) {
      throw new OStorageException("File " + osFile + " is closed");
    }
  }

  private final class WriteHandler implements CompletionHandler<Integer, CountDownLatch> {
    private final ByteBuffer byteBuffer;
    private final AsyncIOResult ioResult;
    private final long position;

    private WriteHandler(ByteBuffer byteBuffer, AsyncIOResult ioResult, long position) {
      this.byteBuffer = byteBuffer;
      this.ioResult = ioResult;
      this.position = position;
    }

    @Override
    public void completed(Integer result, CountDownLatch attachment) {
      if (byteBuffer.remaining() > 0) {
        lock.sharedLock();
        try {
          checkForClose();

          fileChannel.write(byteBuffer, position + byteBuffer.position(), attachment, this);
        } finally {
          lock.sharedUnlock();
        }
      } else {
        dirtyCounter.incrementAndGet();
        attachment.countDown();
      }
    }

    @Override
    public void failed(Throwable exc, CountDownLatch attachment) {
      ioResult.exc = exc;
      OLogManager.instance().error(this, "Error during write operation to the file " + osFile, exc);

      dirtyCounter.incrementAndGet();
      attachment.countDown();
    }
  }

  private static final class AsyncIOResult implements IOResult {
    private final CountDownLatch latch;
    private Throwable exc;

    private AsyncIOResult(CountDownLatch latch) {
      this.latch = latch;
    }

    @Override
    public void await() {
      try {
        latch.await();
      } catch (InterruptedException e) {
        throw OException.wrapException(new OInterruptedException("File write was interrupted"), e);
      }
      if (exc != null) {
        throw OException.wrapException(new OStorageException("Error during IO operation"), exc);
      }
    }
  }
}
