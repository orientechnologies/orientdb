package com.orientechnologies.orient.core.storage.fs;

import com.kenai.jffi.MemoryIO;
import com.kenai.jffi.Platform;
import com.orientechnologies.common.concur.lock.ScalableRWLock;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.jnr.LastErrorException;
import com.orientechnologies.common.jnr.ONative;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.orient.core.exception.OStorageException;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
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
  private static final int ALLOCATION_THRESHOLD = 1024 * 1024;

  private final    ScalableRWLock lock = new ScalableRWLock();
  private volatile Path           osFile;

  private final AtomicLong dirtyCounter   = new AtomicLong();
  private final Object     flushSemaphore = new Object();

  private final AtomicLong size          = new AtomicLong();
  private final AtomicLong committedSize = new AtomicLong();

  private AsynchronousFileChannel fileChannel;
  private int                     fd = -1;

  public AsyncFile(Path osFile) {
    this.osFile = osFile;
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
        } catch (InterruptedException | ExecutionException e) {
          throw OException.wrapException(new OStorageException("Error during write operation to the file " + osFile), e);
        }
      } while (written < HEADER_SIZE);

      dirtyCounter.incrementAndGet();
    }

    final long currentSize = fileChannel.size() - HEADER_SIZE;

    size.set(currentSize);
    this.committedSize.set(currentSize);
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

    fileChannel = AsynchronousFileChannel.open(osFile, StandardOpenOption.READ, StandardOpenOption.WRITE);
    if (Platform.getPlatform().getOS() == Platform.OS.LINUX) {
      try {
        fd = ONative.instance().open(osFile.toAbsolutePath().toString(), ONative.O_CREAT | ONative.O_RDONLY | ONative.O_WRONLY);
      } catch (LastErrorException e) {
        fd = -1;
      }
    }

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
        final Future<Integer> writeFuture = fileChannel.write(buffer, offset + HEADER_SIZE + written);
        try {
          written += writeFuture.get();
        } catch (InterruptedException | ExecutionException e) {
          throw OException.wrapException(new OStorageException("Error during write operation to the file " + osFile), e);
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
        fileChannel.write(byteBuffer, position, latch, new WriteHandler(byteBuffer, asyncIOResult, position));
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
        } catch (InterruptedException | ExecutionException e) {
          throw OException.wrapException(new OStorageException("Error during read operation from the file " + osFile), e);
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
  public long allocateSpace(int size) throws IOException {
    lock.sharedLock();
    final long allocatedPosition;
    try {
      final long currentSize = this.size.addAndGet(size);
      allocatedPosition = currentSize - size;

      long currentCommittedSize = this.committedSize.get();

      final long sizeDifference = currentSize - currentCommittedSize;
      if (fd >= 0 && sizeDifference <= ALLOCATION_THRESHOLD) {
        return allocatedPosition;
      }

      while (currentCommittedSize < currentSize) {
        if (this.committedSize.compareAndSet(currentCommittedSize, currentSize)) {
          break;
        }

        currentCommittedSize = committedSize.get();
      }

      if (fd < 0) {
        final MemoryIO memoryIO = MemoryIO.getInstance();
        final long ptr = memoryIO.allocateMemory(size, true);
        try {
          final ByteBuffer buffer = memoryIO.newDirectByteBuffer(ptr, size).order(ByteOrder.nativeOrder());
          int written = 0;
          do {
            buffer.position(written);
            final Future<Integer> writeFuture = fileChannel.write(buffer, allocatedPosition + written + HEADER_SIZE);
            try {
              written += writeFuture.get();
            } catch (InterruptedException | ExecutionException e) {
              throw OException.wrapException(new OStorageException("Error during write operation to the file " + osFile), e);
            }
          } while (written < size);

          assert written == size;
        } finally {
          memoryIO.freeMemory(ptr);
        }
      } else {
        final long sizeDiff = currentSize - currentCommittedSize;
        if (sizeDiff > 0) {
          ONative.instance().fallocate(fd, currentCommittedSize + HEADER_SIZE, sizeDiff);
        }
      }

      assert fileChannel.size() >= currentSize + HEADER_SIZE;
    } finally {
      lock.sharedUnlock();
    }

    return allocatedPosition;
  }

  @Override
  public void shrink(long size) throws IOException {
    lock.exclusiveLock();
    try {
      checkForClose();

      this.size.set(0);
      this.committedSize.set(size);

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
              .warn(this, "Error during flush of file %s. Data may be lost in case of power failure", e, getName());
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
      throw OException.wrapException(new OStorageException("Error during closing the file " + osFile), e);
    } finally {
      lock.exclusiveUnlock();
    }
  }

  private void doClose() throws IOException {
    //ignore if closed
    if (fileChannel != null) {
      fileChannel.close();
      fileChannel = null;

      if (fd >= 0) {
        final long sizeDiff = this.size.get() - this.committedSize.get();
        if (sizeDiff > 0) {
          ONative.instance().fallocate(fd, this.committedSize.get() + HEADER_SIZE, sizeDiff);
        }

        ONative.instance().close(fd);
        fd = -1;
      }
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
          "You are going to access region outside of allocated file position. File size = " + fileSize + ", requested position "
              + offset);
    }
  }

  private void checkForClose() {
    if (fileChannel == null) {
      throw new OStorageException("File " + osFile + " is closed");
    }
  }

  private final class WriteHandler implements CompletionHandler<Integer, CountDownLatch> {
    private final ByteBuffer    byteBuffer;
    private final AsyncIOResult ioResult;
    private final long          position;

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
    private       Throwable      exc;

    private AsyncIOResult(CountDownLatch latch) {
      this.latch = latch;
    }

    @Override
    public void await() {
      try {
        latch.await();
      } catch (InterruptedException e) {
        throw OException.wrapException(new OStorageException("IO operation was interrupted"), e);
      }

      if (exc != null) {
        throw OException.wrapException(new OStorageException("Error during IO operation"), exc);
      }
    }
  }
}
