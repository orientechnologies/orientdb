/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://orientdb.com
 *
 */
package com.orientechnologies.orient.core.storage.fs;

import com.kenai.jffi.MemoryIO;
import com.kenai.jffi.Platform;
import com.orientechnologies.common.concur.lock.ScalableRWLock;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.io.OIOException;
import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.common.jnr.LastErrorException;
import com.orientechnologies.common.jnr.ONative;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import static com.orientechnologies.common.io.OIOUtils.readByteBuffer;
import static com.orientechnologies.common.io.OIOUtils.writeByteBuffer;

public final class OFileClassic implements OFile {
  private static final int ALLOCATION_THRESHOLD = 1024 * 1024;

  private final    ScalableRWLock lock = new ScalableRWLock();
  private volatile Path           osFile;

  private FileChannel channel;

  private final AtomicLong dirtyCounter   = new AtomicLong();
  private final Object     flushSemaphore = new Object();

  private final AtomicLong size          = new AtomicLong();
  private final AtomicLong committedSize = new AtomicLong();

  private AllocationMode allocationMode;
  private int            fd;

  /**
   * Map which calculates which files are opened and how many users they have
   */
  private static final ConcurrentHashMap<Path, FileUser> openedFilesMap = new ConcurrentHashMap<>();

  /**
   * Whether only single file user is allowed.
   */
  private final boolean exclusiveFileAccess = OGlobalConfiguration.STORAGE_EXCLUSIVE_FILE_ACCESS.getValueAsBoolean();

  /**
   * Whether it should be tracked which thread opened file in exclusive mode.
   */
  private final boolean trackFileOpen = OGlobalConfiguration.STORAGE_TRACK_FILE_ACCESS.getValueAsBoolean();

  public OFileClassic(final Path osFile) {
    this.osFile = osFile;
  }

  @Override
  public long allocateSpace(final int size) throws IOException {
    acquireReadLock();
    final long allocatedPosition;
    try {
      final long currentSize = this.size.addAndGet(size);
      allocatedPosition = currentSize - size;

      long currentCommittedSize = this.committedSize.get();

      final long sizeDifference = currentSize - currentCommittedSize;
      if (sizeDifference <= ALLOCATION_THRESHOLD) {
        return allocatedPosition;
      }

      while (currentCommittedSize < currentSize) {
        if (this.committedSize.compareAndSet(currentCommittedSize, currentSize)) {
          break;
        }

        currentCommittedSize = committedSize.get();
      }

      final long sizeDiff = currentSize - currentCommittedSize;
      if (sizeDiff > 0) {
        final MemoryIO memoryIO = MemoryIO.getInstance();
        assert allocationMode != null;
        if (allocationMode == AllocationMode.WRITE) {
          final long ptr = memoryIO.allocateMemory(sizeDiff, true);
          try {
            final ByteBuffer buffer = memoryIO.newDirectByteBuffer(ptr, (int) sizeDiff).order(ByteOrder.nativeOrder());
            buffer.position(0);
            OIOUtils.writeByteBuffer(buffer, channel, currentCommittedSize + HEADER_SIZE);
          } finally {
            memoryIO.freeMemory(ptr);
          }
        } else if (allocationMode == AllocationMode.DESCRIPTOR) {
          assert fd > 0;

          try {
            ONative.instance().fallocate(fd, currentCommittedSize + HEADER_SIZE, sizeDiff);
          } catch (final LastErrorException e) {
            OLogManager.instance().debug(this,
                "Can not allocate space (error %d) for file %s using native Linux API, more slower methods will be used",
                e.getErrorCode(), osFile.toAbsolutePath().toString());

            allocationMode = AllocationMode.WRITE;

            try {
              ONative.instance().close(fd);
            } catch (final LastErrorException lee) {
              OLogManager.instance()
                  .warnNoDb(this, "Can not close Linux descriptor of file %s, error %d", osFile.toAbsolutePath().toString(),
                      lee.getErrorCode());
            }

            final long ptr = memoryIO.allocateMemory(sizeDiff, true);
            try {
              final ByteBuffer buffer = memoryIO.newDirectByteBuffer(ptr, (int) sizeDiff).order(ByteOrder.nativeOrder());
              buffer.position(0);
              OIOUtils.writeByteBuffer(buffer, channel, currentCommittedSize + HEADER_SIZE);
            } finally {
              memoryIO.freeMemory(ptr);
            }
          }

        } else {
          throw new IllegalStateException("Unknown allocation mode");

        }

        assert channel.size() >= currentSize + HEADER_SIZE;
      }
    } finally {
      releaseReadLock();
    }

    return allocatedPosition;
  }

  /**
   * Shrink the file content (filledUpTo attribute only)
   */
  @Override
  public void shrink(final long size) throws IOException {
    acquireWriteLock();
    try {
      //noinspection resource
      channel.truncate(HEADER_SIZE + size);
      this.size.set(size);
      this.committedSize.set(size);

      assert this.committedSize.get() >= 0;
      assert this.size.get() >= 0;

    } finally {
      releaseWriteLock();
    }
  }

  @Override
  public long getFileSize() {
    return size.get();
  }

  @Override
  public void read(long offset, final ByteBuffer buffer, final boolean throwOnEof) throws IOException {
    acquireReadLock();
    try {
      buffer.rewind();

      offset = checkRegions(offset, buffer.limit());
      readByteBuffer(buffer, channel, offset, throwOnEof);
    } finally {
      releaseReadLock();
    }
  }

  @Override
  public void write(long offset, final ByteBuffer buffer) throws IOException {
    acquireReadLock();
    try {
      buffer.rewind();

      offset = checkRegions(offset, buffer.limit());
      writeByteBuffer(buffer, channel, offset);
      dirtyCounter.incrementAndGet();
    } finally {
      releaseReadLock();
    }
  }

  @Override
  public IOResult write(List<ORawPair<Long, ByteBuffer>> buffers) throws IOException {
    for (final ORawPair<Long, ByteBuffer> pair : buffers) {
      final long position = pair.first;

      final ByteBuffer buffer = pair.second;
      write(position, buffer);
    }

    return SyncIOResult.INSTANCE;
  }

  /**
   * Synchronizes the buffered changes to disk.
   */
  @Override
  public void synch() {
    acquireReadLock();
    try {
      synchronized (flushSemaphore) {
        long dirtyCounterValue = dirtyCounter.get();
        if (dirtyCounterValue > 0) {
          try {
            channel.force(false);
          } catch (final IOException e) {
            OLogManager.instance()
                .warn(this, "Error during flush of file %s. Data may be lost in case of power failure", e, getName());
          }

          dirtyCounter.addAndGet(-dirtyCounterValue);
        }
      }
    } finally {
      releaseReadLock();
    }
  }

  /**
   * Creates the file.
   */
  @Override
  public void create() throws IOException {
    acquireWriteLock();
    try {
      acquireExclusiveAccess();

      openChannel();
      init();

      initAllocationMode();
    } finally {
      releaseWriteLock();
    }
  }

  private void initAllocationMode() {
    if (allocationMode != null) {
      return;
    }

    if (Platform.getPlatform().getOS() == Platform.OS.LINUX) {
      allocationMode = AllocationMode.DESCRIPTOR;
      int fd = 0;
      try {
        fd = ONative.instance().open(osFile.toAbsolutePath().toString(), ONative.O_CREAT | ONative.O_RDONLY | ONative.O_WRONLY);
      } catch (final LastErrorException e) {
        OLogManager.instance().warnNoDb(this, "File %s can not be opened using Linux native API,"
                + " more slower methods of allocation will be used. Error code : %d.", osFile.toAbsolutePath().toString(),
            e.getErrorCode());
        allocationMode = AllocationMode.WRITE;
      }
      this.fd = fd;
    } else {
      allocationMode = AllocationMode.WRITE;
    }
  }

  /**
   * ALWAYS ADD THE HEADER SIZE BECAUSE ON THIS TYPE IS ALWAYS NEEDED
   */
  private long checkRegions(final long offset, final long iLength) {
    acquireReadLock();
    try {
      if (offset < 0 || offset + iLength > size.get()) {
        throw new OIOException(
            "You cannot access outside the file size (" + size.get() + " bytes). You have requested portion from " + offset + "-"
                + (offset + iLength) + " bytes. File: " + this);
      }

      return offset + HEADER_SIZE;
    } finally {
      releaseReadLock();
    }

  }

  /**
   * Opens the file.
   */
  /*
   * (non-Javadoc)
   *
   * @see com.orientechnologies.orient.core.storage.fs.OFileAAA#open()
   */
  @Override
  public void open() {
    acquireWriteLock();
    try {
      doOpen();
    } catch (final IOException e) {
      throw OException.wrapException(new OIOException("Error during file open"), e);
    } finally {
      releaseWriteLock();
    }
  }

  private void doOpen() throws IOException {
    if (!Files.exists(osFile)) {
      throw new FileNotFoundException("File: " + osFile);
    }

    acquireExclusiveAccess();

    openChannel();
    init();

    OLogManager.instance().debug(this, "Checking file integrity of " + osFile.getFileName() + "...");

    initAllocationMode();
  }

  private void acquireExclusiveAccess() {
    if (exclusiveFileAccess) {
      while (true) {
        final FileUser fileUser = openedFilesMap.computeIfAbsent(osFile.toAbsolutePath(), p -> {
          if (trackFileOpen) {
            return new FileUser(0, Thread.currentThread().getStackTrace());
          }

          return new FileUser(0, null);
        });

        final int usersCount = fileUser.users;

        if (usersCount > 0) {
          if (!trackFileOpen) {
            throw new IllegalStateException(
                "File is allowed to be opened only once, to get more information start JVM with system property "
                    + OGlobalConfiguration.STORAGE_TRACK_FILE_ACCESS.getKey() + " set to true.");
          } else {
            final StringWriter sw = new StringWriter();
            try (final PrintWriter pw = new PrintWriter(sw)) {
              pw.append("File is allowed to be opened only once.\n");
              if (fileUser.openStackTrace != null) {
                pw.append("File is already opened under: \n");
                pw.append("----------------------------------------------------------------------------------------------------\n");
                for (final StackTraceElement se : fileUser.openStackTrace) {
                  pw.append("\t").append(se.toString()).append("\n");
                }
                pw.append("----------------------------------------------------------------------------------------------------\n");
              }

              pw.flush();
              throw new IllegalStateException(sw.toString());
            }
          }
        } else {
          final FileUser newFileUser = new FileUser(1, Thread.currentThread().getStackTrace());
          if (openedFilesMap.replace(osFile.toAbsolutePath(), fileUser, newFileUser)) {
            break;
          }
        }
      }
    }
  }

  private void releaseExclusiveAccess() {
    if (exclusiveFileAccess) {
      while (true) {
        final FileUser fileUser = openedFilesMap.get(osFile.toAbsolutePath());
        final FileUser newFileUser;
        if (trackFileOpen) {
          newFileUser = new FileUser(fileUser.users - 1, Thread.currentThread().getStackTrace());
        } else {
          newFileUser = new FileUser(fileUser.users - 1, null);
        }

        if (openedFilesMap.replace(osFile.toAbsolutePath(), fileUser, newFileUser)) {
          break;
        }
      }
    }
  }

  /**
   * Closes the file.
   */
  /*
   * (non-Javadoc)
   *
   * @see com.orientechnologies.orient.core.storage.fs.OFileAAA#close()
   */
  @Override
  public void close() {
    try {
      acquireWriteLock();
      try {
        doClose();
      } finally {
        releaseWriteLock();
      }
    } catch (final IOException ioe) {
      throw OException.wrapException(new OIOException("Error during file close"), ioe);
    }
  }

  private void doClose() throws IOException {
    if (channel != null && channel.isOpen()) {
      channel.close();
      channel = null;
    }

    closeFD();

    dirtyCounter.set(0);
    releaseExclusiveAccess();
  }

  private void closeFD() {
    if (allocationMode == AllocationMode.DESCRIPTOR && fd > 0) {
      try {
        ONative.instance().close(fd);
      } catch (final LastErrorException e) {
        OLogManager.instance()
            .warnNoDb(this, "Can not close Linux descriptor of file %s, error %d", osFile.toAbsolutePath().toString(),
                e.getErrorCode());
      }

      allocationMode = null;
      fd = 0;
    }
  }

  /**
   * Deletes the file.
   */
  /*
   * (non-Javadoc)
   *
   * @see com.orientechnologies.orient.core.storage.fs.OFileAAA#delete()
   */
  @Override
  public void delete() throws IOException {
    acquireWriteLock();
    try {
      doClose();

      if (osFile != null) {
        Files.deleteIfExists(osFile);
      }
    } finally {
      releaseWriteLock();
    }
  }

  private void openChannel() throws IOException {
    channel = FileChannel.open(osFile, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE);

    if (channel == null) {
      throw new FileNotFoundException(osFile.toString());
    }

    if (channel.size() == 0) {
      final ByteBuffer buffer = ByteBuffer.allocate(HEADER_SIZE);
      OIOUtils.writeByteBuffer(buffer, channel, 0);
    }
  }

  private void init() throws IOException {
    size.set(channel.size() - HEADER_SIZE);
    assert size.get() >= 0;

    this.committedSize.set(size.get());
  }

  /*
   * (non-Javadoc)
   *
   * @see com.orientechnologies.orient.core.storage.fs.OFileAAA#isOpen()
   */
  @Override
  public boolean isOpen() {
    acquireReadLock();
    try {
      return channel != null;
    } finally {
      releaseReadLock();
    }

  }

  /*
   * (non-Javadoc)
   *
   * @see com.orientechnologies.orient.core.storage.fs.OFileAAA#exists()
   */
  @Override
  public boolean exists() {
    acquireReadLock();
    try {
      return osFile != null && Files.exists(osFile);
    } finally {
      releaseReadLock();
    }
  }

  @Override
  public String getName() {
    acquireReadLock();
    try {
      return osFile.getFileName().toString();
    } finally {
      releaseReadLock();
    }
  }

  @Override
  public void renameTo(final Path newFile) throws IOException {
    acquireWriteLock();
    try {
      doClose();

      //noinspection NonAtomicOperationOnVolatileField
      osFile = Files.move(osFile, newFile);

      doOpen();
    } finally {
      releaseWriteLock();
    }
  }

  /**
   * Replaces the file content with the content of the provided file.
   *
   * @param newContentFile the new content file to replace the content with.
   */
  @Override
  public void replaceContentWith(final Path newContentFile) throws IOException {
    acquireWriteLock();
    try {
      doClose();

      Files.copy(newContentFile, osFile, StandardCopyOption.REPLACE_EXISTING);

      doOpen();
    } finally {
      releaseWriteLock();
    }
  }

  private void acquireWriteLock() {
    lock.writeLock().lock();
  }

  private void releaseWriteLock() {
    lock.writeLock().unlock();
  }

  private void acquireReadLock() {
    lock.readLock().lock();
  }

  private void releaseReadLock() {
    lock.readLock().unlock();
  }

  /*
   * (non-Javadoc)
   *
   * @see com.orientechnologies.orient.core.storage.fs.OFileAAA#toString()
   */
  @Override
  public String toString() {
    final StringBuilder builder = new StringBuilder();
    builder.append("File: ");
    builder.append(osFile.getFileName());
    if (channel != null) {
      builder.append(" os-size=");
      try {
        builder.append(channel.size());
      } catch (final IOException ignore) {
        builder.append("?");
      }
    }
    builder.append(", stored=");
    builder.append(getFileSize());
    return builder.toString();
  }

  /**
   * Container of information about files which are opened inside of storage in exclusive mode
   *
   * @see OGlobalConfiguration#STORAGE_EXCLUSIVE_FILE_ACCESS
   * @see OGlobalConfiguration#STORAGE_TRACK_FILE_ACCESS
   */
  private static final class FileUser {
    private final int                 users;
    private final StackTraceElement[] openStackTrace;

    FileUser(final int users, final StackTraceElement[] openStackTrace) {
      this.users = users;
      this.openStackTrace = openStackTrace;
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      final FileUser fileUser = (FileUser) o;
      return users == fileUser.users && Arrays.equals(openStackTrace, fileUser.openStackTrace);
    }

    @Override
    public int hashCode() {

      int result = Objects.hash(users);
      result = 31 * result + Arrays.hashCode(openStackTrace);
      return result;
    }

  }

  private enum AllocationMode {
    DESCRIPTOR, WRITE
  }

  private static final class SyncIOResult implements IOResult {
    private static SyncIOResult INSTANCE = new SyncIOResult();

    @Override
    public void await() {
    }
  }

}

