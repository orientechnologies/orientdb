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

import com.orientechnologies.common.collection.closabledictionary.OClosableItem;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.io.OIOException;
import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.common.jna.ONative;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.serialization.OBinaryProtocol;
import com.sun.jna.LastErrorException;
import com.sun.jna.Native;
import com.sun.jna.Platform;
import com.sun.jna.Pointer;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static com.orientechnologies.common.io.OIOUtils.readByteBuffer;
import static com.orientechnologies.common.io.OIOUtils.writeByteBuffer;

public final class OFileClassic implements OClosableItem {
  public static final  String NAME            = "classic";
  private static final int    CURRENT_VERSION = 2;

  public static final int HEADER_SIZE    = 1024;
  public static final int VERSION_OFFSET = 48;

  private static final int OPEN_RETRY_MAX = 10;

  private final    ReadWriteLock lock = new ReentrantReadWriteLock();
  private volatile Path          osFile;

  private FileChannel      channel;
  private RandomAccessFile frnd;

  private final AtomicLong dirtyCounter   = new AtomicLong();
  private final Object     flushSemaphore = new Object();

  private int version;

  private volatile long size;

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

  public long allocateSpace(final int size) throws IOException {
    acquireWriteLock();
    try {
      final long currentSize = this.size;
      assert channel.size() == currentSize + HEADER_SIZE;

      //noinspection NonAtomicOperationOnVolatileField
      this.size += size;

      assert this.size >= size;

      assert allocationMode != null;
      if (allocationMode == AllocationMode.WRITE) {
        final long ptr = Native.malloc(size);
        try {
          final ByteBuffer buffer = new Pointer(ptr).getByteBuffer(0, size);
          buffer.position(0);
          OIOUtils.writeByteBuffer(buffer, channel, currentSize + HEADER_SIZE);
        } finally {
          Native.free(ptr);
        }
      } else if (allocationMode == AllocationMode.DESCRIPTOR) {
        assert fd > 0;

        try {
          ONative.instance().fallocate(fd, currentSize + HEADER_SIZE, size);
        } catch (final LastErrorException e) {
          OLogManager.instance()
              .debug(this, "Can not allocate space (error %d) for file %s using native Linux API, more slower methods will be used",
                  e.getErrorCode(), osFile.toAbsolutePath().toString());

          allocationMode = AllocationMode.WRITE;

          try {
            ONative.instance().close(fd);
          } catch (final LastErrorException lee) {
            OLogManager.instance()
                .warnNoDb(this, "Can not close Linux descriptor of file %s, error %d", osFile.toAbsolutePath().toString(),
                    lee.getErrorCode());
          }

          final long ptr = Native.malloc(size);
          try {
            final ByteBuffer buffer = new Pointer(ptr).getByteBuffer(0, size);
            buffer.position(0);
            OIOUtils.writeByteBuffer(buffer, channel, currentSize + HEADER_SIZE);
          } finally {
            Native.free(ptr);
          }
        }

      } else if (allocationMode == AllocationMode.LENGTH) {
        frnd.setLength(this.size + HEADER_SIZE);
      } else {
        throw new IllegalStateException("Unknown allocation mode");
      }

      assert channel.size() == this.size + HEADER_SIZE;
      return currentSize;
    } finally {
      releaseWriteLock();
    }
  }

  /**
   * Shrink the file content (filledUpTo attribute only)
   */
  public void shrink(final long size) throws IOException {
    acquireWriteLock();
    try {
      //noinspection resource
      channel.truncate(HEADER_SIZE + size);
      this.size = size;

      assert this.size >= 0;

    } finally {
      releaseWriteLock();
    }
  }

  public long getFileSize() {
    return size;
  }

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

  public CountDownLatch write(List<ORawPair<Long, ByteBuffer>> buffers) throws IOException {
    final CountDownLatch latch = new CountDownLatch(0);

    for (final ORawPair<Long, ByteBuffer> pair : buffers) {
      final long position = pair.getFirst();

      final ByteBuffer buffer = pair.getSecond();
      write(position, buffer);
    }

    return latch;
  }

  /**
   * Synchronizes the buffered changes to disk.
   */
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
  public void create() throws IOException {
    acquireWriteLock();
    try {
      acquireExclusiveAccess();

      openChannel();
      init();

      setVersion(OFileClassic.CURRENT_VERSION);
      version = OFileClassic.CURRENT_VERSION;

      initAllocationMode();
    } finally {
      releaseWriteLock();
    }
  }

  private void initAllocationMode() {
    if (allocationMode != null) {
      return;
    }

    if (Platform.isLinux()) {
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
    } else if (Platform.isWindows()) {
      allocationMode = AllocationMode.LENGTH;
    } else {
      allocationMode = AllocationMode.WRITE;
    }
  }

  /**
   * ALWAYS ADD THE HEADER SIZE BECAUSE ON THIS TYPE IS ALWAYS NEEDED
   */
  private long checkRegions(final long iOffset, final long iLength) {
    acquireReadLock();
    try {
      if (iOffset < 0 || iOffset + iLength > size) {
        throw new OIOException(
            "You cannot access outside the file size (" + size + " bytes). You have requested portion from " + iOffset + "-" + (iOffset
                + iLength) + " bytes. File: " + this);
      }

      return iOffset + HEADER_SIZE;
    } finally {
      releaseReadLock();
    }

  }

  @SuppressWarnings("SameParameterValue")
  private void setVersion(final int version) throws IOException {
    acquireWriteLock();
    try {
      final ByteBuffer buffer = ByteBuffer.allocate(OBinaryProtocol.SIZE_BYTE);
      buffer.put((byte) version);
      buffer.rewind();
      writeByteBuffer(buffer, channel, VERSION_OFFSET);
      dirtyCounter.incrementAndGet();
    } finally {
      releaseWriteLock();
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
  public void open() {
    acquireWriteLock();
    try {
      if (!Files.exists(osFile)) {
        throw new FileNotFoundException("File: " + osFile);
      }

      acquireExclusiveAccess();

      openChannel();
      init();

      OLogManager.instance().debug(this, "Checking file integrity of " + osFile.getFileName() + "...");

      if (version < CURRENT_VERSION) {
        setVersion(CURRENT_VERSION);
        version = CURRENT_VERSION;
      }

      initAllocationMode();
    } catch (final IOException e) {
      throw OException.wrapException(new OIOException("Error during file open"), e);
    } finally {
      releaseWriteLock();
    }
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
  public void close() {
    try {
      acquireWriteLock();
      try {
        if (channel != null && channel.isOpen()) {
          channel.close();
          channel = null;
        }

        if (frnd != null) {
          frnd.close();
          frnd = null;
        }

        closeFD();
      } finally {
        releaseWriteLock();
      }

      dirtyCounter.set(0);
      releaseExclusiveAccess();
    } catch (final IOException ioe) {
      throw OException.wrapException(new OIOException("Error during file close"), ioe);
    }
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
  public void delete() throws IOException {
    acquireWriteLock();
    try {
      close();
      if (osFile != null) {
        Files.deleteIfExists(osFile);
      }
    } finally {
      releaseWriteLock();
    }
  }

  private void openChannel() throws IOException {
    acquireWriteLock();
    try {
      for (int i = 0; i < OPEN_RETRY_MAX; ++i) {
        try {
          frnd = new RandomAccessFile(osFile.toFile(), "rw");
          channel = frnd.getChannel();
          break;
        } catch (final FileNotFoundException e) {
          if (i == OPEN_RETRY_MAX - 1) {
            throw e;
          }

          // TRY TO RE-CREATE THE DIRECTORY (THIS HAPPENS ON WINDOWS AFTER A DELETE IS PENDING, USUALLY WHEN REOPEN THE DB VERY
          // FREQUENTLY)
          Files.createDirectories(osFile.getParent());
        }
      }

      if (channel == null) {
        throw new FileNotFoundException(osFile.toString());
      }

      if (channel.size() == 0) {
        final ByteBuffer buffer = ByteBuffer.allocate(HEADER_SIZE);
        OIOUtils.writeByteBuffer(buffer, channel, 0);
      }

    } finally {
      releaseWriteLock();
    }
  }

  private void init() throws IOException {
    size = channel.size() - HEADER_SIZE;
    assert size >= 0;

    final ByteBuffer buffer = ByteBuffer.allocate(1);

    channel.read(buffer, VERSION_OFFSET);

    buffer.position(0);
    version = buffer.get();
  }

  /*
   * (non-Javadoc)
   *
   * @see com.orientechnologies.orient.core.storage.fs.OFileAAA#isOpen()
   */
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
  public boolean exists() {
    acquireReadLock();
    try {
      return osFile != null && Files.exists(osFile);
    } finally {
      releaseReadLock();
    }
  }

  public String getName() {
    acquireReadLock();
    try {
      return osFile.getFileName().toString();
    } finally {
      releaseReadLock();
    }
  }

  public String getPath() {
    acquireReadLock();
    try {
      return osFile.toString();
    } finally {
      releaseReadLock();
    }
  }

  public void renameTo(final Path newFile) throws IOException {
    acquireWriteLock();
    try {
      close();

      //noinspection NonAtomicOperationOnVolatileField
      osFile = Files.move(osFile, newFile);

      open();
    } finally {
      releaseWriteLock();
    }
  }

  /**
   * Replaces the file content with the content of the provided file.
   *
   * @param newContentFile the new content file to replace the content with.
   */
  public void replaceContentWith(final Path newContentFile) throws IOException {
    acquireWriteLock();
    try {
      close();

      Files.copy(newContentFile, osFile, StandardCopyOption.REPLACE_EXISTING);

      open();
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
    LENGTH, DESCRIPTOR, WRITE
  }

}

