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
import com.orientechnologies.common.directmemory.ODirectMemoryAllocator;
import com.orientechnologies.common.directmemory.OPointer;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.io.OIOException;
import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.common.jna.ONative;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.sun.jna.LastErrorException;
import com.sun.jna.Platform;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class OFileClassic implements OFile, OClosableItem {
  public final static  String NAME            = "classic";
  private static final int    CURRENT_VERSION = 3;

  public static final int HEADER_SIZE_V2 = 1024;
  public static final int HEADER_SIZE_V3 = 64 * 1024;

  public static final int VERSION_OFFSET = 48;

  private static final int OPEN_RETRY_MAX = 10;

  private final ReadWriteLock lock = new ReentrantReadWriteLock();

  private volatile Path osFile;

  private FileChannel channel;
  private int         fd;

  private final int blockSize;

  private volatile boolean dirty       = false;
  private volatile boolean headerDirty = false;
  private          int     version;

  private volatile long    size;
  private          int     headerSize;
  private          boolean allowDirectIO;

  private AllocationMode allocationMode;

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

  public OFileClassic(Path osFile, int blockSize) {
    this.osFile = osFile;
    this.blockSize = blockSize;
  }

  @Override
  public long allocateSpace(int size) throws IOException {
    if (allowDirectIO && size % blockSize != 0) {
      throw new IOException(
          "Allocated size should by quantified by block size in direct IO mode ( block size: " + blockSize + ", size :" + size
              + ")");
    }

    acquireWriteLock();
    try {
      final long currentSize = this.size;
      assert Files.size(osFile) == currentSize + headerSize;

      //noinspection NonAtomicOperationOnVolatileField
      this.size += size;

      assert this.size >= size;

      assert allocationMode != null;

      if (allocationMode == AllocationMode.WRITE) {
        try (ByteBufferHolder byteBufferHolder = allocateByteBuffer(size)) {
          writeByteBuffer(byteBufferHolder.buffer(), currentSize + headerSize);
        }
      } else if (allocationMode == AllocationMode.DESCRIPTOR) {
        assert fd > 0;
        ONative.instance().fallocate(fd, currentSize + headerSize, size);
      } else if (allocationMode == AllocationMode.LENGTH) {
        try (ByteBufferHolder byteBufferHolder = allocateByteBuffer(1)) {
          writeByteBuffer(byteBufferHolder.buffer(), this.size + headerSize - 1);
        }
      } else {
        throw new IllegalStateException("Unknown allocation mode");
      }

      assert Files.size(osFile) == this.size + headerSize;
      return currentSize;
    } finally

    {
      releaseWriteLock();
    }
  }

  @Override
  public void shrink(long size) throws IOException {
    if (allowDirectIO && size % blockSize != 0) {
      throw new IOException(
          "Allocated size should by quantified by block size in direct IO mode ( block size: " + blockSize + ", size :" + size
              + ")");
    }

    int attempts = 0;

    while (true) {
      try {
        acquireWriteLock();
        try {
          //noinspection resource
          if (channel != null) {
            channel.truncate(headerSize + size);
          } else {
            assert fd > 0;
            ONative.instance().ftruncate(fd, headerSize + size);
          }

          this.size = size;

          assert this.size >= 0;
          break;

        } finally {
          releaseWriteLock();
          attempts++;
        }
      } catch (IOException e) {
        OLogManager.instance().error(this, "Error during file shrink for file '" + getName() + "' " + attempts + "-th attempt", e);
        reopenFile(attempts, e);
      }
    }
  }

  @Override
  public long getFileSize() {
    return size;
  }

  @Override
  public void read(long offset, ByteBuffer buffer, boolean throwOnEof) throws IOException {
    int attempts = 0;

    while (true) {
      try {
        acquireReadLock();
        try {
          offset = checkRegions(offset, buffer.limit());
          readByteBuffer(buffer, offset, throwOnEof);

          break;
        } finally {
          releaseReadLock();
          attempts++;
        }
      } catch (IOException e) {
        OLogManager.instance().error(this, "Error during data read for file '" + getName() + "' " + attempts + "-th attempt", e);
        reopenFile(attempts, e);
      }
    }
  }

  @Override
  public void read(long offset, ByteBuffer[] buffers, boolean throwOnEof) throws IOException {
    if (buffers.length == 0) {
      return;
    }

    int attempts = 0;

    checkRegions(offset, buffers.length * buffers[0].limit());

    while (true) {
      try {
        acquireWriteLock();
        try {
          offset += headerSize;

          readByteBuffers(buffers, offset, throwOnEof);
          break;

        } finally {
          releaseWriteLock();
          attempts++;
        }
      } catch (IOException e) {
        OLogManager.instance().error(this, "Error during data read for file '" + getName() + "' " + attempts + "-th attempt", e);
        reopenFile(attempts, e);
      }
    }
  }

  @Override
  public void write(long offset, ByteBuffer buffer) throws IOException {
    int attempts = 0;

    checkRegions(offset, buffer.limit());
    while (true) {
      try {
        acquireWriteLock();
        try {
          offset += headerSize;

          writeByteBuffer(buffer, offset);
          setDirty();

          break;
        } finally {
          releaseWriteLock();
          attempts++;
        }
      } catch (IOException e) {
        OLogManager.instance().error(this, "Error during data write for file '" + getName() + "' " + attempts + "-th attempt", e);
        reopenFile(attempts, e);
      }
    }
  }

  @Override
  public void write(long offset, ByteBuffer[] buffers) throws IOException {
    if (buffers.length == 0) {
      return;
    }

    int attempts = 0;

    checkRegions(offset, buffers.length * buffers[0].limit());
    while (true) {
      try {
        acquireWriteLock();
        try {
          offset += headerSize;
          //noinspection resource

          writeByteBuffers(buffers, offset);
          setDirty();

          break;
        } finally {
          releaseWriteLock();
          attempts++;
        }
      } catch (IOException e) {
        OLogManager.instance().error(this, "Error during data write for file '" + getName() + "' " + attempts + "-th attempt", e);
        reopenFile(attempts, e);
      }
    }
  }

  /**
   * Synchronizes the buffered changes to disk.
   */
  @Override
  public void synch() {
    acquireWriteLock();
    try {
      flushHeader();
    } finally {
      releaseWriteLock();
    }
  }

  private void flushHeader() {
    acquireWriteLock();
    try {
      if (headerDirty || dirty) {
        headerDirty = dirty = false;
        try {
          if (channel != null) {
            channel.force(false);
          } else {
            ONative.instance().fsync(fd);
          }
        } catch (IOException e) {
          OLogManager.instance()
              .warn(this, "Error during flush of file %s. Data may be lost in case of power failure", e, getName());
        }

      }
    } finally {
      releaseWriteLock();
    }
  }

  @Override
  public void create() throws IOException {
    acquireWriteLock();
    try {
      acquireExclusiveAccess();

      headerSize = HEADER_SIZE_V3;
      allowDirectIO = blockSize > 0 && headerSize % blockSize == 0;

      version = OFileClassic.CURRENT_VERSION;
      size = Files.size(osFile) - headerSize;
      assert size >= 0;

      openChannelOrFD();
      setVersion(OFileClassic.CURRENT_VERSION);

      initAllocationMode();
    } finally {
      releaseWriteLock();
    }
  }

  private void initAllocationMode() {
    if (allocationMode != null) {
      return;
    }

    if (Platform.isLinux() && fd > 0) {
      allocationMode = AllocationMode.DESCRIPTOR;
    } else if (Platform.isWindows()) {
      allocationMode = AllocationMode.LENGTH;
    } else {
      allocationMode = AllocationMode.WRITE;
    }
  }

  /**
   * ALWAYS ADD THE HEADER SIZE BECAUSE ON THIS TYPE IS ALWAYS NEEDED
   */
  private long checkRegions(final long offset, final long length) {
    acquireReadLock();
    try {
      if (offset < 0 || offset + length > size)
        throw new OIOException(
            "You cannot access outside the file size (" + size + " bytes). You have requested portion " + offset + "-" + (offset
                + length) + " bytes. File: " + toString());

      if (allowDirectIO) {
        if (offset % blockSize != 0 || length % blockSize != 0) {
          throw new OIOException(
              "In direct IO mode both offset and lenth of content " + "should be quantified to boloc size ( block size : "
                  + blockSize + ", offset : " + offset + ", length : " + length + ")");
        }
      }

      return offset + headerSize;
    } finally {
      releaseReadLock();
    }

  }

  @SuppressWarnings("SameParameterValue")
  private void setVersion(int version) throws IOException {
    acquireWriteLock();
    try {
      try (ByteBufferHolder byteBufferHolder = allocateByteBuffer(headerSize)) {
        final ByteBuffer buffer = byteBufferHolder.buffer();
        buffer.put(VERSION_OFFSET, (byte) version);
        writeByteBuffer(buffer, 0);
      }

      setHeaderDirty();
    } finally {
      releaseWriteLock();
    }
  }

  /*
   * (non-Javadoc)
   *
   * @see com.orientechnologies.orient.core.storage.fs.OFileAAA#open()
   */
  @Override
  public void open() {
    acquireWriteLock();
    try {
      if (!Files.exists(osFile)) {
        throw new FileNotFoundException("File: " + osFile);
      }

      acquireExclusiveAccess();

      try (FileChannel channel = FileChannel.open(osFile, StandardOpenOption.READ)) {
        channel.position(VERSION_OFFSET);

        final ByteBuffer buffer = ByteBuffer.allocate(1);
        OIOUtils.readByteBuffer(buffer, channel);

        version = buffer.get(0);
        if (version < 3) {
          headerSize = HEADER_SIZE_V2;
        } else {
          headerSize = HEADER_SIZE_V3;
        }
      }

      size = Files.size(osFile) - headerSize;
      assert size >= 0;

      allowDirectIO = blockSize > 0 && headerSize % blockSize == 0;

      openChannelOrFD();
      initAllocationMode();
    } catch (IOException e) {
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
                for (StackTraceElement se : fileUser.openStackTrace) {
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

  /*
   * (non-Javadoc)
   *
   * @see com.orientechnologies.orient.core.storage.fs.OFileAAA#close()
   */
  @Override
  public void close() {
    int attempts = 0;

    while (true) {
      try {
        acquireWriteLock();
        try {
          if (channel != null && channel.isOpen()) {
            channel.close();
            channel = null;
          }

          closeFD();
        } finally {
          releaseWriteLock();
          attempts++;
        }

        releaseExclusiveAccess();
        break;
      } catch (IOException ioe) {
        OLogManager.instance().error(this, "Error during closing of file '" + getName() + "' " + attempts + "-th attempt", ioe);

        try {
          reopenFile(attempts, ioe);
        } catch (IOException e) {
          throw OException.wrapException(new OIOException("Error during file close"), e);
        }
      }
    }

  }

  private void closeFD() {
    if (fd > 0) {
      try {
        ONative.instance().close(fd);
      } catch (LastErrorException e) {
        OLogManager.instance()
            .warnNoDb(this, "Can not close Linux descriptor of file %s, error %d", osFile.toAbsolutePath().toString(),
                e.getErrorCode());
      }

      fd = 0;
    }
  }

  private ByteBufferHolder allocateByteBuffer(int size) {
    if (blockSize <= 0) {
      return new ByteArrayBufferHolder(ByteBuffer.allocate(size));
    } else {
      final ODirectMemoryAllocator allocator = ODirectMemoryAllocator.instance();
      final OPointer pointer = allocator.allocate(size, blockSize);
      final ByteBuffer buffer = pointer.getNativeByteBuffer();

      final byte[] zeros = new byte[size];
      buffer.put(zeros);
      buffer.rewind();
      return new DirectByteBufferHolder(pointer, allocator, buffer);
    }
  }

  /*
   * (non-Javadoc)
   *
   * @see com.orientechnologies.orient.core.storage.fs.OFileAAA#delete()
   */
  @Override
  public void delete() throws IOException {
    int attempts = 0;

    while (true) {
      try {
        acquireWriteLock();
        try {
          close();
          if (osFile != null) {
            Files.deleteIfExists(osFile);
          }
        } finally {
          releaseWriteLock();
          attempts++;
        }

        break;
      } catch (IOException ioe) {
        OLogManager.instance().error(this, "Error during deletion of file '" + getName() + "' " + attempts + "-th attempt", ioe);
        reopenFile(attempts, ioe);
      }
    }

  }

  private void openChannelOrFD() throws IOException {
    acquireWriteLock();
    try {
      if (Platform.isLinux()) {
        int fd = 0;
        try {
          int flags = ONative.O_RDWR | ONative.O_CREAT;

          if (allowDirectIO) {
            flags = flags | ONative.O_DIRECT;
          }

          fd = ONative.instance().open(osFile.toAbsolutePath().toString(), flags);
        } catch (LastErrorException e) {
          OLogManager.instance().warnNoDb(this, "File %s can not be opened using Linux native API," + ". Error code : %d.",
              osFile.toAbsolutePath().toString(), e.getErrorCode());
        }

        this.fd = fd;
      }

      if (fd <= 0) {
        for (int i = 0; i < OPEN_RETRY_MAX; ++i)
          try {
            channel = FileChannel.open(osFile, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.READ);
            break;
          } catch (FileNotFoundException e) {
            if (i == OPEN_RETRY_MAX - 1)
              throw e;

            // TRY TO RE-CREATE THE DIRECTORY (THIS HAPPENS ON WINDOWS AFTER A DELETE IS PENDING, USUALLY WHEN REOPEN THE DB VERY
            // FREQUENTLY)
            Files.createDirectories(osFile.getParent());
          }

        if (channel == null) {
          throw new FileNotFoundException(osFile.toString());
        }
      }

      if (Files.size(osFile) == 0) {
        try (ByteBufferHolder byteBufferHolder = allocateByteBuffer(headerSize)) {
          writeByteBuffer(byteBufferHolder.buffer(), 0);
        }
      }

    } finally {
      releaseWriteLock();
    }
  }

  private void writeByteBuffer(ByteBuffer byteBuffer, long position) throws IOException {
    byteBuffer.rewind();

    if (fd > 0) {
      OIOUtils.writeByteBuffer(byteBuffer, fd, position);
    } else {
      if (channel == null) {
        throw new IllegalStateException("File channel is not initialized");
      }

      OIOUtils.writeByteBuffer(byteBuffer, channel, position);
    }
  }

  private void writeByteBuffers(ByteBuffer[] buffers, long position) throws IOException {
    if (fd > 0) {
      OIOUtils.writeByteBuffers(buffers, fd, position, buffers.length * buffers[0].limit());
    } else {
      if (channel == null) {
        throw new IllegalStateException("File channel is not initialized");
      }

      channel.position(position);
      OIOUtils.writeByteBuffers(buffers, channel, buffers.length * buffers[0].limit());
    }
  }

  private void readByteBuffer(ByteBuffer byteBuffer, long position, boolean throwEndOf) throws IOException {
    byteBuffer.rewind();

    if (fd > 0) {
      OIOUtils.readByteBuffer(byteBuffer, fd, position, throwEndOf);
    } else {
      if (channel == null) {
        throw new IllegalStateException("File channel is not initialized");
      }

      OIOUtils.readByteBuffer(byteBuffer, channel, position, throwEndOf);
    }

    byteBuffer.rewind();
  }

  private void readByteBuffers(ByteBuffer[] buffers, long position, boolean throwEndOf) throws IOException {
    if (fd > 0) {
      OIOUtils.readByteBuffers(buffers, fd, position, buffers.length * buffers[0].limit(), throwEndOf);
    } else {
      if (channel == null) {
        throw new IllegalStateException("File channel is not initialized");
      }

      channel.position(position);
      OIOUtils.readByteBuffers(buffers, channel, buffers.length * buffers[0].limit(), throwEndOf);
    }
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
      return channel != null || fd > 0;
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

  private void setDirty() {
    acquireWriteLock();
    try {
      if (!dirty) {
        dirty = true;
      }
    } finally {
      releaseWriteLock();
    }
  }

  private void setHeaderDirty() {
    acquireWriteLock();
    try {
      if (!headerDirty) {
        headerDirty = true;
      }
    } finally {
      releaseWriteLock();
    }
  }

  @Override
  public String getName() {
    acquireReadLock();
    try {
      if (osFile == null) {
        return null;
      }

      return osFile.getFileName().toString();
    } finally {
      releaseReadLock();
    }
  }

  @Override
  public String getPath() {
    acquireReadLock();
    try {
      return osFile.toString();
    } finally {
      releaseReadLock();
    }
  }

  @Override
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

  @Override
  public void replaceContentWith(Path newContentFile) throws IOException {
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
    return "File: " + osFile.getFileName() + ", stored=" + getFileSize();
  }

  private void reopenFile(int attempt, IOException e) throws IOException {
    if (attempt > 1 && e != null) {
      throw e;
    }

    acquireWriteLock();
    try {
      try {
        if (channel != null) {
          channel.close();
        } else {
          closeFD();
        }
      } catch (IOException | LastErrorException ioe) {
        OLogManager.instance()
            .error(this, "Error during channel close for file '" + osFile + "', during IO exception handling", ioe);
      }

      channel = null;
      fd = 0;

      openChannelOrFD();
    } finally {
      releaseWriteLock();
    }
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

    FileUser(int users, StackTraceElement[] openStackTrace) {
      this.users = users;
      this.openStackTrace = openStackTrace;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o)
        return true;
      if (o == null || getClass() != o.getClass())
        return false;
      FileUser fileUser = (FileUser) o;
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

  private interface ByteBufferHolder extends AutoCloseable {
    ByteBuffer buffer();

    void close();
  }

  private static final class ByteArrayBufferHolder implements ByteBufferHolder {
    private final ByteBuffer buffer;

    private ByteArrayBufferHolder(ByteBuffer buffer) {
      this.buffer = buffer;
    }

    @Override
    public ByteBuffer buffer() {
      return buffer;
    }

    @Override
    public void close() {
    }
  }

  private static final class DirectByteBufferHolder implements ByteBufferHolder {
    private final OPointer               pointer;
    private final ODirectMemoryAllocator allocator;
    private final ByteBuffer             buffer;

    DirectByteBufferHolder(OPointer pointer, ODirectMemoryAllocator allocator, ByteBuffer buffer) {
      this.pointer = pointer;
      this.allocator = allocator;
      this.buffer = buffer;
    }

    @Override
    public ByteBuffer buffer() {
      return buffer;
    }

    @Override
    public void close() {
      allocator.deallocate(pointer);
    }
  }
}

