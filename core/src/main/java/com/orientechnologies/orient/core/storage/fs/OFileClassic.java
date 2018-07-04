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
import com.orientechnologies.orient.core.serialization.OBinaryProtocol;
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
  private static final int    CURRENT_VERSION = 2;

  public static final int HEADER_SIZE    = 1024;
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

  private volatile long size;

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
    acquireWriteLock();
    try {
      final long currentSize = this.size;
      assert Files.size(osFile) == currentSize + HEADER_SIZE;

      //noinspection NonAtomicOperationOnVolatileField
      this.size += size;

      assert this.size >= size;

      assert allocationMode != null;
      if (allocationMode == AllocationMode.WRITE) {
        try (ByteBufferHolder byteBufferHolder = allocateByteBuffer(size)) {
          writeByteBuffer(byteBufferHolder.buffer(), currentSize + HEADER_SIZE);
        }
      } else if (allocationMode == AllocationMode.DESCRIPTOR) {
        assert fd > 0;
        ONative.instance().fallocate(fd, currentSize + HEADER_SIZE, size);
      } else if (allocationMode == AllocationMode.LENGTH) {
        try (ByteBufferHolder byteBufferHolder = allocateByteBuffer(1)) {
          writeByteBuffer(byteBufferHolder.buffer(), this.size + HEADER_SIZE - 1);
        }
      } else {
        throw new IllegalStateException("Unknown allocation mode");
      }

      assert Files.size(osFile) == this.size + HEADER_SIZE;
      return currentSize;
    } finally

    {
      releaseWriteLock();
    }
  }

  @Override
  public void shrink(long size) throws IOException {
    int attempts = 0;

    while (true) {
      try {
        acquireWriteLock();
        try {
          //noinspection resource
          if (channel != null) {
            channel.truncate(HEADER_SIZE + size);
          } else {
            assert fd > 0;
            ONative.instance().ftruncate(fd, HEADER_SIZE + size);
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
  public void read(long offset, byte[] data, int length, int arrayOffset) throws IOException {
    int attempts = 0;

    while (true) {
      try {
        acquireReadLock();
        try {
          offset = checkRegions(offset, length);

          try (ByteBufferHolder byteBufferHolder = allocateByteBuffer(length)) {
            final ByteBuffer buffer = byteBufferHolder.buffer();
            readByteBuffer(buffer, offset, true);

            buffer.position(0);
            buffer.get(data, arrayOffset, length);
          }
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
    int attempts = 0;

    while (true) {
      try {
        acquireWriteLock();
        try {
          offset += HEADER_SIZE;

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

    while (true) {
      try {
        acquireWriteLock();
        try {
          offset += HEADER_SIZE;

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
    int attempts = 0;

    while (true) {
      try {
        acquireWriteLock();
        try {
          offset += HEADER_SIZE;
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

  @Override
  public void write(long iOffset, byte[] iData, int iSize, int iArrayOffset) throws IOException {
    int attempts = 0;

    while (true) {
      try {
        acquireWriteLock();
        try {
          writeInternal(iOffset, iData, iSize, iArrayOffset);
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

  private void writeInternal(long offset, byte[] data, int size, int arrayOffset) throws IOException {
    if (data != null) {
      offset += HEADER_SIZE;

      try (ByteBufferHolder byteBufferHolder = allocateByteBuffer(data, arrayOffset, size)) {
        writeByteBuffer(byteBufferHolder.buffer(), offset);
      }

      setDirty();
    }
  }

  @Override
  public void read(long offset, byte[] destBuffer, int length) throws IOException {
    read(offset, destBuffer, length, 0);
  }

  @Override
  public int readInt(long offset) throws IOException {
    int attempts = 0;
    while (true) {
      try {
        acquireReadLock();
        try {
          offset = checkRegions(offset, OBinaryProtocol.SIZE_INT);

          try (ByteBufferHolder byteBufferHolder = readData(offset, OBinaryProtocol.SIZE_INT)) {
            return byteBufferHolder.buffer().getInt();
          }

        } finally {
          releaseReadLock();
          attempts++;
        }
      } catch (IOException e) {
        OLogManager.instance()
            .error(this, "Error during read of int data for file '" + getName() + "' " + attempts + "-th attempt", e);
        reopenFile(attempts, e);
      }
    }
  }

  @Override
  public long readLong(long offset) throws IOException {
    int attempts = 0;

    while (true) {
      try {
        acquireReadLock();
        try {
          offset = checkRegions(offset, OBinaryProtocol.SIZE_LONG);
          try (ByteBufferHolder byteBufferHolder = readData(offset, OBinaryProtocol.SIZE_LONG)) {
            return byteBufferHolder.buffer().getLong();
          }
        } finally {
          releaseReadLock();
          attempts++;
        }
      } catch (IOException e) {
        OLogManager.instance()
            .error(this, "Error during read of long data for file '" + getName() + "' " + attempts + "-th attempt", e);
        reopenFile(attempts, e);
      }
    }
  }

  @Override
  public void writeInt(long offset, final int value) throws IOException {
    int attempts = 0;

    while (true) {
      try {
        acquireWriteLock();
        try {
          offset += HEADER_SIZE;

          try (ByteBufferHolder byteBufferHolder = allocateByteBuffer(OBinaryProtocol.SIZE_INT)) {
            ByteBuffer buffer = byteBufferHolder.buffer();
            buffer.putInt(value);
            writeByteBuffer(buffer, offset);
          }

          setDirty();

          break;
        } finally {
          releaseWriteLock();
          attempts++;
        }
      } catch (IOException e) {
        OLogManager.instance()
            .error(this, "Error during write of int data for file '" + getName() + "' " + attempts + "-th attempt", e);
        reopenFile(attempts, e);
      }
    }
  }

  @Override
  public void writeLong(long offset, final long value) throws IOException {
    int attempts = 0;

    while (true) {
      try {
        acquireWriteLock();
        try {
          offset += HEADER_SIZE;

          try (ByteBufferHolder byteArrayBufferHolder = allocateByteBuffer(OBinaryProtocol.SIZE_LONG)) {
            final ByteBuffer buffer = byteArrayBufferHolder.buffer();
            buffer.putLong(value);
            writeByteBuffer(buffer, offset);
          }

          setDirty();
          break;
        } finally {
          releaseWriteLock();
          attempts++;
        }
      } catch (IOException e) {
        OLogManager.instance()
            .error(this, "Error during write of long data for file '" + getName() + "' " + attempts + "-th attempt", e);
        reopenFile(attempts, e);
      }
    }
  }

  @Override
  public void writeByte(long offset, final byte value) throws IOException {
    int attempts = 0;
    while (true) {
      try {
        acquireWriteLock();
        try {
          offset += HEADER_SIZE;

          try (ByteBufferHolder byteBufferHolder = allocateByteBuffer(OBinaryProtocol.SIZE_BYTE)) {
            final ByteBuffer buffer = byteBufferHolder.buffer();
            buffer.put(value);
            writeByteBuffer(buffer, offset);
          }

          setDirty();
          break;
        } finally {
          releaseWriteLock();
          attempts++;
        }
      } catch (IOException e) {
        OLogManager.instance()
            .error(this, "Error during write of byte data for file '" + getName() + "' " + attempts + "-th attempt", e);
        reopenFile(attempts, e);
      }
    }

  }

  @Override
  public void write(long offset, final byte[] sourceBuffer) throws IOException {
    int attempts = 0;
    while (true) {
      try {
        acquireWriteLock();
        try {
          if (sourceBuffer != null) {
            writeInternal(offset, sourceBuffer, sourceBuffer.length, 0);
            break;
          }
        } finally {
          releaseWriteLock();
          attempts++;
        }

      } catch (IOException e) {
        OLogManager.instance()
            .error(this, "Error during write of data for file '" + getName() + "' " + attempts + "-th attempt", e);
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

      openChannelOrFD();
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

      return offset + HEADER_SIZE;
    } finally {
      releaseReadLock();
    }

  }

  private ByteBufferHolder readData(final long offset, final int size) throws IOException {
    final ByteBufferHolder byteBufferHolder = allocateByteBuffer(size);
    final ByteBuffer buffer = byteBufferHolder.buffer();

    readByteBuffer(buffer, offset, true);
    buffer.rewind();

    return byteBufferHolder;
  }

  @SuppressWarnings("SameParameterValue")
  private void setVersion(int version) throws IOException {
    acquireWriteLock();
    try {
      try (ByteBufferHolder byteBufferHolder = allocateByteBuffer(OBinaryProtocol.SIZE_BYTE)) {
        final ByteBuffer buffer = byteBufferHolder.buffer();
        buffer.put((byte) version);
        writeByteBuffer(buffer, VERSION_OFFSET);
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
      if (!Files.exists(osFile))
        throw new FileNotFoundException("File: " + osFile);

      acquireExclusiveAccess();

      openChannelOrFD();
      init();

      OLogManager.instance().debug(this, "Checking file integrity of " + osFile.getFileName() + "...");

      if (version < CURRENT_VERSION) {
        setVersion(CURRENT_VERSION);
        version = CURRENT_VERSION;
      }

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

  private ByteBufferHolder allocateByteBuffer(byte[] data, int arrayOffset, int len) {
    if (blockSize <= 0) {
      return new ByteArrayBufferHolder(ByteBuffer.wrap(data, arrayOffset, len));
    } else {
      final ODirectMemoryAllocator allocator = ODirectMemoryAllocator.instance();
      final OPointer pointer = allocator.allocate(len, blockSize);
      final ByteBuffer buffer = pointer.getNativeByteBuffer();
      buffer.put(data, arrayOffset, len);
      buffer.position(0);

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
      if (Platform.isLinux() && blockSize > 0) {
        int fd = 0;
        try {
          fd = ONative.instance().open(osFile.toAbsolutePath().toString(), ONative.O_RDWR | ONative.O_CREAT);
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
        try (ByteBufferHolder byteBufferHolder = allocateByteBuffer(HEADER_SIZE)) {
          writeByteBuffer(byteBufferHolder.buffer(), 0);
        }
      }

    } finally {
      releaseWriteLock();
    }
  }

  private void init() throws IOException {
    size = Files.size(osFile) - HEADER_SIZE;
    assert size >= 0;

    try (ByteBufferHolder byteBufferHolder = allocateByteBuffer(1)) {
      final ByteBuffer buffer = byteBufferHolder.buffer();
      readByteBuffer(buffer, VERSION_OFFSET, true);
      version = buffer.get();
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

