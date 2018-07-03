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
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.serialization.OBinaryProtocol;
import com.sun.jna.LastErrorException;
import com.sun.jna.Native;
import com.sun.jna.Platform;
import com.sun.jna.Pointer;

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

import static com.orientechnologies.common.io.OIOUtils.readByteBuffer;

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

  public OFileClassic(Path osFile) {
    this.osFile = osFile;
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
        final long ptr = Native.malloc(size);
        try {
          final ByteBuffer buffer = new Pointer(ptr).getByteBuffer(0, size);
          buffer.position(0);

          if (channel != null) {
            OIOUtils.writeByteBuffer(buffer, channel, currentSize + HEADER_SIZE);
          } else {
            OIOUtils.writeByteBuffer(buffer, fd, currentSize + HEADER_SIZE);
          }

        } finally {
          Native.free(ptr);
        }
      } else if (allocationMode == AllocationMode.DESCRIPTOR) {
        assert fd > 0;
        ONative.instance().fallocate(fd, currentSize + HEADER_SIZE, size);
      } else if (allocationMode == AllocationMode.LENGTH) {
        if (channel != null) {
          OIOUtils.writeByteBuffer(ByteBuffer.allocate(1), channel, this.size + HEADER_SIZE - 1);
        } else {
          OIOUtils.writeByteBuffer(ByteBuffer.allocate(1), fd, this.size + HEADER_SIZE - 1);
        }
      } else {
        throw new IllegalStateException("Unknown allocation mode");
      }

      assert channel.size() == this.size + HEADER_SIZE;
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
            ONative.instance().ftruncate(fd, 0);
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
  public void read(long offset, byte[] iData, int iLength, int iArrayOffset) throws IOException {
    int attempts = 0;

    while (true) {
      try {
        acquireReadLock();
        try {
          offset = checkRegions(offset, iLength);

          final ByteBuffer buffer = ByteBuffer.wrap(iData, iArrayOffset, iLength);

          if (channel != null) {
            readByteBuffer(buffer, channel, offset, true);
          } else {
            readByteBuffer(buffer, fd, offset, true);
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
          if (channel != null) {
            readByteBuffer(buffer, channel, offset, throwOnEof);
          } else {
            readByteBuffer(buffer, fd, offset, throwOnEof);
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
  public void read(long offset, ByteBuffer[] buffers, boolean throwOnEof) throws IOException {
    int attempts = 0;

    while (true) {
      try {
        acquireWriteLock();
        try {
          offset += HEADER_SIZE;

          if (channel != null) {
            //noinspection resource
            channel.position(offset);
            OIOUtils.readByteBuffers(buffers, channel, buffers.length * buffers[0].limit(), throwOnEof);
          } else {
            OIOUtils.readByteBuffers(buffers, fd, buffers.length * buffers[0].limit(), offset, throwOnEof);
          }
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

          if (channel != null) {
            OIOUtils.writeByteBuffer(buffer, channel, offset);
          } else {
            OIOUtils.writeByteBuffer(buffer, fd, offset);
          }
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
          if (channel != null) {
            channel.position(offset);
            OIOUtils.writeByteBuffers(buffers, channel, buffers.length * buffers[0].limit());
          } else {
            OIOUtils.writeByteBuffers(buffers, fd, offset, buffers.length * buffers[0].limit());
          }

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
      ByteBuffer byteBuffer = ByteBuffer.wrap(data, arrayOffset, size);
      if (channel != null) {
        OIOUtils.writeByteBuffer(byteBuffer, channel, offset);
      } else {
        OIOUtils.writeByteBuffer(byteBuffer, fd, offset);
      }
      setDirty();
    }
  }

  @Override
  public void read(long iOffset, byte[] iDestBuffer, int iLength) throws IOException {
    read(iOffset, iDestBuffer, iLength, 0);
  }

  @Override
  public int readInt(long iOffset) throws IOException {
    int attempts = 0;
    while (true) {
      try {
        acquireReadLock();
        try {
          iOffset = checkRegions(iOffset, OBinaryProtocol.SIZE_INT);
          return readData(iOffset, OBinaryProtocol.SIZE_INT).getInt();
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
  public long readLong(long iOffset) throws IOException {
    int attempts = 0;

    while (true) {
      try {
        acquireReadLock();
        try {
          iOffset = checkRegions(iOffset, OBinaryProtocol.SIZE_LONG);
          return readData(iOffset, OBinaryProtocol.SIZE_LONG).getLong();
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
  public void writeInt(long iOffset, final int iValue) throws IOException {
    int attempts = 0;

    while (true) {
      try {
        acquireWriteLock();
        try {
          iOffset += HEADER_SIZE;

          final ByteBuffer buffer = ByteBuffer.allocate(OBinaryProtocol.SIZE_INT);
          buffer.putInt(iValue);
          writeBuffer(buffer, iOffset);
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
  public void writeLong(long iOffset, final long iValue) throws IOException {
    int attempts = 0;

    while (true) {
      try {
        acquireWriteLock();
        try {
          iOffset += HEADER_SIZE;
          final ByteBuffer buffer = ByteBuffer.allocate(OBinaryProtocol.SIZE_LONG);
          buffer.putLong(iValue);
          writeBuffer(buffer, iOffset);
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
  public void writeByte(long iOffset, final byte iValue) throws IOException {
    int attempts = 0;
    while (true) {
      try {
        acquireWriteLock();
        try {
          iOffset += HEADER_SIZE;
          final ByteBuffer buffer = ByteBuffer.allocate(OBinaryProtocol.SIZE_BYTE);
          buffer.put(iValue);
          writeBuffer(buffer, iOffset);
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
  public void write(long iOffset, final byte[] iSourceBuffer) throws IOException {
    int attempts = 0;
    while (true) {
      try {
        acquireWriteLock();
        try {
          if (iSourceBuffer != null) {
            writeInternal(iOffset, iSourceBuffer, iSourceBuffer.length, 0);
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
  private long checkRegions(final long iOffset, final long iLength) {
    acquireReadLock();
    try {
      if (iOffset < 0 || iOffset + iLength > size)
        throw new OIOException(
            "You cannot access outside the file size (" + size + " bytes). You have requested portion " + iOffset + "-" + (iOffset
                + iLength) + " bytes. File: " + toString());

      return iOffset + HEADER_SIZE;
    } finally {
      releaseReadLock();
    }

  }

  private ByteBuffer readData(final long iOffset, final int iSize) throws IOException {
    ByteBuffer buffer = ByteBuffer.allocate(iSize);
    if (channel != null) {
      OIOUtils.readByteBuffer(buffer, channel, iOffset, true);
    } else {
      OIOUtils.readByteBuffer(buffer, fd, iOffset, true);
    }

    buffer.rewind();
    return buffer;
  }

  private void writeBuffer(final ByteBuffer buffer, final long offset) throws IOException {
    buffer.rewind();
    if (channel != null) {
      OIOUtils.writeByteBuffer(buffer, channel, offset);
    } else {
      OIOUtils.writeByteBuffer(buffer, fd, offset);
    }
  }

  @SuppressWarnings("SameParameterValue")
  private void setVersion(int version) throws IOException {
    acquireWriteLock();
    try {
      final ByteBuffer buffer = ByteBuffer.allocate(OBinaryProtocol.SIZE_BYTE);
      buffer.put((byte) version);
      writeBuffer(buffer, VERSION_OFFSET);
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
          fd = ONative.instance().open(osFile.toAbsolutePath().toString(), ONative.O_RDWR | ONative.O_DIRECT | ONative.O_CREAT);
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
        final ByteBuffer buffer = ByteBuffer.allocate(HEADER_SIZE);

        if (channel != null) {
          OIOUtils.writeByteBuffer(buffer, channel, 0);
        } else {
          OIOUtils.writeByteBuffer(buffer, fd, 0);
        }
      }

    } finally {
      releaseWriteLock();
    }
  }

  private void init() throws IOException {
    size = Files.size(osFile) - HEADER_SIZE;
    assert size >= 0;

    final ByteBuffer buffer = ByteBuffer.allocate(1);

    if (channel != null) {
      OIOUtils.readByteBuffer(buffer, channel, VERSION_OFFSET, true);
    } else {
      OIOUtils.readByteBuffer(buffer, fd, VERSION_OFFSET, true);
    }

    buffer.position(0);
    version = buffer.get();
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
      if (!dirty)
        dirty = true;
    } finally {
      releaseWriteLock();
    }
  }

  private void setHeaderDirty() {
    acquireWriteLock();
    try {
      if (!headerDirty)
        headerDirty = true;
    } finally {
      releaseWriteLock();
    }
  }

  @Override
  public String getName() {
    acquireReadLock();
    try {
      if (osFile == null)
        return null;

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
    if (attempt > 1 && e != null)
      throw e;

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

}

