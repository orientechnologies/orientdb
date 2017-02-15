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
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.serialization.OBinaryProtocol;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static com.orientechnologies.common.io.OIOUtils.*;

public class OFileClassic implements OFile, OClosableItem {
  public final static  String NAME            = "classic";
  private static final int    CURRENT_VERSION = 2;

  public static final  int HEADER_SIZE    = 1024;
  private static final int VERSION_OFFSET = 48;
  private static final int SIZE_OFFSET    = 52;

  private static final int OPEN_RETRY_MAX = 10;

  private final ReadWriteLock lock = new ReentrantReadWriteLock();

  private volatile Path osFile;

  private FileChannel channel;
  private volatile boolean dirty       = false;
  private volatile boolean headerDirty = false;
  private int version;

  private volatile long size;                                                                                // PART OF

  public OFileClassic(Path osFile) {
    this.osFile = osFile;
  }

  @Override
  public long allocateSpace(long size) throws IOException {
    acquireWriteLock();
    try {
      final long currentSize = this.size;
      this.size += size;

      assert this.size >= size;

      setSize(this.size);
      channel.truncate(this.size + HEADER_SIZE);

      return currentSize;
    } finally {
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
          channel.truncate(HEADER_SIZE + size);
          this.size = size;
          setSize(this.size);

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

  public void read(long offset, byte[] iData, int iLength, int iArrayOffset) throws IOException {
    int attempts = 0;

    while (true) {
      try {
        acquireReadLock();
        try {
          offset = checkRegions(offset, iLength);

          final ByteBuffer buffer = ByteBuffer.wrap(iData, iArrayOffset, iLength);
          readByteBuffer(buffer, channel, offset);
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
  public void read(long offset, ByteBuffer buffer) throws IOException {
    int attempts = 0;

    while (true) {
      try {
        acquireReadLock();
        try {
          offset = checkRegions(offset, buffer.limit());
          readByteBuffer(buffer, channel, offset);

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
  public void read(long offset, ByteBuffer[] buffers) throws IOException {
    int attempts = 0;

    while (true) {
      try {
        acquireWriteLock();
        try {
          offset += HEADER_SIZE;

          channel.position(offset);
          readByteBuffers(buffers, channel, buffers.length * buffers[0].limit());
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

          writeByteBuffer(buffer, channel, offset);
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
          channel.position(offset);
          writeByteBuffers(buffers, channel, buffers.length * buffers[0].limit());

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
      writeByteBuffer(byteBuffer, channel, offset);
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
  public short readShort(long iOffset) throws IOException {
    int attempts = 0;

    while (true) {
      try {
        acquireReadLock();
        try {
          iOffset = checkRegions(iOffset, OBinaryProtocol.SIZE_SHORT);
          return readData(iOffset, OBinaryProtocol.SIZE_SHORT).getShort();
        } finally {
          releaseReadLock();
          attempts++;
        }
      } catch (IOException e) {
        OLogManager.instance()
            .error(this, "Error during read of short data for file '" + getName() + "' " + attempts + "-th attempt", e);
        reopenFile(attempts, e);
      }
    }
  }

  @Override
  public byte readByte(long iOffset) throws IOException {
    int attempts = 0;

    while (true) {
      try {
        acquireReadLock();
        try {
          iOffset = checkRegions(iOffset, OBinaryProtocol.SIZE_BYTE);
          return readData(iOffset, OBinaryProtocol.SIZE_BYTE).get();
        } finally {
          releaseReadLock();
          attempts++;
        }
      } catch (IOException e) {
        OLogManager.instance()
            .error(this, "Error during read of byte data for file '" + getName() + "' " + attempts + "-th attempt", e);
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
  public void writeShort(long iOffset, final short iValue) throws IOException {
    int attempts = 0;

    while (true) {
      try {
        acquireWriteLock();
        try {
          iOffset += HEADER_SIZE;
          final ByteBuffer buffer = ByteBuffer.allocate(OBinaryProtocol.SIZE_SHORT);
          buffer.putShort(iValue);
          writeBuffer(buffer, iOffset);
          setDirty();
          break;
        } finally {
          releaseWriteLock();
          attempts++;
        }
      } catch (IOException e) {
        OLogManager.instance()
            .error(this, "Error during write of short data for file '" + getName() + "' " + attempts + "-th attempt", e);
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
  public boolean synch() throws IOException {
    acquireWriteLock();
    try {
      flushHeader();
      return true;
    } finally {
      releaseWriteLock();
    }
  }

  private void flushHeader() throws IOException {
    acquireWriteLock();
    try {
      if (headerDirty || dirty) {
        headerDirty = dirty = false;
        try {
          channel.force(false);
        } catch (IOException e) {
          OLogManager.instance()
              .warn(this, "Error during flush of file %s. Data may be lost in case of power failure", getName(), e);
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
      openChannel();
      init();

      setVersion(OFileClassic.CURRENT_VERSION);
      version = OFileClassic.CURRENT_VERSION;
    } finally {
      releaseWriteLock();
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
    readByteBuffer(buffer, channel, iOffset);
    buffer.rewind();
    return buffer;
  }

  private void writeBuffer(final ByteBuffer buffer, final long offset) throws IOException {
    buffer.rewind();
    writeByteBuffer(buffer, channel, offset);
  }

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

  private void setSize(long size) throws IOException {
    final ByteBuffer buffer = ByteBuffer.allocate(OLongSerializer.LONG_SIZE);
    buffer.putLong(size);
    writeBuffer(buffer, SIZE_OFFSET);
    setHeaderDirty();
  }

  private long getSize() throws IOException {
    if (channel.size() == 0)
      return 0;

    final ByteBuffer buffer = ByteBuffer.allocate(OLongSerializer.LONG_SIZE);

    readByteBuffer(buffer, channel, SIZE_OFFSET);
    return buffer.getLong(0);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.orientechnologies.orient.core.storage.fs.OFileAAA#open()
   */
  public void open() {
    acquireWriteLock();
    try {
      if (!Files.exists(osFile))
        throw new FileNotFoundException("File: " + osFile);

      openChannel();
      init();

      OLogManager.instance().debug(this, "Checking file integrity of " + osFile.getFileName() + "...");

      if (version < CURRENT_VERSION) {
        setVersion(CURRENT_VERSION);
        version = CURRENT_VERSION;
      }
    } catch (IOException e) {
      throw OException.wrapException(new OIOException("Error during file open"), e);
    } finally {
      releaseWriteLock();
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.orientechnologies.orient.core.storage.fs.OFileAAA#close()
   */
  public void close() {
    acquireWriteLock();
    try {
      if (channel != null && channel.isOpen()) {
        channel.close();
        channel = null;
      }
    } catch (Exception e) {
      final String message = "Error on closing file " + osFile;
      OLogManager.instance().error(this, message, e);
      throw OException.wrapException(new OIOException(message), e);
    } finally {
      releaseWriteLock();
    }
  }

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
      for (int i = 0; i < OPEN_RETRY_MAX; ++i)
        try {
          channel = FileChannel.open(osFile, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
          break;
        } catch (FileNotFoundException e) {
          if (i == OPEN_RETRY_MAX - 1)
            throw e;

          // TRY TO RE-CREATE THE DIRECTORY (THIS HAPPENS ON WINDOWS AFTER A DELETE IS PENDING, USUALLY WHEN REOPEN THE DB VERY
          // FREQUENTLY)
          Files.createDirectories(osFile.getParent());
        }

      if (channel == null)
        throw new FileNotFoundException(osFile.toString());

      if (channel.size() == 0) {
        final ByteBuffer buffer = ByteBuffer.allocate(HEADER_SIZE);
        OIOUtils.writeByteBuffer(buffer, channel, 0);
      }

    } finally {
      releaseWriteLock();
    }
  }

  private void init() throws IOException {
    size = getSize();
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

      osFile = Files.move(osFile, newFile);

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
    StringBuilder builder = new StringBuilder();
    builder.append("File: ");
    builder.append(osFile.getFileName());
    if (channel != null) {
      builder.append(" os-size=");
      try {
        builder.append(channel.size());
      } catch (IOException e) {
        builder.append("?");
      }
    }
    builder.append(", stored=");
    builder.append(getFileSize());
    builder.append("");
    return builder.toString();
  }

  private void reopenFile(int attempt, IOException e) throws IOException {
    if (attempt > 1 && e != null)
      throw e;

    acquireWriteLock();
    try {
      try {
        channel.close();
      } catch (IOException ioe) {
        OLogManager.instance()
            .error(this, "Error during channel close for file '" + osFile + "', during IO exception handling", ioe);
      }

      channel = null;

      openChannel();
    } finally {
      releaseWriteLock();
    }
  }



}

