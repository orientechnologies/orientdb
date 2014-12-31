/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
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
 *  * For more information: http://www.orientechnologies.com
 *
 */
package com.orientechnologies.orient.core.storage.fs;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.serialization.OBinaryProtocol;

public class OFileClassic extends OAbstractFile {
  public final static String NAME                = "classic";
  protected ByteBuffer       internalWriteBuffer = ByteBuffer.allocate(OBinaryProtocol.SIZE_LONG);

  @Override
  public long allocateSpace(long size) throws IOException {
    acquireWriteLock();
    try {
      final long currentSize = this.size;
      if (maxSize > 0 && currentSize + size > maxSize)
        throw new IllegalArgumentException("Cannot enlarge file since the configured max size ("
            + OFileUtils.getSizeAsString(maxSize) + ") was reached! " + toString());

      this.size += size;
      return currentSize;
    } finally {
      releaseWriteLock();
    }
  }

  @Override
  public void shrink(long iSize) throws IOException {
    acquireWriteLock();
    try {
      channel.truncate(HEADER_SIZE + iSize);
      size = iSize;
    } finally {
      releaseWriteLock();
    }
  }

  @Override
  public long getFileSize() {
    return size;
  }

  @Override
  public long getFilledUpTo() {
    return size;
  }

  public void read(long iOffset, byte[] iData, int iLength, int iArrayOffset) throws IOException {
    acquireReadLock();
    try {
      iOffset = checkRegions(iOffset, iLength);

      ByteBuffer buffer = ByteBuffer.wrap(iData, iArrayOffset, iLength);
      channel.read(buffer, iOffset);
    } finally {
      releaseReadLock();
    }
  }

  public void write(long iOffset, byte[] iData, int iSize, int iArrayOffset) throws IOException {
    acquireWriteLock();
    try {
      writeInternal(iOffset, iData, iSize, iArrayOffset);
    } finally {
      releaseWriteLock();
    }
  }

  private void writeInternal(long iOffset, byte[] iData, int iSize, int iArrayOffset) throws IOException {
    if (iData != null) {
      iOffset += HEADER_SIZE;
      ByteBuffer byteBuffer = ByteBuffer.wrap(iData, iArrayOffset, iSize);
      channel.write(byteBuffer, iOffset);
      setDirty();
    }
  }

  @Override
  public void read(long iOffset, byte[] iDestBuffer, int iLenght) throws IOException {
    read(iOffset, iDestBuffer, iLenght, 0);
  }

  @Override
  public int readInt(long iOffset) throws IOException {
    acquireReadLock();
    try {
      iOffset = checkRegions(iOffset, OBinaryProtocol.SIZE_INT);
      return readData(iOffset, OBinaryProtocol.SIZE_INT).getInt();
    } finally {
      releaseReadLock();
    }
  }

  @Override
  public long readLong(long iOffset) throws IOException {
    acquireReadLock();
    try {
      iOffset = checkRegions(iOffset, OBinaryProtocol.SIZE_LONG);
      return readData(iOffset, OBinaryProtocol.SIZE_LONG).getLong();
    } finally {
      releaseReadLock();
    }
  }

  @Override
  public short readShort(long iOffset) throws IOException {
    acquireReadLock();
    try {
      iOffset = checkRegions(iOffset, OBinaryProtocol.SIZE_SHORT);
      return readData(iOffset, OBinaryProtocol.SIZE_SHORT).getShort();
    } finally {
      releaseReadLock();
    }
  }

  @Override
  public byte readByte(long iOffset) throws IOException {
    acquireReadLock();
    try {
      iOffset = checkRegions(iOffset, OBinaryProtocol.SIZE_BYTE);
      return readData(iOffset, OBinaryProtocol.SIZE_BYTE).get();
    } finally {
      releaseReadLock();
    }
  }

  @Override
  public void writeInt(long iOffset, final int iValue) throws IOException {
    acquireWriteLock();
    try {
      iOffset += HEADER_SIZE;
      final ByteBuffer buffer = getWriteBuffer(OBinaryProtocol.SIZE_INT);
      buffer.putInt(iValue);
      writeBuffer(buffer, iOffset);
      setDirty();
    } finally {
      releaseWriteLock();
    }
  }

  @Override
  public void writeLong(long iOffset, final long iValue) throws IOException {
    acquireWriteLock();
    try {
      iOffset += HEADER_SIZE;
      final ByteBuffer buffer = getWriteBuffer(OBinaryProtocol.SIZE_LONG);
      buffer.putLong(iValue);
      writeBuffer(buffer, iOffset);
      setDirty();
    } finally {
      releaseWriteLock();
    }
  }

  @Override
  public void writeShort(long iOffset, final short iValue) throws IOException {
    acquireWriteLock();
    try {
      iOffset += HEADER_SIZE;
      final ByteBuffer buffer = getWriteBuffer(OBinaryProtocol.SIZE_SHORT);
      buffer.putShort(iValue);
      writeBuffer(buffer, iOffset);
      setDirty();
    } finally {
      releaseWriteLock();
    }
  }

  @Override
  public void writeByte(long iOffset, final byte iValue) throws IOException {
    acquireWriteLock();
    try {
      iOffset += HEADER_SIZE;
      final ByteBuffer buffer = getWriteBuffer(OBinaryProtocol.SIZE_BYTE);
      buffer.put(iValue);
      writeBuffer(buffer, iOffset);
      setDirty();
    } finally {
      releaseWriteLock();
    }

  }

  @Override
  public long write(long iOffset, final byte[] iSourceBuffer) throws IOException {
    long allocationDiff = 0;

    acquireWriteLock();
    try {
      if (iSourceBuffer != null) {
        final long start = accessFile.length();
        writeInternal(iOffset, iSourceBuffer, iSourceBuffer.length, 0);
        final long end = accessFile.length();
        allocationDiff = end - start;
      }
    } finally {
      releaseWriteLock();
    }

    return allocationDiff;
  }

  /**
   * Synchronizes the buffered changes to disk.
   * 
   * @throws IOException
   * 
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

  protected void flushHeader() throws IOException {
    acquireWriteLock();
    try {
      if (headerDirty || dirty) {
        headerDirty = dirty = false;
        try {
          channel.force(false);
        } catch (IOException e) {
          OLogManager.instance().warn(this, "Error during flush of file %s. Data may be lost in case of power failure.", getName());
        }

      }
    } finally {
      releaseWriteLock();
    }
  }

  @Override
  public void create(int iStartSize) throws IOException {
    acquireWriteLock();
    try {
      super.create(HEADER_SIZE);
    } finally {
      releaseWriteLock();
    }
  }

  @Override
  protected void init() throws IOException {
    acquireWriteLock();
    try {
      size = accessFile.length() - HEADER_SIZE;
    } finally {
      releaseWriteLock();
    }

  }

  @Override
  protected void setFilledUpTo(final long value) throws IOException {
    setFilledUpTo(value, false);
  }

  @Override
  protected void setFilledUpTo(long iHow, boolean force) {
    acquireWriteLock();
    try {
      size = iHow;
    } finally {
      releaseWriteLock();
    }
  }

  @Override
  public void setSize(final long iSize) throws IOException {
    setSize(iSize, false);
  }

  @Override
  protected void setSize(long size, boolean force) throws IOException {
  }

  @Override
  public void writeHeaderLong(final int iPosition, final long iValue) throws IOException {
    acquireWriteLock();
    try {
      final ByteBuffer buffer = getWriteBuffer(OBinaryProtocol.SIZE_LONG);
      buffer.putLong(iValue);
      writeBuffer(buffer, HEADER_DATA_OFFSET + iPosition);
      setHeaderDirty();
    } finally {
      releaseWriteLock();
    }
  }

  @Override
  public long readHeaderLong(final int iPosition) throws IOException {
    acquireReadLock();
    try {
      return readData(HEADER_DATA_OFFSET + iPosition, OBinaryProtocol.SIZE_LONG).getLong();
    } finally {
      releaseReadLock();
    }
  }

  @Override
  public boolean isSoftlyClosed() throws IOException {
    acquireReadLock();
    try {
      final ByteBuffer buffer;
      if (version == 0)
        buffer = readData(SOFTLY_CLOSED_OFFSET_V_0, 1);
      else
        buffer = readData(SOFTLY_CLOSED_OFFSET, 1);

      return buffer.get(0) > 0;
    } finally {
      releaseReadLock();
    }
  }

  public void setSoftlyClosed(final boolean value) throws IOException {
    acquireWriteLock();
    try {
      if (channel == null || mode.indexOf('w') < 0)
        return;

      final ByteBuffer buffer = getBuffer(1);
      buffer.put(0, (byte) (value ? 1 : 0));

      writeBuffer(buffer, SOFTLY_CLOSED_OFFSET);

      try {
        channel.force(true);
      } catch (IOException e) {
        OLogManager.instance().warn(this, "Error during flush of file %s. Data may be lost in case of power failure.", getName());
      }
    } finally {
      releaseWriteLock();
    }
  }

  /**
   * ALWAYS ADD THE HEADER SIZE BECAUSE ON THIS TYPE IS ALWAYS NEEDED
   */
  @Override
  protected long checkRegions(final long iOffset, final long iLength) {
    acquireReadLock();
    try {
      return super.checkRegions(iOffset, iLength) + HEADER_SIZE;
    } finally {
      releaseReadLock();
    }

  }

  private ByteBuffer readData(final long iOffset, final int iSize) throws IOException {
    ByteBuffer buffer = getBuffer(iSize);
    channel.read(buffer, iOffset);
    buffer.rewind();
    return buffer;
  }

  private void writeBuffer(final ByteBuffer iBuffer, final long iOffset) throws IOException {
    iBuffer.rewind();
    channel.write(iBuffer, iOffset);
  }

  private ByteBuffer getBuffer(final int iLenght) {
    return ByteBuffer.allocate(iLenght);
  }

  private ByteBuffer getWriteBuffer(final int iLenght) {
    setDirty();
    if (iLenght <= OBinaryProtocol.SIZE_LONG)
      // RECYCLE WRITE BYTE BUFFER SINCE WRITES ARE SYNCHRONIZED
      return (ByteBuffer) internalWriteBuffer.rewind();

    return getBuffer(iLenght);
  }

  @Override
  protected void setVersion(int version) throws IOException {
    acquireWriteLock();
    try {
      final ByteBuffer buffer = getWriteBuffer(OBinaryProtocol.SIZE_BYTE);
      buffer.put((byte) version);
      writeBuffer(buffer, VERSION_OFFSET);
      setHeaderDirty();
    } finally {
      releaseWriteLock();
    }
  }
}
