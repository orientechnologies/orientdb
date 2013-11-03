/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.core.storage.fs;

import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.io.OIOException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.profiler.OAbstractProfiler.OProfilerHookValue;
import com.orientechnologies.common.profiler.OProfilerMBean.METRIC_TYPE;
import com.orientechnologies.common.util.OByteBufferUtils;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.serialization.OBinaryProtocol;

/**
 * OFile implementation that use the Memory Mapping techniques to get faster access on read/write. The Memory Mapping is handled by
 * {@link OMMapManager} class. If the Memory Mapping buffer pools is full and a new segment is requested, then read/write are
 * executed against the channel directly without Memory Mapping.<br/>
 * Header structure:<br/>
 * <br/>
 * +-----------+--------------+---------------+---------------+<br/>
 * | FILE SIZE | FILLED UP TO | SOFTLY CLOSED | SECURITY CODE |<br/>
 * | 8 bytes . | 8 bytes .... | 1 byte ...... | 32 bytes .... |<br/>
 * +-----------+--------------+---------------+---------------+<br/>
 * = 1024 bytes<br/>
 * <br/>
 */
public class OFileMMap extends OAbstractFile {
  public final static String                 NAME                      = "mmap";
  // PART OF HEADER (4 bytes)
  protected volatile long                    filledUpTo;
  protected volatile MappedByteBuffer        headerBuffer;
  protected static final Queue<ByteBuffer>   bufferPool                = new ConcurrentLinkedQueue<ByteBuffer>();

  private static int                         BYTEBUFFER_POOLABLE_SIZE;
  private static OMMapManager.ALLOC_STRATEGY strategy;

  private static long                        metricPooledBufferCreated = 0;
  private static long                        metricPooledBufferUsed    = 0;
  private static long                        metricNonPooledBufferUsed = 0;

  static {
    Orient
        .instance()
        .getProfiler()
        .registerHookValue("system.file.mmap.pooledBufferCreated", "Number of file buffers created", METRIC_TYPE.COUNTER,
            new OProfilerHookValue() {
              public Object getValue() {
                return metricPooledBufferCreated;
              }
            });
    Orient
        .instance()
        .getProfiler()
        .registerHookValue("system.file.mmap.pooledBufferUsed", "Number of times a file buffers has been reused",
            METRIC_TYPE.COUNTER, new OProfilerHookValue() {
              public Object getValue() {
                return metricPooledBufferUsed;
              }
            });
    Orient
        .instance()
        .getProfiler()
        .registerHookValue("system.file.mmap.nonPooledBufferUsed", "Number of times a file buffers has not been reused",
            METRIC_TYPE.COUNTER, new OProfilerHookValue() {
              public Object getValue() {
                return metricNonPooledBufferUsed;
              }
            });

    BYTEBUFFER_POOLABLE_SIZE = OGlobalConfiguration.FILE_MMAP_BUFFER_SIZE.getValueAsInteger();
    strategy = OMMapManager.ALLOC_STRATEGY.values()[OGlobalConfiguration.FILE_MMAP_STRATEGY.getValueAsInteger()];
  }

  @Override
  public void delete() throws IOException {
    acquireWriteLock();
    try {
      OMMapManagerLocator.getInstance().removeFile(this);
      super.delete();
    } finally {
      releaseWriteLock();
    }
  }

  public long getFileSize() {
    return size;
  }

  public long getFilledUpTo() {
    return filledUpTo;
  }

  @Override
  public void read(long iOffset, byte[] iData, int iLength, int iArrayOffset) throws IOException {
    acquireReadLock();
    try {
      byte[] source = new byte[iLength];
      readInternal(iOffset, source, iLength);
      System.arraycopy(source, 0, iData, iArrayOffset, iLength);
    } finally {
      releaseReadLock();
    }
  }

  @Override
  public void read(long iOffset, final byte[] iDestBuffer, final int iLenght) throws IOException {
    acquireReadLock();
    try {
      readInternal(iOffset, iDestBuffer, iLenght);
    } finally {
      releaseReadLock();
    }
  }

  @Override
  public int readInt(long iOffset) throws IOException {
    acquireReadLock();
    try {

      iOffset = checkRegions(iOffset, OBinaryProtocol.SIZE_INT);
      final OMMapBufferEntry[] entries = OMMapManagerLocator.getInstance().acquire(this, iOffset, OBinaryProtocol.SIZE_INT,
          OMMapManager.OPERATION_TYPE.READ, strategy);
      if (entries != null) {
        // MMAP READ
        try {

          if (entries.length == 1)
            return entries[0].buffer.getInt((int) (iOffset - entries[0].beginOffset));
          else {
            entries[0].buffer.position((int) (iOffset - entries[0].beginOffset));
            entries[1].buffer.position(0);
            return OByteBufferUtils.mergeIntFromBuffers(entries[0].buffer, entries[1].buffer);
          }

        } finally {
          OMMapManagerLocator.getInstance().release(entries, OMMapManager.OPERATION_TYPE.READ);
        }
      } else {
        // DIRECT READ
        final ByteBuffer buffer = acquireByteBuffer(OBinaryProtocol.SIZE_INT);
        channel.read(buffer, iOffset + HEADER_SIZE);
        buffer.rewind();
        final int value = buffer.getInt();
        releaseByteBuffer(buffer);
        return value;
      }
    } finally {
      releaseReadLock();
    }
  }

  @Override
  public long readLong(long iOffset) throws IOException {
    acquireReadLock();
    try {
      iOffset = checkRegions(iOffset, OBinaryProtocol.SIZE_LONG);
      final OMMapBufferEntry[] entries = OMMapManagerLocator.getInstance().acquire(this, iOffset, OBinaryProtocol.SIZE_LONG,
          OMMapManager.OPERATION_TYPE.READ, strategy);
      if (entries != null) {
        // MMAP READ
        try {
          if (entries.length == 1)
            return entries[0].buffer.getLong((int) (iOffset - entries[0].beginOffset));
          else {
            entries[0].buffer.position((int) (iOffset - entries[0].beginOffset));
            entries[1].buffer.position(0);
            return OByteBufferUtils.mergeLongFromBuffers(entries[0].buffer, entries[1].buffer);
          }

        } finally {
          OMMapManagerLocator.getInstance().release(entries, OMMapManager.OPERATION_TYPE.READ);
        }
      } else {
        // DIRECT READ
        final ByteBuffer buffer = acquireByteBuffer(OBinaryProtocol.SIZE_LONG);
        channel.read(buffer, iOffset + HEADER_SIZE);
        buffer.rewind();
        final long value = buffer.getLong();
        releaseByteBuffer(buffer);
        return value;
      }
    } finally {
      releaseReadLock();
    }
  }

  @Override
  public short readShort(long iOffset) throws IOException {
    acquireReadLock();
    try {
      iOffset = checkRegions(iOffset, OBinaryProtocol.SIZE_SHORT);
      final OMMapBufferEntry[] entries = OMMapManagerLocator.getInstance().acquire(this, iOffset, OBinaryProtocol.SIZE_SHORT,
          OMMapManager.OPERATION_TYPE.READ, strategy);
      if (entries != null) {
        // MMAP READ
        try {
          if (entries.length == 1) {
            return entries[0].buffer.getShort((int) (iOffset - entries[0].beginOffset));
          } else {
            entries[0].buffer.position((int) (iOffset - entries[0].beginOffset));
            entries[1].buffer.position(0);

            return OByteBufferUtils.mergeShortFromBuffers(entries[0].buffer, entries[1].buffer);
          }

        } finally {
          OMMapManagerLocator.getInstance().release(entries, OMMapManager.OPERATION_TYPE.READ);
        }
      } else {
        // DIRECT READ
        final ByteBuffer buffer = acquireByteBuffer(OBinaryProtocol.SIZE_SHORT);
        channel.read(buffer, iOffset + HEADER_SIZE);
        buffer.rewind();
        final short value = buffer.getShort();
        releaseByteBuffer(buffer);
        return value;
      }
    } finally {
      releaseReadLock();
    }
  }

  @Override
  public byte readByte(long iOffset) throws IOException {
    acquireReadLock();
    try {
      iOffset = checkRegions(iOffset, OBinaryProtocol.SIZE_BYTE);
      final OMMapBufferEntry[] entries = OMMapManagerLocator.getInstance().acquire(this, iOffset, OBinaryProtocol.SIZE_BYTE,
          OMMapManager.OPERATION_TYPE.READ, strategy);
      if (entries != null) {
        // MMAP READ
        try {
          return entries[0].buffer.get((int) (iOffset - entries[0].beginOffset));
        } finally {
          OMMapManagerLocator.getInstance().release(entries, OMMapManager.OPERATION_TYPE.READ);
        }
      } else {
        // DIRECT READ
        final ByteBuffer buffer = acquireByteBuffer(OBinaryProtocol.SIZE_BYTE);
        channel.read(buffer, iOffset + HEADER_SIZE);
        buffer.rewind();
        final byte value = buffer.get();
        releaseByteBuffer(buffer);
        return value;
      }
    } finally {
      releaseReadLock();
    }
  }

  private void readInternal(long iOffset, byte[] iDestBuffer, int iLenght) throws IOException {
    iOffset = checkRegions(iOffset, iLenght);

    final OMMapBufferEntry[] entries = OMMapManagerLocator.getInstance().acquire(this, iOffset, iLenght,
        OMMapManager.OPERATION_TYPE.READ, strategy);
    if (entries != null) {
      // MMAP READ
      try {
        int position = (int) (iOffset - entries[0].beginOffset);

        int remaining;
        int remainingLength = iLenght;
        for (OMMapBufferEntry entry : entries) {
          entry.buffer.position(position);
          remaining = entry.buffer.remaining();
          int toRead = Math.min(remaining, remainingLength);
          entry.buffer.get(iDestBuffer, iLenght - remainingLength, toRead);
          position = 0;
          remainingLength -= toRead;
        }
      } finally {
        OMMapManagerLocator.getInstance().release(entries, OMMapManager.OPERATION_TYPE.READ);
      }
    } else {
      // DIRECT READ
      final ByteBuffer buffer = acquireByteBuffer(iLenght);
      channel.read(buffer, iOffset + HEADER_SIZE);
      buffer.rewind();
      buffer.get(iDestBuffer);
      releaseByteBuffer(buffer);
    }
  }

  @Override
  public void writeInt(long iOffset, final int iValue) throws IOException {
    acquireWriteLock();
    try {
      iOffset = checkRegions(iOffset, OBinaryProtocol.SIZE_INT);
      final OMMapBufferEntry[] entries = OMMapManagerLocator.getInstance().acquire(this, iOffset, OBinaryProtocol.SIZE_INT,
          OMMapManager.OPERATION_TYPE.WRITE, strategy);
      if (entries != null) {
        // MMAP WRITE
        try {
          if (entries.length == 1)
            entries[0].buffer.putInt((int) (iOffset - entries[0].beginOffset), iValue);
          else {
            entries[0].buffer.position((int) (iOffset - entries[0].beginOffset));
            entries[1].buffer.position(0);

            OByteBufferUtils.splitIntToBuffers(entries[0].buffer, entries[1].buffer, iValue);
          }
        } finally {
          OMMapManagerLocator.getInstance().release(entries, OMMapManager.OPERATION_TYPE.WRITE);
        }
      } else {
        // DIRECT WRITE
        final ByteBuffer buffer = acquireByteBuffer(OBinaryProtocol.SIZE_INT);
        buffer.putInt(iValue);
        buffer.rewind();
        channel.write(buffer, iOffset + HEADER_SIZE);
        releaseByteBuffer(buffer);
      }
    } finally {
      releaseWriteLock();
    }
  }

  @Override
  public void writeLong(long iOffset, final long iValue) throws IOException {
    acquireWriteLock();
    try {

      iOffset = checkRegions(iOffset, OBinaryProtocol.SIZE_LONG);
      final OMMapBufferEntry[] entries = OMMapManagerLocator.getInstance().acquire(this, iOffset, OBinaryProtocol.SIZE_LONG,
          OMMapManager.OPERATION_TYPE.WRITE, strategy);
      if (entries != null) {
        // MMAP WRITE
        try {
          if (entries.length == 1)
            entries[0].buffer.putLong((int) (iOffset - entries[0].beginOffset), iValue);
          else {
            entries[0].buffer.position((int) (iOffset - entries[0].beginOffset));
            entries[1].buffer.position(0);

            OByteBufferUtils.splitLongToBuffers(entries[0].buffer, entries[1].buffer, iValue);
          }
        } finally {
          OMMapManagerLocator.getInstance().release(entries, OMMapManager.OPERATION_TYPE.WRITE);
        }
      } else {
        // DIRECT WRITE
        final ByteBuffer buffer = acquireByteBuffer(OBinaryProtocol.SIZE_LONG);
        buffer.putLong(iValue);
        buffer.rewind();
        channel.write(buffer, iOffset + HEADER_SIZE);
        releaseByteBuffer(buffer);
      }
    } finally {
      releaseWriteLock();
    }
  }

  @Override
  public void writeShort(long iOffset, final short iValue) throws IOException {
    acquireWriteLock();
    try {

      iOffset = checkRegions(iOffset, OBinaryProtocol.SIZE_SHORT);
      final OMMapBufferEntry[] entries = OMMapManagerLocator.getInstance().acquire(this, iOffset, OBinaryProtocol.SIZE_SHORT,
          OMMapManager.OPERATION_TYPE.WRITE, strategy);
      if (entries != null) {
        // MMAP WRITE
        try {
          if (entries.length == 1)
            entries[0].buffer.putShort((int) (iOffset - entries[0].beginOffset), iValue);
          else {
            entries[0].buffer.position((int) (iOffset - entries[0].beginOffset));
            entries[1].buffer.position(0);

            OByteBufferUtils.splitShortToBuffers(entries[0].buffer, entries[1].buffer, iValue);
          }
        } finally {
          OMMapManagerLocator.getInstance().release(entries, OMMapManager.OPERATION_TYPE.WRITE);
        }
      } else {
        // DIRECT WRITE
        final ByteBuffer buffer = acquireByteBuffer(OBinaryProtocol.SIZE_SHORT);
        buffer.putShort(iValue);
        buffer.rewind();
        channel.write(buffer, iOffset + HEADER_SIZE);
        releaseByteBuffer(buffer);
      }
    } finally {
      releaseWriteLock();
    }
  }

  @Override
  public void writeByte(long iOffset, final byte iValue) throws IOException {
    acquireWriteLock();
    try {

      iOffset = checkRegions(iOffset, OBinaryProtocol.SIZE_BYTE);
      final OMMapBufferEntry[] entries = OMMapManagerLocator.getInstance().acquire(this, iOffset, OBinaryProtocol.SIZE_BYTE,
          OMMapManager.OPERATION_TYPE.WRITE, strategy);
      if (entries != null) {
        // MMAP WRITE
        try {
          entries[0].buffer.put((int) (iOffset - entries[0].beginOffset), iValue);
        } finally {
          OMMapManagerLocator.getInstance().release(entries, OMMapManager.OPERATION_TYPE.WRITE);
        }
      } else {
        // DIRECT WRITE
        final ByteBuffer buffer = acquireByteBuffer(OBinaryProtocol.SIZE_BYTE);
        buffer.put(iValue);
        buffer.rewind();
        channel.write(buffer, iOffset + HEADER_SIZE);
        releaseByteBuffer(buffer);
      }
    } finally {
      releaseWriteLock();
    }
  }

  @Override
  public void write(long iOffset, final byte[] iSourceBuffer) throws IOException {
    acquireWriteLock();
    try {

      if (iSourceBuffer == null || iSourceBuffer.length == 0)
        return;

      iOffset = checkRegions(iOffset, iSourceBuffer.length);

      try {
        final OMMapBufferEntry[] entries = OMMapManagerLocator.getInstance().acquire(this, iOffset, iSourceBuffer.length,
            OMMapManager.OPERATION_TYPE.WRITE, strategy);
        if (entries != null) {
          // MMAP WRITE
          try {

            int position = (int) (iOffset - entries[0].beginOffset);

            int remaining;
            final int iLenght = iSourceBuffer.length;
            int remainingLength = iLenght;
            for (OMMapBufferEntry entry : entries) {
              entry.buffer.position(position);
              remaining = entry.buffer.remaining();
              int toWrite = Math.min(remaining, remainingLength);
              entry.buffer.put(iSourceBuffer, iLenght - remainingLength, toWrite);
              position = 0;
              remainingLength -= toWrite;
            }
          } finally {
            OMMapManagerLocator.getInstance().release(entries, OMMapManager.OPERATION_TYPE.WRITE);
          }
        } else {
          // DIRECT WRITE
          final ByteBuffer buffer = acquireByteBuffer(iSourceBuffer.length);
          buffer.put(iSourceBuffer);
          buffer.rewind();
          channel.write(buffer, iOffset + HEADER_SIZE);
          releaseByteBuffer(buffer);
        }
      } catch (BufferOverflowException e) {
        OLogManager.instance().error(this,
            "Error on write in the range " + iOffset + "-" + (iOffset + iSourceBuffer.length) + "." + toString(), e,
            OIOException.class);
      }
    } finally {
      releaseWriteLock();
    }
  }

  public void write(long iOffset, byte[] iData, int iSize, int iArrayOffset) throws IOException {
    acquireWriteLock();
    try {
      if (iData == null || iData.length == 0 || iSize == 0)
        return;

      assert iOffset > -1;
      assert iArrayOffset > -1;

      iOffset = checkRegions(iOffset, iSize);

      try {
        final OMMapBufferEntry[] entries = OMMapManagerLocator.getInstance().acquire(this, iOffset, iSize,
            OMMapManager.OPERATION_TYPE.WRITE, strategy);
        if (entries != null) {
          // MMAP WRITE
          try {

            int position = (int) (iOffset - entries[0].beginOffset);

            int remaining;
            final int iLength = iSize;
            int remainingLength = iLength;
            for (OMMapBufferEntry entry : entries) {
              entry.buffer.position(position);
              remaining = entry.buffer.remaining();
              int toWrite = Math.min(remaining, remainingLength);
              entry.buffer.put(iData, iArrayOffset + iLength - remainingLength, toWrite);
              position = 0;
              remainingLength -= toWrite;
            }
          } finally {
            OMMapManagerLocator.getInstance().release(entries, OMMapManager.OPERATION_TYPE.WRITE);
          }
        } else {
          // DIRECT WRITE
          final ByteBuffer buffer = acquireByteBuffer(iSize);
          buffer.put(iData, iArrayOffset, iSize);
          buffer.rewind();
          channel.write(buffer, iOffset + HEADER_SIZE);
          releaseByteBuffer(buffer);
        }
      } catch (BufferOverflowException e) {
        OLogManager.instance().error(this, "Error on write in the range " + iOffset + "-" + (iOffset + iSize) + "." + toString(),
            e, OIOException.class);
      }
    } finally {
      releaseWriteLock();
    }
  }

  /**
   * Synchronizes buffered changes to the file.
   */
  @Override
  public boolean synch() {
    acquireWriteLock();
    try {

      boolean allFlushed = OMMapManagerLocator.getInstance().flushFile(this);
      flushHeader();

      return allFlushed;

    } finally {
      releaseWriteLock();
    }
  }

  @Override
  public void writeHeaderLong(final int iPosition, final long iValue) {
    acquireWriteLock();
    try {

      if (headerBuffer != null) {
        headerBuffer.putLong(HEADER_DATA_OFFSET + iPosition, iValue);
        setHeaderDirty();
      }
    } finally {
      releaseWriteLock();
    }
  }

  @Override
  public long readHeaderLong(final int iPosition) {
    acquireReadLock();
    try {

      return headerBuffer.getLong(HEADER_DATA_OFFSET + iPosition);
    } finally {
      releaseReadLock();
    }
  }

  @Override
  public void close() throws IOException {
    acquireWriteLock();
    try {
      OMMapManagerLocator.getInstance().flushFile(this);

      super.close();
      headerBuffer = null;
    } finally {
      releaseWriteLock();
    }
  }

  public void setSoftlyClosed(final boolean value) {
    acquireWriteLock();
    try {
      if (headerBuffer == null)
        return;

      headerBuffer.put(SOFTLY_CLOSED_OFFSET, (byte) (value ? 1 : 0));
      setHeaderDirty();
      flushHeader();
    } finally {
      releaseWriteLock();
    }
  }

  @Override
  public boolean isSoftlyClosed() throws IOException {
    acquireReadLock();
    try {
      if (version == 0)
        return headerBuffer.get(SOFTLY_CLOSED_OFFSET_V_0) > 0;

      return headerBuffer.get(SOFTLY_CLOSED_OFFSET) > 0;
    } finally {
      releaseReadLock();
    }
  }

  MappedByteBuffer map(final long iBeginOffset, final int iSize) throws IOException {
    return channel.map(mode.equals("r") ? FileChannel.MapMode.READ_ONLY : FileChannel.MapMode.READ_WRITE, iBeginOffset
        + HEADER_SIZE, iSize);
  }

  @Override
  protected void openChannel(final long newSize) throws IOException {
    super.openChannel(newSize);
    headerBuffer = channel.map(mode.equals("r") ? FileChannel.MapMode.READ_ONLY : FileChannel.MapMode.READ_WRITE, 0, HEADER_SIZE);
  }

  public boolean isClosed() {
    return headerBuffer == null;
  }

  /**
   * Acquires a byte buffer to use in read/write operations. If the requested size is minor-equals to BYTEBUFFER_POOLABLE_SIZE
   * bytes, then is returned from the bufferPool if any. Buffer bigger than BYTEBUFFER_POOLABLE_SIZE bytes.
   * 
   * @param iSize
   *          The requested size
   * @return A buffer in the pool if any and if size is compatible, otherwise a new one
   */
  protected ByteBuffer acquireByteBuffer(final int iSize) {
    if (iSize > BYTEBUFFER_POOLABLE_SIZE) {
      try {
        metricNonPooledBufferUsed++;
        // CREATE A BUFFER AT THE FLY. IT WILL BE DISCARDED WHEN FINISHED
        return ByteBuffer.allocate(iSize);
      } catch (OutOfMemoryError e) {
        // LOG THE EXCEPTION AND RE-THROW IT
        OLogManager.instance().error(this, "Error on allocating direct buffer of size %d bytes", e, iSize);
        throw e;
      }
    }

    // POP THE FIRST AVAILABLE
    ByteBuffer buffer = bufferPool.poll();
    if (buffer != null)
      metricPooledBufferUsed++;
    else {
      metricPooledBufferCreated++;
      buffer = ByteBuffer.allocateDirect(BYTEBUFFER_POOLABLE_SIZE);
    }

    buffer.limit(iSize);

    return buffer;
  }

  protected void releaseByteBuffer(final ByteBuffer iBuffer) {
    if (iBuffer.limit() > BYTEBUFFER_POOLABLE_SIZE)
      // DISCARD: IT'S TOO BIG TO KEEP IT IN MEMORY
      return;

    iBuffer.rewind();

    // PUSH INTO THE POOL
    bufferPool.add(iBuffer);
  }

  @Override
  protected void init() {
    acquireWriteLock();
    try {
      if (version == 0) {
        size = headerBuffer.getInt(SIZE_OFFSET_V_0);
        filledUpTo = headerBuffer.getInt(FILLEDUPTO_OFFSET_V_0);
      } else {
        size = headerBuffer.getLong(SIZE_OFFSET);
        filledUpTo = headerBuffer.getLong(FILLEDUPTO_OFFSET);
      }
    } finally {
      releaseWriteLock();
    }
  }

  @Override
  protected void setFilledUpTo(final long iHow) {
    setFilledUpTo(iHow, false);
  }

  protected void setFilledUpTo(final long iHow, boolean force) {
    acquireWriteLock();
    try {
      if (force || iHow != filledUpTo) {
        filledUpTo = iHow;
        headerBuffer.putLong(FILLEDUPTO_OFFSET, filledUpTo);
        setHeaderDirty();
      }
    } finally {
      releaseWriteLock();
    }
  }

  @Override
  protected void setVersion(int version) throws IOException {
    acquireWriteLock();
    try {
      headerBuffer.put(VERSION_OFFSET, (byte) version);
      setHeaderDirty();
    } finally {
      releaseWriteLock();
    }
  }

  @Override
  public void setSize(final long iSize) throws IOException {
    setSize(iSize, false);
  }

  protected void setSize(final long size, final boolean force) throws IOException {
    acquireWriteLock();
    try {
      if (maxSize > 0 && size > maxSize)
        throw new IllegalArgumentException("Cannot extend the file to " + OFileUtils.getSizeAsString(size) + " because the max is "
            + OFileUtils.getSizeAsString(maxSize));
      if (force || this.size != size) {
        checkSize(size);
        this.size = size;
        headerBuffer.putLong(SIZE_OFFSET, size);
        setHeaderDirty();
      }
    } finally {
      releaseWriteLock();
    }
  }

  protected void flushHeader() {
    if (headerDirty) {
      headerBuffer.force();
      headerDirty = false;
    }
  }
}
