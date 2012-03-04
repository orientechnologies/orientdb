/*
 * Copyright 1999-2010 Luca Garulli (l.garulli--at--orientechnologies.com)
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
import java.util.ArrayList;
import java.util.List;

import com.orientechnologies.common.io.OIOException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.serialization.OBinaryProtocol;
import com.orientechnologies.orient.core.storage.fs.OMMapManager.OPERATION_TYPE;

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
	public final static String									NAME							= "mmap";
	protected MappedByteBuffer									headerBuffer;
	protected int																bufferBeginOffset	= -1;
	protected static List<ByteBuffer>						bufferPool				= new ArrayList<ByteBuffer>(10);

	private static int													BYTEBUFFER_POOLABLE_SIZE;
	private static OMMapManager.ALLOC_STRATEGY	strategy;

	@Override
	public OFileMMap init(String iFileName, String iMode) {
		super.init(iFileName, iMode);
		BYTEBUFFER_POOLABLE_SIZE = OGlobalConfiguration.FILE_MMAP_BUFFER_SIZE.getValueAsInteger();
		strategy = OMMapManager.ALLOC_STRATEGY.values()[OGlobalConfiguration.FILE_MMAP_STRATEGY.getValueAsInteger()];
		return this;
	}

	@Override
	public void read(long iOffset, final byte[] iDestBuffer, final int iLenght) throws IOException {
		iOffset = checkRegions(iOffset, iLenght);

		final OMMapBufferEntry entry = OMMapManager.acquire(this, iOffset, iLenght, OPERATION_TYPE.READ, strategy);
		if (entry != null) {
			// MMAP READ
			try {
				entry.buffer.position((int) (iOffset - entry.beginOffset));
				entry.buffer.get(iDestBuffer, 0, iLenght);
			} finally {
				entry.release();
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
	public int readInt(long iOffset) throws IOException {
		iOffset = checkRegions(iOffset, OBinaryProtocol.SIZE_INT);
		final OMMapBufferEntry entry = OMMapManager.acquire(this, iOffset, OBinaryProtocol.SIZE_INT, OPERATION_TYPE.READ, strategy);
		if (entry != null) {
			// MMAP READ
			try {
				return entry.buffer.getInt((int) (iOffset - entry.beginOffset));
			} finally {
				entry.release();
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
	}

	@Override
	public long readLong(long iOffset) throws IOException {
		iOffset = checkRegions(iOffset, OBinaryProtocol.SIZE_LONG);
		final OMMapBufferEntry entry = OMMapManager.acquire(this, iOffset, OBinaryProtocol.SIZE_LONG, OPERATION_TYPE.READ, strategy);
		if (entry != null) {
			// MMAP READ
			try {
				return entry.buffer.getLong((int) (iOffset - entry.beginOffset));
			} finally {
				entry.release();
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
	}

	@Override
	public short readShort(long iOffset) throws IOException {
		iOffset = checkRegions(iOffset, OBinaryProtocol.SIZE_SHORT);
		final OMMapBufferEntry entry = OMMapManager.acquire(this, iOffset, OBinaryProtocol.SIZE_SHORT, OPERATION_TYPE.READ, strategy);
		if (entry != null) {
			// MMAP READ
			try {
				return entry.buffer.getShort((int) (iOffset - entry.beginOffset));
			} finally {
				entry.release();
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
	}

	@Override
	public byte readByte(long iOffset) throws IOException {
		iOffset = checkRegions(iOffset, OBinaryProtocol.SIZE_BYTE);
		final OMMapBufferEntry entry = OMMapManager.acquire(this, iOffset, OBinaryProtocol.SIZE_BYTE, OPERATION_TYPE.READ, strategy);
		if (entry != null) {
			// MMAP READ
			try {
				return entry.buffer.get((int) (iOffset - entry.beginOffset));
			} finally {
				entry.release();
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
	}

	@Override
	public void writeInt(long iOffset, final int iValue) throws IOException {
		iOffset = checkRegions(iOffset, OBinaryProtocol.SIZE_INT);
		final OMMapBufferEntry entry = OMMapManager.acquire(this, iOffset, OBinaryProtocol.SIZE_INT, OPERATION_TYPE.WRITE, strategy);
		if (entry != null) {
			// MMAP WRITE
			try {
				entry.buffer.putInt((int) (iOffset - entry.beginOffset), iValue);
			} finally {
				entry.release();
			}
		} else {
			// DIRECT WRITE
			final ByteBuffer buffer = acquireByteBuffer(OBinaryProtocol.SIZE_INT);
			buffer.putInt(iValue);
			buffer.rewind();
			channel.write(buffer, iOffset + HEADER_SIZE);
			releaseByteBuffer(buffer);
		}
	}

	@Override
	public void writeLong(long iOffset, final long iValue) throws IOException {
		iOffset = checkRegions(iOffset, OBinaryProtocol.SIZE_LONG);
		final OMMapBufferEntry entry = OMMapManager.acquire(this, iOffset, OBinaryProtocol.SIZE_LONG, OPERATION_TYPE.WRITE, strategy);
		if (entry != null) {
			// MMAP WRITE
			try {
				entry.buffer.putLong((int) (iOffset - entry.beginOffset), iValue);
			} finally {
				entry.release();
			}
		} else {
			// DIRECT WRITE
			final ByteBuffer buffer = acquireByteBuffer(OBinaryProtocol.SIZE_LONG);
			buffer.putLong(iValue);
			buffer.rewind();
			channel.write(buffer, iOffset + HEADER_SIZE);
			releaseByteBuffer(buffer);
		}
	}

	@Override
	public void writeShort(long iOffset, final short iValue) throws IOException {
		iOffset = checkRegions(iOffset, OBinaryProtocol.SIZE_SHORT);
		final OMMapBufferEntry entry = OMMapManager.acquire(this, iOffset, OBinaryProtocol.SIZE_SHORT, OPERATION_TYPE.WRITE, strategy);
		if (entry != null) {
			// MMAP WRITE
			try {
				entry.buffer.putShort((int) (iOffset - entry.beginOffset), iValue);
			} finally {
				entry.release();
			}
		} else {
			// DIRECT WRITE
			final ByteBuffer buffer = acquireByteBuffer(OBinaryProtocol.SIZE_SHORT);
			buffer.putShort(iValue);
			buffer.rewind();
			channel.write(buffer, iOffset + HEADER_SIZE);
			releaseByteBuffer(buffer);
		}
	}

	@Override
	public void writeByte(long iOffset, final byte iValue) throws IOException {
		iOffset = checkRegions(iOffset, OBinaryProtocol.SIZE_BYTE);
		final OMMapBufferEntry entry = OMMapManager.acquire(this, iOffset, OBinaryProtocol.SIZE_BYTE, OPERATION_TYPE.WRITE, strategy);
		if (entry != null) {
			// MMAP WRITE
			try {
				entry.buffer.put((int) (iOffset - entry.beginOffset), iValue);
			} finally {
				entry.release();
			}
		} else {
			// DIRECT WRITE
			final ByteBuffer buffer = acquireByteBuffer(OBinaryProtocol.SIZE_BYTE);
			buffer.put(iValue);
			buffer.rewind();
			channel.write(buffer, iOffset + HEADER_SIZE);
			releaseByteBuffer(buffer);
		}
	}

	@Override
	public void write(long iOffset, final byte[] iSourceBuffer) throws IOException {
		if (iSourceBuffer == null || iSourceBuffer.length == 0)
			return;

		iOffset = checkRegions(iOffset, iSourceBuffer.length);

		try {
			final OMMapBufferEntry entry = OMMapManager.acquire(this, iOffset, iSourceBuffer.length, OPERATION_TYPE.WRITE, strategy);
			if (entry != null) {
				// MMAP WRITE
				try {
					entry.buffer.position((int) (iOffset - entry.beginOffset));
					entry.buffer.put(iSourceBuffer);
				} finally {
					entry.release();
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
	}

	/**
	 * Synchronizes buffered changes to the file.
	 * 
	 * @see OFileMMapSecure
	 */
	@Override
	public void synch() {
		flushHeader();
		OMMapManager.flushFile(this);
	}

	@Override
	public void writeHeaderLong(final int iPosition, final long iValue) {
		if (headerBuffer != null) {
			headerBuffer.putLong(HEADER_DATA_OFFSET + iPosition, iValue);
			setHeaderDirty();
		}
	}

	@Override
	public long readHeaderLong(final int iPosition) {
		return headerBuffer.getLong(HEADER_DATA_OFFSET + iPosition);
	}

	@Override
	public void close() throws IOException {
		if (headerBuffer != null) {
			setSoftlyClosed(true);
			headerBuffer = null;
		}

		super.close();

		OMMapManager.flush();
	}

	@Override
	public boolean isSoftlyClosed() {
		return headerBuffer.get(SOFTLY_CLOSED_OFFSET) == 1;
	}

	@Override
	protected void setSoftlyClosed(final boolean iValue) {
		if (headerBuffer == null)
			return;

		headerBuffer.put(SOFTLY_CLOSED_OFFSET, (byte) (iValue ? 1 : 0));
		setHeaderDirty();
		flushHeader();
	}

	MappedByteBuffer map(final long iBeginOffset, final int iSize) throws IOException {
		return channel.map(mode.equals("r") ? FileChannel.MapMode.READ_ONLY : FileChannel.MapMode.READ_WRITE, iBeginOffset
				+ HEADER_SIZE, iSize);
	}

	@Override
	protected void openChannel(final int iNewSize) throws IOException {
		super.openChannel(iNewSize);
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
				OProfiler.getInstance().updateStat("MMap.extraBufferSize", iSize);
				// CREATE A BUFFER AT THE FLY. IT WILL BE DISCARDED WHEN FINISHED
				return ByteBuffer.allocate(iSize);
			} catch (OutOfMemoryError e) {
				// LOG THE EXCEPTION AND RE-THROW IT
				OLogManager.instance().error(this, "Error on allocating direct buffer of size %d bytes", e, iSize);
				throw e;
			}
		}

		final ByteBuffer buffer;

		synchronized (bufferPool) {
			if (bufferPool.isEmpty()) {
				buffer = ByteBuffer.allocateDirect(BYTEBUFFER_POOLABLE_SIZE);
				OProfiler.getInstance().updateStat("MMap.pooledBufferSize", BYTEBUFFER_POOLABLE_SIZE);
			} else {
				// POP THE FIRST AVAILABLE
				buffer = bufferPool.remove(0);
				OProfiler.getInstance().updateCounter("MMap.pooledBuffers", -1);
			}
		}

		buffer.limit(iSize);

		return buffer;
	}

	protected synchronized void releaseByteBuffer(final ByteBuffer iBuffer) {
		if (iBuffer.limit() > BYTEBUFFER_POOLABLE_SIZE)
			// DISCARD: IT'S TOO BIG TO KEEP IT IN MEMORY
			return;

		iBuffer.rewind();

		// PUSH INTO THE POOL
		synchronized (bufferPool) {
			bufferPool.add(iBuffer);
			OProfiler.getInstance().updateCounter("MMap.pooledBuffers", +1);
		}
	}

	@Override
	protected void init() {
		size = headerBuffer.getInt(SIZE_OFFSET);
		filledUpTo = headerBuffer.getInt(FILLEDUPTO_OFFSET);
	}

	@Override
	protected void setFilledUpTo(final int iHow) {
		if (iHow != filledUpTo) {
			filledUpTo = iHow;
			headerBuffer.putInt(FILLEDUPTO_OFFSET, filledUpTo);
			setHeaderDirty();
		}
	}

	@Override
	public void setSize(final int iSize) throws IOException {
		if (iSize != size) {
			checkSize(iSize);
			size = iSize;
			headerBuffer.putInt(SIZE_OFFSET, size);
			setHeaderDirty();
		}
	}

	protected void flushHeader() {
		if (headerDirty) {
			headerBuffer.force();
			headerDirty = false;
		}
	}
}
