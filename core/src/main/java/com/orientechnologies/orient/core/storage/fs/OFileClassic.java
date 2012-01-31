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
import java.nio.ByteBuffer;

import com.orientechnologies.common.io.OIOException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.serialization.OBinaryProtocol;

/**
 * Need to be synchronized by the external. Multiple Reader, Single Writer.<br/>
 * Header structure:<br/>
 * <br/>
 * +-----------+--------------+---------------+---------------+<br/>
 * | FILE SIZE | FILLED UP TO | SOFTLY CLOSED | SECURITY CODE |<br/>
 * | 8 bytes . | 8 bytes .... | 1 byte ...... | 32 bytes .... |<br/>
 * +-----------+--------------+---------------+---------------+<br/>
 * = 1024 bytes<br/>
 * <br/>
 */
public class OFileClassic extends OAbstractFile {
	public final static String	NAME								= "classic";
	protected ByteBuffer				internalWriteBuffer	= ByteBuffer.allocate(OBinaryProtocol.SIZE_LONG);

	public OFileClassic init(String iFileName, String iMode) {
		super.init(iFileName, iMode);
		return this;
	}

	@Override
	public void close() throws IOException {
		if (channel != null)
			setSoftlyClosed(true);

		if (internalWriteBuffer != null)
			internalWriteBuffer = null;

		super.close();
	}

	@Override
	public void read(long iOffset, byte[] iDestBuffer, int iLenght) throws IOException {
		iOffset = checkRegions(iOffset, iLenght);

		ByteBuffer buffer = ByteBuffer.wrap(iDestBuffer);
		channel.read(buffer, iOffset);
	}

	@Override
	public int readInt(long iOffset) throws IOException {
		iOffset = checkRegions(iOffset, OBinaryProtocol.SIZE_INT);
		return readData(iOffset, OBinaryProtocol.SIZE_INT).getInt();
	}

	@Override
	public long readLong(long iOffset) throws IOException {
		iOffset = checkRegions(iOffset, OBinaryProtocol.SIZE_LONG);
		return readData(iOffset, OBinaryProtocol.SIZE_LONG).getLong();
	}

	@Override
	public short readShort(long iOffset) throws IOException {
		iOffset = checkRegions(iOffset, OBinaryProtocol.SIZE_SHORT);
		return readData(iOffset, OBinaryProtocol.SIZE_SHORT).getShort();
	}

	@Override
	public byte readByte(long iOffset) throws IOException {
		iOffset = checkRegions(iOffset, OBinaryProtocol.SIZE_BYTE);
		return readData(iOffset, OBinaryProtocol.SIZE_BYTE).get();
	}

	@Override
	public void writeInt(long iOffset, final int iValue) throws IOException {
		setDirty();
		iOffset = checkRegions(iOffset, OBinaryProtocol.SIZE_INT);
		final ByteBuffer buffer = getWriteBuffer(OBinaryProtocol.SIZE_INT);
		buffer.putInt(iValue);
		writeBuffer(buffer, iOffset);
		setDirty();
	}

	@Override
	public void writeLong(long iOffset, final long iValue) throws IOException {
		iOffset = checkRegions(iOffset, OBinaryProtocol.SIZE_LONG);
		final ByteBuffer buffer = getWriteBuffer(OBinaryProtocol.SIZE_LONG);
		buffer.putLong(iValue);
		writeBuffer(buffer, iOffset);
		setDirty();
	}

	@Override
	public void writeShort(long iOffset, final short iValue) throws IOException {
		iOffset = checkRegions(iOffset, OBinaryProtocol.SIZE_INT);
		final ByteBuffer buffer = getWriteBuffer(OBinaryProtocol.SIZE_SHORT);
		buffer.putShort(iValue);
		writeBuffer(buffer, iOffset);
		setDirty();
	}

	@Override
	public void writeByte(long iOffset, final byte iValue) throws IOException {
		iOffset = checkRegions(iOffset, OBinaryProtocol.SIZE_BYTE);
		final ByteBuffer buffer = getWriteBuffer(OBinaryProtocol.SIZE_BYTE);
		buffer.put(iValue);
		writeBuffer(buffer, iOffset);
		setDirty();
	}

	@Override
	public void write(long iOffset, final byte[] iSourceBuffer) throws IOException {
		if (iSourceBuffer != null) {
			iOffset = checkRegions(iOffset, iSourceBuffer.length);
			channel.write(ByteBuffer.wrap(iSourceBuffer), iOffset);
			setDirty();
		}
	}

	/**
	 * Synchronizes the buffered changes to disk.
	 * 
	 * @throws IOException
	 * 
	 * @see OFileMMapSecure
	 */
	@Override
	public void synch() throws IOException {
		flushHeader();
	}

	protected void flushHeader() throws IOException {
		if (headerDirty || dirty) {
			headerDirty = dirty = false;
			channel.force(false);
		}
	}

	@Override
	protected void init() throws IOException {
		size = readData(SIZE_OFFSET, OBinaryProtocol.SIZE_INT).getInt();
		filledUpTo = readData(FILLEDUPTO_OFFSET, OBinaryProtocol.SIZE_INT).getInt();
	}

	@Override
	protected void setFilledUpTo(final int iValue) throws IOException {
		if (iValue != filledUpTo) {
			filledUpTo = iValue;
			final ByteBuffer buffer = getWriteBuffer(OBinaryProtocol.SIZE_INT);
			buffer.putInt(filledUpTo);
			writeBuffer(buffer, FILLEDUPTO_OFFSET);
			setHeaderDirty();
		}
	}

	@Override
	public void setSize(final int iSize) throws IOException {
		if (iSize != size) {
			super.checkSize(iSize);

			size = iSize;
			final ByteBuffer buffer = getWriteBuffer(OBinaryProtocol.SIZE_INT);
			buffer.putInt(size);
			writeBuffer(buffer, SIZE_OFFSET);
			setHeaderDirty();

			try {
				synch();
				channel.close();
				openChannel(iSize);

			} catch (IOException e) {
				OLogManager.instance().error(this, "Error on changing the file size to " + iSize + " bytes", e, OIOException.class);
			}

		}
	}

	@Override
	public void writeHeaderLong(final int iPosition, final long iValue) throws IOException {
		final ByteBuffer buffer = getWriteBuffer(OBinaryProtocol.SIZE_LONG);
		buffer.putLong(iValue);
		writeBuffer(buffer, HEADER_DATA_OFFSET + iPosition);
		setHeaderDirty();
	}

	@Override
	public long readHeaderLong(final int iPosition) throws IOException {
		return readData(HEADER_DATA_OFFSET + iPosition, OBinaryProtocol.SIZE_LONG).getLong();
	}

	@Override
	public boolean isSoftlyClosed() throws IOException {
		return readData(SOFTLY_CLOSED_OFFSET, OBinaryProtocol.SIZE_BYTE).get() == 1;
	}

	@Override
	protected void setSoftlyClosed(final boolean iValue) throws IOException {
		final ByteBuffer buffer = getWriteBuffer(OBinaryProtocol.SIZE_BYTE);
		buffer.put((byte) (iValue ? 1 : 0));
		writeBuffer(buffer, SOFTLY_CLOSED_OFFSET);
		setHeaderDirty();
		synch();
	}

	/**
	 * ALWAYS ADD THE HEADER SIZE BECAUSE ON THIS TYPE IS ALWAYS NEEDED
	 */
	@Override
	protected long checkRegions(final long iOffset, final int iLength) {
		return super.checkRegions(iOffset, iLength) + HEADER_SIZE;
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
}
