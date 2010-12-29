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
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import com.orientechnologies.common.io.OIOException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.OConstants;

/**
 * 
 * Header structure:<br/>
 * <br/>
 * +-----------+--------------+---------------+---------------+<br/>
 * | FILE SIZE | FILLED UP TO | SOFTLY CLOSED | SECURITY CODE |<br/>
 * | 8 bytes . | 8 bytes .... | 1 byte ...... | 32 bytes .... |<br/>
 * +-----------+--------------+---------------+---------------+<br/>
 * = 1024 bytes<br/>
 * <br/>
 */
public class OFileMMap extends OFile {
	protected MappedByteBuffer	headerBuffer;
	protected int								bufferBeginOffset	= -1;
	protected int								bufferSize				= 0;

	public OFileMMap(String iFileName, String iMode) throws IOException {
		super(iFileName, iMode);
	}

	@Override
	public void read(int iOffset, final byte[] iDestBuffer, final int iLenght) {
		iOffset = checkRegions(iOffset, iLenght);

		final OMMapBufferEntry entry = OMMapManager.request(this, iOffset, iLenght);
		entry.buffer.position(iOffset - entry.beginOffset);
		entry.buffer.get(iDestBuffer, 0, iLenght);
	}

	@Override
	public int readInt(int iOffset) {
		iOffset = checkRegions(iOffset, OConstants.SIZE_INT);
		final OMMapBufferEntry entry = OMMapManager.request(this, iOffset, OConstants.SIZE_INT);
		return entry.buffer.getInt(iOffset - entry.beginOffset);
	}

	@Override
	public long readLong(int iOffset) {
		iOffset = checkRegions(iOffset, OConstants.SIZE_LONG);
		final OMMapBufferEntry entry = OMMapManager.request(this, iOffset, OConstants.SIZE_LONG);
		return entry.buffer.getLong(iOffset - entry.beginOffset);
	}

	@Override
	public short readShort(int iOffset) {
		iOffset = checkRegions(iOffset, OConstants.SIZE_SHORT);
		final OMMapBufferEntry entry = OMMapManager.request(this, iOffset, OConstants.SIZE_SHORT);
		return entry.buffer.getShort(iOffset - entry.beginOffset);
	}

	@Override
	public byte readByte(int iOffset) {
		iOffset = checkRegions(iOffset, OConstants.SIZE_BYTE);
		final OMMapBufferEntry entry = OMMapManager.request(this, iOffset, OConstants.SIZE_BYTE);
		return entry.buffer.get(iOffset - entry.beginOffset);
	}

	@Override
	public void writeInt(int iOffset, final int iValue) {
		iOffset = checkRegions(iOffset, OConstants.SIZE_INT);
		final OMMapBufferEntry entry = OMMapManager.request(this, iOffset, OConstants.SIZE_INT);
		entry.buffer.putInt(iOffset - entry.beginOffset, iValue);
	}

	@Override
	public void writeLong(int iOffset, final long iValue) {
		iOffset = checkRegions(iOffset, OConstants.SIZE_LONG);
		final OMMapBufferEntry entry = OMMapManager.request(this, iOffset, OConstants.SIZE_LONG);
		entry.buffer.putLong(iOffset - entry.beginOffset, iValue);
	}

	@Override
	public void writeShort(int iOffset, final short iValue) {
		iOffset = checkRegions(iOffset, OConstants.SIZE_SHORT);
		final OMMapBufferEntry entry = OMMapManager.request(this, iOffset, OConstants.SIZE_SHORT);
		entry.buffer.putShort(iOffset - entry.beginOffset, iValue);
	}

	@Override
	public void writeByte(int iOffset, final byte iValue) {
		iOffset = checkRegions(iOffset, OConstants.SIZE_BYTE);
		final OMMapBufferEntry entry = OMMapManager.request(this, iOffset, OConstants.SIZE_BYTE);
		entry.buffer.put(iOffset - entry.beginOffset, iValue);
	}

	@Override
	public void write(int iOffset, final byte[] iSourceBuffer) {
		if (iSourceBuffer.length == 0)
			return;

		iOffset = checkRegions(iOffset, iSourceBuffer.length);

		try {
			final OMMapBufferEntry entry = OMMapManager.request(this, iOffset, iSourceBuffer.length);
			entry.buffer.position(iOffset - entry.beginOffset);
			entry.buffer.put(iSourceBuffer);
		} catch (BufferOverflowException e) {
			OLogManager.instance()
					.error(this, "Error on write in the range " + iOffset + "-" + iOffset + iSourceBuffer.length + "." + toString(), e,
							OIOException.class);
		}
	}

	@Override
	public void changeSize(final int iSize) {
		super.changeSize(iSize);
		size = iSize;
	}

	/**
	 * Synchronize buffered changes to the file.
	 * 
	 * @see OFileMMapSecure
	 */
	@Override
	public void synch() {
		headerBuffer.force();
	}

	@Override
	protected void readHeader() {
		headerBuffer.rewind();
		size = headerBuffer.getInt();
		filledUpTo = headerBuffer.getInt();
		// for (int i = 0; i < securityCode.length; ++i)
		// securityCode[i] = buffer.get();
		//
		// StringBuilder check = new StringBuilder();
		// check.append('X');
		// check.append('3');
		// check.append('O');
		// check.append('!');
		// check.append(fileSize);
		// check.append(filledUpTo);
		// check.append('-');
		// check.append('p');
		// check.append('R');
		// check.append('<');
		//
		// OIntegrityFileManager.instance().check(check.toString(),
		// securityCode);
	}

	@Override
	protected void writeHeader() {
		headerBuffer.rewind();
		headerBuffer.putInt(size);
		headerBuffer.putInt(filledUpTo);
		//
		// StringBuilder check = new StringBuilder();
		// check.append('X');
		// check.append('3');
		// check.append('O');
		// check.append('!');
		// check.append(fileSize);
		// check.append(filledUpTo);
		// check.append('-');
		// check.append('p');
		// check.append('R');
		// check.append('<');
		//
		// securityCode =
		// OIntegrityFileManager.instance().digest(check.toString());
		// for (int i = 0; i < securityCode.length; ++i)
		// buffer.put(securityCode[i]);
	}

	@Override
	public void writeHeaderLong(final int iPosition, final long iValue) {
		if (headerBuffer != null)
			headerBuffer.putLong(HEADER_DATA_OFFSET + iPosition, iValue);
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
		synch();
	}

	MappedByteBuffer map(final int iBeginOffset, final int iSize) throws IOException {
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
}
