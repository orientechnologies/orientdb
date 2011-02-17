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
package com.orientechnologies.orient.core.serialization;

import java.io.IOException;

import com.orientechnologies.common.util.OArrays;
import com.orientechnologies.orient.core.OConstants;

/**
 * Class to parse and write buffers in very fast way.
 * 
 * @author Luca Garulli
 * 
 */
public class OMemoryInputStream {
	private byte[]	buffer;
	private int			position	= 0;

	public OMemoryInputStream() {
	}

	public OMemoryInputStream(final byte[] iBuffer) {
		setSource(iBuffer);
	}

	public byte[] getAsByteArrayFixed(final int iSize) throws IOException {
		if (position >= buffer.length)
			return null;

		final byte[] portion = OArrays.copyOfRange(buffer, position, position + iSize);
		position += iSize;

		return portion;
	}

	/**
	 * Browse the stream but just return the begin of the byte array. This is used to lazy load the information only when needed.
	 * 
	 */
	public int getAsByteArrayOffset() {
		if (position >= buffer.length)
			return -1;

		final int begin = position;

		final int size = OBinaryProtocol.bytes2int(buffer, position);
		position += OConstants.SIZE_INT + size;

		return begin;
	}

	public byte[] getAsByteArray(int iOffset) throws IOException {
		if (buffer == null || iOffset >= buffer.length)
			return null;

		final int size = OBinaryProtocol.bytes2int(buffer, iOffset);

		if (size == 0)
			return null;

		iOffset += OConstants.SIZE_INT;

		return OArrays.copyOfRange(buffer, iOffset, iOffset + size);
	}

	public byte[] getAsByteArray() throws IOException {
		if (position >= buffer.length)
			return null;

		final int size = OBinaryProtocol.bytes2int(buffer, position);
		position += OConstants.SIZE_INT;

		final byte[] portion = OArrays.copyOfRange(buffer, position, position + size);
		position += size;

		return portion;
	}

	public String getAsString() throws IOException {
		return OBinaryProtocol.bytes2string(getAsByteArray());
	}

	public boolean getAsBoolean() throws IOException {
		return buffer[position++] == 1;
	}

	public char getAsChar() throws IOException {
		final char value = OBinaryProtocol.bytes2char(buffer, position);
		position += OConstants.SIZE_CHAR;
		return value;
	}

	public byte getAsByte() throws IOException {
		final byte value = buffer[position];
		position += OConstants.SIZE_BYTE;
		return value;
	}

	public int getAsInteger() throws IOException {
		final int value = OBinaryProtocol.bytes2int(buffer, position);
		position += OConstants.SIZE_INT;
		return value;
	}

	public short getAsShort() throws IOException {
		final short value = OBinaryProtocol.bytes2short(buffer, position);
		position += OConstants.SIZE_SHORT;
		return value;
	}

	public void close() {
		buffer = null;
	}

	public byte peek() {
		return buffer[position];
	}

	public void setSource(final byte[] iBuffer) {
		buffer = iBuffer;
		position = 0;
	}

	public byte[] move(final int iOffset, final int iCopyToOffset) {
		final byte[] copy = new byte[buffer.length - iOffset + iCopyToOffset];
		System.arraycopy(buffer, iOffset, copy, iCopyToOffset, copy.length);
		return copy;
	}

	public byte[] copy() {
		if (buffer == null)
			return null;

		final int size = position > 0 ? position : buffer.length;

		final byte[] copy = new byte[size];
		System.arraycopy(buffer, 0, copy, 0, size);
		return copy;
	}
}
