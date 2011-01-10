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
import java.io.OutputStream;

import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.orient.core.OConstants;

/**
 * Class to parse and write buffers in very fast way.
 * 
 * @author Luca Garulli
 * 
 */
public class OMemoryOutputStream extends OutputStream {
	private byte[]						buffer;
	private int								position;

	private static final int	NATIVE_COPY_THRESHOLD	= 9;
	private static final int	DEF_SIZE							= 1024;

	// private int fixedSize = 0;

	public OMemoryOutputStream() {
		this(DEF_SIZE);
	}

	/**
	 * Callee takes ownership of 'buf'.
	 */
	public OMemoryOutputStream(final int initialCapacity) {
		buffer = new byte[initialCapacity];
	}

	public OMemoryOutputStream(byte[] stream) {
		buffer = stream;
	}

	public final void writeTo(final OutputStream out) throws IOException {
		out.write(buffer, 0, position);
	}

	public final byte[] getByteArray() {
		position = 0;
		return buffer;
	}

	/**
	 * 
	 * @return [result.length = size()]
	 */
	public final byte[] toByteArray() {
		final int pos = position;

		final byte[] result = new byte[pos];
		final byte[] mbuf = buffer;

		if (pos < NATIVE_COPY_THRESHOLD)
			for (int i = 0; i < pos; ++i)
				result[i] = mbuf[i];
		else
			System.arraycopy(mbuf, 0, result, 0, pos);

		return result;
	}

	public final int size() {
		return position;
	}

	public final int capacity() {
		return buffer.length;
	}

	/**
	 * Does not reduce the current capacity.
	 */
	public final void reset() {
		position = 0;
	}

	// OutputStream:

	@Override
	public final void write(final int b) {
		final int pos = position;
		byte[] mbuf = buffer;

		assureSpaceFor(1);

		mbuf[pos] = (byte) b;
		position = pos + 1;
	}

	@Override
	public final void write(final byte[] iBuffer, final int iOffset, final int iLength) {

		final int pos = position;
		final int capacity = pos + iLength;

		assureSpaceFor(iLength);

		byte[] mbuf = buffer;

		if (iLength < NATIVE_COPY_THRESHOLD)
			for (int i = 0; i < iLength; ++i)
				mbuf[pos + i] = iBuffer[iOffset + i];
		else
			System.arraycopy(iBuffer, iOffset, mbuf, pos, iLength);

		position = capacity;
	}

	/**
	 * Equivalent to {@link #reset()}.
	 */
	@Override
	public final void close() {
		reset();
	}

	public final void addAsFixed(final byte[] iContent) throws IOException {
		if (iContent == null)
			return;
		assureSpaceFor(iContent.length);
		write(iContent, 0, iContent.length);
	}

	public final void add(final byte[] iContent) throws IOException {
		if (iContent == null)
			return;

		assureSpaceFor(OConstants.SIZE_INT + iContent.length);

		OBinaryProtocol.int2bytes(iContent.length, buffer, position);
		position += OConstants.SIZE_INT;
		write(iContent, 0, iContent.length);
	}

	public void add(final byte iContent) throws IOException {
		write(iContent);
	}

	// TODO: AVOID TEMP BUFFER BUT USE DIRECTLY THE CURRENT ONE
	public final void add(final String iContent) throws IOException {
		add(OBinaryProtocol.string2bytes(iContent));
	}

	public void add(final boolean iContent) throws IOException {
		add((byte) (iContent ? 1 : 0));
	}

	public void add(final char iContent) throws IOException {
		assureSpaceFor(OConstants.SIZE_CHAR);
		OBinaryProtocol.char2bytes(iContent, buffer, position);
		position += OConstants.SIZE_CHAR;
	}

	public void add(final int iContent) throws IOException {
		assureSpaceFor(OConstants.SIZE_INT);
		OBinaryProtocol.int2bytes(iContent, buffer, position);
		position += OConstants.SIZE_INT;
	}

	public void add(final short iContent) throws IOException {
		assureSpaceFor(OConstants.SIZE_SHORT);
		OBinaryProtocol.short2bytes(iContent, buffer, position);
		position += OConstants.SIZE_SHORT;
	}

	private void assureSpaceFor(final int iLength) {
		final byte[] mbuf = buffer;
		final int pos = position;
		final int capacity = position + iLength;

		final int mbuflen = mbuf.length;

		if (mbuflen < capacity) {
			OProfiler.getInstance().updateCounter("OMemOutStream.resize", +1);

			final byte[] newbuf = new byte[Math.max(mbuflen << 1, capacity)];

			if (pos < NATIVE_COPY_THRESHOLD)
				for (int i = 0; i < pos; ++i)
					newbuf[i] = mbuf[i];
			else
				System.arraycopy(mbuf, 0, newbuf, 0, pos);

			buffer = newbuf;
		}
	}
}
