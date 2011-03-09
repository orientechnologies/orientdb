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
import java.io.InputStream;

import com.orientechnologies.orient.core.OConstants;

/**
 * Class to parse and write buffers in very fast way.
 * 
 * @author Luca Garulli
 * 
 */
public class OMemoryInputStreamCursor extends InputStream {
	private final OMemoryInputStream	source;
	private int												offset;
	private int												position;
	private final int									size;

	public OMemoryInputStreamCursor(final OMemoryInputStream iSource, final int iOffset, final int iSize) {
		source = iSource;
		offset = iOffset;
		size = iSize;
		position = offset;
	}

	@Override
	public int read() throws IOException {
		position++;
		return source.read();
	}

	@Override
	public int available() throws IOException {
		return size - (position - offset);
	}

	public String getAsString() throws IOException {
		final int beginOffset = source.getPosition();
		String s = source.getAsString();
		position += source.getPosition() - beginOffset;
		return s;
	}

	public byte[] getAsByteArrayFixed(final int iSize) throws IOException {
		position += iSize;
		return source.getAsByteArrayFixed(iSize);
	}

	public long getAsLong() throws IOException {
		position += OConstants.SIZE_LONG;
		return source.getAsLong();
	}

	public int getPosition() {
		return position;
	}

	public int getSize() {
		return size;
	}

}
