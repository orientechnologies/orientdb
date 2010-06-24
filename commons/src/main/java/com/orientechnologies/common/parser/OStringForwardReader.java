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
package com.orientechnologies.common.parser;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;

import com.orientechnologies.common.io.OIOException;

/**
 * Keep in memory only one chunk per time.
 * 
 * @author Luca Garulli
 * 
 */
public class OStringForwardReader implements CharSequence {
	private final BufferedReader	input;
	private char[]								buffer				= new char[DEFAULT_SIZE];
	private long									start					= -1;
	private long									end						= -1;
	private long									current				= 0;
	private long									size					= 0;

	private static final int			DEFAULT_SIZE	= 1000;

	public OStringForwardReader(final InputStream iInput) {
		this.input = new BufferedReader(new InputStreamReader(iInput));
	}

	public OStringForwardReader(final Reader iReader) {
		this.input = new BufferedReader(iReader);
	}

	public OStringForwardReader(final File file) throws FileNotFoundException {
		this(new FileInputStream(file));
		size = file.length();
	}

	public char charAt(final int iIndex) {
		if (iIndex < start)
			throw new IllegalStateException("Can't read backward");

		if (iIndex >= end)
			read(iIndex);

		if (iIndex > current)
			current = iIndex;
		
		return buffer[(int) (iIndex - start)];
	}

	private void read(final int iIndex) {
		try {
			// JUMP CHARACTERS
			for (long i = end; i < iIndex - 1; ++i)
				input.read();

			start = iIndex;
			final int byteRead = input.read(buffer);
			end = start + byteRead;
			current = start;
		} catch (IOException e) {
			throw new OIOException("Error in read", e);
		}
	}

	public void close() throws IOException {
		if (input != null)
			input.close();

		start = end = -1;
		current = size = 0;
	}

	public boolean ready() {
		try {
			return current < end || input.ready();
		} catch (IOException e) {
			throw new OIOException("Error in ready", e);
		}
	}

	public int length() {
		return (int) size;
	}

	public CharSequence subSequence(final int start, final int end) {
		throw new UnsupportedOperationException();
	}

	public long getPosition() {
		return current;
	}

	@Override
	public String toString() {
		return (start > 0 ? "..." : "") + new String(buffer) + (ready() ? "..." : "");
	}

	public int indexOf(final char iToFind) {
		for (int i = (int) current; i < size; ++i) {
			if (charAt(i) == iToFind)
				return i;
		}
		return -1;
	}

	public String subString(int iOffset, final char iToFind, boolean iIncluded) {
		StringBuilder buffer = new StringBuilder();

		char c;
		for (int i = iOffset; i < size; ++i) {
			c = charAt(i);
			if (c == iToFind) {
				if (iIncluded)
					buffer.append(c);

				return buffer.toString();
			}

			buffer.append(c);
		}

		buffer.setLength(0);
		return null;
	}
}
