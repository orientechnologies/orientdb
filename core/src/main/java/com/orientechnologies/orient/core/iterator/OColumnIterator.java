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
package com.orientechnologies.orient.core.iterator;

import java.util.Iterator;
import java.util.NoSuchElementException;

import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;

public class OColumnIterator implements Iterator<String>, Iterable<String> {
	private char			separator	= OStringSerializerHelper.RECORD_SEPARATOR_AS_CHAR;
	private String		buffer;
	private int				cursor		= 0;
	private String[]	values;

	public OColumnIterator(String iBuffer) {
		this.buffer = iBuffer;
	}

	public Iterator<String> iterator() {
		return this;
	}

	public boolean hasNext() {
		if (values == null)
			values = OStringSerializerHelper.split(buffer, separator);

		return cursor < values.length;
	}

	public String next() {
		if (!hasNext())
			throw new NoSuchElementException("Position " + cursor + " is out of range: 0-" + values.length);

		return values[cursor++];
	}

	public void reset(String iBuffer) {
		reset();
		buffer = iBuffer;
	}

	public void reset() {
		cursor = 0;
	}

	public void remove() {
		throw new UnsupportedOperationException("Remove is not supported");
	}

	public char getSeparator() {
		return separator;
	}

	public void setSeparator(char separator) {
		this.separator = separator;
	}
}
