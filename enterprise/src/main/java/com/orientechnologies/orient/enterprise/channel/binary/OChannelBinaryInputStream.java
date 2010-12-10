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
package com.orientechnologies.orient.enterprise.channel.binary;

import java.io.IOException;
import java.io.InputStream;


/**
 * InputStream class bound to a binary channel. Reuses the shared buffer of the channel.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OChannelBinaryInputStream extends InputStream {
	private OChannelBinary	channel;
	private final byte[]		buffer;
	private int							pos		= 0;
	private int							total	= -1;
	private boolean					again	= true;

	public OChannelBinaryInputStream(final OChannelBinary channel) {
		this.channel = channel;
		buffer = channel.getBuffer();
	}

	@Override
	public int read() throws IOException {
		if (pos >= total)
			if (again)
				fetch();
			else
				return -1;

		return buffer[pos++];
	}

	@Override
	public int available() throws IOException {
		if (total < 0)
			// ONLY THE FIRST TIME
			fetch();

		final int remaining = total - pos;
		return remaining > 0 ? remaining : again ? 1 : 0;
	}

	private void fetch() throws IOException {
		// FETCH DATA
		pos = 0;

		total = channel.in.readInt();

		if (total > buffer.length)
			throw new ONetworkProtocolException("Bad chunk size received: " + total + " when the maximum can be: " + buffer.length);

		if (total > 0)
			channel.in.readFully(buffer, 0, total);

		again = channel.in.readByte() == 1;
	}
}
