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
package com.orientechnologies.orient.enterprise.channel;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

public abstract class OChannel {
	public Socket							socket;

	public InputStream				inStream;
	public OutputStream				outStream;

	private static final int	DEFAULT_BUFFER_SIZE	= 4096;

	public OChannel(Socket iSocket) throws IOException {
		socket = iSocket;
	}

	public void connect() throws IOException {
		inStream = new BufferedInputStream(socket.getInputStream(), DEFAULT_BUFFER_SIZE);
		outStream = socket.getOutputStream();
	}

	public void flush() throws IOException {
		outStream.flush();
	}

	public void close() {
		try {
			socket.close();
		} catch (IOException e) {
		}

		try {
			inStream.close();
		} catch (IOException e) {
		}

		try {
			outStream.close();
		} catch (IOException e) {
		}
	}
}