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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

import com.orientechnologies.common.concur.resource.OSharedResourceExternalTimeout;
import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;

public abstract class OChannel extends OSharedResourceExternalTimeout {
	public Socket				socket;

	public InputStream	inStream;
	public OutputStream	outStream;

	public int					socketBufferSize;

	public OChannel(final Socket iSocket, final OContextConfiguration iConfig) throws IOException {
		super(OGlobalConfiguration.NETWORK_LOCK_TIMEOUT.getValueAsInteger());
		socket = iSocket;
		socketBufferSize = iConfig.getValueAsInteger(OGlobalConfiguration.NETWORK_SOCKET_BUFFER_SIZE);
	}

	public void flush() throws IOException {
		outStream.flush();
	}

	public void close() {
		try {
			if (socket != null)
				socket.close();
		} catch (IOException e) {
		}

		try {
			if (inStream != null)
				inStream.close();
		} catch (IOException e) {
		}

		try {
			if (outStream != null)
				outStream.close();
		} catch (IOException e) {
		}
	}

	@Override
	public String toString() {
		return socket != null ? socket.getRemoteSocketAddress().toString() : "Not connected";
	}
}