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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;

public class OChannelBinaryClient extends OChannelBinary {
	protected int	timeout	= 5000; // IN MS

	public OChannelBinaryClient(String remoteHost, int remotePort, int iTimeout) throws IOException {
		super(new Socket());
		timeout = iTimeout;

		socket.setPerformancePreferences(0, 2, 1);
		socket.connect(new InetSocketAddress(remoteHost, remotePort), timeout);

		inStream = new BufferedInputStream(socket.getInputStream(), DEFAULT_BUFFER_SIZE);
		outStream = new BufferedOutputStream(socket.getOutputStream(), DEFAULT_BUFFER_SIZE);

		in = new ObjectInputStream(inStream);
		out = new ObjectOutputStream(outStream);
	}

	public void reconnect() throws IOException {
		SocketAddress address = socket.getRemoteSocketAddress();
		socket.close();
		socket.connect(address, timeout);
	}
}