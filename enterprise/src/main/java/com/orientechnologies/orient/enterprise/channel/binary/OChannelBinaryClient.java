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
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;

import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;

public class OChannelBinaryClient extends OChannelBinaryAsynch {
	final protected int	timeout;	// IN MS

	public OChannelBinaryClient(final String remoteHost, final int remotePort, final OContextConfiguration iConfig)
			throws IOException {
		super(new Socket(), iConfig);
		timeout = iConfig.getValueAsInteger(OGlobalConfiguration.NETWORK_SOCKET_TIMEOUT);

		socket.setPerformancePreferences(0, 2, 1);

		socket.setKeepAlive(true);
		socket.setSendBufferSize(socketBufferSize);
		socket.setReceiveBufferSize(socketBufferSize);
		socket.connect(new InetSocketAddress(remoteHost, remotePort), timeout);

		inStream = new BufferedInputStream(socket.getInputStream(), socketBufferSize);
		outStream = new BufferedOutputStream(socket.getOutputStream(), socketBufferSize);

		in = new DataInputStream(inStream);
		out = new DataOutputStream(outStream);
	}

	public void reconnect() throws IOException {
		SocketAddress address = socket.getRemoteSocketAddress();
		socket.close();
		socket.connect(address, timeout);
	}

	/**
	 * Tells if the channel is connected.
	 * 
	 * @return true if it's connected, otherwise false.
	 */
	public boolean isConnected() {
		if (socket != null && socket.isConnected() && !socket.isInputShutdown() && !socket.isOutputShutdown()) {
			// try {
			// out.flush();
			return true;
			// } catch (IOException e) {
			// }
		}

		return false;
	}
}