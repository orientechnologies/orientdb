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
package com.orientechnologies.orient.server.network;

import java.io.IOException;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.enterprise.channel.OChannel;
import com.orientechnologies.orient.server.OClientConnection;
import com.orientechnologies.orient.server.OClientConnectionManager;
import com.orientechnologies.orient.server.network.protocol.ONetworkProtocol;

public class OServerNetworkListener extends Thread {
	private ServerSocket											serverSocket;
	private InetSocketAddress									inboundAddr;
	private Class<? extends ONetworkProtocol>	protocolType;
	private volatile int											connectionSerial	= 0;
	private volatile boolean									active						= true;

	public OServerNetworkListener(final String iHostName, final String iHostPortRange, final String iProtocolName,
			final Class<? extends ONetworkProtocol> iProtocol) {
		listen(iHostName, iHostPortRange, iProtocolName);
		protocolType = iProtocol;
		start();
	}

	public void shutdown() {
		this.active = false;
	}

	/**
	 * Initialize a server socket for communicating with the client.
	 * 
	 * @param iHostPortRange
	 * @param iHostName
	 */
	private void listen(final String iHostName, final String iHostPortRange, final String iProtocolName) {
		int[] ports;

		if (iHostPortRange.contains(",")) {
			// MULTIPLE ENUMERATED PORTS
			String[] portValues = iHostPortRange.split(",");
			ports = new int[portValues.length];
			for (int i = 0; i < portValues.length; ++i)
				ports[i] = Integer.parseInt(portValues[i]);

		} else if (iHostPortRange.contains("-")) {
			// MULTIPLE RANGE PORTS
			String[] limits = iHostPortRange.split("-");
			int lowerLimit = Integer.parseInt(limits[0]);
			int upperLimit = Integer.parseInt(limits[1]);
			ports = new int[upperLimit - lowerLimit + 1];
			for (int i = 0; i < upperLimit - lowerLimit + 1; ++i)
				ports[i] = lowerLimit + i;

		} else
			// SINGLE PORT SPECIFIED
			ports = new int[] { Integer.parseInt(iHostPortRange) };

		for (int port : ports) {
			inboundAddr = new InetSocketAddress(iHostName, port);
			try {
				serverSocket = new java.net.ServerSocket(port);

				if (serverSocket.isBound()) {
					OLogManager.instance().config(this,
							"Listening " + iProtocolName + " connections on " + inboundAddr.getHostName() + ":" + inboundAddr.getPort());
					return;
				}
			} catch (BindException be) {
				OLogManager.instance().info(this, "Port %s:%d busy, trying the next available...", iHostName, port);
			} catch (SocketException se) {
				OLogManager.instance().error(this, "Unable to create socket", se);
				System.exit(1);
			} catch (IOException ioe) {
				OLogManager.instance().error(this, "Unable to read data from an open socket", ioe);
				System.err.println("Unable to read data from an open socket.");
				System.exit(1);
			}
		}

		OLogManager.instance().error(this, "Unable to listen connection using the configured ports '%s' on host '%s'", iHostPortRange,
				iHostName);
		System.exit(1);
	}

	public boolean isActive() {
		return active;
	}

	@Override
	public void run() {
		ONetworkProtocol protocol;
		OClientConnection connection;

		try {
			while (active) {
				try {
					// listen for and accept a client connection to serverSocket
					Socket socket = serverSocket.accept();

					socket.setPerformancePreferences(0, 2, 1);
					socket.setSendBufferSize(OChannel.DEFAULT_BUFFER_SIZE);
					socket.setReceiveBufferSize(OChannel.DEFAULT_BUFFER_SIZE);

					// CREATE A NEW PROTOCOL INSTANCE
					protocol = protocolType.newInstance();

					// CTEARE THE CLIENT CONNECTION
					connection = new OClientConnection(connectionSerial++, socket, protocol);

					// CONFIGURE THE PROTOCOL FOR THE INCOMING CONNECTION
					protocol.config(socket, connection);

					// EXECUTE THE CONNECTION
					OClientConnectionManager.instance().connect(socket, connection);

				} catch (Throwable e) {
					OLogManager.instance().error(this, "Error on client connection", e);
				} finally {
				}
			}
		} finally {
			try {
				if (serverSocket != null && !serverSocket.isClosed())
					serverSocket.close();
			} catch (IOException ioe) {
			}
		}
	}
}