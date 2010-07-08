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
package com.orientechnologies.orient.test.internal.network;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;

public class TcpServer {
	public final static int	COMM_PORT	= 5050; // socket port for client comms

	private ServerSocket		serverSocket;
	private byte[]					payload;

	/** Default constructor. */
	public TcpServer() {
		initServerSocket();

		long transferred = 0;
		long date = System.currentTimeMillis();

		try {
			while (true) {
				// listen for and accept a client connection to serverSocket
				Socket sock = this.serverSocket.accept();
				sock.setSendBufferSize(65000);

				InputStream iStream = sock.getInputStream();
				OutputStream oStream = sock.getOutputStream();
				ObjectOutputStream ooStream = new ObjectOutputStream(oStream);
				ObjectInputStream iiStream = null;
				for (int i = 0; i < 1000000; ++i) {
					this.payload = new String("10|Gipsy|European|Rome|300.00").getBytes();

					ooStream.writeInt(this.payload.length);
					transferred += 4;
					ooStream.write(this.payload); // send serialized payload
					transferred += this.payload.length;

					ooStream.flush();
					oStream.flush();

					if (iiStream == null)
						iiStream = new ObjectInputStream(iStream);

					iiStream.readByte();
				}
				ooStream.writeInt(0);

				System.out.println("Transferred total MB: " + (float) transferred / 1000000 + " in " + (System.currentTimeMillis() - date)
						+ "ms");

				Thread.sleep(1000);
				ooStream.close();
			}
		} catch (SecurityException se) {
			System.err.println("Unable to get host address due to security.");
			System.err.println(se.toString());
		} catch (IOException ioe) {
			System.err.println("Unable to read data from an open socket.");
			System.err.println(ioe.toString());
		} catch (InterruptedException ie) {
		} // Thread sleep interrupted
		finally {
			try {
				this.serverSocket.close();
			} catch (IOException ioe) {
				System.err.println("Unable to close an open socket.");
				System.err.println(ioe.toString());
				System.exit(1);
			}
		}
	}

	/** Initialize a server socket for communicating with the client. */
	private void initServerSocket() {
		try {
			this.serverSocket = new java.net.ServerSocket(COMM_PORT);
			assert this.serverSocket.isBound();
			if (this.serverSocket.isBound()) {
				System.out.println("SERVER inbound data port " + this.serverSocket.getLocalPort()
						+ " is ready and waiting for client to connect...");
			}
		} catch (SocketException se) {
			System.err.println("Unable to create socket.");
			System.err.println(se.toString());
			System.exit(1);
		} catch (IOException ioe) {
			System.err.println("Unable to read data from an open socket.");
			System.err.println(ioe.toString());
			System.exit(1);
		}
	}

	/**
	 * Run this class as an application.
	 */
	public static void main(String[] args) {
		new TcpServer();
	}
}