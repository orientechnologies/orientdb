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
import java.net.Socket;
import java.net.UnknownHostException;

/**
 * TcpClient.java
 * 
 * This class works in conjunction with TcpServer.java and TcpPayload.java
 * 
 * This client test class connects to server class TcpServer, and in response, it receives a serialized an instance of TcpPayload.
 */

public class TcpClient {
	public final static String	SERVER_HOSTNAME	= "localhost";
	public final static int			COMM_PORT				= 5050;				// socket port for client comms

	private Socket							socket;

	/** Default constructor. */
	public TcpClient() {
		long transferred = 0;
		long date = System.currentTimeMillis();

		try {
			this.socket = new Socket(SERVER_HOSTNAME, COMM_PORT);
			this.socket.setSendBufferSize(65000);
			InputStream iStream = this.socket.getInputStream();
			ObjectInputStream oiStream = new ObjectInputStream(iStream);

			OutputStream oStream = this.socket.getOutputStream();
			ObjectOutputStream ooStream = new ObjectOutputStream(oStream);

			int i = 0;
			byte[] buffer = null;
			while (true) {
				int size = oiStream.readInt();
				transferred += 4;

				if (size == 0)
					break;

				++i;

				if (i > 10 && i % (1000000 / 10) == 0)
					System.out.print(".");

				buffer = new byte[size];
				oiStream.readFully(buffer);
				transferred += size;

				ooStream.writeByte(0);
				ooStream.flush();
				oStream.flush();
			}
		} catch (UnknownHostException uhe) {
			System.out.println("Don't know about host: " + SERVER_HOSTNAME);
		} catch (IOException ioe) {
			System.out.println("Couldn't get I/O for the connection to: " + SERVER_HOSTNAME + ":" + COMM_PORT);
		} finally {
			System.out.println("Transferred total MB: " + (float) transferred / 1000000 + " in " + (System.currentTimeMillis() - date)
					+ "ms");
		}
	}

	/**
	 * Run this class as an application.
	 */
	public static void main(String[] args) {
		new TcpClient();
	}
}