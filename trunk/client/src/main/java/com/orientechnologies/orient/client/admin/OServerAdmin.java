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
package com.orientechnologies.orient.client.admin;

import java.io.IOException;
import java.net.UnknownHostException;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryClient;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;

/**
 * Remote admin of Orient Servers.
 */
public class OServerAdmin {
	private String									url;

	// private String userName;
	// private String userPassword;
	protected OChannelBinaryClient	network;
	protected int										sessionId;

	public OServerAdmin(String iURL) throws IOException {
		url = iURL;

		// CONNECT
	}

	public OServerAdmin connect() throws IOException {
		try {
			createNetworkConnection();

			network.writeByte((byte) OChannelBinaryProtocol.CONNECT);
			network.flush();

			readStatus();

			sessionId = network.readInt();

		} catch (Exception e) {
			OLogManager.instance().error(this, "Can't connect the remote server: " + url, e, OStorageException.class);
			close();
		}
		return this;
	}

	public OServerAdmin createDatabase(String iDatabaseName, String iDatabasePath, String iStorageMode) throws IOException {
		try {
			if (iStorageMode == null)
				iStorageMode = "csv";

			network.writeByte(OChannelBinaryProtocol.DB_CREATE);
			network.writeString(iStorageMode);
			network.flush();

			readStatus();

		} catch (Exception e) {
			OLogManager.instance().error(this, "Can't create the remote storage: " + iDatabaseName, e, OStorageException.class);
			close();
		}
		return this;
	}

	public void close() throws IOException {
		network.socket.close();
	}

	protected void readStatus() throws IOException {
		byte result = network.readByte();

		if (result == OChannelBinaryProtocol.ERROR)
			OLogManager.instance().error(this, network.readString(), null, OStorageException.class);
	}

	protected void createNetworkConnection() throws IOException, UnknownHostException {
		String remoteHost;
		int remotePort = 8000;

		int pos = url.indexOf("/");
		if (pos == -1) {
			remoteHost = "localhost";
		} else {
			int posRemotePort = url.indexOf(":");

			if (posRemotePort != -1) {
				remoteHost = url.substring(0, posRemotePort);
				remotePort = Integer.parseInt(url.substring(posRemotePort + 1, pos));
			} else {
				remoteHost = url.substring(0, pos);
			}
		}

		network = new OChannelBinaryClient(remoteHost, remotePort, remotePort);
	}
}
