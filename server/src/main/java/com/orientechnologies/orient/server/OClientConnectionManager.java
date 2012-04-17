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
package com.orientechnologies.orient.server;

import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.orientechnologies.common.concur.resource.OSharedResourceAbstract;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinary;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;
import com.orientechnologies.orient.server.network.protocol.ONetworkProtocol;
import com.orientechnologies.orient.server.network.protocol.binary.ONetworkProtocolBinary;

public class OClientConnectionManager extends OSharedResourceAbstract {
	protected Map<Integer, OClientConnection>			connections				= new HashMap<Integer, OClientConnection>();
	protected int																	connectionSerial	= 0;

	private static final OClientConnectionManager	instance					= new OClientConnectionManager();

	public OClientConnectionManager() {
	}

	/**
	 * Create a connection.
	 * 
	 * @param iSocket
	 * @param iProtocol
	 * @return
	 * @throws IOException
	 */
	public OClientConnection connect(final Socket iSocket, final ONetworkProtocol iProtocol) throws IOException {
		OProfiler.getInstance().updateCounter("OServer.connections.actives", +1);

		final OClientConnection connection;

		acquireExclusiveLock();
		try {
			connection = new OClientConnection(++connectionSerial, iProtocol);

			connections.put(connection.id, connection);

		} finally {
			releaseExclusiveLock();
		}

		OLogManager.instance().config(this, "Remote client connected from: " + connection);

		return connection;
	}

	public OClientConnection getConnection(final Socket socket, final int iChannelId) {
		acquireSharedLock();
		try {
			OClientConnection conn = null;

			// SEARCH THE CONNECTION BY ID
			conn = connections.get(iChannelId);
			
			// COMMENTED TO USE SOCKET POOL: THINK TO ANOTHER WAY TO IMPROVE SECURITY
			// if (conn != null && conn.getChannel().socket != socket)
			// throw new IllegalStateException("Requested sessionId " + iChannelId + " by connection " + socket
			// + " while it's tied to connection " + conn.getChannel().socket);

			return conn;

		} finally {
			releaseSharedLock();
		}
	}

	/**
	 * Disconnects a client connections
	 * 
	 * @param iChannelId
	 * @return true if was last one, otherwise false
	 */
	public boolean disconnect(final int iChannelId) {
		OProfiler.getInstance().updateCounter("OServer.connections.actives", -1);

		acquireExclusiveLock();
		try {
			final OClientConnection connection = connections.remove(iChannelId);

			if (connection != null) {
				connection.close();
				// CHECK IF THERE ARE OTHER CONNECTIONS
				for (Entry<Integer, OClientConnection> entry : connections.entrySet()) {
					if (entry.getValue().getProtocol().equals(connection.getProtocol()))
						return false;
				}
				return true;
			}

		} finally {
			releaseExclusiveLock();
		}
		return false;
	}

	public void disconnect(final OClientConnection connection) {
		OProfiler.getInstance().updateCounter("OServer.connections.actives", -1);

		connection.close();

		acquireExclusiveLock();
		try {
			for (Entry<Integer, OClientConnection> entry : new HashMap<Integer, OClientConnection>(connections).entrySet()) {
				if (entry.getValue().equals(connection))
					connections.remove(entry.getKey());
			}

		} finally {
			releaseExclusiveLock();
		}
	}

	public static OClientConnectionManager instance() {
		return instance;
	}

	public List<OClientConnection> getConnections() {
		acquireSharedLock();
		try {
			return new ArrayList<OClientConnection>(connections.values());
		} finally {
			releaseSharedLock();
		}
	}

	/**
	 * Pushes the record to all the connected clients with the same database.
	 * 
	 * @param iRecord
	 *          Record to broadcast
	 * @param iExcludeConnection
	 *          Connection to exclude if any, usually the current where the change has been just applied
	 */
	public void broadcastRecord2Clients(final ORecordInternal<?> iRecord, final OClientConnection iExcludeConnection)
			throws InterruptedException, IOException {
		acquireSharedLock();
		try {
			final String dbName = iRecord.getDatabase().getName();

			for (OClientConnection c : connections.values()) {
				if (c != iExcludeConnection) {
					final ONetworkProtocolBinary p = (ONetworkProtocolBinary) c.protocol;
					final OChannelBinary channel = (OChannelBinary) p.getChannel();

					if (c.database != null && c.database.getName().equals(dbName))
						synchronized (c.records2Push) {
							channel.acquireExclusiveLock();
							try {
								channel.writeByte(OChannelBinaryProtocol.PUSH_DATA);
								channel.writeInt(Integer.MIN_VALUE);
								channel.writeByte(OChannelBinaryProtocol.REQUEST_PUSH_RECORD);
								p.writeIdentifiable(iRecord);
							} finally {
								channel.releaseExclusiveLock();
							}
						}

				}
			}

		} finally {
			releaseSharedLock();
		}
	}

	/**
	 * Retrieves the connection by address/port.
	 * 
	 * @param iAddress
	 *          The address as string in the format address as format <ip>:<port>
	 * @return The connection if any, otherwise null
	 */
	public OClientConnection getConnection(final String iAddress) {
		acquireSharedLock();
		try {
			for (OClientConnection conn : connections.values()) {
				if (iAddress.equals(conn.getRemoteAddress()))
					return conn;
			}
			return null;
		} finally {
			releaseSharedLock();
		}
	}
}
