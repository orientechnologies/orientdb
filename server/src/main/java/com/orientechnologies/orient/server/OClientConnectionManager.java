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

import com.orientechnologies.common.concur.resource.OSharedResource;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.orient.server.network.protocol.ONetworkProtocol;

public class OClientConnectionManager extends OSharedResource {
	protected Map<Integer, OClientConnection>			connections				= new HashMap<Integer, OClientConnection>();
	protected Map<Integer, ONetworkProtocol>			handlers					= new HashMap<Integer, ONetworkProtocol>();
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
		OProfiler.getInstance().updateCounter("OServer.threads.actives", +1);

		final OClientConnection connection;

		acquireExclusiveLock();
		try {
			connection = new OClientConnection(++connectionSerial, iSocket, iProtocol);

			connections.put(connection.id, connection);
			handlers.put(connection.id, connection.protocol);

		} finally {
			releaseExclusiveLock();
		}

		OLogManager.instance().config(this, "Remote client connected from: " + connection);

		return connection;
	}

	public OClientConnection getConnection(final int iChannelId) {
		acquireSharedLock();
		try {
			return connections.get(iChannelId);
		} finally {
			releaseSharedLock();
		}
	}

	public void onClientDisconnection(final int iChannelId) {
		OProfiler.getInstance().updateCounter("OServer.threads.actives", -1);

		acquireExclusiveLock();
		try {
			final OClientConnection conn = connections.remove(iChannelId);
			if (conn == null)
				return;

			handlers.remove(iChannelId);

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

	public List<ONetworkProtocol> getHandlers() {
		acquireSharedLock();
		try {
			return new ArrayList<ONetworkProtocol>(handlers.values());
		} finally {
			releaseSharedLock();
		}
	}
}
