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
import java.util.HashMap;
import java.util.Map;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.orient.server.network.protocol.ONetworkProtocol;

public class OClientConnectionManager {
	public static final int												DEFAULT_CONN_EXPIRATION	= 60;																			// SECONDS
	protected int																	expiration							= DEFAULT_CONN_EXPIRATION;
	protected Map<String, OClientConnection>			connections							= new HashMap<String, OClientConnection>();
	protected Map<String, ONetworkProtocol>				handlers								= new HashMap<String, ONetworkProtocol>();

	private static final OClientConnectionManager	instance								= new OClientConnectionManager();

	public OClientConnectionManager() {
	}

	public void connect(final Socket iSocket, final OClientConnection iConnection) throws IOException {
		OProfiler.getInstance().updateCounter("OServer.threads.actives", +1);

		connections.put(iConnection.id, iConnection);

		handlers.put(iConnection.id, iConnection.protocol);

		OLogManager.instance().config(this, "Remote client connected from: " + iConnection);
	}

	public OClientConnection getConnection(final String iChannelId) {
		return connections.get(iChannelId);
	}

	public void onClientDisconnection(final String iChannelId) {
		OProfiler.getInstance().updateCounter("OServer.threads.actives", -1);

		OClientConnection conn = connections.remove(iChannelId);
		if (conn == null)
			return;
		
		handlers.remove(iChannelId);
	}

	public int getExpiration() {
		return expiration;
	}

	public void setExpiration(final int iExpiration) {
		this.expiration = iExpiration;
	}

	public static OClientConnectionManager instance() {
		return instance;
	}

	public Map<String, OClientConnection> getConnections() {
		return connections;
	}

	public Map<String, ONetworkProtocol> getHandlers() {
		return handlers;
	}
}
