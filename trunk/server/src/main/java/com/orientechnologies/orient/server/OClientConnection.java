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

import com.orientechnologies.orient.core.db.vobject.ODatabaseVObjectTx;
import com.orientechnologies.orient.server.network.protocol.ONetworkProtocol;

public class OClientConnection {
	public String							id;
	public ONetworkProtocol		protocol;
	public long								since;
	public ODatabaseVObjectTx	database;

	public OClientConnection(final int iId, final Socket iSocket, final ONetworkProtocol iProtocol) throws IOException {
		this.id = String.valueOf(iId);
		this.protocol = iProtocol;
		this.since = System.currentTimeMillis();
	}

	@Override
	public String toString() {
		return "OClientConnection [id=" + id + ", source="
				+ (protocol != null && protocol.getChannel() != null ? protocol.getChannel().socket.getRemoteSocketAddress() : "?")
				+ ", since=" + since + "]";
	}

	@Override
	public int hashCode() {
		return id.hashCode();
	}

	@Override
	public boolean equals(final Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		OClientConnection other = (OClientConnection) obj;
		if (id != other.id)
			return false;
		return true;
	}
}
