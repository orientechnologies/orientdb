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
package com.orientechnologies.orient.server.managed;

import java.util.Collection;
import java.util.List;

import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.server.OClientConnection;
import com.orientechnologies.orient.server.OClientConnectionManager;
import com.orientechnologies.orient.server.network.protocol.ONetworkProtocol;

public class OrientServer implements OrientServerMBean {
	protected List<OStorage>	storages;
	protected String					info;

	@Override
	public OStorage[] getOpenedStorages() {
		OStorage[] output = new OStorage[Orient.instance().getStorages().size()];
		output = Orient.instance().getStorages().toArray(output);
		return output;
	}

	@Override
	public OClientConnection[] getConnections() {
		final Collection<OClientConnection> conns = OClientConnectionManager.instance().getConnections();
		final OClientConnection[] output = new OClientConnection[conns.size()];
		conns.toArray(output);
		return output;
	}

	@Override
	public ONetworkProtocol[] getProtocols() {
		final Collection<ONetworkProtocol> handlers = OClientConnectionManager.instance().getHandlers();
		final ONetworkProtocol[] output = new ONetworkProtocol[handlers.size()];
		handlers.toArray(output);
		return output;
	}
}
