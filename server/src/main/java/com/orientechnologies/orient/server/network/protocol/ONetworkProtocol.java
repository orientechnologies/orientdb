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
package com.orientechnologies.orient.server.network.protocol;

import java.io.IOException;
import java.net.Socket;

import com.orientechnologies.common.thread.OSoftThread;
import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.enterprise.channel.OChannel;
import com.orientechnologies.orient.server.OClientConnection;
import com.orientechnologies.orient.server.OServer;

public abstract class ONetworkProtocol extends OSoftThread {
	protected ONetworkProtocolData	data	= new ONetworkProtocolData();
	protected OServer								server;

	public ONetworkProtocol(ThreadGroup group, String name) {
		super(group, name);
	}

	public abstract void config(OServer iServer, Socket iSocket, OClientConnection iConnection, OContextConfiguration iConfiguration)
			throws IOException;

	public abstract OChannel getChannel();

	public void registerCommand(final Object iServerCommandInstance) {
	}

	public ONetworkProtocolData getData() {
		return data;
	}

	public OServer getServer() {
		return server;
	}
}
