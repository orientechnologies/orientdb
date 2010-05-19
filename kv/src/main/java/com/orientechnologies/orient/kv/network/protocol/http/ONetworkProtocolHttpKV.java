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
package com.orientechnologies.orient.kv.network.protocol.http;

import java.io.IOException;
import java.net.Socket;

import com.orientechnologies.orient.core.OConstants;
import com.orientechnologies.orient.kv.network.protocol.http.command.OKVServerCommandDeleteEntry;
import com.orientechnologies.orient.kv.network.protocol.http.command.OKVServerCommandGetEntry;
import com.orientechnologies.orient.kv.network.protocol.http.command.OKVServerCommandPostEntry;
import com.orientechnologies.orient.kv.network.protocol.http.command.OKVServerCommandPutEntry;
import com.orientechnologies.orient.server.OClientConnection;
import com.orientechnologies.orient.server.network.protocol.http.ONetworkProtocolHttpAbstract;
import com.orientechnologies.orient.server.network.protocol.http.command.get.OServerCommandGetStaticContent;

public abstract class ONetworkProtocolHttpKV extends ONetworkProtocolHttpAbstract {
	private static final String	ORIENT_SERVER_KV	= "Orient Key Value v." + OConstants.ORIENT_VERSION;

	protected OKVDictionary			dictionary;

	@Override
	public void config(Socket iSocket, OClientConnection iConnection) throws IOException {
		setName("HTTP-KV");
		data.serverInfo = ORIENT_SERVER_KV;

		registerCommand(new OServerCommandGetStaticContent());

		registerCommand(new OKVServerCommandGetEntry(dictionary));
		registerCommand(new OKVServerCommandPostEntry(dictionary));
		registerCommand(new OKVServerCommandPutEntry(dictionary));
		registerCommand(new OKVServerCommandDeleteEntry(dictionary));

		super.config(iSocket, iConnection);
	}
}
