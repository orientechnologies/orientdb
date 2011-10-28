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
package com.orientechnologies.orient.server.network.protocol.http;

import java.io.IOException;
import java.net.Socket;

import com.orientechnologies.orient.core.OConstants;
import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.server.OClientConnection;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.network.protocol.http.command.delete.OServerCommandDeleteClass;
import com.orientechnologies.orient.server.network.protocol.http.command.delete.OServerCommandDeleteDatabase;
import com.orientechnologies.orient.server.network.protocol.http.command.delete.OServerCommandDeleteDocument;
import com.orientechnologies.orient.server.network.protocol.http.command.delete.OServerCommandDeleteIndex;
import com.orientechnologies.orient.server.network.protocol.http.command.delete.OServerCommandDeleteProperty;
import com.orientechnologies.orient.server.network.protocol.http.command.get.OServerCommandGetClass;
import com.orientechnologies.orient.server.network.protocol.http.command.get.OServerCommandGetCluster;
import com.orientechnologies.orient.server.network.protocol.http.command.get.OServerCommandGetConnect;
import com.orientechnologies.orient.server.network.protocol.http.command.get.OServerCommandGetDatabase;
import com.orientechnologies.orient.server.network.protocol.http.command.get.OServerCommandGetDictionary;
import com.orientechnologies.orient.server.network.protocol.http.command.get.OServerCommandGetDisconnect;
import com.orientechnologies.orient.server.network.protocol.http.command.get.OServerCommandGetDocument;
import com.orientechnologies.orient.server.network.protocol.http.command.get.OServerCommandGetDocumentByClass;
import com.orientechnologies.orient.server.network.protocol.http.command.get.OServerCommandGetFileDownload;
import com.orientechnologies.orient.server.network.protocol.http.command.get.OServerCommandGetIndex;
import com.orientechnologies.orient.server.network.protocol.http.command.get.OServerCommandGetListDatabases;
import com.orientechnologies.orient.server.network.protocol.http.command.get.OServerCommandGetQuery;
import com.orientechnologies.orient.server.network.protocol.http.command.get.OServerCommandGetServer;
import com.orientechnologies.orient.server.network.protocol.http.command.get.OServerCommandGetStorageAllocation;
import com.orientechnologies.orient.server.network.protocol.http.command.options.OServerCommandOptions;
import com.orientechnologies.orient.server.network.protocol.http.command.post.OServerCommandPostClass;
import com.orientechnologies.orient.server.network.protocol.http.command.post.OServerCommandPostCommand;
import com.orientechnologies.orient.server.network.protocol.http.command.post.OServerCommandPostCreateDatabase;
import com.orientechnologies.orient.server.network.protocol.http.command.post.OServerCommandPostDatabase;
import com.orientechnologies.orient.server.network.protocol.http.command.post.OServerCommandPostDocument;
import com.orientechnologies.orient.server.network.protocol.http.command.post.OServerCommandPostProperty;
import com.orientechnologies.orient.server.network.protocol.http.command.post.OServerCommandPostStudio;
import com.orientechnologies.orient.server.network.protocol.http.command.post.OServerCommandPostUploadSingleFile;
import com.orientechnologies.orient.server.network.protocol.http.command.put.OServerCommandPutDocument;
import com.orientechnologies.orient.server.network.protocol.http.command.put.OServerCommandPutIndex;

public class ONetworkProtocolHttpDb extends ONetworkProtocolHttpAbstract {
	private static final String	ORIENT_SERVER_DB	= "OrientDB Server v." + OConstants.getVersion();

	@Override
	public void config(final OServer iServer, final Socket iSocket, final OClientConnection iConnection,
			final OContextConfiguration iConfiguration) throws IOException {
		server = iServer;
		setName("HTTP-DB");
		data.serverInfo = ORIENT_SERVER_DB;

		registerCommand(new OServerCommandGetConnect());
		registerCommand(new OServerCommandGetDisconnect());

		registerCommand(new OServerCommandGetClass());
		registerCommand(new OServerCommandGetCluster());
		registerCommand(new OServerCommandGetDatabase());
		registerCommand(new OServerCommandGetDictionary());
		registerCommand(new OServerCommandGetDocument());
		registerCommand(new OServerCommandGetDocumentByClass());
		registerCommand(new OServerCommandGetQuery());
		registerCommand(new OServerCommandGetServer());
		registerCommand(new OServerCommandGetStorageAllocation());
		registerCommand(new OServerCommandGetFileDownload());
		registerCommand(new OServerCommandGetIndex());
		registerCommand(new OServerCommandGetListDatabases());

		registerCommand(new OServerCommandPostClass());
		registerCommand(new OServerCommandPostCommand());
		registerCommand(new OServerCommandPostDatabase());
		registerCommand(new OServerCommandPostDocument());
		registerCommand(new OServerCommandPostProperty());
		registerCommand(new OServerCommandPostStudio());
		registerCommand(new OServerCommandPostUploadSingleFile());
		registerCommand(new OServerCommandPostCreateDatabase());

		registerCommand(new OServerCommandPutDocument());
		registerCommand(new OServerCommandPutIndex());

		registerCommand(new OServerCommandDeleteClass());
		registerCommand(new OServerCommandDeleteDatabase());
		registerCommand(new OServerCommandDeleteDocument());
		registerCommand(new OServerCommandDeleteProperty());
		registerCommand(new OServerCommandDeleteIndex());

		registerCommand(new OServerCommandOptions());

		super.config(server, iSocket, iConnection, iConfiguration);
	}
}
