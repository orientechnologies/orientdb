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
package com.orientechnologies.orient.server.network.protocol.distributed;

import java.io.IOException;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.tool.ODatabaseImport;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.metadata.security.OUser;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.impl.local.OStorageLocal;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryInputStream;
import com.orientechnologies.orient.enterprise.channel.distributed.OChannelDistributedProtocol;
import com.orientechnologies.orient.server.OServerMain;
import com.orientechnologies.orient.server.handler.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.handler.distributed.ODistributedServerNode;
import com.orientechnologies.orient.server.network.protocol.binary.ONetworkProtocolBinary;

/**
 * Extends binary protocol to include cluster commands.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class ONetworkProtocolDistributed extends ONetworkProtocolBinary implements OCommandOutputListener {
	private ODistributedServerManager	manager;

	public ONetworkProtocolDistributed() {
		super("Distributed-DB");

		manager = OServerMain.server().getHandler(ODistributedServerManager.class);
		if (manager == null)
			throw new OConfigurationException(
					"Can't find a ODistributedServerDiscoveryManager instance registered as handler. Check the server configuration in the handlers section.");
	}

	@Override
	protected void parseCommand() throws IOException {
		if (requestType < 80) {
			// BINARY REQUESTS
			super.parseCommand();
			return;
		}

		// DISTRIBUTED SERVER REQUESTS
		switch (requestType) {
		case OChannelDistributedProtocol.REQUEST_DISTRIBUTED_HEARTBEAT:
			data.commandInfo = "Keep-alive";
			manager.updateHeartBeatTime();
			sendOk(0);
			break;

		case OChannelDistributedProtocol.REQUEST_DISTRIBUTED_CONNECT: {
			data.commandInfo = "Cluster connection";

			manager.receivedLeaderConnection(this);
			sendOk(0);
			break;
		}

		case OChannelDistributedProtocol.REQUEST_DISTRIBUTED_DB_SHARE_SENDER: {
			data.commandInfo = "Share the database to a remote server";

			ODatabaseDocumentTx db = null;

			try {
				final String dbName = channel.readString();
				final String dbUser = channel.readString();
				final String dbPassword = channel.readString();
				final String remoteServerName = channel.readString();
				final String mode = channel.readString();

				checkServerAccess("database.share");

				db = openDatabase(dbName, dbUser, dbPassword);

				final String engineName = db.getStorage() instanceof OStorageLocal ? "local" : "memory";

				final ODistributedServerNode remoteServerNode = manager.getNode(remoteServerName);

				remoteServerNode.shareDatabase(db, remoteServerName, engineName, mode);

				manager.addServer(remoteServerName, remoteServerName, mode);

				sendOk(0);

			} finally {
				if (db != null)
					db.close();
			}
			break;
		}

		case OChannelDistributedProtocol.REQUEST_DISTRIBUTED_DB_SHARE_RECEIVER: {
			data.commandInfo = "Received a shared database from a remote server to install";

			final String dbName = channel.readString();
			final String engineName = channel.readString();

			OLogManager.instance().info(this, "Received database '%s' to share on local server node", dbName);

			final ODatabaseDocumentTx db = getDatabaseInstance(dbName, engineName);

			try {
				if (db.exists()) {
					OLogManager.instance().info(this, "Deleting existent database '%s'", db.getName());
					db.delete();
				}

				createDatabase(db);

				if (db.isClosed())
					db.open(OUser.ADMIN, OUser.ADMIN);

				OLogManager.instance().info(this, "Importing database '%s' via streaming from remote server node...", dbName);

				new ODatabaseImport(db, new OChannelBinaryInputStream(channel), this).importDatabase();

				OLogManager.instance().info(this, "Database imported correctly", dbName);

				sendOk(0);

			} finally {
				db.close();
			}
			break;
		}

		case OChannelDistributedProtocol.REQUEST_DISTRIBUTED_DB_CONFIG: {
			data.commandInfo = "Update db configuration from server node leader";
			ODocument config = (ODocument) new ODocument().fromStream(channel.readBytes());

			OLogManager.instance().warn(this, "Changed distributed server configuration:\n%s", config.toJSON("indent:2"));

			sendOk(0);
			break;
		}

		default:
			data.commandInfo = "Command not supported";
			OLogManager.instance().error(this, "Request not supported. Code: " + requestType);
			channel.clearInput();
			sendError(clientTxId, null);
		}
	}

	public void onMessage(String iText) {
	}
}
