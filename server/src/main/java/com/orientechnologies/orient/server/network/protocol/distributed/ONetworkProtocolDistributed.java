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
import com.orientechnologies.orient.core.db.tool.ODatabaseImport;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.impl.local.OStorageLocal;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryInputStream;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;
import com.orientechnologies.orient.enterprise.channel.distributed.OChannelDistributedProtocol;
import com.orientechnologies.orient.server.OServerMain;
import com.orientechnologies.orient.server.handler.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.handler.distributed.ODistributedServerNodeRemote;
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
	protected void parseCommand() throws IOException, InterruptedException {

		try {
			// DISTRIBUTED SERVER REQUESTS
			switch (lastRequestType) {

			case OChannelBinaryProtocol.REQUEST_RECORD_UPDATE: {
				data.commandInfo = "Update record";

				final ORecordId rid = channel.readRID();
				final byte[] content = channel.readBytes();
				@SuppressWarnings("unused")
				final int version = channel.readInt();

				// BYPASS VERSION CHECK BY USING -1
				final long newVersion = connection.rawDatabase.save(rid, content, -1, channel.readByte());

				// TODO: Handle it by using triggers
				if (connection.database.getMetadata().getSchema().getDocument().getIdentity().equals(rid))
					connection.database.getMetadata().getSchema().reload();
				else if (connection.database.getMetadata().getIndexManager().getDocument().getIdentity().equals(rid))
					connection.database.getMetadata().getIndexManager().reload();

				sendOk(lastClientTxId);

				channel.writeInt((int) newVersion);
				break;
			}

			case OChannelDistributedProtocol.REQUEST_DISTRIBUTED_HEARTBEAT:
				checkConnected();
				data.commandInfo = "Keep-alive";
				manager.updateHeartBeatTime();

				sendOk(lastClientTxId);

				// SEND DB VERSION BACK
				// channel.writeLong(connection.database == null ? 0 : connection.database.getStorage().getVersion());
				break;

			case OChannelDistributedProtocol.REQUEST_DISTRIBUTED_CONNECT: {
				data.commandInfo = "Cluster connection";
				manager.receivedLeaderConnection(this);
				sendOk(lastClientTxId);
				break;
			}

			case OChannelDistributedProtocol.REQUEST_DISTRIBUTED_DB_OPEN: {
				checkConnected();
				data.commandInfo = "Open database connection";

				// REOPEN PREVIOUSLY MANAGED DATABASE
				final String dbName = channel.readString();
				openDatabase(dbName, channel.readString(), channel.readString());

				ODistributedRequesterThreadLocal.INSTANCE.set(true);

				sendOk(lastClientTxId);

				channel.writeInt(connection.id);
				channel.writeLong(connection.database.getStorage().getVersion());
				break;
			}

			case OChannelDistributedProtocol.REQUEST_DISTRIBUTED_DB_SHARE_SENDER: {
				data.commandInfo = "Share the database to a remote server";

				final String dbName = channel.readString();
				final String dbUser = channel.readString();
				final String dbPassword = channel.readString();
				final String remoteServerName = channel.readString();
				final boolean synchronousMode = channel.readByte() == 1;

				checkServerAccess("database.share");

				openDatabase(dbName, dbUser, dbPassword);

				final String engineName = connection.database.getStorage() instanceof OStorageLocal ? "local" : "memory";

				final ODistributedServerNodeRemote remoteServerNode = manager.getNode(remoteServerName);

				remoteServerNode.shareDatabase(connection.database, remoteServerName, dbUser, dbPassword, engineName, synchronousMode);

				sendOk(lastClientTxId);

				manager.addServerInConfiguration(dbName, remoteServerName, engineName, synchronousMode);

				break;
			}

			case OChannelDistributedProtocol.REQUEST_DISTRIBUTED_DB_SHARE_RECEIVER: {
				checkConnected();
				data.commandInfo = "Received a shared database from a remote server to install";

				final String dbName = channel.readString();
				final String dbUser = channel.readString();
				final String dbPasswd = channel.readString();
				final String engineName = channel.readString();

				ODistributedRequesterThreadLocal.INSTANCE.set(true);

				manager.setStatus(ODistributedServerManager.STATUS.SYNCHRONIZING);
				try {
					OLogManager.instance().info(this, "Received database '%s' to share on local server node", dbName);

					connection.database = getDatabaseInstance(dbName, engineName);

					if (connection.database.exists()) {
						OLogManager.instance().info(this, "Deleting existent database '%s'", connection.database.getName());
						connection.database.delete();
					}

					createDatabase(connection.database, dbUser, dbPasswd);

					if (connection.database.isClosed())
						connection.database.open(dbUser, dbPasswd);

					OLogManager.instance().info(this, "Importing database '%s' via streaming from remote server node...", dbName);

					new ODatabaseImport(connection.database, new OChannelBinaryInputStream(channel), this).importDatabase();

					OLogManager.instance().info(this, "Database imported correctly", dbName);

					sendOk(lastClientTxId);
					channel.writeInt(connection.id);
					channel.writeLong(connection.database.getStorage().getVersion());
				} finally {
					manager.updateHeartBeatTime();
					manager.setStatus(ODistributedServerManager.STATUS.ONLINE);
				}
				break;
			}

			case OChannelDistributedProtocol.REQUEST_DISTRIBUTED_DB_CONFIG: {
				checkConnected();
				data.commandInfo = "Update db configuration from server node leader";

				final ODocument config = (ODocument) new ODocument().fromStream(channel.readBytes());
				manager.setClusterConfiguration(connection.database.getName(), config);

				OLogManager.instance().warn(this, "Changed distributed server configuration:\n%s", config.toJSON("indent:2"));

				sendOk(lastClientTxId);
				break;
			}

			default:
				// BINARY REQUESTS
				super.parseCommand();

			}
		} finally {
			ODistributedRequesterThreadLocal.INSTANCE.remove();
		}
	}

	@Override
	public void onMessage(String iText) {
	}
}
