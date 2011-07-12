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
import java.util.Arrays;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.db.tool.ODatabaseImport;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.exception.OSecurityException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.impl.local.OStorageLocal;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryInputStream;
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

		// DISTRIBUTED SERVER REQUESTS
		switch (lastRequestType) {
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
			final String clusterName = channel.readString();
			final byte[] encodedSecurityKey = channel.readBytes();
			final long runningSince = channel.readLong();

			if (!clusterName.equals(manager.getName()) || !Arrays.equals(encodedSecurityKey, manager.getSecurityKey()))
				throw new OSecurityException("Invalid combination of cluster name and key received");

			channel.acquireExclusiveLock();
			try {
				sendOk(lastClientTxId);

				if (manager.isLeader()) {
					OLogManager.instance().warn(this,
							"Received remote connection from the leader node %s, but current node is leader itself: split network problem?",
							channel.socket.getRemoteSocketAddress());

					if (runningSince > manager.getRunningSince()) {
						// OTHER NODE IS OLDER: WINS
						OLogManager.instance().warn(this, "Current node becames Non-Leader since the other node is running since longer time");
						manager.receivedLeaderConnection(this);
						channel.writeByte((byte) 1);
					} else {
						OLogManager.instance().warn(this, "Current node remains Leader since it's running since longer time");
						// THIS NODE IS OLDER: WIN! REFUSE THE CONNECTION
						channel.writeByte((byte) 0);
					}
				} else {
					manager.receivedLeaderConnection(this);
					channel.writeByte((byte) 1);
				}

			} finally {
				channel.releaseExclusiveLock();
			}
			break;
		}

		case OChannelDistributedProtocol.REQUEST_DISTRIBUTED_DB_OPEN: {
			checkConnected();
			data.commandInfo = "Open database connection";

			// REOPEN PREVIOUSLY MANAGED DATABASE
			final String dbUrl = channel.readString();
			openDatabase(dbUrl, channel.readString(), channel.readString());

			ODistributedRequesterThreadLocal.INSTANCE.set(true);

			channel.acquireExclusiveLock();
			try {
				sendOk(lastClientTxId);
				channel.writeInt(connection.id);
				channel.writeLong(connection.database.getStorage().getVersion());
			} finally {
				channel.releaseExclusiveLock();
			}
			break;
		}

		case OChannelDistributedProtocol.REQUEST_DISTRIBUTED_DB_SHARE_SENDER: {
			data.commandInfo = "Share the database to a remote server";

			final String dbUrl = channel.readString();
			final String dbUser = channel.readString();
			final String dbPassword = channel.readString();
			final String remoteServerName = channel.readString();
			final boolean synchronousMode = channel.readByte() == 1;

			checkServerAccess("database.share");

			openDatabase(dbUrl, dbUser, dbPassword);

			final String engineName = connection.database.getStorage() instanceof OStorageLocal ? "local" : "memory";

			final ODistributedServerNodeRemote remoteServerNode = manager.getNode(remoteServerName);

			remoteServerNode.shareDatabase(connection.database, remoteServerName, dbUser, dbPassword, engineName, synchronousMode);

			channel.acquireExclusiveLock();
			try {
				sendOk(lastClientTxId);
			} finally {
				channel.releaseExclusiveLock();
			}

			manager.addServerInConfiguration(dbUrl, remoteServerName, synchronousMode);

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

				channel.acquireExclusiveLock();
				try {
					new ODatabaseImport(connection.database, new OChannelBinaryInputStream(channel), this).importDatabase();

					OLogManager.instance().info(this, "Database imported correctly", dbName);

					sendOk(lastClientTxId);
					channel.writeInt(connection.id);
					channel.writeLong(connection.database.getStorage().getVersion());
				} finally {
					channel.releaseExclusiveLock();
				}

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

			OLogManager.instance().warn(this, "Changed distributed server configuration:\n%s", config.toJSON(""));

			channel.acquireExclusiveLock();
			try {
				sendOk(lastClientTxId);
			} finally {
				channel.releaseExclusiveLock();
			}
			break;
		}

		default:
			// BINARY REQUESTS
			super.parseCommand();
			return;
		}

		try {
			channel.flush();
		} catch (Throwable t) {
			OLogManager.instance().debug(this, "Error on send data over the network", t);
		}

	}

	@Override
	public void onMessage(String iText) {
	}

	protected void checkConnected() {
		if (!manager.isLeaderConnected())
			throw new OSecurityException("Invalid request from a non-connected node");
	}

}
