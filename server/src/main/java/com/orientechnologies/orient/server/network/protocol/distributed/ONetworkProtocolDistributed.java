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
import java.net.InetAddress;
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
import com.orientechnologies.orient.server.network.protocol.binary.ONetworkProtocolBinary;
import com.orientechnologies.orient.server.replication.ODistributedNode;
import com.orientechnologies.orient.server.replication.OReplicator;

/**
 * Extends binary protocol to include cluster commands.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class ONetworkProtocolDistributed extends ONetworkProtocolBinary implements OCommandOutputListener {
	private ODistributedServerManager	manager;
	private OReplicator								replicator;

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
		case OChannelDistributedProtocol.REQUEST_DISTRIBUTED_LEADER_CONNECT: {
			data.commandInfo = "Clustered connection from leader";
			final ODocument doc = new ODocument().fromStream(channel.readBytes());
			final String clusterName = doc.field("clusterName");
			final byte[] encodedSecurityKey = doc.field("clusterKey");
			final String leaderAddress = doc.field("leaderNodeAddress");

			if (!clusterName.equals(manager.getName()) || !Arrays.equals(encodedSecurityKey, manager.getConfig().getSecurityKey()))
				throw new OSecurityException("Invalid combination of cluster name and key received");

			// final long leaderNodeRunningSince = doc.field("leaderNodeRunningSince");

			channel.acquireExclusiveLock();
			try {
				sendOk(lastClientTxId);
				channel.writeInt(connection.id);

				if (manager.isLeader()) {
					OLogManager.instance().warn(this,
							"Received remote connection from the leader node %s, but current node is leader itself: split network problem?",
							leaderAddress);

					// CHECK WHAT LEADER WINS
					final String myUid = InetAddress.getLocalHost().getHostAddress() + ":" + channel.socket.getLocalPort();

					if (leaderAddress.compareTo(myUid) > 0) {
						// BY CONVENTION THE LOWER VALUE WINS AND REMAIN LEADER
						// THIS NODE IS OLDER: WIN! REFUSE THE CONNECTION
						channel.writeByte((byte) 0);
						channel.flush();

						OLogManager.instance().warn(this, "Current node remains the Leader of the cluster because has lower network address",
								leaderAddress);
						return;
					}
				}

				channel.writeByte((byte) 1);
				manager.becomePeer(this);

				// SEND AVAILABLE DATABASES
				doc.reset();
				doc.field("availableDatabases", OServerMain.server().getAvailableStorageNames().keySet());
				channel.writeBytes(doc.toStream());
				channel.flush();

				manager.getPeer().updateHeartBeatTime();

				replicator = new OReplicator(manager, new ODocument(channel.readBytes()));

			} finally {
				channel.releaseExclusiveLock();
			}
			break;
		}

		case OChannelDistributedProtocol.REQUEST_DISTRIBUTED_HEARTBEAT:
			checkConnected();
			data.commandInfo = "Cluster Heartbeat";
			manager.getPeer().updateHeartBeatTime();

			sendOk(lastClientTxId);
			break;

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

			final ODistributedNode remoteServerNode = null;// manager.getPeerNode(remoteServerName);

			remoteServerNode.shareDatabase(connection.database, remoteServerName, dbUser, dbPassword, engineName, synchronousMode);

			channel.acquireExclusiveLock();
			try {
				sendOk(lastClientTxId);
			} finally {
				channel.releaseExclusiveLock();
			}

			manager.getPeer().updateConfigurationToLeader(dbUrl, remoteServerName, synchronousMode);

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
				} finally {
					channel.releaseExclusiveLock();
				}

			} finally {
				manager.getPeer().updateHeartBeatTime();
			}
			break;
		}

		case OChannelDistributedProtocol.REQUEST_DISTRIBUTED_DB_CONFIG: {
			checkConnected();
			data.commandInfo = "Update db configuration from server node leader";

			replicator.getClusterConfiguration().fromStream(channel.readBytes());

			OLogManager.instance().warn(this, "Changed distributed server configuration:\n%s",
					replicator.getClusterConfiguration().toJSON(""));

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
	}
}
