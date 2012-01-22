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
import java.util.Collection;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.db.tool.ODatabaseImport;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.exception.OSecurityException;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryInputStream;
import com.orientechnologies.orient.enterprise.channel.distributed.OChannelDistributedProtocol;
import com.orientechnologies.orient.server.OServerMain;
import com.orientechnologies.orient.server.handler.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.network.protocol.binary.ONetworkProtocolBinary;
import com.orientechnologies.orient.server.replication.ODistributedDatabaseInfo;
import com.orientechnologies.orient.server.replication.ODistributedNode;
import com.orientechnologies.orient.server.replication.ODistributedStorage;
import com.orientechnologies.orient.server.replication.OOperationLog;

/**
 * Extends binary protocol to include cluster commands.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class ONetworkProtocolDistributed extends ONetworkProtocolBinary implements OCommandOutputListener {
	private ODistributedServerManager	manager;
	private String										clientId;

	public ONetworkProtocolDistributed() {
		super("OrientDB DistributedBinaryNetworkProtocolListener");

		manager = OServerMain.server().getHandler(ODistributedServerManager.class);
		if (manager == null)
			throw new OConfigurationException(
					"Cannot find a ODistributedServerDiscoveryManager instance registered as handler. Check the server configuration in the handlers section.");
	}

	@Override
	protected void parseCommand() throws IOException, InterruptedException {

		if (clientId == null && connection != null && connection.data.clientId != null
				&& connection.data.clientId.startsWith(ODistributedStorage.DNODE_PREFIX))
			// ASSIGN CLIENT-ID ONCE
			clientId = connection.data.clientId.substring(ODistributedStorage.DNODE_PREFIX.length());

		// DISTRIBUTED SERVER REQUESTS
		switch (lastRequestType) {

		case OChannelDistributedProtocol.REQUEST_DISTRIBUTED_SYNCHRONIZE: {
			connection.data.commandInfo = "Synchronization between nodes";
			final ODocument cfg = new ODocument(channel.readBytes());

			sendOk(lastClientTxId);

			final ORecordOperation op = new ORecordOperation();

			// BROWSE ALL THE NODES
			Collection<ODocument> nodes = cfg.field("nodes");
			for (ODocument nodeCfg : nodes) {
				final String node = nodeCfg.field("node");
				final long lastLog = (Long) nodeCfg.field("lastLog");

				final String dbName = connection.database.getName();
				OLogManager.instance().info(this,
						"<-> DB %s: received synchronization request from node %s reading operation logs after %d", dbName, clientId, lastLog);

				// channel.
				final OOperationLog opLog = manager.getReplicator().getOperationLog(node, dbName);

				if (opLog != null) {
					// SEND NODE LOGS
					channel.writeByte((byte) 1);
					channel.writeString(node);

					// SEND LOG DELTA
					int position = opLog.findOperationId(lastLog);
					int sent = 0;

					sendOk(lastClientTxId);

					for (int i = position - 1; i >= 0; --i) {
						channel.writeByte((byte) 1);
						opLog.getEntry(i, op);

						channel.writeBytes(op.toStream());
						sent++;

						OLogManager.instance().info(this, ">> %s: (%d) operation %d with RID %s", dbName, sent, op.serial,
								op.record.getIdentity());
					}

					// END OF OPERATIONS PER NODE
					channel.writeByte((byte) 0);
				}
			}
			// END OF NODES
			channel.writeByte((byte) 0);

			break;
		}

		case OChannelDistributedProtocol.REQUEST_DISTRIBUTED_LEADER_CONNECT: {
			connection.data.commandInfo = "Clustered connection from leader";
			final ODocument doc = new ODocument().fromStream(channel.readBytes());
			final String clusterName = doc.field("clusterName");
			final byte[] encodedSecurityKey = doc.field("clusterKey");
			final String leaderAddress = doc.field("leaderNodeAddress");

			if (!clusterName.equals(manager.getName()) || !Arrays.equals(encodedSecurityKey, manager.getConfig().getSecurityKey()))
				throw new OSecurityException("Invalid combination of cluster name and key received");

			// final long leaderNodeRunningSince = doc.field("leaderNodeRunningSince");

			boolean remainTheLeader = false;

			channel.acquireExclusiveLock();
			try {
				sendOk(lastClientTxId);
				channel.writeInt(connection.id);

				if (manager.isLeader()) {
					OLogManager
							.instance()
							.warn(
									this,
									"Received remote connection from the leader node %s, but current node is itself leader: split network problem or high network latency?",
									leaderAddress);

					// CHECK WHAT LEADER WINS
					final String myUid = InetAddress.getLocalHost().getHostAddress() + ":" + channel.socket.getLocalPort();

					if (leaderAddress.compareTo(myUid) > 0) {
						// BY CONVENTION THE LOWER VALUE WINS AND REMAIN LEADER
						// THIS NODE IS OLDER: WIN! REFUSE THE CONNECTION
						remainTheLeader = true;
						OLogManager.instance().warn(this,
								"Current node remains the Leader of the cluster because it has lower network address", leaderAddress);
					}
				}

				channel.writeByte((byte) (remainTheLeader ? 0 : 1));

				if (!remainTheLeader) {
					// OK TO BE A PEER
					final ODocument localCfg = manager.getReplicator().getLocalDatabaseConfiguration();
					channel.writeBytes(localCfg.toStream());
				}
			} finally {
				channel.releaseExclusiveLock();
				channel.flush();
			}

			manager.getReplicator().updateConfiguration(new ODocument(channel.readBytes()));

			manager.becomePeer(this);

			if (remainTheLeader)
				// ABORT THE CONNECTION & THREAD
				shutdown();

			break;
		}

		case OChannelDistributedProtocol.REQUEST_DISTRIBUTED_HEARTBEAT:
			checkConnected();
			connection.data.commandInfo = "Cluster Heartbeat";
			manager.updateHeartBeatTime();

			channel.acquireExclusiveLock();
			try {
				sendOk(lastClientTxId);
			} finally {
				channel.releaseExclusiveLock();
			}
			break;

		case OChannelDistributedProtocol.REQUEST_DISTRIBUTED_RECORD_CHANGE: {
			connection.data.commandInfo = "Distributed record change";

			final byte operationType = channel.readByte();
			final long operationId = channel.readLong(); // USE THIS FOR LOGGING

			final ORecordId rid = channel.readRID();
			final byte[] buffer = channel.readBytes();
			final int version = channel.readInt();
			final byte recordType = channel.readByte();

			final long result;

			// REPLICATION SOURCE: AVOID LOOP
			ODistributedRequesterThreadLocal.INSTANCE.set(true);
			try {

				switch (operationType) {
				case ORecordOperation.CREATED:
					result = createRecord(rid, buffer, recordType);
					break;

				case ORecordOperation.UPDATED:
					result = updateRecord(rid, buffer, version, recordType);
					break;

				case ORecordOperation.DELETED:
					result = deleteRecord(rid, version);
					break;

				default:
					throw new IllegalArgumentException("Received invalid distributed record change operation type: " + operationType);
				}
			} finally {
				ODistributedRequesterThreadLocal.INSTANCE.set(false);
			}

			// LOGS THE CHANGE
			final ODistributedNode node = manager.getReplicator().getNode(clientId);
			final ODistributedDatabaseInfo db = node.getDatabase(connection.database.getName());
			db.log.appendLog(operationId, operationType, rid);

			channel.acquireExclusiveLock();
			try {
				sendOk(lastClientTxId);
				channel.writeLong(result);
			} finally {
				channel.releaseExclusiveLock();
			}
			break;
		}

		case OChannelDistributedProtocol.REQUEST_DISTRIBUTED_DB_SHARE_SENDER: {
			connection.data.commandInfo = "Share the database to a remote server";

			final String dbUrl = channel.readString();
			final String dbUser = channel.readString();
			final String dbPassword = channel.readString();
			final String remoteServerName = channel.readString();
			final String remoteServerEngine = channel.readString();

			checkServerAccess("database.share");

			openDatabase(dbUrl, dbUser, dbPassword);

			final ODistributedNode node = manager.getReplicator().getOrCreateDistributedNode(remoteServerName);

			final ODistributedDatabaseInfo db = node.shareDatabase(connection.database, remoteServerEngine, manager.getReplicator()
					.getReplicatorUser().name, manager.getReplicator().getReplicatorUser().password);

			channel.acquireExclusiveLock();
			try {
				sendOk(lastClientTxId);
			} finally {
				channel.releaseExclusiveLock();
			}

			manager.getPeer().updateConfigurationToLeader();

			break;
		}

		case OChannelDistributedProtocol.REQUEST_DISTRIBUTED_DB_SHARE_RECEIVER: {
			checkConnected();
			connection.data.commandInfo = "Received a shared database from a remote server to install";

			final String dbName = channel.readString();
			final String dbUser = channel.readString();
			final String dbPasswd = channel.readString();
			final String engineName = channel.readString();

			try {
				OLogManager.instance().info(this, "<-> DB %s: importing database...", dbName);

				connection.database = getDatabaseInstance(dbName, engineName);

				if (connection.database.exists()) {
					OLogManager.instance().info(this, "<-> DB %s: deleting existent database...", connection.database.getName());
					connection.database.delete();
				}

				createDatabase(connection.database, dbUser, dbPasswd);

				if (connection.database.isClosed())
					connection.database.open(dbUser, dbPasswd);

				OLogManager.instance().info(this, "<-> DB %s: reading database content via streaming from remote server node...", dbName);

				channel.acquireExclusiveLock();
				try {
					new ODatabaseImport(connection.database, new OChannelBinaryInputStream(channel), this).importDatabase();

					OLogManager.instance().info(this, "<-> DB %s: database imported correctly", dbName);

					sendOk(lastClientTxId);
					channel.writeInt(connection.id);
					channel.flush();
				} finally {
					channel.releaseExclusiveLock();
				}

			} finally {
				manager.getPeer().updateHeartBeatTime();
			}

			manager.getPeer().updateConfigurationToLeader();

			break;
		}

		case OChannelDistributedProtocol.REQUEST_DISTRIBUTED_DB_CONFIG: {
			checkConnected();
			connection.data.commandInfo = "Update db configuration from server node leader";

			final ODocument cfg = new ODocument().fromStream(channel.readBytes());
			manager.getReplicator().updateConfiguration(cfg);

			OLogManager.instance().warn(this, "Cluster <%s>: changed distributed server configuration:\n%s", manager.getConfig().name,
					cfg.toJSON(""));

			for (String dbName : cfg.fieldNames())
				manager.sendClusterConfigurationToClients(dbName, cfg);

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

	public String getType() {
		return "distributed";
	}

	protected void checkConnected() {
	}
}
