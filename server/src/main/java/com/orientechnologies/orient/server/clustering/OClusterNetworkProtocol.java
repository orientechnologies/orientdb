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
package com.orientechnologies.orient.server.clustering;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.db.tool.ODatabaseImport;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.exception.OSecurityException;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryInputStream;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.OServerMain;
import com.orientechnologies.orient.server.handler.distributed.OClusterProtocol;
import com.orientechnologies.orient.server.handler.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.network.protocol.binary.OBinaryNetworkProtocolAbstract;
import com.orientechnologies.orient.server.network.protocol.distributed.ODistributedRequesterThreadLocal;
import com.orientechnologies.orient.server.replication.ODistributedDatabaseInfo;
import com.orientechnologies.orient.server.replication.ODistributedDatabaseInfo.SYNCH_TYPE;
import com.orientechnologies.orient.server.replication.ODistributedNode;
import com.orientechnologies.orient.server.replication.OOperationLog;
import com.orientechnologies.orient.server.replication.conflict.OReplicationConflictException;

/**
 * Extends binary protocol to include cluster commands.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OClusterNetworkProtocol extends OBinaryNetworkProtocolAbstract implements OCommandOutputListener {
	private ODistributedServerManager						manager;
	private String															remoteNodeId;
	private String															commandInfo;
	private final Map<String, ODatabaseRecord>	databases	= new HashMap<String, ODatabaseRecord>(5);

	public OClusterNetworkProtocol() {
		super("OrientDB <- Node/?");
		manager = OServerMain.server().getHandler(ODistributedServerManager.class);
		if (manager == null)
			throw new OConfigurationException(
					"Cannot find a ODistributedServerDiscoveryManager instance registered as handler. Check the server configuration in the handlers section.");
	}

	@Override
	public void config(OServer iServer, Socket iSocket, OContextConfiguration iConfig) throws IOException {
		super.config(iServer, iSocket, iConfig);

		// SEND PROTOCOL VERSION
		channel.writeShort((short) OClusterProtocol.CURRENT_PROTOCOL_VERSION);
		channel.flush();
		start();
	}

	@Override
	protected boolean executeRequest() throws IOException {

		// DISTRIBUTED SERVER REQUESTS
		switch (requestType) {

		case OClusterProtocol.REQUEST_NODE2NODE_CONNECT: {
			commandInfo = "Connection from node";

			remoteNodeId = channel.readString();

			if (OLogManager.instance().isDebugEnabled())
				OLogManager.instance().debug(this, "REPL NODE <%s> connected, authenticating it...", remoteNodeId);

			setName("OrientDB <- Node/" + remoteNodeId);

			// AUTHENTICATE
			final String userName = channel.readString();
			serverLogin(userName, channel.readString(), "connect");

			if (OLogManager.instance().isDebugEnabled())
				OLogManager.instance().debug(this, "REPL NODE <%s> authenticated correctly with user '%s'", remoteNodeId, userName);

			beginResponse();
			try {
				sendOk(clientTxId);
			} finally {
				endResponse();
			}
			break;
		}

		case OClusterProtocol.REQUEST_LEADER2PEER_CONNECT: {
			commandInfo = "Connection from leader";
			final ODocument doc = new ODocument().fromStream(channel.readBytes());
			final String clusterName = doc.field("clusterName");
			final byte[] encodedSecurityKey = doc.field("clusterKey");
			final String leaderAddress = doc.field("leaderNodeAddress");

			if (!clusterName.equals(manager.getName()) || !Arrays.equals(encodedSecurityKey, manager.getConfig().getSecurityKey()))
				throw new OSecurityException("Invalid combination of cluster name and key received");

			// final long leaderNodeRunningSince = doc.field("leaderNodeRunningSince");

			boolean remainTheLeader = false;

			beginResponse();
			try {
				sendOk(clientTxId);

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
					// OK TO BE A PEER, SEND DB CFG TO THE LEADER
					final ODocument localCfg = manager.getReplicator().getLocalDatabaseConfiguration();
					channel.writeBytes(localCfg.toStream());
				}
			} finally {
				endResponse();
			}

			if (remainTheLeader)
				// ABORT THE CONNECTION & THREAD
				sendShutdown();
			else {
				// OK TO BE A PEER
				setName("OrientDB <- Distributed Leader");
				manager.becomePeer(this);
				manager.getReplicator().updateConfiguration(new ODocument(channel.readBytes()));
			}

			break;
		}

		case OClusterProtocol.REQUEST_LEADER2PEER_HEARTBEAT:
			checkConnected();
			commandInfo = "Cluster Heartbeat";

			final long lastInterval = manager.updateHeartBeatTime();

			if (OLogManager.instance().isDebugEnabled())
				OLogManager.instance().debug(this, "Received heartbeat message from leader. Last interval was " + lastInterval + "ms");

			beginResponse();
			try {
				sendOk(clientTxId);
			} finally {
				endResponse();
			}
			break;

		case OClusterProtocol.REQUEST_NODE2NODE_REPLICATION_SYNCHRONIZE: {
			commandInfo = "Synchronization between nodes";
			final String dbName = channel.readString();
			final ODocument cfg = new ODocument(channel.readBytes());

			if (OLogManager.instance().isInfoEnabled())
				OLogManager.instance().info(this, "REPL DB <-(%s) received synchronization request from node %s...", dbName, remoteNodeId);

			if (!databases.containsKey(dbName)) {
				// OPEN THE DB FOR THE FIRST TIME
				final ODatabaseDocumentTx db = (ODatabaseDocumentTx) openDatabase(ODatabaseDocumentTx.TYPE, dbName, serverUser.name,
						serverUser.password);
				databases.put(dbName, db);
			}

			beginResponse();
			try {
				sendOk(clientTxId);

				final ORecordOperation op = new ORecordOperation();

				// SYNCHRONIZE ALL THE NODES
				Collection<ODocument> nodes = cfg.field("nodes");

				// SEND TOTAL OF NODES
				channel.writeInt(nodes.size());

				for (ODocument nodeCfg : nodes) {
					final String node = nodeCfg.field("node");
					channel.writeString(node);
					final long lastLog = (Long) nodeCfg.field("lastLog");

					OLogManager.instance()
							.info(this, "REPL DB <-(%s) reading operation logs from %s after %d", dbName, remoteNodeId, lastLog);

					// channel.
					final OOperationLog opLog = manager.getReplicator().getOperationLog(node, dbName);

					if (opLog != null) {
						// SEND NODE LOGS

						// SEND LOG DELTA
						int position = opLog.findOperationId(lastLog);
						int sent = 0;

						// SEND TOTAL OF LOG ENTRIES
						channel.writeInt(position);

						for (int i = position - 1; i >= 0; --i) {
							opLog.getEntry(i, op);

							channel.writeBytes(op.toStream());
							sent++;

							OLogManager.instance().info(this, "REPL DB <-(%s) #%d operation %d with RID %s", dbName, sent, op.serial,
									op.record.getIdentity());
						}
					} else
						channel.writeInt(0);
				}
			} finally {
				endResponse();
			}

			OLogManager.instance().info(this, "REPL DB (%s)-> synchronization completed from node %s, starting inverse replication...",
					dbName, remoteNodeId);

			// START REPLICATION BACK
			manager.getReplicator().startReplication(dbName, remoteNodeId, SYNCH_TYPE.ASYNCH.toString());

			OLogManager.instance().info(this, "REPL DB <-(%s) reverse synchronization completed to node %s", dbName, remoteNodeId);

			break;
		}

		case OClusterProtocol.REQUEST_NODE2NODE_REPLICATION_RECORD_CHANGE: {
			commandInfo = "Distributed record change";

			final String dbName = channel.readString();
			final byte operationType = channel.readByte();
			final long operationId = channel.readLong(); // USE THIS FOR LOGGING

			final ORecordId rid = channel.readRID();
			final byte[] buffer = channel.readBytes();
			final int version = channel.readInt();
			final byte recordType = channel.readByte();

			OLogManager.instance().info(this, "REPL DB <-(%s) %s record %s from %s...", dbName, ORecordOperation.getName(operationType),
					rid, remoteNodeId);

			final long result;

			ODatabaseRecord database = databases.get(dbName);

			// REPLICATION SOURCE: AVOID LOOP
			ODistributedRequesterThreadLocal.INSTANCE.set(true);
			try {

				switch (operationType) {
				case ORecordOperation.CREATED:
					long origClusterPosition = rid.clusterPosition;
					rid.clusterPosition = -1;
					result = createRecord(database, rid, buffer, recordType);
					if (result != origClusterPosition)
						throw new OReplicationConflictException("Record created has RID different by the original: original " + rid.clusterId
								+ ":" + origClusterPosition + ", local " + rid.clusterId + ":" + result);
					break;

				case ORecordOperation.UPDATED:
					result = updateRecord(database, rid, buffer, version, recordType);
					break;

				case ORecordOperation.DELETED:
					result = deleteRecord(database, rid, version);
					break;

				default:
					throw new IllegalArgumentException("Received invalid distributed record change operation type: " + operationType);
				}
			} finally {
				ODistributedRequesterThreadLocal.INSTANCE.set(false);
			}

			// LOGS THE CHANGE
			final ODistributedNode node = manager.getReplicator().getNode(remoteNodeId);
			final ODistributedDatabaseInfo db = node.getDatabase(database.getName());
			db.log.appendLog(operationId, operationType, rid);

			beginResponse();
			try {
				sendOk(clientTxId);
				channel.writeLong(result);
			} finally {
				endResponse();
			}
			break;
		}

		case OClusterProtocol.REQUEST_NODE2NODE_DB_COPY: {
			checkConnected();
			commandInfo = "Importing a database from a remote node";

			final String dbName = channel.readString();
			final String dbUser = channel.readString();
			final String dbPasswd = channel.readString();
			final String dbType = channel.readString();
			final String engineType = channel.readString();

			try {
				OLogManager.instance().info(this, "REPL <%s> importing database...", dbName);

				ODatabaseDocumentTx database = getDatabaseInstance(dbName, dbType, engineType);

				if (database.exists()) {
					OLogManager.instance().info(this, "REPL <%s> deleting existent database...", database.getName());
					database.drop();
				}

				database = createDatabase(database, dbUser, dbPasswd);

				if (database.isClosed())
					database.open(dbUser, dbPasswd);

				OLogManager.instance().info(this, "REPL <%s> reading database content via streaming from remote server node...", dbName);

				beginResponse();
				try {
					new ODatabaseImport(database, new OChannelBinaryInputStream(channel), this).importDatabase();

					OLogManager.instance().info(this, "REPL <%s> database imported correctly", dbName);

					sendOk(clientTxId);
				} finally {
					endResponse();
				}

			} finally {
				manager.getPeer().updateHeartBeatTime();
			}

			manager.getPeer().updateConfigurationToLeader();

			break;
		}

		default:
			return false;
		}

		return true;
	}

	private void beginResponse() {
		channel.acquireExclusiveLock();
	}

	private void endResponse() throws IOException {
		channel.flush();
		channel.releaseExclusiveLock();
	}

	@Override
	public void onMessage(String iText) {
	}

	public String getType() {
		return "cluster";
	}

	protected void checkConnected() {
	}
}
