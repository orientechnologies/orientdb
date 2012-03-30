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
import java.util.logging.Level;

import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.db.tool.ODatabaseImport;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.exception.OSecurityException;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryInputStream;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.OServerMain;
import com.orientechnologies.orient.server.clustering.OClusterLogger.DIRECTION;
import com.orientechnologies.orient.server.clustering.OClusterLogger.TYPE;
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
	private OClusterLogger											logger		= new OClusterLogger();
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

			logger.setNode(remoteNodeId);
			logger.log(this, Level.FINE, TYPE.REPLICATION, DIRECTION.IN, "connected, authenticating it...");

			setName("OrientDB <- Node/" + remoteNodeId);

			// AUTHENTICATE
			final String userName = channel.readString();
			serverLogin(userName, channel.readString(), "connect");

			logger.log(this, Level.FINE, TYPE.REPLICATION, DIRECTION.IN, "authenticated correctly with user '%s'", userName);

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

			logger.setNode(leaderAddress);

			if (!clusterName.equals(manager.getName()) || !Arrays.equals(encodedSecurityKey, manager.getConfig().getSecurityKey()))
				throw new OSecurityException("Invalid combination of cluster name and key received");

			// final long leaderNodeRunningSince = doc.field("leaderNodeRunningSince");

			boolean remainTheLeader = false;

			beginResponse();
			try {
				sendOk(clientTxId);

				if (manager.isLeader()) {
					logger
							.log(this, Level.WARNING, TYPE.CLUSTER, DIRECTION.IN,
									"received remote connection from the leader, but current node is itself leader: split network problem or high network latency?");

					// CHECK WHAT LEADER WINS
					final String myUid = InetAddress.getLocalHost().getHostAddress() + ":" + channel.socket.getLocalPort();

					if (leaderAddress.compareTo(myUid) > 0) {
						// BY CONVENTION THE LOWER VALUE WINS AND REMAIN LEADER
						// THIS NODE IS OLDER: WIN! REFUSE THE CONNECTION
						remainTheLeader = true;

						logger.log(this, Level.WARNING, TYPE.CLUSTER, DIRECTION.NONE,
								"current node remains the Leader of the cluster because it has lower network address");
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

			logger.log(this, Level.FINE, TYPE.CLUSTER, DIRECTION.IN, "heartbeat. Last msg was %dms ago", lastInterval);

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

			logger.setDatabase(dbName);
			logger.log(this, Level.INFO, TYPE.REPLICATION, DIRECTION.IN, "received synchronization request");

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

					logger
							.log(this, Level.INFO, TYPE.REPLICATION, DIRECTION.IN, "reading operation for node %s logs after %d", node, lastLog);

					// channel.
					final OOperationLog opLog = manager.getReplicator().getOperationLog(node, dbName);

					if (opLog != null) {
						int sent = 0;

						// SEND LOG DELTA
						int position = opLog.findOperationId(lastLog);
						// SEND TOTAL OF LOG ENTRIES
						final int totalToSend = opLog.totalEntries();
						channel.writeInt(totalToSend - position);

						for (int i = position; i < totalToSend; ++i) {
							opLog.getEntry(i, op);

							try {
								channel.writeBytes(op.toStream());
								sent++;

								logger.log(this, Level.INFO, TYPE.REPLICATION, DIRECTION.IN, "#%d operation %d with RID %s", sent, op.serial,
										op.record.getIdentity());

							} catch (OSerializationException e) {

								logger.log(this, Level.SEVERE, TYPE.REPLICATION, DIRECTION.OUT,
										"#%d cannot be transmitted, log entry %d, record %s: ", sent, op.serial, op.record.getIdentity(), e.getCause());
								// TRANSMIT EMPTY RECORD TO CONTINUE WITHOUT ERRORS
								channel.writeBytes(null);

							} catch (RuntimeException e) {
								logger.log(this, Level.SEVERE, TYPE.REPLICATION, DIRECTION.OUT,
										"#%d cannot be transmitted, log entry %d, record %s", e, sent, op.serial, op.record.getIdentity());
								throw e;
							}
						}
					} else
						channel.writeInt(0);
				}
			} finally {
				endResponse();
			}

			logger.log(this, Level.INFO, TYPE.REPLICATION, DIRECTION.OUT, "synchronization completed, starting inverse replication...");

			// START REPLICATION BACK
			manager.getReplicator().startReplication(dbName, remoteNodeId, SYNCH_TYPE.ASYNCH.toString());

			logger.log(this, Level.INFO, TYPE.REPLICATION, DIRECTION.OUT, "reverse synchronization completed");

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

			logger.setNode(dbName);
			logger.log(this, Level.INFO, TYPE.REPLICATION, DIRECTION.IN, "%s record %s...", ORecordOperation.getName(operationType), rid);

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
					rid.clusterPosition = result;
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
				logger.setNode(dbName);
				logger.log(this, Level.INFO, TYPE.REPLICATION, DIRECTION.IN, "importing database...");

				ODatabaseDocumentTx database = getDatabaseInstance(dbName, dbType, engineType);

				if (database.exists()) {
					logger.log(this, Level.INFO, TYPE.REPLICATION, DIRECTION.NONE, "deleting existent database...", database.getName());
					database.drop();
				}

				database = createDatabase(database, dbUser, dbPasswd);

				if (database.isClosed())
					database.open(dbUser, dbPasswd);

				logger.log(this, Level.INFO, TYPE.REPLICATION, DIRECTION.IN,
						"reading database content via streaming from remote server node...");

				beginResponse();
				try {
					new ODatabaseImport(database, new OChannelBinaryInputStream(channel), this).importDatabase();

					logger.log(this, Level.INFO, TYPE.REPLICATION, DIRECTION.IN, "database imported correctly");

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
