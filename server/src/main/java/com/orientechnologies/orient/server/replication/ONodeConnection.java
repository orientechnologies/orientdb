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
package com.orientechnologies.orient.server.replication;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.logging.Level;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.io.OIOException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.db.tool.ODatabaseExport;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.enterprise.channel.binary.OAsynchChannelServiceThread;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryClient;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryOutputStream;
import com.orientechnologies.orient.server.clustering.OClusterLogger.DIRECTION;
import com.orientechnologies.orient.server.clustering.OClusterLogger.TYPE;
import com.orientechnologies.orient.server.clustering.leader.ORemoteNodeAbstract;
import com.orientechnologies.orient.server.handler.distributed.OClusterProtocol;
import com.orientechnologies.orient.server.replication.ODistributedDatabaseInfo.SYNCH_TYPE;
import com.orientechnologies.orient.server.replication.conflict.OReplicationConflictResolver;

/**
 * Distributed version of remote storage
 */
public class ONodeConnection extends ORemoteNodeAbstract implements OCommandOutputListener {

	private final OReplicationConflictResolver	conflictResolver;
	protected final ExecutorService							asynchExecutor;

	public ONodeConnection(final OReplicator iReplicator, final String iNodeId, final OReplicationConflictResolver iConflictResolver)
			throws IOException {
		super(iNodeId.split(":")[0], Integer.parseInt(iNodeId.split(":")[1]));

		logger.setNode(iNodeId);
		logger.log(this, Level.INFO, TYPE.REPLICATION, DIRECTION.OUT, "connecting...", iNodeId);

		channel = new OChannelBinaryClient(networkAddress, networkPort, new OContextConfiguration(),
				OClusterProtocol.CURRENT_PROTOCOL_VERSION);

		beginRequest(OClusterProtocol.REQUEST_NODE2NODE_CONNECT);

		try {
			// CONNECT TO THE SERVER
			channel.writeString(iReplicator.getManager().getId());
			channel.writeString(iReplicator.getReplicatorUser().name);
			channel.writeString(iReplicator.getReplicatorUser().password);
		} finally {
			endRequest();
		}

		try {
			beginResponse();
		} finally {
			endResponse();
		}

		logger.log(this, Level.INFO, TYPE.REPLICATION, DIRECTION.OUT, "connected");

		serviceThread = new OAsynchChannelServiceThread(new ODistributedRemoteAsynchEventListener(iReplicator.getManager(),
				new ODistributedRemoteAsynchEventListener(iReplicator.getManager(), null, iNodeId), iNodeId), channel,
				"OrientDB <- Asynch Node/" + iNodeId);

		conflictResolver = iConflictResolver;
		asynchExecutor = Executors.newSingleThreadExecutor();
	}

	public void synchronize(final String iDatabaseName, final Set<ODocument> iDbCfg) {
		logger.setDatabase(iDatabaseName);
		logger.log(this, Level.INFO, TYPE.REPLICATION, DIRECTION.IN, "synchronization started. Storing delta of updates...");

		try {
			ODocument cfg = new ODocument().field("nodes", iDbCfg, OType.EMBEDDEDSET);

			// SEND CURRENT CONFIGURATION FOR CURRENT DATABASE
			final OChannelBinaryClient network = beginRequest(OClusterProtocol.REQUEST_NODE2NODE_REPLICATION_SYNCHRONIZE);

			try {
				network.writeString(iDatabaseName);
				network.writeBytes(cfg.toStream());
				network.flush();
			} finally {
				endRequest();
			}

			beginResponse();
			try {
			} finally {
				endResponse();
			}

		} catch (OException e) {
			// PASS THROUGH
			throw e;
		} catch (Exception e) {
			throw new OIOException("REPL DB (" + iDatabaseName + ") error on synchronization", e);
		}
	}

	public void distributeChange(final ODistributedDatabaseInfo databaseEntry, final ORecordOperation iRequest,
			final SYNCH_TYPE iRequestType, final ORecordInternal<?> iRecord) {

		logger.setNode(databaseEntry.serverId);
		logger.setDatabase(databaseEntry.databaseName);

		if (OLogManager.instance().isInfoEnabled())
			logger.log(this, Level.INFO, TYPE.REPLICATION, DIRECTION.OUT, "%s record %s in %s mode",
					ORecordOperation.getName(iRequest.type), iRecord.getIdentity(), iRequestType);

		do {
			try {
				final OChannelBinaryClient network = beginRequest(OClusterProtocol.REQUEST_NODE2NODE_REPLICATION_RECORD_CHANGE);
				try {
					network.writeString(databaseEntry.databaseName);
					network.writeByte(iRequest.type);
					network.writeLong(iRequest.serial); // OPERATION ID
					network.writeRID(iRecord.getIdentity());
					network.writeBytes(iRecord.toStream());
					network.writeInt(iRecord.getVersion() - 1);
					network.writeByte(iRecord.getRecordType());

				} finally {
					endRequest();
				}

				if (iRequestType == SYNCH_TYPE.SYNCH)
					try {
						beginResponse();
						handleRemoteResponse(iRequest.type, iRequestType, iRecord, network.readLong());
					} finally {
						endResponse();
					}
				else {
					Callable<Object> response = new Callable<Object>() {
						public Object call() throws Exception {
							beginResponse();
							try {
								handleRemoteResponse(iRequest.type, iRequestType, iRecord, network.readLong());
							} finally {
								endResponse();
							}
							return null;
						}

					};
					asynchExecutor.submit(new FutureTask<Object>(response));
				}
				return;
			} catch (OConcurrentModificationException e) {
				conflictResolver.handleUpdateConflict(iRequest.type, iRequestType, iRecord, e.getRecordVersion(), e.getDatabaseVersion());
				return;
			} catch (ODatabaseException e) {
				conflictResolver.handleUpdateConflict(iRequest.type, iRequestType, iRecord, iRecord.getVersion(), -1);
				return;
			} catch (OException e) {
				// PASS THROUGH
				throw e;
			} catch (Exception e) {
				throw new OIOException("REPL <" + databaseEntry.databaseName + "> error on distribute record: " + iRecord.getIdentity(), e);

			}
		} while (true);
	}

	public void copy(final ODatabaseRecord iDatabase, final String dbName, final String iDbUser, final String iDbPasswd,
			final String iEngineName) throws IOException {
		checkConnection();

		final OChannelBinaryClient network = beginRequest(OClusterProtocol.REQUEST_NODE2NODE_DB_COPY);

		try {
			network.writeString(dbName);
			network.writeString(iDbUser);
			network.writeString(iDbPasswd);
			network.writeString(iEngineName);

			// START THE EXPORT GIVING AS OUTPUTSTREAM THE CHANNEL TO STREAM THE EXPORT
			new ODatabaseExport(iDatabase, new OChannelBinaryOutputStream(network), this).exportDatabase();

		} finally {
			endRequest();
		}

		try {
			beginResponse();
		} finally {
			endResponse();
		}

		disconnect();
	}

	private void handleRemoteResponse(final byte iOperation, final SYNCH_TYPE iRequestType, final ORecordInternal<?> iRecord,
			final long iResponse) {

		switch (iOperation) {
		case ORecordOperation.CREATED:
			if (iResponse != iRecord.getIdentity().getClusterPosition())
				conflictResolver.handleCreateConflict(ORecordOperation.CREATED, iRequestType, iRecord, iResponse);
			break;
		case ORecordOperation.UPDATED:
			if ((int) iResponse != iRecord.getVersion())
				conflictResolver.handleUpdateConflict(ORecordOperation.UPDATED, iRequestType, iRecord, iRecord.getVersion(),
						(int) iResponse);
			break;
		case ORecordOperation.DELETED:
			if ((int) iResponse == 0)
				conflictResolver.handleDeleteConflict(ORecordOperation.DELETED, iRequestType, iRecord);
			break;
		}
	}

	public void onMessage(final String iText) {
	}
}
