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
import java.util.concurrent.FutureTask;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.client.remote.OStorageRemote;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryClient;
import com.orientechnologies.orient.enterprise.channel.distributed.OChannelDistributedProtocol;
import com.orientechnologies.orient.server.replication.ODistributedDatabaseInfo.SYNCH_TYPE;
import com.orientechnologies.orient.server.replication.conflict.OReplicationConflictResolver;

/**
 * Distributed version of remote storage
 */
public class ODistributedStorage extends OStorageRemote {

	public static final String									DNODE_PREFIX	= "dnode=";
	private final OReplicator										replicator;
	private final OReplicationConflictResolver	conflictResolver;

	public ODistributedStorage(final OReplicator iReplicator, final String iNodeId, final String iURL, final String iMode,
			final OReplicationConflictResolver iConflictResolver) throws IOException {
		super(DNODE_PREFIX + iNodeId, iURL, iMode);
		replicator = iReplicator;
		conflictResolver = iConflictResolver;
	}

	public void synchronize(final Set<ODocument> iDbCfg) {
		checkConnection();

		final long time = System.currentTimeMillis();

		OLogManager.instance().info(this, "Starting synchronization of database '%s'. Storing delta of updates...", getName());

		try {
			final OChannelBinaryClient network = beginRequest(OChannelDistributedProtocol.REQUEST_DISTRIBUTED_SYNCHRONIZE);

			for (ODocument node : iDbCfg) {
				final String nodeId = (String) node.field("node");
				try {
					network.writeBytes(node.toStream());
				} finally {
					endRequest(network);
				}

				beginResponse(network);
				try {
					int ops = 0;
					final ORecordOperation opLog = new ORecordOperation();
					while (network.readByte() == 1) {
						opLog.fromStream(network.readBytes());
						ops++;

						OLogManager.instance().info(this, "%d: received record %s", ops, opLog.record);

						replicator.getOperationLog(nodeId, getName()).appendLog(opLog.serial, opLog.type,
								(ORecordId) opLog.record.getIdentity());
					}

					if (OLogManager.instance().isInfoEnabled())
						OLogManager.instance().info(this,
								"Synchronization of database '%s' completed: received %d operations from remote node. Total time: %dms", getName(),
								ops, (System.currentTimeMillis() - time));

				} finally {
					endResponse(network);
				}
			}

		} catch (OException e) {
			// PASS THROUGH
			throw e;
		} catch (Exception e) {
			handleException("Error on synchronization of database '" + getName() + "'", e);
		}
	}

	public void distributeChange(final ODistributedDatabaseInfo databaseEntry, final ORecordOperation iRequest,
			final SYNCH_TYPE iRequestType, final ORecordInternal<?> iRecord) {

		if (OLogManager.instance().isWarnEnabled()) {
			String operation = "?";
			switch (iRequest.type) {
			case ORecordOperation.CREATED:
				operation = "CREATE";
				break;
			case ORecordOperation.UPDATED:
				operation = "UPDATE";
				break;
			case ORecordOperation.DELETED:
				operation = "DELETE";
				break;
			}

			OLogManager.instance().warn(this, "-> %s (%s mode) %s record %s...", this, iRequestType, operation, iRecord.getIdentity());
		}

		checkConnection();

		do {
			try {
				final OChannelBinaryClient network = beginRequest(OChannelDistributedProtocol.REQUEST_DISTRIBUTED_RECORD_CHANGE);
				try {
					network.writeByte(iRequest.type);
					network.writeLong(0); // OPERATION ID
					network.writeRID(iRecord.getIdentity());
					network.writeBytes(iRecord.toStream());
					network.writeInt(iRecord.getVersion() - 1);
					network.writeByte(iRecord.getRecordType());

				} finally {
					endRequest(network);
				}

				if (iRequestType == SYNCH_TYPE.SYNCH)
					try {
						beginResponse(network);
						handleRemoteResponse(iRequest.type, iRequestType, iRecord, network.readLong());
					} finally {
						endResponse(network);
					}
				else {
					Callable<Object> response = new Callable<Object>() {
						public Object call() throws Exception {
							beginResponse(network);
							try {
								handleRemoteResponse(iRequest.type, iRequestType, iRecord, network.readLong());
							} finally {
								endResponse(network);
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
				handleException("Error on distribute record: " + iRecord.getIdentity(), e);

			}
		} while (true);
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
}
