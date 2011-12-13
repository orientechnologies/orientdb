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
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.client.remote.OStorageRemote;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.tx.OTransactionRecordEntry;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryClient;
import com.orientechnologies.orient.enterprise.channel.distributed.OChannelDistributedProtocol;
import com.orientechnologies.orient.server.replication.ODistributedDatabaseInfo.SYNCH_TYPE;
import com.orientechnologies.orient.server.replication.conflict.ODistributedConflictResolver;

/**
 * Distributed version of remote storage
 */
public class ODistributedStorage extends OStorageRemote {

	private final ODistributedConflictResolver	conflictResolver;

	public ODistributedStorage(final String iURL, final String iMode, final ODistributedConflictResolver iConflictResolver)
			throws IOException {
		super(iURL, iMode);
		conflictResolver = iConflictResolver;
	}

	public void distributeChange(final ODistributedDatabaseInfo databaseEntry, final OTransactionRecordEntry iRequest,
			final SYNCH_TYPE iRequestType, final ORecordInternal<?> iRecord) {

		if (OLogManager.instance().isWarnEnabled()) {
			String operation = "?";
			switch (iRequest.status) {
			case OTransactionRecordEntry.CREATED:
				operation = "CREATE";
				break;
			case OTransactionRecordEntry.UPDATED:
				operation = "UPDATE";
				break;
			case OTransactionRecordEntry.DELETED:
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
					network.writeByte(iRequest.status);
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
						handleRemoteResponse(iRequest.status, iRequestType, iRecord, network.readLong());
					} finally {
						endResponse(network);
					}
				else {
					Callable<Object> response = new Callable<Object>() {
						public Object call() throws Exception {
							beginResponse(network);
							try {
								handleRemoteResponse(iRequest.status, iRequestType, iRecord, network.readLong());
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
				conflictResolver.handleUpdateConflict(iRequest.status, iRequestType, iRecord, e.getRecordVersion(), e.getDatabaseVersion());
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
		case OTransactionRecordEntry.CREATED:
			if (iResponse != iRecord.getIdentity().getClusterPosition())
				conflictResolver.handleCreateConflict(OTransactionRecordEntry.CREATED, iRequestType, iRecord, iResponse);
			break;
		case OTransactionRecordEntry.UPDATED:
			if ((int) iResponse != iRecord.getVersion())
				conflictResolver.handleUpdateConflict(OTransactionRecordEntry.UPDATED, iRequestType, iRecord, iRecord.getVersion(),
						(int) iResponse);
			break;
		case OTransactionRecordEntry.DELETED:
			if ((int) iResponse == 0)
				conflictResolver.handleDeleteConflict(OTransactionRecordEntry.DELETED, iRequestType, iRecord);
			break;
		}
	}
}
