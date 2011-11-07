/*
 * Copyright 1999-2011 Luca Garulli (l.garulli--at--orientechnologies.com)
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
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.tx.OTransactionRecordEntry;
import com.orientechnologies.orient.server.handler.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.replication.ODistributedDatabaseInfo.SYNCH_TYPE;

/**
 * Represents a member of the cluster.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class ODistributedNode implements OCommandOutputListener {
	public enum STATUS {
		ONLINE, SYNCHRONIZING
	}

	private String																id;
	public String																	networkAddress;
	public int																		networkPort;
	public Date																		connectedOn;
	private List<OTransactionRecordEntry>					bufferedChanges	= new ArrayList<OTransactionRecordEntry>();
	private Map<String, ODistributedDatabaseInfo>	databases				= new HashMap<String, ODistributedDatabaseInfo>();
	private STATUS																status;

	public ODistributedNode(final ODistributedServerManager iNode, final String iId) {
		id = iId;

		final String[] parts = iId.split(":");
		networkAddress = parts[0];
		networkPort = Integer.parseInt(parts[1]);
	}

	public void connectDatabase(final ODistributedDatabaseInfo iDatabase) throws IOException {
		synchronized (this) {
			// REMOVE ANY OTHER PREVIOUS ENTRY
			databases.remove(iDatabase.databaseName);

			OLogManager.instance().warn(this, "Starting replication for database '%s' against distributed node %s:%d...",
					iDatabase.databaseName, networkAddress, networkPort);

			try {
				iDatabase.storage = new ODistributedStorage(id + "/" + iDatabase.databaseName, "rw");
				iDatabase.storage.open(iDatabase.userName, iDatabase.userPassword, null);

				iDatabase.sessionId = iDatabase.storage.getSessionId();

				databases.put(iDatabase.databaseName, iDatabase);

			} catch (Exception e) {
				databases.remove(iDatabase.databaseName);
				OLogManager.instance().warn(this,
						"Database '" + iDatabase.databaseName + "' is not present on remote server. Removing database from shared list.", e);
			}
		}
	}

	public void sendRequest(final OTransactionRecordEntry iRequest, final SYNCH_TYPE iRequestType) throws IOException {
		logChange(iRequest);

		final ODistributedDatabaseInfo databaseEntry = databases.get(iRequest.getRecord().getDatabase().getName());
		if (databaseEntry == null)
			return;

		databaseEntry.storage.setSessionId(databaseEntry.sessionId);

		final ORecordInternal<?> record = iRequest.getRecord();

		try {
			databaseEntry.storage.distributeChange(databaseEntry, iRequest, iRequestType, record);

		} catch (Exception e) {
			handleError(iRequest, iRequestType, e);
		}
	}

	protected void handleError(final OTransactionRecordEntry iRequest, final SYNCH_TYPE iRequestType, final Exception iException)
			throws RuntimeException {
		disconnect();

		// ERROR
		OLogManager.instance().warn(this, "Remote server node %s:%d seems down, retrying to connect...", networkAddress, networkPort);

		// RECONNECT ALL DATABASES
		try {
			for (ODistributedDatabaseInfo dbEntry : databases.values()) {
				connectDatabase(dbEntry);
			}
		} catch (IOException e) {
			// IO ERROR: THE NODE SEEMED ALWAYS MORE DOWN: START TO COLLECT DATA FOR IT WAITING FOR A FUTURE RE-CONNECTION
			OLogManager.instance()
					.warn(this, "Remote server node %s:%d is down, remove it from replication", networkAddress, networkPort);
		}

		disconnect();

		if (iRequestType == SYNCH_TYPE.SYNCH) {
			// SYNCHRONOUS CASE: RE-THROW THE EXCEPTION NOW TO BEING PROPAGATED UP TO THE CLIENT
			if (iException instanceof RuntimeException)
				throw (RuntimeException) iException;
		}
	}

	/**
	 * Log changes to the disk. TODO: Write to disk not memory
	 * 
	 * @param iRequest
	 */
	protected void logChange(final OTransactionRecordEntry iRequest) {
		synchronized (bufferedChanges) {
			try {
			} finally {
			}
		}
	}

	public void startSynchronization() throws InterruptedException, IOException {
		if (status != STATUS.SYNCHRONIZING) {
			synchronizeDelta();
			status = STATUS.ONLINE;
		}
	}

	public void shareDatabase(final ODatabaseRecord iDatabase, final String iRemoteServerName, final String iDbUser,
			final String iDbPasswd, final String iEngineName, final boolean iSynchronousMode) throws IOException, InterruptedException {
		if (status != STATUS.ONLINE)
			throw new ODistributedSynchronizationException("Can't share database '" + iDatabase.getName() + "' on remote server node '"
					+ iRemoteServerName + "' because is disconnected");

		// final String dbName = iDatabase.getName();
		// final ODistributedDatabaseInfo databaseEntry;
		//
		// channel.beginRequest();
		// try {
		// status = STATUS.SYNCHRONIZING;
		//
		// OLogManager.instance().info(this, "Sharing database '" + dbName + "' to remote server " + iRemoteServerName + "...");
		//
		// // EXECUTE THE REQUEST ON THE REMOTE SERVER NODE
		// channel.writeByte(OChannelDistributedProtocol.REQUEST_DISTRIBUTED_DB_SHARE_RECEIVER);
		// channel.writeInt(clientTxId);
		// channel.writeString(dbName);
		// channel.writeString(iDbUser);
		// channel.writeString(iDbPasswd);
		// channel.writeString(iEngineName);
		// } finally {
		// channel.endRequest();
		// }
		//
		// OLogManager.instance().info(this, "Exporting database '%s' via streaming to remote server node: %s...", iDatabase.getName(),
		// iRemoteServerName);
		//
		// // START THE EXPORT GIVING AS OUTPUTSTREAM THE CHANNEL TO STREAM THE EXPORT
		// new ODatabaseExport(iDatabase, new OChannelBinaryOutputStream(channel), this).exportDatabase();
		//
		// OLogManager.instance().info(this, "Database exported correctly");
		//
		// databaseEntry = new ODistributedDatabaseInfo();
		// databaseEntry.databaseName = dbName;
		// databaseEntry.userName = iDbUser;
		// databaseEntry.userPassword = iDbPasswd;
		//
		// channel.beginResponse(clientTxId);
		// try {
		// databaseEntry.sessionId = channel.readInt();
		//
		// databases.put(dbName, databaseEntry);
		// } finally {
		// channel.endResponse();
		// }

		status = STATUS.ONLINE;
	}

	@Override
	public void onMessage(final String iText) {
	}

	@Override
	public String toString() {
		return id;
	}

	private void synchronizeDelta() throws IOException {
		synchronized (bufferedChanges) {
			if (bufferedChanges.isEmpty())
				return;

			OLogManager.instance().info(this, "Started realignment of remote node '%s' after a reconnection. Found %d updates", id,
					bufferedChanges.size());

			status = STATUS.SYNCHRONIZING;

			final long time = System.currentTimeMillis();

			for (OTransactionRecordEntry entry : bufferedChanges) {
				sendRequest(entry, SYNCH_TYPE.SYNCH);
			}
			bufferedChanges.clear();

			OLogManager.instance()
					.info(this, "Realignment of remote node '%s' completed in %d ms", id, System.currentTimeMillis() - time);

			status = STATUS.ONLINE;
		}
	}

	public String getName() {
		return networkAddress + ":" + networkPort;
	}

	public void disconnect() {
		for (ODistributedDatabaseInfo db : databases.values()) {
			if (db.storage != null)
				db.storage.close();
		}
		databases.values().clear();
	}

	public Map<String, ODistributedDatabaseInfo> getDatabases() {
		return databases;
	}
}
