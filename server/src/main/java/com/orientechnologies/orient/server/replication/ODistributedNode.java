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
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.server.replication.ODistributedDatabaseInfo.SYNCH_TYPE;

/**
 * Represents a member of the cluster.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class ODistributedNode {
	public enum STATUS {
		ONLINE, SYNCHRONIZING
	}

	private final OReplicator											replicator;
	private final String													id;
	public String																	networkAddress;
	public int																		networkPort;
	public Date																		connectedOn;
	private Map<String, ODistributedDatabaseInfo>	databases	= new HashMap<String, ODistributedDatabaseInfo>();
	private STATUS																status;

	public ODistributedNode(final OReplicator iReplicator, final String iId) throws IOException {
		replicator = iReplicator;
		id = iId;

		final String[] parts = iId.split(":");
		networkAddress = parts[0];
		networkPort = Integer.parseInt(parts[1]);
	}

	protected ODistributedDatabaseInfo createDatabaseEntry(final String dbName, SYNCH_TYPE iSynchType, final String iUserName,
			final String iUserPasswd) throws IOException {
		final ODistributedDatabaseInfo dbInfo = new ODistributedDatabaseInfo();
		dbInfo.databaseName = dbName;
		dbInfo.userName = iUserName;
		dbInfo.userPassword = iUserPasswd;
		dbInfo.synchType = iSynchType;
		dbInfo.log = new OOperationLog(id, dbName);
		return dbInfo;
	}

	public void startDatabaseReplication(final ODistributedDatabaseInfo iDatabase) throws IOException {
		synchronized (this) {
			// REMOVE ANY OTHER PREVIOUS ENTRY
			databases.remove(iDatabase.databaseName);

			OLogManager.instance().warn(this, "<-> DB %s: starting replication against distributed node %s:%d", iDatabase.databaseName,
					networkAddress, networkPort);

			try {
				if (iDatabase.storage == null)
					iDatabase.storage = new ODistributedStorage(replicator, replicator.getManager().getId(), id + "/"
							+ iDatabase.databaseName, "rw", replicator.getConflictResolver());
				
				iDatabase.storage.open(iDatabase.userName, iDatabase.userPassword, null);

				databases.put(iDatabase.databaseName, iDatabase);

				status = STATUS.SYNCHRONIZING;
				iDatabase.storage.synchronize(replicator.getDatabaseConfiguration(iDatabase.databaseName));
				status = STATUS.ONLINE;

			} catch (Exception e) {
				databases.remove(iDatabase.databaseName);
				OLogManager.instance().warn(this, "<> DB %s: cannot find database on remote server. Removing it from shared list.", e,
						iDatabase.databaseName);
			}
		}
	}

	public void sendRequest(final long iOperationId, final ORecordOperation iRequest, final SYNCH_TYPE iRequestType)
			throws IOException {
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

	public ODistributedDatabaseInfo shareDatabase(final ODatabaseRecord iDb, final String iRemoteEngine, String iUserName,
			String iUserPasswd) throws IOException, InterruptedException {
		ODistributedDatabaseInfo db = getDatabase(iDb.getName());

		if (db != null)
			throw new ODistributedSynchronizationException("Database '" + iDb.getName() + "' is already shared on remote server node '"
					+ id + "'");

		db = createDatabaseEntry(iDb.getName(), SYNCH_TYPE.SYNCH, iUserName, iUserPasswd);

		if (status != STATUS.ONLINE)
			throw new ODistributedSynchronizationException("Cannot share database '" + db.databaseName + "' on remote server node '" + id
					+ "' because is disconnected");

		OLogManager.instance().info(this,
				"<-> DB %s: sharing database exporting to the remote server %s via streaming across the network...", db.databaseName, id);

		final long time = System.currentTimeMillis();

		db.storage = new ODistributedStorage(replicator, replicator.getManager().getId(), id, "rw", replicator.getConflictResolver());
		db.storage.share(iDb, db.databaseName, db.userName, db.userPassword, iRemoteEngine);
		db.sessionId = db.storage.getSessionId();

		OLogManager.instance().info(this, "<-> DB %s: sharing completed (%dms)", db.databaseName, System.currentTimeMillis() - time);

		status = STATUS.ONLINE;

		startDatabaseReplication(db);

		return db;
	}

	@Override
	public String toString() {
		return id;
	}

	public String getName() {
		return networkAddress + ":" + networkPort;
	}

	/**
	 * Closes all the opened databases.
	 */
	public void disconnect() {
		for (ODistributedDatabaseInfo db : databases.values()) {
			if (db.storage != null)
				db.storage.close();
		}
		databases.clear();
	}

	public ODistributedDatabaseInfo getDatabase(final String iDatabaseName) {
		return databases.get(iDatabaseName);
	}

	public long getLastOperationId(final String iDatabaseName) throws IOException {
		return databases.get(iDatabaseName).log.getLastOperationId();
	}

	protected void handleError(final ORecordOperation iRequest, final SYNCH_TYPE iRequestType, final Exception iException)
			throws RuntimeException {

		final Set<ODistributedDatabaseInfo> currentDbList = new HashSet<ODistributedDatabaseInfo>(databases.values());

		disconnect();

		// ERROR
		OLogManager.instance().warn(this, "<-> NODE %s:%d seems down, retrying to connect...", networkAddress, networkPort);

		// RECONNECT ALL DATABASES
		try {
			for (ODistributedDatabaseInfo dbEntry : currentDbList) {
				startDatabaseReplication(dbEntry);
			}
		} catch (IOException e) {
			// IO ERROR: THE NODE SEEMED ALWAYS MORE DOWN: START TO COLLECT DATA FOR IT WAITING FOR A FUTURE RE-CONNECTION
			OLogManager.instance().warn(this, "<-> NODE %s:%d is down, remove it from replication", networkAddress, networkPort);
		}

		if (iRequestType == SYNCH_TYPE.SYNCH) {
			// SYNCHRONOUS CASE: RE-THROW THE EXCEPTION NOW TO BEING PROPAGATED UP TO THE CLIENT
			if (iException instanceof RuntimeException)
				throw (RuntimeException) iException;
		}
	}
}
