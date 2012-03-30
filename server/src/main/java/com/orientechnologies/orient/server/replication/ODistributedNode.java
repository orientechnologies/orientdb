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
import java.util.logging.Level;

import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.server.clustering.OClusterLogger;
import com.orientechnologies.orient.server.clustering.OClusterLogger.DIRECTION;
import com.orientechnologies.orient.server.clustering.OClusterLogger.TYPE;
import com.orientechnologies.orient.server.replication.ODistributedDatabaseInfo.STATUS_TYPE;
import com.orientechnologies.orient.server.replication.ODistributedDatabaseInfo.SYNCH_TYPE;

/**
 * Represents a member of the cluster.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class ODistributedNode {
	public enum STATUS {
		OFFLINE, ONLINE, SYNCHRONIZING
	}

	private final OReplicator											replicator;
	private final String													id;
	public String																	networkAddress;
	public int																		networkPort;
	public Date																		connectedOn;
	private Map<String, ODistributedDatabaseInfo>	databases	= new HashMap<String, ODistributedDatabaseInfo>();
	private STATUS																status		= STATUS.OFFLINE;
	protected OClusterLogger											logger		= new OClusterLogger();

	public ODistributedNode(final OReplicator iReplicator, final String iId) throws IOException {
		replicator = iReplicator;
		id = iId;

		final String[] parts = iId.split(":");
		networkAddress = parts[0];
		networkPort = Integer.parseInt(parts[1]);
		logger.setNode(iId);
	}

	protected ODistributedDatabaseInfo createDatabaseEntry(final String dbName, SYNCH_TYPE iSynchType, final String iUserName,
			final String iUserPasswd) throws IOException {
		return new ODistributedDatabaseInfo(id, dbName, iUserName, iUserPasswd, iSynchType, STATUS_TYPE.OFFLINE);
	}

	public void startDatabaseReplication(final ODistributedDatabaseInfo iDatabase) throws IOException {
		synchronized (this) {
			if (status == STATUS.ONLINE)
				// ALREADY ONLINE
				return;

			// REMOVE ANY OTHER PREVIOUS ENTRY
			final ODistributedDatabaseInfo oldDbInfo = databases.remove(iDatabase.databaseName);
			if (oldDbInfo != null)
				oldDbInfo.close();

			logger.setDatabase(iDatabase.databaseName);
			logger.log(this, Level.WARNING, TYPE.REPLICATION, DIRECTION.OUT, "starting replication against distributed node");

			try {
				databases.put(iDatabase.databaseName, iDatabase);

				if (iDatabase.connection == null)
					iDatabase.connection = new ONodeConnection(replicator, id, replicator.getConflictResolver());

				iDatabase.connected();

				setStatus(STATUS.SYNCHRONIZING);
				iDatabase.connection.synchronize(iDatabase.databaseName, replicator.getLocalDatabaseConfiguration(iDatabase.databaseName));
				iDatabase.setOnline();
				setStatus(STATUS.ONLINE);

			} catch (Exception e) {
				iDatabase.setOffline();
				iDatabase.close();
				databases.remove(iDatabase.databaseName);
				logger.log(this, Level.WARNING, TYPE.REPLICATION, DIRECTION.NONE,
						"cannot find database on remote server. Removing it from shared list", e);
			}
		}
	}

	public void sendRequest(final long iOperationId, final ORecordOperation iRequest, final SYNCH_TYPE iRequestType)
			throws IOException {
		final ODistributedDatabaseInfo databaseEntry = databases.get(iRequest.getRecord().getDatabase().getName());
		if (databaseEntry == null)
			return;

		final ORecordInternal<?> record = iRequest.getRecord();

		try {
			databaseEntry.connection.distributeChange(databaseEntry, iRequest, iRequestType, record);

		} catch (Exception e) {
			handleError(iRequest, iRequestType, e);
		}
	}

	public ODistributedDatabaseInfo copyDatabase(final ODatabaseRecord iDb, final String iRemoteEngine, String iUserName,
			String iUserPasswd) throws IOException {
		ODistributedDatabaseInfo db = getDatabase(iDb.getName());

		if (db != null)
			throw new ODistributedSynchronizationException("Database '" + iDb.getName() + "' is already shared on remote server node '"
					+ id + "'");

		if (status != STATUS.ONLINE)
			throw new ODistributedSynchronizationException("Cannot share database '" + iDb.getName() + "' on remote server node '" + id
					+ "' because is disconnected");

		final long time = System.currentTimeMillis();

		db = createDatabaseEntry(iDb.getName(), SYNCH_TYPE.SYNCH, iUserName, iUserPasswd);

		try {
			setStatus(STATUS.SYNCHRONIZING);

			logger.log(this, Level.INFO, TYPE.REPLICATION, DIRECTION.NONE,
					"sharing database exporting to the remote server via streaming across the network...");

			db.connection = new ONodeConnection(replicator, id, replicator.getConflictResolver());
			db.connection.copy(iDb, db.databaseName, db.userName, db.userPassword, iRemoteEngine);
			db.connected();
			setStatus(STATUS.ONLINE);

		} catch (IOException e) {
			// ERROR
			databases.remove(iDb.getName());
			setStatus(STATUS.OFFLINE);
			throw e;
		}

		logger.log(this, Level.INFO, TYPE.REPLICATION, DIRECTION.NONE, "sharing completed (%dms)", System.currentTimeMillis() - time);

		return db;
	}

	@Override
	public String toString() {
		return id;
	}

	public String getName() {
		return id;
	}

	/**
	 * Closes all the opened databases.
	 */
	public void disconnect() {
		for (ODistributedDatabaseInfo db : databases.values()) {
			if (db.connection != null)
				db.connection.disconnect();
		}
		databases.clear();
	}

	public ODistributedDatabaseInfo getDatabase(final String iDatabaseName) {
		return databases.get(iDatabaseName);
	}

	public long[] getLogRange(final String iDatabaseName) throws IOException {
		return new long[] { databases.get(iDatabaseName).log.getFirstOperationId(),
				databases.get(iDatabaseName).log.getLastOperationId() };
	}

	protected void handleError(final ORecordOperation iRequest, final SYNCH_TYPE iRequestType, final Exception iException)
			throws RuntimeException {

		final Set<ODistributedDatabaseInfo> currentDbList = new HashSet<ODistributedDatabaseInfo>(databases.values());

		disconnect();

		// ERROR
		logger.log(this, Level.WARNING, TYPE.REPLICATION, DIRECTION.NONE, "seems down, retrying to connect...");

		// RECONNECT ALL DATABASES
		try {
			for (ODistributedDatabaseInfo dbEntry : currentDbList) {
				startDatabaseReplication(dbEntry);
			}
		} catch (IOException e) {
			// IO ERROR: THE NODE SEEMED ALWAYS MORE DOWN: START TO COLLECT DATA FOR IT WAITING FOR A FUTURE RE-CONNECTION
			logger.log(this, Level.WARNING, TYPE.REPLICATION, DIRECTION.NONE, "is down, remove it from replication");
		}

		if (iRequestType == SYNCH_TYPE.SYNCH) {
			// SYNCHRONOUS CASE: RE-THROW THE EXCEPTION NOW TO BEING PROPAGATED UP TO THE CLIENT
			if (iException instanceof RuntimeException)
				throw (RuntimeException) iException;
		}
	}

	public com.orientechnologies.orient.server.replication.ODistributedNode.STATUS getStatus() {
		return status;
	}

	public void registerDatabase(final ODistributedDatabaseInfo iDatabaseEntry) throws IOException {
		databases.put(iDatabaseEntry.databaseName, iDatabaseEntry);
		iDatabaseEntry.connected();
	}

	private void setStatus(final STATUS iStatus) {
		logger.log(this, Level.WARNING, TYPE.REPLICATION, DIRECTION.NONE, "changed status %s -> %s", status, iStatus);
		status = iStatus;
	}
}
