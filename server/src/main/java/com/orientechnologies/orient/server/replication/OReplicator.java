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

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.parser.OSystemVariableResolver;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.OServerMain;
import com.orientechnologies.orient.server.clustering.OClusterLogger;
import com.orientechnologies.orient.server.clustering.OClusterLogger.DIRECTION;
import com.orientechnologies.orient.server.clustering.OClusterLogger.TYPE;
import com.orientechnologies.orient.server.config.OServerUserConfiguration;
import com.orientechnologies.orient.server.handler.distributed.ODistributedServerConfiguration;
import com.orientechnologies.orient.server.handler.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.replication.ODistributedDatabaseInfo.STATUS_TYPE;
import com.orientechnologies.orient.server.replication.ODistributedDatabaseInfo.SYNCH_TYPE;
import com.orientechnologies.orient.server.replication.conflict.OReplicationConflictResolver;

/**
 * Replicates requests across network remote server nodes.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OReplicator {
	public enum STATUS {
		OFFLINE, ONLINE, SYNCHRONIZING
	}

	/**
	 * Cluster configuration in this format:
	 * <p>
	 * <code>
	 * { "&lt;db-name&gt;" : [ { "&lt;node-address&gt;", "&lt;replication-mode&gt;" }* ] }
	 * </code>
	 * </p>
	 * * Example:
	 * <p>
	 * <code>{ "demo" : [ { "192.168.0.10:2425", "synch" } ] } </code>
	 * </p>
	 */
	public final static String									DIRECTORY_NAME					= "${ORIENTDB_HOME}/replication";
	private ODocument														clusterConfiguration;
	private OReplicatorRecordHook								trigger;
	private volatile STATUS											status									= STATUS.ONLINE;
	private Map<String, ODistributedNode>				nodes										= new HashMap<String, ODistributedNode>();
	private OServerUserConfiguration						replicatorUser;
	private ODistributedServerManager						manager;
	private Map<String, OOperationLog>					localLogs								= new HashMap<String, OOperationLog>();
	private final Set<String>										ignoredClusters					= new HashSet<String>();
	private final Set<String>										ignoredDocumentClasses	= new HashSet<String>();
	private final Set<ORecordId>								ignoredRecords					= new HashSet<ORecordId>();
	private final OReplicationConflictResolver	conflictResolver;
	private final OClusterLogger								logger									= new OClusterLogger();

	public OReplicator(final ODistributedServerManager iManager) throws IOException {
		manager = iManager;
		trigger = new OReplicatorRecordHook(this);
		replicatorUser = OServerMain.server().getConfiguration().getUser(ODistributedServerConfiguration.REPLICATOR_USER);

		final String conflictResolvertStrategy = iManager.getConfig().replicationConflictResolverConfig.get("strategy");
		try {
			conflictResolver = (OReplicationConflictResolver) Class.forName(conflictResolvertStrategy).newInstance();
			conflictResolver.config(this, iManager.getConfig().replicationConflictResolverConfig);
		} catch (Exception e) {
			throw new ODistributedException("Cannot create the configured replication conflict resolver: " + conflictResolvertStrategy);
		}
	}

	public void shutdown() {
		nodes.clear();
		status = STATUS.OFFLINE;
	}

	/**
	 * Updates the distributed configuration and connects to new servers if needed.
	 * 
	 * @param iDocument
	 *          Configuration as JSON document
	 * @throws IOException
	 */
	public void updateConfiguration(final ODocument iDocument) throws IOException {
		if (iDocument == null)
			return;

		clusterConfiguration = iDocument;

		// OPEN CONNECTIONS AGAINST OTHER SERVERS
		for (String dbName : clusterConfiguration.fieldNames()) {

			final ODocument db = clusterConfiguration.field(dbName);
			final Collection<ODocument> dbNodes = db.field("nodes");

			for (ODocument node : dbNodes)
				startReplication((String) node.field("id"), dbName, node.field("mode").toString());
		}
	}

	public boolean connect(final String nodeId, final String dbName, final String mode) throws IOException {
		if (manager.itsMe(nodeId))
			// DON'T OPEN A CONNECTION TO MYSELF BUT START REPLICATION
			return false;

		// GET THE NODE
		final ODistributedNode dNode = getOrCreateDistributedNode(nodeId);
		ODistributedDatabaseInfo db = dNode.getDatabase(dbName);
		if (db == null)
			db = dNode.createDatabaseEntry(dbName, SYNCH_TYPE.valueOf(mode.toUpperCase()), replicatorUser.name, replicatorUser.password);

		if (db.connection == null)
			db.connection = new ONodeConnection(this, nodeId, getConflictResolver());

		if (!localLogs.containsKey(dbName))
			// INITIALIZING OPERATION LOG
			localLogs.put(dbName, new OOperationLog(manager.getId(), dbName));

		return db.status != STATUS_TYPE.ONLINE;
	}

	public void startReplication(final String nodeId, final String dbName, final String mode) throws IOException {
		if (!connect(nodeId, dbName, mode))
			return;

		// GET THE NODE
		synchronized (this) {
			final ODistributedNode dNode = getOrCreateDistributedNode(nodeId);
			ODistributedDatabaseInfo db = dNode.getDatabase(dbName);

			dNode.startDatabaseReplication(db);
		}
	}

	protected void removeDistributedNode(final String iNodeId, final IOException iCause) {
		OLogManager.instance().warn(this, "<-> NODE %s: error connecting distributed node. Remove it from the available nodes", iCause,
				iNodeId);
		synchronized (this) {
			nodes.remove(iNodeId);
		}
	}

	/**
	 * Distributes the request to all the configured nodes. Each node has the responsibility to bring the message early (synch-mode)
	 * or using an asynchronous queue. The current server node is what has received the request from the client.
	 * 
	 * @throws IOException
	 */
	public void distributeRequest(final ORecordOperation iTransactionEntry) throws IOException {
		final String dbName = iTransactionEntry.getRecord().getDatabase().getName();

		synchronized (this) {
			// LOG THE OPERATION
			final OOperationLog log = localLogs.get(dbName);
			if (log == null)
				// DB NOT REPLICATED: IGNORE IT
				return;

			iTransactionEntry.serial = log
					.appendLocalLog(iTransactionEntry.type, (ORecordId) iTransactionEntry.getRecord().getIdentity());

			if (nodes.isEmpty())
				return;

			// GET THE NODES INVOLVED IN THE UPDATE
			for (ODistributedNode node : nodes.values()) {
				if (node.getStatus() != ODistributedNode.STATUS.ONLINE)
					logger.log(this, Level.INFO, TYPE.REPLICATION, DIRECTION.OUT, "node status %s: the change is not propagated",
							node.getStatus());
				else {
					final ODistributedDatabaseInfo dbEntry = node.getDatabase(dbName);
					if (dbEntry != null) {
						if (dbEntry.status != STATUS_TYPE.ONLINE)
							OLogManager.instance().info(this, "REPL <%s> status %s, the change is not propagated", dbEntry.databaseName,
									dbEntry.status);
						else
							// SEND THE REQUEST
							node.sendRequest(iTransactionEntry, dbEntry.synchType);
					}
				}
			}
		}
	}

	public ODocument getClusterConfiguration() {
		return clusterConfiguration;
	}

	public STATUS getStatus() {
		return status;
	}

	public void setStatus(final STATUS iOnline) {
		status = iOnline;
	}

	/**
	 * Returns the database configuration to send to the leader node. Example:
	 * 
	 * <code>
	 * <br/>
	 * { <br/>
	 * &nbsp;  'demo': [ { 'node': '10.10.10.10:2480', 'firstLog': 312, 'lastLog': 21212 }, { 'node': '10.10.10.20:2480', 'lastLog':  32133} ], <br/>
	 * &nbsp;  'test': [ { 'node': '10.10.10.10:2480', 'firstLog': 1333,  'lastLog': 3223  },  { 'node': '10.10.10.20:2480', 'lastLog':  78} ] <br/>
	 * }
	 * </code>
	 * 
	 * @return
	 * @throws IOException
	 */
	public ODocument getLocalDatabaseConfiguration() throws IOException {
		final ODocument doc = new ODocument();
		for (String dbName : OServerMain.server().getAvailableStorageNames().keySet())
			doc.field(dbName, getLocalDatabaseConfiguration(dbName));

		return doc;
	}

	/**
	 * Returns the previous replication configuration for a database to send to the leader node. Example:
	 * 
	 * <code>
	 * <br/>
	 * [ { 'node': '10.10.10.10:2480', 'firstLog': 0, 'lastLog': 21212 }, { 'node': '10.10.10.20:2480', 'firstLog': 2323, 'lastLog': 32133} ]
	 * </code>
	 * 
	 * @return
	 * @throws IOException
	 */
	public Set<ODocument> getLocalDatabaseConfiguration(final String dbName) throws IOException {
		final Set<ODocument> set = new HashSet<ODocument>();

		final File dbDir = new File(OSystemVariableResolver.resolveSystemVariables(DIRECTORY_NAME + "/" + dbName));
		if (dbDir.exists() && dbDir.isDirectory()) {
			for (File f : dbDir.listFiles()) {
				if (f.isFile() && f.getName().endsWith(OOperationLog.EXTENSION)) {
					final String nodeId = f.getName().substring(0, f.getName().indexOf('.')).replace('_', '.').replace('-', ':');

					if (manager.itsMe(nodeId))
						// JUMP MYSELF
						continue;

					synchronized (this) {
						final ODistributedNode node = getOrCreateDistributedNode(nodeId);

						if (node.getDatabase(dbName) == null) {
							node.registerDatabase(node.createDatabaseEntry(dbName, SYNCH_TYPE.ASYNCH,
									manager.getReplicator().getReplicatorUser().name, manager.getReplicator().getReplicatorUser().password));
						}

						final ODocument nodeCfg = new ODocument();
						set.add(nodeCfg);

						try {
							final long[] logRange = node.getLogRange(dbName);

							nodeCfg.field("node", nodeId);
							nodeCfg.field("firstLog", logRange[0]);
							nodeCfg.field("lastLog", logRange[1]);
						} catch (IOException e) {
						}
					}
				}
			}
		}

		return set;
	}

	public boolean isIgnoredDocumentClass(final String ignoredDocumentClass) {
		return ignoredDocumentClasses.contains(ignoredDocumentClass);
	}

	public void addIgnoredDocumentClass(final String ignoredDocumentClass) {
		ignoredDocumentClasses.add(ignoredDocumentClass);
	}

	public void removeIgnoreDocumentClasses(final String ignoredDocumentClass) {
		ignoredDocumentClasses.remove(ignoredDocumentClass);
	}

	public boolean isIgnoredCluster(final String ignoredCluster) {
		return ignoredClusters.contains(ignoredCluster);
	}

	public void addIgnoredCluster(final String ignoredCluster) {
		ignoredClusters.add(ignoredCluster);
	}

	public void removeIgnoreCluster(final String ignoredCluster) {
		ignoredClusters.remove(ignoredCluster);
	}

	public boolean isIgnoredRecord(final ORecordId ignoredRecord) {
		return ignoredRecords.contains(ignoredRecord);
	}

	public void addIgnoredRecord(final ORecordId ignoredRecord) {
		ignoredRecords.add(ignoredRecord);
	}

	public void removeIgnoreRecord(final ORecordId ignoredRecord) {
		ignoredRecords.remove(ignoredRecord);
	}

	public OReplicationConflictResolver getConflictResolver() {
		return conflictResolver;
	}

	public ODistributedNode getOrCreateDistributedNode(final String nodeId) throws IOException {
		synchronized (this) {
			ODistributedNode dNode = nodes.get(nodeId);
			if (dNode == null) {
				// CREATE IT
				dNode = new ODistributedNode(this, nodeId);
				nodes.put(nodeId, dNode);
			}
			return dNode;
		}
	}

	public ODistributedNode getNode(final String iNodeId) {
		return nodes.get(iNodeId);
	}

	/**
	 * Returns the operation log by node and db name
	 * 
	 * @param iNodeId
	 *          Node id
	 * @param iDatabaseName
	 *          Database's name
	 * @return OOperationLog instance
	 */
	public OOperationLog getOperationLog(final String iNodeId, final String iDatabaseName) {
		synchronized (this) {
			if (manager.itsMe(iNodeId))
				return localLogs.get(iDatabaseName);

			final ODistributedNode node = nodes.get(iNodeId);
			return node != null ? node.getDatabase(iDatabaseName).log : null;
		}
	}

	public boolean isReplicated(final String iDatabaseName) {
		synchronized (this) {
			return localLogs.containsKey(iDatabaseName);
		}
	}

	public ODistributedServerManager getManager() {
		return manager;
	}

	public OServerUserConfiguration getReplicatorUser() {
		return replicatorUser;
	}
}
