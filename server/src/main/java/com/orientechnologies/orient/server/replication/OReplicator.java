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

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.parser.OSystemVariableResolver;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.OServerMain;
import com.orientechnologies.orient.server.config.OServerUserConfiguration;
import com.orientechnologies.orient.server.handler.distributed.ODistributedServerConfiguration;
import com.orientechnologies.orient.server.handler.distributed.ODistributedServerManager;
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
	private final OReplicationConflictResolver	conflictResolver;

	public OReplicator(final ODistributedServerManager iManager) throws IOException {
		manager = iManager;
		trigger = new OReplicatorRecordHook(this);
		replicatorUser = OServerMain.server().getConfiguration().getUser(ODistributedServerConfiguration.REPLICATOR_USER);
		Orient.instance().registerEngine(new ODistributedEngine(this));

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
		clusterConfiguration = iDocument;

		// OPEN CONNECTIONS AGAINST OTHER SERVERS
		for (String dbName : clusterConfiguration.fieldNames()) {

			final ODocument db = clusterConfiguration.field(dbName);
			final Collection<ODocument> dbNodes = db.field("nodes");

			for (ODocument node : dbNodes)
				startReplication(dbName, (String) node.field("id"), node.field("mode").toString());
		}
	}

	public void startReplication(final String dbName, final String nodeId, final String mode) throws IOException {
		if (manager.itsMe(nodeId))
			// DON'T OPEN A CONNECTION TO MYSELF
			return;

		// GET THE NODE
		ODistributedNode dNode = nodes.get(nodeId);
		if (dNode == null) {
			// CREATE IT
			dNode = new ODistributedNode(this, nodeId);
			nodes.put(nodeId, dNode);
		}

		if (dNode.getDatabase(dbName) != null)
			// ALREADY CONNECTED
			return;

		if (!localLogs.containsKey(dbName))
			// INITIALIZING OPERATION LOG
			localLogs.put(dbName, new OOperationLog(manager.getId(), dbName));

		dNode.startDatabaseReplication(dNode.createDatabaseEntry(dbName, SYNCH_TYPE.valueOf(mode.toUpperCase()), replicatorUser.name,
				replicatorUser.password));
	}

	protected void removeDistributedNode(final String iNodeId, final IOException iCause) {
		OLogManager.instance().warn(this, "<-> NODE %s: error connecting distributed node. Remove it from the available nodes", iCause,
				iNodeId);
		nodes.remove(iNodeId);
	}

	/**
	 * Distributes the request to all the configured nodes. Each node has the responsibility to bring the message early (synch-mode)
	 * or using an asynchronous queue. The current server node is what has received the request from the client.
	 * 
	 * @throws IOException
	 */
	public void distributeRequest(final ORecordOperation iTransactionEntry) throws IOException {
		synchronized (this) {

			if (nodes.isEmpty())
				return;

			final String dbName = iTransactionEntry.getRecord().getDatabase().getName();

			// LOG THE OPERATION
			final OOperationLog log = localLogs.get(dbName);
			if (log == null)
				// DB NOT REPLICATED: IGNORE IT
				return;

			final long opId = log.appendLocalLog(iTransactionEntry.type, (ORecordId) iTransactionEntry.getRecord().getIdentity());

			// GET THE NODES INVOLVED IN THE UPDATE
			for (ODistributedNode node : nodes.values()) {
				final ODistributedDatabaseInfo dbEntry = node.getDatabase(dbName);
				if (dbEntry != null)
					node.sendRequest(opId, iTransactionEntry, dbEntry.synchType);
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
	 * &nbsp;  'demo': {'10.10.10.10:2480' : 21212, '10.10.10.20:2480' : 32133}, <br/>
	 * &nbsp;  'test':  {'10.10.10.10:2480' : 1333, '10.10.10.20:2480' : 78} <br/>
	 * }
	 * </code>
	 * 
	 * @return
	 */
	public ODocument getDatabaseConfiguration() {
		final ODocument doc = new ODocument();
		for (String dbName : OServerMain.server().getAvailableStorageNames().keySet())
			doc.field(dbName, getDatabaseConfiguration(dbName));

		return doc;
	}

	/**
	 * Returns the database configuration to send to the leader node. Example:
	 * 
	 * <code>
	 * <br/>
	 * { <br/>
	 * &nbsp;  'demo': [ { 'node': '10.10.10.10:2480', 'lastOperation': 21212 }, { 'node': '10.10.10.20:2480', 'lastOperation':  32133} ], <br/>
	 * &nbsp;  'test': [ { 'node': '10.10.10.10:2480', 'lastOperation': 1333 },  { 'node': '10.10.10.20:2480', 'lastOperation':  78} ] <br/>
	 * }
	 * </code>
	 * 
	 * @return
	 */
	public Set<ODocument> getDatabaseConfiguration(final String dbName) {
		final Set<ODocument> set = new HashSet<ODocument>();

		final File dbDir = new File(OSystemVariableResolver.resolveSystemVariables(DIRECTORY_NAME + "/" + dbName));
		if (dbDir.exists() && dbDir.isDirectory()) {
			for (File f : dbDir.listFiles()) {
				if (f.isFile() && f.getName().endsWith(OOperationLog.EXTENSION)) {
					final String nodeId = f.getName().substring(0, f.getName().indexOf('.')).replace('_', '.').replace('-', ':');

					final ODistributedNode node = nodes.get(nodeId);
					if (node != null) {

						final ODocument nodeCfg = new ODocument();
						set.add(nodeCfg);

						try {
							nodeCfg.field("node", nodeId);
							nodeCfg.field("lastOperation", node.getLastOperationId(dbName));
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

	public OReplicationConflictResolver getConflictResolver() {
		return conflictResolver;
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
		if (manager.getId().equals(iNodeId))
			return localLogs.get(iDatabaseName);

		return nodes.get(iNodeId).getDatabase(iDatabaseName).log;
	}

	public ODistributedServerManager getManager() {
		return manager;
	}

	public OServerUserConfiguration getReplicatorUser() {
		return replicatorUser;
	}
}
