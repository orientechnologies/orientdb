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
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.tx.OTransactionRecordEntry;
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
	private Map<String, OOperationLog>					logs										= new HashMap<String, OOperationLog>();
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
	 * Updates the distributed configuration and reconnect to new servers.
	 * 
	 * @param iDocument
	 *          Configuration as JSON document
	 * @throws IOException
	 */
	public void updateConfiguration(final ODocument iDocument) throws IOException {
		clusterConfiguration = iDocument;

		// OPEN CONNECTIONS AGAINST OTHER SERVERS
		for (String dbName : clusterConfiguration.fieldNames()) {

			if (!logs.containsKey(dbName))
				logs.put(dbName, new OOperationLog(manager.getId(), dbName));

			final ODocument db = clusterConfiguration.field(dbName);
			final Collection<ODocument> dbNodes = db.field("nodes");

			boolean currentNodeInvolved = false;

			// CHECK IF CURRENT NODE IS INVOLVED
			for (ODocument node : dbNodes) {
				final String nodeId = node.field("id");
				if (manager.itsMe(nodeId)) {
					currentNodeInvolved = true;
					break;
				}
			}

			if (currentNodeInvolved)
				for (ODocument node : dbNodes) {
					final String nodeId = node.field("id");

					if (manager.itsMe(nodeId))
						// DON'T OPEN A CONNECTION TO MYSELF
						continue;

					if (!nodes.containsKey(nodeId)) {
						final ODistributedNode dNode = new ODistributedNode(this, nodeId);
						nodes.put(nodeId, dNode);

						try {
							final ODistributedDatabaseInfo dbInfo = new ODistributedDatabaseInfo();
							dbInfo.databaseName = dbName;
							dbInfo.userName = replicatorUser.name;
							dbInfo.userPassword = replicatorUser.password;
							dbInfo.synchType = SYNCH_TYPE.valueOf(node.field("mode").toString().toUpperCase());
							dbInfo.log = new OOperationLog(nodeId, dbName);

							dNode.connectDatabase(dbInfo);
						} catch (IOException e) {
							// REMOVE THE NODE
							removeDistributedNode(nodeId, e);
						}
					}
				}
		}
	}

	protected void removeDistributedNode(final String iNodeId, final IOException iCause) {
		OLogManager.instance().warn(this, "[OReplicator] Error connecting distributed node '%s'. Remove it from the available nodes",
				iCause, iNodeId);
		nodes.remove(iNodeId);
	}

	/**
	 * Distributes the request to all the configured nodes. Each node has the responsibility to bring the message early (synch-mode)
	 * or using an asynchronous queue.
	 * 
	 * @throws IOException
	 */
	public void distributeRequest(final OTransactionRecordEntry iTransactionEntry) throws IOException {
		synchronized (this) {

			if (nodes.isEmpty())
				return;

			final String dbName = iTransactionEntry.getRecord().getDatabase().getName();

			// LOG THE OPERATION
			final long opId = logs.get(dbName).addLog(iTransactionEntry.status, (ORecordId) iTransactionEntry.getRecord().getIdentity());

			// GET THE NODES INVOLVED IN THE UPDATE
			for (ODistributedNode node : nodes.values()) {
				final ODistributedDatabaseInfo dbEntry = node.getDatabases().get(dbName);
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

	public ODocument getDatabaseConfiguration() {
		final ODocument doc = new ODocument();
		for (String dbName : OServerMain.server().getAvailableStorageNames().keySet()) {
			final ODocument dbCfg = new ODocument().addOwner(doc);
			doc.field(dbName, dbCfg);

			final File dbDir = new File(OSystemVariableResolver.resolveSystemVariables(DIRECTORY_NAME + "/" + dbName));
			if (dbDir.exists() && dbDir.isDirectory()) {
				for (File f : dbDir.listFiles()) {
					if (f.isFile() && f.getName().endsWith(OOperationLog.EXTENSION)) {
						final String nodeId = f.getName().substring(0, f.getName().indexOf('.')).replace('_', '.').replace('-', ':');

						final ODistributedNode node = nodes.get(nodeId);
						if (node != null) {
							try {
								dbCfg.field(nodeId, node.getOperationId());
							} catch (IOException e) {
							}
						}
					}
				}
			}
		}

		return doc;
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

	public ODistributedServerManager getManager() {
		return manager;
	}
}
