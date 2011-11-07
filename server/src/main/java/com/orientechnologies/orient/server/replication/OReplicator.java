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
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.tx.OTransactionRecordEntry;
import com.orientechnologies.orient.server.OServerMain;
import com.orientechnologies.orient.server.config.OServerUserConfiguration;
import com.orientechnologies.orient.server.handler.distributed.ODistributedServerConfiguration;
import com.orientechnologies.orient.server.handler.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.replication.ODistributedDatabaseInfo.SYNCH_TYPE;

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
	private ODocument											clusterConfiguration;
	private OReplicatorRecordHook					trigger;
	private volatile STATUS								status	= STATUS.ONLINE;
	private Map<String, ODistributedNode>	nodes		= new HashMap<String, ODistributedNode>();
	private OServerUserConfiguration			replicatorUser;
	private ODistributedServerManager			manager;

	public OReplicator(final ODistributedServerManager iManager) {
		manager = iManager;
		trigger = new OReplicatorRecordHook(this);

		replicatorUser = OServerMain.server().getConfiguration().getUser(ODistributedServerConfiguration.REPLICATOR_USER);
		Orient.instance().registerEngine(new ODistributedEngine());
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
	 */
	public void updateConfiguration(final ODocument iDocument) {
		clusterConfiguration = iDocument;

		// OPEN CONNECTIONS AGAINST OTHER SERVERS
		for (String dbName : clusterConfiguration.fieldNames()) {
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
						final ODistributedNode dNode = new ODistributedNode(manager, nodeId);
						nodes.put(nodeId, dNode);

						try {
							final ODistributedDatabaseInfo dbInfo = new ODistributedDatabaseInfo();
							dbInfo.databaseName = dbName;
							dbInfo.userName = replicatorUser.name;
							dbInfo.userPassword = replicatorUser.password;
							dbInfo.synchType = SYNCH_TYPE.valueOf(node.field("mode").toString().toUpperCase());

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

			// GET THE NODES INVOLVED IN THE UPDATE
			for (ODistributedNode node : nodes.values()) {
				final ODistributedDatabaseInfo dbEntry = node.getDatabases().get(dbName);
				if (dbEntry != null)
					node.sendRequest(iTransactionEntry, dbEntry.synchType);
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
}
