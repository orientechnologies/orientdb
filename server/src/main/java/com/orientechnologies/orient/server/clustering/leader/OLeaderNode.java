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
package com.orientechnologies.orient.server.clustering.leader;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.logging.Level;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordLazyList;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.OServerMain;
import com.orientechnologies.orient.server.clustering.OClusterLogger;
import com.orientechnologies.orient.server.clustering.OClusterLogger.DIRECTION;
import com.orientechnologies.orient.server.clustering.OClusterLogger.TYPE;
import com.orientechnologies.orient.server.handler.distributed.ODistributedServerManager;

/**
 * Leader node.
 * <p>
 * clusterDbConfigurations attribute handles the database configuration in JSON format, EXAMPLE:<br/>
 * <code>
 * { "name" : "demo", "nodes" : [ { "id" : "192.168.0.20:2424", "mode" : "synch" }, { "id" : "192.168.0.10:2424", "mode" : "asynch" } ] }
 * </code
 * </p>
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * @see ODistributedServerDiscoveryListener, ODistributedServerDiscoverySignaler
 * 
 */
public class OLeaderNode {
	private ODistributedServerManager						manager;
	private final HashMap<String, ORemotePeer>	nodes										= new LinkedHashMap<String, ORemotePeer>();	;
	private ODocument														clusterDbConfigurations	= new ODocument();
	private ODiscoveryListener									discoveryListener;
	private OClusterLogger											logger									= new OClusterLogger();

	public OLeaderNode(final ODistributedServerManager iManager) {
		this.manager = iManager;

		logger.log(this, Level.WARNING, TYPE.CLUSTER, DIRECTION.NONE, "current node is the new Leader Node of cluster %s",
				iManager.getConfig().name);

		for (String db : OServerMain.server().getAvailableStorageNames().keySet()) {
			try {
				addServerInConfiguration(db, manager.getId(), "synch");
			} catch (UnknownHostException e) {
				e.printStackTrace();
			}
		}

		discoveryListener = new ODiscoveryListener(manager, manager.getDistributedNetworkListener());

		// START HEARTBEAT FOR CONNECTIONS
		Orient.getTimer().schedule(new OPeerCheckerTask(this), manager.getConfig().networkHeartbeatDelay,
				manager.getConfig().networkHeartbeatDelay);
	}

	/**
	 * Abandon leadership
	 * 
	 * @param iForce
	 */
	public void shutdown() {
		synchronized (this) {
			for (Entry<String, ORemotePeer> node : nodes.entrySet())
				node.getValue().disconnect();

			nodes.clear();
			clusterDbConfigurations.clear();

			if (discoveryListener != null) {
				discoveryListener.sendShutdown();
				discoveryListener = null;
			}
		}
	}

	/**
	 * Connects to a peer.
	 * 
	 * @param iServerAddresses
	 *          Array of Server addresses where to connect. The order will be respected.
	 * @param iServerPort
	 *          Server port
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public void connect2Peer(final String[] iServerAddresses, final int iServerPort) {
		Throwable lastException = null;

		if (OLogManager.instance().isDebugEnabled())
			logger.log(this, Level.FINE, TYPE.CLUSTER, DIRECTION.NONE, "trying to connecting to peer %s:%d",
					Arrays.toString(iServerAddresses), iServerPort);

		for (String serverAddress : iServerAddresses) {
			final String key = ODistributedServerManager.getNodeName(serverAddress, iServerPort);
			final ORemotePeer node;

			synchronized (this) {
				if (nodes.containsKey(key)) {
					// ALREADY REGISTERED, MAYBE IT WAS DISCONNECTED
					node = nodes.get(key);

					logger.log(this, Level.FINE, TYPE.CLUSTER, DIRECTION.NONE, "peer %s already registered. Now has status: %s", key,
							node.getStatus());

					if (node.getStatus() != ORemotePeer.STATUS.UNREACHABLE && node.getStatus() != ORemotePeer.STATUS.DISCONNECTED
							&& node.checkConnection())
						// CONNECTION OK
						return;
				} else
					node = new ORemotePeer(this, serverAddress, iServerPort);

				try {
					if (node.connect(manager.getConfig().networkTimeoutNode, manager.getConfig().name, manager.getConfig().securityKey))
						// CONNECTION OK: ADD IT IN THE NODE LIST
						nodes.put(key, node);

					return;

				} catch (Exception e) {
					lastException = e;
				}
			}
		}

		logger.error(this, TYPE.CLUSTER, DIRECTION.NONE, "cannot connect to distributed server node using addresses %s:%d and %s:%d",
				lastException, null, iServerAddresses[0], iServerPort, iServerAddresses[1], iServerPort);
	}

	/**
	 * Handles the failure of a node
	 */
	public void handlePeerNodeFailure(final ORemotePeer iNode) {
		iNode.disconnect();

		// ERROR
		logger.log(this, Level.WARNING, TYPE.CLUSTER, DIRECTION.NONE, "peer node %s seems down, retrying to connect...", iNode.getId());

		// RETRY TO CONNECT
		try {
			if (iNode.connect(manager.getConfig().networkTimeoutNode, manager.getConfig().name, manager.getConfig().securityKey))
				return;
		} catch (IOException e) {
			// IO ERROR: THE NODE SEEMED ALWAYS MORE DOWN: START TO COLLECT DATA FOR IT WAITING FOR A FUTURE RE-CONNECTION
			logger.log(this, Level.WARNING, TYPE.CLUSTER, DIRECTION.NONE,
					"remote server node %s is down, set it as DISCONNECTED and start to buffer changes", iNode.getId());
		}

		removePeer(iNode);
	}

	public List<ORemotePeer> getPeerNodeList() {
		synchronized (this) {

			if (nodes.isEmpty())
				return null;

			return new ArrayList<ORemotePeer>(nodes.values());
		}
	}

	public ODocument updatePeerDatabases(final String iNodeId, final ODocument iConfiguration) throws UnknownHostException {
		// RECEIVE AVAILABLE DATABASES
		ODocument answer = new ODocument();

		for (String dbName : iConfiguration.fieldNames()) {
			// UPDATE LEADER'S CONFIGURATION
			manager.getLeader().addServerInConfiguration(dbName, iNodeId, "synch");

			// ANSWER WITH THE SERVER LIST THAT OWN THE REQUESTED DATABASES
			answer.field(dbName, manager.getLeader().getClusteredConfigurationForDatabase(dbName));
		}

		return answer;
	}

	/**
	 * Returns the Peer Nodes that own a database.
	 * 
	 * @param iDatabaseName
	 *          Database name to search
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public List<ODocument> getPeerNodesOwnDatabase(final String iDatabaseName) {
		final ODocument doc = clusterDbConfigurations.field(iDatabaseName);
		return (List<ODocument>) (doc != null ? doc.field("nodes") : null);
	}

	/**
	 * Returns the clustered configuration of a database.
	 * 
	 * @param iDatabaseName
	 *          Database name to search
	 * @return
	 */
	public ODocument getClusteredConfigurationForDatabase(final String iDatabaseName) {
		return clusterDbConfigurations.field(iDatabaseName);
	}

	public ODocument addServerInConfiguration(final String iDatabaseName, final String iNodeId, final String iReplicationMode)
			throws UnknownHostException {

		ODocument dbConfiguration = clusterDbConfigurations.field(iDatabaseName);

		final List<OIdentifiable> nodeList;

		if (dbConfiguration == null) {
			dbConfiguration = new ODocument().addOwner(clusterDbConfigurations);
			nodeList = new ORecordLazyList(dbConfiguration);
			dbConfiguration.field("nodes", nodeList);
			clusterDbConfigurations.field(iDatabaseName, dbConfiguration);
		} else {
			nodeList = dbConfiguration.field("nodes");
			for (OIdentifiable d : nodeList) {
				if (((ODocument) d).field("id").equals(iNodeId))
					// ALREADY PRESENT
					return (ODocument) d;
			}
		}

		// ADD THE NODE TO THE LIST
		final ODocument node = new ODocument().addOwner(dbConfiguration);
		nodeList.add(node);

		node.field("id", iNodeId);
		node.field("mode", iReplicationMode);

		manager.sendClusterConfigurationToClients(iDatabaseName, getClusteredConfigurationForDatabase(iDatabaseName));

		return node;
	}

	public ODistributedServerManager getManager() {
		return manager;
	}

	/**
	 * Remove the peer node from the node list and databases configuration.
	 * 
	 * @param iNode
	 */
	protected void removePeer(final ORemotePeer iNode) {
		synchronized (this) {
			nodes.remove(iNode.getId());
			for (Object cfg : clusterDbConfigurations.fieldValues()) {
				final List<ODocument> servers = ((ODocument) cfg).field("nodes");
				for (ODocument server : servers) {
					if (server.field("id").equals(iNode.getId())) {
						servers.remove(server);
						break;
					}
				}
			}
		}

		logger.log(this, Level.WARNING, TYPE.CLUSTER, DIRECTION.NONE, "removed server node %s", iNode.getId());
	}
}
