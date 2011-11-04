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
package com.orientechnologies.orient.server.clustering;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map.Entry;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordLazyList;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.OClientConnectionManager;
import com.orientechnologies.orient.server.OServerMain;
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

	public OLeaderNode(final ODistributedServerManager iManager) {
		this.manager = iManager;
		OLogManager.instance().warn(this, "Cluster '%s': current node is the new Leader Node", iManager.getConfig().name);

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
				discoveryListener.shutdown();
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

		for (String serverAddress : iServerAddresses) {
			final String key = ODistributedServerManager.getNodeName(serverAddress, iServerPort);
			final ORemotePeer node;

			synchronized (this) {
				if (nodes.containsKey(key)) {
					// ALREADY REGISTERED, MAYBE IT WAS DISCONNECTED
					node = nodes.get(key);
					if (node.getStatus() != ORemotePeer.STATUS.UNREACHABLE && node.getStatus() != ORemotePeer.STATUS.DISCONNECTED
							&& node.checkConnection())
						// CONNECTION OK
						return;
				} else
					node = new ORemotePeer(this, serverAddress, iServerPort);

				try {
					if (!node.connect(manager.getConfig().networkTimeoutNode, manager.getConfig().name, manager.getConfig().securityKey))
						// LEADERSHIP NOT ACCEPTED
						return;

					// CONNECTION OK: ADD IT IN THE NODE LIST
					nodes.put(key, node);
					return;

				} catch (Exception e) {
					lastException = e;
				}
			}
		}

		OLogManager.instance().error(this, "Can't connect to distributed server node using addresses %s:%d and %s:%d", lastException,
				iServerAddresses[0], iServerPort, iServerAddresses[1], iServerPort);
	}

	/**
	 * Handles the failure of a node
	 */
	public void handlePeerNodeFailure(final ORemotePeer iNode) {
		iNode.disconnect();

		// ERROR
		OLogManager.instance().warn(this, "Remote server node %s:%d seems down, retrying to connect...", iNode.networkAddress,
				iNode.networkPort);

		// RETRY TO CONNECT
		try {
			if (iNode.connect(manager.getConfig().networkTimeoutNode, manager.getConfig().name, manager.getConfig().securityKey))
				return;
		} catch (IOException e) {
			// IO ERROR: THE NODE SEEMED ALWAYS MORE DOWN: START TO COLLECT DATA FOR IT WAITING FOR A FUTURE RE-CONNECTION
			OLogManager.instance().debug(this, "Remote server node %s:%d is down, set it as DISCONNECTED and start to buffer changes",
					iNode.networkAddress, iNode.networkPort);
		}

		removePeer(iNode);
	}

	public ORemotePeer getPeerNode(final String iNodeName) {
		synchronized (this) {
			final ORemotePeer node = nodes.get(ODistributedServerManager.resolveNetworkHost(iNodeName));
			if (node != null)
				return node;

			throw new IllegalArgumentException("Node '" + iNodeName + "' is not configured on server: " + manager.getId());
		}
	}

	public List<ORemotePeer> getPeerNodeList() {
		synchronized (this) {

			if (nodes.isEmpty())
				return null;

			return new ArrayList<ORemotePeer>(nodes.values());
		}
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

		broadcastClusterConfiguration(iDatabaseName);

		return node;
	}

	public void broadcastClusterConfiguration(final String iDatabaseName) {
		if (getPeerNodeList() == null && OClientConnectionManager.instance().getConnections().size() == 0)
			return;

		// GET UPDATED CONFIGURATION
		final ODocument config = getClusteredConfigurationForDatabase(iDatabaseName);

		OLogManager.instance().info(this,
				"Broadcasting distributed configuration for database '%s' to the connected servers and clients: %s", iDatabaseName,
				config.toJSON("attribSameRow"));

		// UPDATE ALL THE NODES
		if (getPeerNodeList() != null)
			for (ORemotePeer node : getPeerNodeList()) {
				if (node.getStatus() == ORemotePeer.STATUS.CONNECTED) {
					node.sendConfiguration(config);
				}
			}

		// // UPDATE ALL THE CLIENTS
		// OChannelBinary ch;
		// for (OClientConnection c : OClientConnectionManager.instance().getConnections()) {
		// if (c.protocol.getChannel() instanceof OChannelBinary) {
		// ch = (OChannelBinary) c.protocol.getChannel();
		//
		// OLogManager.instance().info(this, "Sending distributed configuration for database '%s' to the connected client %s...",
		// iDatabaseName, ch.socket.getRemoteSocketAddress());
		//
		// try {
		// ch.acquireExclusiveLock();
		//
		// try {
		// ch.writeByte(OChannelBinaryProtocol.PUSH_DATA);
		// ch.writeInt(Integer.MIN_VALUE);
		// ch.writeByte(OChannelDistributedProtocol.PUSH_DISTRIBUTED_CONFIG);
		//
		// ch.writeBytes(config.toStream());
		//
		// } catch (IOException e) {
		// e.printStackTrace();
		// } finally {
		// ch.releaseExclusiveLock();
		// }
		// } catch (InterruptedException e1) {
		// OLogManager.instance().warn(this, "[broadcastClusterConfiguration] Timeout on sending configuration to remote node %s",
		// ch.socket.getRemoteSocketAddress());
		// }
		// }
		// }
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

		OLogManager.instance().warn(this, "Removed server node %s:%d from distributed cluster '%s'", iNode.networkAddress,
				iNode.networkPort, manager.getConfig().name);
	}
}
