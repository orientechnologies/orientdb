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
package com.orientechnologies.orient.server.handler.distributed;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.crypto.SecretKey;

import com.orientechnologies.common.concur.resource.OSharedResourceExternal;
import com.orientechnologies.common.io.OIOException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.security.OSecurityManager;
import com.orientechnologies.orient.core.serialization.OBase64Utils;
import com.orientechnologies.orient.core.tx.OTransactionEntry;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinary;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;
import com.orientechnologies.orient.enterprise.channel.distributed.OChannelDistributedProtocol;
import com.orientechnologies.orient.server.OClientConnection;
import com.orientechnologies.orient.server.OClientConnectionManager;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.config.OServerHandlerConfiguration;
import com.orientechnologies.orient.server.config.OServerParameterConfiguration;
import com.orientechnologies.orient.server.handler.OServerHandlerAbstract;
import com.orientechnologies.orient.server.handler.distributed.ODistributedServerNodeRemote.SYNCH_TYPE;
import com.orientechnologies.orient.server.network.OServerNetworkListener;
import com.orientechnologies.orient.server.network.protocol.distributed.ODistributedRequesterThreadLocal;
import com.orientechnologies.orient.server.network.protocol.distributed.ONetworkProtocolDistributed;

/**
 * Distributed server node handler. It starts the discovery signaler and listener and manages the configuration of the distributed
 * server nodes. When the node startups and after a configurable timeout no nodes are joined, then the node became the LEADER.
 * <p>
 * Distributed server messages are sent using IP Multicast.
 * </p>
 * <p>
 * Distributed server message format:
 * </p>
 * <code>
 * Orient v. &lt;orientdb-version&gt;-&lt;protocol-version&gt;-&lt;cluster-name&gt;-&lt;cluster-password&gt-&lt;callback-address&gt-&lt;callback-port&gt;
 * </code>
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * @see ODistributedServerDiscoveryListener, ODistributedServerDiscoverySignaler
 * 
 */
public class ODistributedServerManager extends OServerHandlerAbstract {
	public enum STATUS {
		ONLINE, SYNCHRONIZING
	}

	protected OServer																						server;

	protected String																						name;
	protected String																						id;
	protected SecretKey																					securityKey;
	protected String																						securityAlgorithm;
	protected InetAddress																				networkMulticastAddress;
	protected int																								networkMulticastPort;
	protected int																								networkMulticastHeartbeat;																														// IN
	protected int																								networkTimeoutLeader;																																// IN
	protected int																								networkTimeoutNode;																																	// IN
	private int																									networkHeartbeatDelay;																																// IN
	protected int																								serverUpdateDelay;																																		// IN
	protected int																								serverOutSynchMaxBuffers;
	protected boolean																						serverElectedForLeadership;

	private ODistributedServerDiscoverySignaler									discoverySignaler;
	private ODistributedServerDiscoveryListener									discoveryListener;
	private ODistributedServerLeaderChecker											leaderCheckerTask;
	private ODistributedServerRecordHook												trigger;
	private final OSharedResourceExternal												lock										= new OSharedResourceExternal();

	private final HashMap<String, ODistributedServerNodeRemote>	nodes										= new LinkedHashMap<String, ODistributedServerNodeRemote>();	;

	static final String																					CHECKSUM								= "ChEcKsUm1976";

	static final String																					PACKET_HEADER						= "OrientDB v.";
	static final int																						PROTOCOL_VERSION				= 0;

	private OServerNetworkListener															distributedNetworkListener;
	private ONetworkProtocolDistributed													leaderConnection;
	public long																									lastHeartBeat;
	private Map<String, ODocument>															clusterDbConfigurations	= new HashMap<String, ODocument>();

	private volatile STATUS																			status									= STATUS.ONLINE;

	@Override
	public void startup() {
		trigger = new ODistributedServerRecordHook(this);
		broadcastPresence(serverElectedForLeadership);
	}

	@Override
	public void shutdown() {
		if (discoverySignaler != null)
			discoverySignaler.sendShutdown();
		if (discoveryListener != null)
			discoveryListener.sendShutdown();
	}

	public void receivedLeaderConnection(final ONetworkProtocolDistributed iNetworkProtocolDistributed) {
		OLogManager.instance().info(this, "Joined the cluster '" + name + "'");

		// STOP TO SEND PACKETS TO BEING DISCOVERED
		if (discoverySignaler != null) {
			discoverySignaler.sendShutdown();
			discoverySignaler = null;
		}

		leaderConnection = iNetworkProtocolDistributed;

		// FIRST TIME: SCHEDULE THE HEARTBEAT CHECKER
		leaderCheckerTask = new ODistributedServerLeaderChecker(this);
		Orient.getTimer().schedule(leaderCheckerTask, networkHeartbeatDelay, networkHeartbeatDelay / 2);
	}

	/**
	 * Callback invoked by OClusterDiscoveryListener when a good packed has been received.
	 * 
	 * @param iServerAddress
	 *          Server address where to connect
	 * @param iServerPort
	 *          Server port
	 */
	public void joinNode(final String iServerAddress, final int iServerPort) {
		final String key = getNodeName(iServerAddress, iServerPort);
		final ODistributedServerNodeRemote node;

		lock.acquireExclusiveLock();

		try {
			if (nodes.containsKey(key)) {
				// ALREADY REGISTERED, MAYBE IT WAS DISCONNECTED. INVOKE THE RECONNECTION
				node = nodes.get(key);
				if (node.getStatus() == ODistributedServerNodeRemote.STATUS.CONNECTED)
					return;
			} else {
				node = new ODistributedServerNodeRemote(this, iServerAddress, iServerPort);
				nodes.put(key, node);
			}

		} finally {
			lock.releaseExclusiveLock();
		}

		try {
			node.connect(networkTimeoutNode, name, securityKey);
			node.startSynchronization();
		} catch (Exception e) {
			OLogManager.instance().error(this, "Can't connect to distributed server node: %s:%d", e, node.networkAddress,
					node.networkPort);
		}
	}

	/**
	 * Handle the failure of a node
	 */
	public void handleNodeFailure(final ODistributedServerNodeRemote node) {
		// ERROR
		OLogManager.instance().warn(this, "Remote server node %s:%d seems down, retrying to connect...", node.networkAddress,
				node.networkPort);

		// RETRY TO CONNECT
		try {
			node.connect(networkTimeoutNode, name, securityKey);
		} catch (IOException e) {
			// IO ERROR: THE NODE SEEMD ALWAYS MORE DOWN: START TO COLLECT DATA FOR IT WAITING FOR A FUTURE RE-CONNECTION
			OLogManager.instance().debug(this, "Remote server node %s:%d is down, set it as DISCONNECTED and start to buffer changes",
					node.networkAddress, node.networkPort);

			node.setAsTemporaryDisconnected(serverOutSynchMaxBuffers);
		}
	}

	/**
	 * Became the cluster leader
	 * 
	 * @param iForce
	 */
	public void becameLeader(final boolean iForce) {
		synchronized (lock) {
			if (discoveryListener != null)
				// I'M ALREADY THE LEADER, DO NOTHING
				return;

			if (iForce)
				leaderConnection = null;
			else if (leaderConnection != null)
				// I'M NOT THE LEADER CAUSE I WAS BEEN CONNECTED BY THE LEADER
				return;

			OLogManager.instance().warn(this, "Current node is the new cluster Leader of distributed nodes");

			if (leaderCheckerTask != null)
				// STOP THE CHECK OF HEART-BEAT
				leaderCheckerTask.cancel();

			// NO NODE HAS JOINED: BECAME THE LEADER AND LISTEN FOR OTHER NODES
			discoveryListener = new ODistributedServerDiscoveryListener(this, distributedNetworkListener);

			// START HEARTBEAT FOR CONNECTIONS
			Orient.getTimer().schedule(new ODistributedServerNodeChecker(this), networkHeartbeatDelay, networkHeartbeatDelay);
		}
	}

	@Override
	public void onAfterClientRequest(final OClientConnection iConnection, final byte iRequestType) {
		if (iRequestType == OChannelBinaryProtocol.REQUEST_DB_OPEN)
			try {
				final ODocument clusterConfig = getClusterConfiguration(iConnection.database.getName());
				byte[] serializedDocument = clusterConfig != null ? clusterConfig.toStream() : null;
				((OChannelBinary) iConnection.protocol.getChannel()).writeBytes(serializedDocument);
			} catch (IOException e) {
				throw new OIOException("Error on marshalling of cluster configuration", e);
			}
	}

	@Override
	public void onClientError(final OClientConnection iConnection, final Throwable iThrowable) {
		// handleNodeFailure(node);
	}

	/**
	 * Parse parameters and configure services.
	 */
	@Override
	public void config(final OServer iServer, final OServerParameterConfiguration[] iParams) {
		server = iServer;

		try {
			name = "unknown";
			securityKey = null;
			networkMulticastAddress = InetAddress.getByName("235.1.1.1");
			networkMulticastPort = 2424;
			networkMulticastHeartbeat = 5000;
			networkTimeoutLeader = 3000;
			networkTimeoutNode = 5000;
			networkHeartbeatDelay = 5000;
			securityAlgorithm = "Blowfish";
			serverUpdateDelay = 0;
			serverOutSynchMaxBuffers = 300;
			serverElectedForLeadership = true;
			byte[] tempSecurityKey = null;

			if (iParams != null)
				for (OServerParameterConfiguration param : iParams) {
					if ("name".equalsIgnoreCase(param.name))
						name = param.value;
					else if ("security.algorithm".equalsIgnoreCase(param.name))
						securityAlgorithm = param.value;
					else if ("security.key".equalsIgnoreCase(param.name))
						tempSecurityKey = OBase64Utils.decode(param.value);
					else if ("network.multicast.address".equalsIgnoreCase(param.name))
						networkMulticastAddress = InetAddress.getByName(param.value);
					else if ("network.multicast.port".equalsIgnoreCase(param.name))
						networkMulticastPort = Integer.parseInt(param.value);
					else if ("network.multicast.heartbeat".equalsIgnoreCase(param.name))
						networkMulticastHeartbeat = Integer.parseInt(param.value);
					else if ("network.timeout.leader".equalsIgnoreCase(param.name))
						networkTimeoutLeader = Integer.parseInt(param.value);
					else if ("network.timeout.connection".equalsIgnoreCase(param.name))
						networkTimeoutNode = Integer.parseInt(param.value);
					else if ("network.heartbeat.delay".equalsIgnoreCase(param.name))
						networkHeartbeatDelay = Integer.parseInt(param.value);
					else if ("server.update.delay".equalsIgnoreCase(param.name))
						serverUpdateDelay = Integer.parseInt(param.value);
					else if ("server.outsynch.maxbuffers".equalsIgnoreCase(param.name))
						serverOutSynchMaxBuffers = Integer.parseInt(param.value);
					else if ("server.electedForLeadership".equalsIgnoreCase(param.name))
						serverElectedForLeadership = Boolean.parseBoolean(param.value);
				}

			if (tempSecurityKey == null) {
				OLogManager.instance().info(this, "Generating Server security key and save it in configuration...");
				// GENERATE NEW SECURITY KEY
				securityKey = OSecurityManager.instance().generateKey(securityAlgorithm, 96);

				// CHANGE AND SAVE THE NEW CONFIGURATION
				for (OServerHandlerConfiguration handler : iServer.getConfiguration().handlers) {
					if (handler.clazz.equals(getClass().getName())) {
						handler.parameters = new OServerParameterConfiguration[iParams.length + 1];
						for (int i = 0; i < iParams.length; ++i) {
							handler.parameters[i] = iParams[i];
						}
						handler.parameters[iParams.length] = new OServerParameterConfiguration("security.key",
								OBase64Utils.encodeBytes(securityKey.getEncoded()));
					}
				}
				iServer.saveConfiguration();

			} else
				// CREATE IT FROM STRING REPRESENTATION
				securityKey = OSecurityManager.instance().createKey(securityAlgorithm, tempSecurityKey);

			distributedNetworkListener = server.getListenerByProtocol(ONetworkProtocolDistributed.class);
			if (distributedNetworkListener == null)
				OLogManager.instance().error(this,
						"Can't find a configured network listener with 'distributed' protocol. Can't start distributed node", null,
						OConfigurationException.class);

			id = distributedNetworkListener.getInboundAddr().getHostName() + ":" + distributedNetworkListener.getInboundAddr().getPort();

		} catch (Exception e) {
			throw new OConfigurationException("Can't configure OrientDB Server as Cluster Node", e);
		}
	}

	public ODistributedServerNodeRemote getNode(final String iNodeName) {
		lock.acquireSharedLock();
		try {
			final ODistributedServerNodeRemote node = nodes.get(iNodeName);
			if (node == null)
				throw new IllegalArgumentException("Node '" + iNodeName + "' is not configured on server: " + getId());

			return node;

		} finally {
			lock.releaseSharedLock();
		}
	}

	public List<ODistributedServerNodeRemote> getNodeList() {
		lock.acquireSharedLock();
		try {
			return new ArrayList<ODistributedServerNodeRemote>(nodes.values());
		} finally {
			lock.releaseSharedLock();
		}
	}

	public void removeNode(final ODistributedServerNodeRemote iNode) {
		lock.acquireExclusiveLock();
		try {
			OLogManager.instance().warn(this, "Removed server node %s:%d from distributed cluster", iNode.networkAddress,
					iNode.networkPort);

			nodes.remove(iNode.toString());

		} finally {
			lock.releaseExclusiveLock();
		}
	}

	public int getServerUpdateDelay() {
		return serverUpdateDelay;
	}

	/**
	 * Tells if there is a distributed configuration active right now.
	 */
	public boolean isDistributedConfiguration() {
		return !nodes.isEmpty();
	}

	@Override
	public String getName() {
		return name;
	}

	/**
	 * Tells if the cluster's owner for the requested node is the current one or not
	 * 
	 * @param iDatabaseName
	 *          Database name
	 * @param iClusterName
	 *          Cluster name
	 * @return true if the cluster's owner for the requested node is the current one, otherwise false
	 */
	public boolean isCurrentNodeTheClusterOwner(final String iDatabaseName, final String iClusterName) {
		// GET THE NODES INVOLVED IN THE UPDATE
		final ODocument servers = getServersForCluster(iDatabaseName, iClusterName);
		if (servers == null)
			// NOT DISTRIBUTED CFG
			return true;

		if (ODistributedRequesterThreadLocal.INSTANCE.get())
			return true;

		return servers.field("owner").equals(getId());

	}

	public ODocument getServersForCluster(final String iDatabaseName, final String iClusterName) {
		// GET THE NODES INVOLVED IN THE UPDATE
		final ODocument database = clusterDbConfigurations.get(iDatabaseName);
		if (database == null)
			return null;

		final ODocument clusters = database.field("clusters");
		return (ODocument) (clusters.containsField(iClusterName) ? clusters.field(iClusterName) : clusters.field("*"));
	}

	/**
	 * Distributed the request to all the configured nodes. Each node has the responsibility to bring the message early (synch-mode)
	 * or using an asynchronous queue.
	 * 
	 * @throws IOException
	 */
	@SuppressWarnings("unchecked")
	public void distributeRequest(final OTransactionEntry iTransactionEntry) throws IOException {
		final HashMap<ODistributedServerNodeRemote, SYNCH_TYPE> nodeList;

		lock.acquireSharedLock();
		try {
			if (nodes.isEmpty())
				return;

			// GET THE NODES INVOLVED IN THE UPDATE
			final ODocument servers = getServersForCluster(iTransactionEntry.getRecord().getDatabase().getName(),
					iTransactionEntry.clusterName);

			if (servers == null)
				return;

			nodeList = new HashMap<ODistributedServerNodeRemote, ODistributedServerNodeRemote.SYNCH_TYPE>();
			if (servers.field("synch") != null)
				for (String s : ((Map<String, Object>) servers.field("synch")).keySet()) {
					nodeList.put(nodes.get(s), ODistributedServerNodeRemote.SYNCH_TYPE.SYNCHRONOUS);
				}
			if (servers.field("asynch") != null)
				for (String s : ((Map<String, Object>) servers.field("asynch")).keySet()) {
					nodeList.put(nodes.get(s), ODistributedServerNodeRemote.SYNCH_TYPE.ASYNCHRONOUS);
				}

		} finally {
			lock.releaseSharedLock();
		}

		for (Entry<ODistributedServerNodeRemote, SYNCH_TYPE> entry : nodeList.entrySet()) {
			entry.getKey().sendRequest(iTransactionEntry, entry.getValue());
		}
	}

	public int getNetworkHeartbeatDelay() {
		return networkHeartbeatDelay;
	}

	public long getLastHeartBeat() {
		return lastHeartBeat;
	}

	public void updateHeartBeatTime() {
		this.lastHeartBeat = System.currentTimeMillis();
		if (discoverySignaler != null) {
			// SIGNALER ON: SHUT DOWN IT
			discoverySignaler.sendShutdown();
			discoverySignaler = null;
		}
	}

	public ODocument getClusterConfiguration(final String iDatabaseName) {
		return clusterDbConfigurations.get(iDatabaseName);
	}

	public void setClusterConfiguration(final String iDatabaseName, final ODocument iConfiguration) {
		clusterDbConfigurations.put(iDatabaseName, iConfiguration);
	}

	public String getId() {
		return id;
	}

	private static String getNodeName(final String iServerAddress, final int iServerPort) {
		return iServerAddress + ":" + iServerPort;
	}

	// private ODocument createInitialDatabaseConfiguration(final String iDatabaseName) {
	// return addServerInConfiguration(iDatabaseName, getId(), getId(), "{\"*\":{\"owner\":{\"" + getId() + "\":{}}}}");
	// }

	public ODocument addServerInConfiguration(final String iDatabaseName, final String iAlias, final String iAddress,
			final boolean iSynchronous) {
		final StringBuilder cfg = new StringBuilder();

		cfg.append("{ \"*\" : { ");
		cfg.append("\"owner\" : \"");
		cfg.append(getId());
		cfg.append("\", ");
		cfg.append(iSynchronous ? "\"synch\"" : "\"asynch\"");
		cfg.append(" : { \"");
		cfg.append(iAlias);
		cfg.append("\" : \"");
		cfg.append(iAlias);
		cfg.append("\" } } }");

		return addServerInConfiguration(iDatabaseName, iAlias, iAddress, cfg.toString());
	}

	@SuppressWarnings("unchecked")
	public ODocument addServerInConfiguration(final String iDatabaseName, final String iAlias, final String iAddress,
			final String iServerClusterConfiguration) {

		ODocument dbConfiguration = clusterDbConfigurations.get(iDatabaseName);
		if (dbConfiguration == null) {
			dbConfiguration = new ODocument();
			clusterDbConfigurations.put(iDatabaseName, dbConfiguration);
		}

		// ADD IT IN THE SERVER LIST
		ODocument servers = dbConfiguration.field("servers");
		if (servers == null) {
			servers = new ODocument();
			dbConfiguration.field("servers", servers);
		}
		servers.field(iAlias, iAddress);

		ODocument clusters = dbConfiguration.field("clusters");
		if (clusters == null) {
			clusters = new ODocument();
			dbConfiguration.field("clusters", clusters);
		}

		ODocument allClusters = clusters.field("*");
		if (allClusters == null) {
			allClusters = new ODocument();
			clusters.field("*", allClusters);
		}

		// MERGE CONFIG
		final ODocument cfgDoc = new ODocument().fromJSON(iServerClusterConfiguration);

		Object fieldValue;
		for (String fieldName : cfgDoc.fieldNames()) {
			fieldValue = cfgDoc.field(fieldName);

			ODocument clusterConfig = clusters.field(fieldName);
			if (clusterConfig == null)
				// GET THE CONFIG OF THE NEW SERVER
				clusterConfig = (ODocument) fieldValue;
			else {
				// MERGE CLUSTER CONFIG
				if (fieldValue instanceof ODocument)
					clusterConfig.merge((ODocument) cfgDoc.field(fieldName), true, true);
				else
					clusterConfig.merge((Map<String, Object>) cfgDoc.field(fieldName), true, true);
			}
		}

		OLogManager.instance().info(this, "Updated server node configuration: %s", dbConfiguration.toJSON(""));

		broadcastClusterConfiguration(iDatabaseName);

		return dbConfiguration;
	}

	public void broadcastClusterConfiguration(final String iDatabaseName) {
		// UPDATE ALL THE NODES
		for (ODistributedServerNodeRemote node : getNodeList()) {
			if (node.getStatus() == ODistributedServerNodeRemote.STATUS.CONNECTED)
				node.sendConfiguration(iDatabaseName);
		}

		// UPDATE ALL THE CLIENTS
		OChannelBinary ch;
		for (OClientConnection c : OClientConnectionManager.instance().getConnections()) {
			if (c.protocol.getChannel() instanceof OChannelBinary) {
				ch = (OChannelBinary) c.protocol.getChannel();

				OLogManager.instance().info(this, "Sending the configuration to the connected client %s...",
						ch.socket.getRemoteSocketAddress());

				try {
					ch.acquireExclusiveLock();

					try {
						ch.writeByte(OChannelBinaryProtocol.PUSH_DATA);
						ch.writeInt(-10);
						ch.writeByte(OChannelDistributedProtocol.PUSH_DISTRIBUTED_CONFIG);
						ch.writeBytes(clusterDbConfigurations.get(iDatabaseName).toStream());

					} catch (IOException e) {
						e.printStackTrace();
					} finally {
						ch.releaseExclusiveLock();
					}
				} catch (InterruptedException e1) {
					OLogManager.instance().warn(this, "[broadcastClusterConfiguration] Timeout on sending configuration to remote node %s",
							ch.socket.getRemoteSocketAddress());
				}
			}
		}
	}

	public boolean isLeaderConnected() {
		return leaderConnection != null;
	}

	public String getSecurityAlgorithm() {
		return securityAlgorithm;
	}

	public byte[] getSecurityKey() {
		return securityKey.getEncoded();
	}

	public STATUS getStatus() {
		return status;
	}

	public void setStatus(final STATUS iOnline) {
		status = iOnline;
	}

	protected void broadcastPresence(final boolean iForceLeadership) {
		if (discoverySignaler != null) {
			// SHUT DOWN PREVIOUS THREAD
			discoverySignaler.sendShutdown();
			discoverySignaler = null;
		}

		// LAUNCH THE SIGNAL AND WAIT FOR A CONNECTION
		discoverySignaler = new ODistributedServerDiscoverySignaler(this, distributedNetworkListener, iForceLeadership);
	}

}
