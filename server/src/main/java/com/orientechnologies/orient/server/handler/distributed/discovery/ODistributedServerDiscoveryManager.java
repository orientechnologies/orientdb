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
package com.orientechnologies.orient.server.handler.distributed.discovery;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.TimerTask;

import javax.crypto.SecretKey;

import com.orientechnologies.common.concur.resource.OSharedResourceAdaptiveExternal;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.security.OSecurityManager;
import com.orientechnologies.orient.core.serialization.OBase64Utils;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.config.OServerHandlerConfiguration;
import com.orientechnologies.orient.server.config.OServerParameterConfiguration;
import com.orientechnologies.orient.server.handler.OServerHandler;
import com.orientechnologies.orient.server.handler.distributed.ODistributedServerNode;
import com.orientechnologies.orient.server.network.OServerNetworkListener;
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
public class ODistributedServerDiscoveryManager implements OServerHandler {
	protected OServer																			server;

	protected String																			name;
	protected SecretKey																		securityKey;
	protected String																			securityAlgorithm;
	protected InetAddress																	networkMulticastAddress;
	protected int																					networkMulticastPort;
	protected int																					networkMulticastHeartbeat;																					// IN MS
	protected int																					networkTimeoutLeader;																							// IN MS
	protected int																					networkTimeoutNode;																								// IN MS
	protected int																					networkKeepaliveDelay;																							// IN MS

	private ODistributedServerDiscoverySignaler						discoverySignaler;
	private ODistributedServerDiscoveryListener						discoveryListener;
	private final OSharedResourceAdaptiveExternal					lock							= new OSharedResourceAdaptiveExternal();

	private final HashMap<String, ODistributedServerNode>	nodes							= new HashMap<String, ODistributedServerNode>();	;

	static final String																		CHECKSUM					= "ChEcKsUm1976";

	static final String																		PACKET_HEADER			= "OrientDB v.";
	static final int																			PROTOCOL_VERSION	= 0;

	private OServerNetworkListener												distributedNetworkListener;
	private ONetworkProtocolDistributed										leaderConnection;

	/**
	 * Parse parameters and configure services.
	 */
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
			networkKeepaliveDelay = 5000;
			securityAlgorithm = "Blowfish";
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
					else if ("network.keepalive.delay".equalsIgnoreCase(param.name))
						networkKeepaliveDelay = Integer.parseInt(param.value);
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

		} catch (Exception e) {
			throw new OConfigurationException("Can't configure OrientDB Server as Cluster Node", e);
		}
	}

	public void startup() {
		distributedNetworkListener = server.getListenerByProtocol(ONetworkProtocolDistributed.class);
		if (distributedNetworkListener == null)
			OLogManager.instance().error(this,
					"Can't find a configured network listener with 'cluster' protocol. Can't start cluster node", null,
					OConfigurationException.class);

		// LAUNCH THE SIGNAL AND WAIT FOR A CONNECTION
		launchTheSignalOfLife();
	}

	public void shutdown() {
		if (discoverySignaler != null)
			discoverySignaler.sendShutdown();
		if (discoveryListener != null)
			discoveryListener.sendShutdown();
	}

	public String getName() {
		return "Cluster node '" + name + "'";
	}

	public void receivedLeaderConnection(final ONetworkProtocolDistributed iNetworkProtocolDistributed) {
		// STOP TO SEND PACKETS TO BEING DISCOVERED
		discoverySignaler.sendShutdown();

		leaderConnection = iNetworkProtocolDistributed;
	}

	/**
	 * Callback invoked by OClusterDiscoveryListener when a good packed has been received.
	 * 
	 * @param iServerAddress
	 *          Server address where to connect
	 * @param iServerPort
	 *          Server port
	 */
	public void receivedNodePresence(final String iServerAddress, final int iServerPort) {

		final String key = iServerAddress + ":" + iServerPort;
		final ODistributedServerNode node;

		final boolean locked = lock.acquireExclusiveLock();

		try {
			if (nodes.containsKey(key))
				// ALREADY REGISTERED, IGNORE IT
				return;

			node = new ODistributedServerNode(this, iServerAddress, iServerPort);
			nodes.put(key, node);
		} finally {
			lock.releaseExclusiveLock(locked);
		}

		OLogManager.instance().warn(this, "Discovered new distributed server node %s. Trying to connect...", key);

		try {
			node.connect(networkTimeoutNode);
			node.startSynchronization();
		} catch (IOException e) {
			OLogManager.instance().error(this, "Can't connect to  distributed server node: %s", key);
		}
	}

	private void startListener() {
		discoveryListener = new ODistributedServerDiscoveryListener(this, distributedNetworkListener);

		// START KEEPALIVE FOR CONNECTIONS
		Orient.getTimer().schedule(new TimerTask() {
			@Override
			public void run() {
				final List<ODistributedServerNode> nodeList;

				boolean locked = lock.acquireSharedLock();
				try {
					if (nodes.values().size() == 0)
						// NO NODES, JUST RETURN
						return;

					// COPY THE NODE LIST
					nodeList = new ArrayList<ODistributedServerNode>(nodes.values());

				} finally {
					lock.releaseSharedLock(locked);
				}

				try {

					// CHECK EVERY SINGLE NODE
					for (ODistributedServerNode node : nodeList) {
						if (!node.sendKeepAlive(networkTimeoutLeader)) {
							// ERROR
							OLogManager.instance().warn(this, "Remote server node %s:%d seems down, retrying to connect...", node.networkAddress,
									node.networkPort);

							// RETRY TO CONNECT

							OLogManager.instance().warn(this, "Remote server node %s:%d is down, remove from the server list",
									node.networkAddress, node.networkPort);

							locked = lock.acquireExclusiveLock();
							nodes.remove(node.toString());
							lock.releaseExclusiveLock(locked);
						}
					}

					nodeList.clear();

				} catch (Exception e) {
					// AVOID THE TIMER IS NOT SCHEDULED ANYMORE IN CASE OF EXCEPTION
				}
			}
		}, networkKeepaliveDelay, networkKeepaliveDelay);
	}

	private void launchTheSignalOfLife() {
		discoverySignaler = new ODistributedServerDiscoverySignaler(this, distributedNetworkListener);

		// START THE TIMEOUT FOR PRESENCE
		Orient.getTimer().schedule(new TimerTask() {
			@Override
			public void run() {
				synchronized (lock) {
					try {
						// TIMEOUT: STOP TO SEND PACKETS TO BEING DISCOVERED
						discoverySignaler.sendShutdown();

						if (discoveryListener != null)
							// I'M ALREADY THE LEADER, DO NOTHING
							return;

						if (leaderConnection != null)
							// I'M NOT THE LEADER CAUSE I WAS BEEN CONNECTED BY THE LEADER
							return;

						// NO NODE HAS JOINED: BECAME THE LEADER AND LISTEN FOR SLAVES
						startListener();
					} catch (Exception e) {
						// AVOID THE TIMER IS NOT SCHEDULED ANYMORE IN CASE OF EXCEPTION
					}
				}
			}
		}, networkTimeoutLeader);
	}
}
