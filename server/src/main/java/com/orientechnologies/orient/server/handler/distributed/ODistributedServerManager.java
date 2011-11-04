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
import java.net.UnknownHostException;

import com.orientechnologies.common.io.OIOException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinary;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;
import com.orientechnologies.orient.server.OClientConnection;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.clustering.ODiscoveryListener;
import com.orientechnologies.orient.server.clustering.ODiscoverySignaler;
import com.orientechnologies.orient.server.clustering.OLeaderNode;
import com.orientechnologies.orient.server.clustering.OPeerNode;
import com.orientechnologies.orient.server.config.OServerParameterConfiguration;
import com.orientechnologies.orient.server.handler.OServerHandlerAbstract;
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
public class ODistributedServerManager extends OServerHandlerAbstract {
	public enum STATUS {
		OFFLINE, STARTING, LEADER, PEER
	}

	public String															id;
	protected ODistributedServerConfiguration	config;
	protected OServer													server;
	private volatile ODiscoverySignaler				discoverySignaler;
	private volatile ODiscoveryListener				discoveryListener;

	private OServerNetworkListener						distributedNetworkListener;
	private final long												startupDate	= System.currentTimeMillis();

	private OLeaderNode												leader;
	private OPeerNode													peer;
	private STATUS														status			= STATUS.OFFLINE;

	@Override
	public void startup() {
		status = STATUS.STARTING;
		sendPresence();
	}

	@Override
	public void shutdown() {
		if (discoverySignaler != null)
			discoverySignaler.sendShutdown();
		if (discoveryListener != null)
			discoveryListener.sendShutdown();

		status = STATUS.OFFLINE;
	}

	protected void sendPresence() {
		if (discoverySignaler != null)
			return;

		// LAUNCH THE SIGNAL AND WAIT FOR A CONNECTION
		discoverySignaler = new ODiscoverySignaler(this, distributedNetworkListener);
	}

	public void becomePeer() {
		if (discoverySignaler != null) {
			discoverySignaler.shutdown();
			discoverySignaler = null;
		}

		if (leader != null) {
			leader.shutdown();
			leader = null;
		}

		if (peer == null)
			peer = new OPeerNode(this);
	}

	/**
	 * Became the cluster leader.
	 * 
	 */
	public void becameLeader() {
		if (peer != null) {
			peer.shutdown();
			peer = null;
		}

		if (leader == null) {
			leader = new OLeaderNode(this);
			sendPresence();
		}
	}

	@Override
	public void onAfterClientRequest(final OClientConnection iConnection, final byte iRequestType) {
		if (iRequestType == OChannelBinaryProtocol.REQUEST_DB_OPEN)
			try {
				final ODocument clusterConfig = null;// getClusteredConfigurationForDatabase(iConnection.database.getName());
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
			config = new ODistributedServerConfiguration(iServer, this, iParams);

			distributedNetworkListener = server.getListenerByProtocol(ONetworkProtocolDistributed.class);
			if (distributedNetworkListener == null)
				OLogManager.instance().error(this,
						"Can't find a configured network listener with 'distributed' protocol. Can't start distributed node", null,
						OConfigurationException.class);

			id = InetAddress.getLocalHost().getHostAddress() + ":" + distributedNetworkListener.getInboundAddr().getPort();

		} catch (Exception e) {
			throw new OConfigurationException("Can't configure OrientDB Server as Cluster Node", e);
		}
	}

	/**
	 * Tells if current node is the leader.
	 */
	public boolean isLeader() {
		return leader != null;
	}

	public static String resolveNetworkHost(final String iAddress) {
		final String[] parts = iAddress.split(":");
		if (parts.length == 2)
			try {
				final InetAddress address = InetAddress.getByName(parts[0]);
				if (address != null)
					return address.getHostAddress() + ":" + parts[1];
			} catch (UnknownHostException e) {
			}

		return iAddress;

	}

	public OLeaderNode getLeader() {
		return leader;
	}

	public long getRunningSince() {
		return System.currentTimeMillis() - startupDate;
	}

	public OServerNetworkListener getDistributedNetworkListener() {
		return distributedNetworkListener;
	}

	public String getName() {
		return config.name;
	}

	/**
	 * Returns the server network address in the format <ip>:<port>.
	 */
	public String getId() {
		return id;
	}

	public OPeerNode getPeer() {
		return peer;
	}

	public ODistributedServerConfiguration getConfig() {
		return config;
	}

	public static String getNodeName(final String iServerAddress, final int iServerPort) {
		return iServerAddress + ":" + iServerPort;
	}

	public boolean itsMe(final String iNodeId) {
		if (iNodeId.equals(id))
			return true;

		final String[] parts = iNodeId.split(":");

		return iNodeId.equals(distributedNetworkListener.getInboundAddr().getAddress().getHostAddress() + ":" + parts[1]);
	}
}
