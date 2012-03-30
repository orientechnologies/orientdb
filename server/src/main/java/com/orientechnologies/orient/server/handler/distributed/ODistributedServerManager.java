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
import java.util.logging.Level;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinary;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;
import com.orientechnologies.orient.server.OClientConnection;
import com.orientechnologies.orient.server.OClientConnectionManager;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.clustering.OClusterLogger;
import com.orientechnologies.orient.server.clustering.OClusterLogger.DIRECTION;
import com.orientechnologies.orient.server.clustering.OClusterLogger.TYPE;
import com.orientechnologies.orient.server.clustering.OClusterNetworkProtocol;
import com.orientechnologies.orient.server.clustering.ODiscoverySignaler;
import com.orientechnologies.orient.server.clustering.leader.ODiscoveryListener;
import com.orientechnologies.orient.server.clustering.leader.OLeaderNode;
import com.orientechnologies.orient.server.clustering.peer.OPeerNode;
import com.orientechnologies.orient.server.config.OServerParameterConfiguration;
import com.orientechnologies.orient.server.handler.OServerHandlerAbstract;
import com.orientechnologies.orient.server.network.OServerNetworkListener;
import com.orientechnologies.orient.server.replication.ODistributedException;
import com.orientechnologies.orient.server.replication.OReplicator;

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
		OFFLINE, STARTING, LEADER, PEER, DISABLED
	}

	public String															id;
	protected ODistributedServerConfiguration	config;
	protected OServer													server;
	private volatile ODiscoverySignaler				discoverySignaler;
	private volatile ODiscoveryListener				discoveryListener;
	private OServerNetworkListener						distributedNetworkListener;
	private OReplicator												replicator;
	private final long												startupDate	= System.currentTimeMillis();

	private OLeaderNode												leader;
	private OPeerNode													peer;
	protected STATUS													status			= STATUS.OFFLINE;
	protected OClusterLogger									logger			= new OClusterLogger();

	@Override
	public void startup() {
		if (status == STATUS.DISABLED)
			return;

		setStatus(STATUS.STARTING);
		sendPresence();
		try {
			replicator = new OReplicator(this);
		} catch (IOException e) {
			throw new ODistributedException("Cannot start replicator agent");
		}
	}

	@Override
	public void shutdown() {
		if (discoverySignaler != null)
			discoverySignaler.sendShutdown();
		if (discoveryListener != null)
			discoveryListener.sendShutdown();

		replicator.shutdown();

		setStatus(STATUS.OFFLINE);
	}

	protected void sendPresence() {
		if (discoverySignaler != null)
			return;

		// LAUNCH THE SIGNAL AND WAIT FOR A CONNECTION
		discoverySignaler = new ODiscoverySignaler(this, distributedNetworkListener);
	}

	public void becomePeer(final OClusterNetworkProtocol iConnection) {
		synchronized (this) {

			if (discoverySignaler != null) {
				discoverySignaler.sendShutdown();
				discoverySignaler = null;
			}

			if (leader != null) {
				leader.shutdown();
				leader = null;
			}

			if (peer == null)
				peer = new OPeerNode(this, iConnection);

			setStatus(STATUS.PEER);
		}
	}

	/**
	 * Became the cluster leader.
	 * 
	 */
	public void becameLeader() {
		synchronized (this) {

			if (peer != null) {
				peer.shutdown();
				peer = null;
			}

			if (leader == null) {
				leader = new OLeaderNode(this);
				sendPresence();
			}
		}
		setStatus(STATUS.LEADER);
	}

	/**
	 * Parse parameters and configure services.
	 */
	@Override
	public void config(final OServer iServer, final OServerParameterConfiguration[] iParams) {
		server = iServer;

		try {
			config = new ODistributedServerConfiguration(iServer, this, iParams);
			if (status == STATUS.DISABLED)
				return;

			distributedNetworkListener = server.getListenerByProtocol(OClusterNetworkProtocol.class);
			if (distributedNetworkListener == null)
				OLogManager.instance().error(this,
						"Cannot find a configured network listener with 'distributed' protocol. Cannot start distributed node", null,
						OConfigurationException.class);

			id = InetAddress.getLocalHost().getHostAddress() + ":" + distributedNetworkListener.getInboundAddr().getPort();

		} catch (Exception e) {
			throw new OConfigurationException("Cannot configure OrientDB Server as Cluster Node", e);
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

	public long updateHeartBeatTime() {
		synchronized (this) {
			if (peer != null)
				return peer.updateHeartBeatTime();
		}
		return -1;
	}

	public OReplicator getReplicator() {
		return replicator;
	}

	public void sendClusterConfigurationToClients(final String iDatabaseName, final ODocument config) {
		for (OClientConnection c : OClientConnectionManager.instance().getConnections()) {
			if (c != null && c.database != null && iDatabaseName.equals(c.database.getName())
					&& c.protocol.getChannel() instanceof OChannelBinary) {
				OChannelBinary ch = (OChannelBinary) c.protocol.getChannel();

				logger.log(this, Level.INFO, TYPE.REPLICATION, DIRECTION.NONE,
						"pushing distributed configuration to the connected client %s...", ch.socket.getRemoteSocketAddress());

				ch.acquireExclusiveLock();

				try {
					ch.writeByte(OChannelBinaryProtocol.PUSH_DATA);
					ch.writeInt(Integer.MIN_VALUE);
					ch.writeByte(OChannelBinaryProtocol.PUSH_NODE2CLIENT_DB_CONFIG);

					ch.writeBytes(config.toStream());

				} catch (IOException e) {
					e.printStackTrace();
				} finally {
					ch.releaseExclusiveLock();
				}
			}
		}
	}

	private void setStatus(final STATUS iStatus) {
		logger.log(this, Level.INFO, TYPE.CLUSTER, DIRECTION.NONE, "server changed status %s -> %s", status, iStatus);
		status = iStatus;
	}
}
