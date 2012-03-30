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
import java.net.DatagramPacket;
import java.net.MulticastSocket;
import java.util.logging.Level;

import com.orientechnologies.common.io.OIOException;
import com.orientechnologies.common.thread.OSoftThread;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.security.OSecurityManager;
import com.orientechnologies.orient.server.clustering.OClusterLogger;
import com.orientechnologies.orient.server.clustering.OClusterLogger.DIRECTION;
import com.orientechnologies.orient.server.clustering.OClusterLogger.TYPE;
import com.orientechnologies.orient.server.handler.distributed.ODistributedServerConfiguration;
import com.orientechnologies.orient.server.handler.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.network.OServerNetworkListener;

public class ODiscoveryListener extends OSoftThread {
	private final byte[]							recvBuffer	= new byte[50000];
	private DatagramPacket						dgram;
	private ODistributedServerManager	manager;
	private OServerNetworkListener		binaryNetworkListener;

	private MulticastSocket						socket;
	private OClusterLogger						logger			= new OClusterLogger();

	public ODiscoveryListener(final ODistributedServerManager iManager, final OServerNetworkListener iNetworkListener) {
		super(Orient.getThreadGroup(), "OrientDB Distributed-DiscoveryListener");

		manager = iManager;
		binaryNetworkListener = iNetworkListener;

		logger.log(this, Level.INFO, TYPE.CLUSTER, DIRECTION.IN, "listening for distributed nodes on IP multicast: %s:%d",
				iManager.getConfig().networkMulticastAddress, iManager.getConfig().networkMulticastPort);

		dgram = new DatagramPacket(recvBuffer, recvBuffer.length);
		try {
			socket = new MulticastSocket(iManager.getConfig().networkMulticastPort);
			socket.joinGroup(iManager.getConfig().networkMulticastAddress);
		} catch (IOException e) {
			throw new OIOException(
					"Cannot startup the Discovery Listener service to catch distributed server nodes, probably the IP MULTICAST is disabled in current network configuration: "
							+ e.getMessage());
		}

		start();
	}

	@Override
	protected void execute() throws Exception {
		try {
			// RESET THE LENGTH TO RE-RECEIVE THE PACKET
			dgram.setLength(recvBuffer.length);

			// BLOCKS UNTIL SOMETHING IS RECEIVED OR SOCKET SHUTDOWN
			socket.receive(dgram);

			logger.setNode(dgram.getAddress() + ":" + dgram.getPort());
			logger.log(this, Level.FINE, TYPE.CLUSTER, DIRECTION.IN, "received multicast packet %d bytes from", dgram.getLength());

			final byte[] buffer = new byte[dgram.getLength()];
			System.arraycopy(dgram.getData(), 0, buffer, 0, buffer.length);

			try {
				String packet = new String(OSecurityManager.instance().decrypt(manager.getConfig().securityAlgorithm,
						manager.getConfig().securityKey, buffer));

				// UNPACK DATA
				String[] parts = packet.trim().split("\\|");

				int i = 0;

				if (!parts[i].startsWith(ODistributedServerConfiguration.PACKET_HEADER)) {
					logger.log(this, Level.FINE, TYPE.CLUSTER, DIRECTION.IN, "packet discarded because invalid");
					return;
				}

				if (Integer.parseInt(parts[++i]) != ODistributedServerConfiguration.PROTOCOL_VERSION) {
					logger.log(this, Level.FINE, TYPE.CLUSTER, DIRECTION.IN,
							"received bad multicast packet with version %s not equals to the current %d", parts[i],
							ODistributedServerConfiguration.PROTOCOL_VERSION);
					return;
				}

				if (!parts[++i].equals(manager.getConfig().name)) {
					logger
							.log(this, Level.FINE, TYPE.CLUSTER, DIRECTION.IN,
									"received bad multicast packet with cluster name %s not equals to the current %s", parts[i],
									manager.getConfig().name);
					return;
				}

				String configuredServerAddress = parts[++i];
				String sourceServerAddress = dgram.getAddress().getHostAddress();
				final int serverPort = Integer.parseInt(parts[++i]);

				// CHECK IF THE PACKET WAS SENT BY MYSELF
				if (configuredServerAddress.equals(binaryNetworkListener.getInboundAddr().getHostName())
						&& serverPort == binaryNetworkListener.getInboundAddr().getPort()) {
					// IT'S ME, JUST IGNORE
					logger.log(this, Level.FINE, TYPE.CLUSTER, DIRECTION.IN, "ignored because sent by myself");
					return;
				}

				// GOOD PACKET, PASS TO THE DISTRIBUTED NODE MANAGER THIS INFO
				if (manager.getLeader() == null) {
					logger.log(this, Level.FINE, TYPE.CLUSTER, DIRECTION.IN, "packet discarded because I'm not the leader");
					return;
				}

				manager.getLeader().connect2Peer(new String[] { sourceServerAddress, configuredServerAddress }, serverPort);

			} catch (Exception e) {
				// WRONG PACKET
				logger.log(this, Level.FINE, TYPE.CLUSTER, DIRECTION.IN, "received wrong packet from multicast IP", e);
			}
		} catch (Throwable t) {
			logger.error(this, TYPE.CLUSTER, DIRECTION.IN, "Error on executing request", t, null);
		} finally {
		}
	}

}
