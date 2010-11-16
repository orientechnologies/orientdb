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

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.util.TimerTask;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.thread.OPollerThread;
import com.orientechnologies.orient.core.OConstants;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.security.OSecurityManager;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.network.OServerNetworkListener;

/**
 * Sends regularly packets using IP multicast protocol to signal the presence in the network.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class ODistributedServerDiscoverySignaler extends OPollerThread {
	private byte[]															discoveryPacket;
	private DatagramPacket											dgram;
	private DatagramSocket											socket;
	private ODistributedServerManager	manager;

	public ODistributedServerDiscoverySignaler(final ODistributedServerManager iManager,
			final OServerNetworkListener iNetworkListener) {
		super(iManager.networkMulticastHeartbeat * 1000, OServer.getThreadGroup(), "DiscoverySignaler");

		manager = iManager;

		final String buffer = ODistributedServerManager.PACKET_HEADER + OConstants.ORIENT_VERSION + "|"
				+ ODistributedServerManager.PROTOCOL_VERSION + "|" + manager.name + "|"
				+ iNetworkListener.getInboundAddr().getHostName() + "|" + iNetworkListener.getInboundAddr().getPort();

		discoveryPacket = OSecurityManager.instance().encrypt(manager.securityAlgorithm, manager.securityKey, buffer.getBytes());

		startTimeoutPresenceTask();

		start();
	}

	private void startTimeoutPresenceTask() {
		Orient.getTimer().schedule(new TimerTask() {
			@Override
			public void run() {
				try {
					// TIMEOUT: STOP TO SEND PACKETS TO BEING DISCOVERED
					sendShutdown();
					manager.becameLeader();

				} catch (Exception e) {
					// AVOID THE TIMER IS NOT SCHEDULED ANYMORE IN CASE OF EXCEPTION
				}
			}
		}, manager.networkTimeoutLeader);
	}

	public void startup() {
		try {
			dgram = new DatagramPacket(discoveryPacket, discoveryPacket.length, manager.networkMulticastAddress,
					manager.networkMulticastPort);
			socket = new DatagramSocket();
		} catch (Exception e) {
			OLogManager.instance().error(this, "Can't startup distributed server discovery signaler", e);
		}
		super.startup();
	}

	@Override
	protected void execute() throws Exception {
		OLogManager.instance().debug(this, "Discovering distributed server nodes using IP Multicast with address %s, port %d...",
				dgram.getAddress(), dgram.getPort());

		try {
			socket.send(dgram);
		} catch (Throwable t) {
			OLogManager.instance().error(this, "Error on sending signal for distributed server presence", t);
		} finally {
		}
	}

	@Override
	public void shutdown() {
		try {
			socket.close();
		} catch (Throwable t) {
		}
		socket = null;
		dgram = null;
		super.shutdown();
	}
}
