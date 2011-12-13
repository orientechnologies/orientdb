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

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.util.TimerTask;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.thread.OPollerThread;
import com.orientechnologies.orient.core.OConstants;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.security.OSecurityManager;
import com.orientechnologies.orient.server.handler.distributed.ODistributedServerConfiguration;
import com.orientechnologies.orient.server.handler.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.network.OServerNetworkListener;

/**
 * Sends packets using IP multicast protocol to signal the presence in the network. If any Leader Node is listening then it will
 * connect to me ASAP, otherwise the timer will expire and I will become the new Cluster Leader Node.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class ODiscoverySignaler extends OPollerThread {
	private byte[]										discoveryPacket;
	private DatagramPacket						dgram;
	private DatagramSocket						socket;
	private ODistributedServerManager	manager;
	private TimerTask									runningTask;

	public ODiscoverySignaler(final ODistributedServerManager iManager, final OServerNetworkListener iNetworkListener) {
		super(iManager.getConfig().networkMulticastHeartbeat * 1000, Orient.getThreadGroup(), "IO-Cluster-DiscoverySignaler");

		manager = iManager;

		final String buffer = ODistributedServerConfiguration.PACKET_HEADER + OConstants.ORIENT_VERSION + "|"
				+ ODistributedServerConfiguration.PROTOCOL_VERSION + "|" + manager.getConfig().name + "|"
				+ iNetworkListener.getInboundAddr().getHostName() + "|" + iNetworkListener.getInboundAddr().getPort();

		discoveryPacket = OSecurityManager.instance().encrypt(manager.getConfig().securityAlgorithm, manager.getConfig().securityKey,
				buffer.getBytes());

		start();
		startTimeoutPresenceTask();
	}

	private void startTimeoutPresenceTask() {
		runningTask = new TimerTask() {
			@Override
			public void run() {
				try {
					if (running)
						// TIMEOUT: STOP TO SEND PACKETS TO BEING DISCOVERED
						manager.becameLeader();

				} catch (Exception e) {
					// AVOID THE TIMER IS NOT SCHEDULED ANYMORE IN CASE OF EXCEPTION
				}
			}
		};

		Orient.getTimer().schedule(runningTask, manager.getConfig().networkTimeoutLeader);
	}

	@Override
	public void startup() {
		try {
			dgram = new DatagramPacket(discoveryPacket, discoveryPacket.length, manager.getConfig().networkMulticastAddress,
					manager.getConfig().networkMulticastPort);
			socket = new DatagramSocket();
		} catch (Exception e) {
			OLogManager.instance().error(this, "Cannot startup distributed server discovery signaler", e);
		}
		super.startup();
	}

	@Override
	protected void execute() throws Exception {
		if (dgram == null) {
			sendShutdown();
			return;
		}

		OLogManager.instance().debug(this, "Sending node presence signal over the network against IP Multicast %s:%d...",
				dgram.getAddress(), dgram.getPort());

		try {
			socket.send(dgram);
		} catch (Throwable t) {
			shutdown();
			OLogManager
					.instance()
					.error(
							this,
							"Error on sending signal for distributed server presence, probably the IP MULTICAST is disabled in current network configuration: %s",
							t.getMessage());
		} finally {
		}
	}

	@Override
	public void shutdown() {
		if (runningTask != null)
			runningTask.cancel();

		try {
			if (socket != null)
				socket.close();
		} catch (Throwable t) {
		}
		socket = null;
		dgram = null;
		super.shutdown();
	}
}
