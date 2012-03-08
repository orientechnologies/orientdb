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
package com.orientechnologies.orient.server.clustering.peer;

import java.io.IOException;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryServer;
import com.orientechnologies.orient.server.clustering.OClusterNetworkProtocol;
import com.orientechnologies.orient.server.handler.distributed.OClusterProtocol;
import com.orientechnologies.orient.server.handler.distributed.ODistributedServerManager;

/**
 * Peer node. In a clustered configuration there is only one leader and 0-N peer nodes.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OPeerNode {
	private final ODistributedServerManager	manager;
	private OLeaderCheckerTask							leaderCheckerTask;
	private long														lastHeartBeat;
	private OClusterNetworkProtocol					leaderConnection;

	public OPeerNode(final ODistributedServerManager iManager, final OClusterNetworkProtocol iConnection) {
		manager = iManager;
		leaderConnection = iConnection;

		OLogManager.instance().info(this, "Cluster <%s> joined as peer node", iManager.getConfig().name);

		// FIRST TIME: SCHEDULE THE HEARTBEAT CHECKER
		leaderCheckerTask = new OLeaderCheckerTask(this);
		Orient.getTimer().schedule(leaderCheckerTask, manager.getConfig().networkHeartbeatDelay,
				manager.getConfig().networkHeartbeatDelay / 2);

		updateHeartBeatTime();
	}

	public void shutdown() {
		if (leaderCheckerTask != null) {
			leaderCheckerTask.cancel();
			leaderCheckerTask = null;
		}
	}

	public long getLastHeartBeat() {
		return lastHeartBeat;
	}

	public long updateHeartBeatTime() {
		final long now = System.currentTimeMillis();
		final long lastInterval = now - this.lastHeartBeat;
		this.lastHeartBeat = now;
		return lastInterval;
	}

	public ODistributedServerManager getManager() {
		return manager;
	}

	public void updateConfigurationToLeader() throws IOException {
		if (manager.isLeader())
			return;

		final ODocument doc = new ODocument();
		doc.field("availableDatabases", manager.getReplicator().getLocalDatabaseConfiguration());

		final OChannelBinaryServer channel = ((OChannelBinaryServer) leaderConnection.getChannel());

		channel.acquireExclusiveLock();
		try {
			channel.writeByte(OChannelBinaryProtocol.PUSH_DATA);
			channel.writeInt(Integer.MIN_VALUE);
			channel.writeByte(OClusterProtocol.PUSH_LEADER_AVAILABLE_DBS);

			channel.writeBytes(doc.toStream());
			channel.flush();
		} finally {
			channel.releaseExclusiveLock();
		}

		manager.getReplicator().updateConfiguration(new ODocument(channel.readBytes()));

	}
}
