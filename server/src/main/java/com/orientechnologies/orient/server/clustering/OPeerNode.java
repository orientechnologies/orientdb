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

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.server.handler.distributed.ODistributedServerManager;

/**
 * Peer node.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OPeerNode {
	private final ODistributedServerManager	manager;
	private ODiscoveryListener							discoveryListener;
	private OLeaderCheckerTask							leaderCheckerTask;
	private long														lastHeartBeat;

	public OPeerNode(final ODistributedServerManager iManager) {
		manager = iManager;
		OLogManager.instance().info(this, "Cluster '%s' joined", iManager.getConfig().name);

		// FIRST TIME: SCHEDULE THE HEARTBEAT CHECKER
		leaderCheckerTask = new OLeaderCheckerTask(this);
		Orient.getTimer().schedule(leaderCheckerTask, manager.getConfig().networkHeartbeatDelay,
				manager.getConfig().networkHeartbeatDelay / 2);
	}

	public void shutdown() {
		if (leaderCheckerTask != null) {
			leaderCheckerTask.cancel();
			leaderCheckerTask = null;
		}

		if (discoveryListener != null) {
			discoveryListener.sendShutdown();
			discoveryListener = null;
		}
	}

	public long getLastHeartBeat() {
		return lastHeartBeat;
	}

	public void updateHeartBeatTime() {
		this.lastHeartBeat = System.currentTimeMillis();
	}

	public ODistributedServerManager getManager() {
		return manager;
	}

	public void updateConfigurationToLeader(String dbUrl, String remoteServerName, boolean synchronousMode) {
		// TODO Auto-generated method stub
	}
}
