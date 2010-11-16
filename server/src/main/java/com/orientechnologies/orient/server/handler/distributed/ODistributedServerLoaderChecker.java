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

import java.util.TimerTask;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.server.network.protocol.distributed.ONetworkProtocolDistributed;

/**
 * Checks the heartbeat sent by the leader node. If too much time is gone it tries to became the new leader.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class ODistributedServerLoaderChecker extends TimerTask {
	private ODistributedServerManager		manager;
	private ONetworkProtocolDistributed	protocol;
	private long												heartBeatDelay;

	public ODistributedServerLoaderChecker(final ODistributedServerManager iManager, final ONetworkProtocolDistributed iProtocol) {
		this.manager = iManager;
		this.protocol = iProtocol;

		// COMPUTE THE HEARTBEAT THRESHOLD AS THE 30% MORE THAN THE HEARTBEAT TIME
		heartBeatDelay = manager.getNetworkHeartbeatDelay() * 130 / 100;
	}

	@Override
	public void run() {
		final long time = System.currentTimeMillis() - protocol.getLastHeartBeat();
		if (time > heartBeatDelay) {
			// NO LEADER HEARTBEAT RECEIVED FROM LONG TIME: BECAME THE LEADER!
			OLogManager.instance().warn(this,
					"No heartbeat message has been received from the Leader node (last was %dms ago). Trying to became the new Leader...",
					time);

			cancel();

			manager.becameLeader(true);
			return;
		}
	}
}
