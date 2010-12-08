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

import java.util.List;
import java.util.TimerTask;

import com.orientechnologies.orient.server.handler.distributed.ODistributedServerNodeRemote.STATUS;

/**
 * Checks that active nodes are up and running
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class ODistributedServerNodeChecker extends TimerTask {
	private ODistributedServerManager	manager;

	public ODistributedServerNodeChecker(ODistributedServerManager manager) {
		this.manager = manager;
	}

	@Override
	public void run() {
		final List<ODistributedServerNodeRemote> nodeList = manager.getNodeList();

		if (nodeList == null)
			// NO NODES, JUST RETURN
			return;

		try {

			// CHECK EVERY SINGLE NODE
			for (ODistributedServerNodeRemote node : nodeList) {
				if (node.getStatus() == STATUS.CONNECTED)
					if (!node.sendHeartBeat(manager.networkTimeoutLeader)) {
						manager.handleNodeFailure(node);
					}
			}

			nodeList.clear();

		} catch (Exception e) {
			// AVOID THE TIMER IS NOT SCHEDULED ANYMORE IN CASE OF EXCEPTION
			e.printStackTrace();
		}
	}
}
