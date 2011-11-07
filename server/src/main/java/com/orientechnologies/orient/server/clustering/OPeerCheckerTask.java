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

import java.util.List;
import java.util.TimerTask;

/**
 * Checks that active nodes are up and running
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OPeerCheckerTask extends TimerTask {
	private OLeaderNode	leader;

	public OPeerCheckerTask(OLeaderNode oDistributedServerLeader) {
		this.leader = oDistributedServerLeader;
	}

	@Override
	public void run() {
		try {
			final List<ORemotePeer> nodeList = leader.getPeerNodeList();

			if (nodeList == null || nodeList.size() == 0)
				// NO NODES, JUST RETURN
				return;

			// CHECK EVERY SINGLE NODE
			for (ORemotePeer node : nodeList) {
				if (!node.sendHeartBeat(leader.getManager().getConfig().networkTimeoutLeader))
					leader.handlePeerNodeFailure(node);
			}

		} catch (Exception e) {
			// AVOID THE TIMER IS NOT SCHEDULED ANYMORE IN CASE OF EXCEPTION
			e.printStackTrace();
		}
	}
}
