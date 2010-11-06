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
package com.orientechnologies.orient.server.network.protocol.cluster;

import java.io.IOException;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.server.network.protocol.binary.ONetworkProtocolBinary;

/**
 * Extends binary protocol to include cluster commands.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class ONetworkProtocolCluster extends ONetworkProtocolBinary {
	public ONetworkProtocolCluster() {
		super("Cluster-DB");
	}

	@Override
	protected void parseCommand() throws IOException {
		if (commandType < 80) {
			super.parseCommand();
			return;
		}

		switch (commandType) {
		case OChannelClusterProtocol.NODECLUSTER_CONNECT: {
			data.commandInfo = "Cluster connection";

			sendOk(clientTxId);

			break;
		}

		default:
			data.commandInfo = "Command not supported";
			OLogManager.instance().error(this, "Request not supported. Code: " + commandType);
			channel.clearInput();
			sendError(clientTxId, null);
		}
	}
}
