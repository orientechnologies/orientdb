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
package com.orientechnologies.orient.server.network.protocol.distributed;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;
import com.orientechnologies.orient.server.OServerMain;
import com.orientechnologies.orient.server.handler.distributed.discovery.ODistributedServerDiscoveryManager;
import com.orientechnologies.orient.server.network.protocol.binary.ONetworkProtocolBinary;

/**
 * Extends binary protocol to include cluster commands.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class ONetworkProtocolDistributed extends ONetworkProtocolBinary {
	private ODistributedServerDiscoveryManager	manager;

	public ONetworkProtocolDistributed() {
		super("Distributed-DB");

		manager = OServerMain.server().getHandler(ODistributedServerDiscoveryManager.class);
		if (manager == null)
			throw new OConfigurationException(
					"Can't find a ODistributedServerDiscoveryManager instance registered as handler. Check the server configuration in the handlers section.");
	}

	@Override
	protected void parseCommand() throws IOException {
		if (commandType < 80) {
			// BINARY REQUESTS
			super.parseCommand();
			return;
		}

		// DISTRIBUTED SERVER REQUESTS
		switch (commandType) {
		case OChannelDistributedProtocol.SERVERNODE_KEEPALIVE:
			data.commandInfo = "Keep-alive";
			channel.writeByte(OChannelBinaryProtocol.RESPONSE_STATUS_OK);
			break;

		case OChannelDistributedProtocol.SERVERNODE_CONNECT: {
			data.commandInfo = "Cluster connection";

			manager.receivedLeaderConnection(this);

			channel.writeByte(OChannelBinaryProtocol.RESPONSE_STATUS_OK);

			// TRANSMITS FOR ALL THE CONFIGURED STORAGES: STORAGE/VERSION
			Map<String, Long> storages = new HashMap<String, Long>();
			for (OStorage stg : Orient.instance().getStorages()) {
				storages.put(stg.getName(), stg.getVersion());
			}

			channel.writeInt(storages.size());
			for (Entry<String, Long> s : storages.entrySet()) {
				channel.writeString(s.getKey());
				channel.writeLong(s.getValue());
			}

			break;
		}

		case OChannelDistributedProtocol.SERVERNODE_DB_CONFIG: {
			data.commandInfo = "Update db configuration from server node leader";
			ODocument config = (ODocument) new ODocument().fromStream(channel.readBytes());
			config.getClassName();
			channel.writeByte(OChannelBinaryProtocol.RESPONSE_STATUS_OK);
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
