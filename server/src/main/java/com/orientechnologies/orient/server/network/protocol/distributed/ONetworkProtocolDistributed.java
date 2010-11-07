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
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.server.network.protocol.binary.ONetworkProtocolBinary;

/**
 * Extends binary protocol to include cluster commands.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class ONetworkProtocolDistributed extends ONetworkProtocolBinary {
	public ONetworkProtocolDistributed() {
		super("Cluster-DB");
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
		case OChannelDistributedProtocol.NODECLUSTER_CONNECT: {
			data.commandInfo = "Cluster connection";

			sendOk(clientTxId);

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

		default:
			data.commandInfo = "Command not supported";
			OLogManager.instance().error(this, "Request not supported. Code: " + commandType);
			channel.clearInput();
			sendError(clientTxId, null);
		}
	}
}
