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

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryClient;
import com.orientechnologies.orient.server.handler.distributed.discovery.ODistributedServerDiscoveryManager;
import com.orientechnologies.orient.server.network.protocol.distributed.OChannelDistributedProtocol;

/**
 * Contains all the information about a cluster node.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class ODistributedServerNode {
	public enum STATUS {
		CONNECTING, SYNCHRONIZED, UNSYNCHRONIZED
	}

	public String																clusterNetworkAddress;
	public int																	clusterNetworkPort;
	public Date																	joinedOn;
	private ODistributedServerDiscoveryManager	discoveryManager;
	public OChannelBinaryClient									network;
	private OContextConfiguration								configuration;
	private STATUS															status;
	private Map<String, Long>										storages	= new HashMap<String, Long>();

	public ODistributedServerNode(final ODistributedServerDiscoveryManager iNode, final String iServerAddress, final int iServerPort) {
		discoveryManager = iNode;
		clusterNetworkAddress = iServerAddress;
		clusterNetworkPort = iServerPort;
		joinedOn = new Date();
		configuration = new OContextConfiguration();
		status = STATUS.CONNECTING;
	}

	public void connect(final int iTimeout) throws IOException {
		network = new OChannelBinaryClient(clusterNetworkAddress, clusterNetworkPort, configuration);

		OLogManager.instance().info(this, "Connecting to remote cluster node %s:%d...", clusterNetworkAddress, clusterNetworkPort);

		network.out.writeByte(OChannelDistributedProtocol.NODECLUSTER_CONNECT);
		network.out.writeInt(0);
		network.flush();

		readStatus();

		int tot = network.readInt();
		for (int i = 0; i < tot; ++i)
			storages.put(network.readString(), network.readLong());

		OLogManager.instance().info(this, "+--------------------------------+-----------------+----------------+");
		OLogManager.instance().info(this, "| STORAGE                        | LOCAL VERSION   | REMOTE VERSION |");
		OLogManager.instance().info(this, "+--------------------------------+-----------------+----------------+");

		for (OStorage s : Orient.instance().getStorages()) {
			if (storages.containsKey(s.getName()))
				OLogManager.instance().info(this, "| %-30s | %15d | %15d |", s.getName(), s.getVersion(), storages.get(s.getName()));
			else
				OLogManager.instance().info(this, "| %-30s | %15d | unavailable    |", s.getName(), s.getVersion());
		}

		boolean found;
		for (Entry<String, Long> stg : storages.entrySet()) {
			found = false;
			for (OStorage s : Orient.instance().getStorages()) {
				if (s.getName().equals(stg.getKey())) {
					found = true;
					break;
				}
			}

			if (!found)
				OLogManager.instance().info(this, "| %-30s | unavailable   | %15d |", stg.getKey(), stg.getValue());
		}

		OLogManager.instance().info(this, "+--------------------------------+-----------------+----------------+");

		status = STATUS.UNSYNCHRONIZED;
		OLogManager.instance().info(this, "Connection to remote cluster node %s:%d has been established", clusterNetworkAddress,
				clusterNetworkPort);
	}

	public void startSynchronization() {
		// TODO Auto-generated method stub

	}

	private boolean readStatus() throws IOException {
		return network.readByte() != OChannelDistributedProtocol.RESPONSE_STATUS_ERROR;
	}
}
