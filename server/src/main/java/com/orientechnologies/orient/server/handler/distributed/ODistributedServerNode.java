/*
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
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.tx.OTransactionEntry;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryClient;
import com.orientechnologies.orient.server.network.protocol.distributed.OChannelDistributedProtocol;

/**
 * Contains all the information about a cluster node.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class ODistributedServerNode {
	public enum STATUS {
		DISCONNECTED, CONNECTING, SYNCHRONIZED, UNSYNCHRONIZED
	}

	public String																networkAddress;
	public int																	networkPort;
	public Date																	joinedOn;
	private ODistributedServerManager	discoveryManager;
	public OChannelBinaryClient									network;
	private OContextConfiguration								configuration;
	private STATUS															status		= STATUS.DISCONNECTED;
	private Map<String, Long>										storages	= new HashMap<String, Long>();
	private List<OTransactionEntry<?>>					bufferedChanges;

	public ODistributedServerNode(final ODistributedServerManager iNode, final String iServerAddress, final int iServerPort) {
		discoveryManager = iNode;
		networkAddress = iServerAddress;
		networkPort = iServerPort;
		joinedOn = new Date();
		configuration = new OContextConfiguration();
		status = STATUS.CONNECTING;
	}

	public void connect(final int iTimeout) throws IOException {
		configuration.setValue(OGlobalConfiguration.NETWORK_SOCKET_TIMEOUT, iTimeout);
		network = new OChannelBinaryClient(networkAddress, networkPort, configuration);

		OLogManager.instance().info(this, "Connecting to remote cluster node %s:%d...", networkAddress, networkPort);

		network.out.writeByte(OChannelDistributedProtocol.SERVERNODE_CONNECT);
		network.out.writeInt(0);
		network.flush();

		readStatus();

		int tot = network.readInt();
		for (int i = 0; i < tot; ++i)
			storages.put(network.readString(), network.readLong());

		printDatabaseTable();

		status = STATUS.UNSYNCHRONIZED;
		OLogManager.instance().info(this, "Connection to remote cluster node %s:%d has been established", networkAddress, networkPort);
	}

	public long createRecord(final ORecordInternal<?> iRecord, final String iClusterName) throws IOException {
		if (status == STATUS.DISCONNECTED) {
			// BUFFERIZE THE CHANGE
			bufferedChanges.add(new OTransactionEntry<ORecordInternal<?>>(iRecord, OTransactionEntry.CREATED, iClusterName));
			return -1;
		} else {
			network.writeByte(OChannelDistributedProtocol.REQUEST_RECORD_CREATE);
			network.writeInt(0);
			network.writeShort((short) iRecord.getDatabase().getClusterIdByName(iClusterName));
			network.writeBytes(iRecord.toStream());
			network.writeByte(iRecord.getRecordType());
			network.flush();

			readStatus();

			return network.readLong();
		}
	}

	public boolean sendHeartBeat(final int iNetworkTimeout) {
		configuration.setValue(OGlobalConfiguration.NETWORK_SOCKET_TIMEOUT, iNetworkTimeout);
		OLogManager.instance().debug(this, "Sending keepalive message to remote distributed server node %s:%d...", networkAddress,
				networkPort);

		try {
			network.out.writeByte(OChannelDistributedProtocol.SERVERNODE_KEEPALIVE);
			network.out.writeInt(0);
			network.flush();

			if (readStatus())
				return true;

		} catch (Exception e) {
		}

		return false;
	}

	/**
	 * Set the node as DISCONNECTED and begin to collect changes up to iServerOutSynchMaxBuffers entries.
	 * 
	 * @param iServerOutSynchMaxBuffers
	 *          max number of entries to collect before to remove it completely from the server node list
	 */
	public void startToCollectChanges(final int iServerOutSynchMaxBuffers) {
		bufferedChanges = new ArrayList<OTransactionEntry<?>>();
	}

	public void startSynchronization() {
		final ODocument config = createDatabaseConfiguration();

		// SEND THE LAST CONFIGURATION TO THE NODE
		try {
			network.out.writeByte(OChannelDistributedProtocol.SERVERNODE_DB_CONFIG);
			network.out.writeInt(0);
			network.writeBytes(config.toStream());
			network.flush();

			if (readStatus())
				;
		} catch (Exception e) {
		}

	}

	@Override
	public String toString() {
		final StringBuilder builder = new StringBuilder();
		builder.append(networkAddress).append(":").append(networkPort);
		return builder.toString();
	}

	private boolean readStatus() throws IOException {
		return network.readByte() != OChannelDistributedProtocol.RESPONSE_STATUS_ERROR;
	}

	private ODocument createDatabaseConfiguration() {
		final ODocument config = new ODocument();

		config.field("servers",
				new ODocument(discoveryManager.getName(), new ODocument("update-delay", discoveryManager.getServerUpdateDelay())));
		config.field("clusters", new ODocument("*", new ODocument("owner", discoveryManager.getName())));

		return config;
	}

	private void printDatabaseTable() {
		OLogManager.instance().info(this, "+--------------------------------+----------------+----------------+");
		OLogManager.instance().info(this, "| STORAGE                        | LOCAL VERSION  | REMOTE VERSION |");
		OLogManager.instance().info(this, "+--------------------------------+----------------+----------------+");

		for (OStorage s : Orient.instance().getStorages()) {
			if (storages.containsKey(s.getName()))
				OLogManager.instance().info(this, "| %-30s | %14d | %14d |", s.getName(), s.getVersion(), storages.get(s.getName()));
			else
				OLogManager.instance().info(this, "| %-30s | %14d |    unavailable |", s.getName(), s.getVersion());
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
				OLogManager.instance().info(this, "| %-30s |    unavailable | %14d |", stg.getKey(), stg.getValue());
		}

		OLogManager.instance().info(this, "+--------------------------------+----------------+----------------+");
	}
}
