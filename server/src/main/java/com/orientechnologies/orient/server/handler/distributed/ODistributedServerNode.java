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
import com.orientechnologies.orient.enterprise.channel.distributed.OChannelDistributedProtocol;

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

	public String																				networkAddress;
	public int																					networkPort;
	public Date																					joinedOn;
	private ODistributedServerManager										manager;
	public OChannelBinaryClient													network;
	private OContextConfiguration												configuration;
	private STATUS																			status					= STATUS.DISCONNECTED;
	private Map<String, Long>														storages				= new HashMap<String, Long>();
	private List<OTransactionEntry<ORecordInternal<?>>>	bufferedChanges	= new ArrayList<OTransactionEntry<ORecordInternal<?>>>();

	public ODistributedServerNode(final ODistributedServerManager iNode, final String iServerAddress, final int iServerPort) {
		manager = iNode;
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

		network.out.writeByte(OChannelDistributedProtocol.REQUEST_DISTRIBUTED_CONNECT);
		network.out.writeInt(0);
		network.flush();

		readStatus();

		final int tot = network.readInt();
		for (int i = 0; i < tot; ++i)
			storages.put(network.readString(), network.readLong());

		printDatabaseTable();

		status = STATUS.UNSYNCHRONIZED;
		OLogManager.instance().info(this, "Connection to remote cluster node %s:%d has been established", networkAddress, networkPort);
	}

	public void sendRequest(final OTransactionEntry<ORecordInternal<?>> iRequest) throws IOException {
		if (status == STATUS.DISCONNECTED) {
			synchronized (bufferedChanges) {
				if (bufferedChanges.size() > manager.serverOutSynchMaxBuffers) {
					// BUFFER EXCEEDS THE CONFIGURED LIMIT: REMOVE MYSELF AS NODE
					manager.removeNode(this);
					bufferedChanges.clear();
				} else
					// BUFFERIZE THE REQUEST
					bufferedChanges.add(iRequest);
			}
		} else {
			final ORecordInternal<?> record = iRequest.getRecord();

			try {
				switch (iRequest.status) {
				case OTransactionEntry.CREATED:
					network.writeByte(OChannelDistributedProtocol.REQUEST_RECORD_CREATE);
					network.writeInt(0);
					network.writeShort((short) record.getIdentity().getClusterId());
					network.writeBytes(record.toStream());
					network.writeByte(record.getRecordType());
					network.flush();

					network.readStatus();
					break;

				case OTransactionEntry.UPDATED:
					network.writeByte(OChannelDistributedProtocol.REQUEST_RECORD_UPDATE);
					network.writeInt(0);
					network.writeShort((short) record.getIdentity().getClusterId());
					network.writeLong(record.getIdentity().getClusterPosition());
					network.writeBytes(record.toStream());
					network.writeInt(record.getVersion());
					network.writeByte(record.getRecordType());
					network.flush();

					readStatus();

					network.readInt();
					break;

				case OTransactionEntry.DELETED:
					network.writeByte(OChannelDistributedProtocol.REQUEST_RECORD_DELETE);
					network.writeInt(0);
					network.writeShort((short) record.getIdentity().getClusterId());
					network.writeLong(record.getIdentity().getClusterPosition());
					network.writeInt(record.getVersion());
					network.flush();

					readStatus();

					network.readLong();
					break;
				}
			} catch (RuntimeException e) {
				// RE-THROW THE EXCEPTION
				throw e;
			}
		}
	}

	public boolean sendHeartBeat(final int iNetworkTimeout) {
		configuration.setValue(OGlobalConfiguration.NETWORK_SOCKET_TIMEOUT, iNetworkTimeout);
		OLogManager.instance().debug(this, "Sending keepalive message to remote distributed server node %s:%d...", networkAddress,
				networkPort);

		try {
			network.out.writeByte(OChannelDistributedProtocol.REQUEST_DISTRIBUTED_HEARTBEAT);
			network.out.writeInt(0);
			network.flush();

			readStatus();

		} catch (Exception e) {
			return false;
		}

		return true;
	}

	/**
	 * Sets the node as DISCONNECTED and begin to collect changes up to iServerOutSynchMaxBuffers entries.
	 * 
	 * @param iServerOutSynchMaxBuffers
	 *          max number of entries to collect before to remove it completely from the server node list
	 */
	public void setAsTemporaryDisconnected(final int iServerOutSynchMaxBuffers) {
		if (status != STATUS.DISCONNECTED) {
			status = STATUS.DISCONNECTED;
		}
	}

	public void startSynchronization() {
		final ODocument config = createDatabaseConfiguration();

		// SEND THE LAST CONFIGURATION TO THE NODE
		try {
			network.out.writeByte(OChannelDistributedProtocol.REQUEST_DISTRIBUTED_DB_CONFIG);
			network.out.writeInt(0);
			network.writeBytes(config.toStream());
			network.flush();

			readStatus();
			
			if (status == STATUS.DISCONNECTED)
				synchronizeDelta();

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	@Override
	public String toString() {
		final StringBuilder builder = new StringBuilder();
		builder.append(networkAddress).append(":").append(networkPort);
		return builder.toString();
	}

	private void synchronizeDelta() throws IOException {
		synchronized (bufferedChanges) {
			if (bufferedChanges.isEmpty())
				return;

			OLogManager.instance().info(this, "Started realignment of remote node %s:%d after a reconnection. Found %d updates",
					networkAddress, networkPort, bufferedChanges.size());

			for (OTransactionEntry<ORecordInternal<?>> entry : bufferedChanges) {
				sendRequest(entry);
			}
			bufferedChanges.clear();

			status = STATUS.UNSYNCHRONIZED;
		}

		OLogManager.instance().info(this, "Realignment of remote node %s:%d completed", networkAddress, networkPort);
	}

	private int readStatus() throws IOException {
		return network.readStatus();
	}

	private ODocument createDatabaseConfiguration() {
		final ODocument config = new ODocument();

		config.field("servers", new ODocument(manager.getName(), new ODocument("update-delay", manager.getServerUpdateDelay())));
		config.field("clusters", new ODocument("*", new ODocument("owner", manager.getName())));

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

	public STATUS getStatus() {
		return status;
	}
}
