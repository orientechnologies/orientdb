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
import java.util.List;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.tool.ODatabaseExport;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.tx.OTransactionEntry;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryClient;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryClientSynch;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryOutputStream;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;
import com.orientechnologies.orient.enterprise.channel.distributed.OChannelDistributedProtocol;

/**
 * Contains all the information about a cluster node.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class ODistributedServerNode implements OCommandOutputListener {
	public enum STATUS {
		DISCONNECTED, CONNECTING, CONNECTED, SYNCHRONIZING
	}

	private String																			id;
	public String																				networkAddress;
	public int																					networkPort;
	public Date																					joinedOn;
	private ODistributedServerManager										manager;
	public OChannelBinaryClient													channel;
	private OContextConfiguration												configuration;
	private volatile STATUS															status					= STATUS.DISCONNECTED;
	private List<OTransactionEntry<ORecordInternal<?>>>	bufferedChanges	= new ArrayList<OTransactionEntry<ORecordInternal<?>>>();
	private int																					clientTxId			= 0;

	public ODistributedServerNode(final ODistributedServerManager iNode, final String iServerAddress, final int iServerPort) {
		manager = iNode;
		networkAddress = iServerAddress;
		networkPort = iServerPort;
		joinedOn = new Date();
		configuration = new OContextConfiguration();
		id = networkAddress + ":" + networkPort;
		status = STATUS.CONNECTING;
	}

	public void connect(final int iTimeout) throws IOException {
		configuration.setValue(OGlobalConfiguration.NETWORK_SOCKET_TIMEOUT, iTimeout);
		channel = new OChannelBinaryClientSynch(networkAddress, networkPort, configuration);

		OChannelBinaryProtocol.checkProtocolVersion(channel);

		OLogManager.instance().info(this, "Connecting to remote cluster node %s:%d...", networkAddress, networkPort);

		channel.out.writeByte(OChannelDistributedProtocol.REQUEST_DISTRIBUTED_CONNECT);
		channel.out.writeInt(0);
		channel.flush();

		readStatus();

		status = STATUS.CONNECTED;
		OLogManager.instance().info(this, "Connection to remote cluster node %s:%d has been established", networkAddress, networkPort);
	}

	public void sendRequest(final OTransactionEntry<ORecordInternal<?>> iRequest) throws IOException {
		if (status == STATUS.DISCONNECTED) {
			synchronized (bufferedChanges) {
				if (bufferedChanges.size() > manager.serverOutSynchMaxBuffers) {
					// BUFFER EXCEEDS THE CONFIGURED LIMIT: REMOVE MYSELF AS NODE
					manager.removeNode(this);
					bufferedChanges.clear();
				} else {
					// BUFFERIZE THE REQUEST
					bufferedChanges.add(iRequest);

					OLogManager.instance().info(this, "Server node '%s' is temporary disconnected, buffering change %d/%d for the record %s",
							id, bufferedChanges.size(), manager.serverOutSynchMaxBuffers, iRequest.getRecord().getIdentity());
				}
			}
		} else {
			final ORecordInternal<?> record = iRequest.getRecord();

			try {
				switch (iRequest.status) {
				case OTransactionEntry.CREATED:
					channel.acquireExclusiveLock();
					try {
						channel.writeByte(OChannelDistributedProtocol.REQUEST_RECORD_CREATE);
						channel.writeInt(0);
						channel.writeShort((short) record.getIdentity().getClusterId());
						channel.writeBytes(record.toStream());
						channel.writeByte(record.getRecordType());
						channel.flush();

						readStatus();

					} finally {
						channel.releaseExclusiveLock();
					}
					break;

				case OTransactionEntry.UPDATED:
					channel.acquireExclusiveLock();
					try {
						channel.writeByte(OChannelDistributedProtocol.REQUEST_RECORD_UPDATE);
						channel.writeInt(0);
						channel.writeShort((short) record.getIdentity().getClusterId());
						channel.writeLong(record.getIdentity().getClusterPosition());
						channel.writeBytes(record.toStream());
						channel.writeInt(record.getVersion());
						channel.writeByte(record.getRecordType());
						channel.flush();

						readStatus();

						channel.readInt();
					} finally {
						channel.releaseExclusiveLock();
					}
					break;

				case OTransactionEntry.DELETED:
					channel.acquireExclusiveLock();
					try {
						channel.writeByte(OChannelDistributedProtocol.REQUEST_RECORD_DELETE);
						channel.writeInt(0);
						channel.writeShort((short) record.getIdentity().getClusterId());
						channel.writeLong(record.getIdentity().getClusterPosition());
						channel.writeInt(record.getVersion());
						channel.flush();

						readStatus();

						channel.readLong();
					} finally {
						channel.releaseExclusiveLock();
					}
					break;
				}
			} catch (RuntimeException e) {
				// RE-THROW THE EXCEPTION
				throw e;
			}
		}
	}

	public void sendConfiguration(final String iDatabaseName) {
		OLogManager.instance().info(this, "Sending configuration to distributed server node %s:%d...", networkAddress, networkPort);

		channel.acquireExclusiveLock();

		try {
			channel.out.writeByte(OChannelDistributedProtocol.REQUEST_DISTRIBUTED_DB_CONFIG);
			channel.out.writeInt(0);
			channel.writeBytes(manager.getClusterConfiguration(iDatabaseName).toStream());
			channel.flush();

			readStatus();

		} catch (Exception e) {
			e.printStackTrace();

		} finally {
			channel.releaseExclusiveLock();
		}
	}

	public boolean sendHeartBeat(final int iNetworkTimeout) {
		configuration.setValue(OGlobalConfiguration.NETWORK_SOCKET_TIMEOUT, iNetworkTimeout);
		OLogManager.instance()
				.debug(this, "Sending keepalive message to distributed server node %s:%d...", networkAddress, networkPort);

		channel.acquireExclusiveLock();

		try {
			channel.out.writeByte(OChannelDistributedProtocol.REQUEST_DISTRIBUTED_HEARTBEAT);
			channel.out.writeInt(0);
			channel.flush();

			readStatus();

		} catch (Exception e) {
			return false;

		} finally {
			channel.releaseExclusiveLock();
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
		// SEND THE LAST CONFIGURATION TO THE NODE
		// sendConfiguration(iDatabaseName);

		channel.acquireExclusiveLock();
		try {
			if (status == STATUS.DISCONNECTED)
				synchronizeDelta();

		} catch (Exception e) {
			e.printStackTrace();

		} finally {
			channel.releaseExclusiveLock();
		}
	}

	public void shareDatabase(final ODatabaseRecord<?> iDatabase, final String iRemoteServerName, final String iEngineName,
			final boolean iSynchronousMode) throws IOException {
		if (status == STATUS.DISCONNECTED)
			throw new ODistributedSynchronizationException("Can't share database '" + iDatabase.getName() + "' on remote server node '"
					+ iRemoteServerName + "' because is disconnected");

		final String dbName = iDatabase.getName();

		channel.acquireExclusiveLock();

		try {
			status = STATUS.SYNCHRONIZING;

			OLogManager.instance().info(this, "Sharing database '" + dbName + "' to remote server " + iRemoteServerName + "...");

			// EXECUTE THE REQUEST ON REMOTE SERVER NODE
			channel.writeByte(OChannelDistributedProtocol.REQUEST_DISTRIBUTED_DB_SHARE_RECEIVER);
			channel.writeInt(0);
			channel.writeString(dbName);
			channel.writeString(iEngineName);

			OLogManager.instance().info(this, "Exporting database '%s' via streaming to remote server node: %s...", iDatabase.getName(),
					iRemoteServerName);

			// START THE EXPORT GIVING AS OUTPUTSTREAM THE CHANNEL TO STREAM THE EXPORT
			new ODatabaseExport(iDatabase, new OChannelBinaryOutputStream(channel), this).exportDatabase();

			OLogManager.instance().info(this, "Database exported correctly");

			channel.readStatus();
			clientTxId = channel.readInt();

			manager.addServerInConfiguration(dbName, iRemoteServerName, iRemoteServerName, iSynchronousMode);

			status = STATUS.CONNECTED;

		} finally {
			channel.releaseExclusiveLock();
		}
	}

	public void onMessage(String iText) {
	}

	@Override
	public String toString() {
		return id;
	}

	public STATUS getStatus() {
		return status;
	}

	private void synchronizeDelta() throws IOException {
		synchronized (bufferedChanges) {
			if (bufferedChanges.isEmpty())
				return;

			OLogManager.instance().info(this, "Started realignment of remote node '%s' after a reconnection. Found %d updates", id,
					bufferedChanges.size());

			status = STATUS.SYNCHRONIZING;

			for (OTransactionEntry<ORecordInternal<?>> entry : bufferedChanges) {
				sendRequest(entry);
			}
			bufferedChanges.clear();

			OLogManager.instance().info(this, "Realignment of remote node '%s' done", id);

			status = STATUS.CONNECTED;
		}

		OLogManager.instance().info(this, "Realignment of remote node %s:%d completed", networkAddress, networkPort);
	}

	private int readStatus() throws IOException {
		return channel.readStatus();
	}
}
