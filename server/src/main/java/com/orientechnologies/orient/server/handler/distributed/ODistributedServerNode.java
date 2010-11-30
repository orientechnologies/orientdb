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
		DISCONNECTED, CONNECTING, CONNECTED, UNREACHABLE, SYNCHRONIZING
	}

	public enum SYNCH_TYPE {
		SYNCHRONOUS, ASYNCHRONOUS
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
	private long																				lastHeartBeat		= 0;
	private long																				dbVersion;
	private String																			database;

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

		OLogManager.instance().warn(this, "Joining the server node %s:%d to the cluster...", networkAddress, networkPort);

		channel.writeByte(OChannelDistributedProtocol.REQUEST_DISTRIBUTED_CONNECT);
		channel.writeInt(clientTxId);

		// PACKET DB INFO TO SEND
		channel.writeString(database);
		if (database != null) {
			channel.writeString(manager.getClusterDbSecurity(database)[0]);
			channel.writeString(manager.getClusterDbSecurity(database)[1]);
		}

		channel.flush();

		readStatus();
		dbVersion = channel.readLong();

		if (status == STATUS.CONNECTING)
			OLogManager.instance().info(this, "Server node %s:%d has joined the cluster", networkAddress, networkPort);
		else
			OLogManager.instance().info(this, "Server node %s:%d has re-joined the cluster after %d secs", networkAddress, networkPort,
					(System.currentTimeMillis() - lastHeartBeat) / 1000);

		lastHeartBeat = System.currentTimeMillis();
	}

	public void sendRequest(final OTransactionEntry<ORecordInternal<?>> iRequest, final SYNCH_TYPE iRequestType) throws IOException {
		if (status == STATUS.UNREACHABLE)
			bufferChange(iRequest);
		else {
			final ORecordInternal<?> record = iRequest.getRecord();

			status = STATUS.SYNCHRONIZING;

			try {
				switch (iRequest.status) {
				case OTransactionEntry.CREATED:
					channel.acquireExclusiveLock();
					try {
						channel.writeByte(OChannelDistributedProtocol.REQUEST_RECORD_CREATE);
						channel.writeInt(clientTxId);
						channel.writeShort((short) record.getIdentity().getClusterId());
						channel.writeBytes(record.toStream());
						channel.writeByte(record.getRecordType());
						channel.flush();

						dbVersion++;

						readStatus();
						channel.readLong();

					} finally {
						channel.releaseExclusiveLock();
					}
					break;

				case OTransactionEntry.UPDATED:
					channel.acquireExclusiveLock();
					try {
						channel.writeByte(OChannelDistributedProtocol.REQUEST_RECORD_UPDATE);
						channel.writeInt(clientTxId);
						channel.writeShort((short) record.getIdentity().getClusterId());
						channel.writeLong(record.getIdentity().getClusterPosition());
						channel.writeBytes(record.toStream());
						channel.writeInt(record.getVersion());
						channel.writeByte(record.getRecordType());
						channel.flush();

						readStatus();

						dbVersion++;

						channel.readInt();
					} finally {
						channel.releaseExclusiveLock();
					}
					break;

				case OTransactionEntry.DELETED:
					channel.acquireExclusiveLock();
					try {
						channel.writeByte(OChannelDistributedProtocol.REQUEST_RECORD_DELETE);
						channel.writeInt(clientTxId);
						channel.writeShort((short) record.getIdentity().getClusterId());
						channel.writeLong(record.getIdentity().getClusterPosition());
						channel.writeInt(record.getVersion());
						channel.flush();

						dbVersion++;

						readStatus();

						channel.readByte();
					} finally {
						channel.releaseExclusiveLock();
					}
					break;
				}

				status = STATUS.CONNECTED;

			} catch (IOException e) {
				manager.handleNodeFailure(this);

				if (iRequestType == SYNCH_TYPE.SYNCHRONOUS) {
					// SYNCHRONOUS CASE: RE-THROW THE EXCEPTION NOW TO BEING PROPAGATED UP TO THE CLIENT
					throw e;
				} else
					// BUFFER THE REQUEST TO BE RE-EXECUTED WHEN RECONNECTED
					bufferChange(iRequest);
			}
		}
	}

	protected void bufferChange(final OTransactionEntry<ORecordInternal<?>> iRequest) {
		synchronized (bufferedChanges) {
			if (bufferedChanges.size() > manager.serverOutSynchMaxBuffers) {
				// BUFFER EXCEEDS THE CONFIGURED LIMIT: REMOVE MYSELF AS NODE
				manager.removeNode(this);
				bufferedChanges.clear();
			} else {
				try {
					// CHECK IF ANOTHER REQUEST FOR THE SAME RECORD ID HAS BEEN ALREADY BUFFERED
					OTransactionEntry<ORecordInternal<?>> entry;
					for (int i = 0; i < bufferedChanges.size(); ++i) {
						entry = bufferedChanges.get(i);

						if (entry.getRecord().getIdentity().equals(iRequest.getRecord().getIdentity())) {
							// FOUND: REPLACE IT
							bufferedChanges.set(i, iRequest);
							return;
						}
					}

					// BUFFERIZE THE REQUEST
					bufferedChanges.add(iRequest);
				} finally {
					OLogManager.instance().info(this, "Can't reach the remote node '%s', buffering change %d/%d for the record %s", id,
							bufferedChanges.size(), manager.serverOutSynchMaxBuffers, iRequest.getRecord().getIdentity());
				}
			}
		}
	}

	public void sendConfiguration(final String iDatabaseName) {
		OLogManager.instance().info(this, "Sending configuration to distributed server node %s:%d...", networkAddress, networkPort);

		channel.acquireExclusiveLock();

		try {
			channel.writeByte(OChannelDistributedProtocol.REQUEST_DISTRIBUTED_DB_CONFIG);
			channel.writeInt(clientTxId);
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
			channel.writeByte(OChannelDistributedProtocol.REQUEST_DISTRIBUTED_HEARTBEAT);
			channel.writeInt(clientTxId);
			channel.flush();

			readStatus();

			// RESET LAST HEARTBEAT
			lastHeartBeat = System.currentTimeMillis();

			// CHECK DATABASE VERSION
			long remoteVersion = channel.readLong();

			OLogManager.instance().debug(this, "Checking database versions: local %d <-> remote %d", dbVersion, remoteVersion);

			if (remoteVersion != dbVersion)
				throw new ODistributedException("Database version doesn't match between current node (" + dbVersion + ") and remote ("
						+ remoteVersion + ")");

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
		if (status != STATUS.UNREACHABLE) {
			status = STATUS.UNREACHABLE;
		}
	}

	public void startSynchronization() {
		channel.acquireExclusiveLock();
		try {
			if (status != STATUS.CONNECTED) {
				// SEND THE LAST CONFIGURATION TO THE NODE
				// sendConfiguration(iDatabaseName);

				synchronizeDelta();

				status = STATUS.CONNECTED;
			}

		} catch (Exception e) {
			e.printStackTrace();

		} finally {
			channel.releaseExclusiveLock();
		}
	}

	public void shareDatabase(final ODatabaseRecord<?> iDatabase, final String iRemoteServerName, final String iDbUser,
			final String iDbPasswd, final String iEngineName, final boolean iSynchronousMode) throws IOException {
		if (status != STATUS.CONNECTED)
			throw new ODistributedSynchronizationException("Can't share database '" + iDatabase.getName() + "' on remote server node '"
					+ iRemoteServerName + "' because is disconnected");

		final String dbName = iDatabase.getName();

		channel.acquireExclusiveLock();

		try {
			status = STATUS.SYNCHRONIZING;

			OLogManager.instance().info(this, "Sharing database '" + dbName + "' to remote server " + iRemoteServerName + "...");

			// EXECUTE THE REQUEST ON THE REMOTE SERVER NODE
			channel.writeByte(OChannelDistributedProtocol.REQUEST_DISTRIBUTED_DB_SHARE_RECEIVER);
			channel.writeInt(0);
			channel.writeString(dbName);
			channel.writeString(iDbUser);
			channel.writeString(iDbPasswd);
			channel.writeString(iEngineName);

			OLogManager.instance().info(this, "Exporting database '%s' via streaming to remote server node: %s...", iDatabase.getName(),
					iRemoteServerName);

			// START THE EXPORT GIVING AS OUTPUTSTREAM THE CHANNEL TO STREAM THE EXPORT
			new ODatabaseExport(iDatabase, new OChannelBinaryOutputStream(channel), this).exportDatabase();

			OLogManager.instance().info(this, "Database exported correctly");

			clientTxId = channel.readStatus();
			dbVersion = channel.readLong();

		} finally {
			channel.releaseExclusiveLock();
		}

		status = STATUS.CONNECTED;

		database = dbName;
		manager.setClusterDbSecurity(dbName, iDbUser, iDbPasswd);
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

			final long time = System.currentTimeMillis();

			for (OTransactionEntry<ORecordInternal<?>> entry : bufferedChanges) {
				sendRequest(entry, SYNCH_TYPE.SYNCHRONOUS);
			}
			bufferedChanges.clear();

			OLogManager.instance().info(this, "Realignment of remote node '%s' completed in %d ms", id, System.currentTimeMillis() - time);

			status = STATUS.CONNECTED;
		}
	}

	private int readStatus() throws IOException {
		return channel.readStatus();
	}
}
