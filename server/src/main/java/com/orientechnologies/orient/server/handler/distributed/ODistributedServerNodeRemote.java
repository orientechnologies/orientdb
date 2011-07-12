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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicInteger;

import javax.crypto.SecretKey;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.tool.ODatabaseExport;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.tx.OTransactionRecordEntry;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryClient;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryOutputStream;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;
import com.orientechnologies.orient.enterprise.channel.distributed.OChannelDistributedProtocol;

/**
 * Contains all the information about a cluster node.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class ODistributedServerNodeRemote implements OCommandOutputListener {
	public enum STATUS {
		DISCONNECTED, CONNECTING, CONNECTED, UNREACHABLE, SYNCHRONIZING
	}

	public enum SYNCH_TYPE {
		SYNCHRONOUS, ASYNCHRONOUS
	}

	private String																id;
	public String																	networkAddress;
	public int																		networkPort;
	public Date																		joinedOn;
	private ODistributedServerManager							manager;
	private OChannelBinaryClient									channel;
	private OContextConfiguration									configuration;
	private volatile STATUS												status					= STATUS.DISCONNECTED;
	private List<OTransactionRecordEntry>					bufferedChanges	= new ArrayList<OTransactionRecordEntry>();
	private int																		clientTxId;
	private long																	lastHeartBeat		= 0;
	private static AtomicInteger									serialClientId	= new AtomicInteger(-1);
	private final ExecutorService									asynchExecutor;
	private Map<String, OServerNodeDatabaseEntry>	databases				= new HashMap<String, OServerNodeDatabaseEntry>();

	public ODistributedServerNodeRemote(final ODistributedServerManager iNode, final String iServerAddress, final int iServerPort) {
		manager = iNode;
		networkAddress = iServerAddress;
		networkPort = iServerPort;
		joinedOn = new Date();
		configuration = new OContextConfiguration();
		id = networkAddress + ":" + networkPort;
		status = STATUS.CONNECTING;

		asynchExecutor = Executors.newSingleThreadExecutor();
	}

	/**
	 * Connects the current node versus a remote node.
	 * 
	 * @param iTimeout
	 * @param iClusterName
	 * @param iSecurityKey
	 * @return true if the node has been connected, otherwise false. False is the case the other node is a Leader too and wins the
	 *         conflicts.
	 * @throws IOException
	 */
	public boolean connect(final int iTimeout, final String iClusterName, final SecretKey iSecurityKey) throws IOException {
		configuration.setValue(OGlobalConfiguration.NETWORK_SOCKET_TIMEOUT, iTimeout);

		channel = new OChannelBinaryClient(networkAddress, networkPort, configuration);

		OChannelBinaryProtocol.checkProtocolVersion(channel);

		OLogManager.instance().warn(this, "Joining the server node %s:%d to the cluster...", networkAddress, networkPort);

		clientTxId = serialClientId.decrementAndGet();

		// CONNECT TO THE SERVER
		channel.writeByte(OChannelDistributedProtocol.REQUEST_DISTRIBUTED_CONNECT);
		channel.writeInt(clientTxId);

		channel.writeString(iClusterName);
		channel.writeBytes(iSecurityKey.getEncoded());
		channel.writeLong(manager.getRunningSince());

		channel.flush();
		channel.readStatus();

		final boolean connected = channel.readByte() == 1;
		if (!connected) {
			OLogManager.instance().warn(this, "Remote server node %s:%d has refused the connection because it's the new Leader",
					networkAddress, networkPort);
			manager.abandonLeadership();
			return false;
		}

		// CONNECT EACH DATABASES
		final List<OServerNodeDatabaseEntry> servers = new ArrayList<OServerNodeDatabaseEntry>(databases.values());
		for (OServerNodeDatabaseEntry entry : servers) {
			try {
				channel.writeByte(OChannelDistributedProtocol.REQUEST_DISTRIBUTED_DB_OPEN);
				channel.writeInt(clientTxId);

				// PACKET DB INFO TO SEND
				channel.writeString(entry.databaseName);
				channel.writeString(entry.userName);
				channel.writeString(entry.userPassword);
				channel.flush();

				channel.readStatus();
				entry.sessionId = channel.readInt();

				// GET AND CHECK VERSION IF IT'S THE SAME
				final long version = channel.readLong();
				if (version != entry.version) {
					OLogManager.instance().warn(
							this,
							"Remote database '" + entry.databaseName + "' has different version than Leader node (" + entry.version
									+ ") and remote (" + version + "). Removing database from shared list.");

					databases.remove(entry.databaseName);

					// throw new ODistributedException("Database version doesn't match between current node (" + entry.version +
					// ") and remote ("
					// + version + ")");
				} else
					sendConfiguration(entry.databaseName);
			} catch (Exception e) {
				databases.remove(entry.databaseName);
				OLogManager.instance().warn(this,
						"Database '" + entry.databaseName + "' is not present on remote server. Removing database from shared list.");

			}
		}

		if (status == STATUS.CONNECTING)
			OLogManager.instance().info(this, "Server node %s:%d has joined the cluster", networkAddress, networkPort);
		else
			OLogManager.instance().info(this, "Server node %s:%d has re-joined the cluster after %d secs", networkAddress, networkPort,
					(System.currentTimeMillis() - lastHeartBeat) / 1000);

		lastHeartBeat = System.currentTimeMillis();
		return true;
	}

	public void sendRequest(final OTransactionRecordEntry iRequest, final SYNCH_TYPE iRequestType) throws IOException {
		if (status == STATUS.UNREACHABLE)
			bufferChange(iRequest);
		else {
			final OServerNodeDatabaseEntry databaseEntry = databases.get(iRequest.getRecord().getDatabase().getName());
			if (databaseEntry == null)
				return;

			if (OLogManager.instance().isDebugEnabled())
				OLogManager.instance().debug(this, "-> Sending request to remote server %s in %s mode...", this, iRequestType);

			final ORecordInternal<?> record = iRequest.getRecord();

			status = STATUS.SYNCHRONIZING;

			try {
				final Callable<Object> response;

				switch (iRequest.status) {
				case OTransactionRecordEntry.CREATED:
					channel.beginRequest();
					try {
						channel.writeByte(OChannelBinaryProtocol.REQUEST_RECORD_CREATE);
						channel.writeInt(databaseEntry.sessionId);
						channel.writeShort((short) record.getIdentity().getClusterId());
						channel.writeBytes(record.toStream());
						channel.writeByte(record.getRecordType());
					} finally {
						channel.endRequest();
					}

					response = new Callable<Object>() {
						@Override
						public Object call() throws Exception {
							beginResponse(databaseEntry.sessionId);
							try {
								databaseEntry.version++;
								final long clusterPosition = channel.readLong();

								if (clusterPosition != record.getIdentity().getClusterPosition())
									handleError(iRequest, iRequestType, new ODistributedException("Error on distributed insert for database '"
											+ record.getDatabase().getName() + "': the recordId received from the remote server node '" + getName()
											+ "' is different from the current one. Master=" + record.getIdentity() + ", " + getName() + "=#"
											+ record.getIdentity().getClusterId() + ":" + clusterPosition
											+ ". Unsharing the database against the remote server node..."));

							} finally {
								endResponse();
							}
							return null;
						}
					};

					if (iRequestType == SYNCH_TYPE.ASYNCHRONOUS)
						asynchExecutor.submit(new FutureTask<Object>(response));
					else
						try {
							response.call();
						} catch (Exception e) {
						}

					break;

				case OTransactionRecordEntry.UPDATED:
					channel.beginRequest();
					try {
						channel.writeByte(OChannelBinaryProtocol.REQUEST_RECORD_UPDATE);
						channel.writeInt(databaseEntry.sessionId);
						channel.writeShort((short) record.getIdentity().getClusterId());
						channel.writeLong(record.getIdentity().getClusterPosition());
						channel.writeBytes(record.toStream());
						channel.writeInt(record.getVersion());
						channel.writeByte(record.getRecordType());
					} finally {
						channel.endRequest();
					}

					response = new Callable<Object>() {
						@Override
						public Object call() throws Exception {
							beginResponse(databaseEntry.sessionId);
							try {
								databaseEntry.version++;
								final int version = channel.readInt();

//								if (version != record.getVersion())
//									handleError(iRequest, iRequestType, new ODistributedException("Error on distributed update for database '"
//											+ record.getDatabase().getName() + "': the version received from the remote server node '" + getName()
//											+ "' is different from the current one. Master=" + record.getVersion() + ", " + getName() + "=" + version
//											+ ". Unsharing the database against the remote server node..."));

							} finally {
								endResponse();
							}
							return null;
						}
					};

					if (iRequestType == SYNCH_TYPE.ASYNCHRONOUS)
						asynchExecutor.submit(new FutureTask<Object>(response));
					else
						try {
							response.call();
						} catch (Exception e) {
						}

					break;

				case OTransactionRecordEntry.DELETED:
					channel.beginRequest();
					try {
						channel.writeByte(OChannelBinaryProtocol.REQUEST_RECORD_DELETE);
						channel.writeInt(databaseEntry.sessionId);
						channel.writeShort((short) record.getIdentity().getClusterId());
						channel.writeLong(record.getIdentity().getClusterPosition());
						channel.writeInt(record.getVersion());

					} finally {
						channel.endRequest();
					}

					response = new Callable<Object>() {
						@Override
						public Object call() throws Exception {
							try {
								beginResponse(databaseEntry.sessionId);
								databaseEntry.version++;
								channel.readByte();

							} finally {
								endResponse();
							}
							return null;
						}
					};

					if (iRequestType == SYNCH_TYPE.ASYNCHRONOUS)
						asynchExecutor.submit(new FutureTask<Object>(response));
					else
						try {
							response.call();
						} catch (Exception e) {
						}

					break;
				}

				status = STATUS.CONNECTED;

			} catch (IOException e) {
				handleError(iRequest, iRequestType, e);
			}
		}
	}

	protected void handleError(final OTransactionRecordEntry iRequest, final SYNCH_TYPE iRequestType, final Exception iException)
			throws IOException {
		manager.handleNodeFailure(this);

		if (channel != null) {
			try {
				channel.close();
			} catch (Exception e) {
			}
			channel = null;
		}

		if (iRequestType == SYNCH_TYPE.SYNCHRONOUS) {
			// SYNCHRONOUS CASE: RE-THROW THE EXCEPTION NOW TO BEING PROPAGATED UP TO THE CLIENT
			if (iException instanceof IOException)
				throw (IOException) iException;
			else
				throw new IOException("Timeout on get lock against channel", iException);
		} else
			// BUFFER THE REQUEST TO BE RE-EXECUTED WHEN RECONNECTED
			bufferChange(iRequest);
	}

	protected void bufferChange(final OTransactionRecordEntry iRequest) {
		synchronized (bufferedChanges) {
			if (bufferedChanges.size() > manager.serverOutSynchMaxBuffers) {
				// BUFFER EXCEEDS THE CONFIGURED LIMIT: REMOVE MYSELF AS NODE
				manager.removeNode(this);
				bufferedChanges.clear();
				databases.clear();
			} else {
				try {
					// CHECK IF ANOTHER REQUEST FOR THE SAME RECORD ID HAS BEEN ALREADY BUFFERED
					OTransactionRecordEntry entry;
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
		final OServerNodeDatabaseEntry dbEntry = databases.get(iDatabaseName);
		if (dbEntry == null)
			return;

		OLogManager.instance().info(this, "Sending distributed configuration for database '%s' to server node %s:%d...", iDatabaseName,
				networkAddress, networkPort);

		try {
			try {
				channel.beginRequest();

				channel.writeByte(OChannelDistributedProtocol.REQUEST_DISTRIBUTED_DB_CONFIG);
				channel.writeInt(dbEntry.sessionId);
				channel.writeBytes(manager.getClusterConfiguration(iDatabaseName).toStream());

			} finally {
				channel.endRequest();
			}

			try {
				beginResponse(dbEntry.sessionId);
			} finally {
				endResponse();
			}

		} catch (Exception e) {
			OLogManager.instance().warn(this, "Error on sending configuration to server node", toString());
		}
	}

	public boolean sendHeartBeat(final int iNetworkTimeout) throws InterruptedException {
		if (channel == null)
			return false;

		configuration.setValue(OGlobalConfiguration.NETWORK_SOCKET_TIMEOUT, iNetworkTimeout);
		OLogManager.instance()
				.debug(this, "Sending keepalive message to distributed server node %s:%d...", networkAddress, networkPort);

		try {
			channel.beginRequest();
			try {
				channel.writeByte(OChannelDistributedProtocol.REQUEST_DISTRIBUTED_HEARTBEAT);
				channel.writeInt(clientTxId);
			} finally {
				channel.endRequest();
			}

			try {
				channel.beginResponse(clientTxId);
			} finally {
				channel.endResponse();
			}

			// RESET LAST HEARTBEAT
			lastHeartBeat = System.currentTimeMillis();

		} catch (Exception e) {
			OLogManager.instance().debug(this, "Error on sending heartbeat to server node", e, toString());
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
		if (status != STATUS.UNREACHABLE) {
			status = STATUS.UNREACHABLE;
		}
	}

	public void startSynchronization() throws InterruptedException, IOException {
		if (status != STATUS.CONNECTED) {
			// SEND THE LAST CONFIGURATION TO THE NODE
			// sendConfiguration(iDatabaseName);

			synchronizeDelta();

			status = STATUS.CONNECTED;
		}
	}

	public void shareDatabase(final ODatabaseRecord iDatabase, final String iRemoteServerName, final String iDbUser,
			final String iDbPasswd, final String iEngineName, final boolean iSynchronousMode) throws IOException, InterruptedException {
		if (status != STATUS.CONNECTED)
			throw new ODistributedSynchronizationException("Can't share database '" + iDatabase.getName() + "' on remote server node '"
					+ iRemoteServerName + "' because is disconnected");

		final String dbName = iDatabase.getName();

		final OServerNodeDatabaseEntry databaseEntry;

		channel.beginRequest();
		try {
			status = STATUS.SYNCHRONIZING;

			OLogManager.instance().info(this, "Sharing database '" + dbName + "' to remote server " + iRemoteServerName + "...");

			// EXECUTE THE REQUEST ON THE REMOTE SERVER NODE
			channel.writeByte(OChannelDistributedProtocol.REQUEST_DISTRIBUTED_DB_SHARE_RECEIVER);
			channel.writeInt(clientTxId);
			channel.writeString(dbName);
			channel.writeString(iDbUser);
			channel.writeString(iDbPasswd);
			channel.writeString(iEngineName);
		} finally {
			channel.endRequest();
		}

		OLogManager.instance().info(this, "Exporting database '%s' via streaming to remote server node: %s...", iDatabase.getName(),
				iRemoteServerName);

		// START THE EXPORT GIVING AS OUTPUTSTREAM THE CHANNEL TO STREAM THE EXPORT
		new ODatabaseExport(iDatabase, new OChannelBinaryOutputStream(channel), this).exportDatabase();

		OLogManager.instance().info(this, "Database exported correctly");

		databaseEntry = new OServerNodeDatabaseEntry();
		databaseEntry.databaseName = dbName;
		databaseEntry.userName = iDbUser;
		databaseEntry.userPassword = iDbPasswd;

		channel.beginResponse(clientTxId);
		try {
			databaseEntry.sessionId = channel.readInt();
			databaseEntry.version = channel.readLong();

			databases.put(dbName, databaseEntry);
		} finally {
			channel.endResponse();
		}

		status = STATUS.CONNECTED;
	}

	@Override
	public void onMessage(final String iText) {
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

			for (OTransactionRecordEntry entry : bufferedChanges) {
				sendRequest(entry, SYNCH_TYPE.SYNCHRONOUS);
			}
			bufferedChanges.clear();

			OLogManager.instance()
					.info(this, "Realignment of remote node '%s' completed in %d ms", id, System.currentTimeMillis() - time);

			status = STATUS.CONNECTED;
		}
	}

	public void beginResponse(final int iSessionId) throws IOException {
		channel.beginResponse(iSessionId);
	}

	public void beginResponse() throws IOException {
		if (channel != null)
			channel.beginResponse(clientTxId);
	}

	public void endResponse() {
		if (channel != null)
			channel.endResponse();
	}

	public String getName() {
		return networkAddress + ":" + networkPort;
	}

	/**
	 * Check if a remote node is really connected.
	 * 
	 * @return true if it's connected, otherwise false
	 */
	public boolean checkConnection() {
		boolean connected = false;

		if (channel != null && channel.socket != null)
			try {
				connected = channel.socket.isConnected();
			} catch (Exception e) {
			}

		if (!connected)
			status = STATUS.DISCONNECTED;

		return connected;
	}

	public void disconnect() {
		if (channel != null)
			channel.close();
		channel = null;
	}
}
