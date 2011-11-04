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
package com.orientechnologies.orient.server.replication;

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
import com.orientechnologies.orient.server.handler.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.replication.ODistributedDatabaseInfo.SYNCH_TYPE;

/**
 * Represents a member of the cluster.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class ODistributedNode implements OCommandOutputListener {
	public enum STATUS {
		ONLINE, SYNCHRONIZING
	}

	private String																id;
	public String																	networkAddress;
	public int																		networkPort;
	public Date																		connectedOn;
	private ODistributedServerManager							manager;
	private OChannelBinaryClient									channel;
	private OContextConfiguration									configuration;
	private List<OTransactionRecordEntry>					bufferedChanges	= new ArrayList<OTransactionRecordEntry>();
	private int																		clientTxId;
	private final ExecutorService									asynchExecutor;
	private Map<String, ODistributedDatabaseInfo>	databases				= new HashMap<String, ODistributedDatabaseInfo>();
	private static AtomicInteger									serialClientId	= new AtomicInteger(-1);
	private STATUS																status;

	public ODistributedNode(final ODistributedServerManager iNode, final String iId) {
		manager = iNode;
		id = iId;

		configuration = new OContextConfiguration();
		asynchExecutor = Executors.newSingleThreadExecutor();

		final String[] parts = iId.split(":");
		networkAddress = parts[0];
		networkPort = Integer.parseInt(parts[1]);
	}

	public void connectDatabase(final ODistributedDatabaseInfo iDatabase) throws IOException {
		synchronized (this) {
			// REMOVE ANY OTHER PREVIOUS ENTRY
			databases.remove(iDatabase.databaseName);

			if (channel == null)
				// CONNECT TO THE NODE THE FIRST TIME
				connect();

			try {
				channel.writeByte(OChannelDistributedProtocol.REQUEST_DISTRIBUTED_DB_OPEN);
				channel.writeInt(clientTxId);

				// PACKET DB INFO TO SEND
				channel.writeString(iDatabase.databaseName);
				channel.writeString(iDatabase.userName);
				channel.writeString(iDatabase.userPassword);
				channel.flush();

				channel.readStatus();
				iDatabase.sessionId = channel.readInt();
			} catch (Exception e) {
				databases.remove(iDatabase.databaseName);
				OLogManager.instance().warn(this,
						"Database '" + iDatabase.databaseName + "' is not present on remote server. Removing database from shared list.");
			}
		}
	}

	/**
	 * Connects to the remote node.
	 * 
	 * @throws IOException
	 */
	protected void connect() throws IOException {
		configuration.setValue(OGlobalConfiguration.NETWORK_SOCKET_TIMEOUT, manager.getConfig().networkTimeoutNode);

		OLogManager.instance().warn(this, "Connecting to distributed node %s:%d...", networkAddress, networkPort);

		channel = new OChannelBinaryClient(networkAddress, networkPort, configuration);
		OChannelBinaryProtocol.checkProtocolVersion(channel);

		clientTxId = serialClientId.decrementAndGet();
		connectedOn = new Date();
	}

	public void sendRequest(final OTransactionRecordEntry iRequest, final SYNCH_TYPE iRequestType) throws IOException {
		logChange(iRequest);

		final ODistributedDatabaseInfo databaseEntry = databases.get(iRequest.getRecord().getDatabase().getName());
		if (databaseEntry == null)
			return;

		if (OLogManager.instance().isDebugEnabled())
			OLogManager.instance().debug(this, "-> Sending request to remote server %s in %s mode...", this, iRequestType);

		final ORecordInternal<?> record = iRequest.getRecord();

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
							final int version = channel.readInt();

							// if (version != record.getVersion())
							// handleError(iRequest, iRequestType, new ODistributedException("Error on distributed update for database '"
							// + record.getDatabase().getName() + "': the version received from the remote server node '" + getName()
							// + "' is different from the current one. Master=" + record.getVersion() + ", " + getName() + "=" + version
							// + ". Unsharing the database against the remote server node..."));

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

		} catch (IOException e) {
			handleError(iRequest, iRequestType, e);
		}
	}

	protected void handleError(final OTransactionRecordEntry iRequest, final SYNCH_TYPE iRequestType, final Exception iException)
			throws IOException {
		disconnect();

		// ERROR
		OLogManager.instance().warn(this, "Remote server node %s:%d seems down, retrying to connect...", networkAddress, networkPort);

		// RECONNECT ALL DATABASES
		try {
			for (ODistributedDatabaseInfo dbEntry : databases.values()) {
				connectDatabase(dbEntry);
			}
		} catch (IOException e) {
			// IO ERROR: THE NODE SEEMED ALWAYS MORE DOWN: START TO COLLECT DATA FOR IT WAITING FOR A FUTURE RE-CONNECTION
			OLogManager.instance()
					.warn(this, "Remote server node %s:%d is down, remove it from replication", networkAddress, networkPort);
		}

		disconnect();

		if (iRequestType == SYNCH_TYPE.SYNCHRONOUS) {
			// SYNCHRONOUS CASE: RE-THROW THE EXCEPTION NOW TO BEING PROPAGATED UP TO THE CLIENT
			if (iException instanceof IOException)
				throw (IOException) iException;
			else
				throw new IOException("Timeout on get lock against channel", iException);
		}
	}

	/**
	 * Log changes to the disk. TODO: Write to disk not memory
	 * 
	 * @param iRequest
	 */
	protected void logChange(final OTransactionRecordEntry iRequest) {
		synchronized (bufferedChanges) {
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
						bufferedChanges.size(), manager.getConfig().serverOutSynchMaxBuffers, iRequest.getRecord().getIdentity());
			}
		}
	}

	public void startSynchronization() throws InterruptedException, IOException {
		if (status != STATUS.SYNCHRONIZING) {
			synchronizeDelta();
			status = STATUS.ONLINE;
		}
	}

	public void shareDatabase(final ODatabaseRecord iDatabase, final String iRemoteServerName, final String iDbUser,
			final String iDbPasswd, final String iEngineName, final boolean iSynchronousMode) throws IOException, InterruptedException {
		if (status != STATUS.ONLINE)
			throw new ODistributedSynchronizationException("Can't share database '" + iDatabase.getName() + "' on remote server node '"
					+ iRemoteServerName + "' because is disconnected");

		final String dbName = iDatabase.getName();

		final ODistributedDatabaseInfo databaseEntry;

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

		databaseEntry = new ODistributedDatabaseInfo();
		databaseEntry.databaseName = dbName;
		databaseEntry.userName = iDbUser;
		databaseEntry.userPassword = iDbPasswd;

		channel.beginResponse(clientTxId);
		try {
			databaseEntry.sessionId = channel.readInt();

			databases.put(dbName, databaseEntry);
		} finally {
			channel.endResponse();
		}

		status = STATUS.ONLINE;
	}

	@Override
	public void onMessage(final String iText) {
	}

	@Override
	public String toString() {
		return id;
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

			status = STATUS.ONLINE;
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

		return connected;
	}

	public void disconnect() {
		if (channel != null)
			channel.close();
		channel = null;
	}

	public Map<String, ODistributedDatabaseInfo> getDatabases() {
		return databases;
	}
}
