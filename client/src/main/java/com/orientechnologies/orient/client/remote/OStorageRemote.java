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
package com.orientechnologies.orient.client.remote;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.io.OIOException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.util.OPair;
import com.orientechnologies.orient.client.dictionary.ODictionaryClient;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.OCommandRequestAsynch;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.config.OStorageConfiguration;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.dictionary.ODictionary;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordFactory;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.ORecordSchemaAware;
import com.orientechnologies.orient.core.serialization.OSerializableStream;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializerAnyRuntime;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializerAnyStreamable;
import com.orientechnologies.orient.core.storage.OCluster;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.OStorageAbstract;
import com.orientechnologies.orient.core.tx.OTransaction;
import com.orientechnologies.orient.core.tx.OTransactionEntry;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryClient;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;

/**
 * This object is bound to each remote ODatabase instances.
 */
@SuppressWarnings("unchecked")
public class OStorageRemote extends OStorageAbstract {
	private static final String							DEFAULT_HOST			= "localhost";
	private static final String[]						DEFAULT_PORTS			= new String[] { "2424" };
	private static final String							ADDRESS_SEPARATOR	= ";";
	private String													userName;
	private String													userPassword;
	private OContextConfiguration						clientConfiguration;
	private int															connectionRetry;
	private int															connectionRetryDelay;

	protected List<OPair<String, String[]>>	serverURLs				= new ArrayList<OPair<String, String[]>>();
	private OChannelBinaryClient						network;
	protected int														txId;
	protected final Map<String, Integer>		clustersIds				= new HashMap<String, Integer>();
	protected final Map<String, String>			clustersTypes			= new HashMap<String, String>();
	protected int														defaultClusterId;
	protected int														retry							= 0;

	public OStorageRemote(final String iURL, final String iMode) throws IOException {
		super(iURL, iURL, iMode);
		configuration = new OStorageConfiguration(this);

		clientConfiguration = new OContextConfiguration();
		connectionRetry = clientConfiguration.getValueAsInteger(OGlobalConfiguration.NETWORK_SOCKET_RETRY);
		connectionRetryDelay = clientConfiguration.getValueAsInteger(OGlobalConfiguration.NETWORK_SOCKET_RETRY_DELAY);
	}

	public void open(final int iRequesterId, final String iUserName, final String iUserPassword) {
		boolean locked = acquireExclusiveLock();

		try {
			userName = iUserName;
			userPassword = iUserPassword;

			openRemoteDatabase();
			addUser();

			configuration.load();

			cache.addUser();

			Orient.instance().registerStorage(this);

		} catch (Exception e) {
			close();
			if (e instanceof OException)
				throw (OException) e;
			else
				throw new OStorageException("Can't open the remote storage: " + name, e);
		} finally {

			releaseExclusiveLock(locked);
		}
	}

	public void create() {
		throw new UnsupportedOperationException(
				"Can't create a database in a remote server. Please use the console or the OServerAdmin class.");
	}

	public boolean exists() {
		checkConnection();

		do {
			boolean locked = acquireExclusiveLock();

			try {
				writeCommand(OChannelBinaryProtocol.REQUEST_DB_EXIST);
				network.readStatus();
				return network.readByte() == 1;
			} catch (Exception e) {
				if (handleException("Error on checking if the database exists", e))
					break;

			} finally {
				releaseExclusiveLock(locked);
			}
		} while (true);
		return false;
	}

	public void close() {
		boolean locked = acquireExclusiveLock();

		try {
			writeCommand(OChannelBinaryProtocol.REQUEST_DB_CLOSE);
			network.out.flush();

			network.socket.close();

			cache.removeUser();
			cache.clear();

			open = false;

			Orient.instance().unregisterStorage(this);

		} catch (Exception e) {

		} finally {
			releaseExclusiveLock(locked);
		}
	}

	public void delete() {
		throw new UnsupportedOperationException(
				"Can't delete a database in a remote server. Please use the console or the OServerAdmin class.");
	}

	public Set<String> getClusterNames() {
		checkConnection();

		boolean locked = acquireSharedLock();

		try {
			return clustersIds.keySet();

		} finally {
			releaseSharedLock(locked);
		}
	}

	public long createRecord(final int iClusterId, final byte[] iContent, final byte iRecordType) {
		checkConnection();

		do {
			boolean locked = acquireExclusiveLock();

			try {
				writeCommand(OChannelBinaryProtocol.REQUEST_RECORD_CREATE);
				network.writeShort((short) iClusterId);
				network.writeBytes(iContent);
				network.writeByte(iRecordType);
				network.readStatus();
				return network.readLong();
			} catch (Exception e) {
				if (handleException("Error on create record in cluster: " + iClusterId, e))
					break;

			} finally {
				releaseExclusiveLock(locked);
			}
		} while (true);
		return -1;
	}

	public ORawBuffer readRecord(final ODatabaseRecord<?> iDatabase, final int iRequesterId, final int iClusterId,
			final long iPosition, final String iFetchPlan) {
		checkConnection();

		if (OStorageRemoteThreadLocal.INSTANCE.get())
			// PENDING NETWORK OPERATION, CAN'T EXECUTE IT NOW
			return null;

		do {
			boolean locked = acquireExclusiveLock();

			try {
				writeCommand(OChannelBinaryProtocol.REQUEST_RECORD_LOAD);
				network.writeShort((short) iClusterId);
				network.writeLong(iPosition);
				network.writeString(iFetchPlan != null ? iFetchPlan : "");
				network.readStatus();

				if (network.readByte() == 0)
					return null;

				final ORawBuffer buffer = new ORawBuffer(network.readBytes(), network.readInt(), network.readByte());

				while (network.readByte() == 2) {
					ORecordInternal<?> record = readRecordFromNetwork(iDatabase);
					// PUT IN THE CLIENT LOCAL CACHE
					cache.pushRecord(record.getIdentity().toString(),
							new ORawBuffer(record.toStream(), record.getVersion(), record.getRecordType()));
				}

				return buffer;

			} catch (Exception e) {
				if (handleException("Error on read record: " + iClusterId + ":" + iPosition, e))
					break;

			} finally {
				releaseExclusiveLock(locked);
			}
		} while (true);
		return null;
	}

	public int updateRecord(final int iRequesterId, final int iClusterId, final long iPosition, final byte[] iContent,
			final int iVersion, final byte iRecordType) {
		checkConnection();

		do {
			boolean locked = acquireExclusiveLock();

			try {
				writeCommand(OChannelBinaryProtocol.REQUEST_RECORD_UPDATE);
				network.writeShort((short) iClusterId);
				network.writeLong(iPosition);
				network.writeBytes(iContent);
				network.writeInt(iVersion);
				network.writeByte(iRecordType);
				network.readStatus();

				return network.readInt();

			} catch (Exception e) {
				if (handleException("Error on update record: " + iClusterId + ":" + iPosition, e))
					break;

			} finally {
				releaseExclusiveLock(locked);
			}
		} while (true);

		return -1;
	}

	public boolean deleteRecord(final int iRequesterId, final int iClusterId, final long iPosition, final int iVersion) {
		checkConnection();

		do {
			boolean locked = acquireExclusiveLock();

			try {
				writeCommand(OChannelBinaryProtocol.REQUEST_RECORD_DELETE);
				network.writeShort((short) iClusterId);
				network.writeLong(iPosition);
				network.writeInt(iVersion);
				network.readStatus();

				return network.readByte() == '1';
			} catch (Exception e) {
				if (handleException("Error on delete record: " + iClusterId + ":" + iPosition, e))
					break;

			} finally {
				releaseExclusiveLock(locked);
			}
		} while (true);
		return false;
	}

	public long count(final int iClusterId) {
		return count(new int[] { iClusterId });
	}

	public long[] getClusterDataRange(final int iClusterId) {
		checkConnection();

		do {
			boolean locked = acquireExclusiveLock();

			try {
				writeCommand(OChannelBinaryProtocol.REQUEST_DATACLUSTER_DATARANGE);
				network.writeShort((short) iClusterId);
				network.readStatus();
				return new long[] { network.readLong(), network.readLong() };
			} catch (Exception e) {
				if (handleException("Error on getting last entry position count in cluster: " + iClusterId, e))
					break;

			} finally {
				releaseExclusiveLock(locked);
			}
		} while (true);
		return null;
	}

	public long count(final int[] iClusterIds) {
		checkConnection();

		do {
			boolean locked = acquireExclusiveLock();

			try {
				writeCommand(OChannelBinaryProtocol.REQUEST_DATACLUSTER_COUNT);
				network.writeShort((short) iClusterIds.length);
				for (int i = 0; i < iClusterIds.length; ++i)
					network.writeShort((short) iClusterIds[i]);
				network.readStatus();
				return network.readLong();
			} catch (Exception e) {
				if (handleException("Error on read record count in clusters: " + iClusterIds, e))
					break;

			} finally {
				releaseExclusiveLock(locked);
			}
		} while (true);
		return -1;
	}

	public long count(final String iClassName) {
		checkConnection();

		do {
			boolean locked = acquireExclusiveLock();

			try {
				writeCommand(OChannelBinaryProtocol.REQUEST_COUNT);
				network.writeString(iClassName);
				network.readStatus();
				return network.readLong();
			} catch (Exception e) {
				if (handleException("Error on executing count on class: " + iClassName, e))
					break;

			} finally {
				releaseExclusiveLock(locked);
			}
		} while (true);
		return -1;
	}

	/**
	 * Execute the command remotely and get the results back.
	 */
	public Object command(final OCommandRequestText iCommand) {
		checkConnection();

		if (!(iCommand instanceof OSerializableStream))
			throw new OCommandExecutionException("Can't serialize the command to being executed to the server side.");

		OSerializableStream command = iCommand;

		Object result = null;

		do {
			boolean locked = acquireExclusiveLock();

			OStorageRemoteThreadLocal.INSTANCE.set(Boolean.TRUE);

			try {
				final OCommandRequestText aquery = (OCommandRequestText) iCommand;

				final boolean asynch = iCommand instanceof OCommandRequestAsynch;

				writeCommand(OChannelBinaryProtocol.REQUEST_COMMAND);
				network.writeByte((byte) (asynch ? 'a' : 's')); // ASYNC / SYNC
				network.writeBytes(OStreamSerializerAnyStreamable.INSTANCE.toStream(iCommand.getDatabase(), command));
				network.readStatus();

				if (asynch) {
					byte status;

					// ASYNCH: READ ONE RECORD AT TIME
					while ((status = network.readByte()) > 0) {
						ORecordSchemaAware<?> record = (ORecordSchemaAware<?>) readRecordFromNetwork(iCommand.getDatabase());
						if (record == null)
							break;

						switch (status) {
						case 1:
							// PUT AS PART OF THE RESULT SET. INVOKE THE LISTENER
							try {
								if (!aquery.getResultListener().result(record)) {
									// EMPTY THE INPUT CHANNEL
									while (network.in.available() > 0)
										network.in.read();

									break;
								}
							} catch (Throwable t) {
								// ABSORBE ALL THE USER EXCEPTIONS
								t.printStackTrace();
							}
							break;

						case 2:
							// PUT IN THE CLIENT LOCAL CACHE
							cache.pushRecord(record.getIdentity().toString(),
									new ORawBuffer(record.toStream(), record.getVersion(), record.getRecordType()));
						}
					}
				} else {
					final byte type = network.readByte();
					switch (type) {
					case 'n':
						result = null;
						break;

					case 'r':
						result = readRecordFromNetwork(iCommand.getDatabase());
						break;

					case 'a':
						result = OStreamSerializerAnyRuntime.INSTANCE.fromStream(iCommand.getDatabase(), network.readBytes());
						break;
					}
				}
				break;

			} catch (Exception e) {
				if (handleException("Error on executing command: " + iCommand, e))
					break;

			} finally {
				OStorageRemoteThreadLocal.INSTANCE.set(Boolean.FALSE);

				releaseExclusiveLock(locked);
			}
		} while (true);

		return result;
	}

	public void commit(final int iRequesterId, final OTransaction<?> iTx) {
		checkConnection();

		do {
			boolean locked = acquireExclusiveLock();

			try {
				writeCommand(OChannelBinaryProtocol.REQUEST_TX_COMMIT);
				network.writeInt(iTx.getId());
				network.writeInt(iTx.size());

				for (OTransactionEntry<? extends ORecord<?>> txEntry : iTx.getEntries()) {
					if (txEntry.status == OTransactionEntry.LOADED)
						// JUMP LOADED OBJECTS
						continue;

					network.writeByte(txEntry.status);
					network.writeShort((short) txEntry.getRecord().getIdentity().getClusterId());
					network.writeByte(txEntry.getRecord().getRecordType());

					switch (txEntry.status) {
					case OTransactionEntry.CREATED:
						network.writeString(txEntry.clusterName);
						network.writeBytes(txEntry.getRecord().toStream());
						break;

					case OTransactionEntry.UPDATED:
						network.writeLong(txEntry.getRecord().getIdentity().getClusterPosition());
						network.writeInt(txEntry.getRecord().getVersion());
						network.writeBytes(txEntry.getRecord().toStream());
						break;

					case OTransactionEntry.DELETED:
						network.writeLong(txEntry.getRecord().getIdentity().getClusterPosition());
						network.writeInt(txEntry.getRecord().getVersion());
						break;
					}
				}
				network.readStatus();
				break;
			} catch (Exception e) {
				if (handleException("Error on commit", e))
					break;

			} finally {
				releaseExclusiveLock(locked);
			}
		} while (true);
	}

	public int getClusterIdByName(final String iClusterName) {
		checkConnection();

		if (iClusterName == null)
			return -1;

		if (Character.isDigit(iClusterName.charAt(0)))
			return Integer.parseInt(iClusterName);

		boolean locked = acquireSharedLock();

		try {
			final Integer id = clustersIds.get(iClusterName.toLowerCase());
			if (id == null)
				return -1;

			return id;

		} finally {
			releaseSharedLock(locked);
		}
	}

	public String getClusterTypeByName(final String iClusterName) {
		checkConnection();

		if (iClusterName == null)
			return null;

		boolean locked = acquireSharedLock();

		try {
			return clustersTypes.get(iClusterName.toLowerCase());

		} finally {
			releaseSharedLock(locked);
		}
	}

	public int getDefaultClusterId() {
		return defaultClusterId;
	}

	public int addCluster(final String iClusterName, final OStorage.CLUSTER_TYPE iClusterType, final Object... iArguments) {
		checkConnection();

		do {
			boolean locked = acquireExclusiveLock();

			try {
				writeCommand(OChannelBinaryProtocol.REQUEST_DATACLUSTER_ADD);
				network.writeString(iClusterType.toString());
				network.writeString(iClusterName);

				switch (iClusterType) {
				case PHYSICAL:
					// FILE PATH + START SIZE
					network.writeString(iArguments.length > 0 ? (String) iArguments[0] : "").writeInt(
							iArguments.length > 0 ? (Integer) iArguments[1] : -1);
					break;

				case LOGICAL:
					// PHY CLUSTER ID
					network.writeInt(iArguments.length > 0 ? (Integer) iArguments[0] : -1);
					break;
				}
				network.readStatus();

				int clusterId = network.readShort();
				clustersIds.put(iClusterName.toLowerCase(), clusterId);
				clustersTypes.put(iClusterName.toLowerCase(), iClusterType.toString());
				return clusterId;
			} catch (Exception e) {
				if (handleException("Error on add new cluster", e))
					break;

			} finally {
				releaseExclusiveLock(locked);
			}
		} while (true);
		return 0;
	}

	public boolean removeCluster(final int iClusterId) {
		checkConnection();

		do {
			boolean locked = acquireExclusiveLock();

			try {
				writeCommand(OChannelBinaryProtocol.REQUEST_DATACLUSTER_REMOVE);
				network.writeShort((short) iClusterId);

				network.readStatus();

				if (network.readByte() == '1') {
					// REMOVE THE CLUSTER LOCALLY
					for (Entry<String, Integer> entry : clustersIds.entrySet())
						if (entry.getValue() != null && entry.getValue().intValue() == iClusterId) {
							clustersIds.remove(entry.getKey());
							clustersTypes.remove(entry.getKey());
							break;
						}

					return true;
				}
				return false;
			} catch (Exception e) {
				if (handleException("Error on removing of cluster", e))
					break;

			} finally {
				releaseExclusiveLock(locked);
			}
		} while (true);
		return false;
	}

	public int addDataSegment(final String iDataSegmentName) {
		return addDataSegment(iDataSegmentName, null);
	}

	public int addDataSegment(final String iSegmentName, final String iSegmentFileName) {
		checkConnection();

		do {
			boolean locked = acquireExclusiveLock();

			try {
				writeCommand(OChannelBinaryProtocol.REQUEST_DATASEGMENT_ADD);
				network.writeString(iSegmentName).writeString(iSegmentFileName);
				network.readStatus();
				return network.readShort();
			} catch (Exception e) {
				if (handleException("Error on add new data segment", e))
					break;

			} finally {
				releaseExclusiveLock(locked);
			}
		} while (true);
		return 0;
	}

	public int getSessionId() {
		boolean locked = acquireSharedLock();

		try {
			return txId;

		} finally {
			releaseSharedLock(locked);
		}
	}

	public <REC extends ORecordInternal<?>> REC dictionaryPut(final ODatabaseRecord<REC> iDatabase, final String iKey,
			final ORecordInternal<?> iRecord) {
		checkConnection();

		do {
			boolean locked = acquireExclusiveLock();

			try {
				final ORID rid = iRecord.getIdentity();

				writeCommand(OChannelBinaryProtocol.REQUEST_DICTIONARY_PUT);
				network.writeString(iKey);
				network.writeByte(iRecord.getRecordType());
				network.writeShort((short) rid.getClusterId());
				network.writeLong(rid.getClusterPosition());
				network.readStatus();

				return (REC) readRecordFromNetwork(iDatabase);

			} catch (Exception e) {
				if (handleException("Error on insert record with key: " + iKey, e))
					break;

			} finally {
				releaseExclusiveLock(locked);
			}
		} while (true);
		return null;
	}

	public <REC extends ORecordInternal<?>> REC dictionaryLookup(ODatabaseRecord<REC> iDatabase, final String iKey) {
		checkConnection();

		do {
			boolean locked = acquireExclusiveLock();

			try {
				writeCommand(OChannelBinaryProtocol.REQUEST_DICTIONARY_LOOKUP);
				network.writeString(iKey);
				network.readStatus();

				return (REC) readRecordFromNetwork(iDatabase);

			} catch (Exception e) {
				if (handleException("Error on lookup record with key: " + iKey, e))
					break;

			} finally {
				releaseExclusiveLock(locked);
			}
		} while (true);
		return null;
	}

	public <REC extends ORecordInternal<?>> REC dictionaryRemove(ODatabaseRecord<REC> iDatabase, Object iKey) {
		checkConnection();

		do {
			boolean locked = acquireExclusiveLock();

			try {
				writeCommand(OChannelBinaryProtocol.REQUEST_DICTIONARY_REMOVE);
				network.writeString(iKey.toString());
				network.readStatus();

				return (REC) readRecordFromNetwork(iDatabase);

			} catch (Exception e) {
				if (handleException("Error on lookup record with key: " + iKey, e))
					break;

			} finally {
				releaseExclusiveLock(locked);
			}
		} while (true);
		return null;
	}

	public int dictionarySize(final ODatabaseRecord<?> iDatabase) {
		checkConnection();

		do {
			boolean locked = acquireExclusiveLock();

			try {
				writeCommand(OChannelBinaryProtocol.REQUEST_DICTIONARY_SIZE);
				network.readStatus();
				return network.readInt();
			} catch (Exception e) {
				if (handleException("Error on getting size of database's dictionary", e))
					break;

			} finally {
				releaseExclusiveLock(locked);
			}
		} while (true);
		return -1;
	}

	public ODictionary<?> createDictionary(final ODatabaseRecord<?> iDatabase) throws Exception {
		return new ODictionaryClient<Object>(iDatabase, this);
	}

	public Set<String> dictionaryKeys() {
		checkConnection();

		do {
			boolean locked = acquireExclusiveLock();

			try {
				writeCommand(OChannelBinaryProtocol.REQUEST_DICTIONARY_KEYS);
				network.readStatus();
				return network.readStringSet();
			} catch (Exception e) {
				if (handleException("Error on getting keys of database's dictionary", e))
					break;

			} finally {
				releaseExclusiveLock(locked);
			}
		} while (true);
		return null;
	}

	public void synch() {
	}

	public String getPhysicalClusterNameById(final int iClusterId) {
		for (Entry<String, Integer> clusterEntry : clustersIds.entrySet()) {
			if (clusterEntry.getValue().intValue() == iClusterId)
				return clusterEntry.getKey();
		}
		return null;
	}

	public Collection<OCluster> getClusters() {
		throw new UnsupportedOperationException("getClusters()");
	}

	public OCluster getClusterById(final int iId) {
		throw new UnsupportedOperationException("getClusterById()");
	}

	protected boolean handleException(final String iMessage, final Exception iException) {
		if (iException instanceof OException)
			// RE-THROW IT
			throw (OException) iException;

		if (!(iException instanceof IOException))
			throw new OStorageException(iMessage, iException);

		if (retry < connectionRetry) {
			// WAIT THE DELAY BEFORE TO RETRY
			try {
				Thread.sleep(connectionRetryDelay);
			} catch (InterruptedException e) {
			}

			try {
				if (OLogManager.instance().isDebugEnabled())
					OLogManager.instance().debug(this, "Retrying to connect to remote server #" + retry + "/" + connectionRetry + "...");

				openRemoteDatabase();

				retry = 0;

				OLogManager.instance().info(this,
						"Connection re-acquired in transparent way: no errors will be thrown at application level");

				return true;
			} catch (Throwable t) {
				++retry;
			}
		} else {
			retry = 0;

			// RECONNECTION FAILED: THROW+LOG THE ORIGINAL EXCEPTION
			throw new OStorageException(iMessage, iException);
		}
		return false;
	}

	protected void openRemoteDatabase() throws IOException {
		// CONNECT TO THE SERVER
		parseServerURLs();
		createNetworkConnection();

		writeCommand(OChannelBinaryProtocol.REQUEST_DB_OPEN);
		network.writeString(name).writeString(userName).writeString(userPassword);
		network.readStatus();

		txId = network.readInt();
		OLogManager.instance().debug(null, "Client connected with session id: " + txId);

		int tot = network.readInt();
		String clusterName;
		for (int i = 0; i < tot; ++i) {
			clusterName = network.readString().toLowerCase();
			clustersIds.put(clusterName, network.readInt());
			clustersTypes.put(clusterName, network.readString());
		}

		defaultClusterId = clustersIds.get(OStorage.CLUSTER_DEFAULT_NAME);

		open = true;
	}

	public OChannelBinaryClient getNetwork() {
		return network;
	}

	/**
	 * Parse the URL in the following formats:<br/>
	 */
	protected void parseServerURLs() {
		String remoteHost;
		String[] remotePorts;

		int dbPos = url.indexOf("/");
		if (dbPos == -1) {
			// SHORT FORM
			name = url;
			remoteHost = getDefaultHost();
			remotePorts = getDefaultPort();
		} else {
			name = url.substring(dbPos + 1);

			int startPos = 0;
			int endPos = 0;

			while (endPos < dbPos) {
				if (url.indexOf(ADDRESS_SEPARATOR, startPos) > -1)
					endPos = url.indexOf(ADDRESS_SEPARATOR, startPos);
				else
					endPos = dbPos;

				int posRemotePort = url.indexOf(":", startPos);

				if (posRemotePort != -1 && posRemotePort < endPos) {
					remoteHost = url.substring(startPos, posRemotePort);
					remotePorts = url.substring(posRemotePort + 1, endPos).split("_");
					startPos = endPos + 1;
				} else {
					remoteHost = url.substring(startPos, endPos);
					remotePorts = getDefaultPort();
					startPos = endPos + 1;
				}

				// REGISTER THE REMOTE SERVER+PORT
				serverURLs.add(new OPair<String, String[]>(remoteHost, remotePorts));
			}
		}
	}

	protected String getDefaultHost() {
		return DEFAULT_HOST;
	}

	protected String[] getDefaultPort() {
		return DEFAULT_PORTS;
	}

	protected void createNetworkConnection() throws IOException, UnknownHostException {
		int port;

		for (OPair<String, String[]> server : serverURLs) {
			port = Integer.parseInt(server.getValue()[server.getValue().length - 1]);

			OLogManager.instance().debug(this, "Trying to connect to the remote host %s:%d...", server.getKey(), port);
			try {
				network = new OChannelBinaryClient(server.getKey(), port, clientConfiguration);
				return;
			} catch (Exception e) {
			}
		}

		final StringBuilder buffer = new StringBuilder();
		for (OPair<String, String[]> server : serverURLs) {
			if (buffer.length() > 0)
				buffer.append(',');
			buffer.append(server.getKey());
		}

		throw new OIOException("Can't connect to any configured remote nodes: " + buffer);
	}

	protected void checkConnection() {
		if (getNetwork() == null)
			throw new ODatabaseException("Connection is closed");
	}

	private ORecordInternal<?> readRecordFromNetwork(final ODatabaseRecord<?> iDatabase) throws IOException {
		final int classId = network.readShort();
		if (classId == OChannelBinaryProtocol.RECORD_NULL)
			return null;

		ORecordInternal<?> record = ORecordFactory.newInstance(network.readByte());

		if (record instanceof ORecordSchemaAware<?>)
			((ORecordSchemaAware<?>) record).fill(iDatabase, classId, network.readShort(), network.readLong(), getNetwork().readInt());
		else
			// DISCARD CLASS ID
			record.fill(iDatabase, network.readShort(), network.readLong(), network.readInt());

		record.fromStream(network.readBytes());

		return record;
	}

	protected void writeCommand(final byte iCommand) throws IOException {
		network.writeByte(iCommand);
		network.writeInt(txId);
	}

	public long getVersion() {
		throw new UnsupportedOperationException("getVersion");
	}
}
