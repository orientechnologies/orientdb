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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.util.OPair;
import com.orientechnologies.orient.client.config.OClientConfiguration;
import com.orientechnologies.orient.client.dictionary.ODictionaryClient;
import com.orientechnologies.orient.client.query.OSQLAsynchQueryRemoteExecutor;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OStorageConfiguration;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.dictionary.ODictionary;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.OIOException;
import com.orientechnologies.orient.core.exception.OQueryExecutionException;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.query.OAsynchQuery;
import com.orientechnologies.orient.core.query.OQuery;
import com.orientechnologies.orient.core.query.OQueryExecutor;
import com.orientechnologies.orient.core.query.OQueryInternal;
import com.orientechnologies.orient.core.query.sql.OSQLAsynchQuery;
import com.orientechnologies.orient.core.query.sql.OSQLQuery;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordFactory;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.ORecordSchemaAware;
import com.orientechnologies.orient.core.serialization.OSerializableStream;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializerAnyStreamable;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import com.orientechnologies.orient.core.storage.OStorageAbstract;
import com.orientechnologies.orient.core.storage.impl.logical.OClusterLogical;
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
	private static final String[]						DEFAULT_PORTS			= new String[] { "8000" };
	private static final String							ADDRESS_SEPARATOR	= ";";

	private String													userName;
	private String													userPassword;
	protected List<OPair<String, String[]>>	serverURLs				= new ArrayList<OPair<String, String[]>>();
	private final OClientConfiguration			clientConfiguration;
	protected OChannelBinaryClient					network;
	protected String												sessionId;
	protected final Map<String, Integer>		clusters					= new HashMap<String, Integer>();
	protected int														retry							= 0;

	public OStorageRemote(final String iURL, final String iMode) throws IOException {
		super(iURL, iURL, iMode);
		configuration = new OStorageConfiguration(this);
		clientConfiguration = new OClientConfiguration();
	}

	public void open(final int iRequesterId, final String iUserName, final String iUserPassword) {
		boolean locked = acquireExclusiveLock();

		try {
			userName = iUserName;
			userPassword = iUserPassword;

			openRemoteDatabase();
			addUser();

			Orient.instance().registerStorage(this);

		} catch (Exception e) {
			close();
			throw new OStorageException("Can't open the remote storage: " + name, e);
		} finally {

			releaseExclusiveLock(locked);
		}
	}

	public void create(final String iStorageMode) {
		throw new UnsupportedOperationException(
				"Can't create a database in a remote server. Please use the console or the OServerAdmin class.");
	}

	public boolean exists() {
		checkConnection();

		do {
			boolean locked = acquireExclusiveLock();

			try {
				network.writeByte(OChannelBinaryProtocol.DB_EXIST);
				network.flush();

				readStatus();
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
			network.writeByte(OChannelBinaryProtocol.DB_CLOSE);
			network.out.flush();

			network.socket.close();

			open = false;

			Orient.instance().unregisterStorage(this);

		} catch (Exception e) {

		} finally {
			releaseExclusiveLock(locked);
		}
	}

	public Set<String> getClusterNames() {
		checkConnection();

		boolean locked = acquireSharedLock();

		try {
			return clusters.keySet();

		} finally {
			releaseSharedLock(locked);
		}
	}

	public long createRecord(final int iClusterId, final byte[] iContent, final byte iRecordType) {
		checkConnection();

		do {
			boolean locked = acquireExclusiveLock();

			try {
				network.writeByte(OChannelBinaryProtocol.RECORD_CREATE);
				network.writeShort((short) iClusterId);
				network.writeByte(iRecordType);
				network.writeBytes((byte[]) iContent);
				network.flush();

				readStatus();
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

	public ORawBuffer readRecord(final int iRequesterId, final int iClusterId, final long iPosition) {
		checkConnection();

		if (OStorageRemoteThreadLocal.INSTANCE.get())
			// PENDING NETWORK OPERATION, CAN'T EXECUTE IT NOW
			return null;

		do {
			boolean locked = acquireExclusiveLock();

			try {
				network.writeByte(OChannelBinaryProtocol.RECORD_LOAD);
				network.writeShort((short) iClusterId);
				network.writeLong(iPosition);
				network.flush();

				readStatus();
				if (network.readByte() == 1)
					return new ORawBuffer(network.readBytes(), network.readInt(), network.readByte());
				else
					return null;

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
				network.writeByte(OChannelBinaryProtocol.RECORD_UPDATE);
				network.writeShort((short) iClusterId);
				network.writeLong(iPosition);
				network.writeInt(iVersion);
				network.writeByte(iRecordType);
				network.writeBytes((byte[]) iContent);
				network.flush();

				readStatus();

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

	public void deleteRecord(final int iRequesterId, final int iClusterId, final long iPosition, final int iVersion) {
		checkConnection();

		do {
			boolean locked = acquireExclusiveLock();

			try {
				network.writeByte(OChannelBinaryProtocol.RECORD_DELETE);
				network.writeShort((short) iClusterId);
				network.writeLong(iPosition);
				network.writeInt(iVersion);
				network.flush();

				readStatus();
				break;
			} catch (Exception e) {
				if (handleException("Error on delete record: " + iClusterId + ":" + iPosition, e))
					break;

			} finally {
				releaseExclusiveLock(locked);
			}
		} while (true);
	}

	public long count(final int iClusterId) {
		return count(new int[] { iClusterId });
	}

	public long count(final int[] iClusterIds) {
		checkConnection();

		do {
			boolean locked = acquireExclusiveLock();

			try {
				network.writeByte(OChannelBinaryProtocol.CLUSTER_COUNT);
				network.writeShort((short) iClusterIds.length);
				for (int i = 0; i < iClusterIds.length; ++i)
					network.writeShort((short) iClusterIds[i]);
				network.flush();

				readStatus();
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
				network.writeByte(OChannelBinaryProtocol.COUNT);
				network.writeString(iClassName);
				network.flush();

				readStatus();
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
	 * Execute the query into the server side and get back the resultset.
	 */
	public <T extends ORecordSchemaAware<?>> List<T> query(final OQuery<T> iQuery, final int iLimit) {
		checkConnection();

		if (!(iQuery instanceof OSerializableStream))
			throw new OQueryExecutionException("Can't serialize the query to being executed to the server side.");

		OSerializableStream query = (OSerializableStream) iQuery;

		final List<T> result = new ArrayList<T>();

		do {
			boolean locked = acquireExclusiveLock();

			OStorageRemoteThreadLocal.INSTANCE.set(Boolean.TRUE);

			try {
				OAsynchQuery<ORecordSchemaAware<?>> aquery = (OAsynchQuery<ORecordSchemaAware<?>>) iQuery;

				network.writeByte(OChannelBinaryProtocol.QUERY);
				network.writeInt(iLimit);
				network.writeBytes(OStreamSerializerAnyStreamable.INSTANCE.toStream(query));
				network.flush();

				readStatus();

				// ASYNCH: READ ONE RECORD AT TIME
				while (network.readByte() == 1) {
					ORecordSchemaAware<?> record = (ORecordSchemaAware<?>) readRecordFromNetwork(iQuery.getDatabase());
					if (record == null)
						break;

					// INVOKE THE LISTENER
					try {
						if (!aquery.getResultListener().result(record)) {
							// EMPTY THE INPUT CHANNEL
							while (network.in.available() > 0)
								network.in.read();

							break;
						}
					} catch (Throwable t) {
						// ABSORBE ALL THE USER EXCEPTIONS
					}
				}
				break;

			} catch (Exception e) {
				if (handleException("Error on executing query: " + ((OSQLQuery<?>) iQuery).text(), e))
					break;

			} finally {
				OStorageRemoteThreadLocal.INSTANCE.set(Boolean.FALSE);

				releaseExclusiveLock(locked);
			}
		} while (true);

		return result;
	}

	/**
	 * Execute the query into the server side and get back the result.
	 */
	public ORecordSchemaAware<?> queryFirst(final OQuery<?> iQuery) {
		checkConnection();

		if (!(iQuery instanceof OSerializableStream))
			throw new OQueryExecutionException("Can't serialize the query to being executed to the server side.");

		OSerializableStream query = (OSerializableStream) iQuery;

		do {
			boolean locked = acquireExclusiveLock();

			try {
				if (iQuery instanceof OSQLQuery<?>) {
					network.writeByte(OChannelBinaryProtocol.QUERY_FIRST);
					network.writeBytes(query.toStream());
					network.flush();

					readStatus();

					ORecordSchemaAware<?> record = ((ORecordSchemaAware<?>) ((OQueryInternal<?>) iQuery).getRecordClass().newInstance());

					return (ORecordSchemaAware<?>) record.fill(iQuery.getDatabase(), network.readShort(), network.readShort(),
							network.readLong(), network.readInt()).fromStream(network.readBytes());
				}
			} catch (Exception e) {
				if (handleException("Error on executing query: " + ((OSQLQuery<?>) iQuery).text(), e))
					break;

			} finally {
				releaseExclusiveLock(locked);
			}
		} while (true);
		return null;
	}

	public void commit(final int iRequesterId, final OTransaction<?> iTx) {
		checkConnection();

		do {
			boolean locked = acquireExclusiveLock();

			try {
				network.writeByte(OChannelBinaryProtocol.TX_COMMIT);
				network.writeInt(iTx.getId());
				network.writeInt(iTx.size());

				for (OTransactionEntry<? extends ORecord<?>> txEntry : iTx.getEntries()) {
					if (txEntry.status == OTransactionEntry.LOADED)
						// JUMP LOADED OBJECTS
						continue;

					network.writeByte((byte) txEntry.status);
					network.writeShort((short) txEntry.record.getIdentity().getClusterId());

					switch (txEntry.status) {
					case OTransactionEntry.CREATED:
						network.writeString(txEntry.clusterName);
						network.writeBytes(txEntry.record.toStream());
						break;

					case OTransactionEntry.UPDATED:
						network.writeLong(txEntry.record.getIdentity().getClusterPosition());
						network.writeInt(txEntry.record.getVersion());
						network.writeBytes(txEntry.record.toStream());

					case OTransactionEntry.DELETED:
						network.writeLong(txEntry.record.getIdentity().getClusterPosition());
						network.writeInt(txEntry.record.getVersion());
					}

				}
				network.flush();

				readStatus();
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
			final Integer id = clusters.get(iClusterName.toLowerCase());
			if (id == null)
				throw new IllegalArgumentException("Cluster " + iClusterName + " id not defined in database " + name);
			return id;

		} finally {
			releaseSharedLock(locked);
		}
	}

	public int addLogicalCluster(OClusterLogical iClusterLogical) {
		checkConnection();

		do {
			boolean locked = acquireExclusiveLock();

			try {
				network.writeByte(OChannelBinaryProtocol.CLUSTER_ADD);
				network.writeString(iClusterLogical.getName());
				network.flush();

				readStatus();

				int clusterId = network.readShort();
				iClusterLogical.setRID(new ORecordId(network.readString()));
				clusters.put(iClusterLogical.getName().toLowerCase(), clusterId);
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

	public int registerLogicalCluster(OClusterLogical iClusterLogical) {
		clusters.put(iClusterLogical.getName().toLowerCase(), iClusterLogical.getId());
		return iClusterLogical.getId();
	}

	public int addClusterSegment(final String iClusterName, final String iClusterFileName, final int iFileSize) {
		checkConnection();

		do {
			boolean locked = acquireExclusiveLock();

			try {
				network.writeByte(OChannelBinaryProtocol.CLUSTER_ADD);
				network.writeString(iClusterName).writeString(iClusterFileName).writeInt(iFileSize);
				network.flush();

				readStatus();

				int clusterId = network.readShort();
				clusters.put(iClusterName.toLowerCase(), clusterId);
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

	public int addDataSegment(final String iDataSegmentName) {
		return addDataSegment(iDataSegmentName, null);
	}

	public int addDataSegment(final String iSegmentName, final String iSegmentFileName) {
		checkConnection();

		do {
			boolean locked = acquireExclusiveLock();

			try {
				network.writeByte(OChannelBinaryProtocol.DATASEGMENT_ADD);
				network.writeString(iSegmentName).writeString(iSegmentFileName);
				network.flush();

				readStatus();
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

	public String getSessionId() {
		boolean locked = acquireSharedLock();

		try {
			return sessionId;

		} finally {
			releaseSharedLock(locked);
		}
	}

	public <REC extends ORecordInternal<?>> REC dictionaryPut(ODatabaseRecord<REC> iDatabase, final String iKey,
			final ORecord<?> iRecord) {
		checkConnection();

		do {
			boolean locked = acquireExclusiveLock();

			try {
				network.writeByte(OChannelBinaryProtocol.DICTIONARY_PUT);
				network.writeString(iKey);
				network.writeString(iRecord.getIdentity().toString());
				network.flush();

				readStatus();

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
				network.writeByte(OChannelBinaryProtocol.DICTIONARY_LOOKUP);
				network.writeString(iKey);
				network.flush();

				readStatus();

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
				network.writeByte(OChannelBinaryProtocol.DICTIONARY_REMOVE);
				network.writeString(iKey.toString());
				network.flush();

				readStatus();

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

	public int dictionarySize(ODatabaseRecord iDatabase) {
		checkConnection();

		do {
			boolean locked = acquireExclusiveLock();

			try {
				network.writeByte(OChannelBinaryProtocol.DICTIONARY_SIZE);
				network.flush();

				readStatus();
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

	public ODictionary createDictionary(ODatabaseRecord iDatabase) throws Exception {
		return new ODictionaryClient(iDatabase, this);
	}

	public Set<String> dictionaryKeys() {
		checkConnection();

		do {
			boolean locked = acquireExclusiveLock();

			try {
				network.writeByte(OChannelBinaryProtocol.DICTIONARY_KEYS);
				network.flush();

				readStatus();
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
		for (Entry<String, Integer> clusterEntry : clusters.entrySet()) {
			if (clusterEntry.getValue().intValue() == iClusterId)
				return clusterEntry.getKey();
		}
		return null;
	}

	public OQueryExecutor getQueryExecutor(OQuery<?> iQuery) {
		if (iQuery instanceof OSQLAsynchQuery<?>)
			return OSQLAsynchQueryRemoteExecutor.INSTANCE;

		throw new OConfigurationException("Query executor not configured for query type: " + iQuery.getClass());
	}

	protected void readStatus() throws IOException {
		final byte result = network.readByte();

		if (result == OChannelBinaryProtocol.ERROR)
			throw new OStorageException(network.readString(), null);
	}

	protected boolean handleException(final String iMessage, final Exception iException) {
		if (!(iException instanceof IOException))
			throw new OStorageException(iMessage, iException);

		if (retry < clientConfiguration.connectionRetry) {
			// WAIT THE DELAY BEFORE TO RETRY
			try {
				Thread.sleep(clientConfiguration.connectionRetryDelay);
			} catch (InterruptedException e) {
			}

			try {
				if (OLogManager.instance().isDebugEnabled())
					OLogManager.instance().debug(this,
							"Retrying to connect to remote server #" + retry + "/" + clientConfiguration.connectionRetry + "...");

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

		network.out.writeByte(OChannelBinaryProtocol.DB_OPEN);
		network.writeString(name).writeString(userName).writeString(userPassword);
		network.flush();

		readStatus();

		sessionId = network.readString();
		OLogManager.instance().debug(null, "Client connected with session id: " + sessionId);

		int tot = network.readInt();
		for (int i = 0; i < tot; ++i)
			clusters.put(network.readString().toLowerCase(), network.readInt());

		open = true;
	}

	/**
	 * Parse the URL in the following formats:<br/>
	 * <ul>
	 * <li>
	 * 
	 * <pre>
	 * <protocol>:<>
	 * </pre>
	 * 
	 * </li>
	 * <li>
	 * 
	 * <pre>
	 * <db-sename>
	 * </pre>
	 * 
	 * , to connect to the localhost, default port 8000</li>
	 * </ul>
	 */
	protected void parseServerURLs() {
		String remoteHost;
		String[] remotePorts;

		int dbPos = fileURL.indexOf("/");
		if (dbPos == -1) {
			// SHORT FORM
			name = fileURL;
			remoteHost = getDefaultHost();
			remotePorts = getDefaultPort();
		} else {
			name = fileURL.substring(dbPos + 1);

			int startPos = 0;
			int endPos = 0;

			while (endPos < dbPos) {
				if (fileURL.indexOf(ADDRESS_SEPARATOR, startPos) > -1)
					endPos = fileURL.indexOf(ADDRESS_SEPARATOR, startPos);
				else
					endPos = dbPos;

				int posRemotePort = fileURL.indexOf(":", startPos);

				if (posRemotePort != -1 && posRemotePort < endPos) {
					remoteHost = fileURL.substring(startPos, posRemotePort);
					remotePorts = fileURL.substring(posRemotePort + 1, endPos).split("_");
					startPos = endPos + 1;
				} else {
					remoteHost = fileURL.substring(startPos, endPos);
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
		for (OPair<String, String[]> server : serverURLs) {
			OLogManager.instance().debug(this, "Trying to connect to the remote host %s:%d...", server.getKey(), server.getValue()[1]);
			try {
				network = new OChannelBinaryClient(server.getKey(), Integer.parseInt(server.getValue()[1]),
						clientConfiguration.connectionTimeout);
				return;
			} catch (Exception e) {
			}
		}

		throw new OIOException("Can't connect to any configured remote nodes: " + serverURLs);
	}

	protected void checkConnection() {
		if (network == null)
			throw new ODatabaseException("Connection is closed");
	}

	private ORecord<?> readRecordFromNetwork(ODatabaseRecord<?> iDatabase) throws IOException {
		int classId = network.readShort();
		if (classId == OChannelBinaryProtocol.RECORD_NULL)
			return null;

		ORecordInternal<?> record = ORecordFactory.getRecord(network.readByte());

		if (record instanceof ORecordSchemaAware<?>)
			((ORecordSchemaAware<?>) record).fill(iDatabase, classId, network.readShort(), network.readLong(), network.readInt())
					.fromStream(network.readBytes());
		else {
			// DISCARD CLASS ID
			network.readShort();
			record.fill(iDatabase, classId, network.readLong(), network.readInt()).fromStream(network.readBytes());
		}

		return record;
	}
}
