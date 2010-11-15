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
package com.orientechnologies.orient.server.network.protocol.binary;

import java.io.EOFException;
import java.io.IOException;
import java.net.Socket;
import java.net.SocketException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.command.OCommandRequestInternal;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.command.OCommandResultListener;
import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseComplex;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.raw.ODatabaseRaw;
import com.orientechnologies.orient.core.db.record.ODatabaseRecordTx;
import com.orientechnologies.orient.core.engine.local.OEngineLocal;
import com.orientechnologies.orient.core.engine.memory.OEngineMemory;
import com.orientechnologies.orient.core.exception.OSecurityAccessException;
import com.orientechnologies.orient.core.fetch.OFetchHelper;
import com.orientechnologies.orient.core.fetch.OFetchListener;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.security.OUser;
import com.orientechnologies.orient.core.query.OQuery;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordAbstract;
import com.orientechnologies.orient.core.record.ORecordFactory;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.ORecordSchemaAware;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.record.OSerializationThreadLocal;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializerAnyRuntime;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializerAnyStreamable;
import com.orientechnologies.orient.core.storage.OCluster;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.impl.local.ODictionaryLocal;
import com.orientechnologies.orient.core.storage.impl.local.OStorageLocal;
import com.orientechnologies.orient.core.storage.impl.memory.OStorageMemory;
import com.orientechnologies.orient.enterprise.channel.OChannel;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinary;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryServer;
import com.orientechnologies.orient.server.OClientConnection;
import com.orientechnologies.orient.server.OClientConnectionManager;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.OServerMain;
import com.orientechnologies.orient.server.config.OServerUserConfiguration;
import com.orientechnologies.orient.server.handler.OServerHandler;
import com.orientechnologies.orient.server.handler.distributed.ODistributedServerRecordHook;
import com.orientechnologies.orient.server.handler.distributed.discovery.ODistributedServerDiscoveryManager;
import com.orientechnologies.orient.server.network.protocol.ONetworkProtocol;
import com.orientechnologies.orient.server.tx.OTransactionOptimisticProxy;
import com.orientechnologies.orient.server.tx.OTransactionRecordProxy;

public class ONetworkProtocolBinary extends ONetworkProtocol {
	protected OClientConnection				connection;
	protected OChannelBinary					channel;
	protected OUser										account;

	protected String									user;
	protected String									passwd;
	protected ODatabaseRaw						underlyingDatabase;
	protected int											commandType;
	protected int											clientTxId;
	private OServerUserConfiguration	serverUser;

	public ONetworkProtocolBinary() {
		super(OServer.getThreadGroup(), "Binary-DB");
	}

	public ONetworkProtocolBinary(final String iThreadName) {
		super(OServer.getThreadGroup(), iThreadName);
	}

	@Override
	public void config(final Socket iSocket, final OClientConnection iConnection, final OContextConfiguration iConfig)
			throws IOException {
		channel = new OChannelBinaryServer(iSocket, iConfig);
		connection = iConnection;
		start();
	}

	@Override
	protected void execute() throws Exception {
		commandType = -1;
		data.commandInfo = "Listening";
		data.commandDetail = "-";

		clientTxId = 0;

		try {
			commandType = channel.readByte();
			clientTxId = channel.readInt();

			++data.totalRequests;

			data.lastCommandReceived = System.currentTimeMillis();

			parseCommand();

		} catch (EOFException eof) {
			shutdown();
		} catch (SocketException e) {
			shutdown();
		} catch (OException e) {
			sendError(clientTxId, e);
		} catch (Throwable t) {
			OLogManager.instance().error(this, "Error on executing request", t);
			sendError(clientTxId, t);
		} finally {
			try {
				channel.flush();
			} catch (Throwable t) {
				OLogManager.instance().debug(this, "Error on send data over the network", t);
			}

			OSerializationThreadLocal.INSTANCE.get().clear();

			data.lastCommandExecutionTime = System.currentTimeMillis() - data.lastCommandReceived;
			data.totalCommandExecutionTime += data.lastCommandExecutionTime;

			data.lastCommandInfo = data.commandInfo;
			data.lastCommandDetail = data.commandDetail;
		}
	}

	@SuppressWarnings("unchecked")
	protected void parseCommand() throws IOException {
		switch (commandType) {

		case OChannelBinaryProtocol.REQUEST_SHUTDOWN: {
			data.commandInfo = "Shutdowning";

			OLogManager.instance().info(this, "Received shutdown command from the remote client %s:%d", channel.socket.getInetAddress(),
					channel.socket.getPort());

			user = channel.readString();
			passwd = channel.readString();

			if (OServerMain.server().authenticate(user, passwd, "shutdown")) {
				OLogManager.instance().info(this, "Remote client %s:%d authenticated. Starting shutdown of server...",
						channel.socket.getInetAddress(), channel.socket.getPort());

				sendOk(clientTxId);
				channel.flush();
				channel.close();
				OServerMain.server().shutdown();
				System.exit(0);
				return;
			}

			OLogManager.instance().error(this, "Authentication error of remote client %s:%d: shutdown is aborted.",
					channel.socket.getInetAddress(), channel.socket.getPort());

			sendError(clientTxId, new OSecurityAccessException("Invalid user/password to shutdown the server"));
			break;
		}

		case OChannelBinaryProtocol.REQUEST_CONNECT: {
			data.commandInfo = "Connect";

			serverLogin(channel.readString(), channel.readString());

			sendOk(clientTxId);
			channel.writeInt(connection.id);
			break;
		}

		case OChannelBinaryProtocol.REQUEST_DB_OPEN: {
			data.commandInfo = "Open database";

			String dbURL = channel.readString();
			String dbName = dbURL.substring(dbURL.lastIndexOf(":") + 1);

			user = channel.readString();
			passwd = channel.readString();

			// SEARCH THE DB IN MEMORY FIRST
			connection.database = (ODatabaseDocumentTx) OServerMain.server().getMemoryDatabases().get(dbName);

			if (connection.database == null)
				// SEARCH THE DB IN LOCAL FS
				connection.database = new ODatabaseDocumentTx(OServerMain.server().getStoragePath(dbName));

			if (connection.database.isClosed())
				if (connection.database.getStorage() instanceof OStorageMemory)
					connection.database.create();
				else
					connection.database.open(user, passwd);

			underlyingDatabase = ((ODatabaseRaw) ((ODatabaseComplex<?>) connection.database.getUnderlying()).getUnderlying());

			installClusterTrigger();

			if (!(underlyingDatabase.getStorage() instanceof OStorageMemory) && !loadUserFromSchema(user, passwd)) {
				sendError(clientTxId, new OSecurityAccessException(connection.database.getName(), "Access denied to database '"
						+ connection.database.getName() + "' for user: " + user));
			} else {
				sendOk(clientTxId);
				channel.writeInt(connection.id);
				channel.writeInt(connection.database.getClusterNames().size());
				for (OCluster c : (connection.database.getStorage()).getClusters()) {
					if (c != null) {
						channel.writeString(c.getName());
						channel.writeInt(c.getId());
						channel.writeString(c.getType());
					}
				}
			}

			break;
		}

		case OChannelBinaryProtocol.REQUEST_DB_CREATE: {
			data.commandInfo = "Create database";

			String dbName = channel.readString();
			String storageMode = channel.readString();

			checkServerAccess("database.create");

			final String path;

			if (storageMode.equals(OEngineLocal.NAME)) {
				if (OServerMain.server().existsStoragePath(dbName))
					throw new IllegalArgumentException("Database '" + dbName + "' already exists.");

				path = storageMode + ":${ORIENTDB_HOME}/databases/" + dbName + "/" + dbName;
			} else if (storageMode.equals(OEngineMemory.NAME)) {
				if (OServerMain.server().getMemoryDatabases().containsKey(dbName))
					throw new IllegalArgumentException("Database '" + dbName + "' already exists.");

				path = storageMode + ":" + dbName;
			} else
				throw new IllegalArgumentException("Can't create databse: storage mode '" + storageMode + "' is not supported.");

			connection.database = new ODatabaseDocumentTx(path);
			connection.database.create();

			if (storageMode.equals(OEngineLocal.NAME)) {
				// CLOSE IT BECAUSE IT WILL BE OPEN AT FIRST USE
				connection.database.close();

			} else if (storageMode.equals(OEngineMemory.NAME)) {
				// SAVE THE DB IN MEMORY
				OServerMain.server().getMemoryDatabases().put(dbName, connection.database);
			}

			underlyingDatabase = ((ODatabaseRaw) ((ODatabaseComplex<?>) connection.database.getUnderlying()).getUnderlying());

			installClusterTrigger();

			sendOk(clientTxId);
			break;
		}

		case OChannelBinaryProtocol.REQUEST_DB_CLOSE:
			data.commandInfo = "Close Database";

			connection.database.close();
			break;

		case OChannelBinaryProtocol.REQUEST_DB_EXIST: {
			data.commandInfo = "Exists database";

			channel.writeByte((byte) (connection.database.exists() ? 1 : 0));

			sendOk(clientTxId);
			break;
		}

		case OChannelBinaryProtocol.REQUEST_DATACLUSTER_COUNT: {
			data.commandInfo = "Count cluster elements";

			int[] clusterIds = new int[channel.readShort()];
			for (int i = 0; i < clusterIds.length; ++i)
				clusterIds[i] = channel.readShort();

			long count = connection.database.countClusterElements(clusterIds);

			sendOk(clientTxId);
			channel.writeLong(count);
			break;
		}

		case OChannelBinaryProtocol.REQUEST_DATACLUSTER_DATARANGE: {
			data.commandInfo = "Get the begin/end range of data in cluster";

			long[] pos = connection.database.getStorage().getClusterDataRange(channel.readShort());

			sendOk(clientTxId);
			channel.writeLong(pos[0]);
			channel.writeLong(pos[1]);
			break;
		}

		case OChannelBinaryProtocol.REQUEST_DATACLUSTER_ADD: {
			data.commandInfo = "Add cluster";

			final String type = channel.readString();
			final String name = channel.readString();

			final int num;
			OStorage.CLUSTER_TYPE t = OStorage.CLUSTER_TYPE.valueOf(type);
			switch (t) {
			case PHYSICAL:
				num = connection.database.addPhysicalCluster(name, channel.readString(), channel.readInt());
				break;

			case MEMORY:
				num = connection.database.getStorage().addCluster(name, t);
				break;

			case LOGICAL:
				num = connection.database.addLogicalCluster(name, channel.readInt());
				break;

			default:
				throw new IllegalArgumentException("Cluster type " + type + " is not supported");
			}

			sendOk(clientTxId);
			channel.writeShort((short) num);
			break;
		}

		case OChannelBinaryProtocol.REQUEST_DATACLUSTER_REMOVE: {
			data.commandInfo = "remove cluster";

			final int id = channel.readShort();

			boolean result = connection.database.getStorage().removeCluster(id);

			sendOk(clientTxId);
			channel.writeByte((byte) (result ? '1' : '0'));
			break;
		}

		case OChannelBinaryProtocol.REQUEST_RECORD_LOAD: {
			data.commandInfo = "Load record";

			final short clusterId = channel.readShort();
			final long clusterPosition = channel.readLong();
			final String fetchPlanString = channel.readString();

			// LOAD THE RAW BUFFER
			final ORawBuffer buffer = underlyingDatabase.read(clusterId, clusterPosition, null);
			sendOk(clientTxId);

			if (buffer != null) {
				// SEND THE ROOT BUFFER
				channel.writeByte((byte) 1);
				channel.writeBytes(buffer.buffer);
				channel.writeInt(buffer.version);
				channel.writeByte(buffer.recordType);

				if (fetchPlanString.length() > 0) {
					// BUILD THE SERVER SIDE RECORD TO ACCES TO THE FETCH
					// PLAN
					final ORecordInternal<?> record = ORecordFactory.newInstance(buffer.recordType);
					record.fill(connection.database, clusterId, clusterPosition, buffer.version);
					record.fromStream(buffer.buffer);

					if (record instanceof ODocument) {
						final Map<String, Integer> fetchPlan = OFetchHelper.buildFetchPlan(fetchPlanString);

						final Set<ODocument> recordsToSend = new HashSet<ODocument>();
						OFetchHelper.fetch((ODocument) record, record, fetchPlan, null, 0, -1, new OFetchListener() {
							public int size() {
								return recordsToSend.size();
							}

							// ADD TO THE SET OF OBJECTS TO SEND
							public Object fetchLinked(final ODocument iRoot, final Object iUserObject, final String iFieldName,
									final Object iLinked) {
								if (iLinked instanceof ODocument)
									return recordsToSend.add((ODocument) iLinked) ? iLinked : null;
								else if (iLinked instanceof Collection<?>)
									return recordsToSend.addAll((Collection<? extends ODocument>) iLinked) ? iLinked : null;
								else if (iLinked instanceof Map<?, ?>)
									return recordsToSend.addAll(((Map<String, ? extends ODocument>) iLinked).values()) ? iLinked : null;
								else
									throw new IllegalArgumentException("Unrecognized type while fetching records: " + iLinked);
							}
						});

						// SEND RECORDS TO LOAD IN CLIENT CACHE
						for (ODocument doc : recordsToSend) {
							channel.writeByte((byte) 2); // CLIENT CACHE
							// RECORD. IT
							// ISN'T PART OF
							// THE RESULT
							// SET
							writeRecord(doc);
						}
					}

					channel.writeByte((byte) 0); // NO MORE RECORDS

				} else
					channel.writeByte((byte) 0);
			} else
				channel.writeByte((byte) 0);
			break;
		}

		case OChannelBinaryProtocol.REQUEST_RECORD_CREATE:
			data.commandInfo = "Create record";

			final long location = underlyingDatabase.save(channel.readShort(), ORID.CLUSTER_POS_INVALID, channel.readBytes(), -1,
					channel.readByte());
			sendOk(clientTxId);
			channel.writeLong(location);
			break;

		case OChannelBinaryProtocol.REQUEST_RECORD_UPDATE:
			data.commandInfo = "Update record";

			final int clusterId = channel.readShort();
			final long position = channel.readLong();

			long newVersion = underlyingDatabase.save(clusterId, position, channel.readBytes(), channel.readInt(), channel.readByte());

			// TODO: Handle it by using triggers
			if (connection.database.getMetadata().getSchema().getDocument().getIdentity().getClusterId() == clusterId
					&& connection.database.getMetadata().getSchema().getDocument().getIdentity().getClusterPosition() == position)
				connection.database.getMetadata().loadSchema();
			else if (((ODictionaryLocal<?>) connection.database.getDictionary()).getTree().getRecord().getIdentity().getClusterId() == clusterId
					&& ((ODictionaryLocal<?>) connection.database.getDictionary()).getTree().getRecord().getIdentity().getClusterPosition() == position)
				((ODictionaryLocal<?>) connection.database.getDictionary()).load();

			sendOk(clientTxId);

			channel.writeInt((int) newVersion);
			break;

		case OChannelBinaryProtocol.REQUEST_RECORD_DELETE:
			data.commandInfo = "Delete record";

			underlyingDatabase.delete(channel.readShort(), channel.readLong(), channel.readInt());
			sendOk(clientTxId);

			channel.writeByte((byte) '1');
			break;

		case OChannelBinaryProtocol.REQUEST_COUNT: {
			data.commandInfo = "Count cluster records";

			final String clusterName = channel.readString();
			final long size = connection.database.countClusterElements(clusterName);

			sendOk(clientTxId);

			channel.writeLong(size);
			break;
		}

		case OChannelBinaryProtocol.REQUEST_COMMAND: {
			data.commandInfo = "Execute remote command";

			final boolean asynch = channel.readByte() == 'a';

			final OCommandRequestText command = (OCommandRequestText) OStreamSerializerAnyStreamable.INSTANCE.fromStream(
					connection.database, channel.readBytes());

			final OQuery<?> query = (OQuery<?>) (command instanceof OQuery<?> ? command : null);

			data.commandDetail = command.getText();

			if (asynch) {
				// ASYNCHRONOUS
				final StringBuilder empty = new StringBuilder();
				final Set<ODocument> recordsToSend = new HashSet<ODocument>();
				final int txId = clientTxId;

				final Map<String, Integer> fetchPlan = query != null ? OFetchHelper.buildFetchPlan(query.getFetchPlan()) : null;
				command.setResultListener(new OCommandResultListener() {
					public boolean result(final Object iRecord) {
						if (empty.length() == 0)
							try {
								sendOk(txId);
								empty.append("-");
							} catch (IOException e1) {
							}

						try {
							channel.writeByte((byte) 1); // ONE MORE RECORD
							writeRecord((ORecordInternal<?>) iRecord);
							// TEMPORARY TEST: ON LINUX THIS SLOW DOWN A LOT
							// channel.flush();

							if (fetchPlan != null && iRecord instanceof ODocument) {
								OFetchHelper.fetch((ODocument) iRecord, iRecord, fetchPlan, null, 0, -1, new OFetchListener() {
									public int size() {
										return recordsToSend.size();
									}

									// ADD TO THE SET OF OBJECT TO
									// SEND
									public Object fetchLinked(final ODocument iRoot, final Object iUserObject, final String iFieldName,
											final Object iLinked) {
										if (iLinked instanceof ODocument)
											return recordsToSend.add((ODocument) iLinked) ? iLinked : null;
										else if (iLinked instanceof Collection<?>)
											return recordsToSend.addAll((Collection<? extends ODocument>) iLinked) ? iLinked : null;
										else if (iLinked instanceof Map<?, ?>)
											return recordsToSend.addAll(((Map<String, ? extends ODocument>) iLinked).values()) ? iLinked : null;
										else
											throw new IllegalArgumentException("Unrecognized type while fetching records: " + iLinked);
									}
								});
							}

						} catch (IOException e) {
							return false;
						}

						return true;
					}
				});

				((OCommandRequestInternal) connection.database.command(command)).execute();

				if (empty.length() == 0)
					try {
						sendOk(clientTxId);
					} catch (IOException e1) {
					}

				// SEND RECORDS TO LOAD IN CLIENT CACHE
				for (ODocument doc : recordsToSend) {
					channel.writeByte((byte) 2); // CLIENT CACHE RECORD. IT
					// ISN'T PART OF THE
					// RESULT SET
					writeRecord(doc);
				}

				channel.writeByte((byte) 0); // NO MORE RECORDS
			} else {
				// SYNCHRONOUS
				final Object result = ((OCommandRequestInternal) connection.database.command(command)).execute();

				sendOk(clientTxId);

				if (result == null) {
					// NULL VALUE
					channel.writeByte((byte) 'n');
					channel.writeBytes(null);
				} else if (result instanceof ORecord<?>) {
					// RECORD
					channel.writeByte((byte) 'r');
					writeRecord((ORecordInternal<?>) result);
				} else {
					// ANY OTHER (INCLUDING LITERALS)
					channel.writeByte((byte) 'a');
					channel.writeBytes(OStreamSerializerAnyRuntime.INSTANCE.toStream(connection.database, result));
				}
			}
			break;
		}

		case OChannelBinaryProtocol.REQUEST_DICTIONARY_LOOKUP: {
			data.commandInfo = "Dictionary lookup";

			final String key = channel.readString();
			final ORecordAbstract<?> value = connection.database.getDictionary().get(key);

			if (value != null)
				((ODatabaseRecordTx<ORecordInternal<?>>) connection.database.getUnderlying()).load(value);

			sendOk(clientTxId);

			writeRecord(value);
			break;
		}

		case OChannelBinaryProtocol.REQUEST_DICTIONARY_PUT: {
			data.commandInfo = "Dictionary put";

			String key = channel.readString();
			ORecordInternal<?> value = ORecordFactory.newInstance(channel.readByte());

			final ORecordId rid = new ORecordId(channel.readShort(), channel.readLong());
			value.setIdentity(rid.clusterId, rid.clusterPosition);
			value.setDatabase(connection.database);

			value = connection.database.getDictionary().putRecord(key, value);

			if (value != null)
				((ODatabaseRecordTx<ORecordInternal<?>>) connection.database.getUnderlying()).load(value);

			sendOk(clientTxId);

			writeRecord(value);
			break;
		}

		case OChannelBinaryProtocol.REQUEST_DICTIONARY_REMOVE: {
			data.commandInfo = "Dictionary remove";

			final String key = channel.readString();
			final ORecordInternal<?> value = connection.database.getDictionary().remove(key);

			if (value != null)
				((ODatabaseRecordTx<ORecordInternal<?>>) connection.database.getUnderlying()).load(value);

			sendOk(clientTxId);

			writeRecord(value);
			break;
		}

		case OChannelBinaryProtocol.REQUEST_DICTIONARY_SIZE: {
			data.commandInfo = "Dictionary size";

			sendOk(clientTxId);
			channel.writeInt(connection.database.getDictionary().size());
			break;
		}

		case OChannelBinaryProtocol.REQUEST_DICTIONARY_KEYS: {
			data.commandInfo = "Dictionary keys";

			sendOk(clientTxId);
			channel.writeCollectionString(connection.database.getDictionary().keySet());
			break;
		}

		case OChannelBinaryProtocol.REQUEST_TX_COMMIT:
			data.commandInfo = "Transaction commit";

			((OStorageLocal) connection.database.getStorage()).commit(connection.database.getId(), new OTransactionOptimisticProxy(
					(ODatabaseRecordTx<OTransactionRecordProxy>) connection.database.getUnderlying(), channel));

			sendOk(clientTxId);
			break;

		case OChannelBinaryProtocol.REQUEST_CONFIG_GET: {
			data.commandInfo = "Get config";

			final String key = channel.readString();
			final OGlobalConfiguration cfg = OGlobalConfiguration.findByKey(key);
			String cfgValue = cfg != null ? cfg.getValueAsString() : "";

			sendOk(clientTxId);
			channel.writeString(cfgValue);
			break;
		}

		case OChannelBinaryProtocol.REQUEST_CONFIG_SET: {
			data.commandInfo = "Get config";

			final String key = channel.readString();
			final String value = channel.readString();
			final OGlobalConfiguration cfg = OGlobalConfiguration.findByKey(key);
			if (cfg != null)
				cfg.setValue(value);

			sendOk(clientTxId);
			break;
		}

		case OChannelBinaryProtocol.REQUEST_CONFIG_LIST: {
			data.commandInfo = "List config";

			sendOk(clientTxId);

			channel.writeShort((short) OGlobalConfiguration.values().length);
			for (OGlobalConfiguration cfg : OGlobalConfiguration.values()) {
				channel.writeString(cfg.getKey());
				channel.writeString(cfg.getValueAsString() != null ? cfg.getValueAsString() : "");
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

	private void serverLogin(final String iUser, final String iPassword) {
		if (!OServerMain.server().authenticate(iUser, iPassword, "connect"))
			throw new OSecurityAccessException(
					"Wrong user/password to [connect] to the remote OrientDB Server instance. Get the user/password from the config/orientdb-server-config.xml file");

		serverUser = OServerMain.server().getUser(iUser);
	}

	private void checkServerAccess(final String iResource) {
		if (serverUser == null)
			throw new OSecurityAccessException("Server user not authenticated.");

		if (!OServerMain.server().authenticate(serverUser.name, null, iResource))
			throw new OSecurityAccessException("User '" + serverUser.name + "' can't access to the resource [" + iResource
					+ "]. Use another server user or change permission in the file config/orientdb-server-config.xml");
	}

	@Override
	public OChannel getChannel() {
		return channel;
	}

	@Override
	public void shutdown() {
		sendShutdown();
		channel.close();

		OClientConnectionManager.instance().onClientDisconnection(connection.id);
	}

	protected void sendOk(final int iClientTxId) throws IOException {
		channel.writeByte(OChannelBinaryProtocol.RESPONSE_STATUS_OK);
		channel.writeInt(iClientTxId);
	}

	protected void sendError(final int iClientTxId, final Throwable t) throws IOException {
		channel.writeByte(OChannelBinaryProtocol.RESPONSE_STATUS_ERROR);
		channel.writeInt(iClientTxId);

		Throwable current = t;
		while (current != null) {
			channel.writeString(current.getClass().getName());
			channel.writeString(current != null ? current.getMessage() : null);

			current = current.getCause();

			if (current != null)
				// MORE DETAILS ARE COMING
				channel.writeByte((byte) 1);
		}
		channel.writeByte((byte) 0);

		channel.clearInput();
	}

	private boolean loadUserFromSchema(final String iUserName, final String iUserPassword) {
		account = connection.database.getMetadata().getSecurity().getUser(iUserName);
		if (account == null)
			throw new OSecurityAccessException(connection.database.getName(), "User '" + iUserName + "' was not found in database: "
					+ connection.database.getName());

		boolean allow = account.checkPassword(iUserPassword);

		if (!allow)
			account = null;

		return allow;
	}

	/**
	 * Write a record using this format:<br/>
	 * - 2 bytes: class id [-2=no record, -1=no class id, > -1 = valid] <br/>
	 * - 1 byte: record type [v,c,b] <br/>
	 * - 2 bytes: cluster id <br/>
	 * - 8 bytes: position in cluster <br/>
	 * - 4 bytes: record version <br/>
	 * - x bytes: record vontent <br/>
	 * 
	 * @param iRecord
	 * @throws IOException
	 */
	private void writeRecord(final ORecordInternal<?> iRecord) throws IOException {
		if (iRecord == null) {
			channel.writeShort((short) OChannelBinaryProtocol.RECORD_NULL);
		} else {
			channel.writeShort((short) (iRecord instanceof ORecordSchemaAware<?>
					&& ((ORecordSchemaAware<?>) iRecord).getSchemaClass() != null ? ((ORecordSchemaAware<?>) iRecord).getSchemaClass()
					.getId() : -1));

			channel.writeByte(iRecord.getRecordType());
			channel.writeShort((short) iRecord.getIdentity().getClusterId());
			channel.writeLong(iRecord.getIdentity().getClusterPosition());
			channel.writeInt(iRecord.getVersion());
			channel.writeBytes(iRecord.toStream());
		}
	}

	private void installClusterTrigger() {
		final OServerHandler manager = OServerMain.server().getHandler(ODistributedServerDiscoveryManager.class);
		if (manager != null)
			// INSTALL TRIGGER TO CATCH ALL THE EVENTS ON RECORDS
			connection.database.registerHook(new ODistributedServerRecordHook(connection));
	}
}
