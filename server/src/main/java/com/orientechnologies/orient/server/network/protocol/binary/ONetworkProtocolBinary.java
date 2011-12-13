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
import java.util.Map.Entry;
import java.util.Set;

import com.orientechnologies.common.concur.lock.OLockException;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.OCommandRequestInternal;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.command.OCommandResultListener;
import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabaseComplex;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.raw.ODatabaseRaw;
import com.orientechnologies.orient.core.db.record.ODatabaseRecordTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.engine.local.OEngineLocal;
import com.orientechnologies.orient.core.engine.memory.OEngineMemory;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.exception.OSecurityAccessException;
import com.orientechnologies.orient.core.exception.OSecurityException;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.fetch.OFetchHelper;
import com.orientechnologies.orient.core.fetch.OFetchListener;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OSchemaProxy;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.metadata.security.OUser;
import com.orientechnologies.orient.core.query.OQuery;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ORecordBytes;
import com.orientechnologies.orient.core.serialization.serializer.record.OSerializationThreadLocal;
import com.orientechnologies.orient.core.serialization.serializer.record.string.ORecordSerializerStringAbstract;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializerAnyStreamable;
import com.orientechnologies.orient.core.storage.OCluster;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.OStorageEmbedded;
import com.orientechnologies.orient.core.storage.impl.local.OStorageLocal;
import com.orientechnologies.orient.core.storage.impl.memory.OStorageMemory;
import com.orientechnologies.orient.enterprise.channel.OChannel;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryServer;
import com.orientechnologies.orient.enterprise.channel.binary.ONetworkProtocolException;
import com.orientechnologies.orient.server.OClientConnection;
import com.orientechnologies.orient.server.OClientConnectionManager;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.OServerMain;
import com.orientechnologies.orient.server.config.OServerUserConfiguration;
import com.orientechnologies.orient.server.handler.OServerHandlerHelper;
import com.orientechnologies.orient.server.network.protocol.ONetworkProtocol;
import com.orientechnologies.orient.server.tx.OTransactionOptimisticProxy;

public class ONetworkProtocolBinary extends ONetworkProtocol {
	protected OClientConnection				connection;
	protected OChannelBinaryServer		channel;
	protected OUser										account;

	protected String									user;
	protected String									passwd;
	protected int											lastRequestType;
	protected int											lastClientTxId;
	private OServerUserConfiguration	serverUser;

	public ONetworkProtocolBinary() {
		super(Orient.getThreadGroup(), "IO-Binary");
	}

	public ONetworkProtocolBinary(final String iThreadName) {
		super(Orient.getThreadGroup(), iThreadName);
	}

	@Override
	public void config(final OServer iServer, final Socket iSocket, final OClientConnection iConnection,
			final OContextConfiguration iConfig) throws IOException {
		server = iServer;
		channel = new OChannelBinaryServer(iSocket, iConfig);
		connection = iConnection;

		// SEND PROTOCOL VERSION
		channel.writeShort((short) OChannelBinaryProtocol.CURRENT_PROTOCOL_VERSION);
		channel.flush();

		start();
	}

	@Override
	protected void execute() throws Exception {
		lastRequestType = -1;
		data.commandInfo = "Listening";
		data.commandDetail = "-";

		lastClientTxId = 0;

		try {
			lastRequestType = channel.readByte();
			lastClientTxId = channel.readInt();

			if (lastClientTxId > -1)
				connection = OClientConnectionManager.instance().getConnection(lastClientTxId);
			else {
				if (connection == null) {
					sendShutdown();
					return;
				} else
					connection = OClientConnectionManager.instance().connect(connection.protocol.getChannel().socket, this);
			}

			if (connection != null)
				ODatabaseRecordThreadLocal.INSTANCE.set(connection.database);

			++data.totalRequests;

			data.lastCommandReceived = System.currentTimeMillis();

			OServerHandlerHelper.invokeHandlerCallbackOnBeforeClientRequest(connection, (byte) lastRequestType);

			parseCommand();

			OServerHandlerHelper.invokeHandlerCallbackOnAfterClientRequest(connection, (byte) lastRequestType);

		} catch (EOFException e) {
			handleConnectionError(e);
			sendShutdown();
		} catch (SocketException e) {
			handleConnectionError(e);
			sendShutdown();
		} catch (OException e) {
			sendError(lastClientTxId, e);
		} catch (RuntimeException e) {
			sendError(lastClientTxId, e);
		} catch (Throwable t) {
			OLogManager.instance().error(this, "Error on executing request", t);
			sendError(lastClientTxId, t);
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
	protected void parseCommand() throws IOException, InterruptedException {
		switch (lastRequestType) {

		case OChannelBinaryProtocol.REQUEST_SHUTDOWN: {
			data.commandInfo = "Shutdowning";

			OLogManager.instance().info(this, "Received shutdown command from the remote client %s:%d", channel.socket.getInetAddress(),
					channel.socket.getPort());

			user = channel.readString();
			passwd = channel.readString();

			if (OServerMain.server().authenticate(user, passwd, "shutdown")) {
				OLogManager.instance().info(this, "Remote client %s:%d authenticated. Starting shutdown of server...",
						channel.socket.getInetAddress(), channel.socket.getPort());

				channel.acquireExclusiveLock();
				try {
					sendOk(lastClientTxId);
				} finally {
					channel.releaseExclusiveLock();
				}
				channel.flush();
				channel.close();
				OServerMain.server().shutdown();
				System.exit(0);
				return;
			}

			OLogManager.instance().error(this, "Authentication error of remote client %s:%d: shutdown is aborted.",
					channel.socket.getInetAddress(), channel.socket.getPort());

			sendError(lastClientTxId, new OSecurityAccessException("Invalid user/password to shutdown the server"));
			break;
		}

		case OChannelBinaryProtocol.REQUEST_CONNECT: {
			data.commandInfo = "Connect";

			serverLogin(channel.readString(), channel.readString(), "connect");

			channel.acquireExclusiveLock();
			try {
				sendOk(lastClientTxId);
				channel.writeInt(connection.id);
			} finally {
				channel.releaseExclusiveLock();
			}
			break;
		}

		case OChannelBinaryProtocol.REQUEST_DB_LIST: {
			data.commandInfo = "List databases";

			ODocument doc = listDatabases();

			channel.acquireExclusiveLock();
			try {
				sendOk(lastClientTxId);
				channel.writeBytes(doc.toStream());
			} finally {
				channel.releaseExclusiveLock();
			}
			break;
		}

		case OChannelBinaryProtocol.REQUEST_DB_OPEN: {
			data.commandInfo = "Open database";

			String dbURL = channel.readString();

			user = channel.readString();
			passwd = channel.readString();

			openDatabase(dbURL, user, passwd);

			if (!(connection.database.getStorage() instanceof OStorageEmbedded) && !loadUserFromSchema(user, passwd)) {
				sendError(lastClientTxId, new OSecurityAccessException(connection.database.getName(),
						"User or password not valid for database: '" + connection.database.getName() + "'"));
			} else {

				channel.acquireExclusiveLock();
				try {
					sendOk(lastClientTxId);
					channel.writeInt(connection.id);

					sendDatabaseInformation();

					if (getClass().equals(ONetworkProtocolBinary.class))
						// NO EXTENSIONS (CLUSTER): SEND NULL DOCUMENT
						channel.writeBytes(null);

				} finally {
					channel.releaseExclusiveLock();
				}
			}

			break;
		}
		case OChannelBinaryProtocol.REQUEST_DB_RELOAD: {
			data.commandInfo = "Reload database information";

			checkDatabase();

			channel.acquireExclusiveLock();
			try {
				sendOk(lastClientTxId);

				sendDatabaseInformation();

			} finally {
				channel.releaseExclusiveLock();
			}

			break;
		}
		case OChannelBinaryProtocol.REQUEST_DB_CREATE: {
			data.commandInfo = "Create database";

			String dbName = channel.readString();
			String storageMode = channel.readString();

			checkServerAccess("database.create");
			checkStorageExistence(dbName);
			connection.database = getDatabaseInstance(dbName, storageMode);
			createDatabase(connection.database, null, null);

			channel.acquireExclusiveLock();
			try {
				sendOk(lastClientTxId);
			} finally {
				channel.releaseExclusiveLock();
			}
			break;
		}

		case OChannelBinaryProtocol.REQUEST_DB_CLOSE:
			data.commandInfo = "Close Database";

			if (connection != null) {
				connection.close();

				OClientConnectionManager.instance().disconnect(connection.id);
				// sendShutdown();
			}

			channel.acquireExclusiveLock();
			try {
				sendOk(lastClientTxId);
			} finally {
				channel.releaseExclusiveLock();
			}
			break;

		case OChannelBinaryProtocol.REQUEST_DB_EXIST: {
			data.commandInfo = "Exists database";
			String dbName = channel.readString();

			checkServerAccess("database.exists");

			connection.database = getDatabaseInstance(dbName, "local");

			channel.acquireExclusiveLock();
			try {
				sendOk(lastClientTxId);
				channel.writeByte((byte) (connection.database.exists() ? 1 : 0));
			} finally {
				channel.releaseExclusiveLock();
			}
			break;
		}

		case OChannelBinaryProtocol.REQUEST_DB_DELETE: {
			data.commandInfo = "Delete database";
			String dbName = channel.readString();

			checkServerAccess("database.delete");

			connection.database = getDatabaseInstance(dbName, "local");

			OLogManager.instance().info(this, "Dropped database '%s", connection.database.getURL());

			connection.database.delete();

			channel.acquireExclusiveLock();
			try {
				sendOk(lastClientTxId);
			} finally {
				channel.releaseExclusiveLock();
			}
			break;
		}

		case OChannelBinaryProtocol.REQUEST_DB_SIZE: {
			data.commandInfo = "Database size";

			checkDatabase();

			channel.acquireExclusiveLock();
			try {
				sendOk(lastClientTxId);
				channel.writeLong(connection.database.getStorage().getSize());
			} finally {
				channel.releaseExclusiveLock();
			}
			break;
		}

		case OChannelBinaryProtocol.REQUEST_DB_COUNTRECORDS: {
			data.commandInfo = "Database count records";

			checkDatabase();

			channel.acquireExclusiveLock();
			try {
				sendOk(lastClientTxId);
				channel.writeLong(connection.database.getStorage().countRecords());
			} finally {
				channel.releaseExclusiveLock();
			}
			break;
		}

		case OChannelBinaryProtocol.REQUEST_DATACLUSTER_COUNT: {
			data.commandInfo = "Count cluster elements";

			checkDatabase();

			int[] clusterIds = new int[channel.readShort()];
			for (int i = 0; i < clusterIds.length; ++i)
				clusterIds[i] = channel.readShort();

			final long count = connection.database.countClusterElements(clusterIds);

			channel.acquireExclusiveLock();
			try {
				sendOk(lastClientTxId);
				channel.writeLong(count);
			} finally {
				channel.releaseExclusiveLock();
			}
			break;
		}

		case OChannelBinaryProtocol.REQUEST_DATACLUSTER_DATARANGE: {
			data.commandInfo = "Get the begin/end range of data in cluster";

			checkDatabase();

			long[] pos = connection.database.getStorage().getClusterDataRange(channel.readShort());

			channel.acquireExclusiveLock();
			try {
				sendOk(lastClientTxId);
				channel.writeLong(pos[0]);
				channel.writeLong(pos[1]);
			} finally {
				channel.releaseExclusiveLock();
			}
			break;
		}

		case OChannelBinaryProtocol.REQUEST_DATACLUSTER_ADD: {
			data.commandInfo = "Add cluster";

			checkDatabase();

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

			channel.acquireExclusiveLock();
			try {
				sendOk(lastClientTxId);
				channel.writeShort((short) num);
			} finally {
				channel.releaseExclusiveLock();
			}
			break;
		}

		case OChannelBinaryProtocol.REQUEST_DATACLUSTER_REMOVE: {
			data.commandInfo = "Remove cluster";

			checkDatabase();

			final int id = channel.readShort();

			boolean result = connection.database.dropCluster(connection.database.getClusterNameById(id));

			channel.acquireExclusiveLock();
			try {
				sendOk(lastClientTxId);
				channel.writeByte((byte) (result ? 1 : 0));
			} finally {
				channel.releaseExclusiveLock();
			}
			break;
		}

		case OChannelBinaryProtocol.REQUEST_RECORD_LOAD: {
			data.commandInfo = "Load record";

			final ORecordId rid = channel.readRID();
			final String fetchPlanString = channel.readString();
			if (rid.clusterId == 0 && rid.clusterPosition == 0) {
				// @COMPATIBILITY 0.9.25
				// SEND THE DB CONFIGURATION INSTEAD SINCE IT WAS ON RECORD 0:0
				OFetchHelper.checkFetchPlanValid(fetchPlanString);
				channel.acquireExclusiveLock();
				try {
					sendOk(lastClientTxId);
					channel.writeByte((byte) 1);
					channel.writeBytes(connection.database.getStorage().getConfiguration().toStream());
					channel.writeInt(0);
					channel.writeByte(ORecordBytes.RECORD_TYPE);
				} finally {
					channel.releaseExclusiveLock();
				}
			} else {
				final ORecordInternal<?> record = connection.database.load(rid, fetchPlanString);

				channel.acquireExclusiveLock();
				try {
					sendOk(lastClientTxId);

					if (record != null) {
						channel.writeByte((byte) 1);
						channel.writeBytes(record.toStream());
						channel.writeInt(record.getVersion());
						channel.writeByte(record.getRecordType());

						if (fetchPlanString.length() > 0) {
							// BUILD THE SERVER SIDE RECORD TO ACCES TO THE FETCH
							// PLAN
							if (record instanceof ODocument) {
								final Map<String, Integer> fetchPlan = OFetchHelper.buildFetchPlan(fetchPlanString);

								final Set<ODocument> recordsToSend = new HashSet<ODocument>();
								final ODocument doc = (ODocument) record;
								OFetchHelper.fetch(doc, doc, doc.fieldNames(), fetchPlan, null, 0, -1, new OFetchListener() {
									@Override
									public int size() {
										return recordsToSend.size();
									}

									// ADD TO THE SET OF OBJECTS TO SEND
									@Override
									public Object fetchLinked(final ODocument iRoot, final Object iUserObject, final String iFieldName,
											final Object iLinked) {
										if (iLinked instanceof ODocument) {
											if (((ODocument) iLinked).getIdentity().isValid())
												return recordsToSend.add((ODocument) iLinked) ? iLinked : null;
											return null;
										} else if (iLinked instanceof Collection<?>)
											return recordsToSend.addAll((Collection<? extends ODocument>) iLinked) ? iLinked : null;
										else if (iLinked instanceof Map<?, ?>)
											return recordsToSend.addAll(((Map<String, ? extends ODocument>) iLinked).values()) ? iLinked : null;
										else
											throw new IllegalArgumentException("Unrecognized type while fetching records: " + iLinked);
									}
								});

								// SEND RECORDS TO LOAD IN CLIENT CACHE
								for (ODocument d : recordsToSend) {
									if (d.getIdentity().isValid()) {
										channel.writeByte((byte) 2); // CLIENT CACHE
										// RECORD. IT ISN'T PART OF THE RESULT SET
										writeIdentifiable(d);
									}
								}
							}

						}
					}
				} finally {
					channel.releaseExclusiveLock();
				}
			}
			channel.writeByte((byte) 0); // NO MORE RECORDS
			break;
		}

		case OChannelBinaryProtocol.REQUEST_RECORD_CREATE: {
			data.commandInfo = "Create record";

			checkDatabase();

			final ORecordId rid = new ORecordId(channel.readShort(), ORID.CLUSTER_POS_INVALID);
			final byte[] buffer = channel.readBytes();
			final byte recordType = channel.readByte();

			final long clusterPosition = createRecord(rid, buffer, recordType);

			channel.acquireExclusiveLock();
			try {
				sendOk(lastClientTxId);
				channel.writeLong(clusterPosition);
			} finally {
				channel.releaseExclusiveLock();
			}
			break;
		}

		// VERSION MANAGEMENT:
		// -1 : DOCUMENT UPDATE, NO VERSION CONTROL
		// -2 : DOCUMENT UPDATE, NO VERSION CONTROL, NO VERSION INCREMENT
		// -3 : DOCUMENT ROLLBACK, DECREMENT VERSION
		// >-1 : MVCC CONTROL, RECORD UPDATE AND VERSION INCREMENT
		// <-3 : WRONG VERSION VALUE
		case OChannelBinaryProtocol.REQUEST_RECORD_UPDATE: {
			data.commandInfo = "Update record";

			checkDatabase();

			final ORecordId rid = channel.readRID();
			final byte[] buffer = channel.readBytes();
			final int version = channel.readInt();
			final byte recordType = channel.readByte();

			final int newVersion = updateRecord(rid, buffer, version, recordType);

			channel.acquireExclusiveLock();
			try {
				sendOk(lastClientTxId);
				channel.writeInt(newVersion);
			} finally {
				channel.releaseExclusiveLock();
			}
			break;
		}

		case OChannelBinaryProtocol.REQUEST_RECORD_DELETE: {
			data.commandInfo = "Delete record";

			checkDatabase();

			final ORID rid = channel.readRID();
			final int version = channel.readInt();

			final int result = deleteRecord(rid, version);

			channel.acquireExclusiveLock();
			try {
				sendOk(lastClientTxId);
				channel.writeByte((byte) result); // TODO: REMOV SINCE IT'S NOT MORE NECESSARY
			} finally {
				channel.releaseExclusiveLock();
			}
			break;
		}

		case OChannelBinaryProtocol.REQUEST_COUNT: {
			data.commandInfo = "Count cluster records";

			checkDatabase();

			final String clusterName = channel.readString();
			final long size = connection.database.countClusterElements(clusterName);

			channel.acquireExclusiveLock();
			try {
				sendOk(lastClientTxId);
				channel.writeLong(size);
			} finally {
				channel.releaseExclusiveLock();
			}
			break;
		}

		case OChannelBinaryProtocol.REQUEST_COMMAND: {
			data.commandInfo = "Execute remote command";

			checkDatabase();

			final boolean asynch = channel.readByte() == 'a';

			final OCommandRequestText command = (OCommandRequestText) OStreamSerializerAnyStreamable.INSTANCE.fromStream(channel
					.readBytes());

			final OQuery<?> query = (OQuery<?>) (command instanceof OQuery<?> ? command : null);

			data.commandDetail = command.getText();

			channel.acquireExclusiveLock();
			try {
				if (asynch) {
					// ASYNCHRONOUS
					final StringBuilder empty = new StringBuilder();
					final Set<ODocument> recordsToSend = new HashSet<ODocument>();
					final int txId = lastClientTxId;

					final Map<String, Integer> fetchPlan = query != null ? OFetchHelper.buildFetchPlan(query.getFetchPlan()) : null;
					command.setResultListener(new OCommandResultListener() {
						@Override
						public boolean result(final Object iRecord) {
							if (empty.length() == 0)
								try {
									sendOk(txId);
									empty.append("-");
								} catch (IOException e1) {
								}

							try {
								channel.writeByte((byte) 1); // ONE MORE RECORD
								writeIdentifiable((ORecordInternal<?>) iRecord);

								if (fetchPlan != null && iRecord instanceof ODocument) {
									final ODocument doc = (ODocument) iRecord;
									OFetchHelper.fetch(doc, iRecord, doc.fieldNames(), fetchPlan, null, 0, -1, new OFetchListener() {
										@Override
										public int size() {
											return recordsToSend.size();
										}

										// ADD TO THE SET OF OBJECT TO
										// SEND
										@Override
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
							sendOk(lastClientTxId);
						} catch (IOException e1) {
						}

					// SEND RECORDS TO LOAD IN CLIENT CACHE
					for (ODocument doc : recordsToSend) {
						channel.writeByte((byte) 2); // CLIENT CACHE RECORD. IT
						// ISN'T PART OF THE
						// RESULT SET
						writeIdentifiable(doc);
					}

					channel.writeByte((byte) 0); // NO MORE RECORDS
				} else {
					// SYNCHRONOUS
					final Object result = ((OCommandRequestInternal) connection.database.command(command)).execute();

					sendOk(lastClientTxId);

					if (result == null) {
						// NULL VALUE
						channel.writeByte((byte) 'n');
					} else if (result instanceof OIdentifiable) {
						// RECORD
						channel.writeByte((byte) 'r');
						writeIdentifiable((OIdentifiable) result);
					} else if (result instanceof Collection<?>) {
						channel.writeByte((byte) 'l');
						final Collection<OIdentifiable> list = (Collection<OIdentifiable>) result;
						channel.writeInt(list.size());
						for (OIdentifiable o : list) {
							writeIdentifiable(o);
						}
					} else {
						// ANY OTHER (INCLUDING LITERALS)
						channel.writeByte((byte) 'a');
						final StringBuilder value = new StringBuilder();
						ORecordSerializerStringAbstract.fieldTypeToString(value, OType.getTypeByClass(result.getClass()), result);
						channel.writeString(value.toString());
					}
				}
			} finally {
				channel.releaseExclusiveLock();
			}
			break;
		}

		case OChannelBinaryProtocol.REQUEST_TX_COMMIT: {
			data.commandInfo = "Transaction commit";

			checkDatabase();

			final OTransactionOptimisticProxy tx = new OTransactionOptimisticProxy(
					(ODatabaseRecordTx) connection.database.getUnderlying(), channel);

			connection.database.begin(tx);
			try {
				connection.database.commit();
				channel.acquireExclusiveLock();
				try {
					sendOk(lastClientTxId);

					// SEND BACK ALL THE RECORD IDS FOR THE CREATED RECORDS
					channel.writeInt(tx.getCreatedRecords().size());
					for (Entry<ORecordId, ORecord<?>> entry : tx.getCreatedRecords().entrySet()) {
						channel.writeRID(entry.getKey());
						channel.writeRID(entry.getValue().getIdentity());

						// IF THE NEW OBJECT HAS VERSION > 0 MEANS THAT HAS BEEN UPDATED IN THE SAME TX. THIS HAPPENS FOR GRAPHS
						if (entry.getValue().getVersion() > 0)
							tx.getUpdatedRecords().put((ORecordId) entry.getValue().getIdentity(), entry.getValue());
					}

					// SEND BACK ALL THE NEW VERSIONS FOR THE UPDATED RECORDS
					channel.writeInt(tx.getUpdatedRecords().size());
					for (Entry<ORecordId, ORecord<?>> entry : tx.getUpdatedRecords().entrySet()) {
						channel.writeRID(entry.getKey());
						channel.writeInt(entry.getValue().getVersion());
					}
				} finally {
					channel.releaseExclusiveLock();
				}
			} catch (Exception e) {
				connection.database.rollback();
				sendError(lastClientTxId, e);
			}

			break;
		}

		case OChannelBinaryProtocol.REQUEST_CONFIG_GET: {
			data.commandInfo = "Get config";

			checkServerAccess("server.config.get");

			final String key = channel.readString();
			final OGlobalConfiguration cfg = OGlobalConfiguration.findByKey(key);
			String cfgValue = cfg != null ? cfg.getValueAsString() : "";

			channel.acquireExclusiveLock();
			try {
				sendOk(lastClientTxId);
				channel.writeString(cfgValue);
			} finally {
				channel.releaseExclusiveLock();
			}
			break;
		}

		case OChannelBinaryProtocol.REQUEST_CONFIG_SET: {
			data.commandInfo = "Get config";

			checkServerAccess("server.config.set");

			final String key = channel.readString();
			final String value = channel.readString();
			final OGlobalConfiguration cfg = OGlobalConfiguration.findByKey(key);
			if (cfg != null)
				cfg.setValue(value);

			channel.acquireExclusiveLock();
			try {
				sendOk(lastClientTxId);
			} finally {
				channel.releaseExclusiveLock();
			}
			break;
		}

		case OChannelBinaryProtocol.REQUEST_CONFIG_LIST: {
			data.commandInfo = "List config";

			checkServerAccess("server.config.get");

			channel.acquireExclusiveLock();
			try {
				sendOk(lastClientTxId);

				channel.writeShort((short) OGlobalConfiguration.values().length);
				for (OGlobalConfiguration cfg : OGlobalConfiguration.values()) {
					channel.writeString(cfg.getKey());
					channel.writeString(cfg.getValueAsString() != null ? cfg.getValueAsString() : "");
				}
			} finally {
				channel.releaseExclusiveLock();
			}

			break;
		}

		default:
			data.commandInfo = "Command not supported";
			OLogManager.instance().error(this, "Request not supported. Code: " + lastRequestType);
			channel.clearInput();
			sendError(lastClientTxId, new ONetworkProtocolException("Request not supported. Code: " + lastRequestType));
		}
	}

	protected int deleteRecord(final ORID rid, final int version) {
		final ORecordInternal<?> record = connection.database.load(rid);
		if (record != null) {
			record.setVersion(version);
			record.delete();
			return 1;
		}
		return 0;
	}

	protected long createRecord(final ORecordId rid, final byte[] buffer, final byte recordType) {
		final ORecordInternal<?> record = Orient.instance().getRecordFactoryManager().newInstance(recordType);
		record.fill(rid, 0, buffer, true);
		connection.database.save(record);
		return record.getIdentity().getClusterPosition();
	}

	protected int updateRecord(final ORecordId rid, final byte[] buffer, final int version, final byte recordType) {
		final ORecordInternal<?> newRecord = Orient.instance().getRecordFactoryManager().newInstance(recordType);
		newRecord.fill(rid, version, buffer, true);

		if (((OSchemaProxy) connection.database.getMetadata().getSchema()).getIdentity().equals(rid))
			// || ((OIndexManagerImpl) connection.database.getMetadata().getIndexManager()).getDocument().getIdentity().equals(rid)) {
			throw new OSecurityAccessException("Cannot update internal record " + rid);

		final ORecordInternal<?> currentRecord;
		if (newRecord instanceof ODocument) {
			currentRecord = connection.database.load(rid);

			if (currentRecord == null)
				throw new ORecordNotFoundException(rid.toString());

			((ODocument) currentRecord).merge((ODocument) newRecord, false, false);

		} else
			currentRecord = newRecord;

		currentRecord.setVersion(version);

		connection.database.save(currentRecord);

		if (currentRecord.getIdentity().toString().equals(connection.database.getStorage().getConfiguration().indexMgrRecordId)) {
			// FORCE INDEX MANAGER UPDATE. THIS HAPPENS FOR DIRECT CHANGES FROM REMOTE LIKE IN GRAPH
			connection.database.getMetadata().getIndexManager().reload();
		}
		return currentRecord.getVersion();
	}

	private void sendDatabaseInformation() throws IOException {
		channel.writeInt(connection.database.getClusterNames().size());
		for (OCluster c : (connection.database.getStorage()).getClusterInstances()) {
			if (c != null) {
				channel.writeString(c.getName());
				channel.writeInt(c.getId());
				channel.writeString(c.getType());
			}
		}
	}

	@Override
	public void startup() {
		OServerHandlerHelper.invokeHandlerCallbackOnClientConnection(connection);
	}

	@Override
	public void shutdown() {
		sendShutdown();
		channel.close();

		if (connection == null)
			return;

		OServerHandlerHelper.invokeHandlerCallbackOnClientDisconnection(connection);

		if (connection.database != null)
			connection.database.close();

		OClientConnectionManager.instance().disconnect(connection.id);
	}

	@Override
	public OChannel getChannel() {
		return channel;
	}

	protected void sendOk(final int iClientTxId) throws IOException {
		channel.writeByte(OChannelBinaryProtocol.RESPONSE_STATUS_OK);
		channel.writeInt(iClientTxId);
	}

	protected void sendError(final int iClientTxId, final Throwable t) throws IOException, InterruptedException {
		channel.acquireExclusiveLock();

		try {
			channel.writeByte(OChannelBinaryProtocol.RESPONSE_STATUS_ERROR);
			channel.writeInt(iClientTxId);

			Throwable current;
			if (t instanceof OLockException && t.getCause() instanceof ODatabaseException)
				// BYPASS THE DB POOL EXCEPTION TO PROPAGATE THE RIGHT SECURITY ONE
				current = t.getCause();
			else
				current = t;

			while (current != null) {
				// MORE DETAILS ARE COMING AS EXCEPTION
				channel.writeByte((byte) 1);

				channel.writeString(current.getClass().getName());
				channel.writeString(current != null ? current.getMessage() : null);

				current = current.getCause();
			}
			channel.writeByte((byte) 0);

			channel.clearInput();

		} finally {
			channel.releaseExclusiveLock();
		}
	}

	private void serverLogin(final String iUser, final String iPassword, final String iResource) {
		if (!OServerMain.server().authenticate(iUser, iPassword, iResource))
			throw new OSecurityAccessException(
					"Wrong user/password to [connect] to the remote OrientDB Server instance. Get the user/password from the config/orientdb-server-config.xml file");

		serverUser = OServerMain.server().getUser(iUser);
	}

	private ODocument listDatabases() {
		checkServerAccess("server.dblist");
		final ODocument result = new ODocument();
		result.field("databases", OServerMain.server().getAvailableStorageNames());
		return result;
	}

	protected void checkServerAccess(final String iResource) {
		if (serverUser == null)
			throw new OSecurityAccessException("Server user not authenticated.");

		if (!OServerMain.server().authenticate(serverUser.name, null, iResource))
			throw new OSecurityAccessException("User '" + serverUser.name + "' cannot access to the resource [" + iResource
					+ "]. Use another server user or change permission in the file config/orientdb-server-config.xml");
	}

	private boolean loadUserFromSchema(final String iUserName, final String iUserPassword) {
		account = connection.database.getMetadata().getSecurity().authenticate(iUserName, iUserPassword);
		return true;
	}

	/**
	 * Write a OIdentifiable instance using this format:<br/>
	 * - 2 bytes: class id [-2=no record, -3=rid, -1=no class id, > -1 = valid] <br/>
	 * - 1 byte: record type [v,c,b] <br/>
	 * - 2 bytes: cluster id <br/>
	 * - 8 bytes: position in cluster <br/>
	 * - 4 bytes: record version <br/>
	 * - x bytes: record vontent <br/>
	 * 
	 * @param o
	 * @throws IOException
	 */
	public void writeIdentifiable(final OIdentifiable o) throws IOException {
		if (o == null)
			channel.writeShort(OChannelBinaryProtocol.RECORD_NULL);
		else if (o instanceof ORecordId) {
			channel.writeShort(OChannelBinaryProtocol.RECORD_RID);
			channel.writeRID((ORID) o);
		} else {
			writeRecord((ORecordInternal<?>) o.getRecord());
		}
	}

	private void writeRecord(final ORecordInternal<?> iRecord) throws IOException {
		channel.writeShort((short) 0);
		channel.writeByte(iRecord.getRecordType());
		channel.writeRID(iRecord.getIdentity());
		channel.writeInt(iRecord.getVersion());
		try {
			final byte[] stream = iRecord.toStream();

			// TRIM TAILING SPACES (DUE TO OVERSIZE)
			int realLength = stream.length;
			for (int i = stream.length - 1; i > -1; --i) {
				if (stream[i] == 32)
					--realLength;
				else
					break;
			}

			channel.writeBytes(stream, realLength);
		} catch (Exception e) {
			channel.writeBytes(null);
			OLogManager.instance().error(this, "Error on unmarshalling record " + iRecord.getIdentity().toString(),
					OSerializationException.class);
		}
	}

	protected ODatabaseDocumentTx openDatabase(final String iDbUrl, final String iUser, final String iPassword)
			throws InterruptedException {
		final String path = OServerMain.server().getStoragePath(iDbUrl);

		connection.database = new ODatabaseDocumentTx(path);

		if (connection.database.isClosed())
			if (connection.database.getStorage() instanceof OStorageMemory)
				connection.database.create();
			else {
				try {
					connection.database.open(iUser, iPassword);
				} catch (OSecurityException e) {
					// TRY WITH SERVER'S USER
					try {
						serverLogin(iUser, iPassword, "database.passthrough");
					} catch (OSecurityException ex) {
						throw e;
					}

					// SERVER AUTHENTICATED, BYPASS SECURITY
					connection.database.setProperty(ODatabase.OPTIONS.SECURITY.toString(), Boolean.FALSE);
					connection.database.open(iUser, iPassword);
				}
			}

		connection.rawDatabase = ((ODatabaseRaw) ((ODatabaseComplex<?>) connection.database.getUnderlying()).getUnderlying());

		return connection.database;
	}

	protected void createDatabase(final ODatabaseDocumentTx iDatabase, String dbUser, String dbPasswd) {
		if (iDatabase.exists())
			throw new ODatabaseException("Database '" + iDatabase.getURL() + "' already exists");

		iDatabase.create();
		if (dbUser != null) {

			OUser oUser = iDatabase.getMetadata().getSecurity().getUser(dbUser);
			if (oUser == null) {
				iDatabase.getMetadata().getSecurity().createUser(dbUser, dbPasswd, new String[] { ORole.ADMIN });
			} else {
				oUser.setPassword(dbPasswd);
				oUser.save();
			}
		}
		OLogManager.instance().info(this, "Created database '%s' of type '%s'", iDatabase.getURL(),
				iDatabase.getStorage() instanceof OStorageLocal ? "local" : "memory");

		if (iDatabase.getStorage() instanceof OStorageLocal) {
			// CLOSE IT BECAUSE IT WILL BE OPEN AT FIRST USE
			iDatabase.close();
		}

		connection.rawDatabase = ((ODatabaseRaw) ((ODatabaseComplex<?>) iDatabase.getUnderlying()).getUnderlying());
	}

	protected void checkDatabase() {
		if (connection.database == null)
			throw new OSecurityAccessException("You need to authenticate before to execute the requested operation");
	}

	protected void checkStorageExistence(final String iDatabaseName) {
		for (OStorage stg : Orient.instance().getStorages()) {
			if (stg.getName().equalsIgnoreCase(iDatabaseName) && stg.exists())
				throw new ODatabaseException("Database named '" + iDatabaseName + "' already exists: " + stg);
		}
	}

	protected ODatabaseDocumentTx getDatabaseInstance(final String iDbName, final String iStorageMode) {
		final String path;

		final OStorage stg = Orient.instance().getStorage(iDbName);
		if (stg != null)
			path = stg.getURL();
		else if (iStorageMode.equals(OEngineLocal.NAME)) {
			path = iStorageMode + ":${ORIENTDB_HOME}/databases/" + iDbName;
		} else if (iStorageMode.equals(OEngineMemory.NAME)) {
			path = iStorageMode + ":" + iDbName;
		} else
			throw new IllegalArgumentException("Cannot create database: storage mode '" + iStorageMode + "' is not supported.");

		return new ODatabaseDocumentTx(path);
	}

	private void handleConnectionError(Throwable e) {
		OServerHandlerHelper.invokeHandlerCallbackOnClientError(connection, e);
	}
}
