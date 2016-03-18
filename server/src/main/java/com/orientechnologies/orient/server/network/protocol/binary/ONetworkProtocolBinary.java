/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://www.orientechnologies.com
 *
 */
package com.orientechnologies.orient.server.network.protocol.binary;

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.common.concur.lock.OLockException;
import com.orientechnologies.common.exception.OSystemException;
import com.orientechnologies.common.io.OIOException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.ONullSerializer;
import com.orientechnologies.common.util.OCommonConst;
import com.orientechnologies.orient.client.remote.OCollectionNetworkSerializer;
import com.orientechnologies.orient.core.OConstants;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.cache.OCommandCache;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.command.OCommandResultListener;
import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ridbag.sbtree.OBonsaiCollectionPointer;
import com.orientechnologies.orient.core.db.record.ridbag.sbtree.OSBTreeCollectionManager;
import com.orientechnologies.orient.core.db.record.ridbag.sbtree.OSBTreeRidBag;
import com.orientechnologies.orient.core.exception.*;
import com.orientechnologies.orient.core.fetch.OFetchContext;
import com.orientechnologies.orient.core.fetch.OFetchHelper;
import com.orientechnologies.orient.core.fetch.OFetchListener;
import com.orientechnologies.orient.core.fetch.OFetchPlan;
import com.orientechnologies.orient.core.fetch.remote.ORemoteFetchContext;
import com.orientechnologies.orient.core.fetch.remote.ORemoteFetchListener;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.sbtree.OTreeInternal;
import com.orientechnologies.orient.core.index.sbtreebonsai.local.OSBTreeBonsai;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ORecordBytes;
import com.orientechnologies.orient.core.serialization.OMemoryStream;
import com.orientechnologies.orient.core.serialization.serializer.ONetworkThreadLocalSerializer;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializerFactory;
import com.orientechnologies.orient.core.serialization.serializer.record.string.ORecordSerializerSchemaAware2CSV;
import com.orientechnologies.orient.core.serialization.serializer.record.string.ORecordSerializerStringAbstract;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializerAnyStreamable;
import com.orientechnologies.orient.core.sql.query.OConcurrentResultSet;
import com.orientechnologies.orient.core.sql.query.OResultSet;
import com.orientechnologies.orient.core.sql.query.OSQLAsynchQuery;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.core.storage.*;
import com.orientechnologies.orient.core.type.ODocumentWrapper;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryServer;
import com.orientechnologies.orient.enterprise.channel.binary.OTokenSecurityException;
import com.orientechnologies.orient.server.OClientConnection;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.OServerInfo;
import com.orientechnologies.orient.server.ShutdownHelper;
import com.orientechnologies.orient.server.distributed.*;
import com.orientechnologies.orient.server.network.OServerNetworkListener;
import com.orientechnologies.orient.server.plugin.OServerPlugin;
import com.orientechnologies.orient.server.plugin.OServerPluginHelper;
import com.orientechnologies.orient.server.tx.OTransactionOptimisticProxy;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.SocketException;
import java.util.*;
import java.util.Map.Entry;

public class ONetworkProtocolBinary extends OBinaryNetworkProtocolAbstract {
  protected OClientConnection connection;

  public ONetworkProtocolBinary() {
    super("OrientDB <- BinaryClient/?");
  }

  public ONetworkProtocolBinary(final String iThreadName) {
    super(iThreadName);
  }

  @Override
  public void config(final OServerNetworkListener iListener, final OServer iServer, final Socket iSocket,
      final OContextConfiguration iConfig) throws IOException {
    // CREATE THE CLIENT CONNECTION
    // connection = iServer.getClientConnectionManager().connect(this);

    super.config(iListener, iServer, iSocket, iConfig);

    // SEND PROTOCOL VERSION
    channel.writeShort((short) getVersion());

    channel.flush();
    start();
    setName("OrientDB <- BinaryClient (" + iSocket.getRemoteSocketAddress() + ")");
  }

  @Override
  public void startup() {
    super.startup();
    OServerPluginHelper.invokeHandlerCallbackOnClientConnection(server, connection);
  }

  @Override
  public void shutdown() {
    sendShutdown();
    super.shutdown();

    if (connection == null)
      return;

    OServerPluginHelper.invokeHandlerCallbackOnClientDisconnection(server, connection);

    server.getClientConnectionManager().disconnect(connection);
  }

  @Override
  protected void onBeforeRequest() throws IOException {
    solveSession();

    if (connection != null) {
      connection.statsUpdate();
    } else {
      ODatabaseRecordThreadLocal.INSTANCE.remove();
      if (requestType != OChannelBinaryProtocol.REQUEST_DB_CLOSE && requestType != OChannelBinaryProtocol.REQUEST_SHUTDOWN) {
        OLogManager.instance().debug(this, "Found unknown session %d, shutdown current connection", clientTxId);
        shutdown();
        throw new OIOException("Found unknown session " + clientTxId);
      }
    }

    OServerPluginHelper.invokeHandlerCallbackOnBeforeClientRequest(server, connection, (byte) requestType);
  }

  private void solveSession() throws IOException {
    connection = server.getClientConnectionManager().getConnection(clientTxId, this);
    boolean noToken = false;
    if (connection == null && clientTxId < 0 && requestType != OChannelBinaryProtocol.REQUEST_DB_REOPEN) {
      // OPEN OF OLD STYLE SESSION.
      noToken = true;
    }
    if (requestType == OChannelBinaryProtocol.REQUEST_CONNECT || requestType == OChannelBinaryProtocol.REQUEST_DB_OPEN
        || requestType == OChannelBinaryProtocol.REQUEST_SHUTDOWN) {
      // OPERATIONS THAT DON'T USE TOKEN
      noToken = true;
    }
    if (connection != null && !Boolean.TRUE.equals(connection.getTokenBased())) {
      // CONNECTION WITHOUT TOKEN/OLD MODE
      noToken = true;
    }

    if (noToken) {
      if (clientTxId < 0) {
        connection = server.getClientConnectionManager().connect(this);
        connection.getData().sessionId = clientTxId;
      }
      if (connection != null) {
        // This should not be needed
        connection.setTokenBytes(null);
        connection.acquire();
      }
    } else {
      byte[] bytes = channel.readBytes();
      if (connection == null && bytes != null && bytes.length > 0) {
        // THIS IS THE CASE OF A TOKEN OPERATION WITHOUT HANDSHAKE ON THIS CONNECTION.
        connection = server.getClientConnectionManager().connect(this);
      }

      if (connection == null) {
        throw new OTokenSecurityException("missing session and token");
      }
      if (requestType != OChannelBinaryProtocol.REQUEST_DB_REOPEN) {
        connection.acquire();
        connection.validateSession(bytes, server.getTokenHandler(), this);
      } else {
        connection.validateSession(bytes, server.getTokenHandler(), this);
        server.getClientConnectionManager().disconnect(clientTxId);
        connection = server.getClientConnectionManager().reConnect(this, connection.getTokenBytes(), connection.getToken());
        connection.acquire();
      }

      if (requestType != OChannelBinaryProtocol.REQUEST_DB_CLOSE) {
        connection.init(server);
      }
      if (connection.getData().serverUser) {
        connection.setServerUser(server.getUser(connection.getData().serverUsername));
      }
    }
  }

  @Override
  protected void onAfterRequest() throws IOException {
    OServerPluginHelper.invokeHandlerCallbackOnAfterClientRequest(server, connection, (byte) requestType);

    if (connection != null) {
      connection.endOperation();
    }
    setDataCommandInfo("Listening");
  }

  protected boolean executeRequest() throws IOException {
    try {
      switch (requestType) {

      case OChannelBinaryProtocol.REQUEST_SHUTDOWN:
        shutdownConnection();
        break;

      case OChannelBinaryProtocol.REQUEST_CONNECT:
        connect();
        break;

      case OChannelBinaryProtocol.REQUEST_DB_LIST:
        listDatabases();
        break;

      case OChannelBinaryProtocol.REQUEST_SERVER_INFO:
        serverInfo();
        break;

      case OChannelBinaryProtocol.REQUEST_DB_OPEN:
        openDatabase();
        break;

      case OChannelBinaryProtocol.REQUEST_DB_REOPEN:
        reopenDatabase();
        break;

      case OChannelBinaryProtocol.REQUEST_DB_RELOAD:
        reloadDatabase();
        break;

      case OChannelBinaryProtocol.REQUEST_DB_CREATE:
        createDatabase();
        break;

      case OChannelBinaryProtocol.REQUEST_DB_CLOSE:
        closeDatabase();
        break;

      case OChannelBinaryProtocol.REQUEST_DB_EXIST:
        existsDatabase();
        break;

      case OChannelBinaryProtocol.REQUEST_DB_DROP:
        dropDatabase();
        break;

      case OChannelBinaryProtocol.REQUEST_DB_SIZE:
        sizeDatabase();
        break;

      case OChannelBinaryProtocol.REQUEST_DB_COUNTRECORDS:
        countDatabaseRecords();
        break;

      case OChannelBinaryProtocol.REQUEST_DB_COPY:
        copyDatabase();
        break;

      case OChannelBinaryProtocol.REQUEST_REPLICATION:
        replicationDatabase();
        break;

      case OChannelBinaryProtocol.REQUEST_CLUSTER:
        distributedCluster();
        break;

      case OChannelBinaryProtocol.REQUEST_DATACLUSTER_COUNT:
        countClusters();
        break;

      case OChannelBinaryProtocol.REQUEST_DATACLUSTER_DATARANGE:
        rangeCluster();
        break;

      case OChannelBinaryProtocol.REQUEST_DATACLUSTER_ADD:
        addCluster();
        break;

      case OChannelBinaryProtocol.REQUEST_DATACLUSTER_DROP:
        removeCluster();
        break;

      case OChannelBinaryProtocol.REQUEST_RECORD_METADATA:
        readRecordMetadata();
        break;

      case OChannelBinaryProtocol.REQUEST_RECORD_LOAD:
        readRecord();
        break;

      case OChannelBinaryProtocol.REQUEST_RECORD_LOAD_IF_VERSION_NOT_LATEST:
        readRecordIfVersionIsNotLatest();
        break;

      case OChannelBinaryProtocol.REQUEST_RECORD_CREATE:
        createRecord();
        break;

      case OChannelBinaryProtocol.REQUEST_RECORD_UPDATE:
        updateRecord();
        break;

      case OChannelBinaryProtocol.REQUEST_RECORD_DELETE:
        deleteRecord();
        break;

      case OChannelBinaryProtocol.REQUEST_RECORD_HIDE:
        hideRecord();
        break;

      case OChannelBinaryProtocol.REQUEST_POSITIONS_HIGHER:
        higherPositions();
        break;

      case OChannelBinaryProtocol.REQUEST_POSITIONS_CEILING:
        ceilingPositions();
        break;

      case OChannelBinaryProtocol.REQUEST_POSITIONS_LOWER:
        lowerPositions();
        break;

      case OChannelBinaryProtocol.REQUEST_POSITIONS_FLOOR:
        floorPositions();
        break;

      case OChannelBinaryProtocol.REQUEST_COUNT:
        throw new UnsupportedOperationException("Operation OChannelBinaryProtocol.REQUEST_COUNT has been deprecated");

      case OChannelBinaryProtocol.REQUEST_COMMAND:
        command();
        break;

      case OChannelBinaryProtocol.REQUEST_TX_COMMIT:
        commit();
        break;

      case OChannelBinaryProtocol.REQUEST_CONFIG_GET:
        configGet();
        break;

      case OChannelBinaryProtocol.REQUEST_CONFIG_SET:
        configSet();
        break;

      case OChannelBinaryProtocol.REQUEST_CONFIG_LIST:
        configList();
        break;

      case OChannelBinaryProtocol.REQUEST_DB_FREEZE:
        freezeDatabase();
        break;

      case OChannelBinaryProtocol.REQUEST_DB_RELEASE:
        releaseDatabase();
        break;

      case OChannelBinaryProtocol.REQUEST_RECORD_CLEAN_OUT:
        cleanOutRecord();
        break;

      case OChannelBinaryProtocol.REQUEST_CREATE_SBTREE_BONSAI:
        createSBTreeBonsai();
        break;

      case OChannelBinaryProtocol.REQUEST_SBTREE_BONSAI_GET:
        sbTreeBonsaiGet();
        break;

      case OChannelBinaryProtocol.REQUEST_SBTREE_BONSAI_FIRST_KEY:
        sbTreeBonsaiFirstKey();
        break;

      case OChannelBinaryProtocol.REQUEST_SBTREE_BONSAI_GET_ENTRIES_MAJOR:
        sbTreeBonsaiGetEntriesMajor();
        break;

      case OChannelBinaryProtocol.REQUEST_RIDBAG_GET_SIZE:
        ridBagSize();
        break;

      case OChannelBinaryProtocol.REQUEST_INCREMENTAL_BACKUP:
        incrementalBackup();
        break;

      case OChannelBinaryProtocol.REQUEST_INCREMENTAL_RESTORE:
        incrementalRestore();
        break;

      case OChannelBinaryProtocol.DISTRIBUTED_REQUEST:
        executeDistributedRequest();
        break;

      case OChannelBinaryProtocol.DISTRIBUTED_RESPONSE:
        executeDistributedResponse();
        break;

      default:
        setDataCommandInfo("Command not supported");
        return false;
      }

      return true;
    } catch (RuntimeException e) {
      if (connection != null && connection.getDatabase() != null) {
        final OSBTreeCollectionManager collectionManager = connection.getDatabase().getSbTreeCollectionManager();
        if (collectionManager != null)
          collectionManager.clearChangedIds();
      }

      throw e;
    }
  }

  private void reopenDatabase() throws IOException {
    // TODO:REASSOCIATE CONNECTION TO CLIENT.
    beginResponse();
    try {
      sendOk(clientTxId);
      channel.writeInt(connection.getId());
    } finally {
      endResponse();
    }
  }

  private void indexPut() throws IOException {
    setDataCommandInfo("Put an entry in an index");

    if (!isConnectionAlive())
      return;
    final String indexName = channel.readString();
    Object key = new ODocument().fromStream(channel.readBytes()).field("key");
    final ORecordId value = channel.readRID();
    final OIndex<?> index = connection.getDatabase().getMetadata().getIndexManager().getIndex(indexName);

    if (index == null)
      throw new OSystemException("index with name '" + indexName + "' was not found");

    key = index.getDefinition().createValue(key);

    if (!isConnectionAlive())
      return;

    beginResponse();
    try {
      try {
        index.put(key, value);
        sendOk(clientTxId);
      } catch (ORecordDuplicatedException ex) {
        sendError(clientTxId, ex);
      }
    } finally {
      connection.getData().command = null;
      endResponse();
    }
  }

  private void indexRemove() throws IOException {
    setDataCommandInfo("Remove an entry from index");

    if (!isConnectionAlive())
      return;
    final String indexName = channel.readString();
    Object key = new ODocument().fromStream(channel.readBytes()).field("key");
    final OIndex<?> index = connection.getDatabase().getMetadata().getIndexManager().getIndex(indexName);
    if (index == null)
      throw new OSystemException("index with name '" + indexName + "' not found");

    key = index.getDefinition().createValue(key);

    if (!isConnectionAlive())
      return;

    final boolean res = index.remove(key);
    beginResponse();
    try {
      sendOk(clientTxId);
      channel.writeBoolean(res);
    } finally {
      connection.getData().command = null;
      endResponse();
    }
  }

  private void indexGet() throws IOException {
    setDataCommandInfo("Retrieve an entry from index");

    if (!isConnectionAlive())
      return;

    final String indexName = channel.readString();

    ODocument document = new ODocument();
    fillRecord(new ORecordId(), channel.readBytes(), 0, document, connection.getDatabase());
    Object key = document.field("key");
    final String fetchPlan = channel.readString();
    final OIndex<?> index = connection.getDatabase().getMetadata().getIndexManager().getIndex(indexName);
    if (index == null)
      throw new OSystemException("index with name '" + indexName + "' not found");

    key = index.getDefinition().createValue(key);
    OAbstractCommandResultListener listener = new OSyncCommandResultListener(null);

    if (!isConnectionAlive())
      return;

    listener.setFetchPlan(fetchPlan);

    Object result = index.get(key);
    beginResponse();
    try {
      sendOk(clientTxId);

      serializeValue(listener, result, true);

      if (listener instanceof OSyncCommandResultListener) {
        // SEND FETCHED RECORDS TO LOAD IN CLIENT CACHE
        for (ORecord rec : ((OSyncCommandResultListener) listener).getFetchedRecordsToSend()) {
          channel.writeByte((byte) 2); // CLIENT CACHE RECORD. IT
          // ISN'T PART OF THE
          // RESULT SET
          writeIdentifiable(rec);
        }

        channel.writeByte((byte) 0); // NO MORE RECORDS
      }

    } finally {
      connection.getData().command = null;
      endResponse();
    }

  }

  protected void checkServerAccess(final String iResource) {
    if (connection.getData().protocolVersion <= OChannelBinaryProtocol.PROTOCOL_VERSION_26) {
      if (connection.getServerUser() == null)
        throw new OSecurityAccessException("Server user not authenticated");

      if (!server.isAllowed(connection.getServerUser().name, iResource))
        throw new OSecurityAccessException("User '" + connection.getServerUser().name + "' cannot access to the resource ["
            + iResource + "]. Use another server user or change permission in the file config/orientdb-server-config.xml");
    } else {
      if (!connection.getData().serverUser)
        throw new OSecurityAccessException("Server user not authenticated");

      if (!server.isAllowed(connection.getData().serverUsername, iResource))
        throw new OSecurityAccessException("User '" + connection.getData().serverUsername + "' cannot access to the resource ["
            + iResource + "]. Use another server user or change permission in the file config/orientdb-server-config.xml");
    }
  }

  protected void removeCluster() throws IOException {
    setDataCommandInfo("Remove cluster");

    if (!isConnectionAlive())
      return;

    final int id = channel.readShort();

    final String clusterName = connection.getDatabase().getClusterNameById(id);
    if (clusterName == null)
      throw new IllegalArgumentException(
          "Cluster " + id + " does not exist anymore. Refresh the db structure or just reconnect to the database");

    boolean result = connection.getDatabase().dropCluster(clusterName, false);

    beginResponse();
    try {
      sendOk(clientTxId);
      channel.writeByte((byte) (result ? 1 : 0));
    } finally {
      endResponse();
    }
  }

  protected void addCluster() throws IOException {
    setDataCommandInfo("Add cluster");

    if (!isConnectionAlive())
      return;

    String type = "";
    if (connection.getData().protocolVersion < 24)
      type = channel.readString();

    final String name = channel.readString();
    int clusterId = -1;

    final String location;
    if (connection.getData().protocolVersion < 24 || type.equalsIgnoreCase("PHYSICAL"))
      location = channel.readString();
    else
      location = null;

    if (connection.getData().protocolVersion < 24) {
      final String dataSegmentName;
      dataSegmentName = channel.readString();
    }

    clusterId = channel.readShort();

    final int num;
    if (clusterId < 0)
      num = connection.getDatabase().addCluster(name);
    else
      num = connection.getDatabase().addCluster(name, clusterId, null);

    beginResponse();
    try {
      sendOk(clientTxId);
      channel.writeShort((short) num);
    } finally {
      endResponse();
    }
  }

  protected void rangeCluster() throws IOException {
    setDataCommandInfo("Get the begin/end range of data in cluster");

    if (!isConnectionAlive())
      return;

    final long[] pos = connection.getDatabase().getStorage().getClusterDataRange(channel.readShort());

    beginResponse();
    try {
      sendOk(clientTxId);
      channel.writeLong(pos[0]);
      channel.writeLong(pos[1]);
    } finally {
      endResponse();
    }
  }

  protected void countClusters() throws IOException {
    setDataCommandInfo("Count cluster elements");

    if (!isConnectionAlive())
      return;

    int[] clusterIds = new int[channel.readShort()];
    for (int i = 0; i < clusterIds.length; ++i)
      clusterIds[i] = channel.readShort();

    boolean countTombstones = false;
    countTombstones = channel.readByte() > 0;

    final long count = connection.getDatabase().countClusterElements(clusterIds, countTombstones);

    beginResponse();
    try {
      sendOk(clientTxId);
      channel.writeLong(count);
    } finally {
      endResponse();
    }
  }

  protected void reloadDatabase() throws IOException {
    setDataCommandInfo("Reload database information");

    if (!isConnectionAlive())
      return;

    beginResponse();
    try {
      sendOk(clientTxId);

      sendDatabaseInformation();

    } finally {
      endResponse();
    }
  }

  protected void openDatabase() throws IOException {
    setDataCommandInfo("Open database");

    readConnectionData();

    final String dbURL = channel.readString();

    String dbType = ODatabaseDocument.TYPE;
    if (connection.getData().protocolVersion <= OChannelBinaryProtocol.PROTOCOL_VERSION_32)
      // READ DB-TYPE FROM THE CLIENT. NOT USED ANYMORE
      dbType = channel.readString();

    final String user = channel.readString();
    final String passwd = channel.readString();
    connection.setDatabase((ODatabaseDocumentTx) server.openDatabase(dbURL, user, passwd, connection.getData()));
    byte[] token = null;

    if (Boolean.TRUE.equals(connection.getTokenBased())) {
      token = server.getTokenHandler().getSignedBinaryToken(connection.getDatabase(), connection.getDatabase().getUser(),
          connection.getData());
      // TODO: do not use the parse split getSignedBinaryToken in two methods.
      getServer().getClientConnectionManager().connect(this, connection, token, server.getTokenHandler());
    }

    if (connection.getDatabase().getStorage() instanceof OStorageProxy && !loadUserFromSchema(user, passwd)) {
      sendErrorOrDropConnection(clientTxId, new OSecurityAccessException(connection.getDatabase().getName(),
          "User or password not valid for database: '" + connection.getDatabase().getName() + "'"));
    } else {

      beginResponse();
      try {
        sendOk(clientTxId);
        channel.writeInt(connection.getId());
        if (connection.getData().protocolVersion > OChannelBinaryProtocol.PROTOCOL_VERSION_26) {
          if (Boolean.TRUE.equals(connection.getTokenBased())) {

            channel.writeBytes(token);
          } else
            channel.writeBytes(OCommonConst.EMPTY_BYTE_ARRAY);
        }

        sendDatabaseInformation();

        final OServerPlugin plugin = server.getPlugin("cluster");
        ODocument distributedCfg = null;
        if (plugin != null && plugin instanceof ODistributedServerManager) {
          distributedCfg = ((ODistributedServerManager) plugin).getClusterConfiguration();

          final ODistributedConfiguration dbCfg = ((ODistributedServerManager) plugin)
              .getDatabaseConfiguration(connection.getDatabase().getName());
          if (dbCfg != null) {
            // ENHANCE SERVER CFG WITH DATABASE CFG
            distributedCfg.field("database", dbCfg.serialize(), OType.EMBEDDED);
          }
        }

        channel.writeBytes(distributedCfg != null ? getRecordBytes(distributedCfg) : null);

        if (connection.getData().protocolVersion >= 14)
          channel.writeString(OConstants.getVersion());
      } finally {
        endResponse();
      }
    }
  }

  protected void connect() throws IOException {
    setDataCommandInfo("Connect");

    readConnectionData();

    connection.setServerUser(server.serverLogin(channel.readString(), channel.readString(), "connect"));

    if (connection.getServerUser() == null)
      throw new OSecurityAccessException(
          "Wrong user/password to [connect] to the remote OrientDB Server instance. Get the user/password from the config/orientdb-server-config.xml file");

    beginResponse();
    try {
      sendOk(clientTxId);
      channel.writeInt(connection.getId());
      if (connection.getData().protocolVersion > OChannelBinaryProtocol.PROTOCOL_VERSION_26) {
        connection.getData().serverUsername = connection.getServerUser().name;
        connection.getData().serverUser = true;
        byte[] token;
        if (Boolean.TRUE.equals(connection.getTokenBased())) {
          token = server.getTokenHandler().getSignedBinaryToken(null, null, connection.getData());
        } else
          token = OCommonConst.EMPTY_BYTE_ARRAY;
        channel.writeBytes(token);
      }

    } finally {
      endResponse();
    }
  }

  private void incrementalBackup() throws IOException {
    setDataCommandInfo("Incremental backup");

    if (!isConnectionAlive())
      return;

    final String path = channel.readString();

    connection.getDatabase().incrementalBackup(path);

    beginResponse();
    try {
      sendOk(clientTxId);
    } finally {
      endResponse();
    }
  }

  private void incrementalRestore() throws IOException {
    setDataCommandInfo("Incremental backup");

    if (!isConnectionAlive())
      return;

    final String path = channel.readString();

    connection.getDatabase().incrementalBackup(path);

    beginResponse();
    try {
      sendOk(clientTxId);
    } finally {
      endResponse();
    }
  }

  private void executeDistributedRequest() throws IOException {
    setDataCommandInfo("Distributed request");

    checkServerAccess("server.replication");

    final byte[] serializedReq = channel.readBytes();

    final ODistributedServerManager manager = server.getDistributedManager();
    final ODistributedRequest req = manager.createRequest();

    final ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(serializedReq));
    try {
      req.readExternal(in);
    } catch (ClassNotFoundException e) {
      throw new IOException("Error on unmarshalling of remote task", e);
    } finally {
      in.close();
    }

    ODistributedServerLog.debug(this, manager.getLocalNodeName(), manager.getNodeNameById(req.getSenderNodeId()),
        ODistributedServerLog.DIRECTION.IN, "Received request %s (%d bytes)", req, serializedReq.length);

    final String dbName = req.getDatabaseName();
    if (dbName != null) {
      final ODistributedDatabase ddb = manager.getMessageService().getDatabase(dbName);
      if (ddb == null)
        throw new ODistributedException("Database configuration not found for database '" + req.getDatabaseName() + "'");
      ddb.processRequest(req);
    } else
      manager.executeOnLocalNode(req, null);
  }

  private void executeDistributedResponse() throws IOException {
    setDataCommandInfo("Distributed response");

    checkServerAccess("server.replication");

    final byte[] serializedResponse = channel.readBytes();

    final ODistributedServerManager manager = server.getDistributedManager();
    final ODistributedResponse response = manager.createResponse();

    final ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(serializedResponse));
    try {
      response.readExternal(in);
    } catch (ClassNotFoundException e) {
      throw new IOException("Error on unmarshalling of remote task", e);
    }

    ODistributedServerLog.debug(this, manager.getLocalNodeName(), response.getSenderNodeName(), ODistributedServerLog.DIRECTION.IN,
        "Executing distributed response %s", response);

    manager.getMessageService().dispatchResponseToThread(response);
  }

  protected void sendError(final int iClientTxId, final Throwable t) throws IOException {
    channel.acquireWriteLock();
    try {

      channel.writeByte(OChannelBinaryProtocol.RESPONSE_STATUS_ERROR);
      channel.writeInt(iClientTxId);
      if ((connection != null && connection.getTokenBased() != null)
          && (connection != null && Boolean.TRUE.equals(connection.getTokenBased()))
          && requestType != OChannelBinaryProtocol.REQUEST_CONNECT
          && (requestType != OChannelBinaryProtocol.REQUEST_DB_OPEN && requestType != OChannelBinaryProtocol.REQUEST_SHUTDOWN
              || (connection != null && connection.getData() != null
                  && connection.getData().protocolVersion <= OChannelBinaryProtocol.PROTOCOL_VERSION_32))
          || requestType == OChannelBinaryProtocol.REQUEST_DB_REOPEN) {
        // TODO: Check if the token is expiring and if it is send a new token

        if (connection.getToken() != null) {
          byte[] renewedToken = server.getTokenHandler().renewIfNeeded(connection.getToken());
          channel.writeBytes(renewedToken);
        } else
          channel.writeBytes(new byte[] {});
      }

      final Throwable current;
      if (t instanceof OLockException && t.getCause() instanceof ODatabaseException)
        // BYPASS THE DB POOL EXCEPTION TO PROPAGATE THE RIGHT SECURITY ONE
        current = t.getCause();
      else
        current = t;

      sendErrorDetails(current);

      serializeExceptionObject(current);

      channel.flush();

      if (OLogManager.instance().isLevelEnabled(logClientExceptions)) {
        if (logClientFullStackTrace)
          OLogManager.instance().log(this, logClientExceptions, "Sent run-time exception to the client %s: %s", t,
              channel.socket.getRemoteSocketAddress(), t.toString());
        else
          OLogManager.instance().log(this, logClientExceptions, "Sent run-time exception to the client %s: %s", null,
              channel.socket.getRemoteSocketAddress(), t.toString());
      }
    } catch (Exception e) {
      if (e instanceof SocketException)
        shutdown();
      else
        OLogManager.instance().error(this, "Error during sending an error to client", e);
    } finally {
      if (channel.getLockWrite().isHeldByCurrentThread())
        // NO EXCEPTION SO FAR: UNLOCK IT
        channel.releaseWriteLock();
    }
  }

  protected void shutdownConnection() throws IOException {
    setDataCommandInfo("Shutdowning");

    OLogManager.instance().info(this, "Received shutdown command from the remote client %s:%d", channel.socket.getInetAddress(),
        channel.socket.getPort());

    final String user = channel.readString();
    final String passwd = channel.readString();

    if (server.authenticate(user, passwd, "shutdown")) {
      OLogManager.instance().info(this, "Remote client %s:%d authenticated. Starting shutdown of server...",
          channel.socket.getInetAddress(), channel.socket.getPort());

      beginResponse();
      try {
        sendOk(clientTxId);
      } finally {
        endResponse();
      }
      runShutdownInNonDaemonThread();
      return;
    }

    OLogManager.instance().error(this, "Authentication error of remote client %s:%d: shutdown is aborted.",
        channel.socket.getInetAddress(), channel.socket.getPort());

    sendErrorOrDropConnection(clientTxId, new OSecurityAccessException("Invalid user/password to shutdown the server"));
  }

  protected void copyDatabase() throws IOException {
    setDataCommandInfo("Copy the database to a remote server");

    final String dbUrl = channel.readString();
    final String dbUser = channel.readString();
    final String dbPassword = channel.readString();
    final String remoteServerName = channel.readString();
    final String remoteServerEngine = channel.readString();

    checkServerAccess("database.copy");

    final ODatabaseDocumentTx db = (ODatabaseDocumentTx) server.openDatabase(dbUrl, dbUser, dbPassword);

    beginResponse();
    try {
      sendOk(clientTxId);
    } finally {
      endResponse();
    }
  }

  protected void replicationDatabase() throws IOException {
    setDataCommandInfo("Replication command");

    final ODocument request = new ODocument(channel.readBytes());

    final ODistributedServerManager dManager = server.getDistributedManager();
    if (dManager == null)
      throw new OConfigurationException("No distributed manager configured");

    final String operation = request.field("operation");

    ODocument response = null;

    if (operation.equals("start")) {
      checkServerAccess("server.replication.start");

    } else if (operation.equals("stop")) {
      checkServerAccess("server.replication.stop");

    } else if (operation.equals("config")) {
      checkServerAccess("server.replication.config");

      response = new ODocument()
          .fromJSON(dManager.getDatabaseConfiguration((String) request.field("db")).serialize().toJSON("prettyPrint"));

    }

    sendResponse(response);

  }

  protected void distributedCluster() throws IOException {
    setDataCommandInfo("Cluster status");

    final ODocument req = new ODocument(channel.readBytes());

    ODocument response = null;

    final String operation = req.field("operation");
    if (operation == null)
      throw new IllegalArgumentException("Cluster operation is null");

    if (operation.equals("status")) {
      final OServerPlugin plugin = server.getPlugin("cluster");
      if (plugin != null && plugin instanceof ODistributedServerManager)
        response = ((ODistributedServerManager) plugin).getClusterConfiguration();
    } else
      throw new IllegalArgumentException("Cluster operation '" + operation + "' is not supported");

    sendResponse(response);
  }

  protected void countDatabaseRecords() throws IOException {
    setDataCommandInfo("Database count records");

    if (!isConnectionAlive())
      return;

    beginResponse();
    try {
      sendOk(clientTxId);
      channel.writeLong(connection.getDatabase().getStorage().countRecords());
    } finally {
      endResponse();
    }
  }

  protected void sizeDatabase() throws IOException {
    setDataCommandInfo("Database size");

    if (!isConnectionAlive())
      return;

    beginResponse();
    try {
      sendOk(clientTxId);
      channel.writeLong(connection.getDatabase().getStorage().getSize());
    } finally {
      endResponse();
    }
  }

  protected void dropDatabase() throws IOException {
    setDataCommandInfo("Drop database");
    String dbName = channel.readString();

    String storageType = null;
    storageType = channel.readString();

    if (storageType == null)
      storageType = "plocal";

    checkServerAccess("database.delete");

    connection.setDatabase(getDatabaseInstance(dbName, ODatabaseDocument.TYPE, storageType));

    if (connection.getDatabase().exists()) {
      if (connection.getDatabase().isClosed())
        server.openDatabaseBypassingSecurity(connection.getDatabase(), connection.getData(), connection.getServerUser().name);

      connection.getDatabase().drop();

      OLogManager.instance().info(this, "Dropped database '%s'", connection.getDatabase().getName());

      connection.close();
    } else {
      throw new OStorageException("Database with name '" + dbName + "' does not exist");
    }

    beginResponse();
    try {
      sendOk(clientTxId);
    } finally {
      endResponse();
    }
  }

  protected void existsDatabase() throws IOException {
    setDataCommandInfo("Exists database");
    final String dbName = channel.readString();

    String storageType = null;
    storageType = channel.readString();

    if (storageType == null)
      storageType = "plocal";

    checkServerAccess("database.exists");

    boolean result = false;
    ODatabaseDocumentInternal database;

    database = getDatabaseInstance(dbName, ODatabaseDocument.TYPE, storageType);
    if (database.exists())
      result = true;
    else
      Orient.instance().unregisterStorage(database.getStorage());

    beginResponse();
    try {
      sendOk(clientTxId);
      channel.writeByte((byte) (result ? 1 : 0));
    } finally {
      endResponse();
    }
  }

  protected void createDatabase() throws IOException {
    setDataCommandInfo("Create database");

    String dbName = channel.readString();
    String dbType = ODatabaseDocument.TYPE;
    // READ DB-TYPE FROM THE CLIENT
    dbType = channel.readString();
    String storageType = channel.readString();

    checkServerAccess("database.create");
    checkStorageExistence(dbName);
    connection.setDatabase(getDatabaseInstance(dbName, dbType, storageType));
    createDatabase(connection.getDatabase(), null, null);

    beginResponse();
    try {
      sendOk(clientTxId);
    } finally {
      endResponse();
    }
  }

  protected void closeDatabase() throws IOException {
    setDataCommandInfo("Close Database");

    if (connection != null) {
      if (server.getClientConnectionManager().disconnect(connection.getId()) && Boolean.FALSE.equals(connection.getTokenBased()))
        sendShutdown();
    }
  }

  protected void configList() throws IOException {
    setDataCommandInfo("List config");

    checkServerAccess("server.config.get");

    beginResponse();
    try {
      sendOk(clientTxId);

      channel.writeShort((short) OGlobalConfiguration.values().length);
      for (OGlobalConfiguration cfg : OGlobalConfiguration.values()) {

        String key;
        try {
          key = cfg.getKey();
        } catch (Exception e) {
          key = "?";
        }

        String value;
        if (cfg.isHidden())
          value = "<hidden>";
        else
          try {
            value = cfg.getValueAsString() != null ? cfg.getValueAsString() : "";
          } catch (Exception e) {
            value = "";
          }

        channel.writeString(key);
        channel.writeString(value);
      }
    } finally {
      endResponse();
    }
  }

  protected void configSet() throws IOException {
    setDataCommandInfo("Get config");

    checkServerAccess("server.config.set");

    final String key = channel.readString();
    final String value = channel.readString();
    final OGlobalConfiguration cfg = OGlobalConfiguration.findByKey(key);

    if (cfg != null) {
      cfg.setValue(value);
      if (!cfg.isChangeableAtRuntime())
        throw new OConfigurationException("Property '" + key + "' cannot be changed at runtime. Change the setting at startup");
    } else
      throw new OConfigurationException("Property '" + key + "' was not found in global configuration");

    beginResponse();
    try {
      sendOk(clientTxId);
    } finally {
      endResponse();
    }
  }

  protected void configGet() throws IOException {
    setDataCommandInfo("Get config");

    checkServerAccess("server.config.get");

    final String key = channel.readString();
    final OGlobalConfiguration cfg = OGlobalConfiguration.findByKey(key);
    String cfgValue = cfg != null ? cfg.isHidden() ? "<hidden>" : cfg.getValueAsString() : "";

    beginResponse();
    try {
      sendOk(clientTxId);
      channel.writeString(cfgValue);
    } finally {
      endResponse();
    }
  }

  protected void commit() throws IOException {
    setDataCommandInfo("Transaction commit");

    if (!isConnectionAlive())
      return;

    final OTransactionOptimisticProxy tx = new OTransactionOptimisticProxy(connection.getDatabase(), channel,
        connection.getData().protocolVersion, this);

    try {
      connection.getDatabase().begin(tx);

      try {
        connection.getDatabase().commit();
        beginResponse();
        try {
          sendOk(clientTxId);

          // SEND BACK ALL THE RECORD IDS FOR THE CREATED RECORDS
          channel.writeInt(tx.getCreatedRecords().size());
          for (Entry<ORecordId, ORecord> entry : tx.getCreatedRecords().entrySet()) {
            channel.writeRID(entry.getKey());
            channel.writeRID(entry.getValue().getIdentity());

            // IF THE NEW OBJECT HAS VERSION > 0 MEANS THAT HAS BEEN UPDATED IN THE SAME TX. THIS HAPPENS FOR GRAPHS
            if (entry.getValue().getVersion() > 0)
              tx.getUpdatedRecords().put((ORecordId) entry.getValue().getIdentity(), entry.getValue());
          }

          // SEND BACK ALL THE NEW VERSIONS FOR THE UPDATED RECORDS
          channel.writeInt(tx.getUpdatedRecords().size());
          for (Entry<ORecordId, ORecord> entry : tx.getUpdatedRecords().entrySet()) {
            channel.writeRID(entry.getKey());
            channel.writeVersion(entry.getValue().getVersion());
          }

          if (connection.getData().protocolVersion >= 20)
            sendCollectionChanges();
        } finally {
          endResponse();
        }
      } catch (Exception e) {
        if (connection != null && connection.getDatabase() != null) {
          if (connection.getDatabase().getTransaction().isActive())
            connection.getDatabase().rollback(true);

          final OSBTreeCollectionManager collectionManager = connection.getDatabase().getSbTreeCollectionManager();
          if (collectionManager != null)
            collectionManager.clearChangedIds();
        }

        sendErrorOrDropConnection(clientTxId, e);
      }
    } catch (OTransactionAbortedException e) {
      // TX ABORTED BY THE CLIENT
    } catch (Exception e) {
      // Error during TX initialization, possibly index constraints violation.
      if (tx.isActive())
        tx.rollback(true, -1);

      sendErrorOrDropConnection(clientTxId, e);
    }
  }

  protected void command() throws IOException {
    setDataCommandInfo("Execute remote command");

    byte type = channel.readByte();
    final boolean live = type == 'l';
    final boolean asynch = type == 'a';
    String dbSerializerName = connection.getDatabase().getSerializer().toString();
    String name = getRecordSerializerName();

    if (!dbSerializerName.equals(name)) {
      ORecordSerializer ser = ORecordSerializerFactory.instance().getFormat(name);
      ONetworkThreadLocalSerializer.setNetworkSerializer(ser);
    }
    OCommandRequestText command = (OCommandRequestText) OStreamSerializerAnyStreamable.INSTANCE.fromStream(channel.readBytes());
    ONetworkThreadLocalSerializer.setNetworkSerializer(null);

    final Map<Object, Object> params = command.getParameters();

    if (asynch && command instanceof OSQLSynchQuery) {
      // CONVERT IT IN ASYNCHRONOUS QUERY
      final OSQLAsynchQuery asynchQuery = new OSQLAsynchQuery(command.getText());
      asynchQuery.setFetchPlan(command.getFetchPlan());
      asynchQuery.setLimit(command.getLimit());
      asynchQuery.setTimeout(command.getTimeoutTime(), command.getTimeoutStrategy());
      asynchQuery.setUseCache(((OSQLSynchQuery) command).isUseCache());
      command = asynchQuery;
    }

    connection.getData().commandDetail = command.getText();

    beginResponse();
    try {
      connection.getData().command = command;
      OAbstractCommandResultListener listener = null;
      OLiveCommandResultListener liveListener = null;

      OCommandResultListener cmdResultListener = command.getResultListener();

      if (live) {
        liveListener = new OLiveCommandResultListener(this.connection, clientTxId, cmdResultListener);
        listener = new OSyncCommandResultListener(null);
        command.setResultListener(liveListener);
      } else if (asynch) {
        // IF COMMAND CACHE IS ENABLED, RESULT MUST BE COLLECTED
        final OCommandCache cmdCache = connection.getDatabase().getMetadata().getCommandCache();
        if (cmdCache.isEnabled())
          // CREATE E COLLECTOR OF RESULT IN RAM TO CACHE THE RESULT
          cmdResultListener = new OAbstractCommandResultListener(cmdResultListener) {
            private OResultSet collector = new OConcurrentResultSet<ORecord>();

            @Override
            public boolean isEmpty() {
              return collector != null && collector.isEmpty();
            }

            @Override
            public boolean result(final Object iRecord) {
              if (collector != null) {
                if (collector.currentSize() > cmdCache.getMaxResultsetSize()) {
                  // TOO MANY RESULTS: STOP COLLECTING IT BECAUSE THEY WOULD NEVER CACHED
                  collector = null;
                } else if (iRecord != null && iRecord instanceof ORecord)
                  collector.add(iRecord);
              }
              return true;
            }

            @Override
            public Object getResult() {
              return collector;
            }

            @Override
            public void end() {
              collector.setCompleted();
            }
          };

        listener = new OAsyncCommandResultListener(this, clientTxId, cmdResultListener);
        command.setResultListener(listener);
      } else {
        listener = new OSyncCommandResultListener(null);
      }

      final long serverTimeout = OGlobalConfiguration.COMMAND_TIMEOUT.getValueAsLong();

      if (serverTimeout > 0 && command.getTimeoutTime() > serverTimeout)
        // FORCE THE SERVER'S TIMEOUT
        command.setTimeout(serverTimeout, command.getTimeoutStrategy());

      if (!isConnectionAlive())
        return;

      // REQUEST CAN'T MODIFY THE RESULT, SO IT'S CACHEABLE
      command.setCacheableResult(true);

      // ASSIGNED THE PARSED FETCHPLAN
      listener.setFetchPlan(connection.getDatabase().command(command).getFetchPlan());

      final Object result;
      if (params == null)
        result = connection.getDatabase().command(command).execute();
      else
        result = connection.getDatabase().command(command).execute(params);

      // FETCHPLAN HAS TO BE ASSIGNED AGAIN, because it can be changed by SQL statement
      listener.setFetchPlan(command.getFetchPlan());

      if (asynch) {
        // ASYNCHRONOUS
        if (listener.isEmpty())
          try {
            sendOk(clientTxId);
          } catch (IOException ignored) {
          }
        channel.writeByte((byte) 0); // NO MORE RECORDS

      } else {
        // SYNCHRONOUS
        sendOk(clientTxId);

        serializeValue(listener, result, false);

        if (listener instanceof OSyncCommandResultListener) {
          // SEND FETCHED RECORDS TO LOAD IN CLIENT CACHE
          for (ORecord rec : ((OSyncCommandResultListener) listener).getFetchedRecordsToSend()) {
            channel.writeByte((byte) 2); // CLIENT CACHE RECORD. IT
            // ISN'T PART OF THE
            // RESULT SET
            writeIdentifiable(rec);
          }

          channel.writeByte((byte) 0); // NO MORE RECORDS
        }
      }

    } finally {
      connection.getData().command = null;
      endResponse();
    }
  }

  public void serializeValue(final OAbstractCommandResultListener listener, Object result, boolean load) throws IOException {
    if (result == null) {
      // NULL VALUE
      channel.writeByte((byte) 'n');
    } else if (result instanceof OIdentifiable) {
      // RECORD
      channel.writeByte((byte) 'r');
      if (load && result instanceof ORecordId)
        result = ((ORecordId) result).getRecord();

      if (listener != null)
        listener.result(result);
      writeIdentifiable((OIdentifiable) result);
    } else if (result instanceof ODocumentWrapper) {
      // RECORD
      channel.writeByte((byte) 'r');
      final ODocument doc = ((ODocumentWrapper) result).getDocument();
      if (listener != null)
        listener.result(doc);
      writeIdentifiable(doc);
    } else if (OMultiValue.isMultiValue(result)) {
      final byte collectionType = result instanceof Set ? (byte) 's' : (byte) 'l';
      channel.writeByte(collectionType);
      channel.writeInt(OMultiValue.getSize(result));
      for (Object o : OMultiValue.getMultiValueIterable(result, false)) {
        try {
          if (load && o instanceof ORecordId)
            o = ((ORecordId) o).getRecord();
          if (listener != null)
            listener.result(o);

          writeIdentifiable((OIdentifiable) o);
        } catch (Exception e) {
          OLogManager.instance().warn(this, "Cannot serialize record: " + o);
          // WRITE NULL RECORD TO AVOID BREAKING PROTOCOL
          writeIdentifiable(null);
        }
      }
    } else if (OMultiValue.isIterable(result)) {
      if (connection.getData().protocolVersion >= OChannelBinaryProtocol.PROTOCOL_VERSION_32) {
        channel.writeByte((byte) 'i');
        for (Object o : OMultiValue.getMultiValueIterable(result)) {
          try {
            if (load && o instanceof ORecordId)
              o = ((ORecordId) o).getRecord();
            if (listener != null)
              listener.result(o);

            channel.writeByte((byte) 1); // ONE MORE RECORD
            writeIdentifiable((OIdentifiable) o);
          } catch (Exception e) {
            OLogManager.instance().warn(this, "Cannot serialize record: " + o);
          }
        }
        channel.writeByte((byte) 0); // NO MORE RECORD
      } else {
        // OLD RELEASES: TRANSFORM IN A COLLECTION
        final byte collectionType = result instanceof Set ? (byte) 's' : (byte) 'l';
        channel.writeByte(collectionType);
        channel.writeInt(OMultiValue.getSize(result));
        for (Object o : OMultiValue.getMultiValueIterable(result)) {
          try {
            if (load && o instanceof ORecordId)
              o = ((ORecordId) o).getRecord();
            if (listener != null)
              listener.result(o);

            writeIdentifiable((OIdentifiable) o);
          } catch (Exception e) {
            OLogManager.instance().warn(this, "Cannot serialize record: " + o);
          }
        }
      }

    } else {
      // ANY OTHER (INCLUDING LITERALS)
      channel.writeByte((byte) 'a');
      final StringBuilder value = new StringBuilder(64);
      if (listener != null)
        listener.result(result);
      ORecordSerializerStringAbstract.fieldTypeToString(value, OType.getTypeByClass(result.getClass()), result);
      channel.writeString(value.toString());
    }
  }

  protected void deleteRecord() throws IOException {
    setDataCommandInfo("Delete record");

    if (!isConnectionAlive())
      return;

    final ORID rid = channel.readRID();
    final int version = channel.readVersion();
    final byte mode = channel.readByte();

    final int result = deleteRecord(connection.getDatabase(), rid, version);

    if (mode < 2) {
      beginResponse();
      try {
        sendOk(clientTxId);
        channel.writeByte((byte) result);
      } finally {
        endResponse();
      }
    }
  }

  protected void hideRecord() throws IOException {
    setDataCommandInfo("Hide record");

    if (!isConnectionAlive())
      return;

    final ORID rid = channel.readRID();
    final byte mode = channel.readByte();

    final int result = hideRecord(connection.getDatabase(), rid);

    if (mode < 2) {
      beginResponse();
      try {
        sendOk(clientTxId);
        channel.writeByte((byte) result);
      } finally {
        endResponse();
      }
    }
  }

  protected void cleanOutRecord() throws IOException {
    setDataCommandInfo("Clean out record");

    if (!isConnectionAlive())
      return;

    final ORID rid = channel.readRID();
    final int version = channel.readVersion();
    final byte mode = channel.readByte();

    final int result = cleanOutRecord(connection.getDatabase(), rid, version);

    if (mode < 2) {
      beginResponse();
      try {
        sendOk(clientTxId);
        channel.writeByte((byte) result);
      } finally {
        endResponse();
      }
    }
  }

  /**
   * VERSION MANAGEMENT:<br>
   * -1 : DOCUMENT UPDATE, NO VERSION CONTROL<br>
   * -2 : DOCUMENT UPDATE, NO VERSION CONTROL, NO VERSION INCREMENT<br>
   * -3 : DOCUMENT ROLLBACK, DECREMENT VERSION<br>
   * >-1 : MVCC CONTROL, RECORD UPDATE AND VERSION INCREMENT<br>
   * <-3 : WRONG VERSION VALUE
   *
   * @throws IOException
   */
  protected void updateRecord() throws IOException {
    setDataCommandInfo("Update record");

    if (!isConnectionAlive())
      return;

    final ORecordId rid = channel.readRID();
    boolean updateContent = true;
    if (connection.getData().protocolVersion >= 23)
      updateContent = channel.readBoolean();
    final byte[] buffer = channel.readBytes();
    final int version = channel.readVersion();
    final byte recordType = channel.readByte();
    final byte mode = channel.readByte();

    final int newVersion = updateRecord(connection.getDatabase(), rid, buffer, version, recordType, updateContent);

    if (mode < 2) {
      beginResponse();
      try {
        sendOk(clientTxId);
        channel.writeVersion(newVersion);

        if (connection.getData().protocolVersion >= 20)
          sendCollectionChanges();
      } finally {
        endResponse();
      }
    }
  }

  protected void createRecord() throws IOException {
    setDataCommandInfo("Create record");

    if (!isConnectionAlive())
      return;

    final int dataSegmentId = connection.getData().protocolVersion < 24 ? channel.readInt() : 0;

    final ORecordId rid = new ORecordId(channel.readShort(), ORID.CLUSTER_POS_INVALID);
    final byte[] buffer = channel.readBytes();
    final byte recordType = channel.readByte();
    final byte mode = channel.readByte();

    final ORecord record = createRecord(connection.getDatabase(), rid, buffer, recordType);

    if (mode < 2) {
      beginResponse();
      try {
        sendOk(clientTxId);
        if (connection.getData().protocolVersion > OChannelBinaryProtocol.PROTOCOL_VERSION_25)
          channel.writeShort((short) record.getIdentity().getClusterId());
        channel.writeLong(record.getIdentity().getClusterPosition());
        channel.writeVersion(record.getVersion());

        if (connection.getData().protocolVersion >= 20)
          sendCollectionChanges();
      } finally {
        endResponse();
      }
    }
  }

  protected void readRecordMetadata() throws IOException {
    setDataCommandInfo("Record metadata");

    final ORID rid = channel.readRID();

    beginResponse();
    try {
      final ORecordMetadata metadata = connection.getDatabase().getRecordMetadata(rid);
      if (metadata != null) {
        sendOk(clientTxId);
        channel.writeRID(metadata.getRecordId());
        channel.writeVersion(metadata.getVersion());
      } else {
        throw new ODatabaseException(String.format("Record metadata for RID: %s, Not found", rid));
      }
    } finally {
      endResponse();
    }
  }

  protected void readRecord() throws IOException {
    setDataCommandInfo("Load record");

    if (!isConnectionAlive())
      return;

    final ORecordId rid = channel.readRID();
    final String fetchPlanString = channel.readString();
    boolean ignoreCache = false;
    ignoreCache = channel.readByte() == 1;

    boolean loadTombstones = false;
    loadTombstones = channel.readByte() > 0;

    if (rid.clusterId == 0 && rid.clusterPosition == 0) {
      // @COMPATIBILITY 0.9.25
      // SEND THE DB CONFIGURATION INSTEAD SINCE IT WAS ON RECORD 0:0
      OFetchHelper.checkFetchPlanValid(fetchPlanString);

      beginResponse();
      try {
        sendOk(clientTxId);
        channel.writeByte((byte) 1);
        if (connection.getData().protocolVersion <= OChannelBinaryProtocol.PROTOCOL_VERSION_27) {
          channel
              .writeBytes(connection.getDatabase().getStorage().getConfiguration().toStream(connection.getData().protocolVersion));
          channel.writeVersion(0);
          channel.writeByte(ORecordBytes.RECORD_TYPE);
        } else {
          channel.writeByte(ORecordBytes.RECORD_TYPE);
          channel.writeVersion(0);
          channel
              .writeBytes(connection.getDatabase().getStorage().getConfiguration().toStream(connection.getData().protocolVersion));
        }
        channel.writeByte((byte) 0); // NO MORE RECORDS
      } finally {
        endResponse();
      }

    } else {
      final ORecord record = connection.getDatabase().load(rid, fetchPlanString, ignoreCache, loadTombstones,
          OStorage.LOCKING_STRATEGY.NONE);

      beginResponse();
      try {
        sendOk(clientTxId);

        if (record != null) {
          channel.writeByte((byte) 1); // HAS RECORD
          byte[] bytes = getRecordBytes(record);
          int length = trimCsvSerializedContent(bytes);
          if (connection.getData().protocolVersion <= OChannelBinaryProtocol.PROTOCOL_VERSION_27) {
            channel.writeBytes(bytes, length);
            channel.writeVersion(record.getVersion());
            channel.writeByte(ORecordInternal.getRecordType(record));
          } else {
            channel.writeByte(ORecordInternal.getRecordType(record));
            channel.writeVersion(record.getVersion());
            channel.writeBytes(bytes, length);
          }

          if (fetchPlanString.length() > 0) {
            // BUILD THE SERVER SIDE RECORD TO ACCES TO THE FETCH
            // PLAN
            if (record instanceof ODocument) {
              final OFetchPlan fetchPlan = OFetchHelper.buildFetchPlan(fetchPlanString);

              final Set<ORecord> recordsToSend = new HashSet<ORecord>();
              final ODocument doc = (ODocument) record;
              final OFetchListener listener = new ORemoteFetchListener() {
                @Override
                protected void sendRecord(ORecord iLinked) {
                  recordsToSend.add(iLinked);
                }
              };
              final OFetchContext context = new ORemoteFetchContext();
              OFetchHelper.fetch(doc, doc, fetchPlan, listener, context, "");

              // SEND RECORDS TO LOAD IN CLIENT CACHE
              for (ORecord d : recordsToSend) {
                if (d.getIdentity().isValid()) {
                  channel.writeByte((byte) 2); // CLIENT CACHE
                  // RECORD. IT ISN'T PART OF THE RESULT SET
                  writeIdentifiable(d);
                }
              }
            }

          }
        }
        channel.writeByte((byte) 0); // NO MORE RECORDS

      } finally {
        endResponse();
      }
    }
  }

  protected void readRecordIfVersionIsNotLatest() throws IOException {
    setDataCommandInfo("Load record if version is not latest");

    if (!isConnectionAlive())
      return;

    final ORecordId rid = channel.readRID();
    final int recordVersion = channel.readVersion();
    final String fetchPlanString = channel.readString();

    boolean ignoreCache = channel.readByte() == 1;

    if (rid.clusterId == 0 && rid.clusterPosition == 0) {
      // @COMPATIBILITY 0.9.25
      // SEND THE DB CONFIGURATION INSTEAD SINCE IT WAS ON RECORD 0:0
      OFetchHelper.checkFetchPlanValid(fetchPlanString);

      beginResponse();
      try {
        sendOk(clientTxId);
        channel.writeByte((byte) 1);
        if (connection.getData().protocolVersion <= OChannelBinaryProtocol.PROTOCOL_VERSION_27) {
          channel
              .writeBytes(connection.getDatabase().getStorage().getConfiguration().toStream(connection.getData().protocolVersion));
          channel.writeVersion(0);
          channel.writeByte(ORecordBytes.RECORD_TYPE);
        } else {
          channel.writeByte(ORecordBytes.RECORD_TYPE);
          channel.writeVersion(0);
          channel
              .writeBytes(connection.getDatabase().getStorage().getConfiguration().toStream(connection.getData().protocolVersion));
        }
        channel.writeByte((byte) 0); // NO MORE RECORDS
      } finally {
        endResponse();
      }

    } else {
      final ORecord record = connection.getDatabase().loadIfVersionIsNotLatest(rid, recordVersion, fetchPlanString, ignoreCache);

      beginResponse();
      try {
        sendOk(clientTxId);

        if (record != null) {
          channel.writeByte((byte) 1); // HAS RECORD
          byte[] bytes = getRecordBytes(record);
          int length = trimCsvSerializedContent(bytes);

          channel.writeByte(ORecordInternal.getRecordType(record));
          channel.writeVersion(record.getVersion());
          channel.writeBytes(bytes, length);

          if (fetchPlanString.length() > 0) {
            // BUILD THE SERVER SIDE RECORD TO ACCES TO THE FETCH
            // PLAN
            if (record instanceof ODocument) {
              final OFetchPlan fetchPlan = OFetchHelper.buildFetchPlan(fetchPlanString);

              final Set<ORecord> recordsToSend = new HashSet<ORecord>();
              final ODocument doc = (ODocument) record;
              final OFetchListener listener = new ORemoteFetchListener() {
                @Override
                protected void sendRecord(ORecord iLinked) {
                  recordsToSend.add(iLinked);
                }
              };
              final OFetchContext context = new ORemoteFetchContext();
              OFetchHelper.fetch(doc, doc, fetchPlan, listener, context, "");

              // SEND RECORDS TO LOAD IN CLIENT CACHE
              for (ORecord d : recordsToSend) {
                if (d.getIdentity().isValid()) {
                  channel.writeByte((byte) 2); // CLIENT CACHE
                  // RECORD. IT ISN'T PART OF THE RESULT SET
                  writeIdentifiable(d);
                }
              }
            }

          }
        }
        channel.writeByte((byte) 0); // NO MORE RECORDS

      } finally {
        endResponse();
      }
    }
  }

  protected void beginResponse() {
    channel.acquireWriteLock();
  }

  protected void endResponse() throws IOException {
    // resetting transaction state. Commands are stateless and connection should be cleared
    // otherwise reused connection (connections pool) may lead to unpredicted errors
    if (connection != null && connection.getDatabase() != null
        && connection.getDatabase().activateOnCurrentThread().getTransaction() != null) {
      connection.getDatabase().activateOnCurrentThread();
      connection.getDatabase().getTransaction().rollback();
    }
    channel.flush();
    channel.releaseWriteLock();
  }

  protected void setDataCommandInfo(final String iCommandInfo) {
    if (connection != null)
      connection.getData().commandInfo = iCommandInfo;
  }

  protected void readConnectionData() throws IOException {
    connection.getData().driverName = channel.readString();
    connection.getData().driverVersion = channel.readString();
    connection.getData().protocolVersion = channel.readShort();
    connection.getData().clientId = channel.readString();
    if (connection.getData().protocolVersion > OChannelBinaryProtocol.PROTOCOL_VERSION_21)
      connection.getData().serializationImpl = channel.readString();
    else
      connection.getData().serializationImpl = ORecordSerializerSchemaAware2CSV.NAME;

    if (connection.getTokenBased() == null) {
      if (connection.getData().protocolVersion > OChannelBinaryProtocol.PROTOCOL_VERSION_26)
        connection.setTokenBased(channel.readBoolean());
      else
        connection.setTokenBased(false);
    } else {
      if (connection.getData().protocolVersion > OChannelBinaryProtocol.PROTOCOL_VERSION_26)
        if (channel.readBoolean() != connection.getTokenBased()) {
          // throw new OException("Not supported mixed connection managment");
        }
    }

    if (connection.getData().protocolVersion > OChannelBinaryProtocol.PROTOCOL_VERSION_33) {
      connection.getData().supportsPushMessages = channel.readBoolean();
      connection.getData().collectStats = channel.readBoolean();
    } else {
      connection.getData().supportsPushMessages = true;
      connection.getData().collectStats = true;
    }
  }

  protected void sendOk(final int iClientTxId) throws IOException {
    channel.writeByte(OChannelBinaryProtocol.RESPONSE_STATUS_OK);
    channel.writeInt(iClientTxId);
    if (connection != null && Boolean.TRUE.equals(connection.getTokenBased()) && connection.getToken() != null
        && requestType != OChannelBinaryProtocol.REQUEST_CONNECT && requestType != OChannelBinaryProtocol.REQUEST_DB_OPEN) {
      // TODO: Check if the token is expiring and if it is send a new token
      byte[] renewedToken = server.getTokenHandler().renewIfNeeded(connection.getToken());
      channel.writeBytes(renewedToken);
    }
  }

  @Override
  protected void handleConnectionError(final OChannelBinaryServer iChannel, final Throwable e) {
    super.handleConnectionError(channel, e);
    OServerPluginHelper.invokeHandlerCallbackOnClientError(server, connection, e);
  }

  protected void sendResponse(final ODocument iResponse) throws IOException {
    beginResponse();
    try {
      sendOk(clientTxId);
      channel.writeBytes(iResponse != null ? iResponse.toStream() : null);
    } finally {
      endResponse();
    }
  }

  protected void freezeDatabase() throws IOException {
    setDataCommandInfo("Freeze database");
    String dbName = channel.readString();

    checkServerAccess("database.freeze");

    String storageType = null;
    storageType = channel.readString();

    if (storageType == null)
      storageType = "plocal";

    connection.setDatabase(getDatabaseInstance(dbName, ODatabaseDocument.TYPE, storageType));

    if (connection.getDatabase().exists()) {
      OLogManager.instance().info(this, "Freezing database '%s'", connection.getDatabase().getURL());

      if (connection.getDatabase().isClosed())
        server.openDatabaseBypassingSecurity(connection.getDatabase(), connection.getData(), connection.getServerUser().name);

      connection.getDatabase().freeze(true);
    } else {
      throw new OStorageException("Database with name '" + dbName + "' does not exist");
    }

    beginResponse();
    try {
      sendOk(clientTxId);
    } finally {
      endResponse();
    }
  }

  protected void releaseDatabase() throws IOException {
    setDataCommandInfo("Release database");
    String dbName = channel.readString();

    checkServerAccess("database.release");

    String storageType = null;
    storageType = channel.readString();

    if (storageType == null)
      storageType = "plocal";

    connection.setDatabase(getDatabaseInstance(dbName, ODatabaseDocument.TYPE, storageType));

    if (connection.getDatabase().exists()) {
      OLogManager.instance().info(this, "Realising database '%s'", connection.getDatabase().getURL());

      if (connection.getDatabase().isClosed())
        server.openDatabaseBypassingSecurity(connection.getDatabase(), connection.getData(), connection.getServerUser().name);

      connection.getDatabase().release();
    } else {
      throw new OStorageException("Database with name '" + dbName + "' does not exist");
    }

    beginResponse();
    try {
      sendOk(clientTxId);
    } finally {
      endResponse();
    }
  }

  @Override
  public String getRecordSerializerName() {
    return connection.getData().serializationImpl;
  }

  private void sendErrorDetails(Throwable current) throws IOException {
    while (current != null) {
      // MORE DETAILS ARE COMING AS EXCEPTION
      channel.writeByte((byte) 1);

      channel.writeString(current.getClass().getName());
      channel.writeString(current.getMessage());

      current = current.getCause();
    }
    channel.writeByte((byte) 0);
  }

  private void serializeExceptionObject(Throwable original) throws IOException {
    try {
      final ODistributedServerManager srvMgr = server.getDistributedManager();
      if (srvMgr != null)
        original = srvMgr.convertException(original);

      final OMemoryStream memoryStream = new OMemoryStream();
      final ObjectOutputStream objectOutputStream = new ObjectOutputStream(memoryStream);

      objectOutputStream.writeObject(original);
      objectOutputStream.flush();

      final byte[] result = memoryStream.toByteArray();
      objectOutputStream.close();

      channel.writeBytes(result);
    } catch (Exception e) {
      OLogManager.instance().warn(this, "Cannot serialize an exception object", e);

      // Write empty stream for binary compatibility
      channel.writeBytes(OCommonConst.EMPTY_BYTE_ARRAY);
    }
  }

  /**
   * Due to protocol thread is daemon, shutdown should be executed in separate thread to guarantee its complete execution.
   * <p>
   * This method never returns normally.
   */
  private void runShutdownInNonDaemonThread() {
    Thread shutdownThread = new Thread("OrientDB server shutdown thread") {
      public void run() {
        server.shutdown();
        ShutdownHelper.shutdown(1);
      }
    };
    shutdownThread.setDaemon(false);
    shutdownThread.start();
    try {
      shutdownThread.join();
    } catch (InterruptedException ignored) {
    }
  }

  private void ridBagSize() throws IOException {
    setDataCommandInfo("RidBag get size");

    OBonsaiCollectionPointer collectionPointer = OCollectionNetworkSerializer.INSTANCE.readCollectionPointer(channel);
    final byte[] changeStream = channel.readBytes();

    final OSBTreeCollectionManager sbTreeCollectionManager = connection.getDatabase().getSbTreeCollectionManager();
    final OSBTreeBonsai<OIdentifiable, Integer> tree = sbTreeCollectionManager.loadSBTree(collectionPointer);
    try {
      final Map<OIdentifiable, OSBTreeRidBag.Change> changes = OSBTreeRidBag.ChangeSerializationHelper.INSTANCE
          .deserializeChanges(changeStream, 0);

      int realSize = tree.getRealBagSize(changes);

      beginResponse();
      try {
        sendOk(clientTxId);
        channel.writeInt(realSize);
      } finally {
        endResponse();
      }
    } finally {
      sbTreeCollectionManager.releaseSBTree(collectionPointer);
    }
  }

  private void sbTreeBonsaiGetEntriesMajor() throws IOException {
    setDataCommandInfo("SB-Tree bonsai get values major");

    OBonsaiCollectionPointer collectionPointer = OCollectionNetworkSerializer.INSTANCE.readCollectionPointer(channel);
    byte[] keyStream = channel.readBytes();
    boolean inclusive = channel.readBoolean();
    int pageSize = 128;

    if (connection.getData().protocolVersion >= 21)
      pageSize = channel.readInt();

    final OSBTreeCollectionManager sbTreeCollectionManager = connection.getDatabase().getSbTreeCollectionManager();
    final OSBTreeBonsai<OIdentifiable, Integer> tree = sbTreeCollectionManager.loadSBTree(collectionPointer);
    try {
      final OBinarySerializer<OIdentifiable> keySerializer = tree.getKeySerializer();
      OIdentifiable key = keySerializer.deserialize(keyStream, 0);

      final OBinarySerializer<Integer> valueSerializer = tree.getValueSerializer();

      OTreeInternal.AccumulativeListener<OIdentifiable, Integer> listener = new OTreeInternal.AccumulativeListener<OIdentifiable, Integer>(
          pageSize);
      tree.loadEntriesMajor(key, inclusive, true, listener);
      List<Entry<OIdentifiable, Integer>> result = listener.getResult();
      byte[] stream = serializeSBTreeEntryCollection(result, keySerializer, valueSerializer);

      beginResponse();
      try {
        sendOk(clientTxId);
        channel.writeBytes(stream);
      } finally {
        endResponse();
      }
    } finally {
      sbTreeCollectionManager.releaseSBTree(collectionPointer);
    }
  }

  private byte[] serializeSBTreeEntryCollection(List<Entry<OIdentifiable, Integer>> collection,
      OBinarySerializer<OIdentifiable> keySerializer, OBinarySerializer<Integer> valueSerializer) {
    byte[] stream = new byte[OIntegerSerializer.INT_SIZE
        + collection.size() * (keySerializer.getFixedLength() + valueSerializer.getFixedLength())];
    int offset = 0;

    OIntegerSerializer.INSTANCE.serializeLiteral(collection.size(), stream, offset);
    offset += OIntegerSerializer.INT_SIZE;

    for (Entry<OIdentifiable, Integer> entry : collection) {
      keySerializer.serialize(entry.getKey(), stream, offset);
      offset += keySerializer.getObjectSize(entry.getKey());

      valueSerializer.serialize(entry.getValue(), stream, offset);
      offset += valueSerializer.getObjectSize(entry.getValue());
    }
    return stream;
  }

  private void sbTreeBonsaiFirstKey() throws IOException {
    setDataCommandInfo("SB-Tree bonsai get first key");

    OBonsaiCollectionPointer collectionPointer = OCollectionNetworkSerializer.INSTANCE.readCollectionPointer(channel);

    final OSBTreeCollectionManager sbTreeCollectionManager = connection.getDatabase().getSbTreeCollectionManager();
    final OSBTreeBonsai<OIdentifiable, Integer> tree = sbTreeCollectionManager.loadSBTree(collectionPointer);
    try {
      OIdentifiable result = tree.firstKey();
      final OBinarySerializer<? super OIdentifiable> keySerializer;
      if (result == null) {
        keySerializer = ONullSerializer.INSTANCE;
      } else {
        keySerializer = tree.getKeySerializer();
      }

      byte[] stream = new byte[OByteSerializer.BYTE_SIZE + keySerializer.getObjectSize(result)];
      OByteSerializer.INSTANCE.serialize(keySerializer.getId(), stream, 0);
      keySerializer.serialize(result, stream, OByteSerializer.BYTE_SIZE);

      beginResponse();
      try {
        sendOk(clientTxId);
        channel.writeBytes(stream);
      } finally {
        endResponse();
      }
    } finally {
      sbTreeCollectionManager.releaseSBTree(collectionPointer);
    }
  }

  private void sbTreeBonsaiGet() throws IOException {
    setDataCommandInfo("SB-Tree bonsai get");

    OBonsaiCollectionPointer collectionPointer = OCollectionNetworkSerializer.INSTANCE.readCollectionPointer(channel);
    final byte[] keyStream = channel.readBytes();

    final OSBTreeCollectionManager sbTreeCollectionManager = connection.getDatabase().getSbTreeCollectionManager();
    final OSBTreeBonsai<OIdentifiable, Integer> tree = sbTreeCollectionManager.loadSBTree(collectionPointer);
    try {
      final OIdentifiable key = tree.getKeySerializer().deserialize(keyStream, 0);

      Integer result = tree.get(key);
      final OBinarySerializer<? super Integer> valueSerializer;
      if (result == null) {
        valueSerializer = ONullSerializer.INSTANCE;
      } else {
        valueSerializer = tree.getValueSerializer();
      }

      byte[] stream = new byte[OByteSerializer.BYTE_SIZE + valueSerializer.getObjectSize(result)];
      OByteSerializer.INSTANCE.serialize(valueSerializer.getId(), stream, 0);
      valueSerializer.serialize(result, stream, OByteSerializer.BYTE_SIZE);

      beginResponse();
      try {
        sendOk(clientTxId);
        channel.writeBytes(stream);
      } finally {
        endResponse();
      }
    } finally {
      sbTreeCollectionManager.releaseSBTree(collectionPointer);
    }
  }

  private void createSBTreeBonsai() throws IOException {
    setDataCommandInfo("Create SB-Tree bonsai instance");

    int clusterId = channel.readInt();

    OBonsaiCollectionPointer collectionPointer = connection.getDatabase().getSbTreeCollectionManager().createSBTree(clusterId,
        null);

    beginResponse();
    try {
      sendOk(clientTxId);
      OCollectionNetworkSerializer.INSTANCE.writeCollectionPointer(channel, collectionPointer);
    } finally {
      endResponse();
    }
  }

  private void lowerPositions() throws IOException {
    setDataCommandInfo("Retrieve lower positions");

    final int clusterId = channel.readInt();
    final long clusterPosition = channel.readLong();

    beginResponse();
    try {
      sendOk(clientTxId);

      final OPhysicalPosition[] previousPositions = connection.getDatabase().getStorage().lowerPhysicalPositions(clusterId,
          new OPhysicalPosition(clusterPosition));

      if (previousPositions != null) {
        channel.writeInt(previousPositions.length);

        for (final OPhysicalPosition physicalPosition : previousPositions) {
          channel.writeLong(physicalPosition.clusterPosition);
          channel.writeInt(physicalPosition.recordSize);
          channel.writeVersion(physicalPosition.recordVersion);
        }

      } else {
        channel.writeInt(0); // NO MORE RECORDS
      }

    } finally {
      endResponse();
    }
  }

  private void floorPositions() throws IOException {
    setDataCommandInfo("Retrieve floor positions");

    final int clusterId = channel.readInt();
    final long clusterPosition = channel.readLong();

    beginResponse();
    try {
      sendOk(clientTxId);

      final OPhysicalPosition[] previousPositions = connection.getDatabase().getStorage().floorPhysicalPositions(clusterId,
          new OPhysicalPosition(clusterPosition));

      if (previousPositions != null) {
        channel.writeInt(previousPositions.length);

        for (final OPhysicalPosition physicalPosition : previousPositions) {
          channel.writeLong(physicalPosition.clusterPosition);
          channel.writeInt(physicalPosition.recordSize);
          channel.writeVersion(physicalPosition.recordVersion);
        }

      } else {
        channel.writeInt(0); // NO MORE RECORDS
      }

    } finally {
      endResponse();
    }
  }

  private void higherPositions() throws IOException {
    setDataCommandInfo("Retrieve higher positions");

    final int clusterId = channel.readInt();
    final long clusterPosition = channel.readLong();

    beginResponse();
    try {
      sendOk(clientTxId);

      OPhysicalPosition[] nextPositions = connection.getDatabase().getStorage().higherPhysicalPositions(clusterId,
          new OPhysicalPosition(clusterPosition));

      if (nextPositions != null) {

        channel.writeInt(nextPositions.length);
        for (final OPhysicalPosition physicalPosition : nextPositions) {
          channel.writeLong(physicalPosition.clusterPosition);
          channel.writeInt(physicalPosition.recordSize);
          channel.writeVersion(physicalPosition.recordVersion);
        }
      } else {
        channel.writeInt(0); // NO MORE RECORDS
      }
    } finally {
      endResponse();
    }
  }

  private void ceilingPositions() throws IOException {
    setDataCommandInfo("Retrieve ceiling positions");

    final int clusterId = channel.readInt();
    final long clusterPosition = channel.readLong();

    beginResponse();
    try {
      sendOk(clientTxId);

      final OPhysicalPosition[] previousPositions = connection.getDatabase().getStorage().ceilingPhysicalPositions(clusterId,
          new OPhysicalPosition(clusterPosition));

      if (previousPositions != null) {
        channel.writeInt(previousPositions.length);

        for (final OPhysicalPosition physicalPosition : previousPositions) {
          channel.writeLong(physicalPosition.clusterPosition);
          channel.writeInt(physicalPosition.recordSize);
          channel.writeVersion(physicalPosition.recordVersion);
        }

      } else {
        channel.writeInt(0); // NO MORE RECORDS
      }

    } finally {
      endResponse();
    }
  }

  private boolean isConnectionAlive() {
    if (connection == null || connection.getDatabase() == null) {
      // CONNECTION/DATABASE CLOSED, KILL IT
      server.getClientConnectionManager().kill(connection);
      return false;
    }
    return true;
  }

  private void sendCollectionChanges() throws IOException {
    OSBTreeCollectionManager collectionManager = connection.getDatabase().getSbTreeCollectionManager();
    if (collectionManager != null) {
      Map<UUID, OBonsaiCollectionPointer> changedIds = collectionManager.changedIds();

      channel.writeInt(changedIds.size());

      for (Entry<UUID, OBonsaiCollectionPointer> entry : changedIds.entrySet()) {
        UUID id = entry.getKey();
        channel.writeLong(id.getMostSignificantBits());
        channel.writeLong(id.getLeastSignificantBits());

        OCollectionNetworkSerializer.INSTANCE.writeCollectionPointer(channel, entry.getValue());
      }
      collectionManager.clearChangedIds();
    }
  }

  private void sendDatabaseInformation() throws IOException {
    final Collection<? extends OCluster> clusters = connection.getDatabase().getStorage().getClusterInstances();
    int clusterCount = 0;
    for (OCluster c : clusters) {
      if (c != null) {
        ++clusterCount;
      }
    }
    channel.writeShort((short) clusterCount);

    for (OCluster c : clusters) {
      if (c != null) {
        channel.writeString(c.getName());
        channel.writeShort((short) c.getId());

        if (connection.getData().protocolVersion < 24) {
          channel.writeString("none");
          channel.writeShort((short) -1);
        }
      }
    }
  }

  private void listDatabases() throws IOException {
    checkServerAccess("server.dblist");
    final ODocument result = new ODocument();
    result.field("databases", server.getAvailableStorageNames());

    setDataCommandInfo("List databases");

    beginResponse();
    try {
      sendOk(clientTxId);
      byte[] stream = getRecordBytes(result);
      channel.writeBytes(stream);
    } finally {
      endResponse();
    }
  }

  private void serverInfo() throws IOException {
    checkServerAccess("server.info");

    setDataCommandInfo("Server Info");

    beginResponse();
    try {
      sendOk(clientTxId);
      channel.writeString(OServerInfo.getServerInfo(server));
    } finally {
      endResponse();
    }
  }

  private boolean loadUserFromSchema(final String iUserName, final String iUserPassword) {
    connection.getDatabase().getMetadata().getSecurity().authenticate(iUserName, iUserPassword);
    return true;
  }

}
