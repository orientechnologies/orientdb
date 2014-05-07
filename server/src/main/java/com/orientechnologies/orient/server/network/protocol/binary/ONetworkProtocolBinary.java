/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
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

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.common.concur.lock.OLockException;
import com.orientechnologies.common.io.OIOException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.ONullSerializer;
import com.orientechnologies.orient.client.remote.OCollectionNetworkSerializer;
import com.orientechnologies.orient.client.remote.OEngineRemote;
import com.orientechnologies.orient.core.OConstants;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabaseComplex;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.ODatabaseRecordTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ridbag.sbtree.OBonsaiCollectionPointer;
import com.orientechnologies.orient.core.db.record.ridbag.sbtree.OSBTreeCollectionManager;
import com.orientechnologies.orient.core.db.record.ridbag.sbtree.OSBTreeRidBag;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.OSecurityAccessException;
import com.orientechnologies.orient.core.exception.OSecurityException;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.exception.OTransactionAbortedException;
import com.orientechnologies.orient.core.fetch.OFetchContext;
import com.orientechnologies.orient.core.fetch.OFetchHelper;
import com.orientechnologies.orient.core.fetch.OFetchListener;
import com.orientechnologies.orient.core.fetch.remote.ORemoteFetchContext;
import com.orientechnologies.orient.core.fetch.remote.ORemoteFetchListener;
import com.orientechnologies.orient.core.id.OClusterPosition;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.sbtree.OTreeInternal;
import com.orientechnologies.orient.core.index.sbtreebonsai.local.OSBTreeBonsai;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.metadata.security.OUser;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ORecordBytes;
import com.orientechnologies.orient.core.serialization.OMemoryStream;
import com.orientechnologies.orient.core.serialization.serializer.record.string.ORecordSerializerStringAbstract;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializerAnyStreamable;
import com.orientechnologies.orient.core.storage.OCluster;
import com.orientechnologies.orient.core.storage.OPhysicalPosition;
import com.orientechnologies.orient.core.storage.ORecordMetadata;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.OStorageProxy;
import com.orientechnologies.orient.core.storage.impl.memory.OStorageMemory;
import com.orientechnologies.orient.core.type.ODocumentWrapper;
import com.orientechnologies.orient.core.version.ORecordVersion;
import com.orientechnologies.orient.core.version.OVersionFactory;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryServer;
import com.orientechnologies.orient.server.OClientConnection;
import com.orientechnologies.orient.server.OClientConnectionManager;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.plugin.OServerPlugin;
import com.orientechnologies.orient.server.plugin.OServerPluginHelper;
import com.orientechnologies.orient.server.tx.OTransactionOptimisticProxy;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.SocketException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

public class ONetworkProtocolBinary extends OBinaryNetworkProtocolAbstract {
  protected OClientConnection connection;
  protected OUser             account;

  private String              dbType;

  public ONetworkProtocolBinary() {
    super("OrientDB <- BinaryClient/?");
  }

  public ONetworkProtocolBinary(final String iThreadName) {
    super(iThreadName);
  }

  @Override
  public void config(final OServer iServer, final Socket iSocket, final OContextConfiguration iConfig,
      final List<?> iStatelessCommands, List<?> iStatefulCommands) throws IOException {
    // CREATE THE CLIENT CONNECTION
    connection = OClientConnectionManager.instance().connect(this);

    super.config(iServer, iSocket, iConfig, iStatelessCommands, iStatefulCommands);

    // SEND PROTOCOL VERSION
    channel.writeShort((short) OChannelBinaryProtocol.CURRENT_PROTOCOL_VERSION);

    channel.flush();
    start();

    setName("OrientDB <- BinaryClient (" + iSocket.getRemoteSocketAddress() + ")");
  }

  @Override
  public int getVersion() {
    return OChannelBinaryProtocol.CURRENT_PROTOCOL_VERSION;
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

    OClientConnectionManager.instance().disconnect(connection);
  }

  public String getType() {
    return "binary";
  }

  @Override
  protected void onBeforeRequest() throws IOException {
    waitNodeIsOnline();

    connection = OClientConnectionManager.instance().getConnection(clientTxId);

    if (clientTxId < 0) {
      short protocolId = 0;

      if (connection != null)
        protocolId = connection.data.protocolVersion;

      connection = OClientConnectionManager.instance().connect(this);

      if (connection != null)
        connection.data.protocolVersion = protocolId;
    }

    if (connection != null) {
      ODatabaseRecordThreadLocal.INSTANCE.set(connection.database);
      if (connection.database != null) {
        connection.data.lastDatabase = connection.database.getName();
        connection.data.lastUser = connection.database.getUser() != null ? connection.database.getUser().getName() : null;
      } else {
        connection.data.lastDatabase = null;
        connection.data.lastUser = null;
      }

      ++connection.data.totalRequests;
      setDataCommandInfo("Listening");
      connection.data.commandDetail = "-";
      connection.data.lastCommandReceived = System.currentTimeMillis();
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

  @Override
  protected void onAfterRequest() throws IOException {
    OServerPluginHelper.invokeHandlerCallbackOnAfterClientRequest(server, connection, (byte) requestType);

    if (connection != null) {
      if (connection.database != null)
        if (!connection.database.isClosed())
          connection.database.getLevel1Cache().clear();

      connection.data.lastCommandExecutionTime = System.currentTimeMillis() - connection.data.lastCommandReceived;
      connection.data.totalCommandExecutionTime += connection.data.lastCommandExecutionTime;

      connection.data.lastCommandInfo = connection.data.commandInfo;
      connection.data.lastCommandDetail = connection.data.commandDetail;

      setDataCommandInfo("Listening");
      connection.data.commandDetail = "-";
    }
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

      case OChannelBinaryProtocol.REQUEST_DB_OPEN:
        openDatabase();
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

      case OChannelBinaryProtocol.REQUEST_DATASEGMENT_ADD:
        addDataSegment();
        break;

      case OChannelBinaryProtocol.REQUEST_DATASEGMENT_DROP:
        dropDataSegment();
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

      case OChannelBinaryProtocol.REQUEST_DATACLUSTER_FREEZE:
        freezeCluster();
        break;

      case OChannelBinaryProtocol.REQUEST_DATACLUSTER_RELEASE:
        releaseCluster();
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

      default:
        setDataCommandInfo("Command not supported");
        return false;
      }

      return true;
    } catch (RuntimeException e) {
      if (connection != null && connection.database != null) {
        final OSBTreeCollectionManager collectionManager = connection.database.getSbTreeCollectionManager();
        if (collectionManager != null)
          collectionManager.clearChangedIds();
      }

      throw e;
    }
  }

  protected void checkServerAccess(final String iResource) {
    if (connection.serverUser == null)
      throw new OSecurityAccessException("Server user not authenticated.");

    if (!server.authenticate(connection.serverUser.name, null, iResource))
      throw new OSecurityAccessException("User '" + connection.serverUser.name + "' cannot access to the resource [" + iResource
          + "]. Use another server user or change permission in the file config/orientdb-server-config.xml");
  }

  protected ODatabaseComplex<?> openDatabase(final ODatabaseComplex<?> database, final String iUser, final String iPassword) {

    if (database.isClosed())
      if (database.getStorage() instanceof OStorageMemory && !database.exists())
        database.create();
      else {
        try {
          database.open(iUser, iPassword);
        } catch (OSecurityException e) {
          // TRY WITH SERVER'S USER
          try {
            connection.serverUser = server.serverLogin(iUser, iPassword, "database.passthrough");
          } catch (OSecurityException ex) {
            throw e;
          }

          // SERVER AUTHENTICATED, BYPASS SECURITY
          database.setProperty(ODatabase.OPTIONS.SECURITY.toString(), Boolean.FALSE);
          database.open(iUser, iPassword);
        }
      }

    return database;
  }

  protected void addDataSegment() throws IOException {
    setDataCommandInfo("Add data segment");

    if (!isConnectionAlive())
      return;

    final String name = channel.readString();
    final String location = channel.readString();

    final int num = connection.database.addDataSegment(name, location);

    beginResponse();
    try {
      sendOk(clientTxId);
      channel.writeInt(num);
    } finally {
      endResponse();
    }
  }

  protected void dropDataSegment() throws IOException {
    setDataCommandInfo("Drop data segment");

    if (!isConnectionAlive())
      return;

    final String name = channel.readString();

    boolean result = connection.database.dropDataSegment(name);

    beginResponse();
    try {
      sendOk(clientTxId);
      channel.writeByte((byte) (result ? 1 : 0));
    } finally {
      endResponse();
    }
  }

  protected void removeCluster() throws IOException {
    setDataCommandInfo("Remove cluster");

    if (!isConnectionAlive())
      return;

    final int id = channel.readShort();

    final String clusterName = connection.database.getClusterNameById(id);
    if (clusterName == null)
      throw new IllegalArgumentException("Cluster " + id
          + " doesn't exist anymore. Refresh the db structure or just reconnect to the database");

    boolean result = connection.database.dropCluster(clusterName, true);

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

    final String type = channel.readString();
    final String name = channel.readString();
    int clusterId = -1;

    final String location;
    if (connection.data.protocolVersion >= 10 || type.equalsIgnoreCase("PHYSICAL"))
      location = channel.readString();
    else
      location = null;

    final String dataSegmentName;
    if (connection.data.protocolVersion >= 10)
      dataSegmentName = channel.readString();
    else {
      channel.readInt(); // OLD INIT SIZE, NOT MORE USED
      dataSegmentName = null;
    }

    if (connection.data.protocolVersion >= 18)
      clusterId = channel.readShort();

    Object[] params = null;

    final int num;

    if (clusterId < 0)
      num = connection.database.addCluster(type, name, location, dataSegmentName, params);
    else
      num = connection.database.addCluster(type, name, clusterId, location, dataSegmentName, params);

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

    OClusterPosition[] pos = connection.database.getStorage().getClusterDataRange(channel.readShort());

    beginResponse();
    try {
      sendOk(clientTxId);
      channel.writeClusterPosition(pos[0]);
      channel.writeClusterPosition(pos[1]);
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
    if (connection.data.protocolVersion >= 13)
      countTombstones = channel.readByte() > 0;

    final long count = connection.database.countClusterElements(clusterIds, countTombstones);

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

    dbType = ODatabaseDocument.TYPE;
    if (connection.data.protocolVersion >= 8)
      // READ DB-TYPE FROM THE CLIENT
      dbType = channel.readString();

    final String user = channel.readString();
    final String passwd = channel.readString();

    connection.database = (ODatabaseDocumentTx) server.openDatabase(dbType, dbURL, user, passwd);
    connection.rawDatabase = ((ODatabaseComplex<?>) connection.database.getUnderlying()).getUnderlying();

    if (connection.database.getStorage() instanceof OStorageProxy && !loadUserFromSchema(user, passwd)) {
      sendError(clientTxId, new OSecurityAccessException(connection.database.getName(),
          "User or password not valid for database: '" + connection.database.getName() + "'"));
    } else {

      beginResponse();
      try {
        sendOk(clientTxId);
        channel.writeInt(connection.id);

        sendDatabaseInformation();

        final OServerPlugin plugin = server.getPlugin("cluster");
        ODocument distributedCfg = null;
        if (plugin != null && plugin instanceof ODistributedServerManager)
          distributedCfg = ((ODistributedServerManager) plugin).getClusterConfiguration();

        channel.writeBytes(distributedCfg != null ? distributedCfg.toStream() : null);

        if (connection.data.protocolVersion >= 14)
          channel.writeString(OConstants.getVersion());
      } finally {
        endResponse();
      }
    }
  }

  protected void connect() throws IOException {
    setDataCommandInfo("Connect");

    readConnectionData();

    connection.serverUser = server.serverLogin(channel.readString(), channel.readString(), "connect");

    beginResponse();
    try {
      sendOk(clientTxId);
      channel.writeInt(connection.id);
    } finally {
      endResponse();
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
      channel.close();
      server.shutdown();
      System.exit(0);
    }

    OLogManager.instance().error(this, "Authentication error of remote client %s:%d: shutdown is aborted.",
        channel.socket.getInetAddress(), channel.socket.getPort());

    sendError(clientTxId, new OSecurityAccessException("Invalid user/password to shutdown the server"));
  }

  protected void copyDatabase() throws IOException {
    setDataCommandInfo("Copy the database to a remote server");

    final String dbUrl = channel.readString();
    final String dbUser = channel.readString();
    final String dbPassword = channel.readString();
    final String remoteServerName = channel.readString();
    final String remoteServerEngine = channel.readString();

    checkServerAccess("database.copy");

    final ODatabaseDocumentTx db = (ODatabaseDocumentTx) server.openDatabase(ODatabaseDocument.TYPE, dbUrl, dbUser, dbPassword);

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

      response = new ODocument().fromJSON(dManager.getDatabaseConfiguration((String) request.field("db")).serialize()
          .toJSON("prettyPrint"));

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
      channel.writeLong(connection.database.getStorage().countRecords());
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
      channel.writeLong(connection.database.getStorage().getSize());
    } finally {
      endResponse();
    }
  }

  protected void dropDatabase() throws IOException {
    setDataCommandInfo("Drop database");
    String dbName = channel.readString();

    String storageType;
    if (connection.data.protocolVersion >= 16)
      storageType = channel.readString();
    else
      storageType = "local";

    checkServerAccess("database.delete");

    connection.database = getDatabaseInstance(dbName, ODatabaseDocument.TYPE, storageType);

    if (connection.database.exists()) {
      OLogManager.instance().info(this, "Dropped database '%s'", connection.database.getName());

      if (connection.database.isClosed())
        openDatabase(connection.database, connection.serverUser.name, connection.serverUser.password);

      connection.database.drop();
      connection.close();
    } else {
      throw new OStorageException("Database with name '" + dbName + "' doesn't exits.");
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
    final String storageType;

    if (connection.data.protocolVersion >= 16)
      storageType = channel.readString();
    else
      storageType = "local";

    checkServerAccess("database.exists");

    if (storageType != null)
      connection.database = getDatabaseInstance(dbName, ODatabaseDocument.TYPE, storageType);
    else {
      // CHECK AGAINST ALL THE ENGINE TYPES, BUT REMOTE
      for (String engine : Orient.instance().getEngines()) {
        if (!engine.equalsIgnoreCase(OEngineRemote.NAME)) {
          connection.database = getDatabaseInstance(dbName, ODatabaseDocument.TYPE, engine);
          if (connection.database.exists())
            // FOUND
            break;

          // NOT FOUND: ASSURE TO UNREGISTER IT TO AVOID CACHING
          Orient.instance().unregisterStorage(connection.database.getStorage());
        }
      }
    }

    beginResponse();
    try {
      sendOk(clientTxId);
      channel.writeByte((byte) (connection.database.exists() ? 1 : 0));
    } finally {
      endResponse();
    }
  }

  protected void createDatabase() throws IOException {
    setDataCommandInfo("Create database");

    String dbName = channel.readString();
    String dbType = ODatabaseDocument.TYPE;
    if (connection.data.protocolVersion >= 8)
      // READ DB-TYPE FROM THE CLIENT
      dbType = channel.readString();
    String storageType = channel.readString();

    checkServerAccess("database.create");
    checkStorageExistence(dbName);
    connection.database = getDatabaseInstance(dbName, dbType, storageType);
    createDatabase(connection.database, null, null);
    connection.rawDatabase = (((ODatabaseComplex<?>) connection.database.getUnderlying()).getUnderlying());

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
      if (connection.data.protocolVersion > 0 && connection.data.protocolVersion < 9)
        // OLD CLIENTS WAIT FOR A OK
        sendOk(clientTxId);

      if (OClientConnectionManager.instance().disconnect(connection.id))
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
    if (cfg != null)
      cfg.setValue(value);

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
    String cfgValue = cfg != null ? cfg.getValueAsString() : "";

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

    final OTransactionOptimisticProxy tx = new OTransactionOptimisticProxy((ODatabaseRecordTx) connection.database.getUnderlying(),
        channel);

    try {
      connection.database.begin(tx);

      try {
        connection.database.commit();
        beginResponse();
        try {
          sendOk(clientTxId);

          // SEND BACK ALL THE RECORD IDS FOR THE CREATED RECORDS
          channel.writeInt(tx.getCreatedRecords().size());
          for (Entry<ORecordId, ORecordInternal<?>> entry : tx.getCreatedRecords().entrySet()) {
            channel.writeRID(entry.getKey());
            channel.writeRID(entry.getValue().getIdentity());

            // IF THE NEW OBJECT HAS VERSION > 0 MEANS THAT HAS BEEN UPDATED IN THE SAME TX. THIS HAPPENS FOR GRAPHS
            if (entry.getValue().getRecordVersion().getCounter() > 0)
              tx.getUpdatedRecords().put((ORecordId) entry.getValue().getIdentity(), entry.getValue());
          }

          // SEND BACK ALL THE NEW VERSIONS FOR THE UPDATED RECORDS
          channel.writeInt(tx.getUpdatedRecords().size());
          for (Entry<ORecordId, ORecordInternal<?>> entry : tx.getUpdatedRecords().entrySet()) {
            channel.writeRID(entry.getKey());
            channel.writeVersion(entry.getValue().getRecordVersion());
          }

          if (connection.data.protocolVersion >= 20)
            sendCollectionChanges();
        } finally {
          endResponse();
        }
      } catch (Exception e) {
        if (connection != null && connection.database != null) {
          if (connection.database.getTransaction().isActive())
            connection.database.rollback(true);

          final OSBTreeCollectionManager collectionManager = connection.database.getSbTreeCollectionManager();
          if (collectionManager != null)
            collectionManager.clearChangedIds();
        }

        sendError(clientTxId, e);
      }
    } catch (OTransactionAbortedException e) {
      // TX ABORTED BY THE CLIENT
    } catch (Exception e) {
      // Error during TX initialization, possibly index constraints violation.
      if (tx.isActive())
        tx.rollback(true, -1);

      sendError(clientTxId, e);
    }
  }

  protected void command() throws IOException {
    setDataCommandInfo("Execute remote command");

    final boolean asynch = channel.readByte() == 'a';

    final OCommandRequestText command = (OCommandRequestText) OStreamSerializerAnyStreamable.INSTANCE.fromStream(channel
        .readBytes());

    connection.data.commandDetail = command.getText();

    // ENABLES THE CACHE TO IMPROVE PERFORMANCE OF COMPLEX COMMANDS LIKE TRAVERSE
    // connection.database.getLevel1Cache().setEnable(true);
    beginResponse();
    try {
      final OAbstractCommandResultListener listener;

      if (asynch) {
        listener = new OAsyncCommandResultListener(this, clientTxId, command.getResultListener());
        command.setResultListener(listener);
      } else
        listener = new OSyncCommandResultListener();

      final long serverTimeout = OGlobalConfiguration.COMMAND_TIMEOUT.getValueAsLong();

      if (serverTimeout > 0 && command.getTimeoutTime() > serverTimeout)
        // FORCE THE SERVER'S TIMEOUT
        command.setTimeout(serverTimeout, command.getTimeoutStrategy());

      if (!isConnectionAlive())
        return;

      // ASSIGNED THE PARSED FETCHPLAN
      listener.setFetchPlan(connection.database.command(command).getFetchPlan());

      final Object result = connection.database.command(command).execute();

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

        if (result == null) {
          // NULL VALUE
          channel.writeByte((byte) 'n');
        } else if (result instanceof OIdentifiable) {
          // RECORD
          channel.writeByte((byte) 'r');
          listener.result(result);
          writeIdentifiable((OIdentifiable) result);
        } else if (result instanceof ODocumentWrapper) {
          // RECORD
          channel.writeByte((byte) 'r');
          final ODocument doc = ((ODocumentWrapper) result).getDocument();
          listener.result(doc);
          writeIdentifiable(doc);
        } else if (OMultiValue.isMultiValue(result)) {
          channel.writeByte((byte) 'l');
          channel.writeInt(OMultiValue.getSize(result));
          for (Object o : OMultiValue.getMultiValueIterable(result)) {
            try {
              listener.result(o);
              writeIdentifiable((OIdentifiable) o);
            } catch (Exception e) {
              OLogManager.instance().warn(this, "Cannot serialize record: " + o);
              // WRITE NULL RECORD TO AVOID BREAKING PROTOCOL
              writeIdentifiable(null);
            }
          }
        } else {
          // ANY OTHER (INCLUDING LITERALS)
          channel.writeByte((byte) 'a');
          final StringBuilder value = new StringBuilder();
          listener.result(result);
          ORecordSerializerStringAbstract.fieldTypeToString(value, OType.getTypeByClass(result.getClass()), result);
          channel.writeString(value.toString());
        }

        if (connection.data.protocolVersion >= 17) {
          // SEND FETCHED RECORDS TO LOAD IN CLIENT CACHE
          for (ORecord<?> rec : ((OSyncCommandResultListener) listener).getFetchedRecordsToSend()) {
            channel.writeByte((byte) 2); // CLIENT CACHE RECORD. IT
            // ISN'T PART OF THE
            // RESULT SET
            writeIdentifiable(rec);
          }

          channel.writeByte((byte) 0); // NO MORE RECORDS
        }
      }

    } finally {
      endResponse();
    }
  }

  protected void deleteRecord() throws IOException {
    setDataCommandInfo("Delete record");

    if (!isConnectionAlive())
      return;

    final ORID rid = channel.readRID();
    final ORecordVersion version = channel.readVersion();
    final byte mode = channel.readByte();

    final int result = deleteRecord(connection.database, rid, version);

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

    final int result = hideRecord(connection.database, rid);

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
    final ORecordVersion version = channel.readVersion();
    final byte mode = channel.readByte();

    final int result = cleanOutRecord(connection.database, rid, version);

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
   * VERSION MANAGEMENT:<br/>
   * -1 : DOCUMENT UPDATE, NO VERSION CONTROL<br/>
   * -2 : DOCUMENT UPDATE, NO VERSION CONTROL, NO VERSION INCREMENT<br/>
   * -3 : DOCUMENT ROLLBACK, DECREMENT VERSION<br/>
   * >-1 : MVCC CONTROL, RECORD UPDATE AND VERSION INCREMENT<br/>
   * <-3 : WRONG VERSION VALUE
   * 
   * @throws IOException
   */
  protected void updateRecord() throws IOException {
    setDataCommandInfo("Update record");

    if (!isConnectionAlive())
      return;

    final ORecordId rid = channel.readRID();
    final byte[] buffer = channel.readBytes();
    final ORecordVersion version = channel.readVersion();
    final byte recordType = channel.readByte();
    final byte mode = channel.readByte();

    final ORecordVersion newVersion = updateRecord(connection.database, rid, buffer, version, recordType);

    if (mode < 2) {
      beginResponse();
      try {
        sendOk(clientTxId);
        channel.writeVersion(newVersion);

        if (connection.data.protocolVersion >= 20)
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

    final int dataSegmentId = connection.data.protocolVersion >= 10 ? channel.readInt() : 0;
    final ORecordId rid = new ORecordId(channel.readShort(), ORID.CLUSTER_POS_INVALID);
    final byte[] buffer = channel.readBytes();
    final byte recordType = channel.readByte();
    final byte mode = channel.readByte();

    final ORecord<?> record = createRecord(connection.database, rid, buffer, recordType, dataSegmentId);

    if (mode < 2) {
      beginResponse();
      try {
        sendOk(clientTxId);
        channel.writeClusterPosition(record.getIdentity().getClusterPosition());
        if (connection.data.protocolVersion >= 11)
          channel.writeVersion(record.getRecordVersion());

        if (connection.data.protocolVersion >= 20)
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
      final ORecordMetadata metadata = connection.database.getRecordMetadata(rid);
      sendOk(clientTxId);
      channel.writeRID(metadata.getRecordId());
      channel.writeVersion(metadata.getRecordVersion());
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
    if (connection.data.protocolVersion >= 9)
      ignoreCache = channel.readByte() == 1;

    boolean loadTombstones = false;
    if (connection.data.protocolVersion >= 13)
      loadTombstones = channel.readByte() > 0;

    if (rid.clusterId == 0 && rid.clusterPosition.longValue() == 0) {
      // @COMPATIBILITY 0.9.25
      // SEND THE DB CONFIGURATION INSTEAD SINCE IT WAS ON RECORD 0:0
      OFetchHelper.checkFetchPlanValid(fetchPlanString);

      beginResponse();
      try {
        sendOk(clientTxId);
        channel.writeByte((byte) 1);
        channel.writeBytes(connection.database.getStorage().getConfiguration().toStream());
        channel.writeVersion(OVersionFactory.instance().createVersion());
        channel.writeByte(ORecordBytes.RECORD_TYPE);
        channel.writeByte((byte) 0); // NO MORE RECORDS
      } finally {
        endResponse();
      }

    } else {
      final ORecordInternal<?> record = connection.database.load(rid, fetchPlanString, ignoreCache, loadTombstones,
          OStorage.LOCKING_STRATEGY.DEFAULT);

      beginResponse();
      try {
        sendOk(clientTxId);

        if (record != null) {
          channel.writeByte((byte) 1); // HAS RECORD
          channel.writeBytes(record.toStream());
          channel.writeVersion(record.getRecordVersion());
          channel.writeByte(record.getRecordType());

          if (fetchPlanString.length() > 0) {
            // BUILD THE SERVER SIDE RECORD TO ACCES TO THE FETCH
            // PLAN
            if (record instanceof ODocument) {
              final Map<String, Integer> fetchPlan = OFetchHelper.buildFetchPlan(fetchPlanString);

              final Set<ORecord<?>> recordsToSend = new HashSet<ORecord<?>>();
              final ODocument doc = (ODocument) record;
              final OFetchListener listener = new ORemoteFetchListener() {
                @Override
                protected void sendRecord(ORecord<?> iLinked) {
                  recordsToSend.add(iLinked);
                }
              };
              final OFetchContext context = new ORemoteFetchContext();
              OFetchHelper.fetch(doc, doc, fetchPlan, listener, context, "");

              // SEND RECORDS TO LOAD IN CLIENT CACHE
              for (ORecord<?> d : recordsToSend) {
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
    if (connection != null && connection.database != null && connection.database.getTransaction() != null)
      connection.database.getTransaction().rollback();
    channel.flush();
    channel.releaseWriteLock();
  }

  protected void setDataCommandInfo(final String iCommandInfo) {
    if (connection != null)
      connection.data.commandInfo = iCommandInfo;
  }

  protected void readConnectionData() throws IOException {
    connection.data.driverName = channel.readString();
    connection.data.driverVersion = channel.readString();
    connection.data.protocolVersion = channel.readShort();
    connection.data.clientId = channel.readString();
  }

  protected void sendOk(final int iClientTxId) throws IOException {
    channel.writeByte(OChannelBinaryProtocol.RESPONSE_STATUS_OK);
    channel.writeInt(iClientTxId);
  }

  protected void sendError(final int iClientTxId, final Throwable t) throws IOException {
    channel.acquireWriteLock();
    try {

      channel.writeByte(OChannelBinaryProtocol.RESPONSE_STATUS_ERROR);
      channel.writeInt(iClientTxId);

      Throwable current;
      if (t instanceof OLockException && t.getCause() instanceof ODatabaseException)
        // BYPASS THE DB POOL EXCEPTION TO PROPAGATE THE RIGHT SECURITY ONE
        current = t.getCause();
      else
        current = t;

      final Throwable original = current;
      while (current != null) {
        // MORE DETAILS ARE COMING AS EXCEPTION
        channel.writeByte((byte) 1);

        channel.writeString(current.getClass().getName());
        channel.writeString(current.getMessage());

        current = current.getCause();
      }
      channel.writeByte((byte) 0);

      if (connection != null && connection.data.protocolVersion >= 19) {
        final OMemoryStream memoryStream = new OMemoryStream();
        final ObjectOutputStream objectOutputStream = new ObjectOutputStream(memoryStream);

        objectOutputStream.writeObject(original);
        objectOutputStream.flush();

        final byte[] result = memoryStream.toByteArray();
        objectOutputStream.close();

        channel.writeBytes(result);
      }

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
    } finally {
      channel.releaseWriteLock();
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

    final String storageType;

    if (connection.data.protocolVersion >= 16)
      storageType = channel.readString();
    else
      storageType = "local";

    connection.database = getDatabaseInstance(dbName, ODatabaseDocument.TYPE, storageType);

    if (connection.database.exists()) {
      OLogManager.instance().info(this, "Freezing database '%s'", connection.database.getURL());

      if (connection.database.isClosed())
        openDatabase(connection.database, connection.serverUser.name, connection.serverUser.password);

      connection.database.freeze(true);
    } else {
      throw new OStorageException("Database with name '" + dbName + "' doesn't exits.");
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

    final String storageType;
    if (connection.data.protocolVersion >= 16)
      storageType = channel.readString();
    else
      storageType = "local";

    connection.database = getDatabaseInstance(dbName, ODatabaseDocument.TYPE, storageType);

    if (connection.database.exists()) {
      OLogManager.instance().info(this, "Realising database '%s'", connection.database.getURL());

      if (connection.database.isClosed())
        openDatabase(connection.database, connection.serverUser.name, connection.serverUser.password);

      connection.database.release();
    } else {
      throw new OStorageException("Database with name '" + dbName + "' doesn't exits.");
    }

    beginResponse();
    try {
      sendOk(clientTxId);
    } finally {
      endResponse();
    }
  }

  protected void freezeCluster() throws IOException {
    setDataCommandInfo("Freeze cluster");
    final String dbName = channel.readString();
    final int clusterId = channel.readShort();

    checkServerAccess("database.freeze");

    final String storageType;

    if (connection.data.protocolVersion >= 16)
      storageType = channel.readString();
    else
      storageType = "local";

    connection.database = getDatabaseInstance(dbName, ODatabaseDocument.TYPE, storageType);

    if (connection.database.exists()) {
      OLogManager.instance().info(this, "Freezing database '%s' cluster %d", connection.database.getURL(), clusterId);

      if (connection.database.isClosed()) {
        openDatabase(connection.database, connection.serverUser.name, connection.serverUser.password);
      }

      connection.database.freezeCluster(clusterId);
    } else {
      throw new OStorageException("Database with name '" + dbName + "' doesn't exits.");
    }

    beginResponse();
    try {
      sendOk(clientTxId);
    } finally {
      endResponse();
    }
  }

  protected void releaseCluster() throws IOException {
    setDataCommandInfo("Release database");
    final String dbName = channel.readString();
    final int clusterId = channel.readShort();

    checkServerAccess("database.release");

    final String storageType;
    if (connection.data.protocolVersion >= 16)
      storageType = channel.readString();
    else
      storageType = "local";

    connection.database = getDatabaseInstance(dbName, ODatabaseDocument.TYPE, storageType);

    if (connection.database.exists()) {
      OLogManager.instance().info(this, "Realising database '%s' cluster %d", connection.database.getURL(), clusterId);

      if (connection.database.isClosed()) {
        openDatabase(connection.database, connection.serverUser.name, connection.serverUser.password);
      }

      connection.database.releaseCluster(clusterId);
    } else {
      throw new OStorageException("Database with name '" + dbName + "' doesn't exits.");
    }

    beginResponse();
    try {
      sendOk(clientTxId);
    } finally {
      endResponse();
    }
  }

  private void ridBagSize() throws IOException {
    setDataCommandInfo("RidBag get size");

    OBonsaiCollectionPointer collectionPointer = OCollectionNetworkSerializer.INSTANCE.readCollectionPointer(channel);
    final byte[] changeStream = channel.readBytes();

    OSBTreeBonsai<OIdentifiable, Integer> tree = connection.database.getSbTreeCollectionManager().loadSBTree(collectionPointer);
    final Map<OIdentifiable, OSBTreeRidBag.Change> changes = OSBTreeRidBag.ChangeSerializationHelper.INSTANCE.deserializeChanges(
        changeStream, 0);

    int realSize = tree.getRealBagSize(changes);

    beginResponse();
    try {
      sendOk(clientTxId);
      channel.writeInt(realSize);
    } finally {
      endResponse();
    }
  }

  private void sbTreeBonsaiGetEntriesMajor() throws IOException {
    setDataCommandInfo("SB-Tree bonsai get values major");

    OBonsaiCollectionPointer collectionPointer = OCollectionNetworkSerializer.INSTANCE.readCollectionPointer(channel);
    byte[] keyStream = channel.readBytes();
    boolean inclusive = channel.readBoolean();
    int pageSize = 128;

    if (connection.data.protocolVersion >= 21)
      pageSize = channel.readInt();

    OSBTreeBonsai<OIdentifiable, Integer> tree = connection.database.getSbTreeCollectionManager().loadSBTree(collectionPointer);

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
  }

  private byte[] serializeSBTreeEntryCollection(List<Entry<OIdentifiable, Integer>> collection,
      OBinarySerializer<OIdentifiable> keySerializer, OBinarySerializer<Integer> valueSerializer) {
    byte[] stream = new byte[OIntegerSerializer.INT_SIZE + collection.size()
        * (keySerializer.getFixedLength() + valueSerializer.getFixedLength())];
    int offset = 0;

    OIntegerSerializer.INSTANCE.serialize(collection.size(), stream, offset);
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
    OSBTreeBonsai<OIdentifiable, Integer> tree = connection.database.getSbTreeCollectionManager().loadSBTree(collectionPointer);

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
  }

  private void sbTreeBonsaiGet() throws IOException {
    setDataCommandInfo("SB-Tree bonsai get");

    OBonsaiCollectionPointer collectionPointer = OCollectionNetworkSerializer.INSTANCE.readCollectionPointer(channel);
    final byte[] keyStream = channel.readBytes();

    OSBTreeBonsai<OIdentifiable, Integer> tree = connection.database.getSbTreeCollectionManager().loadSBTree(collectionPointer);

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
  }

  private void createSBTreeBonsai() throws IOException {
    setDataCommandInfo("Create SB-Tree bonsai instance");

    int clusterId = channel.readInt();

    OBonsaiCollectionPointer collectionPointer = connection.database.getSbTreeCollectionManager().createSBTree(clusterId, null);

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
    final OClusterPosition clusterPosition = channel.readClusterPosition();

    beginResponse();
    try {
      sendOk(clientTxId);

      final OPhysicalPosition[] previousPositions = connection.database.getStorage().lowerPhysicalPositions(clusterId,
          new OPhysicalPosition(clusterPosition));

      if (previousPositions != null) {
        channel.writeInt(previousPositions.length);

        for (final OPhysicalPosition physicalPosition : previousPositions) {
          channel.writeClusterPosition(physicalPosition.clusterPosition);
          channel.writeInt(physicalPosition.dataSegmentId);
          channel.writeLong(physicalPosition.dataSegmentPos);
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
    final OClusterPosition clusterPosition = channel.readClusterPosition();

    beginResponse();
    try {
      sendOk(clientTxId);

      final OPhysicalPosition[] previousPositions = connection.database.getStorage().floorPhysicalPositions(clusterId,
          new OPhysicalPosition(clusterPosition));

      if (previousPositions != null) {
        channel.writeInt(previousPositions.length);

        for (final OPhysicalPosition physicalPosition : previousPositions) {
          channel.writeClusterPosition(physicalPosition.clusterPosition);
          channel.writeInt(physicalPosition.dataSegmentId);
          channel.writeLong(physicalPosition.dataSegmentPos);
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
    final OClusterPosition clusterPosition = channel.readClusterPosition();

    beginResponse();
    try {
      sendOk(clientTxId);

      OPhysicalPosition[] nextPositions = connection.database.getStorage().higherPhysicalPositions(clusterId,
          new OPhysicalPosition(clusterPosition));

      if (nextPositions != null) {

        channel.writeInt(nextPositions.length);
        for (final OPhysicalPosition physicalPosition : nextPositions) {
          channel.writeClusterPosition(physicalPosition.clusterPosition);
          channel.writeInt(physicalPosition.dataSegmentId);
          channel.writeLong(physicalPosition.dataSegmentPos);
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
    final OClusterPosition clusterPosition = channel.readClusterPosition();

    beginResponse();
    try {
      sendOk(clientTxId);

      final OPhysicalPosition[] previousPositions = connection.database.getStorage().ceilingPhysicalPositions(clusterId,
          new OPhysicalPosition(clusterPosition));

      if (previousPositions != null) {
        channel.writeInt(previousPositions.length);

        for (final OPhysicalPosition physicalPosition : previousPositions) {
          channel.writeClusterPosition(physicalPosition.clusterPosition);
          channel.writeInt(physicalPosition.dataSegmentId);
          channel.writeLong(physicalPosition.dataSegmentPos);
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
    if (connection == null || connection.database == null) {
      // CONNECTION/DATABASE CLOSED, KILL IT
      OClientConnectionManager.instance().kill(connection);
      return false;
    }
    return true;
  }

  private void sendCollectionChanges() throws IOException {
    OSBTreeCollectionManager collectionManager = connection.database.getSbTreeCollectionManager();
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
    final Collection<? extends OCluster> clusters = connection.database.getStorage().getClusterInstances();
    int clusterCount = 0;
    for (OCluster c : clusters) {
      if (c != null) {
        ++clusterCount;
      }
    }
    if (connection.data.protocolVersion >= 7)
      channel.writeShort((short) clusterCount);
    else
      channel.writeInt(clusterCount);

    for (OCluster c : clusters) {
      if (c != null) {
        channel.writeString(c.getName());
        channel.writeShort((short) c.getId());
        channel.writeString(c.getType());
        if (connection.data.protocolVersion >= 12)
          channel.writeShort((short) c.getDataSegmentId());
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
      channel.writeBytes(result.toStream());
    } finally {
      endResponse();
    }
  }

  private boolean loadUserFromSchema(final String iUserName, final String iUserPassword) {
    account = connection.database.getMetadata().getSecurity().authenticate(iUserName, iUserPassword);
    return true;
  }
}
