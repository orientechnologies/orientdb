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
import com.orientechnologies.common.concur.lock.OInterruptedException;
import com.orientechnologies.common.concur.lock.OLockException;
import com.orientechnologies.common.exception.OException;
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
import com.orientechnologies.orient.core.command.*;
import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ridbag.sbtree.OBonsaiCollectionPointer;
import com.orientechnologies.orient.core.db.record.ridbag.sbtree.OSBTreeCollectionManager;
import com.orientechnologies.orient.core.db.record.ridbag.sbtree.OSBTreeRidBag;
import com.orientechnologies.orient.core.db.tool.ODatabaseImport;
import com.orientechnologies.orient.core.engine.local.OEngineLocalPaginated;
import com.orientechnologies.orient.core.engine.memory.OEngineMemory;
import com.orientechnologies.orient.core.exception.*;
import com.orientechnologies.orient.core.fetch.OFetchContext;
import com.orientechnologies.orient.core.fetch.OFetchHelper;
import com.orientechnologies.orient.core.fetch.OFetchListener;
import com.orientechnologies.orient.core.fetch.OFetchPlan;
import com.orientechnologies.orient.core.fetch.remote.ORemoteFetchContext;
import com.orientechnologies.orient.core.fetch.remote.ORemoteFetchListener;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.sbtree.OTreeInternal;
import com.orientechnologies.orient.core.index.sbtreebonsai.local.OSBTreeBonsai;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.metadata.security.OUser;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.OBlob;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import com.orientechnologies.orient.core.serialization.OMemoryStream;
import com.orientechnologies.orient.core.serialization.serializer.ONetworkThreadLocalSerializer;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializerFactory;
import com.orientechnologies.orient.core.serialization.serializer.record.OSerializationThreadLocal;
import com.orientechnologies.orient.core.serialization.serializer.record.string.ORecordSerializerSchemaAware2CSV;
import com.orientechnologies.orient.core.serialization.serializer.record.string.ORecordSerializerStringAbstract;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializerAnyStreamable;
import com.orientechnologies.orient.core.sql.query.OSQLAsynchQuery;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.core.storage.*;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OOfflineClusterException;
import com.orientechnologies.orient.core.type.ODocumentWrapper;
import com.orientechnologies.orient.enterprise.channel.binary.*;
import com.orientechnologies.orient.server.OClientConnection;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.OServerInfo;
import com.orientechnologies.orient.server.ShutdownHelper;
import com.orientechnologies.orient.server.distributed.*;
import com.orientechnologies.orient.server.network.OServerNetworkListener;
import com.orientechnologies.orient.server.network.protocol.OBeforeDatabaseOpenNetworkEventListener;
import com.orientechnologies.orient.server.network.protocol.ONetworkProtocol;
import com.orientechnologies.orient.server.plugin.OServerPlugin;
import com.orientechnologies.orient.server.plugin.OServerPluginHelper;
import com.orientechnologies.orient.server.tx.OTransactionOptimisticProxy;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.logging.Level;

public class ONetworkProtocolBinary extends ONetworkProtocol {
  protected final    Level          logClientExceptions;
  protected final    boolean        logClientFullStackTrace;
  protected          OChannelBinary channel;
  protected volatile int            requestType;
  protected          int            clientTxId;
  protected          boolean        okSent;
  private boolean tokenConnection = true;
  private long    requests        = 0;
  private OServerNetworkListener listener;

  public ONetworkProtocolBinary() {
    this("OrientDB <- BinaryClient/?");
  }

  public ONetworkProtocolBinary(final String iThreadName) {
    super(Orient.instance().getThreadGroup(), iThreadName);
    logClientExceptions = Level.parse(OGlobalConfiguration.SERVER_LOG_DUMP_CLIENT_EXCEPTION_LEVEL.getValueAsString());
    logClientFullStackTrace = OGlobalConfiguration.SERVER_LOG_DUMP_CLIENT_EXCEPTION_FULLSTACKTRACE.getValueAsBoolean();
  }

  /**
   * Internal varialbe injection useful for testing.
   *
   * @param server
   * @param channel
   */
  public void initVariables(final OServer server, OChannelBinaryServer channel) {
    this.server = server;
    this.channel = channel;
  }

  @Override
  public void config(final OServerNetworkListener iListener, final OServer iServer, final Socket iSocket,
      final OContextConfiguration iConfig) throws IOException {
    listener = iListener;
    OChannelBinaryServer channel = new OChannelBinaryServer(iSocket, iConfig);
    initVariables(iServer, channel);

    // SEND PROTOCOL VERSION
    channel.writeShort((short) getVersion());

    channel.flush();
    start();
    setName("OrientDB (" + iSocket.getLocalSocketAddress() + ") <- BinaryClient (" + iSocket.getRemoteSocketAddress() + ")");
  }

  @Override
  public void startup() {
    super.startup();
  }

  @Override
  public void shutdown() {
    sendShutdown();
    channel.close();
  }

  private boolean isHandshaking(int requestType) {
    return requestType == OChannelBinaryProtocol.REQUEST_CONNECT || requestType == OChannelBinaryProtocol.REQUEST_DB_OPEN
        || requestType == OChannelBinaryProtocol.REQUEST_SHUTDOWN || requestType == OChannelBinaryProtocol.REQUEST_DB_REOPEN;
  }

  private boolean isDistributed(int requestType) {
    return requestType == OChannelBinaryProtocol.DISTRIBUTED_REQUEST || requestType == OChannelBinaryProtocol.DISTRIBUTED_RESPONSE;
  }

  @Override
  protected void execute() throws Exception {
    requestType = -1;

    // do not remove this or we will get deadlock upon shutdown.
    if (isShutdownFlag())
      return;

    clientTxId = 0;
    okSent = false;
    try {
      requestType = channel.readByte();

      OChannelBinaryProtocol.checkRequestTypeRange(channel, requestType);

      clientTxId = channel.readInt();
      // GET THE CONNECTION IF EXIST
      OClientConnection connection = server.getClientConnectionManager().getConnection(clientTxId, this);
      if (isHandshaking(requestType)) {
        handshakeRequest(connection, requestType, clientTxId);
      } else if (isDistributed(requestType)) {
        distributedRequest(connection, requestType, clientTxId);
      } else
        sessionRequest(connection, requestType, clientTxId);
    } catch (Exception e) {
      // if an exception arrive to this point we need to kill the current socket.
      sendShutdown();
      throw e;
    }
  }

  private void handshakeRequest(OClientConnection connection, int requestType, int clientTxId) throws IOException {
    long timer = 0;
    try {

      timer = Orient.instance().getProfiler().startChrono();
      try {
        connection = onBeforeHandshakeRequest(connection, channel);
      } catch (Exception e) {
        sendError(null, clientTxId, e);
        handleConnectionError(null, e);
        afterOperationRequest(null);
        sendShutdown();
        return;
      }
      OLogManager.instance().debug(this, "Request id:" + clientTxId + " type:" + requestType);

      try {
        switch (requestType) {
        case OChannelBinaryProtocol.REQUEST_CONNECT:
          connect(connection);
          break;

        case OChannelBinaryProtocol.REQUEST_DB_OPEN:
          openDatabase(connection);
          break;

        case OChannelBinaryProtocol.REQUEST_SHUTDOWN:
          shutdownConnection(connection);
          break;

        case OChannelBinaryProtocol.REQUEST_DB_REOPEN:
          reopenDatabase(connection);
          break;

        }
      } finally {
        requests++;
        afterOperationRequest(connection);
      }

    } catch (IOException e) {
      OLogManager.instance().debug(this, "I/O Error on client clientId=%d reqType=%d", clientTxId, requestType, e);
      sendShutdown();
    } catch (OException e) {
      sendErrorOrDropConnection(connection, clientTxId, e);
    } catch (RuntimeException e) {
      sendErrorOrDropConnection(connection, clientTxId, e);
    } catch (Throwable t) {
      sendErrorOrDropConnection(connection, clientTxId, t);
    } finally {
      Orient.instance().getProfiler()
          .stopChrono("server.network.requests", "Total received requests", timer, "server.network.requests");

      OSerializationThreadLocal.INSTANCE.get().clear();
    }
  }

  private void distributedRequest(OClientConnection connection, int requestType, int clientTxId) throws IOException {
    long timer = 0;
    try {

      timer = Orient.instance().getProfiler().startChrono();
      connection = onBeforeOperationalRequest(connection, channel);
      OLogManager.instance().debug(this, "Request id:" + clientTxId + " type:" + requestType);

      try {
        switch (requestType) {
        case OChannelBinaryProtocol.DISTRIBUTED_REQUEST:
          executeDistributedRequest(connection);
          break;

        case OChannelBinaryProtocol.DISTRIBUTED_RESPONSE:
          executeDistributedResponse(connection);
          break;
        }
      } finally {
        requests++;
        afterOperationRequest(connection);
      }

    } catch (Throwable t) {
      // IN CASE OF DISTRIBUTED ANY EXCEPTION AT THIS POINT CAUSE THIS CONNECTION TO CLOSE
      OLogManager.instance()
          .warn(this, "I/O Error on distributed channel (clientId=%d reqType=%d error=%s)", clientTxId, requestType, t);
      sendShutdown();
    } finally {
      Orient.instance().getProfiler()
          .stopChrono("server.network.requests", "Total received requests", timer, "server.network.requests");
    }

  }

  private void sessionRequest(OClientConnection connection, int requestType, int clientTxId) throws IOException {
    long timer = 0;
    try {

      timer = Orient.instance().getProfiler().startChrono();
      try {
        connection = onBeforeOperationalRequest(connection, channel);
      } catch (Exception e) {
        if (requestType != OChannelBinaryProtocol.REQUEST_DB_CLOSE) {
          try {
            sendError(connection, clientTxId, e);
            channel.flush();
            if (!(e instanceof OTokenSecurityException))
              OLogManager.instance().error(this, "Error executing request", e);
            OServerPluginHelper.invokeHandlerCallbackOnClientError(server, connection, e);
            afterOperationRequest(connection);

          } finally {
            // IF IT IS THE CASE OF A DOS, FORCE A LITTLE WAIT
            Thread.sleep(100);

            try {
              channel.close();
            } catch (Exception exx) {
              // IGNORE IT
            }

            sendShutdown();
          }
        }
        return;
      }

      OLogManager.instance().debug(this, "Request id:" + clientTxId + " type:" + requestType);

      try {
        if (!executeRequest(connection)) {
          OLogManager.instance().error(this, "Request not supported. Code: " + requestType);
          channel.clearInput();
          sendErrorOrDropConnection(connection, clientTxId,
              new ONetworkProtocolException("Request not supported. Code: " + requestType));
        }
      } finally {
        requests++;
        afterOperationRequest(connection);
      }

    } catch (IOException e) {
      OLogManager.instance().debug(this, "I/O Error on client clientId=%d reqType=%d", clientTxId, requestType, e);
      sendShutdown();
    } catch (OException e) {
      sendErrorOrDropConnection(connection, clientTxId, e);
    } catch (RuntimeException e) {
      sendErrorOrDropConnection(connection, clientTxId, e);
    } catch (Throwable t) {
      sendErrorOrDropConnection(connection, clientTxId, t);
    } finally {
      Orient.instance().getProfiler()
          .stopChrono("server.network.requests", "Total received requests", timer, "server.network.requests");

      OSerializationThreadLocal.INSTANCE.get().clear();
    }
  }

  private OClientConnection onBeforeHandshakeRequest(OClientConnection connection, OChannelBinary channel) throws IOException {
    try {
      if (requestType != OChannelBinaryProtocol.REQUEST_DB_REOPEN) {
        if (clientTxId >= 0 && connection == null && (requestType == OChannelBinaryProtocol.REQUEST_DB_OPEN
            || requestType == OChannelBinaryProtocol.REQUEST_CONNECT)) {
          // THIS EXCEPTION SHULD HAPPEN IN ANY CASE OF OPEN/CONNECT WITH SESSIONID >= 0, BUT FOR COMPATIBILITY IT'S ONLY IF THERE
          // IS NO CONNECTION
          shutdown();
          throw new OIOException("Found unknown session " + clientTxId);
        }
        connection = server.getClientConnectionManager().connect(this);
        connection.getData().sessionId = clientTxId;
        connection.setTokenBytes(null);
        connection.acquire();
      } else {
        byte[] bytes = channel.readBytes();
        connection.validateSession(bytes, server.getTokenHandler(), this);
        server.getClientConnectionManager().disconnect(clientTxId);
        connection = server.getClientConnectionManager().reConnect(this, connection.getTokenBytes(), connection.getToken());
        connection.acquire();
        waitDistribuedIsOnline(connection);
        connection.init(server);

        if (connection.getData().serverUser) {
          connection.setServerUser(server.getUser(connection.getData().serverUsername));
        }
      }
    } catch (RuntimeException e) {
      if (connection != null)
        server.getClientConnectionManager().disconnect(connection);
      ODatabaseRecordThreadLocal.INSTANCE.remove();
      throw e;
    } catch (IOException e) {
      if (connection != null)
        server.getClientConnectionManager().disconnect(connection);
      ODatabaseRecordThreadLocal.INSTANCE.remove();
      throw e;
    }

    connection.statsUpdate();

    OServerPluginHelper.invokeHandlerCallbackOnBeforeClientRequest(server, connection, (byte) requestType);
    return connection;
  }

  private OClientConnection onBeforeOperationalRequest(OClientConnection connection, OChannelBinary channel) throws IOException {
    try {
      if (connection == null && requestType == OChannelBinaryProtocol.REQUEST_DB_CLOSE)
        return null;

      if (connection != null && !Boolean.TRUE.equals(connection.getTokenBased())) {
        // BACKWARD COMPATIBILITY MODE
        connection.setTokenBytes(null);
        connection.acquire();
      } else {
        // STANDARD FLOW
        if (!tokenConnection) {
          // ARRIVED HERE FOR DIRECT TOKEN CONNECTION, BUT OLD STYLE SESSION.
          throw new OIOException("Found unknown session " + clientTxId);
        }
        byte[] bytes = channel.readBytes();
        if (connection == null && bytes != null && bytes.length > 0) {
          // THIS IS THE CASE OF A TOKEN OPERATION WITHOUT HANDSHAKE ON THIS CONNECTION.
          connection = server.getClientConnectionManager().connect(this);
          connection.setDisconnectOnAfter(true);
        }
        if (connection == null) {
          throw new OTokenSecurityException("missing session and token");
        }
        connection.acquire();
        connection.validateSession(bytes, server.getTokenHandler(), this);
        waitDistribuedIsOnline(connection);
        connection.init(server);
        if (connection.getData().serverUser) {
          connection.setServerUser(server.getUser(connection.getData().serverUsername));
        }
      }

      connection.statsUpdate();
      OServerPluginHelper.invokeHandlerCallbackOnBeforeClientRequest(server, connection, (byte) requestType);

    } catch (RuntimeException e) {
      if (connection != null)
        server.getClientConnectionManager().disconnect(connection);
      ODatabaseRecordThreadLocal.INSTANCE.remove();
      throw e;
    } catch (IOException e) {
      if (connection != null)
        server.getClientConnectionManager().disconnect(connection);
      ODatabaseRecordThreadLocal.INSTANCE.remove();
      throw e;
    }
    return connection;
  }

  private void waitDistribuedIsOnline(OClientConnection connection) {
    if (requests == 0) {
      final ODistributedServerManager manager = server.getDistributedManager();
      if (manager != null && connection.getDatabase() != null)
        try {
          final ODistributedDatabase dDatabase = manager.getMessageService().getDatabase(connection.getDatabase().getName());
          if (dDatabase != null) {
            dDatabase.waitForOnline();
          } else
            manager.waitUntilNodeOnline(manager.getLocalNodeName(), connection.getToken().getDatabase());

        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new OInterruptedException("Request interrupted");
        }
    }
  }

  protected void afterOperationRequest(OClientConnection connection) throws IOException {
    OServerPluginHelper.invokeHandlerCallbackOnAfterClientRequest(server, connection, (byte) requestType);

    if (connection != null) {
      connection.endOperation();
      if (connection.isDisconnectOnAfter()) {
        server.getClientConnectionManager().disconnect(connection);
      }
    }
    setDataCommandInfo(connection, "Listening");
  }

  protected boolean executeRequest(OClientConnection connection) throws IOException {
    try {
      switch (requestType) {

      case OChannelBinaryProtocol.REQUEST_DB_LIST:
        listDatabases(connection);
        break;

      case OChannelBinaryProtocol.REQUEST_SERVER_INFO:
        serverInfo(connection);
        break;

      case OChannelBinaryProtocol.REQUEST_DB_RELOAD:
        reloadDatabase(connection);
        break;

      case OChannelBinaryProtocol.REQUEST_DB_CREATE:
        createDatabase(connection);
        break;

      case OChannelBinaryProtocol.REQUEST_DB_CLOSE:
        closeDatabase(connection);
        break;

      case OChannelBinaryProtocol.REQUEST_DB_EXIST:
        existsDatabase(connection);
        break;

      case OChannelBinaryProtocol.REQUEST_DB_DROP:
        dropDatabase(connection);
        break;

      case OChannelBinaryProtocol.REQUEST_DB_SIZE:
        sizeDatabase(connection);
        break;

      case OChannelBinaryProtocol.REQUEST_DB_COUNTRECORDS:
        countDatabaseRecords(connection);
        break;

      case OChannelBinaryProtocol.REQUEST_CLUSTER:
        distributedCluster(connection);
        break;

      case OChannelBinaryProtocol.REQUEST_DATACLUSTER_COUNT:
        countClusters(connection);
        break;

      case OChannelBinaryProtocol.REQUEST_DATACLUSTER_DATARANGE:
        rangeCluster(connection);
        break;

      case OChannelBinaryProtocol.REQUEST_DATACLUSTER_ADD:
        addCluster(connection);
        break;

      case OChannelBinaryProtocol.REQUEST_DATACLUSTER_DROP:
        removeCluster(connection);
        break;

      case OChannelBinaryProtocol.REQUEST_RECORD_METADATA:
        readRecordMetadata(connection);
        break;

      case OChannelBinaryProtocol.REQUEST_RECORD_LOAD:
        readRecord(connection);
        break;

      case OChannelBinaryProtocol.REQUEST_RECORD_LOAD_IF_VERSION_NOT_LATEST:
        readRecordIfVersionIsNotLatest(connection);
        break;

      case OChannelBinaryProtocol.REQUEST_RECORD_CREATE:
        createRecord(connection);
        break;

      case OChannelBinaryProtocol.REQUEST_RECORD_UPDATE:
        updateRecord(connection);
        break;

      case OChannelBinaryProtocol.REQUEST_RECORD_DELETE:
        deleteRecord(connection);
        break;

      case OChannelBinaryProtocol.REQUEST_RECORD_HIDE:
        hideRecord(connection);
        break;

      case OChannelBinaryProtocol.REQUEST_POSITIONS_HIGHER:
        higherPositions(connection);
        break;

      case OChannelBinaryProtocol.REQUEST_POSITIONS_CEILING:
        ceilingPositions(connection);
        break;

      case OChannelBinaryProtocol.REQUEST_POSITIONS_LOWER:
        lowerPositions(connection);
        break;

      case OChannelBinaryProtocol.REQUEST_POSITIONS_FLOOR:
        floorPositions(connection);
        break;

      case OChannelBinaryProtocol.REQUEST_COUNT:
        throw new UnsupportedOperationException("Operation OChannelBinaryProtocol.REQUEST_COUNT has been deprecated");

      case OChannelBinaryProtocol.REQUEST_COMMAND:
        command(connection);
        break;

      case OChannelBinaryProtocol.REQUEST_TX_COMMIT:
        commit(connection);
        break;

      case OChannelBinaryProtocol.REQUEST_CONFIG_GET:
        configGet(connection);
        break;

      case OChannelBinaryProtocol.REQUEST_CONFIG_SET:
        configSet(connection);
        break;

      case OChannelBinaryProtocol.REQUEST_CONFIG_LIST:
        configList(connection);
        break;

      case OChannelBinaryProtocol.REQUEST_DB_FREEZE:
        freezeDatabase(connection);
        break;

      case OChannelBinaryProtocol.REQUEST_DB_RELEASE:
        releaseDatabase(connection);
        break;

      case OChannelBinaryProtocol.REQUEST_RECORD_CLEAN_OUT:
        cleanOutRecord(connection);
        break;

      case OChannelBinaryProtocol.REQUEST_CREATE_SBTREE_BONSAI:
        createSBTreeBonsai(connection);
        break;

      case OChannelBinaryProtocol.REQUEST_SBTREE_BONSAI_GET:
        sbTreeBonsaiGet(connection);
        break;

      case OChannelBinaryProtocol.REQUEST_SBTREE_BONSAI_FIRST_KEY:
        sbTreeBonsaiFirstKey(connection);
        break;

      case OChannelBinaryProtocol.REQUEST_SBTREE_BONSAI_GET_ENTRIES_MAJOR:
        sbTreeBonsaiGetEntriesMajor(connection);
        break;

      case OChannelBinaryProtocol.REQUEST_RIDBAG_GET_SIZE:
        ridBagSize(connection);
        break;

      case OChannelBinaryProtocol.REQUEST_INCREMENTAL_BACKUP:
        incrementalBackup(connection);
        break;

      case OChannelBinaryProtocol.REQUEST_DB_IMPORT:
        importDatabase(connection);
        break;

      default:
        setDataCommandInfo(connection, "Command not supported");
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

  private void reopenDatabase(OClientConnection connection) throws IOException {
    // TODO:REASSOCIATE CONNECTION TO CLIENT.
    beginResponse();
    try {
      sendOk(connection, clientTxId);
      channel.writeInt(connection.getId());
    } finally {
      endResponse(connection);
    }
  }

  protected void checkServerAccess(final String iResource, OClientConnection connection) {
    if (connection.getData().protocolVersion <= OChannelBinaryProtocol.PROTOCOL_VERSION_26) {
      if (connection.getServerUser() == null)
        throw new OSecurityAccessException("Server user not authenticated");

      if (!server.isAllowed(connection.getServerUser().name, iResource))
        throw new OSecurityAccessException(
            "User '" + connection.getServerUser().name + "' cannot access to the resource [" + iResource
                + "]. Use another server user or change permission in the file config/orientdb-server-config.xml");
    } else {
      if (!connection.getData().serverUser)
        throw new OSecurityAccessException("Server user not authenticated");

      if (!server.isAllowed(connection.getData().serverUsername, iResource))
        throw new OSecurityAccessException(
            "User '" + connection.getData().serverUsername + "' cannot access to the resource [" + iResource
                + "]. Use another server user or change permission in the file config/orientdb-server-config.xml");
    }
  }

  protected void removeCluster(OClientConnection connection) throws IOException {
    setDataCommandInfo(connection, "Remove cluster");

    if (!isConnectionAlive(connection))
      return;

    final int id = channel.readShort();

    final String clusterName = connection.getDatabase().getClusterNameById(id);
    if (clusterName == null)
      throw new IllegalArgumentException(
          "Cluster " + id + " does not exist anymore. Refresh the db structure or just reconnect to the database");

    boolean result = connection.getDatabase().dropCluster(clusterName, false);

    beginResponse();
    try {
      sendOk(connection, clientTxId);
      channel.writeByte((byte) (result ? 1 : 0));
    } finally {
      endResponse(connection);
    }
  }

  protected void addCluster(OClientConnection connection) throws IOException {
    setDataCommandInfo(connection, "Add cluster");

    if (!isConnectionAlive(connection))
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
      sendOk(connection, clientTxId);
      channel.writeShort((short) num);
    } finally {
      endResponse(connection);
    }
  }

  protected void rangeCluster(OClientConnection connection) throws IOException {
    setDataCommandInfo(connection, "Get the begin/end range of data in cluster");

    if (!isConnectionAlive(connection))
      return;

    final long[] pos = connection.getDatabase().getStorage().getClusterDataRange(channel.readShort());

    beginResponse();
    try {
      sendOk(connection, clientTxId);
      channel.writeLong(pos[0]);
      channel.writeLong(pos[1]);
    } finally {
      endResponse(connection);
    }
  }

  protected void countClusters(OClientConnection connection) throws IOException {
    setDataCommandInfo(connection, "Count cluster elements");

    if (!isConnectionAlive(connection))
      return;

    int[] clusterIds = new int[channel.readShort()];
    for (int i = 0; i < clusterIds.length; ++i)
      clusterIds[i] = channel.readShort();

    boolean countTombstones = false;
    countTombstones = channel.readByte() > 0;

    final long count = connection.getDatabase().countClusterElements(clusterIds, countTombstones);

    beginResponse();
    try {
      sendOk(connection, clientTxId);
      channel.writeLong(count);
    } finally {
      endResponse(connection);
    }
  }

  protected void reloadDatabase(OClientConnection connection) throws IOException {
    setDataCommandInfo(connection, "Reload database information");

    if (!isConnectionAlive(connection))
      return;

    beginResponse();
    try {
      sendOk(connection, clientTxId);

      sendDatabaseInformation(connection);

    } finally {
      endResponse(connection);
    }
  }

  protected void openDatabase(OClientConnection connection) throws IOException {
    setDataCommandInfo(connection, "Open database");

    readConnectionData(connection);

    final String dbURL = channel.readString();

    String dbType = ODatabaseDocument.TYPE;
    if (connection.getData().protocolVersion <= OChannelBinaryProtocol.PROTOCOL_VERSION_32)
      // READ DB-TYPE FROM THE CLIENT. NOT USED ANYMORE
      dbType = channel.readString();

    final String user = channel.readString();
    final String passwd = channel.readString();

    for (OBeforeDatabaseOpenNetworkEventListener l : listener.getBeforeDatabaseOpenNetworkEventListener())
      l.onBeforeDatabaseOpen(dbURL);

    try {
      connection.setDatabase((ODatabaseDocumentTx) server.openDatabase(dbURL, user, passwd, connection.getData()));
    } catch (OException e) {
      server.getClientConnectionManager().disconnect(connection);
      throw e;
    }

    byte[] token = null;

    if (Boolean.TRUE.equals(connection.getTokenBased())) {
      token = server.getTokenHandler()
          .getSignedBinaryToken(connection.getDatabase(), connection.getDatabase().getUser(), connection.getData());
      // TODO: do not use the parse split getSignedBinaryToken in two methods.
      getServer().getClientConnectionManager().connect(this, connection, token, server.getTokenHandler());
    }

    if (connection.getDatabase().getStorage() instanceof OStorageProxy && !loadUserFromSchema(connection, user, passwd)) {
      sendErrorOrDropConnection(connection, clientTxId, new OSecurityAccessException(connection.getDatabase().getName(),
          "User or password not valid for database: '" + connection.getDatabase().getName() + "'"));
    } else {

      beginResponse();
      try {
        sendOk(connection, clientTxId);
        channel.writeInt(connection.getId());
        if (connection.getData().protocolVersion > OChannelBinaryProtocol.PROTOCOL_VERSION_26) {
          if (Boolean.TRUE.equals(connection.getTokenBased())) {

            channel.writeBytes(token);
          } else
            channel.writeBytes(OCommonConst.EMPTY_BYTE_ARRAY);
        }

        sendDatabaseInformation(connection);

        final OServerPlugin plugin = server.getPlugin("cluster");
        ODocument distributedCfg = null;
        if (plugin != null && plugin instanceof ODistributedServerManager) {
          distributedCfg = ((ODistributedServerManager) plugin).getClusterConfiguration();

          final ODistributedConfiguration dbCfg = ((ODistributedServerManager) plugin)
              .getDatabaseConfiguration(connection.getDatabase().getName());
          if (dbCfg != null) {
            // ENHANCE SERVER CFG WITH DATABASE CFG
            distributedCfg.field("database", dbCfg.getDocument(), OType.EMBEDDED);
          }
        }

        channel.writeBytes(distributedCfg != null ? getRecordBytes(connection, distributedCfg) : null);

        if (connection.getData().protocolVersion >= 14)
          channel.writeString(OConstants.getVersion());
      } finally {
        endResponse(connection);
      }
    }
  }

  protected void connect(OClientConnection connection) throws IOException {
    setDataCommandInfo(connection, "Connect");

    readConnectionData(connection);

    final String user = channel.readString();
    final String passwd = channel.readString();
    connection.setServerUser(server.serverLogin(user, passwd, "server.connect"));

    if (connection.getServerUser() == null)
      throw new OSecurityAccessException("Wrong user/password to [connect] to the remote OrientDB Server instance");

    beginResponse();
    try {
      sendOk(connection, clientTxId);
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
      endResponse(connection);
    }
  }

  private void incrementalBackup(OClientConnection connection) throws IOException {
    setDataCommandInfo(connection, "Incremental backup");

    if (!isConnectionAlive(connection))
      return;

    final String path = channel.readString();

    String fileName = connection.getDatabase().incrementalBackup(path);

    beginResponse();
    try {
      sendOk(connection, clientTxId);
      channel.writeString(fileName);
    } finally {
      endResponse(connection);
    }
  }

  private void executeDistributedRequest(OClientConnection connection) throws IOException {
    setDataCommandInfo(connection, "Distributed request");

    checkServerAccess("server.replication", connection);

    final ODistributedServerManager manager = server.getDistributedManager();
    final ODistributedRequest req = new ODistributedRequest(manager.getTaskFactory());

    req.fromStream(channel.getDataInput());

    final String dbName = req.getDatabaseName();
    ODistributedDatabase ddb = null;
    if (dbName != null) {
      ddb = manager.getMessageService().getDatabase(dbName);
      if (ddb == null && req.getTask().isNodeOnlineRequired())
        throw new ODistributedException("Database configuration not found for database '" + req.getDatabaseName() + "'");
    }

    // SET THE SENDER IN THE TASK
    String senderNodeName = manager.getNodeNameById(req.getId().getNodeId());
    req.getTask().setNodeSource(senderNodeName);

    if (ddb != null)
      ddb.processRequest(req);
    else {
      manager.executeOnLocalNode(req.getId(), req.getTask(), null);
    }
  }

  private void executeDistributedResponse(OClientConnection connection) throws IOException {
    setDataCommandInfo(connection, "Distributed response");

    checkServerAccess("server.replication", connection);

    final ODistributedServerManager manager = server.getDistributedManager();
    final ODistributedResponse response = new ODistributedResponse();

    response.fromStream(channel.getDataInput());

    if (ODistributedServerLog.isDebugEnabled())
      ODistributedServerLog
          .debug(this, manager.getLocalNodeName(), response.getExecutorNodeName(), ODistributedServerLog.DIRECTION.IN,
              "Executing distributed response %s", response);

    // WHILE MSG SERVICE IS UP & RUNNING
    while (manager.getMessageService() == null)
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        return;
      }

    manager.getMessageService().dispatchResponseToThread(response);
  }

  protected void sendError(final OClientConnection connection, final int iClientTxId, final Throwable t) throws IOException {
    channel.acquireWriteLock();
    try {

      channel.writeByte(OChannelBinaryProtocol.RESPONSE_STATUS_ERROR);
      channel.writeInt(iClientTxId);
      if (tokenConnection && requestType != OChannelBinaryProtocol.REQUEST_CONNECT && (
          requestType != OChannelBinaryProtocol.REQUEST_DB_OPEN && requestType != OChannelBinaryProtocol.REQUEST_SHUTDOWN || (
              connection != null && connection.getData() != null
                  && connection.getData().protocolVersion <= OChannelBinaryProtocol.PROTOCOL_VERSION_32))
          || requestType == OChannelBinaryProtocol.REQUEST_DB_REOPEN) {
        // TODO: Check if the token is expiring and if it is send a new token

        if (connection != null && connection.getToken() != null) {
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

  protected void shutdownConnection(OClientConnection connection) throws IOException {
    setDataCommandInfo(connection, "Shutdowning");

    OLogManager.instance().info(this, "Received shutdown command from the remote client %s:%d", channel.socket.getInetAddress(),
        channel.socket.getPort());

    final String user = channel.readString();
    final String passwd = channel.readString();

    if (server.authenticate(user, passwd, "server.shutdown")) {
      OLogManager.instance()
          .info(this, "Remote client %s:%d authenticated. Starting shutdown of server...", channel.socket.getInetAddress(),
              channel.socket.getPort());

      beginResponse();
      try {
        sendOk(connection, clientTxId);
      } finally {
        endResponse(connection);
      }
      runShutdownInNonDaemonThread();
      return;
    }

    OLogManager.instance()
        .error(this, "Authentication error of remote client %s:%d: shutdown is aborted.", channel.socket.getInetAddress(),
            channel.socket.getPort());

    sendErrorOrDropConnection(connection, clientTxId, new OSecurityAccessException("Invalid user/password to shutdown the server"));
  }

  protected void distributedCluster(OClientConnection connection) throws IOException {
    setDataCommandInfo(connection, "Cluster status");

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

    sendResponse(connection, response);
  }

  protected void countDatabaseRecords(OClientConnection connection) throws IOException {
    setDataCommandInfo(connection, "Database count records");

    if (!isConnectionAlive(connection))
      return;

    beginResponse();
    try {
      sendOk(connection, clientTxId);
      channel.writeLong(connection.getDatabase().getStorage().countRecords());
    } finally {
      endResponse(connection);
    }
  }

  protected void sizeDatabase(OClientConnection connection) throws IOException {
    setDataCommandInfo(connection, "Database size");

    if (!isConnectionAlive(connection))
      return;

    beginResponse();
    try {
      sendOk(connection, clientTxId);
      channel.writeLong(connection.getDatabase().getStorage().getSize());
    } finally {
      endResponse(connection);
    }
  }

  protected void dropDatabase(OClientConnection connection) throws IOException {
    setDataCommandInfo(connection, "Drop database");
    String dbName = channel.readString();

    String storageType = null;
    storageType = channel.readString();

    if (storageType == null)
      storageType = "plocal";

    checkServerAccess("database.drop", connection);

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
      sendOk(connection, clientTxId);
    } finally {
      endResponse(connection);
    }
  }

  protected void existsDatabase(OClientConnection connection) throws IOException {
    setDataCommandInfo(connection, "Exists database");
    final String dbName = channel.readString();

    String storageType = null;
    storageType = channel.readString();

    if (storageType == null)
      storageType = "plocal";

    checkServerAccess("database.exists", connection);

    boolean result = false;
    ODatabaseDocumentInternal database;

    database = getDatabaseInstance(dbName, ODatabaseDocument.TYPE, storageType);
    if (database.exists())
      result = true;
    else
      Orient.instance().unregisterStorage(database.getStorage());

    beginResponse();
    try {
      sendOk(connection, clientTxId);
      channel.writeByte((byte) (result ? 1 : 0));
    } finally {
      endResponse(connection);
    }
  }

  protected void createDatabase(OClientConnection connection) throws IOException {
    setDataCommandInfo(connection, "Create database");

    String dbName = channel.readString();
    String dbType = ODatabaseDocument.TYPE;
    // READ DB-TYPE FROM THE CLIENT
    dbType = channel.readString();
    String storageType = channel.readString();
    String backupPath = null;

    if (connection.getData().protocolVersion > 35) {
      backupPath = channel.readString();
    }

    checkServerAccess("database.create", connection);
    checkStorageExistence(dbName);
    connection.setDatabase(getDatabaseInstance(dbName, dbType, storageType));

    createDatabase(connection.getDatabase(), null, null, backupPath);

    beginResponse();
    try {
      sendOk(connection, clientTxId);
    } finally {
      endResponse(connection);
    }
  }

  protected void closeDatabase(OClientConnection connection) throws IOException {
    setDataCommandInfo(connection, "Close Database");

    if (connection != null) {
      server.getClientConnectionManager().disconnect(connection);
    }
  }

  protected void configList(OClientConnection connection) throws IOException {
    setDataCommandInfo(connection, "List config");

    checkServerAccess("server.config.get", connection);

    beginResponse();
    try {
      sendOk(connection, clientTxId);

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
      endResponse(connection);
    }
  }

  protected void configSet(OClientConnection connection) throws IOException {
    setDataCommandInfo(connection, "Set config");

    checkServerAccess("server.config.set", connection);

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
      sendOk(connection, clientTxId);
    } finally {
      endResponse(connection);
    }
  }

  protected void configGet(OClientConnection connection) throws IOException {
    setDataCommandInfo(connection, "Get config");

    checkServerAccess("server.config.get", connection);

    final String key = channel.readString();
    final OGlobalConfiguration cfg = OGlobalConfiguration.findByKey(key);
    String cfgValue = cfg != null ? cfg.isHidden() ? "<hidden>" : cfg.getValueAsString() : "";

    beginResponse();
    try {
      sendOk(connection, clientTxId);
      channel.writeString(cfgValue);
    } finally {
      endResponse(connection);
    }
  }

  protected void commit(OClientConnection connection) throws IOException {
    setDataCommandInfo(connection, "Transaction commit");

    if (!isConnectionAlive(connection))
      return;

    final OTransactionOptimisticProxy tx = new OTransactionOptimisticProxy(connection, this);

    try {
      try {
        connection.getDatabase().begin(tx);
      } catch (final ORecordNotFoundException e) {
        sendShutdown();
        throw e.getCause() instanceof OOfflineClusterException ? (OOfflineClusterException) e.getCause() : e;
      }

      try {
        try {
          connection.getDatabase().commit();
        } catch (final ORecordNotFoundException e) {
          throw e.getCause() instanceof OOfflineClusterException ? (OOfflineClusterException) e.getCause() : e;
        }
        beginResponse();
        try {
          sendOk(connection, clientTxId);

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
            sendCollectionChanges(connection);
        } finally {
          endResponse(connection);
        }
      } catch (Exception e) {
        if (connection != null && connection.getDatabase() != null) {
          if (connection.getDatabase().getTransaction().isActive())
            connection.getDatabase().rollback(true);

          final OSBTreeCollectionManager collectionManager = connection.getDatabase().getSbTreeCollectionManager();
          if (collectionManager != null)
            collectionManager.clearChangedIds();
        }

        sendErrorOrDropConnection(connection, clientTxId, e);
      }
    } catch (OTransactionAbortedException e) {
      // TX ABORTED BY THE CLIENT
    } catch (Exception e) {
      // Error during TX initialization, possibly index constraints violation.
      if (tx.isActive())
        tx.rollback(true, -1);

      sendErrorOrDropConnection(connection, clientTxId, e);
    }
  }

  protected void command(OClientConnection connection) throws IOException {
    setDataCommandInfo(connection, "Execute remote command");

    byte type = channel.readByte();
    final boolean live = type == 'l';
    final boolean asynch = type == 'a';
    if (connection == null && connection.getDatabase() == null)
      throw new IOException("Found invalid session");

    String dbSerializerName = connection.getDatabase().getSerializer().toString();
    String name = getRecordSerializerName(connection);

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
        liveListener = new OLiveCommandResultListener(server, connection, clientTxId, cmdResultListener);
        listener = new OSyncCommandResultListener(null);
        command.setResultListener(liveListener);
      } else if (asynch) {
        // IF COMMAND CACHE IS ENABLED, RESULT MUST BE COLLECTED
        final OCommandCache cmdCache = connection.getDatabase().getMetadata().getCommandCache();
        if (cmdCache.isEnabled())
          // CREATE E COLLECTOR OF RESULT IN RAM TO CACHE THE RESULT
          cmdResultListener = new OCommandCacheRemoteResultListener(cmdResultListener, cmdCache);

        listener = new OAsyncCommandResultListener(connection, this, clientTxId, cmdResultListener);
        command.setResultListener(listener);
      } else {
        listener = new OSyncCommandResultListener(null);
      }

      final long serverTimeout = OGlobalConfiguration.COMMAND_TIMEOUT.getValueAsLong();

      if (serverTimeout > 0 && command.getTimeoutTime() > serverTimeout)
        // FORCE THE SERVER'S TIMEOUT
        command.setTimeout(serverTimeout, command.getTimeoutStrategy());

      if (!isConnectionAlive(connection))
        return;

      // REQUEST CAN'T MODIFY THE RESULT, SO IT'S CACHEABLE
      command.setCacheableResult(true);

      // ASSIGNED THE PARSED FETCHPLAN
      final OCommandRequest commandImpl = connection.getDatabase().command(command);
      listener.setFetchPlan(commandImpl.getFetchPlan());

      final Object result;
      if (params == null)
        result = commandImpl.execute();
      else
        result = commandImpl.execute(params);

      // FETCHPLAN HAS TO BE ASSIGNED AGAIN, because it can be changed by SQL statement
      listener.setFetchPlan(commandImpl.getFetchPlan());

      if (asynch) {
        // ASYNCHRONOUS
        if (listener.isEmpty())
          try {
            sendOk(connection, clientTxId);
          } catch (IOException ignored) {
          }
        channel.writeByte((byte) 0); // NO MORE RECORDS

      } else {
        // SYNCHRONOUS
        sendOk(connection, clientTxId);

        boolean isRecordResultSet = true;
        if (command instanceof OCommandRequestInternal)
          isRecordResultSet = command.isRecordResultSet();

        serializeValue(connection, listener, result, false, isRecordResultSet);

        if (listener instanceof OSyncCommandResultListener) {
          // SEND FETCHED RECORDS TO LOAD IN CLIENT CACHE
          for (ORecord rec : ((OSyncCommandResultListener) listener).getFetchedRecordsToSend()) {
            channel.writeByte((byte) 2); // CLIENT CACHE RECORD. IT
            // ISN'T PART OF THE
            // RESULT SET
            writeIdentifiable(connection, rec);
          }

          channel.writeByte((byte) 0); // NO MORE RECORDS
        }
      }

    } finally {
      connection.getData().command = null;
      endResponse(connection);
    }
  }

  public void serializeValue(final OClientConnection connection, final OAbstractCommandResultListener listener, Object result,
      boolean load, boolean isRecordResultSet) throws IOException {
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
      writeIdentifiable(connection, (OIdentifiable) result);
    } else if (result instanceof ODocumentWrapper) {
      // RECORD
      channel.writeByte((byte) 'r');
      final ODocument doc = ((ODocumentWrapper) result).getDocument();
      if (listener != null)
        listener.result(doc);
      writeIdentifiable(connection, doc);
    } else if (!isRecordResultSet) {
      writeSimpleValue(connection, listener, result);
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

          writeIdentifiable(connection, (OIdentifiable) o);
        } catch (Exception e) {
          OLogManager.instance().warn(this, "Cannot serialize record: " + o);
          // WRITE NULL RECORD TO AVOID BREAKING PROTOCOL
          writeIdentifiable(connection, null);
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
            writeIdentifiable(connection, (OIdentifiable) o);
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

            writeIdentifiable(connection, (OIdentifiable) o);
          } catch (Exception e) {
            OLogManager.instance().warn(this, "Cannot serialize record: " + o);
          }
        }
      }

    } else {
      // ANY OTHER (INCLUDING LITERALS)
      writeSimpleValue(connection, listener, result);
    }
  }

  private void writeSimpleValue(OClientConnection connection, OAbstractCommandResultListener listener, Object result)
      throws IOException {

    if (connection.getData().protocolVersion >= OChannelBinaryProtocol.PROTOCOL_VERSION_35) {
      channel.writeByte((byte) 'w');
      ODocument document = new ODocument();
      document.field("result", result);
      writeIdentifiable(connection, document);
      if (listener != null)
        listener.linkdedBySimpleValue(document);
    } else {
      channel.writeByte((byte) 'a');
      final StringBuilder value = new StringBuilder(64);
      if (listener != null) {
        ODocument document = new ODocument();
        document.field("result", result);
        listener.linkdedBySimpleValue(document);
      }
      ORecordSerializerStringAbstract.fieldTypeToString(value, OType.getTypeByClass(result.getClass()), result);
      channel.writeString(value.toString());
    }
  }

  protected void deleteRecord(OClientConnection connection) throws IOException {
    setDataCommandInfo(connection, "Delete record");

    if (!isConnectionAlive(connection))
      return;

    final ORID rid = channel.readRID();
    final int version = channel.readVersion();
    final byte mode = channel.readByte();

    final int result = deleteRecord(connection.getDatabase(), rid, version);

    if (mode < 2) {
      beginResponse();
      try {
        sendOk(connection, clientTxId);
        channel.writeByte((byte) result);
      } finally {
        endResponse(connection);
      }
    }
  }

  protected void hideRecord(OClientConnection connection) throws IOException {
    setDataCommandInfo(connection, "Hide record");

    if (!isConnectionAlive(connection))
      return;

    final ORID rid = channel.readRID();
    final byte mode = channel.readByte();

    final int result = hideRecord(connection.getDatabase(), rid);

    if (mode < 2) {
      beginResponse();
      try {
        sendOk(connection, clientTxId);
        channel.writeByte((byte) result);
      } finally {
        endResponse(connection);
      }
    }
  }

  protected void cleanOutRecord(OClientConnection connection) throws IOException {
    setDataCommandInfo(connection, "Clean out record");

    if (!isConnectionAlive(connection))
      return;

    final ORID rid = channel.readRID();
    final int version = channel.readVersion();
    final byte mode = channel.readByte();

    final int result = cleanOutRecord(connection.getDatabase(), rid, version);

    if (mode < 2) {
      beginResponse();
      try {
        sendOk(connection, clientTxId);
        channel.writeByte((byte) result);
      } finally {
        endResponse(connection);
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
   * @param connection
   *
   * @throws IOException
   */
  protected void updateRecord(OClientConnection connection) throws IOException {
    setDataCommandInfo(connection, "Update record");

    if (!isConnectionAlive(connection))
      return;

    final ORecordId rid = channel.readRID();
    boolean updateContent = true;
    if (connection.getData().protocolVersion >= 23)
      updateContent = channel.readBoolean();
    final byte[] buffer = channel.readBytes();
    final int version = channel.readVersion();
    final byte recordType = channel.readByte();
    final byte mode = channel.readByte();

    final int newVersion = updateRecord(connection, rid, buffer, version, recordType, updateContent);

    if (mode < 2) {
      beginResponse();
      try {
        sendOk(connection, clientTxId);
        channel.writeVersion(newVersion);

        if (connection.getData().protocolVersion >= 20)
          sendCollectionChanges(connection);
      } finally {
        endResponse(connection);
      }
    }
  }

  protected void createRecord(OClientConnection connection) throws IOException {
    setDataCommandInfo(connection, "Create record");

    if (!isConnectionAlive(connection))
      return;

    final int dataSegmentId = connection.getData().protocolVersion < 24 ? channel.readInt() : 0;

    final ORecordId rid = new ORecordId(channel.readShort(), ORID.CLUSTER_POS_INVALID);
    final byte[] buffer = channel.readBytes();
    final byte recordType = channel.readByte();
    final byte mode = channel.readByte();

    final ORecord record = createRecord(connection, rid, buffer, recordType);

    if (mode < 2) {
      beginResponse();
      try {
        sendOk(connection, clientTxId);
        if (connection.getData().protocolVersion > OChannelBinaryProtocol.PROTOCOL_VERSION_25)
          channel.writeShort((short) record.getIdentity().getClusterId());
        channel.writeLong(record.getIdentity().getClusterPosition());
        channel.writeVersion(record.getVersion());

        if (connection.getData().protocolVersion >= 20)
          sendCollectionChanges(connection);
      } finally {
        endResponse(connection);
      }
    }
  }

  protected void readRecordMetadata(OClientConnection connection) throws IOException {
    setDataCommandInfo(connection, "Record metadata");

    final ORID rid = channel.readRID();

    beginResponse();
    try {
      final ORecordMetadata metadata = connection.getDatabase().getRecordMetadata(rid);
      if (metadata != null) {
        sendOk(connection, clientTxId);
        channel.writeRID(metadata.getRecordId());
        channel.writeVersion(metadata.getVersion());
      } else {
        throw new ODatabaseException(String.format("Record metadata for RID: %s, Not found", rid));
      }
    } finally {
      endResponse(connection);
    }
  }

  protected void readRecord(final OClientConnection connection) throws IOException {
    setDataCommandInfo(connection, "Load record");

    if (!isConnectionAlive(connection))
      return;

    final ORecordId rid = channel.readRID();
    final String fetchPlanString = channel.readString();
    boolean ignoreCache = false;
    ignoreCache = channel.readByte() == 1;

    boolean loadTombstones = false;
    loadTombstones = channel.readByte() > 0;

    if (rid.getClusterId() == 0 && rid.getClusterPosition() == 0) {
      // @COMPATIBILITY 0.9.25
      // SEND THE DB CONFIGURATION INSTEAD SINCE IT WAS ON RECORD 0:0
      OFetchHelper.checkFetchPlanValid(fetchPlanString);

      beginResponse();
      try {
        sendOk(connection, clientTxId);
        channel.writeByte((byte) 1);

        final byte[] storageStream = connection.getDatabase().getStorage().callInLock(new Callable<byte[]>() {
          @Override
          public byte[] call() throws Exception {
            return connection.getDatabase().getStorage().getConfiguration().toStream(connection.getData().protocolVersion);
          }
        }, false);

        if (connection.getData().protocolVersion <= OChannelBinaryProtocol.PROTOCOL_VERSION_27) {
          channel.writeBytes(storageStream);
          channel.writeVersion(0);
          channel.writeByte(OBlob.RECORD_TYPE);
        } else {
          channel.writeByte(OBlob.RECORD_TYPE);
          channel.writeVersion(0);
          channel.writeBytes(storageStream);
        }
        channel.writeByte((byte) 0); // NO MORE RECORDS
      } finally {
        endResponse(connection);
      }

    } else {
      final ORecord record = connection.getDatabase()
          .load(rid, fetchPlanString, ignoreCache, loadTombstones, OStorage.LOCKING_STRATEGY.NONE);

      beginResponse();
      try {
        sendOk(connection, clientTxId);

        if (record != null) {
          channel.writeByte((byte) 1); // HAS RECORD
          byte[] bytes = getRecordBytes(connection, record);
          int length = trimCsvSerializedContent(connection, bytes);
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
                  writeIdentifiable(connection, d);
                }
              }
            }

          }
        }
        channel.writeByte((byte) 0); // NO MORE RECORDS

      } finally {
        endResponse(connection);
      }
    }
  }

  protected void readRecordIfVersionIsNotLatest(final OClientConnection connection) throws IOException {
    setDataCommandInfo(connection, "Load record if version is not latest");

    if (!isConnectionAlive(connection))
      return;

    final ORecordId rid = channel.readRID();
    final int recordVersion = channel.readVersion();
    final String fetchPlanString = channel.readString();

    boolean ignoreCache = channel.readByte() == 1;

    if (rid.getClusterId() == 0 && rid.getClusterPosition() == 0) {
      // @COMPATIBILITY 0.9.25
      // SEND THE DB CONFIGURATION INSTEAD SINCE IT WAS ON RECORD 0:0
      OFetchHelper.checkFetchPlanValid(fetchPlanString);

      beginResponse();
      try {
        sendOk(connection, clientTxId);
        channel.writeByte((byte) 1);

        final byte[] storageStream = connection.getDatabase().getStorage().callInLock(new Callable<byte[]>() {
          @Override
          public byte[] call() throws Exception {
            return connection.getDatabase().getStorage().getConfiguration().toStream(connection.getData().protocolVersion);
          }
        }, false);

        if (connection.getData().protocolVersion <= OChannelBinaryProtocol.PROTOCOL_VERSION_27) {
          channel.writeBytes(storageStream);
          channel.writeVersion(0);
          channel.writeByte(OBlob.RECORD_TYPE);
        } else {
          channel.writeByte(OBlob.RECORD_TYPE);
          channel.writeVersion(0);
          channel.writeBytes(storageStream);
        }
        channel.writeByte((byte) 0); // NO MORE RECORDS
      } finally {
        endResponse(connection);
      }

    } else {
      final ORecord record = connection.getDatabase().loadIfVersionIsNotLatest(rid, recordVersion, fetchPlanString, ignoreCache);

      beginResponse();
      try {
        sendOk(connection, clientTxId);

        if (record != null) {
          channel.writeByte((byte) 1); // HAS RECORD
          byte[] bytes = getRecordBytes(connection, record);
          int length = trimCsvSerializedContent(connection, bytes);

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
                  writeIdentifiable(connection, d);
                }
              }
            }

          }
        }
        channel.writeByte((byte) 0); // NO MORE RECORDS

      } finally {
        endResponse(connection);
      }
    }
  }

  protected void beginResponse() {
    channel.acquireWriteLock();
  }

  protected void endResponse(OClientConnection connection) throws IOException {
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

  protected void setDataCommandInfo(OClientConnection connection, final String iCommandInfo) {
    if (connection != null)
      connection.getData().commandInfo = iCommandInfo;
  }

  protected void readConnectionData(OClientConnection connection) throws IOException {
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
    tokenConnection = Boolean.TRUE.equals(connection.getTokenBased());
    if (connection.getData().protocolVersion > OChannelBinaryProtocol.PROTOCOL_VERSION_33) {
      connection.getData().supportsPushMessages = channel.readBoolean();
      connection.getData().collectStats = channel.readBoolean();
    } else {
      connection.getData().supportsPushMessages = true;
      connection.getData().collectStats = true;
    }
  }

  protected void sendOk(OClientConnection connection, final int iClientTxId) throws IOException {
    channel.writeByte(OChannelBinaryProtocol.RESPONSE_STATUS_OK);
    channel.writeInt(iClientTxId);
    okSent = true;
    if (connection != null && Boolean.TRUE.equals(connection.getTokenBased()) && connection.getToken() != null
        && requestType != OChannelBinaryProtocol.REQUEST_CONNECT && requestType != OChannelBinaryProtocol.REQUEST_DB_OPEN) {
      // TODO: Check if the token is expiring and if it is send a new token
      byte[] renewedToken = server.getTokenHandler().renewIfNeeded(connection.getToken());
      channel.writeBytes(renewedToken);
    }
  }

  protected void handleConnectionError(OClientConnection connection, final Throwable e) {
    try {
      channel.flush();
    } catch (IOException e1) {
      OLogManager.instance().debug(this, "Error during channel flush", e1);
    }
    OLogManager.instance().error(this, "Error executing request", e);
    OServerPluginHelper.invokeHandlerCallbackOnClientError(server, connection, e);
  }

  protected void sendResponse(OClientConnection connection, final ODocument iResponse) throws IOException {
    beginResponse();
    try {
      sendOk(connection, clientTxId);
      byte[] bytes = null;
      if (iResponse != null)
        bytes = getRecordBytes(connection, iResponse);
      channel.writeBytes(bytes);
    } finally {
      endResponse(connection);
    }
  }

  protected void freezeDatabase(OClientConnection connection) throws IOException {
    setDataCommandInfo(connection, "Freeze database");
    String dbName = channel.readString();

    checkServerAccess("database.freeze", connection);

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
      sendOk(connection, clientTxId);
    } finally {
      endResponse(connection);
    }
  }

  protected void releaseDatabase(OClientConnection connection) throws IOException {
    setDataCommandInfo(connection, "Release database");
    String dbName = channel.readString();

    checkServerAccess("database.release", connection);

    String storageType = null;
    storageType = channel.readString();

    if (storageType == null)
      storageType = "plocal";

    connection.setDatabase(getDatabaseInstance(dbName, ODatabaseDocument.TYPE, storageType));

    if (connection.getDatabase().exists()) {
      OLogManager.instance().info(this, "Releasing database '%s'", connection.getDatabase().getURL());

      if (connection.getDatabase().isClosed())
        server.openDatabaseBypassingSecurity(connection.getDatabase(), connection.getData(), connection.getServerUser().name);

      connection.getDatabase().release();
    } else {
      throw new OStorageException("Database with name '" + dbName + "' does not exist");
    }

    beginResponse();
    try {
      sendOk(connection, clientTxId);
    } finally {
      endResponse(connection);
    }
  }

  public String getRecordSerializerName(OClientConnection connection) {
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
   * <p/>
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
      Thread.currentThread().interrupt();
    }
  }

  private void ridBagSize(OClientConnection connection) throws IOException {
    setDataCommandInfo(connection, "RidBag get size");

    OBonsaiCollectionPointer collectionPointer = OCollectionNetworkSerializer.INSTANCE.readCollectionPointer(channel);
    final byte[] changeStream = channel.readBytes();

    final OSBTreeCollectionManager sbTreeCollectionManager = connection.getDatabase().getSbTreeCollectionManager();
    final OSBTreeBonsai<OIdentifiable, Integer> tree = sbTreeCollectionManager.loadSBTree(collectionPointer);

    if (tree == null)
      throw new ORecordContentNotFoundException(collectionPointer);

    try {
      final Map<OIdentifiable, OSBTreeRidBag.Change> changes = OSBTreeRidBag.ChangeSerializationHelper.INSTANCE
          .deserializeChanges(changeStream, 0);

      int realSize = tree.getRealBagSize(changes);

      beginResponse();
      try {
        sendOk(connection, clientTxId);
        channel.writeInt(realSize);
      } finally {
        endResponse(connection);
      }
    } finally {
      sbTreeCollectionManager.releaseSBTree(collectionPointer);
    }
  }

  private void sbTreeBonsaiGetEntriesMajor(OClientConnection connection) throws IOException {
    setDataCommandInfo(connection, "SB-Tree bonsai get values major");

    OBonsaiCollectionPointer collectionPointer = OCollectionNetworkSerializer.INSTANCE.readCollectionPointer(channel);
    byte[] keyStream = channel.readBytes();
    boolean inclusive = channel.readBoolean();
    int pageSize = 128;

    if (connection.getData().protocolVersion >= 21)
      pageSize = channel.readInt();

    final OSBTreeCollectionManager sbTreeCollectionManager = connection.getDatabase().getSbTreeCollectionManager();
    final OSBTreeBonsai<OIdentifiable, Integer> tree = sbTreeCollectionManager.loadSBTree(collectionPointer);

    if (tree == null)
      throw new ORecordContentNotFoundException(collectionPointer);

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
        sendOk(connection, clientTxId);
        channel.writeBytes(stream);
      } finally {
        endResponse(connection);
      }
    } finally {
      sbTreeCollectionManager.releaseSBTree(collectionPointer);
    }
  }

  private byte[] serializeSBTreeEntryCollection(List<Entry<OIdentifiable, Integer>> collection,
      OBinarySerializer<OIdentifiable> keySerializer, OBinarySerializer<Integer> valueSerializer) {
    byte[] stream = new byte[OIntegerSerializer.INT_SIZE + collection.size() * (keySerializer.getFixedLength() + valueSerializer
        .getFixedLength())];
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

  private void sbTreeBonsaiFirstKey(OClientConnection connection) throws IOException {
    setDataCommandInfo(connection, "SB-Tree bonsai get first key");

    OBonsaiCollectionPointer collectionPointer = OCollectionNetworkSerializer.INSTANCE.readCollectionPointer(channel);

    final OSBTreeCollectionManager sbTreeCollectionManager = connection.getDatabase().getSbTreeCollectionManager();
    final OSBTreeBonsai<OIdentifiable, Integer> tree = sbTreeCollectionManager.loadSBTree(collectionPointer);

    if (tree == null)
      throw new ORecordContentNotFoundException(collectionPointer);

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
        sendOk(connection, clientTxId);
        channel.writeBytes(stream);
      } finally {
        endResponse(connection);
      }
    } finally {
      sbTreeCollectionManager.releaseSBTree(collectionPointer);
    }
  }

  private void sbTreeBonsaiGet(OClientConnection connection) throws IOException {
    setDataCommandInfo(connection, "SB-Tree bonsai get");

    OBonsaiCollectionPointer collectionPointer = OCollectionNetworkSerializer.INSTANCE.readCollectionPointer(channel);
    final byte[] keyStream = channel.readBytes();

    final OSBTreeCollectionManager sbTreeCollectionManager = connection.getDatabase().getSbTreeCollectionManager();
    final OSBTreeBonsai<OIdentifiable, Integer> tree = sbTreeCollectionManager.loadSBTree(collectionPointer);

    if (tree == null)
      throw new ORecordContentNotFoundException(collectionPointer);

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
        sendOk(connection, clientTxId);
        channel.writeBytes(stream);
      } finally {
        endResponse(connection);
      }
    } finally {
      sbTreeCollectionManager.releaseSBTree(collectionPointer);
    }
  }

  private void createSBTreeBonsai(OClientConnection connection) throws IOException {
    setDataCommandInfo(connection, "Create SB-Tree bonsai instance");

    int clusterId = channel.readInt();

    OBonsaiCollectionPointer collectionPointer = connection.getDatabase().getSbTreeCollectionManager()
        .createSBTree(clusterId, null);

    beginResponse();
    try {
      sendOk(connection, clientTxId);
      OCollectionNetworkSerializer.INSTANCE.writeCollectionPointer(channel, collectionPointer);
    } finally {
      endResponse(connection);
    }
  }

  private void lowerPositions(OClientConnection connection) throws IOException {
    setDataCommandInfo(connection, "Retrieve lower positions");

    final int clusterId = channel.readInt();
    final long clusterPosition = channel.readLong();

    beginResponse();
    try {
      sendOk(connection, clientTxId);

      final OPhysicalPosition[] previousPositions = connection.getDatabase().getStorage()
          .lowerPhysicalPositions(clusterId, new OPhysicalPosition(clusterPosition));

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
      endResponse(connection);
    }
  }

  private void floorPositions(OClientConnection connection) throws IOException {
    setDataCommandInfo(connection, "Retrieve floor positions");

    final int clusterId = channel.readInt();
    final long clusterPosition = channel.readLong();

    beginResponse();
    try {
      sendOk(connection, clientTxId);

      final OPhysicalPosition[] previousPositions = connection.getDatabase().getStorage()
          .floorPhysicalPositions(clusterId, new OPhysicalPosition(clusterPosition));

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
      endResponse(connection);
    }
  }

  private void higherPositions(OClientConnection connection) throws IOException {
    setDataCommandInfo(connection, "Retrieve higher positions");

    final int clusterId = channel.readInt();
    final long clusterPosition = channel.readLong();

    beginResponse();
    try {
      sendOk(connection, clientTxId);

      OPhysicalPosition[] nextPositions = connection.getDatabase().getStorage()
          .higherPhysicalPositions(clusterId, new OPhysicalPosition(clusterPosition));

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
      endResponse(connection);
    }
  }

  private void ceilingPositions(OClientConnection connection) throws IOException {
    setDataCommandInfo(connection, "Retrieve ceiling positions");

    final int clusterId = channel.readInt();
    final long clusterPosition = channel.readLong();

    beginResponse();
    try {
      sendOk(connection, clientTxId);

      final OPhysicalPosition[] previousPositions = connection.getDatabase().getStorage()
          .ceilingPhysicalPositions(clusterId, new OPhysicalPosition(clusterPosition));

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
      endResponse(connection);
    }
  }

  private boolean isConnectionAlive(OClientConnection connection) {
    if (connection == null || connection.getDatabase() == null) {
      // CONNECTION/DATABASE CLOSED, KILL IT
      server.getClientConnectionManager().kill(connection);
      return false;
    }
    return true;
  }

  private void sendCollectionChanges(OClientConnection connection) throws IOException {
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

  private void sendDatabaseInformation(OClientConnection connection) throws IOException {
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

  private void listDatabases(OClientConnection connection) throws IOException {
    checkServerAccess("server.listDatabases", connection);
    final ODocument result = new ODocument();
    result.field("databases", server.getAvailableStorageNames());

    setDataCommandInfo(connection, "List databases");

    beginResponse();
    try {
      sendOk(connection, clientTxId);
      byte[] stream = getRecordBytes(connection, result);
      channel.writeBytes(stream);
    } finally {
      endResponse(connection);
    }
  }

  private void serverInfo(OClientConnection connection) throws IOException {
    checkServerAccess("server.info", connection);

    setDataCommandInfo(connection, "Server Info");

    beginResponse();
    try {
      sendOk(connection, clientTxId);
      channel.writeString(OServerInfo.getServerInfo(server));
    } finally {
      endResponse(connection);
    }
  }

  private boolean loadUserFromSchema(OClientConnection connection, final String iUserName, final String iUserPassword) {
    connection.getDatabase().getMetadata().getSecurity().authenticate(iUserName, iUserPassword);
    return true;
  }

  @Override
  public int getVersion() {
    return OChannelBinaryProtocol.CURRENT_PROTOCOL_VERSION;
  }

  @Override
  public OChannelBinary getChannel() {
    return channel;
  }

  /**
   * Write a OIdentifiable instance using this format:<br>
   * - 2 bytes: class id [-2=no record, -3=rid, -1=no class id, > -1 = valid] <br>
   * - 1 byte: record type [d,b,f] <br>
   * - 2 bytes: cluster id <br>
   * - 8 bytes: position in cluster <br>
   * - 4 bytes: record version <br>
   * - x bytes: record content <br>
   *
   * @param connection
   * @param o
   *
   * @throws IOException
   */
  public void writeIdentifiable(OClientConnection connection, final OIdentifiable o) throws IOException {
    if (o == null)
      channel.writeShort(OChannelBinaryProtocol.RECORD_NULL);
    else if (o instanceof ORecordId) {
      channel.writeShort(OChannelBinaryProtocol.RECORD_RID);
      channel.writeRID((ORID) o);
    } else {
      writeRecord(connection, o.getRecord());
    }
  }

  public String getType() {
    return "binary";
  }

  public void fillRecord(OClientConnection connection, final ORecordId rid, final byte[] buffer, final int version,
      final ORecord record) {
    String dbSerializerName = "";
    if (connection.getDatabase() != null)
      dbSerializerName = connection.getDatabase().getSerializer().toString();

    String name = getRecordSerializerName(connection);
    if (ORecordInternal.getRecordType(record) == ODocument.RECORD_TYPE && !dbSerializerName.equals(name)) {
      ORecordInternal.fill(record, rid, version, null, true);
      ORecordSerializer ser = ORecordSerializerFactory.instance().getFormat(name);
      ser.fromStream(buffer, record, null);
      record.setDirty();
    } else
      ORecordInternal.fill(record, rid, version, buffer, true);
  }

  protected void sendErrorOrDropConnection(OClientConnection connection, final int iClientTxId, final Throwable t)
      throws IOException {
    if (okSent || requestType == OChannelBinaryProtocol.REQUEST_DB_CLOSE) {
      handleConnectionError(connection, t);
      sendShutdown();
    } else {
      okSent = true;
      sendError(connection, iClientTxId, t);
    }
  }

  protected void checkStorageExistence(final String iDatabaseName) {
    for (OStorage stg : Orient.instance().getStorages()) {
      if (!(stg instanceof OStorageProxy) && stg.getName().equalsIgnoreCase(iDatabaseName) && stg.exists())
        throw new ODatabaseException("Database named '" + iDatabaseName + "' already exists: " + stg);
    }
  }

  protected ODatabaseDocumentInternal createDatabase(final ODatabaseDocumentInternal iDatabase, String dbUser,
      final String dbPasswd, final String backupPath) {
    if (iDatabase.exists())
      throw new ODatabaseException("Database '" + iDatabase.getURL() + "' already exists");

    if (backupPath == null)
      iDatabase.create();
    else
      iDatabase.create(backupPath);

    if (dbUser != null) {

      OUser oUser = iDatabase.getMetadata().getSecurity().getUser(dbUser);
      if (oUser == null) {
        iDatabase.getMetadata().getSecurity().createUser(dbUser, dbPasswd, new String[] { ORole.ADMIN });
      } else {
        oUser.setPassword(dbPasswd);
        oUser.save();
      }
    }

    OLogManager.instance().info(this, "Created database '%s' of type '%s'", iDatabase.getName(),
        iDatabase.getStorage().getUnderlying() instanceof OAbstractPaginatedStorage ?
            iDatabase.getStorage().getUnderlying().getType() :
            "memory");

    // if (iDatabase.getStorage() instanceof OStorageLocal)
    // // CLOSE IT BECAUSE IT WILL BE OPEN AT FIRST USE
    // iDatabase.close();

    return iDatabase;
  }

  /**
   * Returns a database instance giving the database name, the database type and storage type.
   *
   * @param dbName
   * @param dbType
   * @param storageType Storage type between "plocal" or "memory".
   *
   * @return
   */
  protected ODatabaseDocumentInternal getDatabaseInstance(final String dbName, final String dbType, final String storageType) {
    String path;

    final OStorage stg = Orient.instance().getStorage(dbName);
    if (stg != null)
      path = stg.getURL();
    else if (storageType.equals(OEngineLocalPaginated.NAME)) {
      // if this storage was configured return always path from config file, otherwise return default path
      path = server.getConfiguration().getStoragePath(dbName);

      if (path == null)
        path = storageType + ":" + server.getDatabaseDirectory() + "/" + dbName;
    } else if (storageType.equals(OEngineMemory.NAME)) {
      path = storageType + ":" + dbName;
    } else
      throw new IllegalArgumentException("Cannot create database: storage mode '" + storageType + "' is not supported.");

    return new ODatabaseDocumentTx(path);
  }

  protected int deleteRecord(final ODatabaseDocument iDatabase, final ORID rid, final int version) {
    try {
      // TRY TO SEE IF THE RECORD EXISTS
      final ORecord record = rid.getRecord();
      if (record == null)
        return 0;

      iDatabase.delete(rid, version);
      return 1;
    } catch (ORecordNotFoundException e) {
      // MAINTAIN COHERENT THE BEHAVIOR FOR ALL THE STORAGE TYPES
      if (e.getCause() instanceof OOfflineClusterException)
        throw (OOfflineClusterException) e.getCause();
    } catch (OOfflineClusterException e) {
      throw e;
    } catch (Exception e) {
      // IGNORE IT
    }
    return 0;
  }

  protected int hideRecord(final ODatabaseDocument iDatabase, final ORID rid) {
    try {
      iDatabase.hide(rid);
      return 1;
    } catch (ORecordNotFoundException e) {
      return 0;
    }
  }

  protected int cleanOutRecord(final ODatabaseDocument iDatabase, final ORID rid, final int version) {
    iDatabase.cleanOutRecord(rid, version);
    return 1;
  }

  protected ORecord createRecord(OClientConnection connection, final ORecordId rid, final byte[] buffer, final byte recordType) {
    final ORecord record = Orient.instance().getRecordFactoryManager().newInstance(recordType);
    fillRecord(connection, rid, buffer, 0, record);
    if (record instanceof ODocument) {
      // Force conversion of value to class for trigger default values.
      ODocumentInternal.autoConvertValueToClass(connection.getDatabase(), (ODocument) record);
    }
    connection.getDatabase().save(record);
    return record;
  }

  protected int updateRecord(OClientConnection connection, final ORecordId rid, final byte[] buffer, final int version,
      final byte recordType, boolean updateContent) {
    ODatabaseDocumentInternal database = connection.getDatabase();
    final ORecord newRecord = Orient.instance().getRecordFactoryManager().newInstance(recordType);
    fillRecord(connection, rid, buffer, version, newRecord);

    ORecordInternal.setContentChanged(newRecord, updateContent);
    ORecordInternal.getDirtyManager(newRecord).clearForSave();
    ORecord currentRecord = null;
    if (newRecord instanceof ODocument) {
      try {
        currentRecord = database.load(rid);
      } catch (ORecordNotFoundException e) {
        // MAINTAIN COHERENT THE BEHAVIOR FOR ALL THE STORAGE TYPES
        if (e.getCause() instanceof OOfflineClusterException)
          throw (OOfflineClusterException) e.getCause();
      }

      if (currentRecord == null)
        throw new ORecordNotFoundException(rid);

      ((ODocument) currentRecord).merge((ODocument) newRecord, false, false);

    } else
      currentRecord = newRecord;

    ORecordInternal.setVersion(currentRecord, version);

    database.save(currentRecord);

    if (currentRecord.getIdentity().toString().equals(database.getStorage().getConfiguration().indexMgrRecordId) && !database
        .getStatus().equals(ODatabase.STATUS.IMPORTING)) {
      // FORCE INDEX MANAGER UPDATE. THIS HAPPENS FOR DIRECT CHANGES FROM REMOTE LIKE IN GRAPH
      database.getMetadata().getIndexManager().reload();
    }
    return currentRecord.getVersion();
  }

  public byte[] getRecordBytes(OClientConnection connection, final ORecord iRecord) {

    final byte[] stream;
    String dbSerializerName = null;
    if (ODatabaseRecordThreadLocal.INSTANCE.getIfDefined() != null)
      dbSerializerName = ((ODatabaseDocumentInternal) iRecord.getDatabase()).getSerializer().toString();
    String name = getRecordSerializerName(connection);
    if (ORecordInternal.getRecordType(iRecord) == ODocument.RECORD_TYPE && (dbSerializerName == null || !dbSerializerName
        .equals(name))) {
      ((ODocument) iRecord).deserializeFields();
      ORecordSerializer ser = ORecordSerializerFactory.instance().getFormat(name);
      stream = ser.toStream(iRecord, false);
    } else
      stream = iRecord.toStream();

    return stream;
  }

  private void writeRecord(OClientConnection connection, final ORecord iRecord) throws IOException {
    channel.writeShort((short) 0);
    channel.writeByte(ORecordInternal.getRecordType(iRecord));
    channel.writeRID(iRecord.getIdentity());
    channel.writeVersion(iRecord.getVersion());
    try {
      final byte[] stream = getRecordBytes(connection, iRecord);

      // TODO: This Logic should not be here provide an api in the Serializer if asked for trimmed content.
      int realLength = trimCsvSerializedContent(connection, stream);

      channel.writeBytes(stream, realLength);
    } catch (Exception e) {
      channel.writeBytes(null);
      final String message = "Error on unmarshalling record " + iRecord.getIdentity().toString() + " (" + e + ")";
      OLogManager.instance().error(this, message, e);

      throw OException.wrapException(new OSerializationException(message), e);
    }
  }

  protected int trimCsvSerializedContent(OClientConnection connection, final byte[] stream) {
    int realLength = stream.length;
    final ODatabaseDocumentInternal db = ODatabaseRecordThreadLocal.INSTANCE.getIfDefined();
    if (db != null && db instanceof ODatabaseDocument) {
      if (ORecordSerializerSchemaAware2CSV.NAME.equals(getRecordSerializerName(connection))) {
        // TRIM TAILING SPACES (DUE TO OVERSIZE)
        for (int i = stream.length - 1; i > -1; --i) {
          if (stream[i] == 32)
            --realLength;
          else
            break;
        }

      }
    }
    return realLength;
  }

  public int getRequestType() {
    return requestType;
  }

  public String getRemoteAddress() {
    final Socket socket = getChannel().socket;
    if (socket != null) {
      final InetSocketAddress remoteAddress = (InetSocketAddress) socket.getRemoteSocketAddress();
      return remoteAddress.getAddress().getHostAddress() + ":" + remoteAddress.getPort();
    }
    return null;
  }

  public void importDatabase(OClientConnection connection) throws IOException {
    setDataCommandInfo(connection, "Create record");

    if (!isConnectionAlive(connection))
      return;

    String options = channel.readString();
    String originalName = channel.readString();
    File file = File.createTempFile(connection.getDatabase().getName() + "import", originalName);
    FileOutputStream output = new FileOutputStream(file);
    byte[] bytes;
    while ((bytes = channel.readBytes()) != null) {
      output.write(bytes);
    }
    output.close();
    beginResponse();
    sendOk(connection, clientTxId);
    try {
      OLogManager.instance().info(this, "Starting database import");
      ODatabaseImport imp = new ODatabaseImport(connection.getDatabase(), file.getAbsolutePath(), new OCommandOutputListener() {
        @Override
        public void onMessage(String iText) {
          try {
            OLogManager.instance().debug(ONetworkProtocolBinary.this, iText);
            if (iText != null)
              channel.writeString(iText);
          } catch (IOException e) {
            OLogManager.instance().warn(ONetworkProtocolBinary.this, "Error sending import message \"%s\" to client", iText);
            OLogManager.instance().debug(ONetworkProtocolBinary.this, "Error sending import message ", e);
          }
        }
      });
      imp.setOptions(options);
      imp.importDatabase();
      imp.close();
      file.delete();
      channel.writeString(null);
    } finally {
      OLogManager.instance().info(this, "Database import finshed");
      endResponse(connection);
    }

  }

}
