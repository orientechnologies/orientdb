/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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
 *  * For more information: http://orientdb.com
 *
 */
package com.orientechnologies.orient.server.network.protocol.binary;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.logging.Level;

import com.orientechnologies.common.concur.lock.OInterruptedException;
import com.orientechnologies.common.concur.lock.OLockException;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.io.OIOException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.ONullSerializer;
import com.orientechnologies.common.util.OCommonConst;
import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.message.OAddClusterRequest;
import com.orientechnologies.orient.client.remote.message.OAddClusterResponse;
import com.orientechnologies.orient.client.remote.message.OBinaryProtocolHelper;
import com.orientechnologies.orient.client.remote.message.OCeilingPhysicalPositionsRequest;
import com.orientechnologies.orient.client.remote.message.OCeilingPhysicalPositionsResponse;
import com.orientechnologies.orient.client.remote.message.OCleanOutRecordRequest;
import com.orientechnologies.orient.client.remote.message.OCleanOutRecordResponse;
import com.orientechnologies.orient.client.remote.message.OCommandRequest;
import com.orientechnologies.orient.client.remote.message.OCommandResponseServer;
import com.orientechnologies.orient.client.remote.message.OCommitRequest;
import com.orientechnologies.orient.client.remote.message.OCommitResponse;
import com.orientechnologies.orient.client.remote.message.OCommitResponse.OCreatedRecordResponse;
import com.orientechnologies.orient.client.remote.message.OCommitResponse.OUpdatedRecordResponse;
import com.orientechnologies.orient.client.remote.message.OConnectRequest;
import com.orientechnologies.orient.client.remote.message.OConnectResponse;
import com.orientechnologies.orient.client.remote.message.OCountRecordsResponse;
import com.orientechnologies.orient.client.remote.message.OCountRequest;
import com.orientechnologies.orient.client.remote.message.OCountResponse;
import com.orientechnologies.orient.client.remote.message.OCreateDatabaseRequest;
import com.orientechnologies.orient.client.remote.message.OCreateDatabaseResponse;
import com.orientechnologies.orient.client.remote.message.OCreateRecordRequest;
import com.orientechnologies.orient.client.remote.message.OCreateRecordResponse;
import com.orientechnologies.orient.client.remote.message.ODeleteRecordRequest;
import com.orientechnologies.orient.client.remote.message.ODeleteRecordResponse;
import com.orientechnologies.orient.client.remote.message.ODistributedStatusRequest;
import com.orientechnologies.orient.client.remote.message.ODistributedStatusResponse;
import com.orientechnologies.orient.client.remote.message.ODropClusterRequest;
import com.orientechnologies.orient.client.remote.message.ODropClusterResponse;
import com.orientechnologies.orient.client.remote.message.ODropDatabaseRequest;
import com.orientechnologies.orient.client.remote.message.ODropDatabaseResponse;
import com.orientechnologies.orient.client.remote.message.OErrorResponse;
import com.orientechnologies.orient.client.remote.message.OExistsDatabaseRequest;
import com.orientechnologies.orient.client.remote.message.OExistsDatabaseResponse;
import com.orientechnologies.orient.client.remote.message.OFloorPhysicalPositionsRequest;
import com.orientechnologies.orient.client.remote.message.OFloorPhysicalPositionsResponse;
import com.orientechnologies.orient.client.remote.message.OFreezeDatabaseRequest;
import com.orientechnologies.orient.client.remote.message.OFreezeDatabaseResponse;
import com.orientechnologies.orient.client.remote.message.OGetClusterDataRangeRequest;
import com.orientechnologies.orient.client.remote.message.OGetClusterDataRangeResponse;
import com.orientechnologies.orient.client.remote.message.OGetGlobalConfigurationRequest;
import com.orientechnologies.orient.client.remote.message.OGetGlobalConfigurationResponse;
import com.orientechnologies.orient.client.remote.message.OGetGlobalConfigurationsRequest;
import com.orientechnologies.orient.client.remote.message.OGetGlobalConfigurationsResponse;
import com.orientechnologies.orient.client.remote.message.OGetRecordMetadataRequest;
import com.orientechnologies.orient.client.remote.message.OGetRecordMetadataResponse;
import com.orientechnologies.orient.client.remote.message.OGetServerInfoRequest;
import com.orientechnologies.orient.client.remote.message.OGetServerInfoResponse;
import com.orientechnologies.orient.client.remote.message.OGetSizeResponse;
import com.orientechnologies.orient.client.remote.message.OHideRecordRequest;
import com.orientechnologies.orient.client.remote.message.OHideRecordResponse;
import com.orientechnologies.orient.client.remote.message.OHigherPhysicalPositionsRequest;
import com.orientechnologies.orient.client.remote.message.OHigherPhysicalPositionsResponse;
import com.orientechnologies.orient.client.remote.message.OImportRequest;
import com.orientechnologies.orient.client.remote.message.OImportResponse;
import com.orientechnologies.orient.client.remote.message.OIncrementalBackupRequest;
import com.orientechnologies.orient.client.remote.message.OIncrementalBackupResponse;
import com.orientechnologies.orient.client.remote.message.OListDatabasesReponse;
import com.orientechnologies.orient.client.remote.message.OListDatabasesRequest;
import com.orientechnologies.orient.client.remote.message.OLowerPhysicalPositionsRequest;
import com.orientechnologies.orient.client.remote.message.OLowerPhysicalPositionsResponse;
import com.orientechnologies.orient.client.remote.message.OOpenRequest;
import com.orientechnologies.orient.client.remote.message.OOpenResponse;
import com.orientechnologies.orient.client.remote.message.OReadRecordIfVersionIsNotLatestRequest;
import com.orientechnologies.orient.client.remote.message.OReadRecordIfVersionIsNotLatestResponse;
import com.orientechnologies.orient.client.remote.message.OReadRecordRequest;
import com.orientechnologies.orient.client.remote.message.OReadRecordResponse;
import com.orientechnologies.orient.client.remote.message.OReleaseDatabaseRequest;
import com.orientechnologies.orient.client.remote.message.OReloadResponse;
import com.orientechnologies.orient.client.remote.message.OReopenResponse;
import com.orientechnologies.orient.client.remote.message.OSBTCreateTreeRequest;
import com.orientechnologies.orient.client.remote.message.OSBTCreateTreeResponse;
import com.orientechnologies.orient.client.remote.message.OSBTFetchEntriesMajorRequest;
import com.orientechnologies.orient.client.remote.message.OSBTFetchEntriesMajorResponse;
import com.orientechnologies.orient.client.remote.message.OSBTFirstKeyRequest;
import com.orientechnologies.orient.client.remote.message.OSBTFirstKeyResponse;
import com.orientechnologies.orient.client.remote.message.OSBTGetRealBagSizeRequest;
import com.orientechnologies.orient.client.remote.message.OSBTGetRealBagSizeResponse;
import com.orientechnologies.orient.client.remote.message.OSBTGetRequest;
import com.orientechnologies.orient.client.remote.message.OSBTGetResponse;
import com.orientechnologies.orient.client.remote.message.OSetGlobalConfigurationRequest;
import com.orientechnologies.orient.client.remote.message.OSetGlobalConfigurationResponse;
import com.orientechnologies.orient.client.remote.message.OUpdateRecordRequest;
import com.orientechnologies.orient.client.remote.message.OUpdateRecordResponse;
import com.orientechnologies.orient.core.OConstants;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.cache.OCommandCache;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.command.OCommandRequestInternal;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.command.OCommandResultListener;
import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.OrientDBFactory.DatabaseType;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ridbag.sbtree.OBonsaiCollectionPointer;
import com.orientechnologies.orient.core.db.record.ridbag.sbtree.OSBTreeCollectionManager;
import com.orientechnologies.orient.core.db.tool.ODatabaseImport;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.exception.OSecurityAccessException;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.exception.OTransactionAbortedException;
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
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.OBlob;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import com.orientechnologies.orient.core.serialization.OMemoryStream;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializerFactory;
import com.orientechnologies.orient.core.serialization.serializer.record.OSerializationThreadLocal;
import com.orientechnologies.orient.core.serialization.serializer.record.string.ORecordSerializerSchemaAware2CSV;
import com.orientechnologies.orient.core.sql.query.OSQLAsynchQuery;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.core.storage.OCluster;
import com.orientechnologies.orient.core.storage.OPhysicalPosition;
import com.orientechnologies.orient.core.storage.ORecordMetadata;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.OStorageProxy;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OOfflineClusterException;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinary;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryServer;
import com.orientechnologies.orient.enterprise.channel.binary.ONetworkProtocolException;
import com.orientechnologies.orient.enterprise.channel.binary.OTokenSecurityException;
import com.orientechnologies.orient.server.OClientConnection;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.OServerInfo;
import com.orientechnologies.orient.server.ShutdownHelper;
import com.orientechnologies.orient.server.distributed.ODistributedConfiguration;
import com.orientechnologies.orient.server.distributed.ODistributedDatabase;
import com.orientechnologies.orient.server.distributed.ODistributedException;
import com.orientechnologies.orient.server.distributed.ODistributedRequest;
import com.orientechnologies.orient.server.distributed.ODistributedResponse;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.network.OServerNetworkListener;
import com.orientechnologies.orient.server.network.protocol.ONetworkProtocol;
import com.orientechnologies.orient.server.plugin.OServerPlugin;
import com.orientechnologies.orient.server.plugin.OServerPluginHelper;
import com.orientechnologies.orient.server.tx.OTransactionOptimisticProxy;

public class ONetworkProtocolBinary extends ONetworkProtocol {
  protected final Level    logClientExceptions;
  protected final boolean  logClientFullStackTrace;
  protected OChannelBinary channel;
  protected volatile int   requestType;
  protected int            clientTxId;
  protected boolean        okSent;
  private boolean          tokenConnection = true;
  private long             requests        = 0;

  public ONetworkProtocolBinary(OServer server) {
    this(server, "OrientDB <- BinaryClient/?");
  }

  public ONetworkProtocolBinary(OServer server, final String iThreadName) {
    super(server.getThreadGroup(), iThreadName);
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
      Orient.instance().getProfiler().stopChrono("server.network.requests", "Total received requests", timer,
          "server.network.requests");

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
      OLogManager.instance().warn(this, "I/O Error on distributed channel  clientId=%d reqType=%d", clientTxId, requestType, t);
      sendShutdown();
    } finally {
      Orient.instance().getProfiler().stopChrono("server.network.requests", "Total received requests", timer,
          "server.network.requests");
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
          sendError(connection, clientTxId, e);
          channel.flush();
          if (!(e instanceof OTokenSecurityException))
            OLogManager.instance().error(this, "Error executing request", e);
          OServerPluginHelper.invokeHandlerCallbackOnClientError(server, connection, e);
          afterOperationRequest(connection);
          Thread.sleep(1000);
          sendShutdown();
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
      Orient.instance().getProfiler().stopChrono("server.network.requests", "Total received requests", timer,
          "server.network.requests");

      OSerializationThreadLocal.INSTANCE.get().clear();
    }
  }

  private OClientConnection onBeforeHandshakeRequest(OClientConnection connection, OChannelBinary channel) throws IOException {
    try {
      if (requestType != OChannelBinaryProtocol.REQUEST_DB_REOPEN) {
        if (clientTxId >= 0 && connection == null
            && (requestType == OChannelBinaryProtocol.REQUEST_DB_OPEN || requestType == OChannelBinaryProtocol.REQUEST_CONNECT)) {
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
        // STANDAR FLOW
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
      if (manager != null)
        try {
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
    OReopenResponse response = new OReopenResponse(connection.getId());
    // TODO:REASSOCIATE CONNECTION TO CLIENT.
    beginResponse();
    try {
      sendOk(connection, clientTxId);
      response.write(channel, connection.getData().protocolVersion, connection.getData().serializationImpl);
    } finally {
      endResponse(connection);
    }
  }

  protected void checkServerAccess(final String iResource, OClientConnection connection) {
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

  protected void removeCluster(OClientConnection connection) throws IOException {
    setDataCommandInfo(connection, "Remove cluster");

    if (!isConnectionAlive(connection))
      return;

    ODropClusterRequest request = new ODropClusterRequest();
    request.read(channel, connection.getData().protocolVersion, connection.getData().serializationImpl);

    final String clusterName = connection.getDatabase().getClusterNameById(request.getClusterId());
    if (clusterName == null)
      throw new IllegalArgumentException("Cluster " + request.getClusterId()
          + " does not exist anymore. Refresh the db structure or just reconnect to the database");

    boolean result = connection.getDatabase().dropCluster(clusterName, false);
    ODropClusterResponse response = new ODropClusterResponse(result);
    beginResponse();
    try {
      sendOk(connection, clientTxId);
      response.write(channel, connection.getData().protocolVersion, connection.getData().serializationImpl);
    } finally {
      endResponse(connection);
    }
  }

  protected void addCluster(OClientConnection connection) throws IOException {
    setDataCommandInfo(connection, "Add cluster");

    if (!isConnectionAlive(connection))
      return;

    OAddClusterRequest request = new OAddClusterRequest();
    request.read(channel, connection.getData().protocolVersion, connection.getData().serializationImpl);

    final int num;
    if (request.getRequestedId() < 0)
      num = connection.getDatabase().addCluster(request.getClusterName());
    else
      num = connection.getDatabase().addCluster(request.getClusterName(), request.getRequestedId(), null);

    OAddClusterResponse response = new OAddClusterResponse(num);
    beginResponse();
    try {
      sendOk(connection, clientTxId);
      response.write(channel, connection.getData().protocolVersion, connection.getData().serializationImpl);
    } finally {
      endResponse(connection);
    }
  }

  protected void rangeCluster(OClientConnection connection) throws IOException {
    setDataCommandInfo(connection, "Get the begin/end range of data in cluster");

    if (!isConnectionAlive(connection))
      return;
    OGetClusterDataRangeRequest request = new OGetClusterDataRangeRequest();
    request.read(channel, connection.getData().protocolVersion, connection.getData().serializationImpl);

    final long[] pos = connection.getDatabase().getStorage().getClusterDataRange(request.getClusterId());
    OGetClusterDataRangeResponse response = new OGetClusterDataRangeResponse(pos);
    beginResponse();
    try {
      sendOk(connection, clientTxId);
      response.write(channel, connection.getData().protocolVersion, connection.getData().serializationImpl);
    } finally {
      endResponse(connection);
    }
  }

  protected void countClusters(OClientConnection connection) throws IOException {
    setDataCommandInfo(connection, "Count cluster elements");

    if (!isConnectionAlive(connection))
      return;

    OCountRequest request = new OCountRequest();
    request.read(channel, connection.getData().protocolVersion, connection.getData().serializationImpl);

    final long count = connection.getDatabase().countClusterElements(request.getClusterIds(), request.isCountTombstones());
    OCountResponse response = new OCountResponse(count);
    beginResponse();
    try {
      sendOk(connection, clientTxId);
      response.write(channel, connection.getData().protocolVersion, connection.getData().serializationImpl);
    } finally {
      endResponse(connection);
    }
  }

  protected void reloadDatabase(OClientConnection connection) throws IOException {
    setDataCommandInfo(connection, "Reload database information");

    if (!isConnectionAlive(connection))
      return;
    // OReloadRequest request = new OReloadRequest();
    final Collection<? extends OCluster> clusters = connection.getDatabase().getStorage().getClusterInstances();
    OReloadResponse response = new OReloadResponse(clusters.toArray(new OCluster[clusters.size()]));
    beginResponse();
    try {
      sendOk(connection, clientTxId);
      response.write(channel, connection.getData().protocolVersion, connection.getData().serializationImpl);
    } finally {
      endResponse(connection);
    }
  }

  protected void openDatabase(OClientConnection connection) throws IOException {
    setDataCommandInfo(connection, "Open database");

    OOpenRequest request = new OOpenRequest();
    request.read(channel, connection.getData().protocolVersion, connection.getData().serializationImpl);
    connection.getData().driverName = request.getDriverName();
    connection.getData().driverVersion = request.getDriverVersion();
    connection.getData().protocolVersion = request.getProtocolVersion();
    connection.getData().clientId = request.getClientId();
    connection.getData().serializationImpl = request.getRecordFormat();
    connection.setTokenBased(request.isUseToken());
    tokenConnection = Boolean.TRUE.equals(connection.getTokenBased());
    connection.getData().supportsPushMessages = request.isSupportsPush();
    connection.getData().collectStats = request.isCollectStats();

    try {
      connection.setDatabase(
          server.openDatabase(request.getDatabaseName(), request.getUserName(), request.getUserPassword(), connection.getData()));
    } catch (OException e) {
      server.getClientConnectionManager().disconnect(connection);
      throw e;
    }

    byte[] token = null;

    if (Boolean.TRUE.equals(connection.getTokenBased())) {
      token = server.getTokenHandler().getSignedBinaryToken(connection.getDatabase(), connection.getDatabase().getUser(),
          connection.getData());
      // TODO: do not use the parse split getSignedBinaryToken in two methods.
      getServer().getClientConnectionManager().connect(this, connection, token, server.getTokenHandler());
    }

    if (connection.getDatabase().getStorage() instanceof OStorageProxy
        && !loadUserFromSchema(connection, request.getUserName(), request.getUserPassword())) {
      sendErrorOrDropConnection(connection, clientTxId, new OSecurityAccessException(connection.getDatabase().getName(),
          "User or password not valid for database: '" + connection.getDatabase().getName() + "'"));
    } else {

      final Collection<? extends OCluster> clusters = connection.getDatabase().getStorage().getClusterInstances();
      final byte[] tokenToSend;
      if (Boolean.TRUE.equals(connection.getTokenBased())) {
        tokenToSend = token;
      } else
        tokenToSend = OCommonConst.EMPTY_BYTE_ARRAY;

      final OServerPlugin plugin = server.getPlugin("cluster");
      byte[] distriConf = null;
      ODocument distributedCfg = null;
      if (plugin != null && plugin instanceof ODistributedServerManager) {
        distributedCfg = ((ODistributedServerManager) plugin).getClusterConfiguration();

        final ODistributedConfiguration dbCfg = ((ODistributedServerManager) plugin)
            .getDatabaseConfiguration(connection.getDatabase().getName());
        if (dbCfg != null) {
          // ENHANCE SERVER CFG WITH DATABASE CFG
          distributedCfg.field("database", dbCfg.getDocument(), OType.EMBEDDED);
        }
        distriConf = getRecordBytes(connection, distributedCfg);
      }

      OOpenResponse response = new OOpenResponse(connection.getId(), tokenToSend, clusters, distriConf, OConstants.getVersion());

      beginResponse();
      try {
        sendOk(connection, clientTxId);
        response.write(channel, connection.getData().protocolVersion, connection.getData().serializationImpl);
      } finally {
        endResponse(connection);
      }
    }
  }

  protected void connect(OClientConnection connection) throws IOException {
    setDataCommandInfo(connection, "Connect");

    OConnectRequest request = new OConnectRequest();
    request.read(channel, connection.getData().protocolVersion, connection.getData().serializationImpl);

    connection.getData().driverName = request.getDriverName();
    connection.getData().driverVersion = request.getDriverVersion();
    connection.getData().protocolVersion = request.getProtocolVersion();
    connection.getData().clientId = request.getClientId();
    connection.getData().serializationImpl = request.getRecordFormat();

    connection.setTokenBased(request.isTokenBased());
    tokenConnection = Boolean.TRUE.equals(connection.getTokenBased());
    connection.getData().supportsPushMessages = request.isSupportPush();
    connection.getData().collectStats = request.isCollectStats();

    connection.setServerUser(server.serverLogin(request.getUsername(), request.getPassword(), "server.connect"));

    if (connection.getServerUser() == null)
      throw new OSecurityAccessException("Wrong user/password to [connect] to the remote OrientDB Server instance");

    byte[] token = null;
    if (connection.getData().protocolVersion > OChannelBinaryProtocol.PROTOCOL_VERSION_26) {
      connection.getData().serverUsername = connection.getServerUser().name;
      connection.getData().serverUser = true;

      if (Boolean.TRUE.equals(connection.getTokenBased())) {
        token = server.getTokenHandler().getSignedBinaryToken(null, null, connection.getData());
      } else
        token = OCommonConst.EMPTY_BYTE_ARRAY;
    }

    OConnectResponse response = new OConnectResponse(connection.getId(), token);

    beginResponse();
    try {
      sendOk(connection, clientTxId);
      response.write(channel, connection.getData().protocolVersion, connection.getData().serializationImpl);
    } finally {
      endResponse(connection);
    }
  }

  private void incrementalBackup(OClientConnection connection) throws IOException {
    setDataCommandInfo(connection, "Incremental backup");

    if (!isConnectionAlive(connection))
      return;
    OIncrementalBackupRequest request = new OIncrementalBackupRequest();
    request.read(channel, connection.getData().protocolVersion, connection.getData().serializationImpl);

    String fileName = connection.getDatabase().incrementalBackup(request.getBackupDirectory());
    OIncrementalBackupResponse response = new OIncrementalBackupResponse(fileName);
    beginResponse();
    try {
      sendOk(connection, clientTxId);
      response.write(channel, connection.getData().protocolVersion, connection.getData().serializationImpl);
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

    if (ddb != null)
      ddb.processRequest(req);
    else
      manager.executeOnLocalNode(req.getId(), req.getTask(), null);
  }

  private void executeDistributedResponse(OClientConnection connection) throws IOException {
    setDataCommandInfo(connection, "Distributed response");

    checkServerAccess("server.replication", connection);

    final ODistributedServerManager manager = server.getDistributedManager();
    final ODistributedResponse response = new ODistributedResponse();

    response.fromStream(channel.getDataInput());

    if (ODistributedServerLog.isDebugEnabled())
      ODistributedServerLog.debug(this, manager.getLocalNodeName(), response.getExecutorNodeName(),
          ODistributedServerLog.DIRECTION.IN, "Executing distributed response %s", response);

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
      if (tokenConnection && requestType != OChannelBinaryProtocol.REQUEST_CONNECT
          && (requestType != OChannelBinaryProtocol.REQUEST_DB_OPEN && requestType != OChannelBinaryProtocol.REQUEST_SHUTDOWN
              || (connection != null && connection.getData() != null
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

      Map<String, String> messages = new HashMap<>();
      Throwable it = current;
      while (it != null) {
        messages.put(current.getClass().getName(), current.getMessage());
        it = it.getCause();
      }
      final OMemoryStream memoryStream = new OMemoryStream();
      final ObjectOutputStream objectOutputStream = new ObjectOutputStream(memoryStream);

      objectOutputStream.writeObject(current);
      objectOutputStream.flush();
      final byte[] result = memoryStream.toByteArray();
      objectOutputStream.close();

      OBinaryResponse<Void> error = new OErrorResponse(messages, result);
      error.write(channel, connection.getData().protocolVersion, connection.getData().serializationImpl);
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
      OLogManager.instance().info(this, "Remote client %s:%d authenticated. Starting shutdown of server...",
          channel.socket.getInetAddress(), channel.socket.getPort());

      beginResponse();
      try {
        sendOk(connection, clientTxId);
      } finally {
        endResponse(connection);
      }
      runShutdownInNonDaemonThread();
      return;
    }

    OLogManager.instance().error(this, "Authentication error of remote client %s:%d: shutdown is aborted.",
        channel.socket.getInetAddress(), channel.socket.getPort());

    sendErrorOrDropConnection(connection, clientTxId, new OSecurityAccessException("Invalid user/password to shutdown the server"));
  }

  protected void distributedCluster(OClientConnection connection) throws IOException {
    setDataCommandInfo(connection, "Cluster status");

    ODistributedStatusRequest request = new ODistributedStatusRequest();
    request.read(channel, connection.getData().protocolVersion, connection.getData().serializationImpl);
    final ODocument req = request.getStatus();

    ODocument clusterConfig = null;

    final String operation = req.field("operation");
    if (operation == null)
      throw new IllegalArgumentException("Cluster operation is null");

    if (operation.equals("status")) {
      final OServerPlugin plugin = server.getPlugin("cluster");
      if (plugin != null && plugin instanceof ODistributedServerManager)
        clusterConfig = ((ODistributedServerManager) plugin).getClusterConfiguration();
    } else
      throw new IllegalArgumentException("Cluster operation '" + operation + "' is not supported");

    ODistributedStatusResponse response = new ODistributedStatusResponse(clusterConfig);
    beginResponse();
    try {
      sendOk(connection, clientTxId);
      response.write(channel, connection.getData().protocolVersion, connection.getData().serializationImpl);
    } finally {
      endResponse(connection);
    }
  }

  protected void countDatabaseRecords(OClientConnection connection) throws IOException {
    setDataCommandInfo(connection, "Database count records");

    if (!isConnectionAlive(connection))
      return;
    // OCountRecordsRequest request = new OCountRecordsRequest();
    OCountRecordsResponse response = new OCountRecordsResponse(connection.getDatabase().getStorage().countRecords());
    beginResponse();
    try {
      sendOk(connection, clientTxId);
      response.write(channel, connection.getData().protocolVersion, connection.getData().serializationImpl);
    } finally {
      endResponse(connection);
    }
  }

  protected void sizeDatabase(OClientConnection connection) throws IOException {
    setDataCommandInfo(connection, "Database size");

    if (!isConnectionAlive(connection))
      return;

    OGetSizeResponse response = new OGetSizeResponse(connection.getDatabase().getStorage().getSize());
    beginResponse();
    try {
      sendOk(connection, clientTxId);
      response.write(channel, connection.getData().protocolVersion, connection.getData().serializationImpl);
    } finally {
      endResponse(connection);
    }
  }

  protected void dropDatabase(OClientConnection connection) throws IOException {
    setDataCommandInfo(connection, "Drop database");

    ODropDatabaseRequest request = new ODropDatabaseRequest();
    request.read(channel, connection.getData().protocolVersion, connection.getData().serializationImpl);

    checkServerAccess("database.drop", connection);

    server.dropDatabase(request.getDatabaseName());
    OLogManager.instance().info(this, "Dropped database '%s'", request.getDatabaseName());
    connection.close();
    ODropDatabaseResponse response = new ODropDatabaseResponse();
    beginResponse();
    try {
      sendOk(connection, clientTxId);
      response.write(channel, connection.getData().protocolVersion, connection.getData().serializationImpl);
    } finally {
      endResponse(connection);
    }
  }

  protected void existsDatabase(OClientConnection connection) throws IOException {
    setDataCommandInfo(connection, "Exists database");
    OExistsDatabaseRequest request = new OExistsDatabaseRequest();
    request.read(channel, connection.getData().protocolVersion, connection.getData().serializationImpl);

    checkServerAccess("database.exists", connection);

    boolean result = server.existsDatabase(request.getDatabaseName());
    OExistsDatabaseResponse response = new OExistsDatabaseResponse(result);
    beginResponse();
    try {
      sendOk(connection, clientTxId);
      response.write(channel, connection.getData().protocolVersion, connection.getData().serializationImpl);
    } finally {
      endResponse(connection);
    }
  }

  protected void createDatabase(OClientConnection connection) throws IOException {
    setDataCommandInfo(connection, "Create database");

    OCreateDatabaseRequest request = new OCreateDatabaseRequest();
    request.read(channel, connection.getData().protocolVersion, connection.getData().serializationImpl);

    checkServerAccess("database.create", connection);

    if (server.existsDatabase(request.getDatabaseName()))
      throw new ODatabaseException("Database named '" + request.getDatabaseName() + "' already exists");
    if (request.getBackupPath() != null && !"".equals(request.getBackupPath().trim())) {
      server.restore(request.getDatabaseName(), request.getBackupPath());
    } else {
      server.createDatabase(request.getDatabaseName(), DatabaseType.valueOf(request.getStorageMode().toUpperCase()), null);
    }
    OLogManager.instance().info(this, "Created database '%s' of type '%s'", request.getDatabaseName(), request.getStorageMode());

    // TODO: it should be here an additional check for open with the right user
    connection.setDatabase(
        server.openDatabase(request.getDatabaseName(), connection.getData().serverUsername, null, connection.getData(), true));

    OCreateDatabaseResponse response = new OCreateDatabaseResponse();
    beginResponse();
    try {
      sendOk(connection, clientTxId);
      response.write(channel, connection.getData().protocolVersion, connection.getData().serializationImpl);
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
    OGetGlobalConfigurationsRequest request = new OGetGlobalConfigurationsRequest();
    request.read(channel, connection.getData().protocolVersion, connection.getData().serializationImpl);

    Map<String, String> configs = new HashMap<>();
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
      configs.put(key, value);
    }

    OGetGlobalConfigurationsResponse response = new OGetGlobalConfigurationsResponse(configs);
    beginResponse();
    try {
      sendOk(connection, clientTxId);
      response.write(channel, connection.getData().protocolVersion, connection.getData().serializationImpl);

    } finally {
      endResponse(connection);
    }
  }

  protected void configSet(OClientConnection connection) throws IOException {
    setDataCommandInfo(connection, "Set config");

    checkServerAccess("server.config.set", connection);

    OSetGlobalConfigurationRequest request = new OSetGlobalConfigurationRequest();
    request.read(channel, connection.getData().protocolVersion, connection.getData().serializationImpl);

    final OGlobalConfiguration cfg = OGlobalConfiguration.findByKey(request.getKey());

    if (cfg != null) {
      cfg.setValue(request.getValue());
      if (!cfg.isChangeableAtRuntime())
        throw new OConfigurationException(
            "Property '" + request.getKey() + "' cannot be changed at runtime. Change the setting at startup");
    } else
      throw new OConfigurationException("Property '" + request.getKey() + "' was not found in global configuration");

    OSetGlobalConfigurationResponse response = new OSetGlobalConfigurationResponse();
    beginResponse();
    try {
      sendOk(connection, clientTxId);
      response.write(channel, connection.getData().protocolVersion, connection.getData().serializationImpl);
    } finally {
      endResponse(connection);
    }
  }

  protected void configGet(OClientConnection connection) throws IOException {
    setDataCommandInfo(connection, "Get config");

    checkServerAccess("server.config.get", connection);
    OGetGlobalConfigurationRequest request = new OGetGlobalConfigurationRequest();
    request.read(channel, connection.getData().protocolVersion, connection.getData().serializationImpl);
    final OGlobalConfiguration cfg = OGlobalConfiguration.findByKey(request.getKey());

    String cfgValue = cfg != null ? cfg.isHidden() ? "<hidden>" : cfg.getValueAsString() : "";
    OGetGlobalConfigurationResponse response = new OGetGlobalConfigurationResponse(cfgValue);
    beginResponse();
    try {
      sendOk(connection, clientTxId);
      response.write(channel, connection.getData().protocolVersion, connection.getData().serializationImpl);
    } finally {
      endResponse(connection);
    }
  }

  protected void commit(OClientConnection connection) throws IOException {
    setDataCommandInfo(connection, "Transaction commit");

    if (!isConnectionAlive(connection))
      return;

    OCommitRequest request = new OCommitRequest();
    request.read(channel, connection.getData().protocolVersion, connection.getData().serializationImpl);

    final OTransactionOptimisticProxy tx = new OTransactionOptimisticProxy(connection.getDatabase(), request.getTxId(),
        request.isUsingLong(), request.getOperations(), request.getIndexChanges(), connection.getData().protocolVersion,
        connection.getData().serializationImpl);

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
        List<OCreatedRecordResponse> createdRecords = new ArrayList<>(tx.getCreatedRecords().size());
        for (Entry<ORecordId, ORecord> entry : tx.getCreatedRecords().entrySet()) {
          createdRecords.add(new OCreatedRecordResponse(entry.getKey(), (ORecordId) entry.getValue().getIdentity()));
          // IF THE NEW OBJECT HAS VERSION > 0 MEANS THAT HAS BEEN UPDATED IN THE SAME TX. THIS HAPPENS FOR GRAPHS
          if (entry.getValue().getVersion() > 0)
            tx.getUpdatedRecords().put((ORecordId) entry.getValue().getIdentity(), entry.getValue());
        }

        List<OUpdatedRecordResponse> updatedRecords = new ArrayList<>(tx.getUpdatedRecords().size());
        for (Entry<ORecordId, ORecord> entry : tx.getUpdatedRecords().entrySet()) {
          updatedRecords.add(new OUpdatedRecordResponse(entry.getKey(), entry.getValue().getVersion()));
        }
        OSBTreeCollectionManager collectionManager = connection.getDatabase().getSbTreeCollectionManager();
        Map<UUID, OBonsaiCollectionPointer> changedIds = null;
        if (collectionManager != null) {
          changedIds = collectionManager.changedIds();
        }
        OCommitResponse response = new OCommitResponse(createdRecords, updatedRecords, changedIds);
        beginResponse();
        try {
          sendOk(connection, clientTxId);
          response.write(channel, connection.getData().protocolVersion, connection.getData().serializationImpl);
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

    OCommandRequest request = new OCommandRequest();
    request.read(channel, connection.getData().protocolVersion, connection.getData().serializationImpl);

    final boolean live = request.isLive();
    final boolean asynch = request.isAsynch();
    if (connection == null && connection.getDatabase() == null)
      throw new IOException("Found invalid session");

    String dbSerializerName = connection.getDatabase().getSerializer().toString();
    String name = getRecordSerializerName(connection);
    if (name == null)
      name = dbSerializerName;
    ORecordSerializer ser = ORecordSerializerFactory.instance().getFormat(name);

    OCommandRequestText command = request.getQuery();

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
        OCommandResponseServer response = new OCommandResponseServer(result, listener, isRecordResultSet);
        response.write(channel, connection.getData().protocolVersion, connection.getData().serializationImpl);
      }

    } finally {
      connection.getData().command = null;
      endResponse(connection);
    }
  }

  protected void deleteRecord(OClientConnection connection) throws IOException {
    setDataCommandInfo(connection, "Delete record");

    if (!isConnectionAlive(connection))
      return;

    ODeleteRecordRequest request = new ODeleteRecordRequest();
    request.read(channel, connection.getData().protocolVersion, connection.getData().serializationImpl);

    final int result = deleteRecord(connection.getDatabase(), request.getRid(), request.getVersion());

    if (request.getMode() < 2) {
      ODeleteRecordResponse response = new ODeleteRecordResponse(result == 1);
      beginResponse();
      try {
        sendOk(connection, clientTxId);
        response.write(channel, connection.getData().protocolVersion, connection.getData().serializationImpl);
      } finally {
        endResponse(connection);
      }
    }
  }

  protected void hideRecord(OClientConnection connection) throws IOException {
    setDataCommandInfo(connection, "Hide record");

    if (!isConnectionAlive(connection))
      return;

    OHideRecordRequest request = new OHideRecordRequest();
    request.read(channel, connection.getData().protocolVersion, connection.getData().serializationImpl);

    final int result = hideRecord(connection.getDatabase(), request.getRecordId());

    if (request.getMode() < 2) {
      OHideRecordResponse response = new OHideRecordResponse(result == 1);
      beginResponse();
      try {
        sendOk(connection, clientTxId);
        response.write(channel, connection.getData().protocolVersion, connection.getData().serializationImpl);
      } finally {
        endResponse(connection);
      }
    }
  }

  protected void cleanOutRecord(OClientConnection connection) throws IOException {
    setDataCommandInfo(connection, "Clean out record");

    if (!isConnectionAlive(connection))
      return;

    OCleanOutRecordRequest request = new OCleanOutRecordRequest();
    request.read(channel, connection.getData().protocolVersion, connection.getData().serializationImpl);

    final int result = cleanOutRecord(connection.getDatabase(), request.getRecordId(), request.getRecordVersion());

    if (request.getMode() < 2) {
      OCleanOutRecordResponse response = new OCleanOutRecordResponse(result == 1);
      beginResponse();
      try {
        sendOk(connection, clientTxId);
        response.write(channel, connection.getData().protocolVersion, connection.getData().serializationImpl);
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
   * @throws IOException
   */
  protected void updateRecord(OClientConnection connection) throws IOException {
    setDataCommandInfo(connection, "Update record");

    if (!isConnectionAlive(connection))
      return;

    OUpdateRecordRequest request = new OUpdateRecordRequest();
    request.read(channel, connection.getData().protocolVersion, connection.getData().serializationImpl);

    final int newVersion = updateRecord(connection, request.getRid(), request.getContent(), request.getVersion(),
        request.getRecordType(), request.isUpdateContent());

    if (request.getMode() < 2) {
      Map<UUID, OBonsaiCollectionPointer> changedIds;
      OSBTreeCollectionManager collectionManager = connection.getDatabase().getSbTreeCollectionManager();
      if (collectionManager != null) {
        changedIds = new HashMap<>(collectionManager.changedIds());
        collectionManager.clearChangedIds();
      } else
        changedIds = new HashMap<>();

      OUpdateRecordResponse response = new OUpdateRecordResponse(newVersion, changedIds);
      beginResponse();
      try {
        sendOk(connection, clientTxId);
        response.write(channel, connection.getData().protocolVersion, connection.getData().serializationImpl);
      } finally {
        endResponse(connection);
      }
    }
  }

  protected void createRecord(OClientConnection connection) throws IOException {
    setDataCommandInfo(connection, "Create record");

    if (!isConnectionAlive(connection))
      return;
    OCreateRecordRequest request = new OCreateRecordRequest();
    request.read(channel, connection.getData().protocolVersion, connection.getData().serializationImpl);

    final ORecord record = createRecord(connection, request.getRid(), request.getContent(), request.getRecordType());

    if (request.getMode() < 2) {
      Map<UUID, OBonsaiCollectionPointer> changedIds;
      OSBTreeCollectionManager collectionManager = connection.getDatabase().getSbTreeCollectionManager();
      if (collectionManager != null) {
        changedIds = new HashMap<>(collectionManager.changedIds());
        collectionManager.clearChangedIds();
      } else
        changedIds = new HashMap<>();

      OCreateRecordResponse response = new OCreateRecordResponse((ORecordId) record.getIdentity(), record.getVersion(), changedIds);
      beginResponse();
      try {
        sendOk(connection, clientTxId);
        response.write(channel, connection.getData().protocolVersion, connection.getData().serializationImpl);
      } finally {
        endResponse(connection);
      }
    }
  }

  protected void readRecordMetadata(OClientConnection connection) throws IOException {
    setDataCommandInfo(connection, "Record metadata");

    OGetRecordMetadataRequest request = new OGetRecordMetadataRequest();
    request.read(channel, connection.getData().protocolVersion, connection.getData().serializationImpl);
    final ORecordMetadata metadata = connection.getDatabase().getRecordMetadata(request.getRid());
    OGetRecordMetadataResponse response;
    if (metadata != null) {
      response = new OGetRecordMetadataResponse(metadata);
    } else {
      throw new ODatabaseException(String.format("Record metadata for RID: %s, Not found", request.getRid()));
    }
    beginResponse();
    try {
      sendOk(connection, clientTxId);
      response.write(channel, connection.getData().protocolVersion, connection.getData().serializationImpl);
    } finally {
      endResponse(connection);
    }
  }

  protected void readRecord(OClientConnection connection) throws IOException {
    setDataCommandInfo(connection, "Load record");

    if (!isConnectionAlive(connection))
      return;
    OReadRecordRequest request = new OReadRecordRequest();
    request.read(channel, connection.getData().protocolVersion, connection.getData().serializationImpl);

    final ORecordId rid = request.getRid();
    final String fetchPlanString = request.getFetchPlan();
    boolean ignoreCache = false;
    ignoreCache = request.isIgnoreCache();

    boolean loadTombstones = false;
    loadTombstones = request.isLoadTumbstone();
    OReadRecordResponse response;
    if (rid.getClusterId() == 0 && rid.getClusterPosition() == 0) {
      // @COMPATIBILITY 0.9.25
      // SEND THE DB CONFIGURATION INSTEAD SINCE IT WAS ON RECORD 0:0
      OFetchHelper.checkFetchPlanValid(fetchPlanString);

      byte[] record = connection.getDatabase().getStorage().getConfiguration().toStream(connection.getData().protocolVersion);
      response = new OReadRecordResponse(OBlob.RECORD_TYPE, 0, record, new HashSet<>());

    } else {
      final ORecord record = connection.getDatabase().load(rid, fetchPlanString, ignoreCache, loadTombstones,
          OStorage.LOCKING_STRATEGY.NONE);
      if (record != null) {
        byte[] bytes = getRecordBytes(connection, record);
        final Set<ORecord> recordsToSend = new HashSet<ORecord>();
        if (record != null) {
          if (fetchPlanString.length() > 0) {
            // BUILD THE SERVER SIDE RECORD TO ACCES TO THE FETCH
            // PLAN
            if (record instanceof ODocument) {
              final OFetchPlan fetchPlan = OFetchHelper.buildFetchPlan(fetchPlanString);

              final ODocument doc = (ODocument) record;
              final OFetchListener listener = new ORemoteFetchListener() {
                @Override
                protected void sendRecord(ORecord iLinked) {
                  recordsToSend.add(iLinked);
                }
              };
              final OFetchContext context = new ORemoteFetchContext();
              OFetchHelper.fetch(doc, doc, fetchPlan, listener, context, "");

            }
          }
        }
        response = new OReadRecordResponse(ORecordInternal.getRecordType(record), record.getVersion(), bytes, recordsToSend);
      } else {
        // No Record to send
        response = new OReadRecordResponse((byte) 0, 0, null, null);
      }

    }
    beginResponse();
    try {
      sendOk(connection, clientTxId);
      response.write(channel, connection.getData().protocolVersion, connection.getData().serializationImpl);
    } finally {
      endResponse(connection);
    }

  }

  protected void readRecordIfVersionIsNotLatest(OClientConnection connection) throws IOException {
    setDataCommandInfo(connection, "Load record if version is not latest");

    if (!isConnectionAlive(connection))
      return;
    OReadRecordIfVersionIsNotLatestRequest request = new OReadRecordIfVersionIsNotLatestRequest();
    request.read(channel, connection.getData().protocolVersion, connection.getData().serializationImpl);

    final ORecordId rid = request.getRid();
    final int recordVersion = request.getRecordVersion();
    final String fetchPlanString = request.getFetchPlan();

    boolean ignoreCache = request.isIgnoreCache();

    OReadRecordIfVersionIsNotLatestResponse response;
    if (rid.getClusterId() == 0 && rid.getClusterPosition() == 0) {
      // @COMPATIBILITY 0.9.25
      // SEND THE DB CONFIGURATION INSTEAD SINCE IT WAS ON RECORD 0:0
      OFetchHelper.checkFetchPlanValid(fetchPlanString);
      byte[] record = connection.getDatabase().getStorage().getConfiguration().toStream(connection.getData().protocolVersion);
      response = new OReadRecordIfVersionIsNotLatestResponse(OBlob.RECORD_TYPE, 0, record, new HashSet<>());

    } else {
      final ORecord record = connection.getDatabase().loadIfVersionIsNotLatest(rid, recordVersion, fetchPlanString, ignoreCache);

      beginResponse();

      if (record != null) {
        byte[] bytes = getRecordBytes(connection, record);
        final Set<ORecord> recordsToSend = new HashSet<ORecord>();
        if (fetchPlanString.length() > 0) {
          // BUILD THE SERVER SIDE RECORD TO ACCES TO THE FETCH
          // PLAN
          if (record instanceof ODocument) {
            final OFetchPlan fetchPlan = OFetchHelper.buildFetchPlan(fetchPlanString);

            final ODocument doc = (ODocument) record;
            final OFetchListener listener = new ORemoteFetchListener() {
              @Override
              protected void sendRecord(ORecord iLinked) {
                recordsToSend.add(iLinked);
              }
            };
            final OFetchContext context = new ORemoteFetchContext();
            OFetchHelper.fetch(doc, doc, fetchPlan, listener, context, "");
          }
        }
        response = new OReadRecordIfVersionIsNotLatestResponse(ORecordInternal.getRecordType(record), record.getVersion(), bytes,
            recordsToSend);
      } else {
        response = new OReadRecordIfVersionIsNotLatestResponse((byte) 0, 0, null, null);
      }

    }
    beginResponse();
    try {
      sendOk(connection, clientTxId);
      response.write(channel, connection.getData().protocolVersion, connection.getData().serializationImpl);
    } finally {
      endResponse(connection);
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
      channel.writeBytes(iResponse != null ? iResponse.toStream() : null);
    } finally {
      endResponse(connection);
    }
  }

  protected void freezeDatabase(OClientConnection connection) throws IOException {
    setDataCommandInfo(connection, "Freeze database");

    checkServerAccess("database.freeze", connection);
    OFreezeDatabaseRequest request = new OFreezeDatabaseRequest();
    request.read(channel, connection.getData().protocolVersion, connection.getData().serializationImpl);

    ODatabaseDocumentInternal database = server.openDatabase(request.getName(), connection.getServerUser().name, null,
        connection.getData(), true);
    connection.setDatabase(database);

    OLogManager.instance().info(this, "Freezing database '%s'", connection.getDatabase().getURL());

    connection.getDatabase().freeze(true);
    OFreezeDatabaseResponse response = new OFreezeDatabaseResponse();

    beginResponse();
    try {
      sendOk(connection, clientTxId);
      response.write(channel, connection.getData().protocolVersion, connection.getData().serializationImpl);
    } finally {
      endResponse(connection);
    }
  }

  protected void releaseDatabase(OClientConnection connection) throws IOException {
    setDataCommandInfo(connection, "Release database");
    OReleaseDatabaseRequest request = new OReleaseDatabaseRequest();
    request.read(channel, connection.getData().protocolVersion, connection.getData().serializationImpl);

    checkServerAccess("database.release", connection);
    String storageType = null;
    storageType = request.getStorageType();

    if (storageType == null)
      storageType = "plocal";

    ODatabaseDocumentInternal database = server.openDatabase(request.getName(), connection.getServerUser().name, null,
        connection.getData(), true);

    connection.setDatabase(database);

    OLogManager.instance().info(this, "Realising database '%s'", connection.getDatabase().getURL());

    connection.getDatabase().release();
    ODeleteRecordResponse response = new ODeleteRecordResponse();

    beginResponse();
    try {
      sendOk(connection, clientTxId);
      response.write(channel, connection.getData().protocolVersion, connection.getData().serializationImpl);
    } finally {
      endResponse(connection);
    }
  }

  public static String getRecordSerializerName(OClientConnection connection) {
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
    OSBTGetRealBagSizeRequest request = new OSBTGetRealBagSizeRequest();
    request.read(channel, connection.getData().protocolVersion, connection.getData().serializationImpl);
    final OSBTreeCollectionManager sbTreeCollectionManager = connection.getDatabase().getSbTreeCollectionManager();
    final OSBTreeBonsai<OIdentifiable, Integer> tree = sbTreeCollectionManager.loadSBTree(request.getCollectionPointer());
    int realSize = tree.getRealBagSize(request.getChanges());
    try {
      OSBTGetRealBagSizeResponse response = new OSBTGetRealBagSizeResponse(realSize);
      beginResponse();
      try {
        sendOk(connection, clientTxId);
        response.write(channel, connection.getData().protocolVersion, connection.getData().serializationImpl);
      } finally {
        endResponse(connection);
      }
    } finally {
      sbTreeCollectionManager.releaseSBTree(request.getCollectionPointer());
    }
  }

  private void sbTreeBonsaiGetEntriesMajor(OClientConnection connection) throws IOException {
    setDataCommandInfo(connection, "SB-Tree bonsai get values major");

    OSBTFetchEntriesMajorRequest request = new OSBTFetchEntriesMajorRequest();
    request.read(channel, connection.getData().protocolVersion, connection.getData().serializationImpl);

    final OSBTreeCollectionManager sbTreeCollectionManager = connection.getDatabase().getSbTreeCollectionManager();
    final OSBTreeBonsai<OIdentifiable, Integer> tree = sbTreeCollectionManager.loadSBTree(request.getPointer());
    try {
      final OBinarySerializer<OIdentifiable> keySerializer = tree.getKeySerializer();
      OIdentifiable key = keySerializer.deserialize(request.getKeyStream(), 0);

      final OBinarySerializer<Integer> valueSerializer = tree.getValueSerializer();

      OTreeInternal.AccumulativeListener<OIdentifiable, Integer> listener = new OTreeInternal.AccumulativeListener<OIdentifiable, Integer>(
          request.getPageSize());
      tree.loadEntriesMajor(key, request.isInclusive(), true, listener);
      List<Entry<OIdentifiable, Integer>> result = listener.getResult();
      OSBTFetchEntriesMajorResponse<OIdentifiable, Integer> response = new OSBTFetchEntriesMajorResponse<>(keySerializer,
          valueSerializer, result);
      beginResponse();
      try {
        sendOk(connection, clientTxId);
        response.write(channel, connection.getData().protocolVersion, connection.getData().serializationImpl);
      } finally {
        endResponse(connection);
      }
    } finally {
      sbTreeCollectionManager.releaseSBTree(request.getPointer());
    }
  }

  private void sbTreeBonsaiFirstKey(OClientConnection connection) throws IOException {
    setDataCommandInfo(connection, "SB-Tree bonsai get first key");

    OSBTFirstKeyRequest request = new OSBTFirstKeyRequest();
    request.read(channel, connection.getData().protocolVersion, connection.getData().serializationImpl);

    final OSBTreeCollectionManager sbTreeCollectionManager = connection.getDatabase().getSbTreeCollectionManager();
    final OSBTreeBonsai<OIdentifiable, Integer> tree = sbTreeCollectionManager.loadSBTree(request.getCollectionPointer());
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

    OSBTFirstKeyResponse response = new OSBTFirstKeyResponse(stream);
    try {

      beginResponse();
      try {
        sendOk(connection, clientTxId);
        response.write(channel, connection.getData().protocolVersion, connection.getData().serializationImpl);
      } finally {
        endResponse(connection);
      }
    } finally {
      sbTreeCollectionManager.releaseSBTree(request.getCollectionPointer());
    }
  }

  private void sbTreeBonsaiGet(OClientConnection connection) throws IOException {
    setDataCommandInfo(connection, "SB-Tree bonsai get");

    OSBTGetRequest request = new OSBTGetRequest();
    request.read(channel, connection.getData().protocolVersion, connection.getData().serializationImpl);

    final OSBTreeCollectionManager sbTreeCollectionManager = connection.getDatabase().getSbTreeCollectionManager();
    final OSBTreeBonsai<OIdentifiable, Integer> tree = sbTreeCollectionManager.loadSBTree(request.getCollectionPointer());
    try {
      final OIdentifiable key = tree.getKeySerializer().deserialize(request.getKeyStream(), 0);

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
      OSBTGetResponse response = new OSBTGetResponse(stream);
      beginResponse();
      try {
        sendOk(connection, clientTxId);
        response.write(channel, connection.getData().protocolVersion, connection.getData().serializationImpl);
      } finally {
        endResponse(connection);
      }
    } finally {
      sbTreeCollectionManager.releaseSBTree(request.getCollectionPointer());
    }
  }

  private void createSBTreeBonsai(OClientConnection connection) throws IOException {
    setDataCommandInfo(connection, "Create SB-Tree bonsai instance");

    OSBTCreateTreeRequest request = new OSBTCreateTreeRequest();
    request.read(channel, connection.getData().protocolVersion, connection.getData().serializationImpl);

    OBonsaiCollectionPointer collectionPointer = connection.getDatabase().getSbTreeCollectionManager()
        .createSBTree(request.getClusterId(), null);

    OSBTCreateTreeResponse response = new OSBTCreateTreeResponse(collectionPointer);

    beginResponse();
    try {
      sendOk(connection, clientTxId);
      response.write(channel, connection.getData().protocolVersion, connection.getData().serializationImpl);
    } finally {
      endResponse(connection);
    }
  }

  private void lowerPositions(OClientConnection connection) throws IOException {
    setDataCommandInfo(connection, "Retrieve lower positions");

    OLowerPhysicalPositionsRequest request = new OLowerPhysicalPositionsRequest();
    request.read(channel, connection.getData().protocolVersion, connection.getData().serializationImpl);
    final OPhysicalPosition[] previousPositions = connection.getDatabase().getStorage()
        .lowerPhysicalPositions(request.getiClusterId(), request.getPhysicalPosition());
    OLowerPhysicalPositionsResponse response = new OLowerPhysicalPositionsResponse(previousPositions);
    beginResponse();
    try {
      sendOk(connection, clientTxId);
      response.write(channel, connection.getData().protocolVersion, connection.getData().serializationImpl);
    } finally {
      endResponse(connection);
    }
  }

  private void floorPositions(OClientConnection connection) throws IOException {
    setDataCommandInfo(connection, "Retrieve floor positions");

    OFloorPhysicalPositionsRequest request = new OFloorPhysicalPositionsRequest();
    request.read(channel, connection.getData().protocolVersion, connection.getData().serializationImpl);

    final OPhysicalPosition[] previousPositions = connection.getDatabase().getStorage()
        .floorPhysicalPositions(request.getClusterId(), request.getPhysicalPosition());
    OFloorPhysicalPositionsResponse response = new OFloorPhysicalPositionsResponse(previousPositions);
    beginResponse();
    try {
      sendOk(connection, clientTxId);
      response.write(channel, connection.getData().protocolVersion, connection.getData().serializationImpl);
    } finally {
      endResponse(connection);
    }
  }

  private void higherPositions(OClientConnection connection) throws IOException {
    setDataCommandInfo(connection, "Retrieve higher positions");

    OHigherPhysicalPositionsRequest request = new OHigherPhysicalPositionsRequest();
    request.read(channel, connection.getData().protocolVersion, connection.getData().serializationImpl);

    OPhysicalPosition[] nextPositions = connection.getDatabase().getStorage().higherPhysicalPositions(request.getClusterId(),
        request.getClusterPosition());
    OHigherPhysicalPositionsResponse response = new OHigherPhysicalPositionsResponse(nextPositions);

    beginResponse();
    try {
      sendOk(connection, clientTxId);
      response.write(channel, connection.getData().protocolVersion, connection.getData().serializationImpl);
    } finally {
      endResponse(connection);
    }
  }

  private void ceilingPositions(OClientConnection connection) throws IOException {
    setDataCommandInfo(connection, "Retrieve ceiling positions");

    OCeilingPhysicalPositionsRequest request = new OCeilingPhysicalPositionsRequest();
    request.read(channel, connection.getData().protocolVersion, connection.getData().serializationImpl);
    final OPhysicalPosition[] previousPositions = connection.getDatabase().getStorage()
        .ceilingPhysicalPositions(request.getClusterId(), request.getPhysicalPosition());
    OCeilingPhysicalPositionsResponse response = new OCeilingPhysicalPositionsResponse(previousPositions);
    beginResponse();
    try {
      sendOk(connection, clientTxId);
      response.write(channel, connection.getData().protocolVersion, connection.getData().serializationImpl);
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

  private void sendDatabaseInformation(OClientConnection connection) throws IOException {
    final Collection<? extends OCluster> clusters = connection.getDatabase().getStorage().getClusterInstances();
    OBinaryProtocolHelper.writeClustersArray(channel, clusters.toArray(new OCluster[clusters.size()]),
        connection.getData().protocolVersion);
  }

  private void listDatabases(OClientConnection connection) throws IOException {
    checkServerAccess("server.listDatabases", connection);

    OListDatabasesRequest request = new OListDatabasesRequest();
    request.read(channel, connection.getData().protocolVersion, connection.getData().serializationImpl);
    Set<String> dbs = server.listDatabases();
    Map<String, String> toSend = new HashMap<String, String>();
    for (String dbName : dbs) {
      toSend.put(dbName, dbName);
    }
    setDataCommandInfo(connection, "List databases");
    OListDatabasesReponse response = new OListDatabasesReponse(toSend);

    beginResponse();
    try {
      sendOk(connection, clientTxId);
      response.write(channel, connection.getData().protocolVersion, connection.getData().serializationImpl);
    } finally {
      endResponse(connection);
    }
  }

  private void serverInfo(OClientConnection connection) throws IOException {
    checkServerAccess("server.info", connection);
    setDataCommandInfo(connection, "Server Info");
    OGetServerInfoRequest request = new OGetServerInfoRequest();
    request.read(channel, connection.getData().protocolVersion, connection.getData().serializationImpl);

    OGetServerInfoResponse response = new OGetServerInfoResponse(OServerInfo.getServerInfo(server));
    beginResponse();
    try {
      sendOk(connection, clientTxId);
      response.write(channel, connection.getData().protocolVersion, connection.getData().serializationImpl);
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
   * @param channel TODO
   * @param connection
   * @param o
   *
   * @throws IOException
   */
  public static void writeIdentifiable(OChannelBinary channel, OClientConnection connection, final OIdentifiable o)
      throws IOException {
    if (o == null)
      channel.writeShort(OChannelBinaryProtocol.RECORD_NULL);
    else if (o instanceof ORecordId) {
      channel.writeShort(OChannelBinaryProtocol.RECORD_RID);
      channel.writeRID((ORID) o);
    } else {
      writeRecord(channel, connection, o.getRecord());
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
    if (server.existsDatabase(iDatabaseName)) {
      throw new ODatabaseException("Database named '" + iDatabaseName + "' already exists");
    }
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

    if (currentRecord.getIdentity().toString().equals(database.getStorage().getConfiguration().indexMgrRecordId)
        && !database.getStatus().equals(ODatabase.STATUS.IMPORTING)) {
      // FORCE INDEX MANAGER UPDATE. THIS HAPPENS FOR DIRECT CHANGES FROM REMOTE LIKE IN GRAPH
      database.getMetadata().getIndexManager().reload();
    }
    return currentRecord.getVersion();
  }

  public static byte[] getRecordBytes(OClientConnection connection, final ORecord iRecord) {

    final byte[] stream;
    String dbSerializerName = null;
    if (ODatabaseRecordThreadLocal.INSTANCE.getIfDefined() != null)
      dbSerializerName = ((ODatabaseDocumentInternal) iRecord.getDatabase()).getSerializer().toString();
    String name = getRecordSerializerName(connection);
    if (ORecordInternal.getRecordType(iRecord) == ODocument.RECORD_TYPE
        && (dbSerializerName == null || !dbSerializerName.equals(name))) {
      ((ODocument) iRecord).deserializeFields();
      ORecordSerializer ser = ORecordSerializerFactory.instance().getFormat(name);
      stream = ser.toStream(iRecord, false);
    } else
      stream = iRecord.toStream();

    return stream;
  }

  private static void writeRecord(OChannelBinary channel, OClientConnection connection, final ORecord iRecord) throws IOException {
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

      throw OException.wrapException(new OSerializationException(message), e);
    }
  }

  protected static int trimCsvSerializedContent(OClientConnection connection, final byte[] stream) {
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
    OImportRequest request = new OImportRequest();
    request.read(channel, connection.getData().protocolVersion, connection.getData().serializationImpl);
    
    List<String> result = new ArrayList<>();
    OLogManager.instance().info(this, "Starting database import");
    ODatabaseImport imp = new ODatabaseImport(connection.getDatabase(), request.getImporPath(), new OCommandOutputListener() {
      @Override
      public void onMessage(String iText) {
        OLogManager.instance().debug(ONetworkProtocolBinary.this, iText);
        if (iText != null)
          result.add(iText);
      }
    });
    imp.setOptions(request.getImporPath());
    imp.importDatabase();
    imp.close();
    new File(request.getImporPath()).delete();
    OImportResponse response = new OImportResponse(result);
    beginResponse();
    try {
      sendOk(connection, clientTxId);
      response.write(channel, connection.getData().protocolVersion, connection.getData().serializationImpl);
    } finally {
      OLogManager.instance().info(this, "Database import finshed");
      endResponse(connection);
    }

  }

}
