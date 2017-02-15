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

import com.orientechnologies.common.concur.lock.OInterruptedException;
import com.orientechnologies.common.concur.lock.OLockException;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.io.OIOException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.client.remote.OBinaryRequest;
import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.message.*;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ridbag.sbtree.OSBTreeCollectionManager;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.OSecurityAccessException;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.OMemoryStream;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializerFactory;
import com.orientechnologies.orient.core.serialization.serializer.record.OSerializationThreadLocal;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ORecordSerializerNetwork;
import com.orientechnologies.orient.core.serialization.serializer.record.string.ORecordSerializerSchemaAware2CSV;
import com.orientechnologies.orient.enterprise.channel.binary.*;
import com.orientechnologies.orient.server.OClientConnection;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.*;
import com.orientechnologies.orient.server.network.OServerNetworkListener;
import com.orientechnologies.orient.server.network.protocol.OBeforeDatabaseOpenNetworkEventListener;
import com.orientechnologies.orient.server.network.protocol.ONetworkProtocol;
import com.orientechnologies.orient.server.plugin.OServerPluginHelper;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
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
  private HandshakeInfo handshakeInfo;

  private Function<Integer, OBinaryRequest<? extends OBinaryResponse>> factory = ONetworkProtocolBinary::createRequest;

  public ONetworkProtocolBinary(OServer server) {
    this(server, "OrientDB <- BinaryClient/?");
  }

  public ONetworkProtocolBinary(OServer server, final String iThreadName) {
    super(server.getThreadGroup(), iThreadName);
    logClientExceptions = Level
        .parse(server.getContextConfiguration().getValueAsString(OGlobalConfiguration.SERVER_LOG_DUMP_CLIENT_EXCEPTION_LEVEL));
    logClientFullStackTrace = server.getContextConfiguration()
        .getValueAsBoolean(OGlobalConfiguration.SERVER_LOG_DUMP_CLIENT_EXCEPTION_FULLSTACKTRACE);
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
      if (requestType == OChannelBinaryProtocol.REQUEST_HANDSHAKE) {
        handleHandshake();
        return;
      }

      clientTxId = channel.readInt();
      // GET THE CONNECTION IF EXIST
      OClientConnection connection = server.getClientConnectionManager().getConnection(clientTxId, this);
      if (isDistributed(requestType)) {
        distributedRequest(connection, requestType, clientTxId);
      } else
        sessionRequest(connection, requestType, clientTxId);
    } catch (IOException e) {
      // if an exception arrive to this point we need to kill the current socket.
      sendShutdown();
      throw e;
    }
  }

  private void handleHandshake() throws IOException {
    short protocolVersion = channel.readShort();
    String driverName = channel.readString();
    String driverVersion = channel.readString();
    this.handshakeInfo = new HandshakeInfo(protocolVersion, driverName, driverVersion);
    this.factory = ONetworkProtocolBinary::createRequest37;
  }

  public boolean shouldReadToken(OClientConnection connection, int requestType) {
    if (connection == null) {
      return !isHandshaking(requestType) || requestType == OChannelBinaryProtocol.REQUEST_DB_REOPEN;
    } else {
      return Boolean.TRUE.equals(connection.getTokenBased()) && !isHandshaking(requestType);
    }
  }

  private void sessionRequest(OClientConnection connection, int requestType, int clientTxId) {
    long timer = 0;

    timer = Orient.instance().getProfiler().startChrono();
    OLogManager.instance().debug(this, "Request id:" + clientTxId + " type:" + requestType);

    try {
      OBinaryRequest<? extends OBinaryResponse> request = factory.apply(requestType);
      if (request != null) {
        byte[] tokenBytes = null;

        try {
          if (shouldReadToken(connection, requestType)) {
            tokenBytes = channel.readBytes();
          }
          int protocolVersion = OChannelBinaryProtocol.CURRENT_PROTOCOL_VERSION;
          ORecordSerializer serializer = ORecordSerializerNetwork.INSTANCE;
          if (connection != null) {
            protocolVersion = connection.getData().protocolVersion;
            serializer = connection.getData().getSerializer();
          }
          request.read(channel, protocolVersion, serializer);
        } catch (IOException e) {
          OLogManager.instance().debug(this, "I/O Error on client clientId=%d reqType=%d", clientTxId, requestType, e);
          sendShutdown();
          return;
        }
        OBinaryResponse response;
        try {
          if (isHandshaking(requestType))
            connection = onBeforeHandshakeRequest(connection, tokenBytes);
          else
            connection = onBeforeOperationalRequest(connection, tokenBytes);
          if (connection != null) {
            connection.getData().commandInfo = request.getDescription();
            connection.setProtocol(this); // This is need for the request command
          }

          if (request.requireServerUser()) {
            checkServerAccess(request.requiredServerRole(), connection);
          }

          if (request.requireDatabaseSession()) {
            if (connection == null || connection.getDatabase() == null)
              throw new ODatabaseException("Required database session");
          }

          response = request.execute(connection.getExecutor());
        } catch (RuntimeException t) {
          // This should be moved in the execution of the command that manipulate data
          if (connection != null && connection.getDatabase() != null) {
            final OSBTreeCollectionManager collectionManager = connection.getDatabase().getSbTreeCollectionManager();
            if (collectionManager != null)
              collectionManager.clearChangedIds();
          }

          // TODO: Replace this with build error response
          try {
            okSent = true;
            sendError(connection, clientTxId, t);
          } catch (IOException e) {
            OLogManager.instance().debug(this, "I/O Error on client clientId=%d reqType=%d", clientTxId, requestType, e);
            sendShutdown();

          }
          return;
        } finally {
          requests++;
          afterOperationRequest(connection);
        }
        if (response != null) {
          try {
            beginResponse();
            try {
              sendOk(connection, clientTxId);
              response.write(channel, connection.getData().protocolVersion, connection.getData().getSerializer());
            } finally {
              endResponse();
            }
          } catch (IOException e) {
            OLogManager.instance().debug(this, "I/O Error on client clientId=%d reqType=%d", clientTxId, requestType, e);
            sendShutdown();
          }

        }
        tokenConnection = Boolean.TRUE.equals(connection.getTokenBased());
      } else {
        OLogManager.instance().error(this, "Request not supported. Code: " + requestType);
        handleConnectionError(connection, new ONetworkProtocolException("Request not supported. Code: " + requestType));
        sendShutdown();
      }

    } finally {

      Orient.instance().getProfiler()
          .stopChrono("server.network.requests", "Total received requests", timer, "server.network.requests");

      OSerializationThreadLocal.INSTANCE.get().clear();
    }

  }

  private void distributedRequest(OClientConnection connection, int requestType, int clientTxId) {
    long timer = 0;
    try {

      timer = Orient.instance().getProfiler().startChrono();
      byte[] tokenBytes = channel.readBytes();
      connection = onBeforeOperationalRequest(connection, tokenBytes);
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
      Orient.instance().getProfiler()
          .stopChrono("server.network.requests", "Total received requests", timer, "server.network.requests");
    }

  }

  private OClientConnection onBeforeHandshakeRequest(OClientConnection connection, byte[] tokenBytes) {
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
        connection.validateSession(tokenBytes, server.getTokenHandler(), this);
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
    }

    connection.statsUpdate();

    OServerPluginHelper.invokeHandlerCallbackOnBeforeClientRequest(server, connection, (byte) requestType);
    return connection;
  }

  private OClientConnection onBeforeOperationalRequest(OClientConnection connection, byte[] tokenBytes) {
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
        if (connection == null && tokenBytes != null && tokenBytes.length > 0) {
          // THIS IS THE CASE OF A TOKEN OPERATION WITHOUT HANDSHAKE ON THIS CONNECTION.
          connection = server.getClientConnectionManager().connect(this);
          connection.setDisconnectOnAfter(true);
        }
        if (connection == null) {
          throw new OTokenSecurityException("missing session and token");
        }
        connection.acquire();
        connection.validateSession(tokenBytes, server.getTokenHandler(), this);
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

  protected void afterOperationRequest(OClientConnection connection) {
    OServerPluginHelper.invokeHandlerCallbackOnAfterClientRequest(server, connection, (byte) requestType);

    if (connection != null) {
      setDataCommandInfo(connection, "Listening");
      connection.endOperation();
      if (connection.isDisconnectOnAfter()) {
        server.getClientConnectionManager().disconnect(connection);
      }
    }
  }

  public static OBinaryRequest<? extends OBinaryResponse> createRequest(int requestType) {
    switch (requestType) {
    case OChannelBinaryProtocol.REQUEST_DB_OPEN:
      return new OOpenRequest();

    case OChannelBinaryProtocol.REQUEST_CONNECT:
      return new OConnectRequest();

    case OChannelBinaryProtocol.REQUEST_DB_REOPEN:
      return new OReopenRequest();

    case OChannelBinaryProtocol.REQUEST_SHUTDOWN:
      return new OShutdownRequest();

    case OChannelBinaryProtocol.REQUEST_DB_LIST:
      return new OListDatabasesRequest();

    case OChannelBinaryProtocol.REQUEST_SERVER_INFO:
      return new OServerInfoRequest();

    case OChannelBinaryProtocol.REQUEST_DB_RELOAD:
      return new OReloadRequest();

    case OChannelBinaryProtocol.REQUEST_DB_CREATE:
      return new OCreateDatabaseRequest();

    case OChannelBinaryProtocol.REQUEST_DB_CLOSE:
      return new OCloseRequest();

    case OChannelBinaryProtocol.REQUEST_DB_EXIST:
      return new OExistsDatabaseRequest();

    case OChannelBinaryProtocol.REQUEST_DB_DROP:
      return new ODropDatabaseRequest();

    case OChannelBinaryProtocol.REQUEST_DB_SIZE:
      return new OGetSizeRequest();

    case OChannelBinaryProtocol.REQUEST_DB_COUNTRECORDS:
      return new OCountRecordsRequest();

    case OChannelBinaryProtocol.REQUEST_CLUSTER:
      return new ODistributedStatusRequest();

    case OChannelBinaryProtocol.REQUEST_CLUSTER_COUNT:
      return new OCountRequest();

    case OChannelBinaryProtocol.REQUEST_CLUSTER_DATARANGE:
      return new OGetClusterDataRangeRequest();

    case OChannelBinaryProtocol.REQUEST_CLUSTER_ADD:
      return new OAddClusterRequest();

    case OChannelBinaryProtocol.REQUEST_CLUSTER_DROP:
      return new ODropClusterRequest();

    case OChannelBinaryProtocol.REQUEST_RECORD_METADATA:
      return new OGetRecordMetadataRequest();

    case OChannelBinaryProtocol.REQUEST_RECORD_LOAD:
      return new OReadRecordRequest();

    case OChannelBinaryProtocol.REQUEST_RECORD_LOAD_IF_VERSION_NOT_LATEST:
      return new OReadRecordIfVersionIsNotLatestRequest();

    case OChannelBinaryProtocol.REQUEST_RECORD_CREATE:
      return new OCreateRecordRequest();

    case OChannelBinaryProtocol.REQUEST_RECORD_UPDATE:
      return new OUpdateRecordRequest();

    case OChannelBinaryProtocol.REQUEST_RECORD_DELETE:
      return new ODeleteRecordRequest();

    case OChannelBinaryProtocol.REQUEST_RECORD_HIDE:
      return new OHideRecordRequest();

    case OChannelBinaryProtocol.REQUEST_POSITIONS_HIGHER:
      return new OHigherPhysicalPositionsRequest();

    case OChannelBinaryProtocol.REQUEST_POSITIONS_CEILING:
      return new OCeilingPhysicalPositionsRequest();

    case OChannelBinaryProtocol.REQUEST_POSITIONS_LOWER:
      return new OLowerPhysicalPositionsRequest();

    case OChannelBinaryProtocol.REQUEST_POSITIONS_FLOOR:
      return new OFloorPhysicalPositionsRequest();

    case OChannelBinaryProtocol.REQUEST_COMMAND:
      return new OCommandRequest();

    case OChannelBinaryProtocol.REQUEST_QUERY:
      return new OQueryRequest();

    case OChannelBinaryProtocol.REQUEST_CLOSE_QUERY:
      return new OCloseQueryRequest();

    case OChannelBinaryProtocol.REQUEST_QUERY_NEXT_PAGE:
      return new OQueryNextPageRequest();

    case OChannelBinaryProtocol.REQUEST_TX_COMMIT:
      return new OCommitRequest();

    case OChannelBinaryProtocol.REQUEST_CONFIG_GET:
      return new OGetGlobalConfigurationRequest();

    case OChannelBinaryProtocol.REQUEST_CONFIG_SET:
      return new OSetGlobalConfigurationRequest();

    case OChannelBinaryProtocol.REQUEST_CONFIG_LIST:
      return new OListGlobalConfigurationsRequest();

    case OChannelBinaryProtocol.REQUEST_DB_FREEZE:
      return new OFreezeDatabaseRequest();

    case OChannelBinaryProtocol.REQUEST_DB_RELEASE:
      return new OReleaseDatabaseRequest();

    case OChannelBinaryProtocol.REQUEST_RECORD_CLEAN_OUT:
      return new OCleanOutRecordRequest();

    case OChannelBinaryProtocol.REQUEST_CREATE_SBTREE_BONSAI:
      return new OSBTCreateTreeRequest();

    case OChannelBinaryProtocol.REQUEST_SBTREE_BONSAI_GET:
      return new OSBTGetRequest();

    case OChannelBinaryProtocol.REQUEST_SBTREE_BONSAI_FIRST_KEY:
      return new OSBTFirstKeyRequest();

    case OChannelBinaryProtocol.REQUEST_SBTREE_BONSAI_GET_ENTRIES_MAJOR:
      return new OSBTFetchEntriesMajorRequest<>();

    case OChannelBinaryProtocol.REQUEST_RIDBAG_GET_SIZE:
      return new OSBTGetRealBagSizeRequest();

    case OChannelBinaryProtocol.REQUEST_INCREMENTAL_BACKUP:
      return new OIncrementalBackupRequest();

    case OChannelBinaryProtocol.REQUEST_DB_IMPORT:
      return new OImportRequest();
    default:
      throw new ODatabaseException("binary protocol command with code: " + requestType);
    }
  }

  public static OBinaryRequest<? extends OBinaryResponse> createRequest37(int requestType) {
    switch (requestType) {

    case OChannelBinaryProtocol.REQUEST_TX_FETCH:
      return new OFetchTransactionRequest();

    case OChannelBinaryProtocol.REQUEST_TX_REBEGIN:
      return new ORebeginTransactionRequest();

    case OChannelBinaryProtocol.REQUEST_TX_BEGIN:
      return new OBeginTransactionRequest();

    case OChannelBinaryProtocol.REQUEST_TX_COMMIT:
      return new OCommit37Request();

    case OChannelBinaryProtocol.REQUEST_TX_ROLLBACK:
      return new ORollbackTransactionRequest();

    case OChannelBinaryProtocol.REQUEST_DB_OPEN:
      return new OOpenRequest();

    case OChannelBinaryProtocol.REQUEST_CONNECT:
      return new OConnectRequest();

    case OChannelBinaryProtocol.REQUEST_DB_REOPEN:
      return new OReopenRequest();

    case OChannelBinaryProtocol.REQUEST_SHUTDOWN:
      return new OShutdownRequest();

    case OChannelBinaryProtocol.REQUEST_DB_LIST:
      return new OListDatabasesRequest();

    case OChannelBinaryProtocol.REQUEST_SERVER_INFO:
      return new OServerInfoRequest();

    case OChannelBinaryProtocol.REQUEST_DB_RELOAD:
      return new OReloadRequest();

    case OChannelBinaryProtocol.REQUEST_DB_CREATE:
      return new OCreateDatabaseRequest();

    case OChannelBinaryProtocol.REQUEST_DB_CLOSE:
      return new OCloseRequest();

    case OChannelBinaryProtocol.REQUEST_DB_EXIST:
      return new OExistsDatabaseRequest();

    case OChannelBinaryProtocol.REQUEST_DB_DROP:
      return new ODropDatabaseRequest();

    case OChannelBinaryProtocol.REQUEST_DB_SIZE:
      return new OGetSizeRequest();

    case OChannelBinaryProtocol.REQUEST_DB_COUNTRECORDS:
      return new OCountRecordsRequest();

    case OChannelBinaryProtocol.REQUEST_CLUSTER:
      return new ODistributedStatusRequest();

    case OChannelBinaryProtocol.REQUEST_CLUSTER_COUNT:
      return new OCountRequest();

    case OChannelBinaryProtocol.REQUEST_CLUSTER_DATARANGE:
      return new OGetClusterDataRangeRequest();

    case OChannelBinaryProtocol.REQUEST_CLUSTER_ADD:
      return new OAddClusterRequest();

    case OChannelBinaryProtocol.REQUEST_CLUSTER_DROP:
      return new ODropClusterRequest();

    case OChannelBinaryProtocol.REQUEST_RECORD_METADATA:
      return new OGetRecordMetadataRequest();

    case OChannelBinaryProtocol.REQUEST_RECORD_LOAD:
      return new OReadRecordRequest();

    case OChannelBinaryProtocol.REQUEST_RECORD_LOAD_IF_VERSION_NOT_LATEST:
      return new OReadRecordIfVersionIsNotLatestRequest();

    case OChannelBinaryProtocol.REQUEST_RECORD_CREATE:
      return new OCreateRecordRequest();

    case OChannelBinaryProtocol.REQUEST_RECORD_UPDATE:
      return new OUpdateRecordRequest();

    case OChannelBinaryProtocol.REQUEST_RECORD_DELETE:
      return new ODeleteRecordRequest();

    case OChannelBinaryProtocol.REQUEST_RECORD_HIDE:
      return new OHideRecordRequest();

    case OChannelBinaryProtocol.REQUEST_POSITIONS_HIGHER:
      return new OHigherPhysicalPositionsRequest();

    case OChannelBinaryProtocol.REQUEST_POSITIONS_CEILING:
      return new OCeilingPhysicalPositionsRequest();

    case OChannelBinaryProtocol.REQUEST_POSITIONS_LOWER:
      return new OLowerPhysicalPositionsRequest();

    case OChannelBinaryProtocol.REQUEST_POSITIONS_FLOOR:
      return new OFloorPhysicalPositionsRequest();

    case OChannelBinaryProtocol.REQUEST_COMMAND:
      return new OCommandRequest();

    case OChannelBinaryProtocol.REQUEST_QUERY:
      return new OQueryRequest();

    case OChannelBinaryProtocol.REQUEST_CLOSE_QUERY:
      return new OCloseQueryRequest();

    case OChannelBinaryProtocol.REQUEST_QUERY_NEXT_PAGE:
      return new OQueryNextPageRequest();

    case OChannelBinaryProtocol.REQUEST_CONFIG_GET:
      return new OGetGlobalConfigurationRequest();

    case OChannelBinaryProtocol.REQUEST_CONFIG_SET:
      return new OSetGlobalConfigurationRequest();

    case OChannelBinaryProtocol.REQUEST_CONFIG_LIST:
      return new OListGlobalConfigurationsRequest();

    case OChannelBinaryProtocol.REQUEST_DB_FREEZE:
      return new OFreezeDatabaseRequest();

    case OChannelBinaryProtocol.REQUEST_DB_RELEASE:
      return new OReleaseDatabaseRequest();

    case OChannelBinaryProtocol.REQUEST_RECORD_CLEAN_OUT:
      return new OCleanOutRecordRequest();

    case OChannelBinaryProtocol.REQUEST_CREATE_SBTREE_BONSAI:
      return new OSBTCreateTreeRequest();

    case OChannelBinaryProtocol.REQUEST_SBTREE_BONSAI_GET:
      return new OSBTGetRequest();

    case OChannelBinaryProtocol.REQUEST_SBTREE_BONSAI_FIRST_KEY:
      return new OSBTFirstKeyRequest();

    case OChannelBinaryProtocol.REQUEST_SBTREE_BONSAI_GET_ENTRIES_MAJOR:
      return new OSBTFetchEntriesMajorRequest<>();

    case OChannelBinaryProtocol.REQUEST_RIDBAG_GET_SIZE:
      return new OSBTGetRealBagSizeRequest();

    case OChannelBinaryProtocol.REQUEST_INCREMENTAL_BACKUP:
      return new OIncrementalBackupRequest();

    case OChannelBinaryProtocol.REQUEST_DB_IMPORT:
      return new OImportRequest();
    default:
      throw new ODatabaseException("binary protocol command with code: " + requestType + " for protocol version 37");
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

      OBinaryResponse error = new OErrorResponse(messages, result);
      int protocolVersion = OChannelBinaryProtocol.CURRENT_PROTOCOL_VERSION;
      ORecordSerializer serializationImpl = ORecordSerializerNetwork.INSTANCE;
      if (connection != null) {
        protocolVersion = connection.getData().protocolVersion;
        serializationImpl = connection.getData().getSerializer();
      }
      error.write(channel, protocolVersion, serializationImpl);
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

  protected void beginResponse() {
    channel.acquireWriteLock();
  }

  protected void endResponse() throws IOException {
    channel.flush();
    channel.releaseWriteLock();
  }

  protected void setDataCommandInfo(OClientConnection connection, final String iCommandInfo) {
    if (connection != null)
      connection.getData().commandInfo = iCommandInfo;
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

  public static String getRecordSerializerName(OClientConnection connection) {
    return connection.getData().getSerializationImpl();
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
   * @param channel    TODO
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

  public static byte[] getRecordBytes(OClientConnection connection, final ORecord iRecord) {
    final byte[] stream;
    String dbSerializerName = null;
    if (ODatabaseRecordThreadLocal.INSTANCE.getIfDefined() != null)
      dbSerializerName = ((ODatabaseDocumentInternal) iRecord.getDatabase()).getSerializer().toString();
    String name = connection.getData().getSerializationImpl();
    if (ORecordInternal.getRecordType(iRecord) == ODocument.RECORD_TYPE && (dbSerializerName == null || !dbSerializerName
        .equals(name))) {
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
      if (ORecordSerializerSchemaAware2CSV.NAME.equals(connection.getData().getSerializationImpl())) {
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

}
