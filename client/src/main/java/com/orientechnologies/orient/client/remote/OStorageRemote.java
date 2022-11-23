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
package com.orientechnologies.orient.client.remote;

import com.orientechnologies.common.concur.OOfflineNodeException;
import com.orientechnologies.common.concur.lock.OInterruptedException;
import com.orientechnologies.common.concur.lock.OModificationOperationProhibitedException;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.io.OIOException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.thread.OThreadPoolExecutors;
import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.common.util.OCommonConst;
import com.orientechnologies.orient.client.ONotSendRequestException;
import com.orientechnologies.orient.client.binary.OChannelBinaryAsynchClient;
import com.orientechnologies.orient.client.remote.message.OAddClusterRequest;
import com.orientechnologies.orient.client.remote.message.OAddClusterResponse;
import com.orientechnologies.orient.client.remote.message.OBeginTransaction38Request;
import com.orientechnologies.orient.client.remote.message.OBeginTransactionResponse;
import com.orientechnologies.orient.client.remote.message.OBinaryPushRequest;
import com.orientechnologies.orient.client.remote.message.OBinaryPushResponse;
import com.orientechnologies.orient.client.remote.message.OCeilingPhysicalPositionsRequest;
import com.orientechnologies.orient.client.remote.message.OCeilingPhysicalPositionsResponse;
import com.orientechnologies.orient.client.remote.message.OCleanOutRecordRequest;
import com.orientechnologies.orient.client.remote.message.OCleanOutRecordResponse;
import com.orientechnologies.orient.client.remote.message.OCloseQueryRequest;
import com.orientechnologies.orient.client.remote.message.OCommandRequest;
import com.orientechnologies.orient.client.remote.message.OCommandResponse;
import com.orientechnologies.orient.client.remote.message.OCommit37Response;
import com.orientechnologies.orient.client.remote.message.OCommit38Request;
import com.orientechnologies.orient.client.remote.message.OCountRecordsRequest;
import com.orientechnologies.orient.client.remote.message.OCountRecordsResponse;
import com.orientechnologies.orient.client.remote.message.OCountRequest;
import com.orientechnologies.orient.client.remote.message.OCountResponse;
import com.orientechnologies.orient.client.remote.message.OCreateRecordRequest;
import com.orientechnologies.orient.client.remote.message.OCreateRecordResponse;
import com.orientechnologies.orient.client.remote.message.ODeleteRecordRequest;
import com.orientechnologies.orient.client.remote.message.ODeleteRecordResponse;
import com.orientechnologies.orient.client.remote.message.ODropClusterRequest;
import com.orientechnologies.orient.client.remote.message.ODropClusterResponse;
import com.orientechnologies.orient.client.remote.message.OExperimentalRequest;
import com.orientechnologies.orient.client.remote.message.OExperimentalResponse;
import com.orientechnologies.orient.client.remote.message.OFetchTransaction38Request;
import com.orientechnologies.orient.client.remote.message.OFetchTransaction38Response;
import com.orientechnologies.orient.client.remote.message.OFloorPhysicalPositionsRequest;
import com.orientechnologies.orient.client.remote.message.OFloorPhysicalPositionsResponse;
import com.orientechnologies.orient.client.remote.message.OGetClusterDataRangeRequest;
import com.orientechnologies.orient.client.remote.message.OGetClusterDataRangeResponse;
import com.orientechnologies.orient.client.remote.message.OGetRecordMetadataRequest;
import com.orientechnologies.orient.client.remote.message.OGetRecordMetadataResponse;
import com.orientechnologies.orient.client.remote.message.OGetSizeRequest;
import com.orientechnologies.orient.client.remote.message.OGetSizeResponse;
import com.orientechnologies.orient.client.remote.message.OHigherPhysicalPositionsRequest;
import com.orientechnologies.orient.client.remote.message.OHigherPhysicalPositionsResponse;
import com.orientechnologies.orient.client.remote.message.OImportRequest;
import com.orientechnologies.orient.client.remote.message.OImportResponse;
import com.orientechnologies.orient.client.remote.message.OIncrementalBackupRequest;
import com.orientechnologies.orient.client.remote.message.OIncrementalBackupResponse;
import com.orientechnologies.orient.client.remote.message.OLiveQueryPushRequest;
import com.orientechnologies.orient.client.remote.message.OLockRecordRequest;
import com.orientechnologies.orient.client.remote.message.OLockRecordResponse;
import com.orientechnologies.orient.client.remote.message.OLowerPhysicalPositionsRequest;
import com.orientechnologies.orient.client.remote.message.OLowerPhysicalPositionsResponse;
import com.orientechnologies.orient.client.remote.message.OOpen37Request;
import com.orientechnologies.orient.client.remote.message.OOpen37Response;
import com.orientechnologies.orient.client.remote.message.OPushDistributedConfigurationRequest;
import com.orientechnologies.orient.client.remote.message.OPushFunctionsRequest;
import com.orientechnologies.orient.client.remote.message.OPushIndexManagerRequest;
import com.orientechnologies.orient.client.remote.message.OPushSchemaRequest;
import com.orientechnologies.orient.client.remote.message.OPushSequencesRequest;
import com.orientechnologies.orient.client.remote.message.OPushStorageConfigurationRequest;
import com.orientechnologies.orient.client.remote.message.OQueryNextPageRequest;
import com.orientechnologies.orient.client.remote.message.OQueryRequest;
import com.orientechnologies.orient.client.remote.message.OQueryResponse;
import com.orientechnologies.orient.client.remote.message.OReadRecordIfVersionIsNotLatestRequest;
import com.orientechnologies.orient.client.remote.message.OReadRecordIfVersionIsNotLatestResponse;
import com.orientechnologies.orient.client.remote.message.OReadRecordRequest;
import com.orientechnologies.orient.client.remote.message.OReadRecordResponse;
import com.orientechnologies.orient.client.remote.message.ORebeginTransaction38Request;
import com.orientechnologies.orient.client.remote.message.OReloadRequest37;
import com.orientechnologies.orient.client.remote.message.OReloadResponse37;
import com.orientechnologies.orient.client.remote.message.ORemoteResultSet;
import com.orientechnologies.orient.client.remote.message.OReopenRequest;
import com.orientechnologies.orient.client.remote.message.OReopenResponse;
import com.orientechnologies.orient.client.remote.message.ORollbackTransactionRequest;
import com.orientechnologies.orient.client.remote.message.ORollbackTransactionResponse;
import com.orientechnologies.orient.client.remote.message.OServerQueryRequest;
import com.orientechnologies.orient.client.remote.message.OServerQueryResponse;
import com.orientechnologies.orient.client.remote.message.OSubscribeDistributedConfigurationRequest;
import com.orientechnologies.orient.client.remote.message.OSubscribeFunctionsRequest;
import com.orientechnologies.orient.client.remote.message.OSubscribeIndexManagerRequest;
import com.orientechnologies.orient.client.remote.message.OSubscribeLiveQueryRequest;
import com.orientechnologies.orient.client.remote.message.OSubscribeLiveQueryResponse;
import com.orientechnologies.orient.client.remote.message.OSubscribeSchemaRequest;
import com.orientechnologies.orient.client.remote.message.OSubscribeSequencesRequest;
import com.orientechnologies.orient.client.remote.message.OSubscribeStorageConfigurationRequest;
import com.orientechnologies.orient.client.remote.message.OUnlockRecordRequest;
import com.orientechnologies.orient.client.remote.message.OUnlockRecordResponse;
import com.orientechnologies.orient.client.remote.message.OUnsubscribeLiveQueryRequest;
import com.orientechnologies.orient.client.remote.message.OUnsubscribeRequest;
import com.orientechnologies.orient.client.remote.message.OUpdateRecordRequest;
import com.orientechnologies.orient.client.remote.message.OUpdateRecordResponse;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.command.OCommandRequestAsynch;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.config.OStorageClusterConfiguration;
import com.orientechnologies.orient.core.config.OStorageConfiguration;
import com.orientechnologies.orient.core.conflict.ORecordConflictStrategy;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.OLiveQueryMonitor;
import com.orientechnologies.orient.core.db.OSharedContext;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.OrientDBRemote;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentRemote;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTxInternal;
import com.orientechnologies.orient.core.db.document.OLiveQueryMonitorRemote;
import com.orientechnologies.orient.core.db.document.OTransactionOptimisticClient;
import com.orientechnologies.orient.core.db.record.OCurrentStorageComponentsFactory;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.exception.OSecurityException;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.security.OTokenException;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.ORecordVersionHelper;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.security.OCredentialInterceptor;
import com.orientechnologies.orient.core.security.OSecurityManager;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializerFactory;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ORecordSerializerNetworkV37Client;
import com.orientechnologies.orient.core.sql.query.OLiveQuery;
import com.orientechnologies.orient.core.storage.OCluster;
import com.orientechnologies.orient.core.storage.OPhysicalPosition;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import com.orientechnologies.orient.core.storage.ORecordCallback;
import com.orientechnologies.orient.core.storage.ORecordMetadata;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.OStorageOperationResult;
import com.orientechnologies.orient.core.storage.OStorageProxy;
import com.orientechnologies.orient.core.storage.cluster.OPaginatedCluster;
import com.orientechnologies.orient.core.storage.impl.local.paginated.ORecordSerializationContext;
import com.orientechnologies.orient.core.storage.ridbag.sbtree.OBonsaiCollectionPointer;
import com.orientechnologies.orient.core.storage.ridbag.sbtree.OSBTreeCollectionManager;
import com.orientechnologies.orient.core.tx.OTransactionAbstract;
import com.orientechnologies.orient.core.tx.OTransactionInternal;
import com.orientechnologies.orient.core.tx.OTransactionOptimistic;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinary;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;
import com.orientechnologies.orient.enterprise.channel.binary.ODistributedRedirectException;
import com.orientechnologies.orient.enterprise.channel.binary.OTokenSecurityException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/** This object is bound to each remote ODatabase instances. */
public class OStorageRemote implements OStorageProxy, ORemotePushHandler, OStorage {
  @Deprecated public static final String PARAM_CONNECTION_STRATEGY = "connectionStrategy";

  public static final String DRIVER_NAME = "OrientDB Java";

  private static final AtomicInteger sessionSerialId = new AtomicInteger(-1);

  public enum CONNECTION_STRATEGY {
    STICKY,
    ROUND_ROBIN_CONNECT,
    ROUND_ROBIN_REQUEST
  }

  private CONNECTION_STRATEGY connectionStrategy = CONNECTION_STRATEGY.STICKY;

  private final OSBTreeCollectionManagerRemote sbTreeCollectionManager =
      new OSBTreeCollectionManagerRemote();
  private final ORemoteURLs serverURLs;
  private final Map<String, OCluster> clusterMap = new ConcurrentHashMap<String, OCluster>();
  private final ExecutorService asynchExecutor;
  private final ODocument clusterConfiguration = new ODocument();
  private final AtomicInteger users = new AtomicInteger(0);
  private final OContextConfiguration clientConfiguration;
  private final int connectionRetry;
  private final int connectionRetryDelay;
  private OCluster[] clusters = OCommonConst.EMPTY_CLUSTER_ARRAY;
  private int defaultClusterId;
  public ORemoteConnectionManager connectionManager;
  private final Set<OStorageRemoteSession> sessions =
      Collections.newSetFromMap(new ConcurrentHashMap<OStorageRemoteSession, Boolean>());

  private final Map<Integer, OLiveQueryClientListener> liveQueryListener =
      new ConcurrentHashMap<>();
  private volatile OStorageRemotePushThread pushThread;
  protected final OrientDBRemote context;
  protected OSharedContext sharedContext = null;
  protected final String url;
  protected final ReentrantReadWriteLock stateLock;

  protected volatile OStorageConfiguration configuration;
  protected volatile OCurrentStorageComponentsFactory componentsFactory;
  protected String name;

  protected volatile STATUS status = STATUS.CLOSED;

  public static final String ADDRESS_SEPARATOR = ";";

  private static String buildUrl(String[] hosts, String name) {
    return String.join(ADDRESS_SEPARATOR, hosts) + "/" + name;
  }

  public OStorageRemote(
      final ORemoteURLs hosts,
      String name,
      OrientDBRemote context,
      final String iMode,
      ORemoteConnectionManager connectionManager,
      OrientDBConfig config)
      throws IOException {
    this(hosts, name, context, iMode, connectionManager, null, config);
  }

  public OStorageRemote(
      final ORemoteURLs hosts,
      String name,
      OrientDBRemote context,
      final String iMode,
      ORemoteConnectionManager connectionManager,
      final STATUS status,
      OrientDBConfig config)
      throws IOException {

    this.name = normalizeName(name);

    if (OStringSerializerHelper.contains(this.name, ','))
      throw new IllegalArgumentException("Invalid character in storage name: " + this.name);

    url = buildUrl(hosts.getUrls().toArray(new String[] {}), name);

    stateLock = new ReentrantReadWriteLock();
    if (status != null) this.status = status;

    configuration = null;

    if (config != null) {
      clientConfiguration = config.getConfigurations();
    } else {
      clientConfiguration = new OContextConfiguration();
    }
    connectionRetry =
        clientConfiguration.getValueAsInteger(OGlobalConfiguration.NETWORK_SOCKET_RETRY);
    connectionRetryDelay =
        clientConfiguration.getValueAsInteger(OGlobalConfiguration.NETWORK_SOCKET_RETRY_DELAY);
    serverURLs = hosts;

    asynchExecutor = OThreadPoolExecutors.newSingleThreadScheduledPool("OStorageRemote Async");

    this.connectionManager = connectionManager;
    this.context = context;
  }

  private String normalizeName(String name) {
    if (OStringSerializerHelper.contains(name, '/')) {
      name = name.substring(name.lastIndexOf("/") + 1);

      if (OStringSerializerHelper.contains(name, '\\'))
        return name.substring(name.lastIndexOf("\\") + 1);
      else return name;

    } else if (OStringSerializerHelper.contains(name, '\\')) {
      name = name.substring(name.lastIndexOf("\\") + 1);

      if (OStringSerializerHelper.contains(name, '/'))
        return name.substring(name.lastIndexOf("/") + 1);
      else return name;
    } else {
      return name;
    }
  }

  public OStorageConfiguration getConfiguration() {
    return configuration;
  }

  public boolean checkForRecordValidity(final OPhysicalPosition ppos) {
    return ppos != null && !ORecordVersionHelper.isTombstone(ppos.recordVersion);
  }

  public String getName() {
    return name;
  }

  public void setSharedContext(OSharedContext sharedContext) {
    this.sharedContext = sharedContext;
  }

  public <T extends OBinaryResponse> T asyncNetworkOperationNoRetry(
      final OBinaryAsyncRequest<T> request,
      int mode,
      final ORecordId recordId,
      final ORecordCallback<T> callback,
      final String errorMessage) {
    return asyncNetworkOperationRetry(request, mode, recordId, callback, errorMessage, 0);
  }

  public <T extends OBinaryResponse> T asyncNetworkOperationRetry(
      final OBinaryAsyncRequest<T> request,
      int mode,
      final ORecordId recordId,
      final ORecordCallback<T> callback,
      final String errorMessage,
      int retry) {
    final int pMode;
    if (mode == 1 && callback == null)
      // ASYNCHRONOUS MODE NO ANSWER
      pMode = 2;
    else pMode = mode;
    request.setMode((byte) pMode);
    return baseNetworkOperation(
        (network, session) -> {
          // Send The request
          try {
            try {
              network.beginRequest(request.getCommand(), session);
              request.write(network, session);
            } finally {
              network.endRequest();
            }
          } catch (IOException e) {
            throw new ONotSendRequestException("Cannot send request on this channel");
          }
          final T response = request.createResponse();
          T ret = null;
          if (pMode == 0) {
            // SYNC
            try {
              beginResponse(network, session);
              response.read(network, session);
            } finally {
              endResponse(network);
            }
            ret = response;
            connectionManager.release(network);
          } else if (pMode == 1) {
            // ASYNC
            asynchExecutor.submit(
                () -> {
                  try {
                    try {
                      beginResponse(network, session);
                      response.read(network, session);
                    } finally {
                      endResponse(network);
                    }
                    callback.call(recordId, response);
                    connectionManager.release(network);
                  } catch (Exception e) {
                    connectionManager.remove(network);
                    OLogManager.instance().error(this, "Exception on async query", e);
                  } catch (Error e) {
                    connectionManager.remove(network);
                    OLogManager.instance().error(this, "Exception on async query", e);
                    throw e;
                  }
                });
          } else {
            // NO RESPONSE
            connectionManager.release(network);
          }
          return ret;
        },
        errorMessage,
        retry);
  }

  public <T extends OBinaryResponse> T networkOperationRetryTimeout(
      final OBinaryRequest<T> request, final String errorMessage, int retry, int timeout) {
    return baseNetworkOperation(
        (network, session) -> {
          try {
            try {
              network.beginRequest(request.getCommand(), session);
              request.write(network, session);
            } finally {
              network.endRequest();
            }
          } catch (IOException e) {
            if (network.isConnected()) {
              OLogManager.instance().warn(this, "Error Writing request on the network", e);
            }
            throw new ONotSendRequestException("Cannot send request on this channel");
          }

          int prev = network.getSocketTimeout();
          T response = request.createResponse();
          try {
            if (timeout > 0) network.setSocketTimeout(timeout);
            beginResponse(network, session);
            response.read(network, session);
          } finally {
            endResponse(network);
            if (timeout > 0) network.setSocketTimeout(prev);
          }
          connectionManager.release(network);
          return response;
        },
        errorMessage,
        retry);
  }

  public <T extends OBinaryResponse> T networkOperationNoRetry(
      final OBinaryRequest<T> request, final String errorMessage) {
    return networkOperationRetryTimeout(request, errorMessage, 0, 0);
  }

  public <T extends OBinaryResponse> T networkOperation(
      final OBinaryRequest<T> request, final String errorMessage) {
    return networkOperationRetryTimeout(request, errorMessage, connectionRetry, 0);
  }

  public <T> T baseNetworkOperation(
      final OStorageRemoteOperation<T> operation, final String errorMessage, int retry) {
    OStorageRemoteSession session = getCurrentSession();
    if (session.commandExecuting)
      throw new ODatabaseException(
          "Cannot execute the request because an asynchronous operation is in progress. Please use a different connection");

    String serverUrl = null;
    do {
      OChannelBinaryAsynchClient network = null;

      if (serverUrl == null) serverUrl = getNextAvailableServerURL(false, session);

      do {
        try {
          network = getNetwork(serverUrl);
        } catch (OException e) {
          if (session.isStickToSession()) {
            throw e;
          } else {
            serverUrl = useNewServerURL(serverUrl);
            if (serverUrl == null) {
              throw e;
            }
          }
        }
      } while (network == null);

      try {
        session.commandExecuting = true;

        // In case i do not have a token or i'm switching between server i've to execute a open
        // operation.
        OStorageRemoteNodeSession nodeSession = session.getServerSession(network.getServerURL());
        if (nodeSession == null || !nodeSession.isValid() && !session.isStickToSession()) {
          if (nodeSession != null) {
            session.removeServerSession(nodeSession.getServerURL());
          }
          openRemoteDatabase(network);
          if (!network.tryLock()) continue;
        }

        return operation.execute(network, session);
      } catch (ONotSendRequestException e) {
        connectionManager.remove(network);
        serverUrl = null;
      } catch (ODistributedRedirectException e) {
        connectionManager.release(network);
        OLogManager.instance()
            .debug(
                this,
                "Redirecting the request from server '%s' to the server '%s' because %s",
                e,
                e.getFromServer(),
                e.toString(),
                e.getMessage());

        // RECONNECT TO THE SERVER SUGGESTED IN THE EXCEPTION
        serverUrl = e.getToServerAddress();
      } catch (OModificationOperationProhibitedException mope) {
        connectionManager.release(network);
        handleDBFreeze();
        serverUrl = null;
      } catch (OTokenException | OTokenSecurityException e) {
        connectionManager.release(network);
        if (session.isStickToSession()) {
          session.removeServerSession(network.getServerURL());
          throw OException.wrapException(new OStorageException(errorMessage), e);
        } else {
          session.removeServerSession(network.getServerURL());
        }
        serverUrl = null;
      } catch (OOfflineNodeException e) {
        connectionManager.release(network);
        // Remove the current url because the node is offline
        this.serverURLs.remove(serverUrl);
        for (OStorageRemoteSession activeSession : sessions) {
          // Not thread Safe ...
          activeSession.removeServerSession(serverUrl);
        }
        serverUrl = null;
      } catch (IOException | OIOException e) {
        OLogManager.instance()
            .info(
                this,
                "Caught Network I/O errors on %s, trying an automatic reconnection... (error: %s)",
                network.getServerURL(),
                e.getMessage());
        OLogManager.instance().debug(this, "I/O error stack: ", e);
        connectionManager.remove(network);
        if (--retry <= 0) throw OException.wrapException(new OIOException(e.getMessage()), e);
        else {
          try {
            Thread.sleep(connectionRetryDelay);
          } catch (InterruptedException e1) {
            OLogManager.instance()
                .error(this, "Exception was suppressed, original exception is ", e);
            throw OException.wrapException(new OInterruptedException(e1.getMessage()), e1);
          }
        }
        serverUrl = null;
      } catch (OException e) {
        connectionManager.release(network);
        throw e;
      } catch (Exception e) {
        connectionManager.release(network);
        throw OException.wrapException(new OStorageException(errorMessage), e);
      } finally {
        session.commandExecuting = false;
      }
    } while (true);
  }

  public boolean isAssigningClusterIds() {
    return false;
  }

  /** Supported only in embedded storage. Use <code>SELECT FROM metadata:storage</code> instead. */
  public String getCreatedAtVersion() {
    throw new UnsupportedOperationException(
        "Supported only in embedded storage. Use 'SELECT FROM metadata:storage' instead.");
  }

  public int getSessionId() {
    OStorageRemoteSession session = getCurrentSession();
    return session != null ? session.getSessionId() : -1;
  }

  public String getServerURL() {
    OStorageRemoteSession session = getCurrentSession();
    return session != null ? session.getServerUrl() : null;
  }

  public void open(
      final String iUserName, final String iUserPassword, final OContextConfiguration conf) {

    addUser();
    try {
      OStorageRemoteSession session = getCurrentSession();
      if (status == STATUS.CLOSED
          || !iUserName.equals(session.connectionUserName)
          || !iUserPassword.equals(session.connectionUserPassword)
          || session.sessions.isEmpty()) {

        OCredentialInterceptor ci = OSecurityManager.instance().newCredentialInterceptor();

        if (ci != null) {
          ci.intercept(getURL(), iUserName, iUserPassword);
          session.connectionUserName = ci.getUsername();
          session.connectionUserPassword = ci.getPassword();
        } else {
          // Do Nothing
          session.connectionUserName = iUserName;
          session.connectionUserPassword = iUserPassword;
        }

        String strategy = conf.getValueAsString(OGlobalConfiguration.CLIENT_CONNECTION_STRATEGY);
        if (strategy != null)
          connectionStrategy = CONNECTION_STRATEGY.valueOf(strategy.toUpperCase(Locale.ENGLISH));

        openRemoteDatabase();

        reload();
        initPush(session);

        componentsFactory = new OCurrentStorageComponentsFactory(configuration);

      } else {
        reopenRemoteDatabase();
      }
    } catch (Exception e) {
      removeUser();
      if (e instanceof RuntimeException)
        // PASS THROUGH
        throw (RuntimeException) e;
      else
        throw OException.wrapException(
            new OStorageException("Cannot open the remote storage: " + name), e);
    }
  }

  public OSBTreeCollectionManager getSBtreeCollectionManager() {
    return sbTreeCollectionManager;
  }

  public void reload() {
    OReloadResponse37 res =
        networkOperation(new OReloadRequest37(), "error loading storage configuration");
    final OStorageConfiguration storageConfiguration =
        new OStorageConfigurationRemote(
            ORecordSerializerFactory.instance().getDefaultRecordSerializer().toString(),
            res.getPayload(),
            clientConfiguration);

    updateStorageConfiguration(storageConfiguration);
  }

  public void create(OContextConfiguration contextConfiguration) {
    throw new UnsupportedOperationException(
        "Cannot create a database in a remote server. Please use the console or the OServerAdmin class.");
  }

  public boolean exists() {
    throw new UnsupportedOperationException(
        "Cannot check the existence of a database in a remote server. Please use the console or the OServerAdmin class.");
  }

  public void close(final boolean iForce, boolean onDelete) {
    if (status == STATUS.CLOSED) return;

    final OStorageRemoteSession session = getCurrentSession();
    if (session != null) {
      final Collection<OStorageRemoteNodeSession> nodes = session.getAllServerSessions();
      if (!nodes.isEmpty()) {
        OContextConfiguration config = null;
        if (configuration != null) {
          config = configuration.getContextConfiguration();
        }
        session.closeAllSessions(connectionManager, config);
        if (!checkForClose(iForce)) return;
      } else {
        if (!iForce) return;
      }
      sessions.remove(session);
      if (!checkForClose(iForce)) return;
    }
  }

  public void shutdown() {
    if (status == STATUS.CLOSED || status == STATUS.CLOSING) return;

    // FROM HERE FORWARD COMPLETELY CLOSE THE STORAGE
    for (Entry<Integer, OLiveQueryClientListener> listener : liveQueryListener.entrySet()) {
      listener.getValue().onEnd();
    }
    liveQueryListener.clear();

    stateLock.writeLock().lock();
    try {
      if (status == STATUS.CLOSED) return;

      status = STATUS.CLOSING;
      close(true, false);
    } finally {
      stateLock.writeLock().unlock();
    }
    if (pushThread != null) {
      pushThread.shutdown();
      try {
        pushThread.join();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
    stateLock.writeLock().lock();
    try {
      // CLOSE ALL THE SOCKET POOLS
      sbTreeCollectionManager.close();

      status = STATUS.CLOSED;

    } finally {
      stateLock.writeLock().unlock();
      ;
    }
  }

  private boolean checkForClose(final boolean force) {
    if (status == STATUS.CLOSED) return false;

    if (status == STATUS.CLOSED) return false;

    final int remainingUsers = getUsers() > 0 ? removeUser() : 0;

    return force || remainingUsers == 0;
  }

  public int getUsers() {
    return users.get();
  }

  public int addUser() {
    return users.incrementAndGet();
  }

  public int removeUser() {
    if (users.get() < 1)
      throw new IllegalStateException(
          "Cannot remove user of the remote storage '"
              + toString()
              + "' because no user is using it");

    return users.decrementAndGet();
  }

  public void delete() {
    throw new UnsupportedOperationException(
        "Cannot delete a database in a remote server. Please use the console or the OServerAdmin class.");
  }

  public Set<String> getClusterNames() {
    stateLock.readLock().lock();
    try {

      return new HashSet<String>(clusterMap.keySet());

    } finally {
      stateLock.readLock().unlock();
      ;
    }
  }

  public OStorageOperationResult<OPhysicalPosition> createRecord(
      final ORecordId iRid,
      final byte[] iContent,
      final int iRecordVersion,
      final byte iRecordType,
      final int iMode,
      final ORecordCallback<Long> iCallback) {

    final OSBTreeCollectionManager collectionManager =
        ODatabaseRecordThreadLocal.instance().get().getSbTreeCollectionManager();
    ORecordCallback<OCreateRecordResponse> realCallback = null;
    if (iCallback != null) {
      realCallback =
          (iRID, response) -> {
            iCallback.call(response.getIdentity(), response.getIdentity().getClusterPosition());
            updateCollectionsFromChanges(collectionManager, response.getChangedIds());
          };
    }
    // The Upper layer require to return this also if it not really received response from the
    // network
    final OPhysicalPosition ppos = new OPhysicalPosition(iRecordType);
    final OCreateRecordRequest request = new OCreateRecordRequest(iContent, iRid, iRecordType);
    final OCreateRecordResponse response =
        asyncNetworkOperationNoRetry(
            request,
            iMode,
            iRid,
            realCallback,
            "Error on create record in cluster " + iRid.getClusterId());
    if (response != null) {
      ppos.clusterPosition = response.getIdentity().getClusterPosition();
      ppos.recordVersion = response.getVersion();
      // THIS IS A COMPATIBILITY FIX TO AVOID TO FILL THE CLUSTER ID IN CASE OF ASYNC
      if (iMode == 0) {
        iRid.setClusterId(response.getIdentity().getClusterId());
        iRid.setClusterPosition(response.getIdentity().getClusterPosition());
      }
      updateCollectionsFromChanges(collectionManager, response.getChangedIds());
    }

    return new OStorageOperationResult<OPhysicalPosition>(ppos);
  }

  private void updateCollectionsFromChanges(
      final OSBTreeCollectionManager collectionManager,
      final Map<UUID, OBonsaiCollectionPointer> changes) {
    if (collectionManager != null) {
      for (Entry<UUID, OBonsaiCollectionPointer> coll : changes.entrySet()) {
        collectionManager.updateCollectionPointer(coll.getKey(), coll.getValue());
      }
      if (ORecordSerializationContext.getDepth() <= 1) collectionManager.clearPendingCollections();
    }
  }

  public ORecordMetadata getRecordMetadata(final ORID rid) {

    OGetRecordMetadataRequest request = new OGetRecordMetadataRequest(rid);
    OGetRecordMetadataResponse response =
        networkOperation(request, "Error on record metadata read " + rid);

    return response.getMetadata();
  }

  public OStorageOperationResult<ORawBuffer> readRecordIfVersionIsNotLatest(
      final ORecordId rid,
      final String fetchPlan,
      final boolean ignoreCache,
      final int recordVersion)
      throws ORecordNotFoundException {
    if (getCurrentSession().commandExecuting)
      // PENDING NETWORK OPERATION, CAN'T EXECUTE IT NOW
      return new OStorageOperationResult<ORawBuffer>(null);

    OReadRecordIfVersionIsNotLatestRequest request =
        new OReadRecordIfVersionIsNotLatestRequest(rid, recordVersion, fetchPlan, ignoreCache);
    OReadRecordIfVersionIsNotLatestResponse response =
        networkOperation(request, "Error on read record " + rid);

    return new OStorageOperationResult<ORawBuffer>(response.getResult());
  }

  public OStorageOperationResult<ORawBuffer> readRecord(
      final ORecordId iRid,
      final String iFetchPlan,
      final boolean iIgnoreCache,
      boolean prefetchRecords,
      final ORecordCallback<ORawBuffer> iCallback) {

    if (getCurrentSession().commandExecuting)
      // PENDING NETWORK OPERATION, CAN'T EXECUTE IT NOW
      return new OStorageOperationResult<ORawBuffer>(null);

    OReadRecordRequest request = new OReadRecordRequest(iIgnoreCache, iRid, iFetchPlan, false);
    OReadRecordResponse response = networkOperation(request, "Error on read record " + iRid);

    return new OStorageOperationResult<ORawBuffer>(response.getResult());
  }

  public String incrementalBackup(final String backupDirectory, OCallable<Void, Void> started) {
    OIncrementalBackupRequest request = new OIncrementalBackupRequest(backupDirectory);
    OIncrementalBackupResponse response =
        networkOperationNoRetry(request, "Error on incremental backup");
    return response.getFileName();
  }

  public boolean supportIncremental() {
    // THIS IS FALSE HERE THOUGH WE HAVE SOME SUPPORT FOR SOME SPECIFIC CASES FROM REMOTE
    return false;
  }

  public void fullIncrementalBackup(final OutputStream stream)
      throws UnsupportedOperationException {
    throw new UnsupportedOperationException(
        "This operations is part of internal API and is not supported in remote storage");
  }

  public void restoreFromIncrementalBackup(final String filePath) {
    throw new UnsupportedOperationException(
        "This operations is part of internal API and is not supported in remote storage");
  }

  public void restoreFullIncrementalBackup(final InputStream stream)
      throws UnsupportedOperationException {
    throw new UnsupportedOperationException(
        "This operations is part of internal API and is not supported in remote storage");
  }

  public OStorageOperationResult<Integer> updateRecord(
      final ORecordId iRid,
      final boolean updateContent,
      final byte[] iContent,
      final int iVersion,
      final byte iRecordType,
      final int iMode,
      final ORecordCallback<Integer> iCallback) {

    final OSBTreeCollectionManager collectionManager =
        ODatabaseRecordThreadLocal.instance().get().getSbTreeCollectionManager();

    ORecordCallback<OUpdateRecordResponse> realCallback = null;
    if (iCallback != null) {
      realCallback =
          (iRID, response) -> {
            iCallback.call(iRID, response.getVersion());
            updateCollectionsFromChanges(collectionManager, response.getChanges());
          };
    }

    OUpdateRecordRequest request =
        new OUpdateRecordRequest(iRid, iContent, iVersion, updateContent, iRecordType);
    OUpdateRecordResponse response =
        asyncNetworkOperationNoRetry(
            request, iMode, iRid, realCallback, "Error on update record " + iRid);

    Integer resVersion = null;
    if (response != null) {
      // Returning given version in case of no answer from server
      resVersion = response.getVersion();
      updateCollectionsFromChanges(collectionManager, response.getChanges());
    }
    return new OStorageOperationResult<Integer>(resVersion);
  }

  public OStorageOperationResult<Boolean> deleteRecord(
      final ORecordId iRid,
      final int iVersion,
      final int iMode,
      final ORecordCallback<Boolean> iCallback) {
    ORecordCallback<ODeleteRecordResponse> realCallback = null;
    if (iCallback != null)
      realCallback = (iRID, response) -> iCallback.call(iRID, response.getResult());

    final ODeleteRecordRequest request = new ODeleteRecordRequest(iRid, iVersion);
    final ODeleteRecordResponse response =
        asyncNetworkOperationNoRetry(
            request, iMode, iRid, realCallback, "Error on delete record " + iRid);
    Boolean resDelete = null;
    if (response != null) resDelete = response.getResult();
    return new OStorageOperationResult<Boolean>(resDelete);
  }

  public boolean cleanOutRecord(
      final ORecordId recordId,
      final int recordVersion,
      final int iMode,
      final ORecordCallback<Boolean> callback) {

    ORecordCallback<OCleanOutRecordResponse> realCallback = null;
    if (callback != null)
      realCallback = (iRID, response) -> callback.call(iRID, response.getResult());

    final OCleanOutRecordRequest request = new OCleanOutRecordRequest(recordVersion, recordId);
    final OCleanOutRecordResponse response =
        asyncNetworkOperationNoRetry(
            request, iMode, recordId, realCallback, "Error on delete record " + recordId);
    Boolean result = null;
    if (response != null) result = response.getResult();
    return result != null ? result : false;
  }

  public List<String> backup(
      OutputStream out,
      Map<String, Object> options,
      Callable<Object> callable,
      final OCommandOutputListener iListener,
      int compressionLevel,
      int bufferSize)
      throws IOException {
    throw new UnsupportedOperationException(
        "backup is not supported against remote storage. Open the database with plocal or use the incremental backup in the Enterprise Edition");
  }

  public void restore(
      InputStream in,
      Map<String, Object> options,
      Callable<Object> callable,
      final OCommandOutputListener iListener)
      throws IOException {
    throw new UnsupportedOperationException(
        "restore is not supported against remote storage. Open the database with plocal or use Enterprise Edition");
  }

  public OContextConfiguration getClientConfiguration() {
    return clientConfiguration;
  }

  public long count(final int iClusterId) {
    return count(new int[] {iClusterId});
  }

  public long count(int iClusterId, boolean countTombstones) {
    return count(new int[] {iClusterId}, countTombstones);
  }

  public long[] getClusterDataRange(final int iClusterId) {
    OGetClusterDataRangeRequest request = new OGetClusterDataRangeRequest(iClusterId);
    OGetClusterDataRangeResponse response =
        networkOperation(
            request, "Error on getting last entry position count in cluster: " + iClusterId);
    return response.getPos();
  }

  public OPhysicalPosition[] higherPhysicalPositions(
      final int iClusterId, final OPhysicalPosition iClusterPosition) {
    OHigherPhysicalPositionsRequest request =
        new OHigherPhysicalPositionsRequest(iClusterId, iClusterPosition);

    OHigherPhysicalPositionsResponse response =
        networkOperation(
            request,
            "Error on retrieving higher positions after " + iClusterPosition.clusterPosition);
    return response.getNextPositions();
  }

  public OPhysicalPosition[] ceilingPhysicalPositions(
      final int clusterId, final OPhysicalPosition physicalPosition) {

    OCeilingPhysicalPositionsRequest request =
        new OCeilingPhysicalPositionsRequest(clusterId, physicalPosition);

    OCeilingPhysicalPositionsResponse response =
        networkOperation(
            request,
            "Error on retrieving ceiling positions after " + physicalPosition.clusterPosition);
    return response.getPositions();
  }

  public OPhysicalPosition[] lowerPhysicalPositions(
      final int iClusterId, final OPhysicalPosition physicalPosition) {
    OLowerPhysicalPositionsRequest request =
        new OLowerPhysicalPositionsRequest(physicalPosition, iClusterId);
    OLowerPhysicalPositionsResponse response =
        networkOperation(
            request,
            "Error on retrieving lower positions after " + physicalPosition.clusterPosition);
    return response.getPreviousPositions();
  }

  public OPhysicalPosition[] floorPhysicalPositions(
      final int clusterId, final OPhysicalPosition physicalPosition) {
    OFloorPhysicalPositionsRequest request =
        new OFloorPhysicalPositionsRequest(physicalPosition, clusterId);
    OFloorPhysicalPositionsResponse response =
        networkOperation(
            request,
            "Error on retrieving floor positions after " + physicalPosition.clusterPosition);
    return response.getPositions();
  }

  public long getSize() {
    OGetSizeRequest request = new OGetSizeRequest();
    OGetSizeResponse response = networkOperation(request, "Error on read database size");
    return response.getSize();
  }

  public long countRecords() {
    OCountRecordsRequest request = new OCountRecordsRequest();
    OCountRecordsResponse response =
        networkOperation(request, "Error on read database record count");
    return response.getCountRecords();
  }

  public long count(final int[] iClusterIds) {
    return count(iClusterIds, false);
  }

  public long count(final int[] iClusterIds, final boolean countTombstones) {
    OCountRequest request = new OCountRequest(iClusterIds, countTombstones);
    OCountResponse response =
        networkOperation(
            request, "Error on read record count in clusters: " + Arrays.toString(iClusterIds));
    return response.getCount();
  }

  /** Execute the command remotely and get the results back. */
  public Object command(final OCommandRequestText iCommand) {

    final boolean live = iCommand instanceof OLiveQuery;
    final ODatabaseDocumentInternal database = ODatabaseRecordThreadLocal.instance().get();
    final boolean asynch =
        iCommand instanceof OCommandRequestAsynch
            && ((OCommandRequestAsynch) iCommand).isAsynchronous();

    OCommandRequest request = new OCommandRequest(database, asynch, iCommand, live);
    OCommandResponse response =
        networkOperation(request, "Error on executing command: " + iCommand);
    return response.getResult();
  }

  public void stickToSession() {
    OStorageRemoteSession session = getCurrentSession();
    session.stickToSession();
  }

  public void unstickToSession() {
    OStorageRemoteSession session = getCurrentSession();
    session.unStickToSession();
  }

  public ORemoteQueryResult query(ODatabaseDocumentRemote db, String query, Object[] args) {
    int recordsPerPage = OGlobalConfiguration.QUERY_REMOTE_RESULTSET_PAGE_SIZE.getValueAsInteger();
    if (recordsPerPage <= 0) {
      recordsPerPage = 100;
    }
    OQueryRequest request =
        new OQueryRequest(
            "sql", query, args, OQueryRequest.QUERY, db.getSerializer(), recordsPerPage);
    OQueryResponse response = networkOperation(request, "Error on executing command: " + query);
    ORemoteResultSet rs =
        new ORemoteResultSet(
            db,
            response.getQueryId(),
            response.getResult(),
            response.getExecutionPlan(),
            response.getQueryStats(),
            response.isHasNextPage());
    if (response.isHasNextPage()) {
      stickToSession();
    } else {
      db.queryClosed(response.getQueryId());
    }
    return new ORemoteQueryResult(rs, response.isTxChanges(), response.isReloadMetadata());
  }

  public ORemoteQueryResult query(ODatabaseDocumentRemote db, String query, Map args) {
    int recordsPerPage = OGlobalConfiguration.QUERY_REMOTE_RESULTSET_PAGE_SIZE.getValueAsInteger();
    if (recordsPerPage <= 0) {
      recordsPerPage = 100;
    }
    OQueryRequest request =
        new OQueryRequest(
            "sql", query, args, OQueryRequest.QUERY, db.getSerializer(), recordsPerPage);
    OQueryResponse response = networkOperation(request, "Error on executing command: " + query);

    ORemoteResultSet rs =
        new ORemoteResultSet(
            db,
            response.getQueryId(),
            response.getResult(),
            response.getExecutionPlan(),
            response.getQueryStats(),
            response.isHasNextPage());
    if (response.isHasNextPage()) {
      stickToSession();
    } else {
      db.queryClosed(response.getQueryId());
    }
    return new ORemoteQueryResult(rs, response.isTxChanges(), response.isReloadMetadata());
  }

  public ORemoteQueryResult command(ODatabaseDocumentRemote db, String query, Object[] args) {
    int recordsPerPage = OGlobalConfiguration.QUERY_REMOTE_RESULTSET_PAGE_SIZE.getValueAsInteger();
    if (recordsPerPage <= 0) {
      recordsPerPage = 100;
    }
    OQueryRequest request =
        new OQueryRequest(
            "sql", query, args, OQueryRequest.COMMAND, db.getSerializer(), recordsPerPage);
    OQueryResponse response =
        networkOperationNoRetry(request, "Error on executing command: " + query);
    ORemoteResultSet rs =
        new ORemoteResultSet(
            db,
            response.getQueryId(),
            response.getResult(),
            response.getExecutionPlan(),
            response.getQueryStats(),
            response.isHasNextPage());
    if (response.isHasNextPage()) {
      stickToSession();
    } else {
      db.queryClosed(response.getQueryId());
    }
    return new ORemoteQueryResult(rs, response.isTxChanges(), response.isReloadMetadata());
  }

  public ORemoteQueryResult command(ODatabaseDocumentRemote db, String query, Map args) {
    int recordsPerPage = OGlobalConfiguration.QUERY_REMOTE_RESULTSET_PAGE_SIZE.getValueAsInteger();
    if (recordsPerPage <= 0) {
      recordsPerPage = 100;
    }
    OQueryRequest request =
        new OQueryRequest(
            "sql", query, args, OQueryRequest.COMMAND, db.getSerializer(), recordsPerPage);
    OQueryResponse response =
        networkOperationNoRetry(request, "Error on executing command: " + query);
    ORemoteResultSet rs =
        new ORemoteResultSet(
            db,
            response.getQueryId(),
            response.getResult(),
            response.getExecutionPlan(),
            response.getQueryStats(),
            response.isHasNextPage());
    if (response.isHasNextPage()) {
      stickToSession();
    } else {
      db.queryClosed(response.getQueryId());
    }
    return new ORemoteQueryResult(rs, response.isTxChanges(), response.isReloadMetadata());
  }

  public ORemoteQueryResult serverCommand(String query, Object[] args) {
    int recordsPerPage = OGlobalConfiguration.QUERY_REMOTE_RESULTSET_PAGE_SIZE.getValueAsInteger();
    if (recordsPerPage <= 0) {
      recordsPerPage = 100;
    }
    OServerQueryRequest request =
        new OServerQueryRequest(
            "sql",
            query,
            args,
            OServerQueryRequest.COMMAND,
            ORecordSerializerNetworkV37Client.INSTANCE,
            recordsPerPage);
    OServerQueryResponse response =
        networkOperationNoRetry(request, "Error on executing command: " + query);
    ORemoteResultSet rs =
        new ORemoteResultSet(
            null,
            response.getQueryId(),
            response.getResult(),
            response.getExecutionPlan(),
            response.getQueryStats(),
            response.isHasNextPage());
    // if (response.isHasNextPage()) {
    // stickToSession();
    // } else {
    // db.queryClosed(response.getQueryId());
    // }
    return new ORemoteQueryResult(rs, response.isTxChanges(), response.isReloadMetadata());
  }

  public ORemoteQueryResult serverCommand(String query, Map args) {
    int recordsPerPage = OGlobalConfiguration.QUERY_REMOTE_RESULTSET_PAGE_SIZE.getValueAsInteger();
    if (recordsPerPage <= 0) {
      recordsPerPage = 100;
    }
    OServerQueryRequest request =
        new OServerQueryRequest(
            "sql",
            query,
            args,
            OServerQueryRequest.COMMAND,
            ORecordSerializerNetworkV37Client.INSTANCE,
            recordsPerPage);
    OServerQueryResponse response =
        networkOperationNoRetry(request, "Error on executing command: " + query);
    ORemoteResultSet rs =
        new ORemoteResultSet(
            null,
            response.getQueryId(),
            response.getResult(),
            response.getExecutionPlan(),
            response.getQueryStats(),
            response.isHasNextPage());
    // if (response.isHasNextPage()) {
    // stickToSession();
    // } else {
    // db.queryClosed(response.getQueryId());
    // }
    return new ORemoteQueryResult(rs, response.isTxChanges(), response.isReloadMetadata());
  }

  public ORemoteQueryResult execute(
      ODatabaseDocumentRemote db, String language, String query, Object[] args) {
    int recordsPerPage = OGlobalConfiguration.QUERY_REMOTE_RESULTSET_PAGE_SIZE.getValueAsInteger();
    if (recordsPerPage <= 0) {
      recordsPerPage = 100;
    }
    OQueryRequest request =
        new OQueryRequest(
            language, query, args, OQueryRequest.EXECUTE, db.getSerializer(), recordsPerPage);
    OQueryResponse response =
        networkOperationNoRetry(request, "Error on executing command: " + query);
    ORemoteResultSet rs =
        new ORemoteResultSet(
            db,
            response.getQueryId(),
            response.getResult(),
            response.getExecutionPlan(),
            response.getQueryStats(),
            response.isHasNextPage());

    if (response.isHasNextPage()) {
      stickToSession();
    } else {
      db.queryClosed(response.getQueryId());
    }

    return new ORemoteQueryResult(rs, response.isTxChanges(), response.isReloadMetadata());
  }

  public ORemoteQueryResult execute(
      ODatabaseDocumentRemote db, String language, String query, Map args) {
    int recordsPerPage = OGlobalConfiguration.QUERY_REMOTE_RESULTSET_PAGE_SIZE.getValueAsInteger();
    if (recordsPerPage <= 0) {
      recordsPerPage = 100;
    }
    OQueryRequest request =
        new OQueryRequest(
            language, query, args, OQueryRequest.EXECUTE, db.getSerializer(), recordsPerPage);
    OQueryResponse response =
        networkOperationNoRetry(request, "Error on executing command: " + query);
    ORemoteResultSet rs =
        new ORemoteResultSet(
            db,
            response.getQueryId(),
            response.getResult(),
            response.getExecutionPlan(),
            response.getQueryStats(),
            response.isHasNextPage());
    if (response.isHasNextPage()) {
      stickToSession();
    } else {
      db.queryClosed(response.getQueryId());
    }
    return new ORemoteQueryResult(rs, response.isTxChanges(), response.isReloadMetadata());
  }

  public void closeQuery(ODatabaseDocumentRemote database, String queryId) {
    unstickToSession();
    OCloseQueryRequest request = new OCloseQueryRequest(queryId);
    networkOperation(request, "Error closing query: " + queryId);
  }

  public void fetchNextPage(ODatabaseDocumentRemote database, ORemoteResultSet rs) {
    int recordsPerPage = OGlobalConfiguration.QUERY_REMOTE_RESULTSET_PAGE_SIZE.getValueAsInteger();
    if (recordsPerPage <= 0) {
      recordsPerPage = 100;
    }
    OQueryNextPageRequest request = new OQueryNextPageRequest(rs.getQueryId(), recordsPerPage);
    OQueryResponse response =
        networkOperation(request, "Error on fetching next page for statment: " + rs.getQueryId());

    rs.fetched(
        response.getResult(),
        response.isHasNextPage(),
        response.getExecutionPlan(),
        response.getQueryStats());
    if (!response.isHasNextPage()) {
      unstickToSession();
      database.queryClosed(response.getQueryId());
    }
  }

  public List<ORecordOperation> commit(final OTransactionInternal iTx) {
    unstickToSession();
    final OCommit38Request request =
        new OCommit38Request(
            iTx.getId(),
            true,
            iTx.isUsingLog(),
            iTx.getRecordOperations(),
            iTx.getIndexOperations());

    final OCommit37Response response = networkOperationNoRetry(request, "Error on commit");
    for (OCommit37Response.OCreatedRecordResponse created : response.getCreated()) {
      iTx.updateIdentityAfterCommit(created.getCurrentRid(), created.getCreatedRid());
      ORecordOperation rop = iTx.getRecordEntry(created.getCurrentRid());
      if (rop != null) {
        if (created.getVersion() > rop.getRecord().getVersion() + 1)
          // IN CASE OF REMOTE CONFLICT STRATEGY FORCE UNLOAD DUE TO INVALID CONTENT
          rop.getRecord().unload();
        ORecordInternal.setVersion(rop.getRecord(), created.getVersion());
      }
    }
    for (OCommit37Response.OUpdatedRecordResponse updated : response.getUpdated()) {
      ORecordOperation rop = iTx.getRecordEntry(updated.getRid());
      if (rop != null) {
        if (updated.getVersion() > rop.getRecord().getVersion() + 1)
          // IN CASE OF REMOTE CONFLICT STRATEGY FORCE UNLOAD DUE TO INVALID CONTENT
          rop.getRecord().unload();
        ORecordInternal.setVersion(rop.getRecord(), updated.getVersion());
      }
    }
    updateCollectionsFromChanges(
        ((OTransactionOptimistic) iTx).getDatabase().getSbTreeCollectionManager(),
        response.getCollectionChanges());
    // SET ALL THE RECORDS AS UNDIRTY
    for (ORecordOperation txEntry : iTx.getRecordOperations())
      ORecordInternal.unsetDirty(txEntry.getRecord());

    // UPDATE THE CACHE ONLY IF THE ITERATOR ALLOWS IT.
    OTransactionAbstract.updateCacheFromEntries(iTx.getDatabase(), iTx.getRecordOperations(), true);
    return null;
  }

  public void rollback(OTransactionInternal iTx) {
    try {
      if (((OTransactionOptimistic) iTx).isAlreadyCleared()
          && getCurrentSession().getAllServerSessions().size() > 0) {
        ORollbackTransactionRequest request = new ORollbackTransactionRequest(iTx.getId());

        ORollbackTransactionResponse response =
            networkOperation(request, "Error on fetching next page for statment: " + request);
      }
    } finally {
      unstickToSession();
    }
  }

  public int getClusterIdByName(final String iClusterName) {
    stateLock.readLock().lock();
    try {

      if (iClusterName == null) return -1;

      if (Character.isDigit(iClusterName.charAt(0))) return Integer.parseInt(iClusterName);

      final OCluster cluster = clusterMap.get(iClusterName.toLowerCase(Locale.ENGLISH));
      if (cluster == null) return -1;

      return cluster.getId();
    } finally {
      stateLock.readLock().unlock();
      ;
    }
  }

  public int getDefaultClusterId() {
    return defaultClusterId;
  }

  public void setDefaultClusterId(int defaultClusterId) {
    this.defaultClusterId = defaultClusterId;
  }

  public int addCluster(final String iClusterName, final Object... iArguments) {
    return addCluster(iClusterName, -1);
  }

  public int addCluster(final String iClusterName, final int iRequestedId) {
    OAddClusterRequest request = new OAddClusterRequest(iRequestedId, iClusterName);
    OAddClusterResponse response = networkOperationNoRetry(request, "Error on add new cluster");
    addNewClusterToConfiguration(response.getClusterId(), iClusterName);
    return response.getClusterId();
  }

  public String getClusterNameById(int clusterId) {
    stateLock.readLock().lock();
    try {
      if (clusterId < 0 || clusterId >= clusters.length) {
        throw new OStorageException("Cluster with id " + clusterId + " does not exist");
      }

      final OCluster cluster = clusters[clusterId];
      return cluster.getName();
    } finally {
      stateLock.readLock().unlock();
    }
  }

  public long getClusterRecordsSizeById(int clusterId) {
    throw new UnsupportedOperationException();
  }

  public long getClusterRecordsSizeByName(String clusterName) {
    throw new UnsupportedOperationException();
  }

  public String getClusterRecordConflictStrategy(int clusterId) {
    throw new UnsupportedOperationException();
  }

  public String getClusterEncryption(int clusterId) {
    throw new UnsupportedOperationException();
  }

  public boolean isSystemCluster(int clusterId) {
    throw new UnsupportedOperationException();
  }

  public long getLastClusterPosition(int clusterId) {
    throw new UnsupportedOperationException();
  }

  public long getClusterNextPosition(int clusterId) {
    throw new UnsupportedOperationException();
  }

  public OPaginatedCluster.RECORD_STATUS getRecordStatus(ORID rid) {
    throw new UnsupportedOperationException();
  }

  public boolean dropCluster(final int iClusterId) {

    ODropClusterRequest request = new ODropClusterRequest(iClusterId);

    ODropClusterResponse response =
        networkOperationNoRetry(request, "Error on removing of cluster");
    if (response.getResult()) removeClusterFromConfiguration(iClusterId);
    return response.getResult();
  }

  public String getClusterName(int clusterId) {
    final OCluster cluster;
    stateLock.readLock().lock();
    try {

      if (clusterId == ORID.CLUSTER_ID_INVALID)
        // GET THE DEFAULT CLUSTER
        clusterId = defaultClusterId;

      if (clusterId >= clusters.length) {
        stateLock.readLock().unlock();
        reload();
        stateLock.readLock().lock();
      }

      if (clusterId < clusters.length) {
        return clusters[clusterId].getName();
      }
    } finally {
      stateLock.readLock().unlock();
    }

    throw new OStorageException("Cluster " + clusterId + " is absent in storage.");
  }

  public boolean setClusterAttribute(int id, OCluster.ATTRIBUTES attribute, Object value) {
    return false;
  }

  public void removeClusterFromConfiguration(int iClusterId) {
    stateLock.writeLock().lock();
    try {
      // If this is false the clusters may be already update by a push
      if (clusters.length > iClusterId && clusters[iClusterId] != null) {
        // Remove cluster locally waiting for the push
        final OCluster cluster = clusters[iClusterId];
        clusters[iClusterId] = null;
        clusterMap.remove(cluster.getName());
        ((OStorageConfigurationRemote) configuration)
            .dropCluster(iClusterId); // endResponse must be called before this line, which
        // call updateRecord
      }
    } finally {
      stateLock.writeLock().unlock();
    }
  }

  public void synch() {}

  public String getPhysicalClusterNameById(final int iClusterId) {
    stateLock.readLock().lock();
    try {

      if (iClusterId >= clusters.length) return null;

      final OCluster cluster = clusters[iClusterId];
      return cluster != null ? cluster.getName() : null;

    } finally {
      stateLock.readLock().unlock();
    }
  }

  public int getClusterMap() {
    stateLock.readLock().lock();
    try {
      return clusterMap.size();
    } finally {
      stateLock.readLock().unlock();
    }
  }

  public Collection<OCluster> getClusterInstances() {
    stateLock.readLock().lock();
    try {

      return Arrays.asList(clusters);

    } finally {
      stateLock.readLock().unlock();
    }
  }

  public long getVersion() {
    throw new UnsupportedOperationException("getVersion");
  }

  public ODocument getClusterConfiguration() {
    return clusterConfiguration;
  }

  /** Ends the request and unlock the write lock */
  public void endRequest(final OChannelBinaryAsynchClient iNetwork) throws IOException {
    if (iNetwork == null) return;

    iNetwork.flush();
    iNetwork.releaseWriteLock();
  }

  /** End response reached: release the channel in the pool to being reused */
  public void endResponse(final OChannelBinaryAsynchClient iNetwork) throws IOException {
    iNetwork.endResponse();
  }

  public boolean isRemote() {
    return true;
  }

  public boolean isPermanentRequester() {
    return false;
  }

  public ORecordConflictStrategy getRecordConflictStrategy() {
    throw new UnsupportedOperationException("getRecordConflictStrategy");
  }

  public void setConflictStrategy(final ORecordConflictStrategy iResolver) {
    throw new UnsupportedOperationException("setConflictStrategy");
  }

  public String getURL() {
    return OEngineRemote.NAME + ":" + url;
  }

  public int getClusters() {
    stateLock.readLock().lock();
    try {
      return clusterMap.size();
    } finally {
      stateLock.readLock().unlock();
    }
  }

  public String getType() {
    return OEngineRemote.NAME;
  }

  public String getUserName() {
    final OStorageRemoteSession session = getCurrentSession();
    if (session == null) return null;
    return session.connectionUserName;
  }

  protected String reopenRemoteDatabase() throws IOException {
    String currentURL = getCurrentServerURL();
    do {
      do {
        final OChannelBinaryAsynchClient network = getNetwork(currentURL);
        try {
          OStorageRemoteSession session = getCurrentSession();
          OStorageRemoteNodeSession nodeSession =
              session.getOrCreateServerSession(network.getServerURL());
          if (nodeSession == null || !nodeSession.isValid()) {
            openRemoteDatabase(network);
            return network.getServerURL();
          } else {
            OReopenRequest request = new OReopenRequest();

            try {
              network.writeByte(request.getCommand());
              network.writeInt(nodeSession.getSessionId());
              network.writeBytes(nodeSession.getToken());
              request.write(network, session);
            } finally {
              endRequest(network);
            }

            OReopenResponse response = request.createResponse();
            try {
              byte[] newToken = network.beginResponse(nodeSession.getSessionId(), true);
              response.read(network, session);
              if (newToken != null && newToken.length > 0) {
                nodeSession.setSession(response.getSessionId(), newToken);
              } else {
                nodeSession.setSession(response.getSessionId(), nodeSession.getToken());
              }
              OLogManager.instance()
                  .debug(
                      this,
                      "Client connected to %s with session id=%d",
                      network.getServerURL(),
                      response.getSessionId());
              return currentURL;
            } finally {
              endResponse(network);
              connectionManager.release(network);
            }
          }
        } catch (OIOException e) {
          if (network != null) {
            // REMOVE THE NETWORK CONNECTION IF ANY
            connectionManager.remove(network);
          }

          OLogManager.instance().error(this, "Cannot open database with url " + currentURL, e);
        } catch (OOfflineNodeException e) {
          if (network != null) {
            // REMOVE THE NETWORK CONNECTION IF ANY
            connectionManager.remove(network);
          }

          OLogManager.instance().debug(this, "Cannot open database with url " + currentURL, e);
        } catch (OSecurityException ex) {
          OLogManager.instance().debug(this, "Invalidate token for url=%s", ex, currentURL);
          OStorageRemoteSession session = getCurrentSession();
          session.removeServerSession(currentURL);

          if (network != null) {
            // REMOVE THE NETWORK CONNECTION IF ANY
            try {
              connectionManager.remove(network);
            } catch (Exception e) {
              // IGNORE ANY EXCEPTION
              OLogManager.instance()
                  .debug(this, "Cannot remove connection or database url=" + currentURL, e);
            }
          }
        } catch (OException e) {
          connectionManager.release(network);
          // PROPAGATE ANY OTHER ORIENTDB EXCEPTION
          throw e;

        } catch (Exception e) {
          OLogManager.instance().debug(this, "Cannot open database with url " + currentURL, e);
          if (network != null) {
            // REMOVE THE NETWORK CONNECTION IF ANY
            try {
              connectionManager.remove(network);
            } catch (Exception ex) {
              // IGNORE ANY EXCEPTION
              OLogManager.instance()
                  .debug(this, "Cannot remove connection or database url=" + currentURL, e);
            }
          }
        }
      } while (connectionManager.getAvailableConnections(currentURL) > 0);

      currentURL = useNewServerURL(currentURL);

    } while (currentURL != null);

    // REFILL ORIGINAL SERVER LIST
    serverURLs.reloadOriginalURLs();

    throw new OStorageException(
        "Cannot create a connection to remote server address(es): " + serverURLs.getUrls());
  }

  protected void openRemoteDatabase() throws IOException {
    final String currentURL = getNextAvailableServerURL(true, getCurrentSession());
    openRemoteDatabase(currentURL);
  }

  public void openRemoteDatabase(OChannelBinaryAsynchClient network) throws IOException {

    OStorageRemoteSession session = getCurrentSession();
    OStorageRemoteNodeSession nodeSession =
        session.getOrCreateServerSession(network.getServerURL());
    OOpen37Request request =
        new OOpen37Request(name, session.connectionUserName, session.connectionUserPassword);
    try {
      network.writeByte(request.getCommand());
      network.writeInt(nodeSession.getSessionId());
      network.writeBytes(null);
      request.write(network, session);
    } finally {
      endRequest(network);
    }
    final int sessionId;
    OOpen37Response response = request.createResponse();
    try {
      network.beginResponse(nodeSession.getSessionId(), true);
      response.read(network, session);
    } finally {
      endResponse(network);
      connectionManager.release(network);
    }
    sessionId = response.getSessionId();
    byte[] token = response.getSessionToken();
    if (token.length == 0) {
      token = null;
    }

    nodeSession.setSession(sessionId, token);

    OLogManager.instance()
        .debug(
            this, "Client connected to %s with session id=%d", network.getServerURL(), sessionId);

    // READ CLUSTER CONFIGURATION
    // updateClusterConfiguration(network.getServerURL(),
    // response.getDistributedConfiguration());

    // This need to be protected by a lock for now, let's see in future
    stateLock.writeLock().lock();
    try {
      status = STATUS.OPEN;
    } finally {
      stateLock.writeLock().unlock();
    }
  }

  private void initPush(OStorageRemoteSession session) {
    if (pushThread == null) {
      stateLock.writeLock().lock();
      try {
        if (pushThread == null) {
          pushThread =
              new OStorageRemotePushThread(
                  this,
                  getCurrentServerURL(),
                  connectionRetryDelay,
                  configuration
                      .getContextConfiguration()
                      .getValueAsLong(OGlobalConfiguration.NETWORK_REQUEST_TIMEOUT));
          pushThread.start();
          subscribeStorageConfiguration(session);
          subscribeDistributedConfiguration(session);
          subscribeSchema(session);
          subscribeIndexManager(session);
          subscribeFunctions(session);
          subscribeSequences(session);
        }
      } finally {
        stateLock.writeLock().unlock();
      }
    }
  }

  private void subscribeDistributedConfiguration(OStorageRemoteSession nodeSession) {
    pushThread.subscribe(new OSubscribeDistributedConfigurationRequest(), nodeSession);
  }

  private void subscribeStorageConfiguration(OStorageRemoteSession nodeSession) {
    pushThread.subscribe(new OSubscribeStorageConfigurationRequest(), nodeSession);
  }

  private void subscribeSchema(OStorageRemoteSession nodeSession) {
    pushThread.subscribe(new OSubscribeSchemaRequest(), nodeSession);
  }

  private void subscribeFunctions(OStorageRemoteSession nodeSession) {
    pushThread.subscribe(new OSubscribeFunctionsRequest(), nodeSession);
  }

  private void subscribeSequences(OStorageRemoteSession nodeSession) {
    pushThread.subscribe(new OSubscribeSequencesRequest(), nodeSession);
  }

  private void subscribeIndexManager(OStorageRemoteSession nodeSession) {
    pushThread.subscribe(new OSubscribeIndexManagerRequest(), nodeSession);
  }

  protected void openRemoteDatabase(String currentURL) {
    do {
      do {
        OChannelBinaryAsynchClient network = null;
        try {
          network = getNetwork(currentURL);
          openRemoteDatabase(network);
          return;
        } catch (ODistributedRedirectException e) {
          connectionManager.release(network);
          // RECONNECT TO THE SERVER SUGGESTED IN THE EXCEPTION
          currentURL = e.getToServerAddress();
        } catch (OModificationOperationProhibitedException mope) {
          connectionManager.release(network);
          handleDBFreeze();
          currentURL = useNewServerURL(currentURL);
        } catch (OOfflineNodeException e) {
          connectionManager.release(network);
          currentURL = useNewServerURL(currentURL);
        } catch (OIOException e) {
          if (network != null) {
            // REMOVE THE NETWORK CONNECTION IF ANY
            connectionManager.remove(network);
          }

          OLogManager.instance().debug(this, "Cannot open database with url " + currentURL, e);

        } catch (OException e) {
          connectionManager.release(network);
          // PROPAGATE ANY OTHER ORIENTDB EXCEPTION
          throw e;

        } catch (IOException e) {
          if (network != null) {
            connectionManager.remove(network);
          }
        } catch (Exception e) {
          if (network != null) {
            // REMOVE THE NETWORK CONNECTION IF ANY
            connectionManager.remove(network);
          }
          throw OException.wrapException(new OStorageException(e.getMessage()), e);
        }
      } while (connectionManager.getReusableConnections(currentURL) > 0);

      currentURL = useNewServerURL(currentURL);

    } while (currentURL != null);

    // REFILL ORIGINAL SERVER LIST
    serverURLs.reloadOriginalURLs();

    throw new OStorageException(
        "Cannot create a connection to remote server address(es): " + serverURLs.getUrls());
  }

  protected String useNewServerURL(final String iUrl) {
    int pos = iUrl.indexOf('/');
    if (pos >= iUrl.length() - 1)
      // IGNORE ENDING /
      pos = -1;

    final String url = pos > -1 ? iUrl.substring(0, pos) : iUrl;
    String newUrl = serverURLs.removeAndGet(url);
    OStorageRemoteSession session = getCurrentSession();
    if (session != null) {
      session.currentUrl = newUrl;
      session.serverURLIndex = 0;
    }
    return newUrl;
  }

  /** Parse the URLs. Multiple URLs must be separated by semicolon (;) */
  protected void parseServerURLs() {
    this.name = serverURLs.parseServerUrls(this.url, getClientConfiguration());
  }

  /**
   * Acquire a network channel from the pool. Don't lock the write stream since the connection usage
   * is exclusive.
   *
   * @param iCommand id. Ids described at {@link OChannelBinaryProtocol}
   * @return connection to server
   */
  public OChannelBinaryAsynchClient beginRequest(
      final OChannelBinaryAsynchClient network, final byte iCommand, OStorageRemoteSession session)
      throws IOException {
    network.beginRequest(iCommand, session);
    return network;
  }

  protected String getNextAvailableServerURL(
      boolean iIsConnectOperation, OStorageRemoteSession session) {

    OContextConfiguration config = null;
    if (configuration != null) config = configuration.getContextConfiguration();
    return serverURLs.getNextAvailableServerURL(
        iIsConnectOperation, session, config, connectionStrategy);
  }

  protected String getCurrentServerURL() {
    return serverURLs.getServerURFromList(
        false, getCurrentSession(), configuration.getContextConfiguration());
  }

  public OChannelBinaryAsynchClient getNetwork(final String iCurrentURL) {
    return getNetwork(iCurrentURL, connectionManager, clientConfiguration);
  }

  public static OChannelBinaryAsynchClient getNetwork(
      final String iCurrentURL,
      ORemoteConnectionManager connectionManager,
      OContextConfiguration config) {
    OChannelBinaryAsynchClient network;
    do {
      try {
        network = connectionManager.acquire(iCurrentURL, config);
      } catch (OIOException cause) {
        throw cause;
      } catch (Exception cause) {
        throw OException.wrapException(
            new OStorageException("Cannot open a connection to remote server: " + iCurrentURL),
            cause);
      }
      if (!network.tryLock()) {
        // CANNOT LOCK IT, MAYBE HASN'T BE CORRECTLY UNLOCKED BY PREVIOUS USER?
        OLogManager.instance()
            .error(
                OStorageRemote.class,
                "Removing locked network channel '%s' (connected=%s)...",
                null,
                iCurrentURL,
                network.isConnected());
        connectionManager.remove(network);
        network = null;
      }
    } while (network == null);
    return network;
  }

  public static void beginResponse(
      OChannelBinaryAsynchClient iNetwork, OStorageRemoteSession session) throws IOException {
    OStorageRemoteNodeSession nodeSession = session.getServerSession(iNetwork.getServerURL());
    byte[] newToken = iNetwork.beginResponse(nodeSession.getSessionId(), true);
    if (newToken != null && newToken.length > 0) {
      nodeSession.setSession(nodeSession.getSessionId(), newToken);
    }
  }

  private boolean handleDBFreeze() {

    boolean retry;
    OLogManager.instance()
        .warn(
            this,
            "DB is frozen will wait for "
                + getClientConfiguration()
                    .getValue(OGlobalConfiguration.CLIENT_DB_RELEASE_WAIT_TIMEOUT)
                + " ms. and then retry.");
    retry = true;
    try {
      Thread.sleep(
          getClientConfiguration()
              .getValueAsInteger(OGlobalConfiguration.CLIENT_DB_RELEASE_WAIT_TIMEOUT));
    } catch (InterruptedException ie) {
      retry = false;

      Thread.currentThread().interrupt();
    }
    return retry;
  }

  public void updateStorageConfiguration(OStorageConfiguration storageConfiguration) {
    if (status != STATUS.OPEN) return;
    stateLock.writeLock().lock();
    try {
      if (status != STATUS.OPEN) return;
      this.configuration = storageConfiguration;
      final List<OStorageClusterConfiguration> configClusters = storageConfiguration.getClusters();
      OCluster[] clusters = new OCluster[configClusters.size()];
      for (OStorageClusterConfiguration clusterConfig : configClusters) {
        if (clusterConfig != null) {
          final OClusterRemote cluster = new OClusterRemote();
          String clusterName = clusterConfig.getName();
          final int clusterId = clusterConfig.getId();
          if (clusterName != null) {
            clusterName = clusterName.toLowerCase(Locale.ENGLISH);
            cluster.configure(clusterId, clusterName);
            if (clusterId >= clusters.length) clusters = Arrays.copyOf(clusters, clusterId + 1);
            clusters[clusterId] = cluster;
          }
        }
      }

      this.clusters = clusters;
      clusterMap.clear();
      for (int i = 0; i < clusters.length; ++i) {
        if (clusters[i] != null) clusterMap.put(clusters[i].getName(), clusters[i]);
      }
      final OCluster defaultCluster = clusterMap.get(OStorage.CLUSTER_DEFAULT_NAME);
      if (defaultCluster != null)
        defaultClusterId = clusterMap.get(OStorage.CLUSTER_DEFAULT_NAME).getId();
    } finally {
      stateLock.writeLock().unlock();
    }
  }

  protected OStorageRemoteSession getCurrentSession() {
    ODatabaseDocumentInternal db = null;
    if (ODatabaseRecordThreadLocal.instance() != null)
      db = ODatabaseRecordThreadLocal.instance().getIfDefined();
    ODatabaseDocumentRemote remote =
        (ODatabaseDocumentRemote) ODatabaseDocumentTxInternal.getInternal(db);
    if (remote == null) return null;
    OStorageRemoteSession session = remote.getSessionMetadata();
    if (session == null) {
      session = new OStorageRemoteSession(sessionSerialId.decrementAndGet());
      sessions.add(session);
      remote.setSessionMetadata(session);
    }
    return session;
  }

  public boolean isClosed() {
    if (status == STATUS.CLOSED) return true;
    final OStorageRemoteSession session = getCurrentSession();
    if (session == null) return false;
    return session.isClosed();
  }

  public OStorageRemote copy(
      final ODatabaseDocumentRemote source, final ODatabaseDocumentRemote dest) {
    ODatabaseDocumentInternal origin = null;
    if (ODatabaseRecordThreadLocal.instance() != null)
      origin = ODatabaseRecordThreadLocal.instance().getIfDefined();

    origin = ODatabaseDocumentTxInternal.getInternal(origin);

    final OStorageRemoteSession session = source.getSessionMetadata();
    if (session != null) {
      // TODO:may run a session reopen
      final OStorageRemoteSession newSession =
          new OStorageRemoteSession(sessionSerialId.decrementAndGet());
      newSession.connectionUserName = session.connectionUserName;
      newSession.connectionUserPassword = session.connectionUserPassword;
      dest.setSessionMetadata(newSession);
    }
    try {
      dest.activateOnCurrentThread();
      openRemoteDatabase();
    } catch (IOException e) {
      OLogManager.instance().error(this, "Error during database open", e);
    } finally {
      ODatabaseRecordThreadLocal.instance().set(origin);
    }
    return this;
  }

  public void importDatabase(
      final String options,
      final InputStream inputStream,
      final String name,
      final OCommandOutputListener listener) {
    OImportRequest request = new OImportRequest(inputStream, options, name);

    OImportResponse response =
        networkOperationRetryTimeout(
            request,
            "Error sending import request",
            0,
            getClientConfiguration()
                .getValueAsInteger(OGlobalConfiguration.NETWORK_REQUEST_TIMEOUT));

    for (String message : response.getMessages()) {
      listener.onMessage(message);
    }
  }

  public void addNewClusterToConfiguration(int clusterId, String iClusterName) {
    stateLock.writeLock().lock();
    try {
      // If this if is false maybe the content was already update by the push
      if (clusters.length <= clusterId || clusters[clusterId] == null) {
        // Adding the cluster waiting for the push
        final OClusterRemote cluster = new OClusterRemote();
        cluster.configure(clusterId, iClusterName.toLowerCase(Locale.ENGLISH));

        if (clusters.length <= clusterId) clusters = Arrays.copyOf(clusters, clusterId + 1);
        clusters[cluster.getId()] = cluster;
        clusterMap.put(cluster.getName().toLowerCase(Locale.ENGLISH), cluster);
      }
    } finally {
      stateLock.writeLock().unlock();
    }
  }

  public void beginTransaction(
      ODatabaseDocumentRemote database, OTransactionOptimistic transaction) {
    OBeginTransaction38Request request =
        new OBeginTransaction38Request(
            transaction.getId(),
            true,
            transaction.isUsingLog(),
            transaction.getRecordOperations(),
            transaction.getIndexOperations());
    OBeginTransactionResponse response =
        networkOperationNoRetry(request, "Error on remote transaction begin");
    for (Map.Entry<ORID, ORID> entry : response.getUpdatedIds().entrySet()) {
      transaction.updateIdentityAfterCommit(entry.getKey(), entry.getValue());
    }
    stickToSession();
  }

  public void reBeginTransaction(
      ODatabaseDocumentRemote database, OTransactionOptimistic transaction) {
    ORebeginTransaction38Request request =
        new ORebeginTransaction38Request(
            transaction.getId(),
            transaction.isUsingLog(),
            transaction.getRecordOperations(),
            transaction.getIndexOperations());
    OBeginTransactionResponse response =
        networkOperationNoRetry(request, "Error on remote transaction begin");
    for (Map.Entry<ORID, ORID> entry : response.getUpdatedIds().entrySet()) {
      transaction.updateIdentityAfterCommit(entry.getKey(), entry.getValue());
    }
  }

  public void fetchTransaction(ODatabaseDocumentRemote remote) {
    OTransactionOptimisticClient transaction = remote.getActiveTx();
    OFetchTransaction38Request request = new OFetchTransaction38Request(transaction.getId());
    OFetchTransaction38Response response =
        networkOperation(request, "Error fetching transaction from server side");
    transaction.replaceContent(response.getOperations(), response.getIndexChanges());
  }

  public OBinaryPushRequest createPush(byte type) {
    switch (type) {
      case OChannelBinaryProtocol.REQUEST_PUSH_DISTRIB_CONFIG:
        return new OPushDistributedConfigurationRequest();
      case OChannelBinaryProtocol.REQUEST_PUSH_LIVE_QUERY:
        return new OLiveQueryPushRequest();
      case OChannelBinaryProtocol.REQUEST_PUSH_STORAGE_CONFIG:
        return new OPushStorageConfigurationRequest();
      case OChannelBinaryProtocol.REQUEST_PUSH_SCHEMA:
        return new OPushSchemaRequest();
      case OChannelBinaryProtocol.REQUEST_PUSH_INDEX_MANAGER:
        return new OPushIndexManagerRequest();
      case OChannelBinaryProtocol.REQUEST_PUSH_FUNCTIONS:
        return new OPushFunctionsRequest();
      case OChannelBinaryProtocol.REQUEST_PUSH_SEQUENCES:
        return new OPushSequencesRequest();
    }
    return null;
  }

  public OBinaryPushResponse executeUpdateDistributedConfig(
      OPushDistributedConfigurationRequest request) {
    serverURLs.updateDistributedNodes(request.getHosts(), configuration.getContextConfiguration());
    return null;
  }

  public OBinaryPushResponse executeUpdateFunction(OPushFunctionsRequest request) {
    ODatabaseDocumentRemote.updateFunction(this);
    return null;
  }

  public OBinaryPushResponse executeUpdateSequences(OPushSequencesRequest request) {
    ODatabaseDocumentRemote.updateSequences(this);
    return null;
  }

  public OBinaryPushResponse executeUpdateStorageConfig(OPushStorageConfigurationRequest payload) {
    final OStorageConfiguration storageConfiguration =
        new OStorageConfigurationRemote(
            ORecordSerializerFactory.instance().getDefaultRecordSerializer().toString(),
            payload.getPayload(),
            clientConfiguration);

    updateStorageConfiguration(storageConfiguration);
    return null;
  }

  public OBinaryPushResponse executeUpdateSchema(OPushSchemaRequest request) {
    ODocument schema = request.getSchema();
    ORecordInternal.setIdentity(schema, new ORecordId(getConfiguration().getSchemaRecordId()));
    ODatabaseDocumentRemote.updateSchema(this, schema);
    return null;
  }

  public OBinaryPushResponse executeUpdateIndexManager(OPushIndexManagerRequest request) {
    ODocument indexManager = request.getIndexManager();
    ORecordInternal.setIdentity(
        indexManager, new ORecordId(getConfiguration().getIndexMgrRecordId()));
    ODatabaseDocumentRemote.updateIndexManager(this, indexManager);
    return null;
  }

  public OLiveQueryMonitor liveQuery(
      ODatabaseDocumentRemote database,
      String query,
      OLiveQueryClientListener listener,
      Object[] params) {

    OSubscribeLiveQueryRequest request = new OSubscribeLiveQueryRequest(query, params);
    OSubscribeLiveQueryResponse response = pushThread.subscribe(request, getCurrentSession());
    if (response == null) {
      throw new ODatabaseException(
          "Impossible to start the live query, check server log for additional information");
    }
    registerLiveListener(response.getMonitorId(), listener);
    return new OLiveQueryMonitorRemote(database, response.getMonitorId());
  }

  public OLiveQueryMonitor liveQuery(
      ODatabaseDocumentRemote database,
      String query,
      OLiveQueryClientListener listener,
      Map<String, ?> params) {
    OSubscribeLiveQueryRequest request =
        new OSubscribeLiveQueryRequest(query, (Map<String, Object>) params);
    OSubscribeLiveQueryResponse response = pushThread.subscribe(request, getCurrentSession());
    if (response == null) {
      throw new ODatabaseException(
          "Impossible to start the live query, check server log for additional information");
    }
    registerLiveListener(response.getMonitorId(), listener);
    return new OLiveQueryMonitorRemote(database, response.getMonitorId());
  }

  public void unsubscribeLive(ODatabaseDocumentRemote database, int monitorId) {
    OUnsubscribeRequest request =
        new OUnsubscribeRequest(new OUnsubscribeLiveQueryRequest(monitorId));
    networkOperation(request, "Error on unsubscribe of live query");
  }

  public void registerLiveListener(int monitorId, OLiveQueryClientListener listener) {
    liveQueryListener.put(monitorId, listener);
  }

  public static HashMap<String, Object> paramsArrayToParamsMap(Object[] positionalParams) {
    HashMap<String, Object> params = new HashMap<>();
    if (positionalParams != null) {
      for (int i = 0; i < positionalParams.length; i++) {
        params.put(Integer.toString(i), positionalParams[i]);
      }
    }
    return params;
  }

  public void executeLiveQueryPush(OLiveQueryPushRequest pushRequest) {
    OLiveQueryClientListener listener = liveQueryListener.get(pushRequest.getMonitorId());
    if (listener.onEvent(pushRequest)) {
      liveQueryListener.remove(pushRequest.getMonitorId());
    }
  }

  public void onPushReconnect(String host) {
    if (status != STATUS.OPEN) {
      // AVOID RECONNECT ON CLOSE
      return;
    }
    OStorageRemoteSession aValidSession = null;
    for (OStorageRemoteSession session : sessions) {
      if (session.getServerSession(host) != null) {
        aValidSession = session;
        break;
      }
    }
    if (aValidSession != null) {
      subscribeDistributedConfiguration(aValidSession);
      subscribeStorageConfiguration(aValidSession);
    } else {
      OLogManager.instance()
          .warn(
              this,
              "Cannot find a valid session for subscribe for event to host '%s' forward the subscribe for the next session open ",
              host);
      OStorageRemotePushThread old;
      stateLock.writeLock().lock();
      try {
        old = pushThread;
        pushThread = null;
      } finally {
        stateLock.writeLock().unlock();
      }
      old.shutdown();
    }
  }

  public void onPushDisconnect(OChannelBinary network, Exception e) {
    if (this.connectionManager.getPool(((OChannelBinaryAsynchClient) network).getServerURL())
        != null) {
      this.connectionManager.remove((OChannelBinaryAsynchClient) network);
    }
    if (e instanceof InterruptedException) {
      for (OLiveQueryClientListener liveListener : liveQueryListener.values()) {
        liveListener.onEnd();
      }
    } else {
      for (OLiveQueryClientListener liveListener : liveQueryListener.values()) {
        if (e instanceof OException) {
          liveListener.onError((OException) e);
        } else {
          liveListener.onError(
              OException.wrapException(new ODatabaseException("Live query disconnection "), e));
        }
      }
    }
  }

  public OLockRecordResponse lockRecord(
      OIdentifiable iRecord, LOCKING_STRATEGY lockingStrategy, long timeout) {
    OExperimentalRequest request =
        new OExperimentalRequest(
            new OLockRecordRequest(iRecord.getIdentity(), lockingStrategy, timeout));
    OExperimentalResponse response = networkOperation(request, "Error locking record");
    OLockRecordResponse realResponse = (OLockRecordResponse) response.getResponse();
    return realResponse;
  }

  public void unlockRecord(OIdentifiable iRecord) {
    OExperimentalRequest request =
        new OExperimentalRequest(new OUnlockRecordRequest(iRecord.getIdentity()));
    OExperimentalResponse response = networkOperation(request, "Error locking record");
    OUnlockRecordResponse realResponse = (OUnlockRecordResponse) response.getResponse();
  }

  public void returnSocket(OChannelBinary network) {
    this.connectionManager.remove((OChannelBinaryAsynchClient) network);
  }

  public void setSchemaRecordId(String schemaRecordId) {
    throw new UnsupportedOperationException();
  }

  public void setDateFormat(String dateFormat) {
    throw new UnsupportedOperationException();
  }

  public void setTimeZone(TimeZone timeZoneValue) {
    throw new UnsupportedOperationException();
  }

  public void setLocaleLanguage(String locale) {
    throw new UnsupportedOperationException();
  }

  public void setCharset(String charset) {
    throw new UnsupportedOperationException();
  }

  public void setIndexMgrRecordId(String indexMgrRecordId) {
    throw new UnsupportedOperationException();
  }

  public void setDateTimeFormat(String dateTimeFormat) {
    throw new UnsupportedOperationException();
  }

  public void setLocaleCountry(String localeCountry) {
    throw new UnsupportedOperationException();
  }

  public void setClusterSelection(String clusterSelection) {
    throw new UnsupportedOperationException();
  }

  public void setMinimumClusters(int minimumClusters) {
    throw new UnsupportedOperationException();
  }

  public void setValidation(boolean validation) {
    throw new UnsupportedOperationException();
  }

  public void removeProperty(String property) {
    throw new UnsupportedOperationException();
  }

  public void setProperty(String property, String value) {
    throw new UnsupportedOperationException();
  }

  public void setRecordSerializer(String recordSerializer, int version) {
    throw new UnsupportedOperationException();
  }

  public void clearProperties() {
    throw new UnsupportedOperationException();
  }

  public List<String> getServerURLs() {
    return serverURLs.getUrls();
  }

  public OSharedContext getSharedContext() {
    return sharedContext;
  }

  public boolean isDistributed() {
    return false;
  }

  public STATUS getStatus() {
    return status;
  }

  public void close() {
    close(false, false);
  }

  public boolean dropCluster(final String iClusterName) {
    return dropCluster(getClusterIdByName(iClusterName));
  }

  public OCurrentStorageComponentsFactory getComponentsFactory() {
    return componentsFactory;
  }

  @Override
  public OStorage getUnderlying() {
    return null;
  }

  @Override
  public int[] getClustersIds(Set<String> filterClusters) {
    throw new UnsupportedOperationException();
  }
}
