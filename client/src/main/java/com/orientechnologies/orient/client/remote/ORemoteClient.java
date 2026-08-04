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
import com.orientechnologies.common.log.OLogger;
import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.common.util.OCommonConst;
import com.orientechnologies.orient.client.ONotSendRequestException;
import com.orientechnologies.orient.client.binary.OChannelBinaryAsynchClient;
import com.orientechnologies.orient.client.remote.db.document.ODatabaseDocumentRemote;
import com.orientechnologies.orient.client.remote.db.document.OLiveQueryMonitorRemote;
import com.orientechnologies.orient.client.remote.db.document.OMetadataPushListener;
import com.orientechnologies.orient.client.remote.db.document.OTransactionOptimisticClient;
import com.orientechnologies.orient.client.remote.message.OAddClusterRequest;
import com.orientechnologies.orient.client.remote.message.OAddClusterResponse;
import com.orientechnologies.orient.client.remote.message.OBeginTransaction38Request;
import com.orientechnologies.orient.client.remote.message.OBeginTransactionResponse;
import com.orientechnologies.orient.client.remote.message.OBinaryPushRequest;
import com.orientechnologies.orient.client.remote.message.OCeilingPhysicalPositionsRequest;
import com.orientechnologies.orient.client.remote.message.OCeilingPhysicalPositionsResponse;
import com.orientechnologies.orient.client.remote.message.OCloseQueryRequest;
import com.orientechnologies.orient.client.remote.message.OCommit37Response;
import com.orientechnologies.orient.client.remote.message.OCommit38Request;
import com.orientechnologies.orient.client.remote.message.OCountRecordsRequest;
import com.orientechnologies.orient.client.remote.message.OCountRecordsResponse;
import com.orientechnologies.orient.client.remote.message.OCountRequest;
import com.orientechnologies.orient.client.remote.message.OCountResponse;
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
import com.orientechnologies.orient.client.remote.message.push.OStorageConfigurationPayload;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.config.OStorageClusterConfiguration;
import com.orientechnologies.orient.core.config.OStorageConfiguration;
import com.orientechnologies.orient.core.conflict.ORecordConflictStrategy;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.OLiveQueryMonitor;
import com.orientechnologies.orient.core.db.OrientDBInternal;
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
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.security.OCredentialInterceptor;
import com.orientechnologies.orient.core.security.OSecurityManager;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializerFactory;
import com.orientechnologies.orient.core.storage.OCluster;
import com.orientechnologies.orient.core.storage.OPhysicalPosition;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import com.orientechnologies.orient.core.storage.ORecordMetadata;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.OStorage.LOCKING_STRATEGY;
import com.orientechnologies.orient.core.storage.OStorage.STATUS;
import com.orientechnologies.orient.core.storage.OStorageInfo;
import com.orientechnologies.orient.core.storage.impl.local.paginated.ORecordSerializationContext;
import com.orientechnologies.orient.core.storage.ridbag.sbtree.OBonsaiCollectionPointer;
import com.orientechnologies.orient.core.storage.ridbag.sbtree.OSBTreeCollectionManager;
import com.orientechnologies.orient.core.tx.OTransactionAbstract;
import com.orientechnologies.orient.core.tx.OTransactionInternal;
import com.orientechnologies.orient.core.tx.OTransactionOptimistic;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinary;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataOutput;
import com.orientechnologies.orient.enterprise.channel.binary.ODistributedRedirectException;
import com.orientechnologies.orient.enterprise.channel.binary.OTokenSecurityException;
import java.io.IOException;
import java.io.InputStream;
import java.net.SocketException;
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
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/** This object is bound to each remote ODatabase instances. */
public class ORemoteClient implements OStorageInfo {
  private static final OLogger logger = OLogManager.instance().logger(ORemoteClient.class);

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
  private final ODocument clusterConfiguration = new ODocument();
  private final AtomicInteger users = new AtomicInteger(0);
  private final OContextConfiguration clientConfiguration;
  private final int connectionRetry;
  private final int connectionRetryDelay;
  private OCluster[] clusters = OCommonConst.EMPTY_CLUSTER_ARRAY;
  private int defaultClusterId;
  public final ORemoteConnectionManager connectionManager;
  private final Set<ORemoteClientSession> sessions =
      Collections.newSetFromMap(new ConcurrentHashMap<ORemoteClientSession, Boolean>());

  private final Map<Integer, OLiveQueryClientListener> liveQueryListener =
      new ConcurrentHashMap<>();
  private volatile ORemoteClientPushThread pushThread;
  protected final OrientDBRemote context;
  protected final String url;
  protected final ReentrantReadWriteLock stateLock;

  protected volatile OStorageConfiguration configuration;
  protected volatile OCurrentStorageComponentsFactory componentsFactory;
  protected final String name;

  protected volatile STATUS status = STATUS.CLOSED;
  public static final String TYPE = "remote";

  public static final String ADDRESS_SEPARATOR = ";";

  private static String buildUrl(String[] hosts, String name) {
    return String.join(ADDRESS_SEPARATOR, hosts) + "/" + name;
  }

  public ORemoteClient(
      final ORemoteURLs hosts,
      String name,
      OrientDBRemote context,
      ORemoteConnectionManager connectionManager,
      OContextConfiguration config) {
    this(hosts, name, context, connectionManager, null, config);
  }

  public ORemoteClient(
      final ORemoteURLs hosts,
      String name,
      OrientDBRemote context,
      ORemoteConnectionManager connectionManager,
      final STATUS status,
      OContextConfiguration config) {

    this.name = normalizeName(name);

    if (OStringSerializerHelper.contains(this.name, ','))
      throw new IllegalArgumentException("Invalid character in storage name: " + this.name);

    url = buildUrl(hosts.getUrls().toArray(new String[] {}), name);

    stateLock = new ReentrantReadWriteLock();
    if (status != null) this.status = status;

    configuration = null;

    clientConfiguration = config;
    connectionRetry =
        clientConfiguration.getValueAsInteger(OGlobalConfiguration.NETWORK_SOCKET_RETRY);
    connectionRetryDelay =
        clientConfiguration.getValueAsInteger(OGlobalConfiguration.NETWORK_SOCKET_RETRY_DELAY);
    serverURLs = hosts;

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
    return ppos != null;
  }

  public String getName() {
    return name;
  }

  public static void writeRequest(
      OBinaryRequest<?> request, OChannelDataOutput output, ORemoteClientNodeSession nodeSession)
      throws IOException {
    output.writeByte(request.getCommand());
    output.writeInt(nodeSession.getSessionId());
    output.writeBytes(nodeSession.getToken());
    request.write(output);
    output.flush();
  }

  public <T extends OBinaryResponse> T networkOperationRetryTimeout(
      ORemoteClientSession baseSession,
      final OBinaryRequest<T> request,
      final String errorMessage,
      int retry,
      int timeout) {
    return baseNetworkOperation(
        baseSession,
        (network, session, nodeSession) -> {
          try {
            try {
              writeRequest(request, network.getChannelDataOutput(), nodeSession);
            } finally {
              network.endRequest();
            }
          } catch (IOException e) {
            if (network.isConnected()) {
              logger.warn("Error Writing request on the network", e);
            }
            throw new ONotSendRequestException("Cannot send request on this channel");
          }

          int prev = network.getSocketTimeout();
          T response = request.createResponse();
          try {
            if (timeout > 0) network.setSocketTimeout(timeout);
            beginResponse(network, nodeSession);
            response.read(network.getChannelDataInput(), session);
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
      ORemoteClientSession session, final OBinaryRequest<T> request, final String errorMessage) {
    return networkOperationRetryTimeout(session, request, errorMessage, 0, 0);
  }

  public <T extends OBinaryResponse> T networkOperation(
      ORemoteClientSession session, final OBinaryRequest<T> request, final String errorMessage) {
    return networkOperationRetryTimeout(session, request, errorMessage, connectionRetry, 0);
  }

  public <T> T baseNetworkOperation(
      ORemoteClientSession session,
      final ORemoteClientOperation<T> operation,
      final String errorMessage,
      int retry) {
    if (session.commandExecuting)
      throw new ODatabaseException(
          "Cannot execute the request because an asynchronous operation is in progress. Please use"
              + " a different connection");

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
            serverUrl = useNewServerURL(session, serverUrl);
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
        ORemoteClientNodeSession nodeSession = session.getServerSession(network.getServerURL());
        if (nodeSession == null || !nodeSession.isValid() && !session.isStickToSession()) {
          if (nodeSession != null) {
            session.removeServerSession(nodeSession.getServerURL());
          }
          openRemoteDatabase(session, network);
          nodeSession = session.getServerSession(network.getServerURL());
          if (!network.tryLock()) continue;
        }

        return operation.execute(network, session, nodeSession);
      } catch (ONotSendRequestException e) {
        connectionManager.remove(network);
        serverUrl = null;
      } catch (ODistributedRedirectException e) {
        connectionManager.release(network);
        logger.debug(
            "Redirecting the request from server '%s' to the server '%s' because %s",
            e, e.getFromServer(), e.toString(), e.getMessage());

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
        for (ORemoteClientSession activeSession : sessions) {
          // Not thread Safe ...
          activeSession.removeServerSession(serverUrl);
        }
        serverUrl = null;
      } catch (IOException | OIOException e) {
        logger.info(
            "Caught Network I/O errors on %s, trying an automatic reconnection... (error: %s)",
            network.getServerURL(), e.getMessage());
        logger.debug("I/O error stack: ", e);
        connectionManager.remove(network);
        if (--retry <= 0) throw OException.wrapException(new OIOException(e.getMessage()), e);
        else {
          try {
            Thread.sleep(connectionRetryDelay);
          } catch (InterruptedException e1) {
            logger.error("Exception was suppressed, original exception is ", e);
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

  public int getSessionId(ORemoteClientSession session) {
    return session != null ? session.getSessionId() : -1;
  }

  public String getServerURL(ORemoteClientSession session) {
    return session != null ? session.getServerUrl() : null;
  }

  public void open(
      ORemoteClientSession session,
      final String iUserName,
      final String iUserPassword,
      final OContextConfiguration conf) {

    addUser();
    try {
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

        openRemoteDatabase(session);

        reload(session);

        componentsFactory = new OCurrentStorageComponentsFactory(configuration);

      } else {
        reopenRemoteDatabase(session);
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

  public void reload(ORemoteClientSession session) {
    OReloadResponse37 res =
        networkOperation(session, new OReloadRequest37(), "error loading storage configuration");
    final OStorageConfiguration storageConfiguration =
        new OStorageConfigurationRemote(
            ORecordSerializerFactory.instance().getDefaultRecordSerializer().toString(),
            res.getPayload(),
            clientConfiguration);

    updateStorageConfiguration(storageConfiguration);
  }

  public void close(final ORemoteClientSession session, final boolean iForce) {
    if (status == STATUS.CLOSED) return;

    if (session != null) {
      final Collection<ORemoteClientNodeSession> nodes = session.getAllServerSessions();
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
      for (ORemoteClientSession session : sessions) {
        close(session, true);
      }
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

  public Set<String> getClusterNames() {
    stateLock.readLock().lock();
    try {

      return new HashSet<String>(clusterMap.keySet());

    } finally {
      stateLock.readLock().unlock();
      ;
    }
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

  public ORecordMetadata getRecordMetadata(ORemoteClientSession session, final ORID rid) {

    OGetRecordMetadataRequest request = new OGetRecordMetadataRequest(rid);
    OGetRecordMetadataResponse response =
        networkOperation(session, request, "Error on record metadata read " + rid);

    return response.getMetadata();
  }

  public ORawBuffer readRecordIfVersionIsNotLatest(
      ORemoteClientSession session,
      final ORecordId rid,
      final String fetchPlan,
      final boolean ignoreCache,
      final int recordVersion)
      throws ORecordNotFoundException {
    if (session.commandExecuting)
      // PENDING NETWORK OPERATION, CAN'T EXECUTE IT NOW
      return null;

    OReadRecordIfVersionIsNotLatestRequest request =
        new OReadRecordIfVersionIsNotLatestRequest(rid, recordVersion, fetchPlan, ignoreCache);
    OReadRecordIfVersionIsNotLatestResponse response =
        networkOperation(session, request, "Error on read record " + rid);

    return response.getResult();
  }

  public ORawBuffer readRecord(
      ORemoteClientSession session,
      final ORecordId iRid,
      final String iFetchPlan,
      final boolean iIgnoreCache,
      boolean prefetchRecords) {

    if (session.commandExecuting)
      // PENDING NETWORK OPERATION, CAN'T EXECUTE IT NOW
      return null;

    OReadRecordRequest request = new OReadRecordRequest(iIgnoreCache, iRid, iFetchPlan, false);
    OReadRecordResponse response =
        networkOperation(session, request, "Error on read record " + iRid);

    return response.getResult();
  }

  public String incrementalBackup(
      ORemoteClientSession session, final String backupDirectory, OCallable<Void, Void> started) {
    OIncrementalBackupRequest request = new OIncrementalBackupRequest(backupDirectory);
    OIncrementalBackupResponse response =
        networkOperationNoRetry(session, request, "Error on incremental backup");
    return response.getFileName();
  }

  public OContextConfiguration getClientConfiguration() {
    return clientConfiguration;
  }

  public long count(ORemoteClientSession session, final int iClusterId) {
    return count(session, new int[] {iClusterId});
  }

  public long[] getClusterDataRange(ORemoteClientSession session, final int iClusterId) {
    OGetClusterDataRangeRequest request = new OGetClusterDataRangeRequest(iClusterId);
    OGetClusterDataRangeResponse response =
        networkOperation(
            session,
            request,
            "Error on getting last entry position count in cluster: " + iClusterId);
    return response.getPos();
  }

  public OPhysicalPosition[] higherPhysicalPositions(
      ORemoteClientSession session,
      final int iClusterId,
      final OPhysicalPosition iClusterPosition) {
    OHigherPhysicalPositionsRequest request =
        new OHigherPhysicalPositionsRequest(iClusterId, iClusterPosition);

    OHigherPhysicalPositionsResponse response =
        networkOperation(
            session,
            request,
            "Error on retrieving higher positions after " + iClusterPosition.clusterPosition);
    return response.getNextPositions();
  }

  public OPhysicalPosition[] ceilingPhysicalPositions(
      ORemoteClientSession session, final int clusterId, final OPhysicalPosition physicalPosition) {

    OCeilingPhysicalPositionsRequest request =
        new OCeilingPhysicalPositionsRequest(clusterId, physicalPosition);

    OCeilingPhysicalPositionsResponse response =
        networkOperation(
            session,
            request,
            "Error on retrieving ceiling positions after " + physicalPosition.clusterPosition);
    return response.getPositions();
  }

  public OPhysicalPosition[] lowerPhysicalPositions(
      ORemoteClientSession session,
      final int iClusterId,
      final OPhysicalPosition physicalPosition) {
    OLowerPhysicalPositionsRequest request =
        new OLowerPhysicalPositionsRequest(physicalPosition, iClusterId);
    OLowerPhysicalPositionsResponse response =
        networkOperation(
            session,
            request,
            "Error on retrieving lower positions after " + physicalPosition.clusterPosition);
    return response.getPreviousPositions();
  }

  public OPhysicalPosition[] floorPhysicalPositions(
      ORemoteClientSession session, final int clusterId, final OPhysicalPosition physicalPosition) {
    OFloorPhysicalPositionsRequest request =
        new OFloorPhysicalPositionsRequest(physicalPosition, clusterId);
    OFloorPhysicalPositionsResponse response =
        networkOperation(
            session,
            request,
            "Error on retrieving floor positions after " + physicalPosition.clusterPosition);
    return response.getPositions();
  }

  public long getSize(ORemoteClientSession session) {
    OGetSizeRequest request = new OGetSizeRequest();
    OGetSizeResponse response = networkOperation(session, request, "Error on read database size");
    return response.getSize();
  }

  public long countRecords(ORemoteClientSession session) {
    OCountRecordsRequest request = new OCountRecordsRequest();
    OCountRecordsResponse response =
        networkOperation(session, request, "Error on read database record count");
    return response.getCountRecords();
  }

  public long count(ORemoteClientSession session, final int[] iClusterIds) {
    OCountRequest request = new OCountRequest(iClusterIds, false);
    OCountResponse response =
        networkOperation(
            session,
            request,
            "Error on read record count in clusters: " + Arrays.toString(iClusterIds));
    return response.getCount();
  }

  public void stickToSession(ORemoteClientSession session) {
    session.stickToSession();
  }

  public void unstickToSession(ORemoteClientSession session) {
    session.unStickToSession();
  }

  public ORemoteQueryResult query(ODatabaseDocumentRemote db, String query, Object[] args) {
    int recordsPerPage = OGlobalConfiguration.QUERY_REMOTE_RESULTSET_PAGE_SIZE.getValueAsInteger();
    if (recordsPerPage <= 0) {
      recordsPerPage = 100;
    }
    ORemoteClientSession session = db.getSession();
    OQueryRequest request =
        OQueryRequest.queryArray(query, args, db.getSerializer(), recordsPerPage);
    OQueryResponse response =
        networkOperation(session, request, "Error on executing command: " + query);
    ORemoteResultSet rs =
        new ORemoteResultSet(
            db,
            response.getQueryId(),
            response.getResult(),
            response.getExecutionPlan(),
            response.getQueryStats(),
            response.isHasNextPage());
    if (response.isHasNextPage()) {
      stickToSession(session);
    } else {
      db.queryClosed(response.getQueryId());
    }
    return new ORemoteQueryResult(rs, response.isTxChanges(), response.isReloadMetadata());
  }

  public ORemoteQueryResult query(ODatabaseDocumentRemote db, String query, Map args) {
    ORemoteClientSession session = db.getSession();

    int recordsPerPage = OGlobalConfiguration.QUERY_REMOTE_RESULTSET_PAGE_SIZE.getValueAsInteger();
    if (recordsPerPage <= 0) {
      recordsPerPage = 100;
    }
    OQueryRequest request = OQueryRequest.queryMap(query, args, db.getSerializer(), recordsPerPage);
    OQueryResponse response =
        networkOperation(session, request, "Error on executing command: " + query);

    ORemoteResultSet rs =
        new ORemoteResultSet(
            db,
            response.getQueryId(),
            response.getResult(),
            response.getExecutionPlan(),
            response.getQueryStats(),
            response.isHasNextPage());
    if (response.isHasNextPage()) {
      stickToSession(session);
    } else {
      db.queryClosed(response.getQueryId());
    }
    return new ORemoteQueryResult(rs, response.isTxChanges(), response.isReloadMetadata());
  }

  public ORemoteQueryResult command(ODatabaseDocumentRemote db, String query, Object[] args) {
    ORemoteClientSession session = db.getSession();
    int recordsPerPage = OGlobalConfiguration.QUERY_REMOTE_RESULTSET_PAGE_SIZE.getValueAsInteger();
    if (recordsPerPage <= 0) {
      recordsPerPage = 100;
    }
    OQueryRequest request =
        OQueryRequest.commandArray(query, args, db.getSerializer(), recordsPerPage);
    OQueryResponse response =
        networkOperationNoRetry(session, request, "Error on executing command: " + query);
    ORemoteResultSet rs =
        new ORemoteResultSet(
            db,
            response.getQueryId(),
            response.getResult(),
            response.getExecutionPlan(),
            response.getQueryStats(),
            response.isHasNextPage());
    if (response.isHasNextPage()) {
      stickToSession(session);
    } else {
      db.queryClosed(response.getQueryId());
    }
    return new ORemoteQueryResult(rs, response.isTxChanges(), response.isReloadMetadata());
  }

  public ORemoteQueryResult command(ODatabaseDocumentRemote db, String query, Map args) {
    ORemoteClientSession session = db.getSession();
    int recordsPerPage = OGlobalConfiguration.QUERY_REMOTE_RESULTSET_PAGE_SIZE.getValueAsInteger();
    if (recordsPerPage <= 0) {
      recordsPerPage = 100;
    }
    OQueryRequest request =
        OQueryRequest.commandMap(query, args, db.getSerializer(), recordsPerPage);
    OQueryResponse response =
        networkOperationNoRetry(session, request, "Error on executing command: " + query);
    ORemoteResultSet rs =
        new ORemoteResultSet(
            db,
            response.getQueryId(),
            response.getResult(),
            response.getExecutionPlan(),
            response.getQueryStats(),
            response.isHasNextPage());
    if (response.isHasNextPage()) {
      stickToSession(session);
    } else {
      db.queryClosed(response.getQueryId());
    }
    return new ORemoteQueryResult(rs, response.isTxChanges(), response.isReloadMetadata());
  }

  public ORemoteQueryResult execute(
      ODatabaseDocumentRemote db, String language, String query, Object[] args) {
    ORemoteClientSession session = db.getSession();
    int recordsPerPage = OGlobalConfiguration.QUERY_REMOTE_RESULTSET_PAGE_SIZE.getValueAsInteger();
    if (recordsPerPage <= 0) {
      recordsPerPage = 100;
    }
    OQueryRequest request =
        OQueryRequest.executeArray(language, query, args, db.getSerializer(), recordsPerPage);
    OQueryResponse response =
        networkOperationNoRetry(session, request, "Error on executing command: " + query);
    ORemoteResultSet rs =
        new ORemoteResultSet(
            db,
            response.getQueryId(),
            response.getResult(),
            response.getExecutionPlan(),
            response.getQueryStats(),
            response.isHasNextPage());

    if (response.isHasNextPage()) {
      stickToSession(session);
    } else {
      db.queryClosed(response.getQueryId());
    }

    return new ORemoteQueryResult(rs, response.isTxChanges(), response.isReloadMetadata());
  }

  public ORemoteQueryResult execute(
      ODatabaseDocumentRemote db, String language, String query, Map args) {
    ORemoteClientSession session = db.getSession();
    int recordsPerPage =
        db.getConfiguration()
            .getValueAsInteger(OGlobalConfiguration.QUERY_REMOTE_RESULTSET_PAGE_SIZE);
    if (recordsPerPage <= 0) {
      recordsPerPage = 100;
    }
    boolean sendExecutionPlan =
        db.getConfiguration()
            .getValueAsBoolean(OGlobalConfiguration.QUERY_REMOTE_SEND_EXECUTION_PLAN);
    OQueryRequest request =
        OQueryRequest.executeMap(language, query, args, db.getSerializer(), recordsPerPage);
    request.setIncludePlan(sendExecutionPlan);
    OQueryResponse response =
        networkOperationNoRetry(session, request, "Error on executing command: " + query);
    ORemoteResultSet rs =
        new ORemoteResultSet(
            db,
            response.getQueryId(),
            response.getResult(),
            response.getExecutionPlan(),
            response.getQueryStats(),
            response.isHasNextPage());
    if (response.isHasNextPage()) {
      stickToSession(session);
    } else {
      db.queryClosed(response.getQueryId());
    }
    return new ORemoteQueryResult(rs, response.isTxChanges(), response.isReloadMetadata());
  }

  public void closeQuery(ODatabaseDocumentRemote db, String queryId) {
    ORemoteClientSession session = db.getSession();
    unstickToSession(session);
    OCloseQueryRequest request = new OCloseQueryRequest(queryId);
    networkOperation(session, request, "Error closing query: " + queryId);
  }

  public void fetchNextPage(ODatabaseDocumentRemote db, ORemoteResultSet rs) {
    ORemoteClientSession session = db.getSession();
    int recordsPerPage =
        db.getConfiguration()
            .getValueAsInteger(OGlobalConfiguration.QUERY_REMOTE_RESULTSET_PAGE_SIZE);
    if (recordsPerPage <= 0) {
      recordsPerPage = 100;
    }
    OQueryNextPageRequest request = new OQueryNextPageRequest(rs.getQueryId(), recordsPerPage);
    OQueryResponse response =
        networkOperation(
            session, request, "Error on fetching next page for statment: " + rs.getQueryId());

    rs.fetched(
        response.getResult(),
        response.isHasNextPage(),
        response.getExecutionPlan(),
        response.getQueryStats());
    if (!response.isHasNextPage()) {
      unstickToSession(session);
      db.queryClosed(response.getQueryId());
    }
  }

  public List<ORecordOperation> commit(ODatabaseDocumentRemote db, final OTransactionInternal iTx) {
    ORemoteClientSession session = db.getSession();
    unstickToSession(session);
    final OCommit38Request request =
        new OCommit38Request(
            iTx.getId(), true, true, iTx.getRecordOperations(), iTx.getIndexOperations());

    final OCommit37Response response = networkOperationNoRetry(session, request, "Error on commit");
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

  public void rollback(ORemoteClientSession session, OTransactionInternal iTx) {
    try {
      if (((OTransactionOptimistic) iTx).isAlreadyCleared()
          && session.getAllServerSessions().size() > 0) {
        ORollbackTransactionRequest request = new ORollbackTransactionRequest(iTx.getId());

        ORollbackTransactionResponse response =
            networkOperation(
                session, request, "Error on fetching next page for statment: " + request);
      }
    } finally {
      unstickToSession(session);
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

  public int addCluster(ORemoteClientSession session, final String iClusterName) {
    return addCluster(session, iClusterName, -1);
  }

  public int addCluster(
      ORemoteClientSession session, final String iClusterName, final int iRequestedId) {
    OAddClusterRequest request = new OAddClusterRequest(iRequestedId, iClusterName);
    OAddClusterResponse response =
        networkOperationNoRetry(session, request, "Error on add new cluster");
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

  public boolean dropCluster(ORemoteClientSession session, final int iClusterId) {

    ODropClusterRequest request = new ODropClusterRequest(iClusterId);

    ODropClusterResponse response =
        networkOperationNoRetry(session, request, "Error on removing of cluster");
    if (response.getResult()) removeClusterFromConfiguration(iClusterId);
    return response.getResult();
  }

  public String getClusterName(ORemoteClientSession session, int clusterId) {
    stateLock.readLock().lock();
    try {

      if (clusterId == ORID.CLUSTER_ID_INVALID)
        // GET THE DEFAULT CLUSTER
        clusterId = defaultClusterId;

      if (clusterId >= clusters.length) {
        stateLock.readLock().unlock();
        reload(session);
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

  public ODocument getClusterConfiguration() {
    return clusterConfiguration;
  }

  /** Ends the request and unlock the write lock */
  public void endRequest(final OChannelBinaryAsynchClient iNetwork) throws IOException {
    if (iNetwork == null) return;

    iNetwork.getChannelDataOutput().flush();
    iNetwork.releaseWriteLock();
  }

  /** End response reached: release the channel in the pool to being reused */
  public void endResponse(final OChannelBinaryAsynchClient iNetwork) throws IOException {
    iNetwork.endResponse();
  }

  public boolean isRemote() {
    return true;
  }

  public String getURL() {
    return ORemoteClient.TYPE + ":" + url;
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
    return ORemoteClient.TYPE;
  }

  public String getUserName(ORemoteClientSession session) {
    if (session == null) return null;
    return session.connectionUserName;
  }

  protected String reopenRemoteDatabase(ORemoteClientSession session) throws IOException {
    String currentURL = getCurrentServerURL(session);
    do {
      do {
        final OChannelBinaryAsynchClient network = getNetwork(currentURL);
        try {
          ORemoteClientNodeSession nodeSession =
              session.getOrCreateServerSession(network.getServerURL());
          if (nodeSession == null || !nodeSession.isValid()) {
            openRemoteDatabase(session, network);
            return network.getServerURL();
          } else {
            OReopenRequest request = new OReopenRequest();

            try {
              network.getChannelDataOutput().writeByte(request.getCommand());
              network.getChannelDataOutput().writeInt(nodeSession.getSessionId());
              network.getChannelDataOutput().writeBytes(nodeSession.getToken());
              request.write(network.getChannelDataOutput());
            } finally {
              endRequest(network);
            }

            OReopenResponse response = request.createResponse();
            try {
              byte[] newToken = network.beginResponse(nodeSession.getSessionId(), true);
              response.read(network.getChannelDataInput(), session);
              if (newToken != null && newToken.length > 0) {
                nodeSession.setSession(response.getSessionId(), newToken);
              } else {
                nodeSession.setSession(response.getSessionId(), nodeSession.getToken());
              }
              logger.debug(
                  "Client connected to %s with session id=%d",
                  network.getServerURL(), response.getSessionId());
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

          logger.error("Cannot open database with url %s", e, currentURL);
        } catch (OOfflineNodeException e) {
          if (network != null) {
            // REMOVE THE NETWORK CONNECTION IF ANY
            connectionManager.remove(network);
          }

          logger.debug("Cannot open database with url %s", e, currentURL);
        } catch (OSecurityException ex) {
          logger.debug("Invalidate token for url=%s", ex, currentURL);
          session.removeServerSession(currentURL);

          if (network != null) {
            // REMOVE THE NETWORK CONNECTION IF ANY
            try {
              connectionManager.remove(network);
            } catch (Exception e) {
              // IGNORE ANY EXCEPTION
              logger.debug("Cannot remove connection or database url=%s", e, currentURL);
            }
          }
        } catch (OException e) {
          connectionManager.release(network);
          // PROPAGATE ANY OTHER ORIENTDB EXCEPTION
          throw e;

        } catch (Exception e) {
          logger.debug("Cannot open database with url %s", e, currentURL);
          if (network != null) {
            // REMOVE THE NETWORK CONNECTION IF ANY
            try {
              connectionManager.remove(network);
            } catch (Exception ex) {
              // IGNORE ANY EXCEPTION
              logger.debug("Cannot remove connection or database url=%s", e, currentURL);
            }
          }
        }
      } while (connectionManager.getAvailableConnections(currentURL) > 0);

      currentURL = useNewServerURL(session, currentURL);

    } while (currentURL != null);

    // REFILL ORIGINAL SERVER LIST
    serverURLs.reloadOriginalURLs();

    throw new OStorageException(
        "Cannot create a connection to remote server address(es): " + serverURLs.getUrls());
  }

  protected void openRemoteDatabase(ORemoteClientSession session) throws IOException {
    final String currentURL = getNextAvailableServerURL(true, session);
    openRemoteDatabase(session, currentURL);
  }

  public void openRemoteDatabase(ORemoteClientSession session, OChannelBinaryAsynchClient network)
      throws IOException {

    ORemoteClientNodeSession nodeSession = session.getOrCreateServerSession(network.getServerURL());
    OOpen37Request request =
        new OOpen37Request(name, session.connectionUserName, session.connectionUserPassword);
    try {
      network.getChannelDataOutput().writeByte(request.getCommand());
      network.getChannelDataOutput().writeInt(nodeSession.getSessionId());
      network.getChannelDataOutput().writeBytes(null);
      request.write(network.getChannelDataOutput());
    } finally {
      endRequest(network);
    }
    final int sessionId;
    OOpen37Response response = request.createResponse();
    try {
      network.beginResponse(nodeSession.getSessionId(), true);
      response.read(network.getChannelDataInput(), session);
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

    logger.debug("Client connected to %s with session id=%d", network.getServerURL(), sessionId);

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

  public void initPush(ORemoteClientSession session, OMetadataPushListener handler) {
    if (pushThread == null) {
      stateLock.writeLock().lock();
      try {
        if (pushThread == null) {
          pushThread =
              new ORemoteClientPushThread(
                  new ORemotePushHandlerImpl(this, handler),
                  getCurrentServerURL(session),
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

  private void subscribeDistributedConfiguration(ORemoteClientSession nodeSession) {
    pushThread.subscribe(new OSubscribeDistributedConfigurationRequest(), nodeSession);
  }

  private void subscribeStorageConfiguration(ORemoteClientSession nodeSession) {
    pushThread.subscribe(new OSubscribeStorageConfigurationRequest(), nodeSession);
  }

  private void subscribeSchema(ORemoteClientSession nodeSession) {
    pushThread.subscribe(new OSubscribeSchemaRequest(), nodeSession);
  }

  private void subscribeFunctions(ORemoteClientSession nodeSession) {
    pushThread.subscribe(new OSubscribeFunctionsRequest(), nodeSession);
  }

  private void subscribeSequences(ORemoteClientSession nodeSession) {
    pushThread.subscribe(new OSubscribeSequencesRequest(), nodeSession);
  }

  private void subscribeIndexManager(ORemoteClientSession nodeSession) {
    pushThread.subscribe(new OSubscribeIndexManagerRequest(), nodeSession);
  }

  protected void openRemoteDatabase(ORemoteClientSession session, String currentURL) {
    do {
      do {
        OChannelBinaryAsynchClient network = null;
        try {
          network = getNetwork(currentURL);
          openRemoteDatabase(session, network);
          return;
        } catch (ODistributedRedirectException e) {
          connectionManager.release(network);
          // RECONNECT TO THE SERVER SUGGESTED IN THE EXCEPTION
          currentURL = e.getToServerAddress();
        } catch (OModificationOperationProhibitedException mope) {
          connectionManager.release(network);
          handleDBFreeze();
          currentURL = useNewServerURL(session, currentURL);
        } catch (OOfflineNodeException e) {
          connectionManager.release(network);
          currentURL = useNewServerURL(session, currentURL);
        } catch (OIOException e) {
          if (network != null) {
            // REMOVE THE NETWORK CONNECTION IF ANY
            connectionManager.remove(network);
          }

          logger.debug("Cannot open database with url %s", e, currentURL);

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

      if (currentURL != null) {
        currentURL = useNewServerURL(session, currentURL);
      }

    } while (currentURL != null);

    // REFILL ORIGINAL SERVER LIST
    serverURLs.reloadOriginalURLs();

    throw new OStorageException(
        "Cannot create a connection to remote server address(es): " + serverURLs.getUrls());
  }

  protected String useNewServerURL(ORemoteClientSession session, final String iUrl) {
    int pos = iUrl.indexOf('/');
    if (pos >= iUrl.length() - 1)
      // IGNORE ENDING /
      pos = -1;

    final String url = pos > -1 ? iUrl.substring(0, pos) : iUrl;
    String newUrl = serverURLs.removeAndGet(url);
    if (session != null) {
      session.currentUrl = newUrl;
      session.serverURLIndex = 0;
    }
    return newUrl;
  }

  protected String getNextAvailableServerURL(
      boolean iIsConnectOperation, ORemoteClientSession session) {

    OContextConfiguration config = null;
    if (configuration != null) config = configuration.getContextConfiguration();
    return serverURLs.getNextAvailableServerURL(
        iIsConnectOperation, session, config, connectionStrategy);
  }

  protected String getCurrentServerURL(ORemoteClientSession session) {
    return serverURLs.getServerURFromList(false, session, configuration.getContextConfiguration());
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
        logger.error(
            "Removing locked network channel '%s' (connected=%s)...",
            null, iCurrentURL, network.isConnected());
        connectionManager.remove(network);
        network = null;
      }
    } while (network == null);
    return network;
  }

  public static void beginResponse(
      OChannelBinaryAsynchClient iNetwork, ORemoteClientNodeSession nodeSession)
      throws IOException {
    byte[] newToken = iNetwork.beginResponse(nodeSession.getSessionId(), true);
    if (newToken != null && newToken.length > 0) {
      nodeSession.setSession(nodeSession.getSessionId(), newToken);
    }
  }

  private boolean handleDBFreeze() {

    boolean retry;
    logger.warn(
        "DB is frozen will wait for %d ms. and then retry.",
        getClientConfiguration().getValue(OGlobalConfiguration.CLIENT_DB_RELEASE_WAIT_TIMEOUT));
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

  public void updateStorageConfiguration(OStorageConfigurationPayload updatedConfiguration) {
    final OStorageConfiguration storageConfiguration =
        new OStorageConfigurationRemote(
            ORecordSerializerFactory.instance().getDefaultRecordSerializer().toString(),
            updatedConfiguration,
            clientConfiguration);
    updateStorageConfiguration(storageConfiguration);
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

  public int nextInitialSessionId() {
    return sessionSerialId.decrementAndGet();
  }

  public ORemoteClientSession newInitialSession() {
    ORemoteClientSession session = new ORemoteClientSession(nextInitialSessionId());
    sessions.add(session);
    return session;
  }

  public boolean isClosed(ORemoteClientSession session) {
    if (status == STATUS.CLOSED) return true;
    if (session == null) return false;
    return session.isClosed();
  }

  public ORemoteClient copy(
      final ODatabaseDocumentRemote source, final ODatabaseDocumentRemote dest) {
    ODatabaseDocumentInternal origin = null;
    if (ODatabaseRecordThreadLocal.instance() != null)
      origin = ODatabaseRecordThreadLocal.instance().getIfDefined();

    final ORemoteClientSession session = source.getSession();
    if (session != null) {
      // TODO:may run a session reopen
      final ORemoteClientSession newSession = new ORemoteClientSession(nextInitialSessionId());
      newSession.connectionUserName = session.connectionUserName;
      newSession.connectionUserPassword = session.connectionUserPassword;
      dest.setSessionMetadata(newSession);
    }
    try {
      dest.activateOnCurrentThread();
      openRemoteDatabase(dest.getSession());
    } catch (IOException e) {
      logger.error("Error during database open", e);
    } finally {
      ODatabaseRecordThreadLocal.instance().set(origin);
    }
    return this;
  }

  public void importDatabase(
      ORemoteClientSession session,
      final String options,
      final InputStream inputStream,
      final String name,
      final OCommandOutputListener listener) {
    OImportRequest request = new OImportRequest(inputStream, options, name);

    OImportResponse response =
        networkOperationRetryTimeout(
            session,
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

  public void beginTransaction(ORemoteClientSession session, OTransactionOptimistic transaction) {
    OBeginTransaction38Request request =
        new OBeginTransaction38Request(
            transaction.getId(),
            true,
            true,
            transaction.getRecordOperations(),
            transaction.getIndexOperations());
    OBeginTransactionResponse response =
        networkOperationNoRetry(session, request, "Error on remote transaction begin");
    for (Map.Entry<ORID, ORID> entry : response.getUpdatedIds().entrySet()) {
      transaction.updateIdentityAfterCommit(entry.getKey(), entry.getValue());
    }
    stickToSession(session);
  }

  public void reBeginTransaction(ORemoteClientSession session, OTransactionOptimistic transaction) {
    ORebeginTransaction38Request request =
        new ORebeginTransaction38Request(
            transaction.getId(),
            true,
            transaction.getRecordOperations(),
            transaction.getIndexOperations());
    OBeginTransactionResponse response =
        networkOperationNoRetry(session, request, "Error on remote transaction begin");
    for (Map.Entry<ORID, ORID> entry : response.getUpdatedIds().entrySet()) {
      transaction.updateIdentityAfterCommit(entry.getKey(), entry.getValue());
    }
  }

  public void fetchTransaction(ODatabaseDocumentRemote db) {
    ORemoteClientSession session = db.getSession();
    OTransactionOptimisticClient transaction = db.getActiveTx();
    OFetchTransaction38Request request = new OFetchTransaction38Request(transaction.getId());
    OFetchTransaction38Response response =
        networkOperation(session, request, "Error fetching transaction from server side");
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

  public OLiveQueryMonitor liveQuery(
      ODatabaseDocumentRemote db,
      String query,
      OLiveQueryClientListener listener,
      Object[] params) {

    OSubscribeLiveQueryRequest request = new OSubscribeLiveQueryRequest(query, params);
    OSubscribeLiveQueryResponse response = pushThread.subscribe(request, db.getSession());
    if (response == null) {
      throw new ODatabaseException(
          "Impossible to start the live query, check server log for additional information");
    }
    registerLiveListener(response.getMonitorId(), listener);
    return new OLiveQueryMonitorRemote(db, response.getMonitorId());
  }

  public OLiveQueryMonitor liveQuery(
      ODatabaseDocumentRemote db,
      String query,
      OLiveQueryClientListener listener,
      Map<String, ?> params) {
    OSubscribeLiveQueryRequest request =
        new OSubscribeLiveQueryRequest(query, (Map<String, Object>) params);
    OSubscribeLiveQueryResponse response = pushThread.subscribe(request, db.getSession());
    if (response == null) {
      throw new ODatabaseException(
          "Impossible to start the live query, check server log for additional information");
    }
    registerLiveListener(response.getMonitorId(), listener);
    return new OLiveQueryMonitorRemote(db, response.getMonitorId());
  }

  public void unsubscribeLive(ORemoteClientSession session, int monitorId) {
    OUnsubscribeRequest request =
        new OUnsubscribeRequest(new OUnsubscribeLiveQueryRequest(monitorId));
    networkOperation(session, request, "Error on unsubscribe of live query");
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

  public void onPushReconnect(String host) {
    if (status != STATUS.OPEN) {
      // AVOID RECONNECT ON CLOSE
      return;
    }
    ORemoteClientSession aValidSession = null;
    for (ORemoteClientSession session : sessions) {
      if (session.getServerSession(host) != null) {
        aValidSession = session;
        break;
      }
    }
    if (aValidSession != null) {
      subscribeDistributedConfiguration(aValidSession);
      subscribeStorageConfiguration(aValidSession);
    } else {
      logger.warn(
          "Cannot find a valid session for subscribe for event to host '%s' forward the"
              + " subscribe for the next session open ",
          host);
      ORemoteClientPushThread old;
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
    this.connectionManager.removeIfPresent((OChannelBinaryAsynchClient) network);
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
      if (e instanceof SocketException) {
        logger.debug("Socket exception on push request", e);
      } else {
        logger.warn("Error on push request", e);
      }
    }
  }

  public OLockRecordResponse lockRecord(
      ORemoteClientSession session,
      OIdentifiable iRecord,
      LOCKING_STRATEGY lockingStrategy,
      long timeout) {
    OExperimentalRequest request =
        new OExperimentalRequest(
            new OLockRecordRequest(iRecord.getIdentity(), lockingStrategy, timeout));
    OExperimentalResponse response = networkOperation(session, request, "Error locking record");
    OLockRecordResponse realResponse = (OLockRecordResponse) response.getResponse();
    return realResponse;
  }

  public void unlockRecord(ORemoteClientSession session, OIdentifiable iRecord) {
    OExperimentalRequest request =
        new OExperimentalRequest(new OUnlockRecordRequest(iRecord.getIdentity()));
    OExperimentalResponse response = networkOperation(session, request, "Error locking record");
    OUnlockRecordResponse realResponse = (OUnlockRecordResponse) response.getResponse();
  }

  public void returnSocket(OChannelBinary network) {
    this.connectionManager.remove((OChannelBinaryAsynchClient) network);
  }

  public List<String> getServerURLs() {
    return serverURLs.getUrls();
  }

  public ORemoteURLs getRemoteURLs() {
    return this.serverURLs;
  }

  public boolean isDistributed() {
    return false;
  }

  public STATUS getStatus() {
    return status;
  }

  public void close(ORemoteClientSession session) {
    close(session, false);
  }

  public boolean dropCluster(ORemoteClientSession session, final String iClusterName) {
    return dropCluster(session, getClusterIdByName(iClusterName));
  }

  public OCurrentStorageComponentsFactory getComponentsFactory() {
    return componentsFactory;
  }

  public OrientDBInternal getContext() {
    return context;
  }

  @Override
  public ORecordConflictStrategy getRecordConflictStrategy() {
    throw new UnsupportedOperationException("getRecordConflictStrategy");
  }

  public void setConflictStrategy(ORecordConflictStrategy strategy) {
    throw new UnsupportedOperationException("setConflictStrategy");
  }

  public Map<Integer, OLiveQueryClientListener> getLiveQueryListener() {
    return liveQueryListener;
  }
}
