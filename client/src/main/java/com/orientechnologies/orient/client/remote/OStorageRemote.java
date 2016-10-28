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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;

import com.orientechnologies.common.concur.OOfflineNodeException;
import com.orientechnologies.common.concur.lock.OInterruptedException;
import com.orientechnologies.common.concur.lock.OModificationOperationProhibitedException;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.io.OIOException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.util.OCommonConst;
import com.orientechnologies.orient.client.binary.OChannelBinaryAsynchClient;
import com.orientechnologies.orient.client.remote.message.OAddClusterRequest;
import com.orientechnologies.orient.client.remote.message.OAddClusterResponse;
import com.orientechnologies.orient.client.remote.message.OCeilingPhysicalPositionsRequest;
import com.orientechnologies.orient.client.remote.message.OCeilingPhysicalPositionsResponse;
import com.orientechnologies.orient.client.remote.message.OCleanOutRecordRequest;
import com.orientechnologies.orient.client.remote.message.OCleanOutRecordResponse;
import com.orientechnologies.orient.client.remote.message.OCloseRequest;
import com.orientechnologies.orient.client.remote.message.OCommandRequest;
import com.orientechnologies.orient.client.remote.message.OCommandResponse;
import com.orientechnologies.orient.client.remote.message.OCommitRequest;
import com.orientechnologies.orient.client.remote.message.OCommitResponse;
import com.orientechnologies.orient.client.remote.message.OCommitResponse.OCreatedRecordResponse;
import com.orientechnologies.orient.client.remote.message.OCommitResponse.OUpdatedRecordResponse;
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
import com.orientechnologies.orient.client.remote.message.OFloorPhysicalPositionsRequest;
import com.orientechnologies.orient.client.remote.message.OFloorPhysicalPositionsResponse;
import com.orientechnologies.orient.client.remote.message.OGetClusterDataRangeRequest;
import com.orientechnologies.orient.client.remote.message.OGetClusterDataRangeResponse;
import com.orientechnologies.orient.client.remote.message.OGetRecordMetadataRequest;
import com.orientechnologies.orient.client.remote.message.OGetRecordMetadataResponse;
import com.orientechnologies.orient.client.remote.message.OGetSizeRequest;
import com.orientechnologies.orient.client.remote.message.OGetSizeResponse;
import com.orientechnologies.orient.client.remote.message.OHideRecordRequest;
import com.orientechnologies.orient.client.remote.message.OHideRecordResponse;
import com.orientechnologies.orient.client.remote.message.OHigherPhysicalPositionsRequest;
import com.orientechnologies.orient.client.remote.message.OHigherPhysicalPositionsResponse;
import com.orientechnologies.orient.client.remote.message.OImportRequest;
import com.orientechnologies.orient.client.remote.message.OImportResponse;
import com.orientechnologies.orient.client.remote.message.OIncrementalBackupRequest;
import com.orientechnologies.orient.client.remote.message.OIncrementalBackupResponse;
import com.orientechnologies.orient.client.remote.message.OLowerPhysicalPositionsRequest;
import com.orientechnologies.orient.client.remote.message.OLowerPhysicalPositionsResponse;
import com.orientechnologies.orient.client.remote.message.OOpenRequest;
import com.orientechnologies.orient.client.remote.message.OOpenResponse;
import com.orientechnologies.orient.client.remote.message.OReadRecordIfVersionIsNotLatestRequest;
import com.orientechnologies.orient.client.remote.message.OReadRecordIfVersionIsNotLatestResponse;
import com.orientechnologies.orient.client.remote.message.OReadRecordRequest;
import com.orientechnologies.orient.client.remote.message.OReadRecordResponse;
import com.orientechnologies.orient.client.remote.message.OReloadRequest;
import com.orientechnologies.orient.client.remote.message.OReloadResponse;
import com.orientechnologies.orient.client.remote.message.OReopenRequest;
import com.orientechnologies.orient.client.remote.message.OReopenResponse;
import com.orientechnologies.orient.client.remote.message.OUpdateRecordRequest;
import com.orientechnologies.orient.client.remote.message.OUpdateRecordResponse;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.command.OCommandRequestAsynch;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.config.OStorageConfiguration;
import com.orientechnologies.orient.core.conflict.ORecordConflictStrategy;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentRemote;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTxInternal;
import com.orientechnologies.orient.core.db.record.OCurrentStorageComponentsFactory;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.db.record.ridbag.sbtree.OBonsaiCollectionPointer;
import com.orientechnologies.orient.core.db.record.ridbag.sbtree.OSBTreeCollectionManager;
import com.orientechnologies.orient.core.exception.OConfigurationException;
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
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializerFactory;
import com.orientechnologies.orient.core.sql.query.OLiveQuery;
import com.orientechnologies.orient.core.storage.OCluster;
import com.orientechnologies.orient.core.storage.OPhysicalPosition;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import com.orientechnologies.orient.core.storage.ORecordCallback;
import com.orientechnologies.orient.core.storage.ORecordMetadata;
import com.orientechnologies.orient.core.storage.OStorageAbstract;
import com.orientechnologies.orient.core.storage.OStorageOperationResult;
import com.orientechnologies.orient.core.storage.OStorageProxy;
import com.orientechnologies.orient.core.storage.impl.local.paginated.ORecordSerializationContext;
import com.orientechnologies.orient.core.tx.OTransaction;
import com.orientechnologies.orient.core.tx.OTransactionAbstract;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;
import com.orientechnologies.orient.enterprise.channel.binary.OTokenSecurityException;

/**
 * This object is bound to each remote ODatabase instances.
 */
public class OStorageRemote extends OStorageAbstract implements OStorageProxy {
  public static final String   PARAM_CONNECTION_STRATEGY = "connectionStrategy";
  private static final String  DEFAULT_HOST              = "localhost";
  private static final int     DEFAULT_PORT              = 2424;
  private static final int     DEFAULT_SSL_PORT          = 2434;
  private static final String  ADDRESS_SEPARATOR         = ";";
  public static final String   DRIVER_NAME               = "OrientDB Java";
  private static final String  LOCAL_IP                  = "127.0.0.1";
  private static final String  LOCALHOST                 = "localhost";
  private static AtomicInteger sessionSerialId           = new AtomicInteger(-1);

  public enum CONNECTION_STRATEGY {
    STICKY, ROUND_ROBIN_CONNECT, ROUND_ROBIN_REQUEST
  }

  private CONNECTION_STRATEGY                  connectionStrategy      = CONNECTION_STRATEGY.STICKY;

  private final OSBTreeCollectionManagerRemote sbTreeCollectionManager = new OSBTreeCollectionManagerRemote();
  protected final List<String>                 serverURLs              = new ArrayList<String>();
  protected final Map<String, OCluster>        clusterMap              = new ConcurrentHashMap<String, OCluster>();
  private final ExecutorService                asynchExecutor;
  private final ODocument                      clusterConfiguration    = new ODocument();
  private final String                         clientId;
  private final AtomicInteger                  users                   = new AtomicInteger(0);
  private OContextConfiguration                clientConfiguration;
  private int                                  connectionRetry;
  private int                                  connectionRetryDelay;
  OCluster[]                                   clusters                = OCommonConst.EMPTY_CLUSTER_ARRAY;
  private int                                  defaultClusterId;
  public OStorageRemoteAsynchEventListener     asynchEventListener;
  public ORemoteConnectionManager              connectionManager;
  private final Set<OStorageRemoteSession>     sessions                = Collections
      .newSetFromMap(new ConcurrentHashMap<OStorageRemoteSession, Boolean>());

  public OStorageRemote(final String iClientId, final String iURL, final String iMode) throws IOException {
    this(iClientId, iURL, iMode, null, true);
  }

  public OStorageRemote(final String iClientId, final String iURL, final String iMode, final STATUS status,
      final boolean managePushMessages) throws IOException {
    super(iURL, iURL, iMode, 0); // NO TIMEOUT @SINCE 1.5
    if (status != null)
      this.status = status;

    clientId = iClientId;
    configuration = null;

    clientConfiguration = new OContextConfiguration();
    connectionRetry = clientConfiguration.getValueAsInteger(OGlobalConfiguration.NETWORK_SOCKET_RETRY);
    connectionRetryDelay = clientConfiguration.getValueAsInteger(OGlobalConfiguration.NETWORK_SOCKET_RETRY_DELAY);
    if (managePushMessages)
      asynchEventListener = new OStorageRemoteAsynchEventListener(this);
    parseServerURLs();

    asynchExecutor = Executors.newSingleThreadScheduledExecutor();

    OEngineRemote engine = (OEngineRemote) Orient.instance().getRunningEngine(OEngineRemote.NAME);
    connectionManager = engine.getConnectionManager();
  }

  public <T, X extends OBinaryResponse<T>> T asyncNetworkOperation(final OBinaryRequest request, X response, int mode,
      final ORecordId recordId, final ORecordCallback<X> callback, final String errorMessage) {
    final int pMode;
    if (mode == 1 && callback == null)
      // ASYNCHRONOUS MODE NO ANSWER
      pMode = 2;
    else
      pMode = mode;
    return baseNetworkOperation(new OStorageRemoteOperation<T>() {
      @Override
      public T execute(final OChannelBinaryAsynchClient network, final OStorageRemoteSession session) throws IOException {
        // Send The request
        try {
          network.beginRequest(request.getCommand(), session);
          request.write(network, session, pMode);
        } finally {
          network.endRequest();
        }
        final T res;
        if (pMode == 0) {
          // SYNC
          try {
            beginResponse(network, session);
            res = response.read(network, session);
          } finally {
            endResponse(network);
          }
          connectionManager.release(network);
        } else if (pMode == 1) {
          // ASYNC
          res = null;
          OStorageRemote.this.asynchExecutor.submit(new Runnable() {
            @Override
            public void run() {
              try {
                T inRes;
                try {
                  beginResponse(network, session);
                  inRes = response.read(network, session);
                } finally {
                  endResponse(network);
                }
                callback.call(recordId, response);
                connectionManager.release(network);
              } catch (Throwable e) {
                connectionManager.remove(network);
                OLogManager.instance().error(this, "Exception on async query", e);
              }
            }
          });
        } else {
          // NO RESPONSE
          connectionManager.release(network);
          res = null;
        }
        return res;
      }
    }, errorMessage, connectionRetry);
  }

  public <T> T networkOperationRetryTimeout(final OBinaryRequest request, final OBinaryResponse<T> response,
      final String errorMessage, int retry, int timeout) {
    return baseNetworkOperation(new OStorageRemoteOperation<T>() {
      @Override
      public T execute(OChannelBinaryAsynchClient network, OStorageRemoteSession session) throws IOException {
        try {
          network.beginRequest(request.getCommand(), session);
          request.write(network, session, 0);
        } finally {
          network.endRequest();
        }
        int prev = network.getSocketTimeout();
        final T res;
        try {
          if (timeout > 0)
            network.setSocketTimeout(timeout);
          beginResponse(network, session);
          res = response.read(network, session);
        } finally {
          endResponse(network);
          if (timeout > 0)
            network.setSocketTimeout(prev);
        }
        connectionManager.release(network);
        return res;
      }
    }, errorMessage, retry);
  }

  public <T> T networkOperation(final OBinaryRequest request, final OBinaryResponse<T> response, final String errorMessage) {
    return networkOperationRetryTimeout(request, response, errorMessage, connectionRetry, 0);
  }

  public <T> T baseNetworkOperation(final OStorageRemoteOperation<T> operation, final String errorMessage, int retry) {
    OStorageRemoteSession session = getCurrentSession();
    if (session.commandExecuting)
      throw new ODatabaseException(
          "Cannot execute the request because an asynchronous operation is in progress. Please use a different connection");

    do {
      session.commandExecuting = true;
      OChannelBinaryAsynchClient network = null;
      String serverUrl = getNextAvailableServerURL(false, session);
      do {
        try {
          network = getNetwork(serverUrl);
        } catch (OException e) {
          serverUrl = useNewServerURL(serverUrl);
          if (serverUrl == null)
            throw e;
        }
      } while (network == null);

      try {
        // In case i do not have a token or i'm switching between server i've to execute a open operation.
        OStorageRemoteNodeSession nodeSession = session.getServerSession(network.getServerURL());
        if (nodeSession == null || !nodeSession.isValid()) {
          openRemoteDatabase(network);
          if (!network.tryLock())
            continue;
        }

        return operation.execute(network, session);
      } catch (OModificationOperationProhibitedException mope) {
        connectionManager.release(network);
        handleDBFreeze();
      } catch (OTokenException e) {
        connectionManager.release(network);
        session.removeServerSession(network.getServerURL());
        if (--retry <= 0)
          throw OException.wrapException(new OStorageException(errorMessage), e);
      } catch (OTokenSecurityException e) {
        connectionManager.release(network);
        session.removeServerSession(network.getServerURL());
        if (--retry <= 0)
          throw OException.wrapException(new OStorageException(errorMessage), e);
      } catch (OOfflineNodeException e) {
        connectionManager.release(network);
        // Remove the current url because the node is offline
        synchronized (serverURLs) {
          serverURLs.remove(serverUrl);
        }
        for (OStorageRemoteSession activeSession : sessions) {
          // Not thread Safe ...
          activeSession.removeServerSession(serverUrl);
        }

      } catch (IOException e) {
        connectionManager.release(network);
        retry = handleIOException(retry, network, e);
      } catch (OIOException e) {
        connectionManager.release(network);
        retry = handleIOException(retry, network, e);
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

  private int handleIOException(int retry, final OChannelBinaryAsynchClient network, final Exception e) {
    OLogManager.instance().info(this, "Caught Network I/O errors on %s, trying an automatic reconnection... (error: %s)",
        network.getServerURL(), e.getMessage());
    OLogManager.instance().debug(this, "I/O error stack: ", e);
    connectionManager.remove(network);
    if (--retry <= 0)
      throw OException.wrapException(new OIOException(e.getMessage()), e);
    else {
      try {
        Thread.sleep(connectionRetryDelay);
      } catch (InterruptedException e1) {
        throw OException.wrapException(new OInterruptedException(e1.getMessage()), e);
      }
    }
    return retry;
  }

  @Override
  public boolean isAssigningClusterIds() {
    return false;
  }

  public int getSessionId() {
    OStorageRemoteSession session = getCurrentSession();
    return session != null ? session.getSessionId() : -1;
  }

  public String getServerURL() {
    OStorageRemoteSession session = getCurrentSession();
    return session != null ? session.getServerUrl() : null;
  }

  public void open(final String iUserName, final String iUserPassword, final OContextConfiguration conf) {

    stateLock.acquireWriteLock();
    addUser();
    try {
      OStorageRemoteSession session = getCurrentSession();
      if (status == STATUS.CLOSED || !iUserName.equals(session.connectionUserName)
          || !iUserPassword.equals(session.connectionUserPassword) || session.sessions.isEmpty()) {

        OCredentialInterceptor ci = OSecurityManager.instance().newCredentialInterceptor();

        if (ci != null) {
          ci.intercept(getURL(), iUserName, iUserPassword);
          session.connectionUserName = ci.getUsername();
          session.connectionUserPassword = ci.getPassword();
        } else // Do Nothing
        {
          session.connectionUserName = iUserName;
          session.connectionUserPassword = iUserPassword;
        }

        String strategy = conf.getValueAsString(OGlobalConfiguration.CLIENT_CONNECTION_STRATEGY);
        if (strategy != null)
          connectionStrategy = CONNECTION_STRATEGY.valueOf(strategy.toUpperCase());

        openRemoteDatabase();

        final OStorageConfiguration storageConfiguration = new OStorageRemoteConfiguration(this,
            ORecordSerializerFactory.instance().getDefaultRecordSerializer().toString());
        storageConfiguration.load(conf);

        configuration = storageConfiguration;

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
        throw OException.wrapException(new OStorageException("Cannot open the remote storage: " + name), e);

    } finally {
      stateLock.releaseWriteLock();
    }
  }

  @Override
  public OSBTreeCollectionManager getSBtreeCollectionManager() {
    return sbTreeCollectionManager;
  }

  public void reload() {

    OBinaryRequest request = new OReloadRequest();
    OBinaryResponse<OCluster[]> response = new OReloadResponse();

    OCluster[] res = networkOperation(request, response, "Error on reloading database information");
    updateStorageInformations(res);
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
    if (status == STATUS.CLOSED)
      return;

    stateLock.acquireWriteLock();
    try {
      if (status == STATUS.CLOSED)
        return;

      final OStorageRemoteSession session = getCurrentSession();
      if (session != null) {
        final Collection<OStorageRemoteNodeSession> nodes = session.getAllServerSessions();
        if (!nodes.isEmpty()) {
          for (OStorageRemoteNodeSession nodeSession : nodes) {
            OChannelBinaryAsynchClient network = null;
            try {
              network = getNetwork(nodeSession.getServerURL());
              OBinaryRequest request = new OCloseRequest();
              network.beginRequest(request.getCommand(), session);
              request.write(network, session, 0);
              endRequest(network);
              connectionManager.release(network);
            } catch (OIOException ex) {
              // IGNORING IF THE SERVER IS DOWN OR NOT REACHABLE THE SESSION IS AUTOMATICALLY CLOSED.
              OLogManager.instance().debug(this, "Impossible to comunicate to the server for close: %s", ex);
              connectionManager.remove(network);
            } catch (IOException ex) {
              // IGNORING IF THE SERVER IS DOWN OR NOT REACHABLE THE SESSION IS AUTOMATICALLY CLOSED.
              OLogManager.instance().debug(this, "Impossible to comunicate to the server for close: %s", ex);
              connectionManager.remove(network);
            }
          }
          session.close();
          sessions.remove(session);
          if (!checkForClose(iForce))
            return;
        } else {
          if (!iForce)
            return;
        }
      }

      status = STATUS.CLOSING;
      // CLOSE ALL THE CONNECTIONS
      for (String url : serverURLs) {
        connectionManager.closePool(url);
      }
      sbTreeCollectionManager.close();

      super.close(iForce, onDelete);
      status = STATUS.CLOSED;

    } finally {
      stateLock.releaseWriteLock();
    }
  }

  private boolean checkForClose(final boolean force) {
    if (status == STATUS.CLOSED)
      return false;

    if (status == STATUS.CLOSED)
      return false;

    final int remainingUsers = getUsers() > 0 ? removeUser() : 0;

    return force || remainingUsers == 0;
  }

  @Override
  public int getUsers() {
    return users.get();
  }

  @Override
  public int addUser() {
    return users.incrementAndGet();
  }

  @Override
  public int removeUser() {
    if (users.get() < 1)
      throw new IllegalStateException("Cannot remove user of the remote storage '" + toString() + "' because no user is using it");

    return users.decrementAndGet();
  }

  public void delete() {
    throw new UnsupportedOperationException(
        "Cannot delete a database in a remote server. Please use the console or the OServerAdmin class.");
  }

  public Set<String> getClusterNames() {
    stateLock.acquireReadLock();
    try {

      return new HashSet<String>(clusterMap.keySet());

    } finally {
      stateLock.releaseReadLock();
    }
  }

  public OStorageOperationResult<OPhysicalPosition> createRecord(final ORecordId iRid, final byte[] iContent,
      final int iRecordVersion, final byte iRecordType, final int iMode, final ORecordCallback<Long> iCallback) {

    final OSBTreeCollectionManager collectionManager = ODatabaseRecordThreadLocal.INSTANCE.get().getSbTreeCollectionManager();
    ORecordCallback<OCreateRecordResponse> realCallback = null;
    if (iCallback != null) {
      realCallback = (iRID, response) -> {
        iCallback.call(iRID, response.getIdentity().getClusterPosition());
        updateCollectionsFromChanges(collectionManager, response.getChangedIds());
      };
    }
    // The Upper layer require to return this also if it not really received response from the network
    final OPhysicalPosition ppos = new OPhysicalPosition(iRecordType);
    final OCreateRecordRequest request = new OCreateRecordRequest(iContent, iRid, iRecordType);
    final OCreateRecordResponse response = new OCreateRecordResponse();
    Long res = asyncNetworkOperation(request, response, iMode, iRid, realCallback,
        "Error on create record in cluster " + iRid.getClusterId());
    if (res != null) {
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

  private void updateCollectionsFromChanges(final OSBTreeCollectionManager collectionManager,
      final Map<UUID, OBonsaiCollectionPointer> changes) {
    if (collectionManager != null) {
      for (Entry<UUID, OBonsaiCollectionPointer> coll : changes.entrySet()) {
        collectionManager.updateCollectionPointer(coll.getKey(), coll.getValue());
      }
      if (ORecordSerializationContext.getDepth() <= 1)
        collectionManager.clearPendingCollections();
    }
  }

  @Override
  public ORecordMetadata getRecordMetadata(final ORID rid) {

    OBinaryRequest request = new OGetRecordMetadataRequest(rid);

    OBinaryResponse<ORecordMetadata> response = new OGetRecordMetadataResponse();

    return networkOperation(request, response, "Error on record metadata read " + rid);
  }

  @Override
  public OStorageOperationResult<ORawBuffer> readRecordIfVersionIsNotLatest(final ORecordId rid, final String fetchPlan,
      final boolean ignoreCache, final int recordVersion) throws ORecordNotFoundException {
    if (getCurrentSession().commandExecuting)
      // PENDING NETWORK OPERATION, CAN'T EXECUTE IT NOW
      return new OStorageOperationResult<ORawBuffer>(null);

    OBinaryRequest request = new OReadRecordIfVersionIsNotLatestRequest(rid, recordVersion, fetchPlan, ignoreCache);
    OBinaryResponse<OStorageOperationResult<ORawBuffer>> response = new OReadRecordIfVersionIsNotLatestResponse();

    return networkOperation(request, response, "Error on read record " + rid);
  }

  public OStorageOperationResult<ORawBuffer> readRecord(final ORecordId iRid, final String iFetchPlan, final boolean iIgnoreCache,
      boolean prefetchRecords, final ORecordCallback<ORawBuffer> iCallback) {

    if (getCurrentSession().commandExecuting)
      // PENDING NETWORK OPERATION, CAN'T EXECUTE IT NOW
      return new OStorageOperationResult<ORawBuffer>(null);

    OReadRecordRequest request = new OReadRecordRequest(iIgnoreCache, iRid, iFetchPlan, false);
    OReadRecordResponse response = new OReadRecordResponse();

    return networkOperation(request, response, "Error on read record " + iRid);
  }

  @Override
  public String incrementalBackup(final String backupDirectory) {
    OBinaryRequest request = new OIncrementalBackupRequest(backupDirectory);
    OBinaryResponse<String> response = new OIncrementalBackupResponse();

    return networkOperation(request, response, "Error on incremental backup");
  }

  @Override
  public void restoreFromIncrementalBackup(final String filePath) {
    throw new UnsupportedOperationException("This operations is part of internal API and is not supported in remote storage");
  }

  public OStorageOperationResult<Integer> updateRecord(final ORecordId iRid, final boolean updateContent, final byte[] iContent,
      final int iVersion, final byte iRecordType, final int iMode, final ORecordCallback<Integer> iCallback) {

    final OSBTreeCollectionManager collectionManager = ODatabaseRecordThreadLocal.INSTANCE.get().getSbTreeCollectionManager();

    ORecordCallback<OUpdateRecordResponse> realCallback = null;
    if (iCallback != null) {
      realCallback = (iRID, response) -> {
        iCallback.call(iRID, response.getVersion());
        updateCollectionsFromChanges(collectionManager, response.getChanges());
      };
    }

    OUpdateRecordRequest request = new OUpdateRecordRequest(iRid, iContent, iVersion, updateContent, iRecordType);
    OUpdateRecordResponse response = new OUpdateRecordResponse();
    Integer resVersion = asyncNetworkOperation(request, response, iMode, iRid, realCallback, "Error on update record " + iRid);

    if (resVersion != null) {
      // Returning given version in case of no answer from server
      resVersion = response.getVersion();
      updateCollectionsFromChanges(collectionManager, response.getChanges());
    }
    return new OStorageOperationResult<Integer>(resVersion);
  }

  @Override
  public OStorageOperationResult<Integer> recyclePosition(ORecordId iRecordId, byte[] iContent, int iVersion, byte recordType) {
    throw new UnsupportedOperationException("recyclePosition");
  }

  public OStorageOperationResult<Boolean> deleteRecord(final ORecordId iRid, final int iVersion, final int iMode,
      final ORecordCallback<Boolean> iCallback) {
    ORecordCallback<ODeleteRecordResponse> realCallback = null;
    if (iCallback != null)
      realCallback = (iRID, response) -> iCallback.call(iRID, response.getResult());

    final ODeleteRecordRequest request = new ODeleteRecordRequest(iRid, iVersion);
    final ODeleteRecordResponse response = new ODeleteRecordResponse();
    Boolean resDelete = asyncNetworkOperation(request, response, iMode, iRid, realCallback, "Error on delete record " + iRid);
    return new OStorageOperationResult<Boolean>(resDelete);
  }

  @Override
  public OStorageOperationResult<Boolean> hideRecord(final ORecordId recordId, final int mode,
      final ORecordCallback<Boolean> callback) {

    ORecordCallback<OHideRecordResponse> realCallback = null;
    if (callback != null)
      realCallback = (iRID, response) -> callback.call(iRID, response.getResult());

    final OHideRecordRequest request = new OHideRecordRequest(recordId);
    final OHideRecordResponse response = new OHideRecordResponse();
    Boolean resHide = asyncNetworkOperation(request, response, mode, recordId, realCallback, "Error on hide record " + recordId);
    return new OStorageOperationResult<Boolean>(resHide);
  }

  @Override
  public boolean cleanOutRecord(final ORecordId recordId, final int recordVersion, final int iMode,
      final ORecordCallback<Boolean> callback) {

    ORecordCallback<OCleanOutRecordResponse> realCallback = null;
    if (callback != null)
      realCallback = (iRID, response) -> callback.call(iRID, response.getResult());

    final OCleanOutRecordRequest request = new OCleanOutRecordRequest(recordVersion, recordId);
    final OCleanOutRecordResponse response = new OCleanOutRecordResponse();
    Boolean result = asyncNetworkOperation(request, response, iMode, recordId, realCallback, "Error on delete record " + recordId);
    return result;
  }

  @Override
  public List<String> backup(OutputStream out, Map<String, Object> options, Callable<Object> callable,
      final OCommandOutputListener iListener, int compressionLevel, int bufferSize) throws IOException {
    throw new UnsupportedOperationException(
        "backup is not supported against remote storage. Open the database with plocal or use the incremental backup in the Enterprise Edition");
  }

  @Override
  public void restore(InputStream in, Map<String, Object> options, Callable<Object> callable,
      final OCommandOutputListener iListener) throws IOException {
    throw new UnsupportedOperationException(
        "restore is not supported against remote storage. Open the database with plocal or use Enterprise Edition");
  }

  public OContextConfiguration getClientConfiguration() {
    return clientConfiguration;
  }

  public long count(final int iClusterId) {
    return count(new int[] { iClusterId });
  }

  @Override
  public long count(int iClusterId, boolean countTombstones) {
    return count(new int[] { iClusterId }, countTombstones);
  }

  public long[] getClusterDataRange(final int iClusterId) {
    OBinaryRequest request = new OGetClusterDataRangeRequest(iClusterId);
    OBinaryResponse<long[]> response = new OGetClusterDataRangeResponse();

    return networkOperation(request, response, "Error on getting last entry position count in cluster: " + iClusterId);
  }

  @Override
  public OPhysicalPosition[] higherPhysicalPositions(final int iClusterId, final OPhysicalPosition iClusterPosition) {
    OBinaryRequest request = new OHigherPhysicalPositionsRequest(iClusterId, iClusterPosition);

    OBinaryResponse<OPhysicalPosition[]> response = new OHigherPhysicalPositionsResponse();

    return networkOperation(request, response, "Error on retrieving higher positions after " + iClusterPosition.clusterPosition);
  }

  @Override
  public OPhysicalPosition[] ceilingPhysicalPositions(final int clusterId, final OPhysicalPosition physicalPosition) {

    OBinaryRequest request = new OCeilingPhysicalPositionsRequest(clusterId, physicalPosition);

    OBinaryResponse<OPhysicalPosition[]> response = new OCeilingPhysicalPositionsResponse();

    return networkOperation(request, response, "Error on retrieving ceiling positions after " + physicalPosition.clusterPosition);
  }

  @Override
  public OPhysicalPosition[] lowerPhysicalPositions(final int iClusterId, final OPhysicalPosition physicalPosition) {
    OBinaryRequest request = new OLowerPhysicalPositionsRequest(physicalPosition, iClusterId);
    OBinaryResponse<OPhysicalPosition[]> response = new OLowerPhysicalPositionsResponse();
    return networkOperation(request, response, "Error on retrieving lower positions after " + physicalPosition.clusterPosition);
  }

  @Override
  public OPhysicalPosition[] floorPhysicalPositions(final int clusterId, final OPhysicalPosition physicalPosition) {
    OBinaryRequest request = new OFloorPhysicalPositionsRequest(physicalPosition, clusterId);
    OBinaryResponse<OPhysicalPosition[]> response = new OFloorPhysicalPositionsResponse();

    return networkOperation(request, response, "Error on retrieving floor positions after " + physicalPosition.clusterPosition);
  }

  public long getSize() {
    OBinaryRequest request = new OGetSizeRequest();
    OBinaryResponse<Long> response = new OGetSizeResponse();
    return networkOperation(request, response, "Error on read database size");
  }

  @Override
  public long countRecords() {
    OBinaryRequest request = new OCountRecordsRequest();
    OBinaryResponse<Long> response = new OCountRecordsResponse();
    return networkOperation(request, response, "Error on read database record count");
  }

  public long count(final int[] iClusterIds) {
    return count(iClusterIds, false);
  }

  public long count(final int[] iClusterIds, final boolean countTombstones) {
    OBinaryRequest request = new OCountRequest(iClusterIds, countTombstones);
    OBinaryResponse<Long> response = new OCountResponse();

    return networkOperation(request, response, "Error on read record count in clusters: " + Arrays.toString(iClusterIds));
  }

  /**
   * Execute the command remotely and get the results back.
   */
  public Object command(final OCommandRequestText iCommand) {

    final boolean live = iCommand instanceof OLiveQuery;
    final ODatabaseDocument database = ODatabaseRecordThreadLocal.INSTANCE.get();
    final boolean asynch = iCommand instanceof OCommandRequestAsynch && ((OCommandRequestAsynch) iCommand).isAsynchronous();

    OBinaryRequest request = new OCommandRequest(asynch, iCommand, live);
    OBinaryResponse<Object> response = new OCommandResponse(this, asynch, iCommand.getResultListener(), database, live);

    return networkOperation(request, response, "Error on executing command: " + iCommand);

  }

  public List<ORecordOperation> commit(final OTransaction iTx, final Runnable callback) {
    OBinaryRequest request = new OCommitRequest(iTx.getId(), iTx.isUsingLog(),
        (Iterable<ORecordOperation>) iTx.getAllRecordEntries(), iTx.getIndexChanges());

    OCommitResponse response = new OCommitResponse();

    networkOperation(request, response, "Error on commit");

    for (OCreatedRecordResponse created : response.getCreated()) {
      iTx.updateIdentityAfterCommit(created.getCurrentRid(), created.getCreatedRid());
    }
    for (OUpdatedRecordResponse updated : response.getUpdated()) {
      ORecordOperation rop = iTx.getRecordEntry(updated.getRid());
      if (rop != null) {
        if (updated.getVersion() > rop.getRecord().getVersion() + 1)
          // IN CASE OF REMOTE CONFLICT STRATEGY FORCE UNLOAD DUE TO INVALID CONTENT
          rop.getRecord().unload();
        ORecordInternal.setVersion(rop.getRecord(), updated.getVersion());
      }
    }
    updateCollectionsFromChanges(ODatabaseRecordThreadLocal.INSTANCE.get().getSbTreeCollectionManager(),
        response.getCollectionChanges());
    // SET ALL THE RECORDS AS UNDIRTY
    for (ORecordOperation txEntry : iTx.getAllRecordEntries())
      ORecordInternal.unsetDirty(txEntry.getRecord());

    // UPDATE THE CACHE ONLY IF THE ITERATOR ALLOWS IT. USE THE STRATEGY TO ALWAYS REMOVE ALL THE RECORDS SINCE THEY COULD BE
    // CHANGED AS CONTENT IN CASE OF TREE AND GRAPH DUE TO CROSS REFERENCES
    OTransactionAbstract.updateCacheFromEntries(iTx, iTx.getAllRecordEntries(), false);
    return null;
  }

  public void rollback(OTransaction iTx) {
  }

  public int getClusterIdByName(final String iClusterName) {
    stateLock.acquireReadLock();
    try {

      if (iClusterName == null)
        return -1;

      if (Character.isDigit(iClusterName.charAt(0)))
        return Integer.parseInt(iClusterName);

      final OCluster cluster = clusterMap.get(iClusterName.toLowerCase());
      if (cluster == null)
        return -1;

      return cluster.getId();
    } finally {
      stateLock.releaseReadLock();
    }
  }

  public int getDefaultClusterId() {
    return defaultClusterId;
  }

  public void setDefaultClusterId(int defaultClusterId) {
    this.defaultClusterId = defaultClusterId;
  }

  public int addCluster(final String iClusterName, boolean forceListBased, final Object... iArguments) {
    return addCluster(iClusterName, -1, forceListBased, iArguments);
  }

  public int addCluster(final String iClusterName, final int iRequestedId, final boolean forceListBased,
      final Object... iParameters) {
    OBinaryRequest request = new OAddClusterRequest(iRequestedId, iClusterName);
    OBinaryResponse<Integer> response = new OAddClusterResponse();
    Integer clusterId = networkOperation(request, response, "Error on add new cluster");
    addNewClusterToConfiguration(clusterId, iClusterName);
    return clusterId;
  }

  public boolean dropCluster(final int iClusterId, final boolean iTruncate) {

    OBinaryRequest request = new ODropClusterRequest(iClusterId);

    OBinaryResponse<Boolean> response = new ODropClusterResponse();

    Boolean clusterRemoved = networkOperation(request, response, "Error on removing of cluster");
    if (clusterRemoved)
      removeClusterFromConfiguration(iClusterId);
    return clusterRemoved;
  }

  public void removeClusterFromConfiguration(int iClusterId) {
    stateLock.acquireWriteLock();
    try {
      // REMOVE THE CLUSTER LOCALLY
      final OCluster cluster = clusters[iClusterId];
      clusters[iClusterId] = null;
      clusterMap.remove(cluster.getName());
      if (configuration.clusters.size() > iClusterId)
        configuration.dropCluster(iClusterId); // endResponse must be called before this line, which call updateRecord
    } finally {
      stateLock.releaseWriteLock();
    }
  }

  public void synch() {
  }

  public String getPhysicalClusterNameById(final int iClusterId) {
    stateLock.acquireReadLock();
    try {

      if (iClusterId >= clusters.length)
        return null;

      final OCluster cluster = clusters[iClusterId];
      return cluster != null ? cluster.getName() : null;

    } finally {
      stateLock.releaseReadLock();
    }
  }

  public int getClusterMap() {
    stateLock.acquireReadLock();
    try {
      return clusterMap.size();
    } finally {
      stateLock.releaseReadLock();
    }
  }

  public Collection<OCluster> getClusterInstances() {
    stateLock.acquireReadLock();
    try {

      return Arrays.asList(clusters);

    } finally {
      stateLock.releaseReadLock();
    }
  }

  public OCluster getClusterById(int iClusterId) {
    stateLock.acquireReadLock();
    try {

      if (iClusterId == ORID.CLUSTER_ID_INVALID)
        // GET THE DEFAULT CLUSTER
        iClusterId = defaultClusterId;

      return clusters[iClusterId];

    } finally {
      stateLock.releaseReadLock();
    }
  }

  @Override
  public long getVersion() {
    throw new UnsupportedOperationException("getVersion");
  }

  public ODocument getClusterConfiguration() {
    return clusterConfiguration;
  }

  /**
   * Ends the request and unlock the write lock
   */
  public void endRequest(final OChannelBinaryAsynchClient iNetwork) throws IOException {
    if (iNetwork == null)
      return;

    iNetwork.flush();
    iNetwork.releaseWriteLock();

  }

  /**
   * End response reached: release the channel in the pool to being reused
   */
  public void endResponse(final OChannelBinaryAsynchClient iNetwork) throws IOException {
    iNetwork.endResponse();
  }

  @Override
  public boolean isRemote() {
    return true;
  }

  public boolean isPermanentRequester() {
    return false;
  }

  @SuppressWarnings("unchecked")
  public void updateClusterConfiguration(final String iConnectedURL, final byte[] obj) {
    if (obj == null)
      return;

    // TEMPORARY FIX: DISTRIBUTED MODE DOESN'T SUPPORT TREE BONSAI, KEEP ALWAYS EMBEDDED RIDS
    OGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.setValue(Integer.MAX_VALUE);

    final List<ODocument> members;
    synchronized (clusterConfiguration) {
      clusterConfiguration.fromStream(obj);
      clusterConfiguration.toString();
      members = clusterConfiguration.field("members");
    }

    // UPDATE IT
    synchronized (serverURLs) {
      if (members != null) {
        // ADD CURRENT SERVER AS FIRST
        if (iConnectedURL != null) {
          addHost(iConnectedURL);
        }

        for (ODocument m : members) {
          if (m == null)
            continue;

          final String nodeStatus = m.field("status");

          if (m != null && !"OFFLINE".equals(nodeStatus)) {
            final Collection<Map<String, Object>> listeners = ((Collection<Map<String, Object>>) m.field("listeners"));
            if (listeners != null)
              for (Map<String, Object> listener : listeners) {
                if (((String) listener.get("protocol")).equals("ONetworkProtocolBinary")) {
                  String url = (String) listener.get("listen");
                  if (!serverURLs.contains(url))
                    addHost(url);
                }
              }
          }
        }
      }
    }
  }

  public void removeSessions(final String url) {
    synchronized (serverURLs) {
      serverURLs.remove(url);
    }

    for (OStorageRemoteSession session : sessions) {
      session.removeServerSession(url + "/" + getName());
    }
  }

  @Override
  public OCluster getClusterByName(final String iClusterName) {
    throw new UnsupportedOperationException("getClusterByName()");
  }

  @Override
  public ORecordConflictStrategy getConflictStrategy() {
    throw new UnsupportedOperationException("getConflictStrategy");
  }

  @Override
  public void setConflictStrategy(final ORecordConflictStrategy iResolver) {
    throw new UnsupportedOperationException("setConflictStrategy");
  }

  @Override
  public String getURL() {
    return OEngineRemote.NAME + ":" + url;
  }

  public int getClusters() {
    stateLock.acquireReadLock();
    try {
      return clusterMap.size();
    } finally {
      stateLock.releaseReadLock();
    }
  }

  @Override
  public String getType() {
    return OEngineRemote.NAME;
  }

  @Override
  public String getUserName() {
    final OStorageRemoteSession session = getCurrentSession();
    if (session == null)
      return null;
    return session.connectionUserName;
  }

  protected String reopenRemoteDatabase() throws IOException {
    String currentURL = getCurrentServerURL();
    do {
      do {
        final OChannelBinaryAsynchClient network = getNetwork(currentURL);
        try {
          OStorageRemoteSession session = getCurrentSession();
          OStorageRemoteNodeSession nodeSession = session.getOrCreateServerSession(network.getServerURL());
          if (nodeSession == null || !nodeSession.isValid()) {
            openRemoteDatabase(network);
            return network.getServerURL();
          } else {
            OBinaryRequest request = new OReopenRequest();
            OReopenResponse response = new OReopenResponse();

            try {
              network.writeByte(request.getCommand());
              network.writeInt(nodeSession.getSessionId());
              network.writeBytes(nodeSession.getToken());
              request.write(network, session, 0);
            } finally {
              endRequest(network);
            }

            final int sessionId;

            try {
              byte[] newToken = network.beginResponse(nodeSession.getSessionId(), true);
              response.read(network, session);
              if (newToken != null && newToken.length > 0) {
                nodeSession.setSession(response.getSessionId(), newToken);
              } else {
                nodeSession.setSession(response.getSessionId(), nodeSession.getToken());
              }
              OLogManager.instance().debug(this, "Client connected to %s with session id=%d", network.getServerURL(),
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
              OLogManager.instance().debug(this, "Cannot remove connection or database url=" + currentURL, e);
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
              OLogManager.instance().debug(this, "Cannot remove connection or database url=" + currentURL, e);
            }
          }
        }
      } while (connectionManager.getAvailableConnections(currentURL) > 0);

      currentURL = useNewServerURL(currentURL);

    } while (currentURL != null);

    // REFILL ORIGINAL SERVER LIST
    parseServerURLs();

    synchronized (serverURLs) {
      throw new OStorageException("Cannot create a connection to remote server address(es): " + serverURLs);
    }
  }

  protected synchronized String openRemoteDatabase() throws IOException {
    final String currentURL = getNextAvailableServerURL(true, getCurrentSession());
    return openRemoteDatabase(currentURL);
  }

  public void openRemoteDatabase(OChannelBinaryAsynchClient network) throws IOException {

    stateLock.acquireWriteLock();
    try {
      OStorageRemoteSession session = getCurrentSession();
      OStorageRemoteNodeSession nodeSession = session.getOrCreateServerSession(network.getServerURL());
      OBinaryRequest request = new OOpenRequest(name, session.connectionUserName, session.connectionUserPassword);
      try {
        network.writeByte(request.getCommand());
        network.writeInt(nodeSession.getSessionId());
        request.write(network, session, 0);
      } finally {
        endRequest(network);
      }
      final int sessionId;
      OOpenResponse response = new OOpenResponse();
      try {
        network.beginResponse(nodeSession.getSessionId(), false);
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

      OLogManager.instance().debug(this, "Client connected to %s with session id=%d", network.getServerURL(), sessionId);

      OCluster[] cl = response.getClusterIds();
      updateStorageInformations(cl);

      // READ CLUSTER CONFIGURATION
      updateClusterConfiguration(network.getServerURL(), response.getDistributedConfiguration());

      status = STATUS.OPEN;
    } finally {
      stateLock.releaseWriteLock();
    }
  }

  protected String openRemoteDatabase(String currentURL) {
    do {
      do {
        OChannelBinaryAsynchClient network = null;
        try {
          network = getNetwork(currentURL);
          openRemoteDatabase(network);
          return currentURL;
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

        } catch (Exception e) {
          if (network != null) {
            // REMOVE THE NETWORK CONNECTION IF ANY
            try {
              connectionManager.remove(network);
            } catch (Exception ex) {
              // IGNORE ANY EXCEPTION
              OLogManager.instance().debug(this, "Cannot remove connection or database url=" + currentURL, e);
            }
          }

          OLogManager.instance().error(this, "Cannot open database url=" + currentURL, e);
        }
      } while (connectionManager.getReusableConnections(currentURL) > 0);

      currentURL = useNewServerURL(currentURL);

    } while (currentURL != null);

    // REFILL ORIGINAL SERVER LIST
    parseServerURLs();

    synchronized (serverURLs) {
      throw new OStorageException("Cannot create a connection to remote server address(es): " + serverURLs);
    }
  }

  protected String useNewServerURL(final String iUrl) {
    int pos = iUrl.indexOf('/');
    if (pos >= iUrl.length() - 1)
      // IGNORE ENDING /
      pos = -1;

    final String postFix = pos > -1 ? iUrl.substring(pos) : "";
    final String url = pos > -1 ? iUrl.substring(0, pos) : iUrl;

    synchronized (serverURLs) {
      // REMOVE INVALID URL
      serverURLs.remove(url);
      for (OStorageRemoteSession activeSession : sessions) {
        // Not thread Safe ...
        activeSession.removeServerSession(url + "/" + getName());
      }

      OLogManager.instance().debug(this, "Updated server list: %s...", serverURLs);

      if (!serverURLs.isEmpty())
        return serverURLs.get(0) + postFix;
    }

    return null;
  }

  /**
   * Parse the URLs. Multiple URLs must be separated by semicolon (;)
   */
  protected void parseServerURLs() {
    String lastHost = null;
    int dbPos = url.indexOf('/');
    if (dbPos == -1) {
      // SHORT FORM
      addHost(url);
      lastHost = url;
      name = url;
    } else {
      name = url.substring(url.lastIndexOf("/") + 1);
      for (String host : url.substring(0, dbPos).split(ADDRESS_SEPARATOR)) {
        lastHost = host;
        addHost(host);
      }
    }

    synchronized (serverURLs) {
      if (serverURLs.size() == 1 && OGlobalConfiguration.NETWORK_BINARY_DNS_LOADBALANCING_ENABLED.getValueAsBoolean()) {
        // LOOK FOR LOAD BALANCING DNS TXT RECORD
        final String primaryServer = lastHost;

        OLogManager.instance().debug(this, "Retrieving URLs from DNS '%s' (timeout=%d)...", primaryServer,
            OGlobalConfiguration.NETWORK_BINARY_DNS_LOADBALANCING_TIMEOUT.getValueAsInteger());

        try {
          final Hashtable<String, String> env = new Hashtable<String, String>();
          env.put("java.naming.factory.initial", "com.sun.jndi.dns.DnsContextFactory");
          env.put("com.sun.jndi.ldap.connect.timeout",
              OGlobalConfiguration.NETWORK_BINARY_DNS_LOADBALANCING_TIMEOUT.getValueAsString());
          final DirContext ictx = new InitialDirContext(env);
          final String hostName = !primaryServer.contains(":") ? primaryServer
              : primaryServer.substring(0, primaryServer.indexOf(":"));
          final Attributes attrs = ictx.getAttributes(hostName, new String[] { "TXT" });
          final Attribute attr = attrs.get("TXT");
          if (attr != null) {
            for (int i = 0; i < attr.size(); ++i) {
              String configuration = (String) attr.get(i);
              if (configuration.startsWith("\""))
                configuration = configuration.substring(1, configuration.length() - 1);
              if (configuration != null) {
                serverURLs.clear();
                final String[] parts = configuration.split(" ");
                for (String part : parts) {
                  if (part.startsWith("s=")) {
                    addHost(part.substring("s=".length()));
                  }
                }
              }
            }
          }
        } catch (NamingException ignore) {
        }
      }
    }
  }

  /**
   * Registers the remote server with port.
   */
  protected String addHost(String host) {
    if (host.startsWith(LOCALHOST))
      host = LOCAL_IP + host.substring("localhost".length());

    if (host.contains("/"))
      host = host.substring(0, host.indexOf("/"));

    // REGISTER THE REMOTE SERVER+PORT
    if (!host.contains(":"))
      host += ":"
          + (clientConfiguration.getValueAsBoolean(OGlobalConfiguration.CLIENT_USE_SSL) ? getDefaultSSLPort() : getDefaultPort());
    else if (host.split(":").length < 2 || host.split(":")[1].trim().length() == 0)
      host += (clientConfiguration.getValueAsBoolean(OGlobalConfiguration.CLIENT_USE_SSL) ? getDefaultSSLPort() : getDefaultPort());

    // DISABLED BECAUSE THIS DID NOT ALLOW TO CONNECT TO LOCAL HOST ANYMORE IF THE SERVER IS BOUND TO 127.0.0.1
    // CONVERT 127.0.0.1 TO THE PUBLIC IP IF POSSIBLE
    // if (host.startsWith(LOCAL_IP)) {
    // try {
    // final String publicIP = InetAddress.getLocalHost().getHostAddress();
    // host = publicIP + host.substring(LOCAL_IP.length());
    // } catch (UnknownHostException e) {
    // // IGNORE IT
    // }
    // }

    synchronized (serverURLs) {
      if (!serverURLs.contains(host)) {
        serverURLs.add(host);
        OLogManager.instance().debug(this, "Registered the new available server '%s'", host);
      }
    }

    return host;
  }

  protected int getDefaultPort() {
    return DEFAULT_PORT;
  }

  protected int getDefaultSSLPort() {
    return DEFAULT_SSL_PORT;
  }

  /**
   * Acquire a network channel from the pool. Don't lock the write stream since the connection usage is exclusive.
   *
   * @param iCommand id. Ids described at {@link OChannelBinaryProtocol}
   * @param session
   * @return connection to server
   * @throws IOException
   */
  public OChannelBinaryAsynchClient beginRequest(final OChannelBinaryAsynchClient network, final byte iCommand,
      OStorageRemoteSession session) throws IOException {
    network.beginRequest(iCommand, session);
    return network;
  }

  protected String getNextAvailableServerURL(boolean iIsConnectOperation, OStorageRemoteSession session) {
    String url = null;
    switch (connectionStrategy) {
    case STICKY:
      url = session != null ? session.getServerUrl() : null;
      if (url == null)
        url = getServerURFromList(false, session);
      break;

    case ROUND_ROBIN_CONNECT:
      if (!iIsConnectOperation)
        url = session != null ? session.getServerUrl() : null;

      if (url == null)
        url = getServerURFromList(iIsConnectOperation, session);
      OLogManager.instance().debug(this,
          "ROUND_ROBIN_CONNECT: Next remote operation will be executed on server: %s (isConnectOperation=%s)", url,
          iIsConnectOperation);
      break;

    case ROUND_ROBIN_REQUEST:
      url = getServerURFromList(true, session);
      OLogManager.instance().debug(this,
          "ROUND_ROBIN_REQUEST: Next remote operation will be executed on server: %s (isConnectOperation=%s)", url,
          iIsConnectOperation);
      break;

    default:
      throw new OConfigurationException("Connection mode " + connectionStrategy + " is not supported");
    }

    return url;
  }

  protected String getCurrentServerURL() {
    return getServerURFromList(false, getCurrentSession());
  }

  protected String getServerURFromList(final boolean iNextAvailable, OStorageRemoteSession session) {
    synchronized (serverURLs) {
      if (serverURLs.isEmpty()) {
        parseServerURLs();
        if (serverURLs.isEmpty())
          throw new OStorageException("Cannot create a connection to remote server because url list is empty");
      }

      // GET CURRENT THREAD INDEX
      int serverURLIndex;
      if (session != null)
        serverURLIndex = session.serverURLIndex;
      else
        serverURLIndex = 0;

      if (iNextAvailable)
        serverURLIndex++;

      if (serverURLIndex < 0 || serverURLIndex >= serverURLs.size())
        // RESET INDEX
        serverURLIndex = 0;

      final String serverURL = serverURLs.get(serverURLIndex) + "/" + getName();

      if (session != null)
        session.serverURLIndex = serverURLIndex;

      return serverURL;
    }
  }

  public OChannelBinaryAsynchClient getNetwork(final String iCurrentURL) {
    OChannelBinaryAsynchClient network;
    do {
      try {
        network = connectionManager.acquire(iCurrentURL, clientConfiguration, asynchEventListener);
      } catch (OIOException cause) {
        throw cause;
      } catch (Exception cause) {
        throw OException.wrapException(new OStorageException("Cannot open a connection to remote server: " + iCurrentURL), cause);
      }
      if (!network.tryLock()) {
        // CANNOT LOCK IT, MAYBE HASN'T BE CORRECTLY UNLOCKED BY PREVIOUS USER?
        OLogManager.instance().error(this, "Removing locked network channel '%s' (connected=%s)...", iCurrentURL,
            network.isConnected());
        connectionManager.remove(network);
        network = null;
      }
    } while (network == null);
    return network;
  }

  public void beginResponse(OChannelBinaryAsynchClient iNetwork, OStorageRemoteSession session) throws IOException {
    OStorageRemoteNodeSession nodeSession = session.getServerSession(iNetwork.getServerURL());
    byte[] newToken = iNetwork.beginResponse(nodeSession.getSessionId(), true);
    if (newToken != null && newToken.length > 0) {
      nodeSession.setSession(nodeSession.getSessionId(), newToken);
    }
  }

  protected void getResponse(final OChannelBinaryAsynchClient iNetwork, OStorageRemoteSession session) throws IOException {
    try {
      beginResponse(iNetwork, session);
    } finally {
      endResponse(iNetwork);
    }
  }

  public static OPhysicalPosition[] readPhysicalPositions(OChannelBinaryAsynchClient network, int positionsCount)
      throws IOException {
    final OPhysicalPosition[] physicalPositions = new OPhysicalPosition[positionsCount];

    for (int i = 0; i < physicalPositions.length; i++) {
      final OPhysicalPosition position = new OPhysicalPosition();

      position.clusterPosition = network.readLong();
      position.recordSize = network.readInt();
      position.recordVersion = network.readVersion();

      physicalPositions[i] = position;
    }
    return physicalPositions;
  }

  public static void readCollectionChanges(OChannelBinaryAsynchClient network, OSBTreeCollectionManager collectionManager)
      throws IOException {
    int count = network.readInt();

    for (int i = 0; i < count; i++) {
      final long mBitsOfId = network.readLong();
      final long lBitsOfId = network.readLong();

      final OBonsaiCollectionPointer pointer = OCollectionNetworkSerializer.INSTANCE.readCollectionPointer(network);

      if (collectionManager != null)
        collectionManager.updateCollectionPointer(new UUID(mBitsOfId, lBitsOfId), pointer);
    }

    if (ORecordSerializationContext.getDepth() <= 1 && collectionManager != null)
      collectionManager.clearPendingCollections();
  }

  private boolean handleDBFreeze() {
    boolean retry;
    OLogManager.instance().warn(this,
        "DB is frozen will wait for " + OGlobalConfiguration.CLIENT_DB_RELEASE_WAIT_TIMEOUT.getValue() + " ms. and then retry.");
    retry = true;
    try {
      Thread.sleep(OGlobalConfiguration.CLIENT_DB_RELEASE_WAIT_TIMEOUT.getValueAsInteger());
    } catch (InterruptedException ie) {
      retry = false;

      Thread.currentThread().interrupt();
    }
    return retry;
  }

  public static OCluster[] readClusterArray(final OChannelBinaryAsynchClient network) throws IOException {

    final int tot = network.readShort();
    OCluster[] clusters = new OCluster[tot];
    for (int i = 0; i < tot; ++i) {
      final OClusterRemote cluster = new OClusterRemote();
      String clusterName = network.readString();
      final int clusterId = network.readShort();
      if (clusterName != null) {
        clusterName = clusterName.toLowerCase();
        cluster.configure(null, clusterId, clusterName);
        if (clusterId >= clusters.length)
          clusters = Arrays.copyOf(clusters, clusterId + 1);
        clusters[clusterId] = cluster;
      }
    }
    return clusters;
  }

  public void updateStorageInformations(OCluster[] clusters) {
    stateLock.acquireWriteLock();
    try {
      this.clusters = clusters;
      clusterMap.clear();
      for (int i = 0; i < clusters.length; ++i) {
        if (clusters[i] != null)
          clusterMap.put(clusters[i].getName(), clusters[i]);
      }
      final OCluster defaultCluster = clusterMap.get(CLUSTER_DEFAULT_NAME);
      if (defaultCluster != null)
        defaultClusterId = clusterMap.get(CLUSTER_DEFAULT_NAME).getId();
    } finally {
      stateLock.releaseWriteLock();
    }
  }

  protected OStorageRemoteSession getCurrentSession() {
    ODatabaseDocumentInternal db = null;
    if (ODatabaseRecordThreadLocal.INSTANCE != null)
      db = ODatabaseRecordThreadLocal.INSTANCE.getIfDefined();
    ODatabaseDocumentRemote remote = (ODatabaseDocumentRemote) ODatabaseDocumentTxInternal.getInternal(db);
    if (remote == null)
      return null;
    OStorageRemoteSession session = (OStorageRemoteSession) remote.getSessionMetadata();
    if (session == null) {
      session = new OStorageRemoteSession(sessionSerialId.decrementAndGet());
      sessions.add(session);
      remote.setSessionMetadata(session);
    }
    return session;
  }

  @Override
  public boolean isClosed() {
    if (super.isClosed())
      return true;
    final OStorageRemoteSession session = getCurrentSession();
    if (session == null)
      return false;
    return session.isClosed();
  }

  public OStorageRemote copy(final ODatabaseDocumentRemote source, final ODatabaseDocumentRemote dest) {
    ODatabaseDocumentInternal origin = null;
    if (ODatabaseRecordThreadLocal.INSTANCE != null)
      origin = ODatabaseRecordThreadLocal.INSTANCE.getIfDefined();

    origin = ODatabaseDocumentTxInternal.getInternal(origin);

    final OStorageRemoteSession session = source.getSessionMetadata();
    if (session != null) {
      // TODO:may run a session reopen
      final OStorageRemoteSession newSession = new OStorageRemoteSession(sessionSerialId.decrementAndGet());
      newSession.connectionUserName = session.connectionUserName;
      newSession.connectionUserPassword = session.connectionUserPassword;
      dest.setSessionMetadata(newSession);
    }
    try {
      dest.activateOnCurrentThread();
      openRemoteDatabase();
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      ODatabaseRecordThreadLocal.INSTANCE.set(origin);
    }
    return this;
  }

  public void importDatabase(final String options, final InputStream inputStream, final String name,
      final OCommandOutputListener listener) {
    OBinaryRequest request = new OImportRequest(inputStream, options, name);

    OImportResponse response = new OImportResponse();

    networkOperationRetryTimeout(request, response, "Error sending import request", 0,
        OGlobalConfiguration.NETWORK_REQUEST_TIMEOUT.getValueAsInteger());

    for (String message : response.getMessages()) {
      listener.onMessage(message);
    }

  }

  public void addNewClusterToConfiguration(int clusterId, String iClusterName) {
    stateLock.acquireWriteLock();
    try {
      final OClusterRemote cluster = new OClusterRemote();
      cluster.configure(this, clusterId, iClusterName.toLowerCase());

      if (clusters.length <= clusterId)
        clusters = Arrays.copyOf(clusters, clusterId + 1);
      clusters[cluster.getId()] = cluster;
      clusterMap.put(cluster.getName().toLowerCase(), cluster);
    } finally {
      stateLock.releaseWriteLock();
    }
  }

}
