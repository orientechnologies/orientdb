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
import com.orientechnologies.common.util.OCommonConst;
import com.orientechnologies.orient.client.binary.OChannelBinaryAsynchClient;
import com.orientechnologies.orient.client.remote.message.*;
import com.orientechnologies.orient.client.remote.message.OCommitResponse.OCreatedRecordResponse;
import com.orientechnologies.orient.client.remote.message.OCommitResponse.OUpdatedRecordResponse;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.command.OCommandRequestAsynch;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.config.OStorageConfiguration;
import com.orientechnologies.orient.core.conflict.ORecordConflictStrategy;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentRemote;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTxInternal;
import com.orientechnologies.orient.core.db.document.OTransactionOptimisticClient;
import com.orientechnologies.orient.core.db.record.OCurrentStorageComponentsFactory;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.db.record.ridbag.sbtree.OBonsaiCollectionPointer;
import com.orientechnologies.orient.core.db.record.ridbag.sbtree.OSBTreeCollectionManager;
import com.orientechnologies.orient.core.exception.*;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.security.OTokenException;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.security.OCredentialInterceptor;
import com.orientechnologies.orient.core.security.OSecurityManager;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializerFactory;
import com.orientechnologies.orient.core.sql.query.OLiveQuery;
import com.orientechnologies.orient.core.storage.*;
import com.orientechnologies.orient.core.storage.impl.local.paginated.ORecordSerializationContext;
import com.orientechnologies.orient.core.tx.OTransaction;
import com.orientechnologies.orient.core.tx.OTransactionAbstract;
import com.orientechnologies.orient.core.tx.OTransactionOptimistic;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;
import com.orientechnologies.orient.enterprise.channel.binary.ODistributedRedirectException;
import com.orientechnologies.orient.enterprise.channel.binary.OTokenSecurityException;

import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This object is bound to each remote ODatabase instances.
 */
public class OStorageRemote extends OStorageAbstract implements OStorageProxy {
  @Deprecated
  public static final String PARAM_CONNECTION_STRATEGY = "connectionStrategy";

  private static final String        DEFAULT_HOST      = "localhost";
  private static final int           DEFAULT_PORT      = 2424;
  private static final int           DEFAULT_SSL_PORT  = 2434;
  private static final String        ADDRESS_SEPARATOR = ";";
  public static final  String        DRIVER_NAME       = "OrientDB Java";
  private static final String        LOCAL_IP          = "127.0.0.1";
  private static final String        LOCALHOST         = "localhost";
  private static       AtomicInteger sessionSerialId   = new AtomicInteger(-1);

  public enum CONNECTION_STRATEGY {
    STICKY, ROUND_ROBIN_CONNECT, ROUND_ROBIN_REQUEST
  }

  private CONNECTION_STRATEGY connectionStrategy = CONNECTION_STRATEGY.STICKY;

  private final   OSBTreeCollectionManagerRemote sbTreeCollectionManager = new OSBTreeCollectionManagerRemote(this);
  protected final List<String>                   serverURLs              = new ArrayList<String>();
  protected final Map<String, OCluster>          clusterMap              = new ConcurrentHashMap<String, OCluster>();
  private final ExecutorService asynchExecutor;
  private final ODocument clusterConfiguration = new ODocument();
  private final String clientId;
  private final AtomicInteger users = new AtomicInteger(0);
  private OContextConfiguration clientConfiguration;
  private int                   connectionRetry;
  private int                   connectionRetryDelay;
  OCluster[] clusters = OCommonConst.EMPTY_CLUSTER_ARRAY;
  private int                               defaultClusterId;
  public  OStorageRemoteAsynchEventListener asynchEventListener;
  public  ORemoteConnectionManager          connectionManager;
  private final Set<OStorageRemoteSession> sessions = Collections
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

  public <T extends OBinaryResponse> T asyncNetworkOperation(final OBinaryAsyncRequest<T> request, int mode,
      final ORecordId recordId, final ORecordCallback<T> callback, final String errorMessage) {
    final int pMode;
    if (mode == 1 && callback == null)
      // ASYNCHRONOUS MODE NO ANSWER
      pMode = 2;
    else
      pMode = mode;
    request.setMode((byte) pMode);
    return baseNetworkOperation(new OStorageRemoteOperation<T>() {
      @Override
      public T execute(final OChannelBinaryAsynchClient network, final OStorageRemoteSession session) throws IOException {
        // Send The request
        try {
          network.beginRequest(request.getCommand(), session);
          request.write(network, session);
        } finally {
          network.endRequest();
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
          OStorageRemote.this.asynchExecutor.submit(new Runnable() {
            @Override
            public void run() {
              try {
                try {
                  beginResponse(network, session);
                  response.read(network, session);
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
        }
        return ret;
      }
    }, errorMessage, connectionRetry);
  }

  public <T extends OBinaryResponse> T networkOperationRetryTimeout(final OBinaryRequest<T> request, final String errorMessage,
      int retry, int timeout) {
    return baseNetworkOperation(new OStorageRemoteOperation<T>() {
      @Override
      public T execute(OChannelBinaryAsynchClient network, OStorageRemoteSession session) throws IOException {
        try {
          network.beginRequest(request.getCommand(), session);
          request.write(network, session);
        } finally {
          network.endRequest();
        }
        int prev = network.getSocketTimeout();
        T response = request.createResponse();
        try {
          if (timeout > 0)
            network.setSocketTimeout(timeout);
          beginResponse(network, session);
          response.read(network, session);
        } finally {
          endResponse(network);
          if (timeout > 0)
            network.setSocketTimeout(prev);
        }
        connectionManager.release(network);
        return response;
      }
    }, errorMessage, retry);
  }

  public <T extends OBinaryResponse> T networkOperation(final OBinaryRequest<T> request, final String errorMessage) {
    return networkOperationRetryTimeout(request, errorMessage, connectionRetry, 0);
  }

  public <T> T baseNetworkOperation(final OStorageRemoteOperation<T> operation, final String errorMessage, int retry) {
    OStorageRemoteSession session = getCurrentSession();
    if (session.commandExecuting)
      throw new ODatabaseException(
          "Cannot execute the request because an asynchronous operation is in progress. Please use a different connection");
    
    String serverUrl = null;
    do {
      session.commandExecuting = true;
      OChannelBinaryAsynchClient network = null;
      
      if (serverUrl == null)
        serverUrl = getNextAvailableServerURL(false, session);
      
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
      } catch (ODistributedRedirectException e) {
        connectionManager.release(network);
        OLogManager.instance()
            .debug(this, "Redirecting the request from server '%s' to the server '%s' because %s", e.getFromServer(), e.toString(),
                e.getMessage());

        // RECONNECT TO THE SERVER SUGGESTED IN THE EXCEPTION
        serverUrl = e.getToServerAddress();
      } catch (OModificationOperationProhibitedException mope) {
        connectionManager.release(network);
        handleDBFreeze();
        serverUrl = null;
      } catch (OTokenException e) {
        connectionManager.release(network);
        session.removeServerSession(network.getServerURL());
        if (--retry <= 0)
          throw OException.wrapException(new OStorageException(errorMessage), e);
        serverUrl = null;
      } catch (OTokenSecurityException e) {
        connectionManager.release(network);
        session.removeServerSession(network.getServerURL());
        if (--retry <= 0)
          throw OException.wrapException(new OStorageException(errorMessage), e);
        serverUrl = null;
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
        serverUrl = null;
      } catch (IOException e) {
        connectionManager.release(network);
        retry = handleIOException(retry, network, e);
        serverUrl = null;        
      } catch (OIOException e) {
        connectionManager.release(network);
        retry = handleIOException(retry, network, e);
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

  private int handleIOException(int retry, final OChannelBinaryAsynchClient network, final Exception e) {
    OLogManager.instance()
        .info(this, "Caught Network I/O errors on %s, trying an automatic reconnection... (error: %s)", network.getServerURL(),
            e.getMessage());
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
      if (status == STATUS.CLOSED || !iUserName.equals(session.connectionUserName) || !iUserPassword
          .equals(session.connectionUserPassword) || session.sessions.isEmpty()) {

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

    OReloadRequest request = new OReloadRequest();
    OReloadResponse response = networkOperation(request, "Error on reloading database information");

    updateStorageInformations(response.getClusters());
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
              OCloseRequest request = new OCloseRequest();
              network.beginRequest(request.getCommand(), session);
              request.write(network, session);
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
        iCallback.call(response.getIdentity(), response.getIdentity().getClusterPosition());
        updateCollectionsFromChanges(collectionManager, response.getChangedIds());
      };
    }
    // The Upper layer require to return this also if it not really received response from the network
    final OPhysicalPosition ppos = new OPhysicalPosition(iRecordType);
    final OCreateRecordRequest request = new OCreateRecordRequest(iContent, iRid, iRecordType);
    final OCreateRecordResponse response = asyncNetworkOperation(request, iMode, iRid, realCallback,
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

    OGetRecordMetadataRequest request = new OGetRecordMetadataRequest(rid);
    OGetRecordMetadataResponse response = networkOperation(request, "Error on record metadata read " + rid);

    return response.getMetadata();
  }

  @Override
  public OStorageOperationResult<ORawBuffer> readRecordIfVersionIsNotLatest(final ORecordId rid, final String fetchPlan,
      final boolean ignoreCache, final int recordVersion) throws ORecordNotFoundException {
    if (getCurrentSession().commandExecuting)
      // PENDING NETWORK OPERATION, CAN'T EXECUTE IT NOW
      return new OStorageOperationResult<ORawBuffer>(null);

    OReadRecordIfVersionIsNotLatestRequest request = new OReadRecordIfVersionIsNotLatestRequest(rid, recordVersion, fetchPlan,
        ignoreCache);
    OReadRecordIfVersionIsNotLatestResponse response = networkOperation(request, "Error on read record " + rid);

    return new OStorageOperationResult<ORawBuffer>(response.getResult());
  }

  public OStorageOperationResult<ORawBuffer> readRecord(final ORecordId iRid, final String iFetchPlan, final boolean iIgnoreCache,
      boolean prefetchRecords, final ORecordCallback<ORawBuffer> iCallback) {

    if (getCurrentSession().commandExecuting)
      // PENDING NETWORK OPERATION, CAN'T EXECUTE IT NOW
      return new OStorageOperationResult<ORawBuffer>(null);

    OReadRecordRequest request = new OReadRecordRequest(iIgnoreCache, iRid, iFetchPlan, false);
    OReadRecordResponse response = networkOperation(request, "Error on read record " + iRid);

    return new OStorageOperationResult<ORawBuffer>(response.getResult());
  }

  @Override
  public String incrementalBackup(final String backupDirectory) {
    OIncrementalBackupRequest request = new OIncrementalBackupRequest(backupDirectory);
    OIncrementalBackupResponse response = networkOperation(request, "Error on incremental backup");
    return response.getFileName();
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
    OUpdateRecordResponse response = asyncNetworkOperation(request, iMode, iRid, realCallback, "Error on update record " + iRid);

    Integer resVersion = null;
    if (response != null) {
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
    final ODeleteRecordResponse response = asyncNetworkOperation(request, iMode, iRid, realCallback,
        "Error on delete record " + iRid);
    Boolean resDelete = null;
    if (response != null)
      resDelete = response.getResult();
    return new OStorageOperationResult<Boolean>(resDelete);
  }

  @Override
  public OStorageOperationResult<Boolean> hideRecord(final ORecordId recordId, final int mode,
      final ORecordCallback<Boolean> callback) {

    ORecordCallback<OHideRecordResponse> realCallback = null;
    if (callback != null)
      realCallback = (iRID, response) -> callback.call(iRID, response.getResult());

    final OHideRecordRequest request = new OHideRecordRequest(recordId);
    final OHideRecordResponse response = asyncNetworkOperation(request, mode, recordId, realCallback,
        "Error on hide record " + recordId);
    Boolean resHide = null;
    if (response != null)
      resHide = response.getResult();
    return new OStorageOperationResult<Boolean>(resHide);
  }

  @Override
  public boolean cleanOutRecord(final ORecordId recordId, final int recordVersion, final int iMode,
      final ORecordCallback<Boolean> callback) {

    ORecordCallback<OCleanOutRecordResponse> realCallback = null;
    if (callback != null)
      realCallback = (iRID, response) -> callback.call(iRID, response.getResult());

    final OCleanOutRecordRequest request = new OCleanOutRecordRequest(recordVersion, recordId);
    final OCleanOutRecordResponse response = asyncNetworkOperation(request, iMode, recordId, realCallback,
        "Error on delete record " + recordId);
    Boolean result = null;
    if (response != null)
      result = response.getResult();
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
    OGetClusterDataRangeRequest request = new OGetClusterDataRangeRequest(iClusterId);
    OGetClusterDataRangeResponse response = networkOperation(request,
        "Error on getting last entry position count in cluster: " + iClusterId);
    return response.getPos();
  }

  @Override
  public OPhysicalPosition[] higherPhysicalPositions(final int iClusterId, final OPhysicalPosition iClusterPosition) {
    OHigherPhysicalPositionsRequest request = new OHigherPhysicalPositionsRequest(iClusterId, iClusterPosition);

    OHigherPhysicalPositionsResponse response = networkOperation(request,
        "Error on retrieving higher positions after " + iClusterPosition.clusterPosition);
    return response.getNextPositions();
  }

  @Override
  public OPhysicalPosition[] ceilingPhysicalPositions(final int clusterId, final OPhysicalPosition physicalPosition) {

    OCeilingPhysicalPositionsRequest request = new OCeilingPhysicalPositionsRequest(clusterId, physicalPosition);

    OCeilingPhysicalPositionsResponse response = networkOperation(request,
        "Error on retrieving ceiling positions after " + physicalPosition.clusterPosition);
    return response.getPositions();
  }

  @Override
  public OPhysicalPosition[] lowerPhysicalPositions(final int iClusterId, final OPhysicalPosition physicalPosition) {
    OLowerPhysicalPositionsRequest request = new OLowerPhysicalPositionsRequest(physicalPosition, iClusterId);
    OLowerPhysicalPositionsResponse response = networkOperation(request,
        "Error on retrieving lower positions after " + physicalPosition.clusterPosition);
    return response.getPreviousPositions();
  }

  @Override
  public OPhysicalPosition[] floorPhysicalPositions(final int clusterId, final OPhysicalPosition physicalPosition) {
    OFloorPhysicalPositionsRequest request = new OFloorPhysicalPositionsRequest(physicalPosition, clusterId);
    OFloorPhysicalPositionsResponse response = networkOperation(request,
        "Error on retrieving floor positions after " + physicalPosition.clusterPosition);
    return response.getPositions();
  }

  public long getSize() {
    OGetSizeRequest request = new OGetSizeRequest();
    OGetSizeResponse response = networkOperation(request, "Error on read database size");
    return response.getSize();
  }

  @Override
  public long countRecords() {
    OCountRecordsRequest request = new OCountRecordsRequest();
    OCountRecordsResponse response = networkOperation(request, "Error on read database record count");
    return response.getCountRecords();
  }

  public long count(final int[] iClusterIds) {
    return count(iClusterIds, false);
  }

  public long count(final int[] iClusterIds, final boolean countTombstones) {
    OCountRequest request = new OCountRequest(iClusterIds, countTombstones);
    OCountResponse response = networkOperation(request, "Error on read record count in clusters: " + Arrays.toString(iClusterIds));
    return response.getCount();
  }

  /**
   * Execute the command remotely and get the results back.
   */
  public Object command(final OCommandRequestText iCommand) {

    final boolean live = iCommand instanceof OLiveQuery;
    final ODatabaseDocumentInternal database = ODatabaseRecordThreadLocal.INSTANCE.get();
    final boolean asynch = iCommand instanceof OCommandRequestAsynch && ((OCommandRequestAsynch) iCommand).isAsynchronous();

    OCommandRequest request = new OCommandRequest(database, asynch, iCommand, live);
    OCommandResponse response = networkOperation(request, "Error on executing command: " + iCommand);
    return response.getResult();

  }

  public ORemoteQueryResult query(ODatabase db, String query, Object[] args) {
    OQueryRequest request = new OQueryRequest("sql", query, args, true, ((ODatabaseDocumentInternal) db).getSerializer(), 100);
    OQueryResponse response = networkOperation(request, "Error on executing command: " + query);
    return new ORemoteQueryResult(response.getResult(), response.isTxChanges());
  }

  public ORemoteQueryResult query(ODatabase db, String query, Map args) {
    OQueryRequest request = new OQueryRequest("sql", query, args, true, ((ODatabaseDocumentInternal) db).getSerializer(), 100);
    OQueryResponse response = networkOperation(request, "Error on executing command: " + query);
    return new ORemoteQueryResult(response.getResult(), response.isTxChanges());
  }

  public ORemoteQueryResult command(ODatabase db, String language, String query, Object[] args) {
    OQueryRequest request = new OQueryRequest(language, query, args, false, ((ODatabaseDocumentInternal) db).getSerializer(), 100);
    OQueryResponse response = networkOperation(request, "Error on executing command: " + query);
    return new ORemoteQueryResult(response.getResult(), response.isTxChanges());
  }

  public ORemoteQueryResult command(ODatabase db, String language, String query, Map args) {
    OQueryRequest request = new OQueryRequest(language, query, args, false, ((ODatabaseDocumentInternal) db).getSerializer(), 100);
    OQueryResponse response = networkOperation(request, "Error on executing command: " + query);
    return new ORemoteQueryResult(response.getResult(), response.isTxChanges());
  }

  public void closeQuery(ODatabaseDocumentRemote database, String queryId) {
    OCloseQueryRequest request = new OCloseQueryRequest(queryId);
    OCloseQueryResponse response = networkOperation(request, "Error closing query: " + queryId);
  }

  public void fetchNextPage(ODatabaseDocumentRemote database, ORemoteResultSet rs) {
    OQueryNextPageRequest request = new OQueryNextPageRequest(rs.getQueryId(), 100);
    OQueryResponse response = networkOperation(request, "Error on fetching next page for statment: " + rs.getQueryId());

    ORemoteResultSet remoteRs = ((ORemoteResultSet) response.getResult());
    rs.setCurrentPage(remoteRs.getCurrentPage());
    rs.setHasNextPage(remoteRs.hasNextPage());
    Map<String, Long> newQueryStats = remoteRs.getQueryStats();
    if (newQueryStats != null) {
      rs.setQueryStats(newQueryStats);
    }
    remoteRs.getExecutionPlan().ifPresent(x -> rs.setExecutionPlan(x));
  }

  public List<ORecordOperation> commit(final OTransaction iTx, final Runnable callback) {
    OCommit37Request request;
    if (((OTransactionOptimistic) iTx).isChanged()) {
      request = new OCommit37Request(iTx.getId(), true, iTx.isUsingLog(), (Iterable<ORecordOperation>) iTx.getAllRecordEntries(),
          ((OTransactionOptimistic) iTx).getIndexEntries());
    } else {
      request = new OCommit37Request(iTx.getId(), false, iTx.isUsingLog(), null, null);
    }

    OCommitResponse response = networkOperation(request, "Error on commit");
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

    // UPDATE THE CACHE ONLY IF THE ITERATOR ALLOWS IT. 
    OTransactionAbstract.updateCacheFromEntries(iTx, iTx.getAllRecordEntries(), true);
    return null;
  }

  public void rollback(OTransaction iTx) {
    if (((OTransactionOptimistic) iTx).isAlreadyCleared()) {
      ORollbackTransactionRequest request = new ORollbackTransactionRequest(iTx.getId());
      ORollbackTransactionResponse response = networkOperation(request, "Error on fetching next page for statment: " + request);
    }
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
    OAddClusterRequest request = new OAddClusterRequest(iRequestedId, iClusterName);
    OAddClusterResponse response = networkOperation(request, "Error on add new cluster");
    addNewClusterToConfiguration(response.getClusterId(), iClusterName);
    return response.getClusterId();
  }

  public boolean dropCluster(final int iClusterId, final boolean iTruncate) {

    ODropClusterRequest request = new ODropClusterRequest(iClusterId);

    ODropClusterResponse response = networkOperation(request, "Error on removing of cluster");
    if (response.getResult())
      removeClusterFromConfiguration(iClusterId);
    return response.getResult();
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
                  .debug(this, "Client connected to %s with session id=%d", network.getServerURL(), response.getSessionId());
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
      OOpenRequest request = new OOpenRequest(name, session.connectionUserName, session.connectionUserPassword);
      try {
        network.writeByte(request.getCommand());
        network.writeInt(nodeSession.getSessionId());
        request.write(network, session);
      } finally {
        endRequest(network);
      }
      final int sessionId;
      OOpenResponse response = request.createResponse();
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
      if (serverURLs.size() == 1 && getClientConfiguration()
          .getValueAsBoolean(OGlobalConfiguration.NETWORK_BINARY_DNS_LOADBALANCING_ENABLED)) {
        // LOOK FOR LOAD BALANCING DNS TXT RECORD
        final String primaryServer = lastHost;

        OLogManager.instance().debug(this, "Retrieving URLs from DNS '%s' (timeout=%d)...", primaryServer,
            getClientConfiguration().getValueAsInteger(OGlobalConfiguration.NETWORK_BINARY_DNS_LOADBALANCING_TIMEOUT));

        try {
          final Hashtable<String, String> env = new Hashtable<String, String>();
          env.put("java.naming.factory.initial", "com.sun.jndi.dns.DnsContextFactory");
          env.put("com.sun.jndi.ldap.connect.timeout",
              getClientConfiguration().getValueAsString(OGlobalConfiguration.NETWORK_BINARY_DNS_LOADBALANCING_TIMEOUT));
          final DirContext ictx = new InitialDirContext(env);
          final String hostName = !primaryServer.contains(":") ?
              primaryServer :
              primaryServer.substring(0, primaryServer.indexOf(":"));
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
      host += ":" + (clientConfiguration.getValueAsBoolean(OGlobalConfiguration.CLIENT_USE_SSL) ?
          getDefaultSSLPort() :
          getDefaultPort());
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
   *
   * @return connection to server
   *
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
      OLogManager.instance()
          .debug(this, "ROUND_ROBIN_CONNECT: Next remote operation will be executed on server: %s (isConnectOperation=%s)", url,
              iIsConnectOperation);
      break;

    case ROUND_ROBIN_REQUEST:
      url = getServerURFromList(true, session);
      OLogManager.instance()
          .debug(this, "ROUND_ROBIN_REQUEST: Next remote operation will be executed on server: %s (isConnectOperation=%s)", url,
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
        OLogManager.instance()
            .error(this, "Removing locked network channel '%s' (connected=%s)...", iCurrentURL, network.isConnected());
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

  private boolean handleDBFreeze() {

    boolean retry;
    OLogManager.instance().warn(this,
        "DB is frozen will wait for " + getClientConfiguration().getValue(OGlobalConfiguration.CLIENT_DB_RELEASE_WAIT_TIMEOUT)
            + " ms. and then retry.");
    retry = true;
    try {
      Thread.sleep(getClientConfiguration().getValueAsInteger(OGlobalConfiguration.CLIENT_DB_RELEASE_WAIT_TIMEOUT));
    } catch (InterruptedException ie) {
      retry = false;

      Thread.currentThread().interrupt();
    }
    return retry;
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
    OImportRequest request = new OImportRequest(inputStream, options, name);

    OImportResponse response = networkOperationRetryTimeout(request, "Error sending import request", 0,
        getClientConfiguration().getValueAsInteger(OGlobalConfiguration.NETWORK_REQUEST_TIMEOUT));

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

  public void beginTransaction(ODatabaseDocumentRemote database, OTransactionOptimistic transaction) {
    OBeginTransactionRequest request = new OBeginTransactionRequest(transaction.getId(), transaction.isUsingLog(),
        transaction.getAllRecordEntries(), transaction.getIndexEntries());
    OBinaryResponse response = networkOperation(request, "Error on remote treansaction begin");

  }

  public void reBeginTransaction(ODatabaseDocumentRemote database, OTransactionOptimistic transaction) {
    ORebeginTransactionRequest request = new ORebeginTransactionRequest(transaction.getId(), transaction.isUsingLog(),
        transaction.getAllRecordEntries(), transaction.getIndexEntries());
    OBinaryResponse response = networkOperation(request, "Error on remote treansaction begin");

  }

  public void fetchTransaction(ODatabaseDocumentRemote remote) {
    OTransactionOptimisticClient transaction = (OTransactionOptimisticClient) remote.getTransaction();
    OFetchTransactionRequest request = new OFetchTransactionRequest(transaction.getId());
    OFetchTransactionResponse respose = networkOperation(request, "Error fetching transaction from server side");
    transaction.replaceContent(respose.getOperations(), respose.getIndexChanges());
  }

}
