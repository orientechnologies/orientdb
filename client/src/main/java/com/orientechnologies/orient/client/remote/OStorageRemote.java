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
package com.orientechnologies.orient.client.remote;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;
import java.util.concurrent.*;
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
import com.orientechnologies.orient.core.OConstants;
import com.orientechnologies.orient.core.OUncompletedCommit;
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
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTxInternal;
import com.orientechnologies.orient.core.db.record.OCurrentStorageComponentsFactory;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.db.record.ridbag.sbtree.OBonsaiCollectionPointer;
import com.orientechnologies.orient.core.db.record.ridbag.sbtree.OSBTreeCollectionManager;
import com.orientechnologies.orient.core.exception.*;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.security.OTokenException;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.security.OCredentialInterceptor;
import com.orientechnologies.orient.core.security.OSecurityManager;
import com.orientechnologies.orient.core.serialization.OSerializableStream;
import com.orientechnologies.orient.core.serialization.serializer.record.string.ORecordSerializerSchemaAware2CSV;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializerAnyStreamable;
import com.orientechnologies.orient.core.sql.query.OLiveQuery;
import com.orientechnologies.orient.core.sql.query.OLiveResultListener;
import com.orientechnologies.orient.core.storage.*;
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
  private OContextConfiguration                clientConfiguration;
  private int                                  connectionRetry;
  private int                                  connectionRetryDelay;
  private OCluster[]                           clusters                = OCommonConst.EMPTY_CLUSTER_ARRAY;
  private int                                  defaultClusterId;
  private OStorageRemoteAsynchEventListener    asynchEventListener;
  private Map<String, Object>                  connectionOptions;
  private OEngineRemote                        engine;
  private String                               recordFormat;

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

    engine = (OEngineRemote) Orient.instance().getRunningEngine(OEngineRemote.NAME);
  }

  public <T> T networkOperation(final OStorageRemoteOperation<T> operation, final String errorMessage) {
    int retry = connectionRetry;
    OStorageRemoteSession session = getCurrentSession();
    do {
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
        OStorageRemoteNodeSession nodeSession = session.get(network.getServerURL());
        if (nodeSession == null || !nodeSession.isValid()) {
          openRemoteDatabase(network);
          if (!network.tryLock())
            continue;
        }

        return operation.execute(network);
      } catch (OModificationOperationProhibitedException mope) {
        handleDBFreeze();
      } catch (OTokenException e) {
        session.remove(network.getServerURL());
        if (--retry <= 0)
          throw OException.wrapException(new OStorageException(errorMessage), e);
      } catch (OTokenSecurityException e) {
        session.remove(network.getServerURL());
        if (--retry <= 0)
          throw OException.wrapException(new OStorageException(errorMessage), e);
      } catch (OOfflineNodeException e) {
        // Remove the current url because the node is offline
        synchronized (serverURLs) {
          serverURLs.remove(serverUrl);
        }
      } catch (IOException e) {
        retry = handleIOException(retry, network, e);
      } catch (OIOException e) {
        retry = handleIOException(retry, network, e);
      } catch (OException e) {
        throw e;
      } catch (Exception e) {
        throw OException.wrapException(new OStorageException(errorMessage), e);
      }
    } while (true);

  }

  private int handleIOException(int retry, OChannelBinaryAsynchClient network, Exception e) {
    OLogManager.instance().warn(this, "Caught I/O errors, trying to reconnect (error: %s)", e.getMessage());
    OLogManager.instance().debug(this, "I/O error stack: ", e);
    engine.getConnectionManager().remove(network);
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

  public void open(final String iUserName, final String iUserPassword, final Map<String, Object> iOptions) {

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

        parseOptions(iOptions);

        openRemoteDatabase();

        final OStorageConfiguration storageConfiguration = new OStorageRemoteConfiguration(this, recordFormat);
        storageConfiguration.load(iOptions);

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

  private void parseOptions(final Map<String, Object> iOptions) {
    if (iOptions == null || iOptions.size() == 0)
      return;

    final Object connType = iOptions.get(PARAM_CONNECTION_STRATEGY.toLowerCase());
    if (connType != null)
      connectionStrategy = CONNECTION_STRATEGY.valueOf(connType.toString().toUpperCase());

    // CREATE A COPY TO AVOID POST OPEN MANIPULATION BY USER
    connectionOptions = new HashMap<String, Object>(iOptions);
  }

  @Override
  public OSBTreeCollectionManager getSBtreeCollectionManager() {
    return sbTreeCollectionManager;
  }

  public void reload() {
    networkOperation(new OStorageRemoteOperation<Void>() {
      @Override
      public Void execute(OChannelBinaryAsynchClient network) throws IOException {
        stateLock.acquireWriteLock();
        try {
          try {
            beginRequest(network, OChannelBinaryProtocol.REQUEST_DB_RELOAD);
          } finally {
            endRequest(network);
          }

          try {
            beginResponse(network);
            readDatabaseInformation(network);
          } finally {
            endResponse(network);
          }
          return null;
        } finally {
          stateLock.releaseWriteLock();
        }

      }
    }, "Error on reloading database information");
  }

  public void create(final Map<String, Object> iOptions) {
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

    OChannelBinaryAsynchClient network = null;

    stateLock.acquireWriteLock();
    try {
      if (status == STATUS.CLOSED)
        return;

      final OStorageRemoteSession session = getCurrentSession();
      if (session != null) {
        final Collection<OStorageRemoteNodeSession> nodes = session.getAll();
        if (!nodes.isEmpty()) {
          for (OStorageRemoteNodeSession nodeSession : nodes) {
            try {
              network = getNetwork(nodeSession.getServerURL());
              network.beginRequest(OChannelBinaryProtocol.REQUEST_DB_CLOSE, session);
              endRequest(network);
              engine.getConnectionManager().release(network);
            } catch (OIOException ex) {
              // IGNORING IF THE SERVER IS DOWN OR NOT REACHABLE THE SESSION IS AUTOMATICALLY CLOSED.
              OLogManager.instance().debug(this, "Impossible to comunicate to the server for close: %s", ex);
            } catch (IOException ex) {
              // IGNORING IF THE SERVER IS DOWN OR NOT REACHABLE THE SESSION IS AUTOMATICALLY CLOSED.
              OLogManager.instance().debug(this, "Impossible to comunicate to the server for close: %s", ex);
            }
          }
          session.close();
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
        engine.getConnectionManager().closePool(url);
      }
      sbTreeCollectionManager.close();

      super.close(iForce, onDelete);
      status = STATUS.CLOSED;

      Orient.instance().unregisterStorage(this);
    } catch (Exception e) {
      if (network != null) {
        OLogManager.instance().debug(this, "Error on closing remote connection: %s", e, network);
        try {
          network.close();
        } catch (Exception e2) {
          OLogManager.instance().debug(this, "Error on closing socket: %s", e2, network);
        }
      }
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
    return dataLock.getUsers();
  }

  @Override
  public int addUser() {
    return dataLock.addUser();
  }

  @Override
  public int removeUser() {
    return dataLock.removeUser();
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
    return networkOperation(new OStorageRemoteOperation<OStorageOperationResult<OPhysicalPosition>>() {
      @Override
      public OStorageOperationResult<OPhysicalPosition> execute(final OChannelBinaryAsynchClient network) throws IOException {

        int mode;
        if (iMode == 1 && iCallback == null)
          mode = 2;
        else
          mode = iMode;
        // ASYNCHRONOUS MODE NO ANSWER

        final OPhysicalPosition ppos = new OPhysicalPosition(iRecordType);

        try {
          beginRequest(network, OChannelBinaryProtocol.REQUEST_RECORD_CREATE);
          network.writeShort((short) iRid.clusterId);
          network.writeBytes(iContent);
          network.writeByte(iRecordType);
          network.writeByte((byte) mode);

        } finally {
          endRequest(network);
        }

        switch (mode) {
        case 0:
          // SYNCHRONOUS
          try {
            beginResponse(network);
            if (network.getSrvProtocolVersion() > OChannelBinaryProtocol.PROTOCOL_VERSION_25)
              iRid.clusterId = network.readShort();

            iRid.clusterPosition = network.readLong();
            ppos.clusterPosition = iRid.clusterPosition;
            if (network.getSrvProtocolVersion() >= 11) {
              ppos.recordVersion = network.readVersion();
            } else
              ppos.recordVersion = 0;

            if (network.getSrvProtocolVersion() >= 20)
              readCollectionChanges(network, ODatabaseRecordThreadLocal.INSTANCE.get().getSbTreeCollectionManager());

            return new OStorageOperationResult<OPhysicalPosition>(ppos);
          } finally {
            endResponse(network);
          }

        case 1:
          // ASYNCHRONOUS
          if (iCallback != null) {
            final OStorageRemoteSession session = getCurrentSession();
            final OSBTreeCollectionManager collectionManager = ODatabaseRecordThreadLocal.INSTANCE.get()
                .getSbTreeCollectionManager();
            Callable<Object> response = new Callable<Object>() {
              public Object call() throws Exception {
                final long result;

                try {
                  beginResponse(network, session);
                  if (network.getSrvProtocolVersion() > OChannelBinaryProtocol.PROTOCOL_VERSION_25)
                    iRid.clusterId = network.readShort();
                  result = network.readLong();
                  if (network.getSrvProtocolVersion() >= 11)
                    network.readVersion();

                  if (network.getSrvProtocolVersion() >= 20)
                    readCollectionChanges(network, collectionManager);
                } catch (Exception e) {
                  OLogManager.instance().error(this, "Exception on async query", e);
                  throw e;
                } finally {
                  endResponse(network);
                }
                iCallback.call(iRid, result);
                return null;
              }

            };
            asynchExecutor.submit(new FutureTask<Object>(response));
          }
          break;

        case 2:
          // FREE THE CHANNEL WITHOUT WAITING ANY RESPONSE
          engine.getConnectionManager().release(network);
          break;
        }

        return new OStorageOperationResult<OPhysicalPosition>(ppos);
      }
    }, "Error on create record in cluster " + iRid.clusterId);

  }

  @Override
  public ORecordMetadata getRecordMetadata(final ORID rid) {

    return networkOperation(new OStorageRemoteOperation<ORecordMetadata>() {
      @Override
      public ORecordMetadata execute(OChannelBinaryAsynchClient network) throws IOException {
        try {
          beginRequest(network, OChannelBinaryProtocol.REQUEST_RECORD_METADATA);
          network.writeRID(rid);
        } finally {
          endRequest(network);
        }
        try {
          beginResponse(network);
          final ORID responseRid = network.readRID();
          final int responseVersion = network.readVersion();

          return new ORecordMetadata(responseRid, responseVersion);
        } finally {
          endResponse(network);
        }
      }
    }, "Error on record metadata read " + rid);
  }

  @Override
  public OStorageOperationResult<ORawBuffer> readRecordIfVersionIsNotLatest(final ORecordId rid, final String fetchPlan,
      final boolean ignoreCache, final int recordVersion) throws ORecordNotFoundException {
    if (getCurrentSession().commandExecuting)
      // PENDING NETWORK OPERATION, CAN'T EXECUTE IT NOW
      return new OStorageOperationResult<ORawBuffer>(null);

    return networkOperation(new OStorageRemoteOperation<OStorageOperationResult<ORawBuffer>>() {
      @Override
      public OStorageOperationResult<ORawBuffer> execute(OChannelBinaryAsynchClient network) throws IOException {
        try {
          beginRequest(network, OChannelBinaryProtocol.REQUEST_RECORD_LOAD_IF_VERSION_NOT_LATEST);
          network.writeRID(rid);
          network.writeVersion(recordVersion);
          network.writeString(fetchPlan != null ? fetchPlan : "");
          network.writeByte((byte) (ignoreCache ? 1 : 0));
        } finally {
          endRequest(network);
        }

        try {
          beginResponse(network);

          if (network.readByte() == 0)
            return new OStorageOperationResult<ORawBuffer>(null);

          byte type = network.readByte();
          int recVersion = network.readVersion();
          byte[] bytes = network.readBytes();
          ORawBuffer buffer = new ORawBuffer(bytes, recVersion, type);

          final ODatabaseDocument database = ODatabaseRecordThreadLocal.INSTANCE.getIfDefined();
          ORecord record;

          while (network.readByte() == 2) {
            record = (ORecord) OChannelBinaryProtocol.readIdentifiable(network);

            if (database != null)
              // PUT IN THE CLIENT LOCAL CACHE
              database.getLocalCache().updateRecord(record);
          }
          return new OStorageOperationResult<ORawBuffer>(buffer);

        } finally {
          endResponse(network);
        }
      }
    }, "Error on read record " + rid);
  }

  public OStorageOperationResult<ORawBuffer> readRecord(final ORecordId iRid, final String iFetchPlan, final boolean iIgnoreCache,
      final ORecordCallback<ORawBuffer> iCallback) {

    if (getCurrentSession().commandExecuting)
      // PENDING NETWORK OPERATION, CAN'T EXECUTE IT NOW
      return new OStorageOperationResult<ORawBuffer>(null);

    return networkOperation(new OStorageRemoteOperation<OStorageOperationResult<ORawBuffer>>() {
      @Override
      public OStorageOperationResult<ORawBuffer> execute(OChannelBinaryAsynchClient network) throws IOException {
        try {
          beginRequest(network, OChannelBinaryProtocol.REQUEST_RECORD_LOAD);
          network.writeRID(iRid);
          network.writeString(iFetchPlan != null ? iFetchPlan : "");
          if (network.getSrvProtocolVersion() >= 9)
            network.writeByte((byte) (iIgnoreCache ? 1 : 0));

          if (network.getSrvProtocolVersion() >= 13)
            network.writeByte((byte) 0);
        } finally {
          endRequest(network);
        }

        try {
          beginResponse(network);

          if (network.readByte() == 0)
            return new OStorageOperationResult<ORawBuffer>(null);

          final ORawBuffer buffer;
          if (network.getSrvProtocolVersion() <= 27)
            buffer = new ORawBuffer(network.readBytes(), network.readVersion(), network.readByte());
          else {
            final byte type = network.readByte();
            final int recVersion = network.readVersion();
            final byte[] bytes = network.readBytes();
            buffer = new ORawBuffer(bytes, recVersion, type);
          }

          final ODatabaseDocument database = ODatabaseRecordThreadLocal.INSTANCE.getIfDefined();
          ORecord record;
          while (network.readByte() == 2) {
            record = (ORecord) OChannelBinaryProtocol.readIdentifiable(network);

            if (database != null)
              // PUT IN THE CLIENT LOCAL CACHE
              database.getLocalCache().updateRecord(record);
          }
          return new OStorageOperationResult<ORawBuffer>(buffer);

        } finally {
          endResponse(network);
        }

      }
    }, "Error on read record " + iRid);
  }

  @Override
  public String incrementalBackup(final String backupDirectory) {
    return networkOperation(new OStorageRemoteOperation<String>() {
      @Override
      public String execute(OChannelBinaryAsynchClient network) throws IOException {
        try {
          network = beginRequest(network, OChannelBinaryProtocol.REQUEST_INCREMENTAL_BACKUP);
          network.writeString(backupDirectory);
        } finally {
          endRequest(network);
        }

        try {
          beginResponse(network);
          String fileName = network.readString();
          return fileName;
        } finally {
          endResponse(network);
        }

      }
    }, "Error on incremental backup");
  }

  @Override
  public void restoreFromIncrementalBackup(final String filePath) {
    throw new UnsupportedOperationException("This operations is part of internal API and is not supported in remote storage");
  }

  public OStorageOperationResult<Integer> updateRecord(final ORecordId iRid, final boolean updateContent, final byte[] iContent,
      final int iVersion, final byte iRecordType, final int iMode, final ORecordCallback<Integer> iCallback) {
    return networkOperation(new OStorageRemoteOperation<OStorageOperationResult<Integer>>() {
      @Override
      public OStorageOperationResult<Integer> execute(final OChannelBinaryAsynchClient network) throws IOException {
        int mode;
        if (iMode == 1 && iCallback == null)
          // ASYNCHRONOUS MODE NO ANSWER
          mode = 2;
        else
          mode = iMode;
        try {
          beginRequest(network, OChannelBinaryProtocol.REQUEST_RECORD_UPDATE);
          network.writeRID(iRid);
          if (network.getSrvProtocolVersion() >= 23) {
            network.writeBoolean(updateContent);
          }
          network.writeBytes(iContent);
          network.writeVersion(iVersion);
          network.writeByte(iRecordType);
          network.writeByte((byte) mode);

        } finally {
          endRequest(network);
        }

        switch (mode) {
        case 0:
          // SYNCHRONOUS
          try {
            beginResponse(network);
            OStorageOperationResult<Integer> r = new OStorageOperationResult<Integer>(network.readVersion());
            readCollectionChanges(network, ODatabaseRecordThreadLocal.INSTANCE.get().getSbTreeCollectionManager());
            return r;
          } finally {
            endResponse(network);
          }

        case 1:
          // ASYNCHRONOUS
          final OStorageRemoteSession session = getCurrentSession();
          final OSBTreeCollectionManager collectionManager = ODatabaseRecordThreadLocal.INSTANCE.get().getSbTreeCollectionManager();
          Callable<Object> response = new Callable<Object>() {
            public Object call() throws Exception {
              int result;

              try {
                beginResponse(network, session);
                result = network.readVersion();

                if (network.getSrvProtocolVersion() >= 20)
                  readCollectionChanges(network, collectionManager);
              } finally {
                endResponse(network);
              }

              iCallback.call(iRid, result);
              return null;
            }

          };
          asynchExecutor.submit(new FutureTask<Object>(response));
        }
        return new OStorageOperationResult<Integer>(iVersion);
      }
    }, "Error on update record " + iRid);
  }

  @Override
  public OStorageOperationResult<Integer> recyclePosition(ORecordId iRecordId, byte[] iContent, int iVersion, byte recordType) {
    throw new UnsupportedOperationException("recyclePosition");
  }

  public OStorageOperationResult<Boolean> deleteRecord(final ORecordId iRid, final int iVersion, final int iMode,
      final ORecordCallback<Boolean> iCallback) {
    return networkOperation(new OStorageRemoteOperation<OStorageOperationResult<Boolean>>() {
      @Override
      public OStorageOperationResult<Boolean> execute(final OChannelBinaryAsynchClient network) throws IOException {
        int mode;
        if (iMode == 1 && iCallback == null)
          // ASYNCHRONOUS MODE NO ANSWER
          mode = 2;
        else
          mode = iMode;
        return new OStorageOperationResult<Boolean>(
            deleteRecord(OChannelBinaryProtocol.REQUEST_RECORD_DELETE, iRid, iVersion, mode, iCallback, network));

      }
    }, "Error on delete record " + iRid);
  }

  @Override
  public OStorageOperationResult<Boolean> hideRecord(final ORecordId recordId, final int mode,
      final ORecordCallback<Boolean> callback) {
    return networkOperation(new OStorageRemoteOperation<OStorageOperationResult<Boolean>>() {
      @Override
      public OStorageOperationResult<Boolean> execute(final OChannelBinaryAsynchClient network) throws IOException {
        int pMode;
        if (mode == 1 && callback == null)
          // ASYNCHRONOUS MODE NO ANSWER
          pMode = 2;
        else
          pMode = mode;

        try {
          beginRequest(network, OChannelBinaryProtocol.REQUEST_RECORD_HIDE);
          network.writeRID(recordId);
          network.writeByte((byte) pMode);

        } finally {
          endRequest(network);
        }

        switch (pMode) {
        case 0:
          // SYNCHRONOUS
          try {
            beginResponse(network);
            return new OStorageOperationResult<Boolean>(network.readByte() == 1);
          } finally {
            endResponse(network);
          }

        case 1:
          // ASYNCHRONOUS
          if (callback != null) {
            final OStorageRemoteSession session = getCurrentSession();
            Callable<Object> response = new Callable<Object>() {
              public Object call() throws Exception {
                Boolean result;

                try {
                  beginResponse(network, session);
                  result = network.readByte() == 1;
                } finally {
                  endResponse(network);
                }

                callback.call(recordId, result);
                return null;
              }
            };
            asynchExecutor.submit(new FutureTask<Object>(response));
          }
        }
        return new OStorageOperationResult<Boolean>(false);
      }
    }, "Error on hide record " + recordId);
  }

  @Override
  public boolean cleanOutRecord(final ORecordId recordId, final int recordVersion, final int iMode,
      final ORecordCallback<Boolean> callback) {

    return networkOperation(new OStorageRemoteOperation<Boolean>() {
      @Override
      public Boolean execute(final OChannelBinaryAsynchClient network) throws IOException {
        int mode;
        if (iMode == 1 && callback == null)
          // ASYNCHRONOUS MODE NO ANSWER
          mode = 2;
        else
          mode = iMode;
        return deleteRecord(OChannelBinaryProtocol.REQUEST_RECORD_CLEAN_OUT, recordId, recordVersion, mode, callback, network);
      }
    }, "Error on clean out record " + recordId);
  }

  @Override
  public List<String> backup(OutputStream out, Map<String, Object> options, Callable<Object> callable,
      final OCommandOutputListener iListener, int compressionLevel, int bufferSize) throws IOException {
    throw new UnsupportedOperationException(
        "backup is not supported against remote storage. Open the database with plocal or use Enterprise Edition");
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

    return networkOperation(new OStorageRemoteOperation<long[]>() {
      @Override
      public long[] execute(final OChannelBinaryAsynchClient network) throws IOException {
        try {
          beginRequest(network, OChannelBinaryProtocol.REQUEST_DATACLUSTER_DATARANGE);

          network.writeShort((short) iClusterId);

        } finally {
          endRequest(network);
        }

        try {
          beginResponse(network);
          return new long[] { network.readLong(), network.readLong() };
        } finally {
          endResponse(network);
        }

      }
    }, "Error on getting last entry position count in cluster: " + iClusterId);
  }

  @Override
  public OPhysicalPosition[] higherPhysicalPositions(final int iClusterId, final OPhysicalPosition iClusterPosition) {

    return networkOperation(new OStorageRemoteOperation<OPhysicalPosition[]>() {
      @Override
      public OPhysicalPosition[] execute(final OChannelBinaryAsynchClient network) throws IOException {
        try {
          beginRequest(network, OChannelBinaryProtocol.REQUEST_POSITIONS_HIGHER);
          network.writeInt(iClusterId);
          network.writeLong(iClusterPosition.clusterPosition);

        } finally {
          endRequest(network);
        }

        try {
          beginResponse(network);
          final int positionsCount = network.readInt();

          if (positionsCount == 0) {
            return OCommonConst.EMPTY_PHYSICAL_POSITIONS_ARRAY;
          } else {
            return readPhysicalPositions(network, positionsCount);
          }

        } finally {
          endResponse(network);
        }

      }
    }, "Error on retrieving higher positions after " + iClusterPosition.clusterPosition);
  }

  @Override
  public OPhysicalPosition[] ceilingPhysicalPositions(final int clusterId, final OPhysicalPosition physicalPosition) {

    return networkOperation(new OStorageRemoteOperation<OPhysicalPosition[]>() {
      @Override
      public OPhysicalPosition[] execute(final OChannelBinaryAsynchClient network) throws IOException {

        try {
          beginRequest(network, OChannelBinaryProtocol.REQUEST_POSITIONS_CEILING);
          network.writeInt(clusterId);
          network.writeLong(physicalPosition.clusterPosition);

        } finally {
          endRequest(network);
        }

        try {
          beginResponse(network);
          final int positionsCount = network.readInt();

          if (positionsCount == 0) {
            return OCommonConst.EMPTY_PHYSICAL_POSITIONS_ARRAY;
          } else {
            return readPhysicalPositions(network, positionsCount);
          }

        } finally {
          endResponse(network);
        }

      }
    }, "Error on retrieving ceiling positions after " + physicalPosition.clusterPosition);
  }

  @Override
  public OPhysicalPosition[] lowerPhysicalPositions(final int iClusterId, final OPhysicalPosition physicalPosition) {
    return networkOperation(new OStorageRemoteOperation<OPhysicalPosition[]>() {
      @Override
      public OPhysicalPosition[] execute(final OChannelBinaryAsynchClient network) throws IOException {
        try {
          beginRequest(network, OChannelBinaryProtocol.REQUEST_POSITIONS_LOWER);
          network.writeInt(iClusterId);
          network.writeLong(physicalPosition.clusterPosition);

        } finally {
          endRequest(network);
        }

        try {
          beginResponse(network);

          final int positionsCount = network.readInt();

          if (positionsCount == 0) {
            return OCommonConst.EMPTY_PHYSICAL_POSITIONS_ARRAY;
          } else {
            return readPhysicalPositions(network, positionsCount);
          }

        } finally {
          endResponse(network);
        }
      }
    }, "Error on retrieving lower positions after " + physicalPosition.clusterPosition);
  }

  @Override
  public OPhysicalPosition[] floorPhysicalPositions(final int clusterId, final OPhysicalPosition physicalPosition) {
    return networkOperation(new OStorageRemoteOperation<OPhysicalPosition[]>() {
      @Override
      public OPhysicalPosition[] execute(final OChannelBinaryAsynchClient network) throws IOException {

        try {
          beginRequest(network, OChannelBinaryProtocol.REQUEST_POSITIONS_FLOOR);
          network.writeInt(clusterId);
          network.writeLong(physicalPosition.clusterPosition);

        } finally {
          endRequest(network);
        }

        try {
          beginResponse(network);

          final int positionsCount = network.readInt();

          if (positionsCount == 0) {
            return OCommonConst.EMPTY_PHYSICAL_POSITIONS_ARRAY;
          } else {
            return readPhysicalPositions(network, positionsCount);
          }

        } finally {
          endResponse(network);
        }
      }
    }, "Error on retrieving floor positions after " + physicalPosition.clusterPosition);
  }

  public long getSize() {
    return networkOperation(new OStorageRemoteOperation<Long>() {
      @Override
      public Long execute(final OChannelBinaryAsynchClient network) throws IOException {
        try {
          beginRequest(network, OChannelBinaryProtocol.REQUEST_DB_SIZE);

        } finally {
          endRequest(network);
        }

        try {
          beginResponse(network);
          return network.readLong();
        } finally {
          endResponse(network);
        }
      }
    }, "Error on read database size");
  }

  @Override
  public long countRecords() {
    return networkOperation(new OStorageRemoteOperation<Long>() {
      @Override
      public Long execute(OChannelBinaryAsynchClient network) throws IOException {
        try {
          beginRequest(network, OChannelBinaryProtocol.REQUEST_DB_COUNTRECORDS);
        } finally {
          endRequest(network);
        }

        try {
          beginResponse(network);
          return network.readLong();
        } finally {
          endResponse(network);
        }
      }
    }, "Error on read database record count");
  }

  public long count(final int[] iClusterIds) {
    return count(iClusterIds, false);
  }

  public long count(final int[] iClusterIds, final boolean countTombstones) {

    return networkOperation(new OStorageRemoteOperation<Long>() {
      @Override
      public Long execute(OChannelBinaryAsynchClient network) throws IOException {
        try {
          beginRequest(network, OChannelBinaryProtocol.REQUEST_DATACLUSTER_COUNT);

          network.writeShort((short) iClusterIds.length);
          for (int iClusterId : iClusterIds)
            network.writeShort((short) iClusterId);

          if (network.getSrvProtocolVersion() >= 13)
            network.writeByte(countTombstones ? (byte) 1 : (byte) 0);
        } finally {
          endRequest(network);
        }

        try {
          beginResponse(network);
          return network.readLong();
        } finally {
          endResponse(network);
        }
      }
    }, "Error on read record count in clusters: " + Arrays.toString(iClusterIds));
  }

  /**
   * Execute the command remotely and get the results back.
   */
  public Object command(final OCommandRequestText iCommand) {

    if (!(iCommand instanceof OSerializableStream))
      throw new OCommandExecutionException("Cannot serialize the command to be executed to the server side.");
    final boolean live = iCommand instanceof OLiveQuery;
    final ODatabaseDocument database = ODatabaseRecordThreadLocal.INSTANCE.get();

    return networkOperation(new OStorageRemoteOperation<Object>() {
      @Override
      public Object execute(final OChannelBinaryAsynchClient network) throws IOException {
        Object result = null;
        getCurrentSession().commandExecuting = true;
        try {

          final boolean asynch = iCommand instanceof OCommandRequestAsynch && ((OCommandRequestAsynch) iCommand).isAsynchronous();

          try {
            beginRequest(network, OChannelBinaryProtocol.REQUEST_COMMAND);

            if (live) {
              network.writeByte((byte) 'l');
            } else {
              network.writeByte((byte) (asynch ? 'a' : 's')); // ASYNC / SYNC
            }
            network.writeBytes(OStreamSerializerAnyStreamable.INSTANCE.toStream(iCommand));

          } finally {
            endRequest(network);
          }

          try {
            beginResponse(network);

            boolean addNextRecord = true;

            if (asynch) {
              byte status;

              // ASYNCH: READ ONE RECORD AT TIME
              while ((status = network.readByte()) > 0) {
                final ORecord record = (ORecord) OChannelBinaryProtocol.readIdentifiable(network);
                if (record == null)
                  continue;

                switch (status) {
                case 1:
                  // PUT AS PART OF THE RESULT SET. INVOKE THE LISTENER
                  if (addNextRecord) {
                    addNextRecord = iCommand.getResultListener().result(record);
                    database.getLocalCache().updateRecord(record);
                  }
                  break;

                case 2:
                  // PUT IN THE CLIENT LOCAL CACHE
                  database.getLocalCache().updateRecord(record);
                }
              }
            } else {
              result = readSynchResult(network, database);
              if (live) {
                final ODocument doc = ((List<ODocument>) result).get(0);
                final Integer token = doc.field("token");
                final Boolean unsubscribe = doc.field("unsubscribe");
                if (token != null) {
                  if (Boolean.TRUE.equals(unsubscribe)) {
                    if (OStorageRemote.this.asynchEventListener != null)
                      OStorageRemote.this.asynchEventListener.unregisterLiveListener(token);
                  } else {
                    OLiveResultListener listener = (OLiveResultListener) iCommand.getResultListener();
                    // TODO pass db copy!!!
                    ORemoteConnectionPool pool = OStorageRemote.this.engine.getConnectionManager().getPool(network.getServerURL());
                    OStorageRemote.this.asynchEventListener.registerLiveListener(pool, token, listener);
                  }
                } else {
                  throw new OStorageException("Cannot execute live query, returned null token");
                }
              }
            }
            return result;
          } finally {
            endResponse(network);
          }
        } finally {
          getCurrentSession().commandExecuting = false;
          if (iCommand.getResultListener() != null && !live)
            iCommand.getResultListener().end();
        }

      }
    }, "Error on executing command: " + iCommand);
  }

  protected Object readSynchResult(final OChannelBinaryAsynchClient network, final ODatabaseDocument database) throws IOException {

    final Object result;

    final byte type = network.readByte();
    switch (type) {
    case 'n':
      result = null;
      break;

    case 'r':
      result = OChannelBinaryProtocol.readIdentifiable(network);
      if (result instanceof ORecord)
        database.getLocalCache().updateRecord((ORecord) result);
      break;

    case 'l':
    case 's':
      final int tot = network.readInt();
      final Collection<OIdentifiable> coll;

      coll = type == 's' ? new HashSet<OIdentifiable>(tot) : new ArrayList<OIdentifiable>(tot);
      for (int i = 0; i < tot; ++i) {
        final OIdentifiable resultItem = OChannelBinaryProtocol.readIdentifiable(network);
        if (resultItem instanceof ORecord)
          database.getLocalCache().updateRecord((ORecord) resultItem);
        coll.add(resultItem);
      }

      result = coll;
      break;
    case 'i':
      coll = new ArrayList<OIdentifiable>();
      byte status;
      while ((status = network.readByte()) > 0) {
        final OIdentifiable record = OChannelBinaryProtocol.readIdentifiable(network);
        if (record == null)
          continue;
        if (status == 1) {
          if (record instanceof ORecord)
            database.getLocalCache().updateRecord((ORecord) record);
          coll.add(record);
        }
      }
      result = coll;
      break;
    case 'w':
      final OIdentifiable record = OChannelBinaryProtocol.readIdentifiable(network);
      // ((ODocument) record).setLazyLoad(false);
      result = ((ODocument) record).field("result");
      break;

    default:
      OLogManager.instance().warn(this, "Received unexpected result from query: %d", type);
      result = null;
    }

    if (network.getSrvProtocolVersion() >= 17) {
      // LOAD THE FETCHED RECORDS IN CACHE
      byte status;
      while ((status = network.readByte()) > 0) {
        final ORecord record = (ORecord) OChannelBinaryProtocol.readIdentifiable(network);
        if (record != null && status == 2)
          // PUT IN THE CLIENT LOCAL CACHE
          database.getLocalCache().updateRecord(record);
      }
    }

    return result;
  }

  public List<ORecordOperation> commit(final OTransaction iTx, final Runnable callback) {
    networkOperation(new OStorageRemoteOperation<Void>() {
      @Override
      public Void execute(OChannelBinaryAsynchClient network) throws IOException {
        final List<ORecordOperation> committedEntries = new ArrayList<ORecordOperation>();
        try {
          getCurrentSession().commandExecuting = true;

          try {
            beginRequest(network, OChannelBinaryProtocol.REQUEST_TX_COMMIT);

            network.writeInt(iTx.getId());
            network.writeByte((byte) (iTx.isUsingLog() ? 1 : 0));

            for (ORecordOperation txEntry : iTx.getAllRecordEntries()) {
              commitEntry(network, txEntry);
            }

            // END OF RECORD ENTRIES
            network.writeByte((byte) 0);

            // SEND EMPTY TX CHANGES, TRACKING MADE SERVER SIDE
            network.writeBytes(iTx.getIndexChanges().toStream());
          } finally {
            endRequest(network);
          }

          try {
            beginResponse(network);
            final int createdRecords = network.readInt();
            ORecordId currentRid;
            ORecordId createdRid;
            for (int i = 0; i < createdRecords; i++) {
              currentRid = network.readRID();
              createdRid = network.readRID();

              iTx.updateIdentityAfterCommit(currentRid, createdRid);
            }

            final int updatedRecords = network.readInt();
            ORecordId rid;
            for (int i = 0; i < updatedRecords; ++i) {
              rid = network.readRID();

              ORecordOperation rop = iTx.getRecordEntry(rid);
              if (rop != null)
                ORecordInternal.setVersion(rop.getRecord(), network.readVersion());
            }

            if (network.getSrvProtocolVersion() >= 20)
              readCollectionChanges(network, ODatabaseRecordThreadLocal.INSTANCE.get().getSbTreeCollectionManager());

          } finally {
            endResponse(network);
          }

          committedEntries.clear();
          // SET ALL THE RECORDS AS UNDIRTY
          for (ORecordOperation txEntry : iTx.getAllRecordEntries())
            ORecordInternal.unsetDirty(txEntry.getRecord());

          // UPDATE THE CACHE ONLY IF THE ITERATOR ALLOWS IT. USE THE STRATEGY TO ALWAYS REMOVE ALL THE RECORDS SINCE THEY COULD BE
          // CHANGED AS CONTENT IN CASE OF TREE AND GRAPH DUE TO CROSS REFERENCES
          OTransactionAbstract.updateCacheFromEntries(iTx, iTx.getAllRecordEntries(), false);

          return null;
        } finally {
          getCurrentSession().commandExecuting = false;
        }
      }
    }, "Error on commit");
    return null;
  }

  @Override
  public OUncompletedCommit<List<ORecordOperation>> initiateCommit(OTransaction iTx, Runnable callback) {
    throw new UnsupportedOperationException("Uncompleted commits are not supported by the remote storage.");
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
    return networkOperation(new OStorageRemoteOperation<Integer>() {
      @Override
      public Integer execute(OChannelBinaryAsynchClient network) throws IOException {
        stateLock.acquireWriteLock();
        try {
          try {
            beginRequest(network, OChannelBinaryProtocol.REQUEST_DATACLUSTER_ADD);

            network.writeString(iClusterName);
            if (network.getSrvProtocolVersion() >= 18)
              network.writeShort((short) iRequestedId);
          } finally {
            endRequest(network);
          }

          try {
            beginResponse(network);
            final int clusterId = network.readShort();

            final OClusterRemote cluster = new OClusterRemote();
            cluster.configure(OStorageRemote.this, clusterId, iClusterName.toLowerCase());

            if (clusters.length <= clusterId)
              clusters = Arrays.copyOf(clusters, clusterId + 1);
            clusters[cluster.getId()] = cluster;
            clusterMap.put(cluster.getName().toLowerCase(), cluster);

            return clusterId;
          } finally {
            endResponse(network);
          }
        } finally {
          stateLock.releaseWriteLock();
        }
      }
    }, "Error on add new cluster");
  }

  public boolean dropCluster(final int iClusterId, final boolean iTruncate) {
    return networkOperation(new OStorageRemoteOperation<Boolean>() {
      @Override
      public Boolean execute(OChannelBinaryAsynchClient network) throws IOException {
        stateLock.acquireWriteLock();
        try {
          try {
            beginRequest(network, OChannelBinaryProtocol.REQUEST_DATACLUSTER_DROP);

            network.writeShort((short) iClusterId);

          } finally {
            endRequest(network);
          }

          byte result = 0;
          try {
            beginResponse(network);
            result = network.readByte();
          } finally {
            endResponse(network);
          }

          if (result == 1) {
            // REMOVE THE CLUSTER LOCALLY
            final OCluster cluster = clusters[iClusterId];
            clusters[iClusterId] = null;
            clusterMap.remove(cluster.getName());
            if (configuration.clusters.size() > iClusterId)
              configuration.dropCluster(iClusterId); // endResponse must be called before this line, which call updateRecord

            return true;
          }
          return false;
        } finally {
          stateLock.releaseWriteLock();
        }
      }
    }, "Error on removing of cluster");
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

    try {
      iNetwork.flush();
      iNetwork.releaseWriteLock();
    } catch (IOException e) {
      engine.getConnectionManager().remove(iNetwork);
      throw e;
    }
  }

  /**
   * End response reached: release the channel in the pool to being reused
   */
  public void endResponse(final OChannelBinaryAsynchClient iNetwork) {
    try {
      iNetwork.endResponse();
      engine.getConnectionManager().release(iNetwork);
    } catch (IOException e) {
      engine.getConnectionManager().remove(iNetwork);
      OLogManager.instance().warn(this, "dirty data left in the socket closing", e);
    }
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
        // serverURLs.clear();

        // ADD CURRENT SERVER AS FIRST
        if (iConnectedURL != null)
          addHost(iConnectedURL);

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

  public OEngineRemote getEngine() {
    return engine;
  }

  @Override
  public String getUserName() {
    OStorageRemoteSession session = getCurrentSession();
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
          OStorageRemoteNodeSession nodeSession = session.getOrCreate(network.getServerURL());
          if (nodeSession == null || !nodeSession.isValid()) {
            openRemoteDatabase(network);
            return network.getServerURL();
          } else {
            try {
              network.writeByte(OChannelBinaryProtocol.REQUEST_DB_REOPEN);
              network.writeInt(nodeSession.getSessionId());
              network.writeBytes(nodeSession.getToken());
            } finally {
              endRequest(network);
            }

            final int sessionId;

            try {
              byte[] newToken = network.beginResponse(nodeSession.getSessionId(), true);
              sessionId = network.readInt();
              if (newToken != null && newToken.length > 0) {
                nodeSession.setSession(sessionId, newToken);
              } else {
                nodeSession.setSession(sessionId, nodeSession.getToken());
              }
              OLogManager.instance().debug(this, "Client connected to %s with session id=%d", network.getServerURL(), sessionId);
              return currentURL;
            } finally {
              endResponse(network);
            }
          }
        } catch (OIOException e) {
          if (network != null) {
            // REMOVE THE NETWORK CONNECTION IF ANY
            engine.getConnectionManager().remove(network);
          }

          OLogManager.instance().error(this, "Cannot open database with url " + currentURL, e);
        } catch (OOfflineNodeException e) {
          if (network != null) {
            // REMOVE THE NETWORK CONNECTION IF ANY
            engine.getConnectionManager().remove(network);
          }

          OLogManager.instance().debug(this, "Cannot open database with url " + currentURL, e);
        } catch (OSecurityException ex) {
          OLogManager.instance().debug(this, "Invalidate token for url=%s", ex, currentURL);
          OStorageRemoteSession session = getCurrentSession();
          session.remove(currentURL);

          if (network != null) {
            // REMOVE THE NETWORK CONNECTION IF ANY
            try {
              engine.getConnectionManager().remove(network);
            } catch (Exception e) {
              // IGNORE ANY EXCEPTION
              OLogManager.instance().debug(this, "Cannot remove connection or database url=" + currentURL, e);
            }
          }
        } catch (OException e) {
          // PROPAGATE ANY OTHER ORIENTDB EXCEPTION
          throw e;

        } catch (Exception e) {
          OLogManager.instance().debug(this, "Cannot open database with url " + currentURL, e);
          if (network != null) {
            // REMOVE THE NETWORK CONNECTION IF ANY
            try {
              engine.getConnectionManager().remove(network);
            } catch (Exception ex) {
              // IGNORE ANY EXCEPTION
              OLogManager.instance().debug(this, "Cannot remove connection or database url=" + currentURL, e);
            }
          }
        }
      } while (engine.getConnectionManager().getAvailableConnections(currentURL) > 0);

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
      OStorageRemoteNodeSession nodeSession = session.getOrCreate(network.getServerURL());
      try {
        network.writeByte(OChannelBinaryProtocol.REQUEST_DB_OPEN);
        network.writeInt(nodeSession.getSessionId());

        // @SINCE 1.0rc8
        sendClientInfo(network, DRIVER_NAME, true, true);

        network.writeString(name);
        network.writeString(session.connectionUserName);
        network.writeString(session.connectionUserPassword);

      } finally {
        endRequest(network);
      }

      final int sessionId;

      try {
        network.beginResponse(nodeSession.getSessionId(), false);
        sessionId = network.readInt();
        byte[] token = network.readBytes();
        if (token.length == 0) {
          token = null;
        }

        nodeSession.setSession(sessionId, token);

        OLogManager.instance().debug(this, "Client connected to %s with session id=%d", network.getServerURL(), sessionId);

        readDatabaseInformation(network);

        // READ CLUSTER CONFIGURATION
        updateClusterConfiguration(network.getServerURL(), network.readBytes());

        // read OrientDB release info
        if (network.getSrvProtocolVersion() >= 14)
          network.readString();

        status = STATUS.OPEN;

      } finally {
        endResponse(network);
      }
    } finally {
      stateLock.releaseWriteLock();
    }
  }

  protected String openRemoteDatabase(String currentURL) {
    do {
      do {
        OChannelBinaryAsynchClient network = null;
        network = getNetwork(currentURL);
        try {
          openRemoteDatabase(network);
          return currentURL;
        } catch (OIOException e) {
          if (network != null) {
            // REMOVE THE NETWORK CONNECTION IF ANY
            engine.getConnectionManager().remove(network);
            network = null;
          }

          OLogManager.instance().error(this, "Cannot open database with url " + currentURL, e);

        } catch (OException e) {
          // PROPAGATE ANY OTHER ORIENTDB EXCEPTION
          throw e;

        } catch (Exception e) {
          if (network != null) {
            // REMOVE THE NETWORK CONNECTION IF ANY
            try {
              engine.getConnectionManager().remove(network);
            } catch (Exception ex) {
              // IGNORE ANY EXCEPTION
              OLogManager.instance().debug(this, "Cannot remove connection or database url=" + currentURL, e);
            }
            network = null;
          }

          OLogManager.instance().error(this, "Cannot open database url=" + currentURL, e);
        }
      } while (engine.getConnectionManager().getReusableConnections(currentURL) > 0);

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

      OLogManager.instance().debug(this, "Updated server list: %s...", serverURLs);

      if (!serverURLs.isEmpty())
        return serverURLs.get(0) + postFix;
    }

    return null;
  }

  protected void sendClientInfo(final OChannelBinaryAsynchClient network, final String driverName,
      final boolean supportsPushMessages, final boolean collectStats) throws IOException {
    if (network.getSrvProtocolVersion() >= 7) {
      // @COMPATIBILITY 1.0rc8
      network.writeString(driverName).writeString(OConstants.ORIENT_VERSION)
          .writeShort((short) OChannelBinaryProtocol.CURRENT_PROTOCOL_VERSION).writeString(clientId);
    }
    if (network.getSrvProtocolVersion() > OChannelBinaryProtocol.PROTOCOL_VERSION_21) {
      network.writeString(ODatabaseDocumentTx.getDefaultSerializer().toString());
      recordFormat = ODatabaseDocumentTx.getDefaultSerializer().toString();
    } else
      recordFormat = ORecordSerializerSchemaAware2CSV.NAME;
    if (network.getSrvProtocolVersion() > OChannelBinaryProtocol.PROTOCOL_VERSION_26)
      network.writeBoolean(true);
    if (network.getSrvProtocolVersion() > OChannelBinaryProtocol.PROTOCOL_VERSION_33) {
      network.writeBoolean(supportsPushMessages);
      network.writeBoolean(collectStats);
    }
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
    if (host.startsWith("localhost"))
      host = "127.0.0.1" + host.substring("localhost".length());

    if (host.contains("/"))
      host = host.substring(0, host.indexOf("/"));

    // REGISTER THE REMOTE SERVER+PORT
    if (!host.contains(":"))
      host += ":"
          + (clientConfiguration.getValueAsBoolean(OGlobalConfiguration.CLIENT_USE_SSL) ? getDefaultSSLPort() : getDefaultPort());
    else if (host.split(":").length < 2 || host.split(":")[1].trim().length() == 0)
      host += (clientConfiguration.getValueAsBoolean(OGlobalConfiguration.CLIENT_USE_SSL) ? getDefaultSSLPort() : getDefaultPort());

    synchronized (serverURLs) {
      if (!serverURLs.contains(host)) {
        serverURLs.add(host);
        OLogManager.instance().info(this, "Registered the new available server '%s'", host);
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
   * @param iCommand
   *          id. Ids described at {@link OChannelBinaryProtocol}
   * @return connection to server
   * @throws IOException
   */
  public OChannelBinaryAsynchClient beginRequest(final OChannelBinaryAsynchClient network, final byte iCommand) throws IOException {
    OStorageRemoteSession session = getCurrentSession();
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
      break;

    case ROUND_ROBIN_REQUEST:
      url = getServerURFromList(true, session);
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
        network = engine.getConnectionManager().acquire(iCurrentURL, clientConfiguration, connectionOptions, asynchEventListener);
      } catch (Exception cause) {
        throw OException.wrapException(new OStorageException("Cannot open a connection to remote server: " + iCurrentURL), cause);
      }
      if (!network.tryLock()) {
        // CANNOT LOCK IT, MAYBE HASN'T BE CORRECTLY UNLOCKED BY PREVIOUS USER
        OLogManager.instance().error(this, "Removing locked network channel '%s'...", iCurrentURL);
        engine.getConnectionManager().remove(network);
        network = null;
      }
    } while (network == null);
    return network;
  }

  /**
   * Starts listening the response.
   */
  public void beginResponse(final OChannelBinaryAsynchClient iNetwork) throws IOException {
    OStorageRemoteSession session = getCurrentSession();
    beginResponse(iNetwork, session);
  }

  private void beginResponse(OChannelBinaryAsynchClient iNetwork, OStorageRemoteSession session) throws IOException {
    OStorageRemoteNodeSession nodeSession = session.get(iNetwork.getServerURL());
    byte[] newToken = iNetwork.beginResponse(nodeSession.getSessionId(), true);
    if (newToken != null && newToken.length > 0) {
      nodeSession.setSession(nodeSession.getSessionId(), newToken);
    }
  }

  protected void getResponse(final OChannelBinaryAsynchClient iNetwork) throws IOException {
    try {
      beginResponse(iNetwork);
    } finally {
      endResponse(iNetwork);
    }
  }

  private OPhysicalPosition[] readPhysicalPositions(OChannelBinaryAsynchClient network, int positionsCount) throws IOException {
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

  private void readCollectionChanges(OChannelBinaryAsynchClient network, OSBTreeCollectionManager collectionManager)
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

  private void commitEntry(final OChannelBinaryAsynchClient iNetwork, final ORecordOperation txEntry) throws IOException {
    if (txEntry.type == ORecordOperation.LOADED)
      // JUMP LOADED OBJECTS
      return;

    // SERIALIZE THE RECORD IF NEEDED. THIS IS DONE HERE TO CATCH EXCEPTION AND SEND A -1 AS ERROR TO THE SERVER TO SIGNAL THE ABORT
    // OF TX COMMIT
    byte[] stream = null;
    try {
      switch (txEntry.type) {
      case ORecordOperation.CREATED:
      case ORecordOperation.UPDATED:
        stream = txEntry.getRecord().toStream();
        break;
      }
    } catch (Exception e) {
      // ABORT TX COMMIT
      iNetwork.writeByte((byte) -1);
      throw OException.wrapException(new OTransactionException("Error on transaction commit"), e);
    }

    iNetwork.writeByte((byte) 1);
    iNetwork.writeByte(txEntry.type);
    iNetwork.writeRID(txEntry.getRecord().getIdentity());
    iNetwork.writeByte(ORecordInternal.getRecordType(txEntry.getRecord()));

    switch (txEntry.type) {
    case ORecordOperation.CREATED:
      iNetwork.writeBytes(stream);
      break;

    case ORecordOperation.UPDATED:
      iNetwork.writeVersion(txEntry.getRecord().getVersion());
      iNetwork.writeBytes(stream);
      if (iNetwork.getSrvProtocolVersion() >= 23)
        iNetwork.writeBoolean(ORecordInternal.isContentChanged(txEntry.getRecord()));
      break;

    case ORecordOperation.DELETED:
      iNetwork.writeVersion(txEntry.getRecord().getVersion());
      break;
    }
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

  private void readDatabaseInformation(final OChannelBinaryAsynchClient network) throws IOException {
    // @COMPATIBILITY 1.0rc8
    final int tot = network.getSrvProtocolVersion() >= 7 ? network.readShort() : network.readInt();

    stateLock.acquireWriteLock();
    try {

      clusters = new OCluster[tot];
      clusterMap.clear();

      for (int i = 0; i < tot; ++i) {
        final OClusterRemote cluster = new OClusterRemote();
        String clusterName = network.readString();
        final int clusterId = network.readShort();
        if (clusterName != null) {
          clusterName = clusterName.toLowerCase();

          if (network.getSrvProtocolVersion() < 24)
            network.readString();

          final int dataSegmentId = network.getSrvProtocolVersion() >= 12 && network.getSrvProtocolVersion() < 24
              ? (int) network.readShort() : 0;

          cluster.configure(this, clusterId, clusterName);

          if (clusterId >= clusters.length)
            clusters = Arrays.copyOf(clusters, clusterId + 1);
          clusters[clusterId] = cluster;
          clusterMap.put(clusterName, cluster);
        }
      }

      final OCluster defaultCluster = clusterMap.get(CLUSTER_DEFAULT_NAME);
      if (defaultCluster != null)
        defaultClusterId = clusterMap.get(CLUSTER_DEFAULT_NAME).getId();

    } finally {
      stateLock.releaseWriteLock();
    }
  }

  private boolean deleteRecord(byte command, final ORecordId iRid, final int iVersion, int iMode,
      final ORecordCallback<Boolean> iCallback, final OChannelBinaryAsynchClient network) throws IOException {
    try {
      beginRequest(network, command);
      network.writeRID(iRid);
      network.writeVersion(iVersion);
      network.writeByte((byte) iMode);

    } finally {
      endRequest(network);
    }

    switch (iMode) {
    case 0:
      // SYNCHRONOUS
      try {
        beginResponse(network);
        return network.readByte() == 1;
      } finally {
        endResponse(network);
      }

    case 1:
      // ASYNCHRONOUS
      if (iCallback != null) {
        final OStorageRemoteSession session = getCurrentSession();
        Callable<Object> response = new Callable<Object>() {
          public Object call() throws Exception {
            Boolean result;

            try {
              beginResponse(network, session);
              result = network.readByte() == 1;
            } finally {
              endResponse(network);
            }

            iCallback.call(iRid, result);
            return null;
          }
        };
        asynchExecutor.submit(new FutureTask<Object>(response));
      }
    }
    return false;
  }

  protected OStorageRemoteSession getCurrentSession() {
    ODatabaseDocumentTx db = (ODatabaseDocumentTx) ODatabaseRecordThreadLocal.INSTANCE.getIfDefined();
    if (db == null)
      return null;
    OStorageRemoteSession session = (OStorageRemoteSession) ODatabaseDocumentTxInternal.getSessionMetadata(db);
    if (session == null) {
      session = new OStorageRemoteSession(sessionSerialId.decrementAndGet());
      ODatabaseDocumentTxInternal.setSessionMetadata(db, session);
    }
    return session;
  }

  @Override
  public boolean isClosed() {
    if (super.isClosed())
      return true;
    OStorageRemoteSession session = getCurrentSession();
    if (session == null)
      return false;
    return session.getAll().isEmpty();
  }

  @Override
  public OStorageRemote copy(ODatabaseDocumentTx source, ODatabaseDocumentTx dest) {
    ODatabaseDocumentInternal origin = ODatabaseRecordThreadLocal.INSTANCE.getIfDefined();

    OStorageRemoteSession session = (OStorageRemoteSession) ODatabaseDocumentTxInternal.getSessionMetadata(source);
    if (session != null) {
      // TODO:may run a session reopen
      OStorageRemoteSession newSession = new OStorageRemoteSession(sessionSerialId.decrementAndGet());
      newSession.connectionUserName = session.connectionUserName;
      newSession.connectionUserPassword = session.connectionUserPassword;
      ODatabaseDocumentTxInternal.setSessionMetadata(dest, newSession);
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

}
