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

import com.orientechnologies.common.concur.lock.OModificationOperationProhibitedException;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.io.OIOException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.util.OCommonConst;
import com.orientechnologies.orient.client.remote.OStorageRemoteThreadLocal.OStorageRemoteSession;
import com.orientechnologies.orient.core.OConstants;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.command.OCommandRequestAsynch;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.config.OStorageConfiguration;
import com.orientechnologies.orient.core.conflict.ORecordConflictStrategy;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OCurrentStorageComponentsFactory;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.db.record.ridbag.sbtree.OBonsaiCollectionPointer;
import com.orientechnologies.orient.core.db.record.ridbag.sbtree.OSBTreeCollectionManager;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.exception.OTransactionException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.OSerializableStream;
import com.orientechnologies.orient.core.serialization.serializer.record.string.ORecordSerializerSchemaAware2CSV;
import com.orientechnologies.orient.core.serialization.serializer.record.string.ORecordSerializerStringAbstract;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializerAnyStreamable;
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
import com.orientechnologies.orient.core.version.ORecordVersion;
import com.orientechnologies.orient.core.version.OVersionFactory;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryAsynchClient;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;
import com.orientechnologies.orient.enterprise.channel.binary.ORemoteServerEventListener;

import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;

/**
 * This object is bound to each remote ODatabase instances.
 */
public class OStorageRemote extends OStorageAbstract implements OStorageProxy {
  public static final String            PARAM_MIN_POOL       = "minpool";
  public static final String            PARAM_MAX_POOL       = "maxpool";
  public static final String            PARAM_DB_TYPE        = "dbtype";
  private static final String           DEFAULT_HOST         = "localhost";
  private static final int              DEFAULT_PORT         = 2424;
  private static final int              DEFAULT_SSL_PORT     = 2434;
  private static final String           ADDRESS_SEPARATOR    = ";";
  private static final String           DRIVER_NAME          = "OrientDB Java";
  protected final List<String>          serverURLs           = new ArrayList<String>();
  protected final Map<String, OCluster> clusterMap           = new ConcurrentHashMap<String, OCluster>();
  private final ExecutorService         asynchExecutor;
  private final ODocument               clusterConfiguration = new ODocument();
  private final String                  clientId;
  private OContextConfiguration         clientConfiguration;
  private int                           connectionRetry;
  private int                           connectionRetryDelay;
  @Deprecated
  private int                           networkPoolCursor    = 0;
  private OCluster[]                    clusters             = OCommonConst.EMPTY_CLUSTER_ARRAY;
  private int                           defaultClusterId;
  private ORemoteServerEventListener    asynchEventListener;
  private String                        connectionDbType;

  private volatile String               connectionUserName;

  private String                        connectionUserPassword;
  private Map<String, Object>           connectionOptions;
  private OEngineRemote                 engine;
  private String                        recordFormat;
  private int                           port                  = DEFAULT_PORT;
  private int                           sslPort               = DEFAULT_SSL_PORT;

  public OStorageRemote(final String iClientId, final String iURL, final String iMode) throws IOException {
    this(iClientId, iURL, iMode, null);
  }

  public OStorageRemote(final String iClientId, final String iURL, final String iMode, STATUS status) throws IOException {
    this(iClientId, iURL, iMode, status, DEFAULT_PORT, DEFAULT_SSL_PORT);
  }

  public OStorageRemote(final String iClientId, final String iURL, final String iMode, STATUS status, int port, int sslPort) throws IOException {
    super(iURL, iURL, iMode, 0); // NO TIMEOUT @SINCE 1.5
    if (status != null) {
      this.status = status;
    }

    clientId = iClientId;
    configuration = null;

    clientConfiguration = new OContextConfiguration();
    connectionRetry = clientConfiguration.getValueAsInteger(OGlobalConfiguration.NETWORK_SOCKET_RETRY);
    connectionRetryDelay = clientConfiguration.getValueAsInteger(OGlobalConfiguration.NETWORK_SOCKET_RETRY_DELAY);
    asynchEventListener = new OStorageRemoteAsynchEventListener(this);
    parseServerURLs();

    asynchExecutor = Executors.newSingleThreadScheduledExecutor();

    engine = (OEngineRemote) Orient.instance().getEngine(OEngineRemote.NAME);
    this.port = port;
    this.sslPort = sslPort;
  }

  @Override
  public boolean isAssigningClusterIds() {
    return false;
  }

  public int getSessionId() {
    return OStorageRemoteThreadLocal.INSTANCE.get().sessionId;
  }

  public String getServerURL() {
    return OStorageRemoteThreadLocal.INSTANCE.get().serverURL;
  }

  public byte[] getSessionToken() {
    return OStorageRemoteThreadLocal.INSTANCE.get().token;
  }

  public void setSessionId(final String iServerURL, final int iSessionId, byte[] token) {
    final OStorageRemoteThreadLocal instance = OStorageRemoteThreadLocal.INSTANCE;
    if (instance != null) {
      final OStorageRemoteSession tl = instance.get();
      tl.serverURL = iServerURL;
      tl.sessionId = iSessionId;
      tl.token = token;
    }
  }

  public void clearToken() {
    final OStorageRemoteThreadLocal instance = OStorageRemoteThreadLocal.INSTANCE;
    if (instance != null) {
      final OStorageRemoteSession tl = instance.get();
      tl.token = null;
    }
  }

  public void clearSession() {
    OStorageRemoteThreadLocal.INSTANCE.remove();
  }

  public ORemoteServerEventListener getAsynchEventListener() {
    return asynchEventListener;
  }

  public void setAsynchEventListener(final ORemoteServerEventListener iListener) {
    asynchEventListener = iListener;
  }

  public void removeRemoteServerEventListener() {
    asynchEventListener = null;
  }

  @Override
  public void open(final String iUserName, final String iUserPassword, final Map<String, Object> iOptions) {
    addUser();

    lock.acquireExclusiveLock();
    try {

      connectionUserName = iUserName;
      connectionUserPassword = iUserPassword;
      connectionOptions = iOptions != null ? new HashMap<String, Object>(iOptions) : null; // CREATE A COPY TO AVOID USER
      // MANIPULATION
      // POST OPEN
      openRemoteDatabase();

      final OStorageConfiguration storageConfiguration = new OStorageRemoteConfiguration(this, recordFormat);
      storageConfiguration.load();

      configuration = storageConfiguration;

      componentsFactory = new OCurrentStorageComponentsFactory(configuration);
    } catch (IOException e) {
      throw new OStorageException("Cannot open the remote storage: " + name, e);

    } catch (OSerializationException e) {
      if (e instanceof RuntimeException) {
        // PASS THROUGH
        throw e;
      } else {
        throw new OStorageException("Cannot open the remote storage: " + name, e);
      }
    } finally {
      lock.releaseExclusiveLock();
    }
  }

  @Override
  public void reload() {

    lock.acquireExclusiveLock();
    try {

      OChannelBinaryAsynchClient network = null;
      do {
        try {

          try {
            network = beginRequest(OChannelBinaryProtocol.REQUEST_DB_RELOAD);
          } finally {
            endRequest(network);
          }

          try {
            beginResponse(network);

            readDatabaseInformation(network);
            break;

          } finally {
            endResponse(network);
          }

        } catch (IOException e) {
          handleException(network, "Error on reloading database information", e);

        }
      } while (true);

    } finally {
      lock.releaseExclusiveLock();
    }
  }

  @Override
  public void create(final Map<String, Object> iOptions) {
    throw new UnsupportedOperationException(
        "Cannot create a database in a remote server. Please use the console or the OServerAdmin class.");
  }

  @Override
  public boolean exists() {
    throw new UnsupportedOperationException(
        "Cannot check the existance of a database in a remote server. Please use the console or the OServerAdmin class.");
  }

  @Override
  public void close(final boolean iForce, boolean onDelete) {
    if (status == STATUS.CLOSED) {
      return;
    }

    OChannelBinaryAsynchClient network = null;

    lock.acquireExclusiveLock();
    try {
      if (status == STATUS.CLOSED) {
        return;
      }

      network = beginRequest(OChannelBinaryProtocol.REQUEST_DB_CLOSE);
      try {
        setSessionId(null, -1, null);
      } finally {
        endRequest(network);
        engine.getConnectionManager().release(network);
      }

      if (!checkForClose(iForce)) {
        return;
      }

      status = STATUS.CLOSING;
      // CLOSE ALL THE CONNECTIONS
      engine.getConnectionManager().closePool(getCurrentServerURL());

      super.close(iForce, onDelete);
      status = STATUS.CLOSED;

      Orient.instance().unregisterStorage(this);
    } catch (IOException e) {
      if (network != null) {
        OLogManager.instance().debug(this, "Error on closing remote connection: %s", network);
        network.close();
      }
    } finally {
      lock.releaseExclusiveLock();
    }
  }

  @Override
  public void delete() {
    throw new UnsupportedOperationException(
        "Cannot delete a database in a remote server. Please use the console or the OServerAdmin class.");
  }

  @Override
  public Set<String> getClusterNames() {
    lock.acquireSharedLock();
    try {

      return new HashSet<String>(clusterMap.keySet());

    } finally {
      lock.releaseSharedLock();
    }
  }

  @Override
  public OStorageOperationResult<OPhysicalPosition> createRecord(final ORecordId iRid, final byte[] iContent,
      ORecordVersion iRecordVersion, final byte iRecordType, int iMode, final ORecordCallback<Long> iCallback) {
    int iMode0 = iMode;
    if (iMode0 == 1 && iCallback == null) {
      // ASYNCHRONOUS MODE NO ANSWER
      iMode0 = 2;
    }

    final OPhysicalPosition ppos = new OPhysicalPosition(iRecordType);

    OChannelBinaryAsynchClient lastNetworkUsed = null;
    do {
      try {
        final OChannelBinaryAsynchClient network = beginRequest(OChannelBinaryProtocol.REQUEST_RECORD_CREATE);
        lastNetworkUsed = network;

        try {
          network.writeShort((short) iRid.clusterId);
          network.writeBytes(iContent);
          network.writeByte(iRecordType);
          network.writeByte((byte) iMode0);

        } finally {
          endRequest(network);
        }

        switch (iMode0) {
        case 0:
          // SYNCHRONOUS
          try {
            beginResponse(network);
            if (network.getSrvProtocolVersion() > OChannelBinaryProtocol.PROTOCOL_VERSION_25) {
              iRid.clusterId = network.readShort();
            }

            iRid.clusterPosition = network.readLong();
            ppos.clusterPosition = iRid.clusterPosition;
            if (network.getSrvProtocolVersion() >= 11) {
              ppos.recordVersion = network.readVersion();
            } else {
              ppos.recordVersion = OVersionFactory.instance().createVersion();
            }

            if (network.getSrvProtocolVersion() >= 20) {
              readCollectionChanges(network, ODatabaseRecordThreadLocal.INSTANCE.get().getSbTreeCollectionManager());
            }

            return new OStorageOperationResult<OPhysicalPosition>(ppos);
          } finally {
            endResponse(network);
          }

        case 1:
          // ASYNCHRONOUS
          if (iCallback != null) {
            final int sessionId = getSessionId();
            final byte[] token = getSessionToken();
            final OSBTreeCollectionManager collectionManager = ODatabaseRecordThreadLocal.INSTANCE.get()
                .getSbTreeCollectionManager();
            Callable<Object> response = new Callable<Object>() {
            @Override
              public Object call() throws Exception {
                final long result;

                try {
                  OStorageRemoteThreadLocal.INSTANCE.get().sessionId = sessionId;
                  OStorageRemoteThreadLocal.INSTANCE.get().token = token;
                  beginResponse(network);
                  if (network.getSrvProtocolVersion() > OChannelBinaryProtocol.PROTOCOL_VERSION_25) {
                    iRid.clusterId = network.readShort();
                  }
                  result = network.readLong();
                  if (network.getSrvProtocolVersion() >= 11) {
                    network.readVersion();
                  }

                  if (network.getSrvProtocolVersion() >= 20) {
                    readCollectionChanges(network, collectionManager);
                  }
                } catch (IOException e) {
                  OLogManager.instance().error(this, "Exception on async query", e);
                  throw e;
                } finally {
                  endResponse(network);
                  OStorageRemoteThreadLocal.INSTANCE.get().sessionId = -1;
                  OStorageRemoteThreadLocal.INSTANCE.get().token = null;
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

      } catch (OModificationOperationProhibitedException mope) {
        handleDBFreeze();
      } catch (IOException e) {
        handleException(lastNetworkUsed, "Error on create record in cluster: " + iRid.clusterId, e);

      }
    } while (true);
  }

  @Override
  public ORecordMetadata getRecordMetadata(final ORID rid) {

    OChannelBinaryAsynchClient network = null;
    do {
      try {
        try {
          network = beginRequest(OChannelBinaryProtocol.REQUEST_RECORD_METADATA);
          network.writeRID(rid);
        } finally {
          endRequest(network);
        }
        if(network == null) {
          throw new IllegalStateException("network is null");
        }

        try {
          beginResponse(network);
          final ORID responseRid = network.readRID();
          final ORecordVersion responseVersion = network.readVersion();

          return new ORecordMetadata(responseRid, responseVersion);
        } finally {
          endResponse(network);
        }
      } catch (IOException e) {
        handleException(network, "Error on read record " + rid, e);
      }
    } while (true);
  }

  @Override
  public OStorageOperationResult<ORawBuffer> readRecord(final ORecordId iRid, final String iFetchPlan, final boolean iIgnoreCache,
      final ORecordCallback<ORawBuffer> iCallback, boolean loadTombstones, LOCKING_STRATEGY iLockingStrategy) {

    if (OStorageRemoteThreadLocal.INSTANCE.get().commandExecuting) {
      // PENDING NETWORK OPERATION, CAN'T EXECUTE IT NOW
      return new OStorageOperationResult<ORawBuffer>(null);
    }

    OChannelBinaryAsynchClient network = null;
    do {
      try {

        try {
          network = beginRequest(OChannelBinaryProtocol.REQUEST_RECORD_LOAD);
          network.writeRID(iRid);
          network.writeString(iFetchPlan != null ? iFetchPlan : "");
          if (network.getSrvProtocolVersion() >= 9) {
            network.writeByte((byte) (iIgnoreCache ? 1 : 0));
          }

          if (network.getSrvProtocolVersion() >= 13) {
            network.writeByte(loadTombstones ? (byte) 1 : (byte) 0);
          }
        } finally {
          endRequest(network);
        }
        if(network == null) {
          throw new IllegalStateException("network is null");
        }

        try {
          beginResponse(network);

          if (network.readByte() == 0) {
            return new OStorageOperationResult<ORawBuffer>(null);
          }

          final ORawBuffer buffer;
          if (network.getSrvProtocolVersion() <= 27) {
            buffer = new ORawBuffer(network.readBytes(), network.readVersion(), network.readByte());
          } else {
            byte type = network.readByte();
            ORecordVersion recVersion = network.readVersion();
            byte[] bytes = network.readBytes();
            buffer = new ORawBuffer(bytes, recVersion, type);
          }

          final ODatabaseDocument database = ODatabaseRecordThreadLocal.INSTANCE.getIfDefined();
          ORecord record;
          while (network.readByte() == 2) {
            record = (ORecord) OChannelBinaryProtocol.readIdentifiable(network);

            if (database != null) {
              // PUT IN THE CLIENT LOCAL CACHE
              database.getLocalCache().updateRecord(record);
            }
          }
          return new OStorageOperationResult<ORawBuffer>(buffer);

        } finally {
          endResponse(network);
        }

      } catch (IOException e) {
        handleException(network, "Error on read record " + iRid, e);

      }
    } while (true);
  }

  @Override
  public OStorageOperationResult<ORecordVersion> updateRecord(final ORecordId iRid, boolean updateContent, final byte[] iContent,
      final ORecordVersion iVersion, final byte iRecordType, int iMode, final ORecordCallback<ORecordVersion> iCallback) {
    int iMode0 = iMode;
    if (iMode0 == 1 && iCallback == null) {
      // ASYNCHRONOUS MODE NO ANSWER
      iMode0 = 2;
    }

    OChannelBinaryAsynchClient lastNetworkUsed = null;
    do {
      try {
        final OChannelBinaryAsynchClient network = beginRequest(OChannelBinaryProtocol.REQUEST_RECORD_UPDATE);
        lastNetworkUsed = network;

        try {
          network.writeRID(iRid);
          if (network.getSrvProtocolVersion() >= 23) {
            network.writeBoolean(updateContent);
          }
          network.writeBytes(iContent);
          network.writeVersion(iVersion);
          network.writeByte(iRecordType);
          network.writeByte((byte) iMode0);

        } finally {
          endRequest(network);
        }

        switch (iMode0) {
        case 0:
          // SYNCHRONOUS
          try {
            beginResponse(network);
            OStorageOperationResult<ORecordVersion> r = new OStorageOperationResult<ORecordVersion>(network.readVersion());
            readCollectionChanges(network, ODatabaseRecordThreadLocal.INSTANCE.get().getSbTreeCollectionManager());
            return r;
          } finally {
            endResponse(network);
          }

        case 1:
          // ASYNCHRONOUS
          final int sessionId = getSessionId();
          final OSBTreeCollectionManager collectionManager = ODatabaseRecordThreadLocal.INSTANCE.get().getSbTreeCollectionManager();
          Callable<Object> response = new Callable<Object>() {
          @Override
            public Object call() throws Exception {
              ORecordVersion result;

              try {
                OStorageRemoteThreadLocal.INSTANCE.get().sessionId = sessionId;
                beginResponse(network);
                result = network.readVersion();

                if (network.getSrvProtocolVersion() >= 20) {
                  readCollectionChanges(network, collectionManager);
                }
              } finally {
                endResponse(network);
                OStorageRemoteThreadLocal.INSTANCE.get().sessionId = -1;
              }

              iCallback.call(iRid, result);
              return null;
            }

          };
          asynchExecutor.submit(new FutureTask<Object>(response));
        }
        return new OStorageOperationResult<ORecordVersion>(iVersion);

      } catch (OModificationOperationProhibitedException mope) {
        handleDBFreeze();
      } catch (IOException e) {
        handleException(lastNetworkUsed, "Error on update record " + iRid, e);

      }
    } while (true);
  }

  @Override
  public OStorageOperationResult<Boolean> deleteRecord(final ORecordId iRid, final ORecordVersion iVersion, int iMode,
      final ORecordCallback<Boolean> iCallback) {
    int iMode0 = iMode;
    if (iMode0 == 1 && iCallback == null) {
      // ASYNCHRONOUS MODE NO ANSWER
      iMode0 = 2;
    }

    OChannelBinaryAsynchClient network = null;
    do {
      try {
        network = beginRequest(OChannelBinaryProtocol.REQUEST_RECORD_DELETE);
        return new OStorageOperationResult<Boolean>(deleteRecord(iRid, iVersion, iMode0, iCallback, network));
      } catch (OModificationOperationProhibitedException mope) {
        handleDBFreeze();
      } catch (IOException e) {
        handleException(network, "Error on delete record " + iRid, e);

      }
    } while (true);
  }

  @Override
  public OStorageOperationResult<Boolean> hideRecord(ORecordId recordId, int mode, ORecordCallback<Boolean> callback) {
    int mode0 = mode;
    if (mode0 == 1 && callback == null) {
      // ASYNCHRONOUS MODE NO ANSWER
      mode0 = 2;
    }

    OChannelBinaryAsynchClient network = null;
    do {
      try {
        network = beginRequest(OChannelBinaryProtocol.REQUEST_RECORD_HIDE);
        return new OStorageOperationResult<Boolean>(hideRecord(recordId, mode0, callback, network));
      } catch (OModificationOperationProhibitedException mope) {
        handleDBFreeze();
      } catch (IOException e) {
        handleException(network, "Error on delete record " + recordId, e);

      }
    } while (true);
  }

  @Override
  public boolean cleanOutRecord(ORecordId recordId, ORecordVersion recordVersion, int iMode, ORecordCallback<Boolean> callback) {
    int iMode0 = iMode;
    if (iMode0 == 1 && callback == null) {
      // ASYNCHRONOUS MODE NO ANSWER
      iMode0 = 2;
    }

    OChannelBinaryAsynchClient network = null;
    do {
      try {
        network = beginRequest(OChannelBinaryProtocol.REQUEST_RECORD_CLEAN_OUT);
        return deleteRecord(recordId, recordVersion, iMode0, callback, network);
      } catch (OModificationOperationProhibitedException mope) {
        handleDBFreeze();
      } catch (IOException e) {
        handleException(network, "Error on clean out record " + recordId, e);

      }
    } while (true);
  }

  @Override
  public void backup(OutputStream out, Map<String, Object> options, Callable<Object> callable,
      final OCommandOutputListener iListener, int compressionLevel, int bufferSize) throws IOException {
    throw new UnsupportedOperationException("backup");
  }

  @Override
  public void restore(InputStream in, Map<String, Object> options, Callable<Object> callable, final OCommandOutputListener iListener)
      throws IOException {
    throw new UnsupportedOperationException("restore");
  }

  @Override
  public long count(final int iClusterId) {
    return count(new int[] { iClusterId });
  }

  @Override
  public long count(int iClusterId, boolean countTombstones) {
    return count(new int[] { iClusterId }, countTombstones);
  }

  @Override
  public long[] getClusterDataRange(final int iClusterId) {

    OChannelBinaryAsynchClient network = null;
    do {
      try {
        try {
          network = beginRequest(OChannelBinaryProtocol.REQUEST_DATACLUSTER_DATARANGE);

          network.writeShort((short) iClusterId);

        } finally {
          endRequest(network);
        }
        if(network == null) {
          throw new IllegalStateException("network is null");
        }

        try {
          beginResponse(network);
          return new long[] { network.readLong(), network.readLong() };
        } finally {
          endResponse(network);
        }

      } catch (IOException e) {
        handleException(network, "Error on getting last entry position count in cluster: " + iClusterId, e);

      }
    } while (true);
  }

  @Override
  public OPhysicalPosition[] higherPhysicalPositions(int iClusterId, OPhysicalPosition iClusterPosition) {

    OChannelBinaryAsynchClient network = null;
    do {
      try {

        try {
          network = beginRequest(OChannelBinaryProtocol.REQUEST_POSITIONS_HIGHER);
          network.writeInt(iClusterId);
          network.writeLong(iClusterPosition.clusterPosition);

        } finally {
          endRequest(network);
        }
        if(network == null) {
          throw new IllegalStateException("network is null");
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

      } catch (IOException e) {
        handleException(network, "Error on retrieving higher positions after " + iClusterPosition.clusterPosition, e);
      }
    } while (true);
  }

  @Override
  public OPhysicalPosition[] ceilingPhysicalPositions(int clusterId, OPhysicalPosition physicalPosition) {

    OChannelBinaryAsynchClient network = null;
    do {
      try {

        try {
          network = beginRequest(OChannelBinaryProtocol.REQUEST_POSITIONS_CEILING);
          network.writeInt(clusterId);
          network.writeLong(physicalPosition.clusterPosition);

        } finally {
          endRequest(network);
        }
        if(network == null) {
          throw new IllegalStateException("network is null");
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

      } catch (IOException e) {
        handleException(network, "Error on retrieving ceiling positions after " + physicalPosition.clusterPosition, e);
      }
    } while (true);
  }

  @Override
  public OPhysicalPosition[] lowerPhysicalPositions(int iClusterId, OPhysicalPosition physicalPosition) {

    OChannelBinaryAsynchClient network = null;
    do {
      try {

        try {
          network = beginRequest(OChannelBinaryProtocol.REQUEST_POSITIONS_LOWER);
          network.writeInt(iClusterId);
          network.writeLong(physicalPosition.clusterPosition);

        } finally {
          endRequest(network);
        }
        if(network == null) {
          throw new IllegalStateException("network is null");
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

      } catch (IOException e) {
        handleException(network, "Error on retrieving lower positions after " + physicalPosition.clusterPosition, e);

      }
    } while (true);
  }

  @Override
  public OPhysicalPosition[] floorPhysicalPositions(int clusterId, OPhysicalPosition physicalPosition) {

    OChannelBinaryAsynchClient network = null;
    do {
      try {

        try {
          network = beginRequest(OChannelBinaryProtocol.REQUEST_POSITIONS_FLOOR);
          network.writeInt(clusterId);
          network.writeLong(physicalPosition.clusterPosition);

        } finally {
          endRequest(network);
        }
        if(network == null) {
          throw new IllegalStateException("network is null");
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

      } catch (IOException e) {
        handleException(network, "Error on retrieving floor positions after " + physicalPosition.clusterPosition, e);
      }
    } while (true);
  }

  @Override
  public long getSize() {

    OChannelBinaryAsynchClient network = null;
    do {
      try {
        try {

          network = beginRequest(OChannelBinaryProtocol.REQUEST_DB_SIZE);

        } finally {
          endRequest(network);
        }
        if(network == null) {
          throw new IllegalStateException("network is null");
        }

        try {
          beginResponse(network);
          return network.readLong();
        } finally {
          endResponse(network);
        }

      } catch (IOException e) {
        handleException(network, "Error on read database size", e);

      }
    } while (true);
  }

  @Override
  public long countRecords() {

    OChannelBinaryAsynchClient network = null;
    do {
      try {
        try {

          network = beginRequest(OChannelBinaryProtocol.REQUEST_DB_COUNTRECORDS);

        } finally {
          endRequest(network);
        }
        if(network == null) {
          throw new IllegalStateException("network is null");
        }

        try {
          beginResponse(network);
          return network.readLong();
        } finally {
          endResponse(network);
        }

      } catch (IOException e) {
        handleException(network, "Error on read database record count", e);

      }
    } while (true);
  }

  @Override
  public long count(final int[] iClusterIds) {
    return count(iClusterIds, false);
  }

  @Override
  public long count(final int[] iClusterIds, boolean countTombstones) {

    OChannelBinaryAsynchClient network = null;
    do {
      try {
        try {
          network = beginRequest(OChannelBinaryProtocol.REQUEST_DATACLUSTER_COUNT);

          network.writeShort((short) iClusterIds.length);
          for (int iClusterId : iClusterIds) {
            network.writeShort((short) iClusterId);
          }

          if (network.getSrvProtocolVersion() >= 13) {
            network.writeByte(countTombstones ? (byte) 1 : (byte) 0);
          }
        } finally {
          endRequest(network);
        }
        if(network == null) {
          throw new IllegalStateException("network is null");
        }

        try {
          beginResponse(network);
          return network.readLong();
        } finally {
          endResponse(network);
        }

      } catch (IOException e) {
        handleException(network, "Error on read record count in clusters: " + Arrays.toString(iClusterIds), e);

      }
    } while (true);
  }

  /**
   * Execute the command remotely and get the results back.
   * @param iCommand
   * @return
   */
  @Override
  @SuppressWarnings("NestedAssignment")
  public Object command(final OCommandRequestText iCommand) {

    if (!(iCommand instanceof OSerializableStream)) {
      throw new OCommandExecutionException("Cannot serialize the command to be executed to the server side.");
    }

    Object result = null;

    final ODatabaseDocument database = ODatabaseRecordThreadLocal.INSTANCE.get();
    try {
      OChannelBinaryAsynchClient network = null;
      do {

        OStorageRemoteThreadLocal.INSTANCE.get().commandExecuting = true;
        try {
          final boolean asynch = iCommand instanceof OCommandRequestAsynch && ((OCommandRequestAsynch) iCommand).isAsynchronous();

          try {
            network = beginRequest(OChannelBinaryProtocol.REQUEST_COMMAND);

            network.writeByte((byte) (asynch ? 'a' : 's')); // ASYNC / SYNC
            network.writeBytes(OStreamSerializerAnyStreamable.INSTANCE.toStream(iCommand));

          } finally {
            endRequest(network);
          }
          if(network == null) {
            throw new IllegalStateException("network is null");
          }

          try {
            beginResponse(network);

            boolean addNextRecord = true;
            if (asynch) {
              byte status0;

              // ASYNCH: READ ONE RECORD AT TIME
              while ((status0 = network.readByte()) > 0) {
                final ORecord record = (ORecord) OChannelBinaryProtocol.readIdentifiable(network);
                if (record == null) {
                  continue;
                }

                switch (status0) {
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
              final byte type = network.readByte();
              switch (type) {
              case 'n':
                result = null;
                break;

              case 'r':
                result = OChannelBinaryProtocol.readIdentifiable(network);
                if (result instanceof ORecord) {
                  database.getLocalCache().updateRecord((ORecord) result);
              }
                break;

              case 'l':
                final int tot = network.readInt();
                final Collection<OIdentifiable> list = new ArrayList<OIdentifiable>(tot);
                for (int i = 0; i < tot; ++i) {
                  final OIdentifiable resultItem = OChannelBinaryProtocol.readIdentifiable(network);
                  if (resultItem instanceof ORecord) {
                    database.getLocalCache().updateRecord((ORecord) resultItem);
                  }
                  list.add(resultItem);
                }
                result = list;
                break;

              case 'a':
                final String value = new String(network.readBytes());
                result = ORecordSerializerStringAbstract.fieldTypeFromStream(null, ORecordSerializerStringAbstract.getType(value),
                    value);
                break;

              default:
                OLogManager.instance().warn(this, "Received unexpected result from query: %d", type);
              }

              if (network.getSrvProtocolVersion() >= 17) {
                // LOAD THE FETCHED RECORDS IN CACHE
                byte status0;
                while ((status0 = network.readByte()) > 0) {
                  final ORecord record = (ORecord) OChannelBinaryProtocol.readIdentifiable(network);
                  if (record != null && status0 == 2) {
                    // PUT IN THE CLIENT LOCAL CACHE
                    database.getLocalCache().updateRecord(record);
                  }
                }
              }
            }
            break;
          } finally {
            endResponse(network);
          }
        } catch (OModificationOperationProhibitedException mope) {
          handleDBFreeze();
        } catch (IOException e) {
          handleException(network, "Error on executing command: " + iCommand, e);

        } finally {
          OStorageRemoteThreadLocal.INSTANCE.get().commandExecuting = false;
        }
      } while (true);
    } finally {
      if (iCommand.getResultListener() != null) {
        iCommand.getResultListener().end();
      }
    }

    return result;
  }

  @Override
  public void commit(final OTransaction iTx, Runnable callback) {

    final List<ORecordOperation> committedEntries = new ArrayList<ORecordOperation>();
    OChannelBinaryAsynchClient network = null;
    do {
      try {
        OStorageRemoteThreadLocal.INSTANCE.get().commandExecuting = true;

        try {
          network = beginRequest(OChannelBinaryProtocol.REQUEST_TX_COMMIT);
          if(network == null) {
            throw new IllegalStateException("network is null");
          }

          network.writeInt(iTx.getId());
          network.writeByte((byte) (iTx.isUsingLog() ? 1 : 0));

          final List<ORecordOperation> tmpEntries = new ArrayList<ORecordOperation>();

          if (iTx.getCurrentRecordEntries().iterator().hasNext()) {
            for (ORecordOperation txEntry : iTx.getCurrentRecordEntries()) {
              committedEntries.add(txEntry);
            }
            while (iTx.getCurrentRecordEntries().iterator().hasNext()) {
              for (ORecordOperation txEntry : iTx.getCurrentRecordEntries()) {
                tmpEntries.add(txEntry);
              }

              iTx.clearRecordEntries();

              if (tmpEntries.size() > 0) {
                for (ORecordOperation txEntry : tmpEntries) {
                  commitEntry(network, txEntry);
                }
                tmpEntries.clear();
              }
            }
          } else if (committedEntries.size() > 0) {
            tmpEntries.addAll(committedEntries);
            while (!tmpEntries.isEmpty()) {
              iTx.clearRecordEntries();
              for (ORecordOperation txEntry : tmpEntries) {
                ORecordInternal.clearSource(txEntry.getRecord());
                commitEntry(network, txEntry);
              }
              tmpEntries.clear();
              for (ORecordOperation txEntry : iTx.getCurrentRecordEntries()) {
                tmpEntries.add(txEntry);
              }
            }
          }

          // END OF RECORD ENTRIES
          network.writeByte((byte) 0);

          // SEND INDEX ENTRIES
          network.writeBytes(iTx.getIndexChanges().toStream());
        } finally {
          endRequest(network);
        }
        if(network == null) {
          throw new IllegalStateException("network is null");
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
            if (rop != null) {
              rop.getRecord().getRecordVersion().copyFrom(network.readVersion());
            }
          }

          if (network.getSrvProtocolVersion() >= 20) {
            readCollectionChanges(network, ODatabaseRecordThreadLocal.INSTANCE.get().getSbTreeCollectionManager());
          }

        } finally {
          endResponse(network);
        }

        committedEntries.clear();
        // SET ALL THE RECORDS AS UNDIRTY
        for (ORecordOperation txEntry : iTx.getAllRecordEntries()) {
          ORecordInternal.unsetDirty(txEntry.getRecord());
        }

        // UPDATE THE CACHE ONLY IF THE ITERATOR ALLOWS IT. USE THE STRATEGY TO ALWAYS REMOVE ALL THE RECORDS SINCE THEY COULD BE
        // CHANGED AS CONTENT IN CASE OF TREE AND GRAPH DUE TO CROSS REFERENCES
        OTransactionAbstract.updateCacheFromEntries(iTx, iTx.getAllRecordEntries(), false);

        break;

      } catch (OModificationOperationProhibitedException mope) {
        handleDBFreeze();
      } catch (IOException e) {
        handleException(network, "Error on commit", e);

      } finally {
        OStorageRemoteThreadLocal.INSTANCE.get().commandExecuting = false;

      }
    } while (true);
  }

  @Override
  public void rollback(OTransaction iTx) {
  }

  @Override
  public int getClusterIdByName(final String iClusterName) {
    lock.acquireSharedLock();
    try {

      if (iClusterName == null) {
        return -1;
      }

      if (Character.isDigit(iClusterName.charAt(0))) {
        return Integer.parseInt(iClusterName);
      }

      final OCluster cluster = clusterMap.get(iClusterName.toLowerCase());
      if (cluster == null) {
        return -1;
      }

      return cluster.getId();
    } finally {
      lock.releaseSharedLock();
    }
  }

  @Override
  public int getDefaultClusterId() {
    return defaultClusterId;
  }

  @Override
  public void setDefaultClusterId(int defaultClusterId) {
    this.defaultClusterId = defaultClusterId;
  }

  @Override
  public int addCluster(final String iClusterName, boolean forceListBased, final Object... iArguments) {
    return addCluster(iClusterName, -1, forceListBased, iArguments);
  }

  @Override
  public int addCluster(String iClusterName, int iRequestedId, boolean forceListBased, Object... iParameters) {

    OChannelBinaryAsynchClient network = null;
    do {
      lock.acquireExclusiveLock();
      try {
        try {
          network = beginRequest(OChannelBinaryProtocol.REQUEST_DATACLUSTER_ADD);

          network.writeString(iClusterName);
          if (network.getSrvProtocolVersion() >= 18) {
            network.writeShort((short) iRequestedId);
          }
        } finally {
          endRequest(network);
        }
        if(network == null) {
          throw new IllegalStateException("network is null");
        }

        try {
          beginResponse(network);
          final int clusterId = network.readShort();

          final OClusterRemote cluster = new OClusterRemote();
          cluster.configure(this, clusterId, iClusterName.toLowerCase());

          if (clusters.length <= clusterId) {
            clusters = Arrays.copyOf(clusters, clusterId + 1);
          }
          clusters[cluster.getId()] = cluster;
          clusterMap.put(cluster.getName().toLowerCase(), cluster);

          return clusterId;
        } finally {
          endResponse(network);
        }
      } catch (OModificationOperationProhibitedException mphe) {
        handleDBFreeze();
      } catch (IOException e) {
        handleException(network, "Error on add new cluster", e);
      } finally {
        lock.releaseExclusiveLock();
      }
    } while (true);
  }

  @Override
  public boolean dropCluster(final int iClusterId, final boolean iTruncate) {

    OChannelBinaryAsynchClient network = null;
    do {
      lock.acquireExclusiveLock();
      try {
        try {
          network = beginRequest(OChannelBinaryProtocol.REQUEST_DATACLUSTER_DROP);

          network.writeShort((short) iClusterId);

        } finally {
          endRequest(network);
        }
        if(network == null) {
          throw new IllegalStateException("network is null");
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
          if (configuration.clusters.size() > iClusterId) {
            configuration.dropCluster(iClusterId); // endResponse must be called before this line, which call updateRecord
          }

          return true;
        }
        return false;

      } catch (OModificationOperationProhibitedException mope) {
        handleDBFreeze();
      } catch (IOException e) {
        handleException(network, "Error on removing of cluster", e);

      } finally {
        lock.releaseExclusiveLock();
      }
    } while (true);
  }

  @Override
  public void synch() {
  }

  @Override
  public String getPhysicalClusterNameById(final int iClusterId) {
    lock.acquireSharedLock();
    try {

      if (iClusterId >= clusters.length) {
        return null;
      }

      final OCluster cluster = clusters[iClusterId];
      return cluster != null ? cluster.getName() : null;

    } finally {
      lock.releaseSharedLock();
    }
  }

  public int getClusterMap() {
    lock.acquireSharedLock();
    try {
      return clusterMap.size();
    } finally {
      lock.releaseSharedLock();
    }
  }

  @Override
  public Collection<OCluster> getClusterInstances() {
    lock.acquireSharedLock();
    try {

      return Arrays.asList(clusters);

    } finally {
      lock.releaseSharedLock();
    }
  }

  @Override
  public OCluster getClusterById(int iClusterId) {
    int iClusterId0 = iClusterId;
    lock.acquireSharedLock();
    try {

      if (iClusterId0 == ORID.CLUSTER_ID_INVALID) {
        // GET THE DEFAULT CLUSTER
        iClusterId0 = defaultClusterId;
      }

      return clusters[iClusterId0];

    } finally {
      lock.releaseSharedLock();
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
   * @param iNetwork
   * @throws java.io.IOException
   */
  public void endRequest(final OChannelBinaryAsynchClient iNetwork) throws IOException {
    if (iNetwork == null) {
      return;
    }

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
   * @param iNetwork
   */
  public void endResponse(final OChannelBinaryAsynchClient iNetwork) {
    iNetwork.endResponse();
    engine.getConnectionManager().release(iNetwork);
  }

  public boolean isPermanentRequester() {
    return false;
  }

  @SuppressWarnings("unchecked")
  public void updateClusterConfiguration(final String iConnectedURL, final byte[] obj) {
    if (obj == null) {
      return;
    }

    // UPDATE IT
    synchronized (clusterConfiguration) {
      clusterConfiguration.fromStream(obj);

      clusterConfiguration.toString();

      final List<ODocument> members = clusterConfiguration.field("members");
      if (members != null) {
        serverURLs.clear();

        // ADD CURRENT SERVER AS FIRST
        addHost(iConnectedURL);

        // parseServerURLs();

        for (ODocument m : members) {
          String serverURL = m.field("name");
          if (!serverURLs.contains(serverURL)) {
            final Collection<Map<String, Object>> listeners = ((Collection<Map<String, Object>>) m.field("listeners"));
            if (listeners == null) {
              throw new ODatabaseException("Received bad distributed configuration: missing 'listeners' array field");
            }

            for (Map<String, Object> listener : listeners) {
              if (listener.get("protocol").equals("ONetworkProtocolBinary")) {
                String url0 = (String) listener.get("listen");
                if (!serverURLs.contains(url0)) {
                  addHost(url0);
                }
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

  public String getClientId() {
    return clientId;
  }

  @Override
  public int getClusters() {
    lock.acquireSharedLock();
    try {
      return clusterMap.size();
    } finally {
      lock.releaseSharedLock();
    }
  }

  @Override
  public String getType() {
    return OEngineRemote.NAME;
  }

  @Override
  public Class<OSBTreeCollectionManagerRemote> getCollectionManagerClass() {
    return OSBTreeCollectionManagerRemote.class;
  }

  public OEngineRemote getEngine() {
    return engine;
  }

  @Override
  public String getUserName() {
    return connectionUserName;
  }

  /**
   * Handles exceptions. In case of IO errors retries to reconnect until the configured retry times has reached.
   * 
   * @param iNetwork
   * @param message
   *          the detail message
   * @param exception
   *          cause of the error
   */
  @SuppressWarnings("SleepWhileInLoop")
  protected void handleException(final OChannelBinaryAsynchClient iNetwork, final String message, Exception exception) {
    Exception exception0 = exception;
    Exception originalException = exception;
    if (exception0 instanceof OIOException) {
      // BYPASS IT TO HANDLE RE-CONNECT
      exception0 = (Exception) exception0.getCause();
    } else if (exception0 instanceof OException) {
      // Release on concurrent modification exception created some issue. to double check
      if (iNetwork != null) {
        engine.getConnectionManager().release(iNetwork);
      }

      // RE-THROW IT
      throw (RuntimeException) exception0;
    } else if (!(exception0 instanceof IOException)) {
      if (iNetwork != null) {
        engine.getConnectionManager().release(iNetwork);
      }
      throw new OStorageException(message, exception0);
    }

    if (status != STATUS.OPEN) {
      // STORAGE CLOSED: DON'T HANDLE RECONNECTION
      return;
    }

    if (iNetwork != null) {
      OLogManager.instance().warn(this, "Caught I/O errors from %s (local socket=%s), trying to reconnect (error: %s)", iNetwork,
          iNetwork.getLocalSocketAddress(), exception0 == null ? originalException : exception0);
      OLogManager.instance().debug(this, "I/O error stack: ", exception0 == null ? originalException : exception0);

      try {
        engine.getConnectionManager().remove(iNetwork);
      } catch (Exception e) {
        // IGNORE ANY EXCEPTION
      }
    } else {
      OLogManager.instance().warn(this, "Caught I/O errors, trying to reconnect (error: %s)",
          exception0 == null ? originalException.toString() : exception0.toString());
      OLogManager.instance().debug(this, "I/O error stack: ", exception0 == null ? originalException : exception0);
    }

    final long lostConnectionTime = System.currentTimeMillis();

    final int currentMaxRetry;
    final int currentRetryDelay;
    if (serverURLs.size() > 1) {
      // IN CLUSTER: NO RETRY AND 0 SLEEP TIME BETWEEN NODES
      currentMaxRetry = 1;
      currentRetryDelay = 0;
    } else {
      currentMaxRetry = connectionRetry;
      currentRetryDelay = connectionRetryDelay;
    }

    for (int retry = 0; retry < currentMaxRetry; ++retry) {
      // WAIT THE DELAY BEFORE TO RETRY (BUT FIRST TRY)
      if (retry > 0 && currentRetryDelay > 0) {
        try {
          Thread.sleep(currentRetryDelay);
        } catch (InterruptedException e) {
          // THREAD INTERRUPTED: RETURN EXCEPTION
          Thread.currentThread().interrupt();
          break;
        }
      }

      try {
        if (OLogManager.instance().isDebugEnabled()) {
          OLogManager.instance()
                  .debug(this, "Retrying to connect to remote server #" + (retry + 1) + "/" + currentMaxRetry + "...");
        }

        // FORCE RESET OF THREAD DATA (SERVER URL + SESSION ID)
        setSessionId(null, -1, null);

        // REACQUIRE DB SESSION ID
        final String currentURL = openRemoteDatabase();

        OLogManager
            .instance()
            .warn(
                this,
                "Connection re-acquired transparently after %dms and %d retries to server '%s': no errors will be thrown at application level",
                System.currentTimeMillis() - lostConnectionTime, retry + 1, currentURL);

        // RECONNECTED!
        return;

      } catch (IOException t) {
        // DO NOTHING BUT CONTINUE IN THE LOOP
      }
    }

    // RECONNECTION FAILED: THROW+LOG THE ORIGINAL EXCEPTION
    throw new OStorageException(message, exception0);
  }

  protected String openRemoteDatabase() throws IOException {
    connectionDbType = ODatabaseDocument.TYPE;

    if (connectionOptions != null && connectionOptions.size() > 0) {
      if (connectionOptions.containsKey(PARAM_DB_TYPE)) {
        connectionDbType = connectionOptions.get(PARAM_DB_TYPE).toString();
      }
    }

    OChannelBinaryAsynchClient network = null;
    String currentURL = getCurrentServerURL();
    do {
      do {
        try {
          clearToken();
          network = getAvailableNetwork(currentURL);
          try {
            network.writeByte(OChannelBinaryProtocol.REQUEST_DB_OPEN);
            network.writeInt(getSessionId());

            // @SINCE 1.0rc8
            sendClientInfo(network);

            network.writeString(name);

            if (network.getSrvProtocolVersion() >= 8) {
              network.writeString(connectionDbType);
            }

            network.writeString(connectionUserName);
            network.writeString(connectionUserPassword);

          } finally {
            endRequest(network);
          }

          final int sessionId;

          try {
            beginResponse(network);
            sessionId = network.readInt();
            byte[] token = network.readBytes();
            if (token.length == 0) {
              token = null;
            } else {
              network.getServiceThread().setTokenBased(true);
            }
            setSessionId(network.getServerURL(), sessionId, token);

            OLogManager.instance().debug(this, "Client connected to %s with session id=%d", network.getServerURL(), sessionId);

            readDatabaseInformation(network);

            // READ CLUSTER CONFIGURATION
            updateClusterConfiguration(network.getServerURL(), network.readBytes());

            // read OrientDB release info
            if (network.getSrvProtocolVersion() >= 14) {
              network.readString();
            }

            status = STATUS.OPEN;

            return currentURL;

          } finally {
            endResponse(network);
          }
        } catch (OIOException e) {
          if (network != null) {
            // REMOVE THE NETWORK CONNECTION IF ANY
            engine.getConnectionManager().remove(network);
            network = null;
          }
        } catch (OException e) {
          // PROPAGATE ANY OTHER ORIENTDB EXCEPTION
          throw e;

        } catch (IOException e) {
          if (network != null) {
            // REMOVE THE NETWORK CONNECTION IF ANY
            engine.getConnectionManager().remove(network);
            network = null;
          }
        }
      } while (engine.getConnectionManager().getAvailableConnections(currentURL) > 0);

      currentURL = useNewServerURL(currentURL);

    } while (currentURL != null);

    // REFILL ORIGINAL SERVER LIST
    parseServerURLs();

    throw new OStorageException("Cannot create a connection to remote server address(es): " + serverURLs);
  }

  protected String useNewServerURL(final String iUrl) {
    int pos = iUrl.indexOf('/');
    if (pos >= iUrl.length() - 1) {
      // IGNORE ENDING /
      pos = -1;
    }

    final String postFix = pos > -1 ? iUrl.substring(pos) : "";
    final String url0 = pos > -1 ? iUrl.substring(0, pos) : iUrl;

    // REMOVE INVALID URL
    serverURLs.remove(url0);

    OLogManager.instance().debug(this, "Updated server list: %s...", serverURLs);

    if (!serverURLs.isEmpty()) {
      return serverURLs.get(0) + postFix;
    }

    return null;
  }

  protected void sendClientInfo(OChannelBinaryAsynchClient network) throws IOException {
    if (network.getSrvProtocolVersion() >= 7) {
      // @COMPATIBILITY 1.0rc8
      network.writeString(DRIVER_NAME).writeString(OConstants.ORIENT_VERSION)
          .writeShort((short) OChannelBinaryProtocol.CURRENT_PROTOCOL_VERSION).writeString(clientId);
    }
    if (network.getSrvProtocolVersion() > OChannelBinaryProtocol.PROTOCOL_VERSION_21) {
      network.writeString(ODatabaseDocumentTx.getDefaultSerializer().toString());
      recordFormat = ODatabaseDocumentTx.getDefaultSerializer().toString();
    } else {
      recordFormat = ORecordSerializerSchemaAware2CSV.NAME;
    }
    if (network.getSrvProtocolVersion() > OChannelBinaryProtocol.PROTOCOL_VERSION_26) {
      network.writeBoolean(OGlobalConfiguration.CLIENT_SESSION_TOKEN_BASED.getValueAsBoolean());
    }
  }

  /**
   * Parse the URLs. Multiple URLs must be separated by semicolon (;)
   */
  @SuppressWarnings("UseOfObsoleteCollectionType")
  private void parseServerURLs() {
    String lastHost = null;
    int dbPos = url.indexOf('/');
    if (dbPos == -1) {
      // SHORT FORM
      addHost(url);
      lastHost = url;
      name = url;
    } else {
      name = url.substring(url.lastIndexOf('/') + 1);
      for (String host : url.substring(0, dbPos).split(ADDRESS_SEPARATOR)) {
        lastHost = host;
        addHost(host);
      }
    }
    if(lastHost == null) {
      throw new IllegalArgumentException("unable to retrieve host from url");
    }

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
        final String hostName = !primaryServer.contains(":") ? primaryServer : primaryServer.substring(0,
            primaryServer.indexOf(':'));
        final Attributes attrs = ictx.getAttributes(hostName, new String[] { "TXT" });
        final Attribute attr = attrs.get("TXT");
        if (attr != null) {
          for (int i = 0; i < attr.size(); ++i) {
            String configuration0 = (String) attr.get(i);
            if (configuration0.startsWith("\"")) {
              configuration0 = configuration0.substring(1, configuration0.length() - 1);
            }
            if (configuration0 != null) {
              serverURLs.clear();
              final String[] parts = configuration0.split(" ");
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

  /**
   * Registers the remote server with port.
   * @param host the host to add
   * @return the canonicalized host which has been added
   */
  protected String addHost(String host) {
    String host0 = host;
    if (host0.startsWith("localhost")) {
      host0 = "127.0.0.1" + host0.substring("localhost".length());
    }

    // REGISTER THE REMOTE SERVER+PORT
    if (!host0.contains(":")) {
      host0 += ":"
              + (clientConfiguration.getValueAsBoolean(OGlobalConfiguration.CLIENT_USE_SSL) ? sslPort : port);
    }

    if (host0.contains("/")) {
      host0 = host0.substring(0, host0.indexOf('/'));
    }

    if (!serverURLs.contains(host0)) {
      serverURLs.add(host0);
    }

    return host0;
  }

  protected String getDefaultHost() {
    return DEFAULT_HOST;
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
  protected OChannelBinaryAsynchClient beginRequest(final byte iCommand) throws IOException {
    final OChannelBinaryAsynchClient network = getAvailableNetwork(getCurrentServerURL());
    network.writeByte(iCommand);
    network.writeInt(getSessionId());
    byte[] token = getSessionToken();
    if (token != null) {
      network.writeBytes(token);
    }

    return network;
  }

  protected String getCurrentServerURL() {
    if (serverURLs.isEmpty()) {
      parseServerURLs();
      if (serverURLs.isEmpty()) {
        throw new OStorageException("Cannot create a connection to remote server because url list is empty");
      }
    }

    return serverURLs.get(0) + "/" + getName();
  }

  protected OChannelBinaryAsynchClient getAvailableNetwork(final String iCurrentURL) throws IOException {
    OChannelBinaryAsynchClient network;

    String lastURL = iCurrentURL;
    do {
      try {
        network = engine.getConnectionManager().acquire(lastURL, clientConfiguration, connectionOptions, asynchEventListener);
      } catch (Exception e) {
        // CATCH ANY EXCEPTION AND TRY WITH A NEXT ONE IF ANY
        network = null;
      }

      if (network == null) {
        lastURL = useNewServerURL(lastURL);
        if (lastURL == null) {
          parseServerURLs();
          throw new OIOException("Cannot open a connection to remote server: " + iCurrentURL);
        }
      } else if (!network.isConnected()) {
        // DISCONNECTED NETWORK, GET ANOTHER ONE
        OLogManager.instance().error(this, "Removing disconnected network channel '%s'...", lastURL);
        engine.getConnectionManager().remove(network);
        network = null;
      } else if (!network.tryLock()) {
        // CANNOT LOCK IT, MAYBE HASN'T BE CORRECTLY UNLOCKED BY PREVIOUS USER
        OLogManager.instance().error(this, "Removing locked network channel '%s'...", lastURL);
        engine.getConnectionManager().remove(network);
        network = null;
      }

    } while (network == null);
    return network;
  }

  /**
   * Starts listening the response.
   * @param iNetwork
   * @throws java.io.IOException
   */
  protected void beginResponse(final OChannelBinaryAsynchClient iNetwork) throws IOException {
    byte[] newToken = iNetwork.beginResponse(getSessionId(), getSessionToken() != null);
    if (newToken != null && newToken.length > 0) {
      setSessionId(getServerURL(), getSessionId(), newToken);
    }
  }

  protected void getResponse(final OChannelBinaryAsynchClient iNetwork) throws IOException {
    try {
      beginResponse(iNetwork);
    } finally {
      endResponse(iNetwork);
    }
  }

  private boolean hideRecord(final ORecordId rid, int mode, final ORecordCallback<Boolean> callback,
      final OChannelBinaryAsynchClient network) throws IOException {
    try {

      network.writeRID(rid);
      network.writeByte((byte) mode);

    } finally {
      endRequest(network);
    }

    switch (mode) {
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
      if (callback != null) {
        final int sessionId = getSessionId();
        Callable<Object> response = new Callable<Object>() {
          @Override
          public Object call() throws Exception {
            Boolean result;

            try {
              OStorageRemoteThreadLocal.INSTANCE.get().sessionId = sessionId;
              beginResponse(network);
              result = network.readByte() == 1;
            } finally {
              endResponse(network);
              OStorageRemoteThreadLocal.INSTANCE.get().sessionId = -1;
            }

            callback.call(rid, result);
            return null;
          }
        };
        asynchExecutor.submit(new FutureTask<Object>(response));
      }
    }
    return false;
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

      if (collectionManager != null) {
        collectionManager.updateCollectionPointer(new UUID(mBitsOfId, lBitsOfId), pointer);
      }
    }

    if (ORecordSerializationContext.getDepth() <= 1 && collectionManager != null) {
      collectionManager.clearPendingCollections();
    }
  }

  private void commitEntry(final OChannelBinaryAsynchClient iNetwork, final ORecordOperation txEntry) throws IOException {
    if (txEntry.type == ORecordOperation.LOADED) {
      // JUMP LOADED OBJECTS
      return;
    }

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
    } catch (OSerializationException e) {
      // ABORT TX COMMIT
      iNetwork.writeByte((byte) -1);
      throw new OTransactionException("Error on transaction commit", e);
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
      iNetwork.writeVersion(txEntry.getRecord().getRecordVersion());
      iNetwork.writeBytes(stream);
      if (iNetwork.getSrvProtocolVersion() >= 23) {
        iNetwork.writeBoolean(ORecordInternal.isContentChanged(txEntry.getRecord()));
      }
      break;

    case ORecordOperation.DELETED:
      iNetwork.writeVersion(txEntry.getRecord().getRecordVersion());
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

    clusters = new OCluster[tot];
    clusterMap.clear();

    for (int i = 0; i < tot; ++i) {
      final OClusterRemote cluster = new OClusterRemote();
      String clusterName = network.readString();
      if (clusterName != null) {
        clusterName = clusterName.toLowerCase();
      }
      final int clusterId = network.readShort();

      if (network.getSrvProtocolVersion() < 24) {
        network.readString();
      }

      final int dataSegmentId = network.getSrvProtocolVersion() >= 12 && network.getSrvProtocolVersion() < 24 ? network
              .readShort() : 0;

      cluster.configure(this, clusterId, clusterName);

      if (clusterId >= clusters.length) {
        clusters = Arrays.copyOf(clusters, clusterId + 1);
      }
      clusters[clusterId] = cluster;
      clusterMap.put(clusterName, cluster);
    }

    defaultClusterId = clusterMap.get(CLUSTER_DEFAULT_NAME).getId();
  }

  private boolean deleteRecord(final ORecordId iRid, ORecordVersion iVersion, int iMode, final ORecordCallback<Boolean> iCallback,
      final OChannelBinaryAsynchClient network) throws IOException {
    try {

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
        final int sessionId = getSessionId();
        Callable<Object> response = new Callable<Object>() {
          @Override
          public Object call() throws Exception {
            Boolean result;

            try {
              OStorageRemoteThreadLocal.INSTANCE.get().sessionId = sessionId;
              beginResponse(network);
              result = network.readByte() == 1;
            } finally {
              endResponse(network);
              OStorageRemoteThreadLocal.INSTANCE.get().sessionId = -1;
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
}
