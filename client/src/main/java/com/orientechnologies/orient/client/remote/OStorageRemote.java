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
package com.orientechnologies.orient.client.remote;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;

import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.common.concur.lock.OAdaptiveLock;
import com.orientechnologies.common.concur.lock.OLock;
import com.orientechnologies.common.concur.lock.OModificationOperationProhibitedException;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.io.OIOException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.client.remote.OStorageRemoteThreadLocal.OStorageRemoteSession;
import com.orientechnologies.orient.core.OConstants;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.cache.OCacheLevelTwoLocatorRemote;
import com.orientechnologies.orient.core.command.OCommandRequestAsynch;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.config.OStorageConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.exception.OTransactionException;
import com.orientechnologies.orient.core.id.OClusterPosition;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.OSerializableStream;
import com.orientechnologies.orient.core.serialization.serializer.record.string.ORecordSerializerStringAbstract;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializerAnyStreamable;
import com.orientechnologies.orient.core.storage.OCluster;
import com.orientechnologies.orient.core.storage.ODataSegment;
import com.orientechnologies.orient.core.storage.OPhysicalPosition;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import com.orientechnologies.orient.core.storage.ORecordCallback;
import com.orientechnologies.orient.core.storage.ORecordMetadata;
import com.orientechnologies.orient.core.storage.OStorageAbstract;
import com.orientechnologies.orient.core.storage.OStorageOperationResult;
import com.orientechnologies.orient.core.storage.OStorageProxy;
import com.orientechnologies.orient.core.tx.OTransaction;
import com.orientechnologies.orient.core.tx.OTransactionAbstract;
import com.orientechnologies.orient.core.version.ORecordVersion;
import com.orientechnologies.orient.core.version.OVersionFactory;
import com.orientechnologies.orient.enterprise.channel.OChannel;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryClient;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelListener;
import com.orientechnologies.orient.enterprise.channel.binary.ONetworkProtocolException;
import com.orientechnologies.orient.enterprise.channel.binary.ORemoteServerEventListener;

/**
 * This object is bound to each remote ODatabase instances.
 */
public class OStorageRemote extends OStorageAbstract implements OStorageProxy, OChannelListener {
  private static final String              DEFAULT_HOST         = "localhost";
  private static final int                 DEFAULT_PORT         = 2424;
  private static final String              ADDRESS_SEPARATOR    = ";";

  public static final String               PARAM_MIN_POOL       = "minpool";
  public static final String               PARAM_MAX_POOL       = "maxpool";
  public static final String               PARAM_DB_TYPE        = "dbtype";

  private static final String              DRIVER_NAME          = "OrientDB Java";

  private final ExecutorService            asynchExecutor;
  private OContextConfiguration            clientConfiguration;
  private int                              connectionRetry;
  private int                              connectionRetryDelay;

  private final List<OChannelBinaryClient> networkPool          = new ArrayList<OChannelBinaryClient>();
  private final OLock                      networkPoolLock      = new OAdaptiveLock();
  private int                              networkPoolCursor    = 0;

  protected final List<String>             serverURLs           = new ArrayList<String>();
  private OCluster[]                       clusters             = new OCluster[0];
  protected final Map<String, OCluster>    clusterMap           = new ConcurrentHashMap<String, OCluster>();
  private int                              defaultClusterId;
  private int                              minPool;
  private int                              maxPool;
  private final ODocument                  clusterConfiguration = new ODocument();
  private ORemoteServerEventListener       asynchEventListener;
  private String                           connectionDbType;
  private String                           connectionUserName;
  private String                           connectionUserPassword;
  private Map<String, Object>              connectionOptions;
  private final String                     clientId;
  private final int                        maxReadQueue;

  public OStorageRemote(final String iClientId, final String iURL, final String iMode) throws IOException {
    super(iURL, iURL, iMode, 0, new OCacheLevelTwoLocatorRemote()); // NO TIMEOUT @SINCE 1.5
    clientId = iClientId;
    configuration = null;

    clientConfiguration = new OContextConfiguration();
    connectionRetry = clientConfiguration.getValueAsInteger(OGlobalConfiguration.NETWORK_SOCKET_RETRY);
    connectionRetryDelay = clientConfiguration.getValueAsInteger(OGlobalConfiguration.NETWORK_SOCKET_RETRY_DELAY);
    asynchEventListener = new OStorageRemoteAsynchEventListener(this);
    parseServerURLs();

    asynchExecutor = Executors.newSingleThreadScheduledExecutor();

    maxReadQueue = Runtime.getRuntime().availableProcessors() - 1;
  }

  public int getSessionId() {
    return OStorageRemoteThreadLocal.INSTANCE.get().sessionId.intValue();
  }

  public String getServerURL() {
    return OStorageRemoteThreadLocal.INSTANCE.get().serverURL;
  }

  public void setSessionId(final String iServerURL, final int iSessionId) {
    final OStorageRemoteSession tl = OStorageRemoteThreadLocal.INSTANCE.get();
    tl.serverURL = iServerURL;
    tl.sessionId = iSessionId;
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

      configuration = new OStorageConfiguration(this);
      configuration.load();

    } catch (Exception e) {
      if (!OGlobalConfiguration.STORAGE_KEEP_OPEN.getValueAsBoolean())
        close();

      if (e instanceof RuntimeException)
        // PASS THROUGH
        throw (RuntimeException) e;
      else
        throw new OStorageException("Cannot open the remote storage: " + name, e);

    } finally {
      lock.releaseExclusiveLock();
    }
  }

  public void reload() {
    checkConnection();

    lock.acquireExclusiveLock();
    try {

      OChannelBinaryClient network = null;
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

        } catch (Exception e) {
          handleException(network, "Error on reloading database information", e);

        }
      } while (true);

    } finally {
      lock.releaseExclusiveLock();
    }
  }

  public void create(final Map<String, Object> iOptions) {
    throw new UnsupportedOperationException(
        "Cannot create a database in a remote server. Please use the console or the OServerAdmin class.");
  }

  public boolean exists() {
    throw new UnsupportedOperationException(
        "Cannot check the existance of a database in a remote server. Please use the console or the OServerAdmin class.");
  }

  public void close(final boolean iForce) {
    OChannelBinaryClient network = null;

    lock.acquireExclusiveLock();
    try {

      networkPoolLock.lock();
      try {
        if (networkPool.size() > 0) {
          try {
            network = beginRequest(OChannelBinaryProtocol.REQUEST_DB_CLOSE);
          } finally {
            endRequest(network);
          }
        }
      } finally {
        networkPoolLock.unlock();
      }

      setSessionId(null, -1);

      if (!checkForClose(iForce))
        return;

      networkPoolLock.lock();
      try {
        for (OChannelBinaryClient n : new ArrayList<OChannelBinaryClient>(networkPool))
          n.close();
        networkPool.clear();
      } finally {
        networkPoolLock.unlock();
      }

      level2Cache.shutdown();
      super.close(iForce);
      status = STATUS.CLOSED;

      Orient.instance().unregisterStorage(this);

    } catch (Exception e) {
      OLogManager.instance().debug(this, "Error on closing remote connection: %s", network);
      network.close();

    } finally {
      lock.releaseExclusiveLock();
    }
  }

  public void delete() {
    throw new UnsupportedOperationException(
        "Cannot delete a database in a remote server. Please use the console or the OServerAdmin class.");
  }

  public Set<String> getClusterNames() {
    lock.acquireSharedLock();
    try {

      return new HashSet<String>(clusterMap.keySet());

    } finally {
      lock.releaseSharedLock();
    }
  }

  public OStorageOperationResult<OPhysicalPosition> createRecord(final int iDataSegmentId, final ORecordId iRid,
      final byte[] iContent, ORecordVersion iRecordVersion, final byte iRecordType, int iMode,
      final ORecordCallback<OClusterPosition> iCallback) {
    checkConnection();

    if (iMode == 1 && iCallback == null)
      // ASYNCHRONOUS MODE NO ANSWER
      iMode = 2;

    final OPhysicalPosition ppos = new OPhysicalPosition(iDataSegmentId, -1, iRecordType);

    OChannelBinaryClient lastNetworkUsed = null;
    do {
      try {
        final OChannelBinaryClient network = beginRequest(OChannelBinaryProtocol.REQUEST_RECORD_CREATE);
        lastNetworkUsed = network;

        try {
          if (network.getSrvProtocolVersion() >= 10)
            // SEND THE DATA SEGMENT ID
            network.writeInt(iDataSegmentId);
          network.writeShort((short) iRid.clusterId);
          network.writeBytes(iContent);
          network.writeByte(iRecordType);
          network.writeByte((byte) iMode);

        } finally {
          endRequest(network);
        }

        switch (iMode) {
        case 0:
          // SYNCHRONOUS
          try {
            beginResponse(network);
            iRid.clusterPosition = network.readClusterPosition();
            ppos.clusterPosition = iRid.clusterPosition;
            if (network.getSrvProtocolVersion() >= 11) {
              ppos.recordVersion = network.readVersion();
            } else
              ppos.recordVersion = OVersionFactory.instance().createVersion();
            return new OStorageOperationResult<OPhysicalPosition>(ppos);
          } finally {
            endResponse(network);
          }

        case 1:
          // ASYNCHRONOUS
          if (iCallback != null) {
            final int sessionId = getSessionId();
            Callable<Object> response = new Callable<Object>() {
              public Object call() throws Exception {
                final OClusterPosition result;

                try {
                  OStorageRemoteThreadLocal.INSTANCE.get().sessionId = sessionId;
                  beginResponse(network);
                  result = network.readClusterPosition();
                  if (network.getSrvProtocolVersion() >= 11)
                    network.readVersion();
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
        return new OStorageOperationResult<OPhysicalPosition>(ppos);

      } catch (OModificationOperationProhibitedException mope) {
        handleDBFreeze();
      } catch (Exception e) {
        handleException(lastNetworkUsed, "Error on create record in cluster: " + iRid.clusterId, e);

      }
    } while (true);
  }

  @Override
  public boolean updateReplica(int dataSegmentId, ORecordId rid, byte[] content, ORecordVersion recordVersion, byte recordType)
      throws IOException {
    throw new UnsupportedOperationException("updateReplica()");
  }

  @Override
  public <V> V callInRecordLock(Callable<V> iCallable, ORID rid, boolean iExclusiveLock) {
    throw new UnsupportedOperationException("callInRecordLock()");
  }

  @Override
  public ORecordMetadata getRecordMetadata(final ORID rid) {

    OChannelBinaryClient network = null;
    do {
      try {
        try {
          network = beginRequest(OChannelBinaryProtocol.REQUEST_RECORD_METADATA);
          network.writeRID(rid);
        } finally {
          endRequest(network);
        }

        try {
          beginResponse(network);
          final ORID responseRid = network.readRID();
          final ORecordVersion responseVersion = network.readVersion();

          return new ORecordMetadata(responseRid, responseVersion);
        } finally {
          endResponse(network);
        }
      } catch (Exception e) {
        handleException(network, "Error on read record " + rid, e);
      }
    } while (true);
  }

  public OStorageOperationResult<ORawBuffer> readRecord(final ORecordId iRid, final String iFetchPlan, final boolean iIgnoreCache,
      final ORecordCallback<ORawBuffer> iCallback, boolean loadTombstones) {
    checkConnection();

    if (OStorageRemoteThreadLocal.INSTANCE.get().commandExecuting)
      // PENDING NETWORK OPERATION, CAN'T EXECUTE IT NOW
      return new OStorageOperationResult<ORawBuffer>(null);

    OChannelBinaryClient network = null;
    do {
      try {

        try {
          network = beginRequest(OChannelBinaryProtocol.REQUEST_RECORD_LOAD);
          network.writeRID(iRid);
          network.writeString(iFetchPlan != null ? iFetchPlan : "");
          if (network.getSrvProtocolVersion() >= 9)
            network.writeByte((byte) (iIgnoreCache ? 1 : 0));

          if (network.getSrvProtocolVersion() >= 13)
            network.writeByte(loadTombstones ? (byte) 1 : (byte) 0);
        } finally {
          endRequest(network);
        }

        try {
          beginResponse(network);

          if (network.readByte() == 0)
            return new OStorageOperationResult<ORawBuffer>(null);

          final ORawBuffer buffer = new ORawBuffer(network.readBytes(), network.readVersion(), network.readByte());

          final ODatabaseRecord database = ODatabaseRecordThreadLocal.INSTANCE.getIfDefined();
          ORecordInternal<?> record;
          while (network.readByte() == 2) {
            record = (ORecordInternal<?>) OChannelBinaryProtocol.readIdentifiable(network);

            if (database != null)
              // PUT IN THE CLIENT LOCAL CACHE
              database.getLevel1Cache().updateRecord(record);
          }
          return new OStorageOperationResult<ORawBuffer>(buffer);

        } finally {
          endResponse(network);
        }

      } catch (Exception e) {
        handleException(network, "Error on read record " + iRid, e);

      }
    } while (true);
  }

  public OStorageOperationResult<ORecordVersion> updateRecord(final ORecordId iRid, final byte[] iContent,
      final ORecordVersion iVersion, final byte iRecordType, int iMode, final ORecordCallback<ORecordVersion> iCallback) {
    checkConnection();

    if (iMode == 1 && iCallback == null)
      // ASYNCHRONOUS MODE NO ANSWER
      iMode = 2;

    OChannelBinaryClient lastNetworkUsed = null;
    do {
      try {
        final OChannelBinaryClient network = beginRequest(OChannelBinaryProtocol.REQUEST_RECORD_UPDATE);
        lastNetworkUsed = network;

        try {
          network.writeRID(iRid);
          network.writeBytes(iContent);
          network.writeVersion(iVersion);
          network.writeByte(iRecordType);
          network.writeByte((byte) iMode);

        } finally {
          endRequest(network);
        }

        switch (iMode) {
        case 0:
          // SYNCHRONOUS
          try {
            beginResponse(network);
            return new OStorageOperationResult<ORecordVersion>(network.readVersion());
          } finally {
            endResponse(network);
          }

        case 1:
          // ASYNCHRONOUS
          if (iCallback != null) {
            final int sessionId = getSessionId();
            Callable<Object> response = new Callable<Object>() {
              public Object call() throws Exception {
                ORecordVersion result;

                try {
                  OStorageRemoteThreadLocal.INSTANCE.get().sessionId = sessionId;
                  beginResponse(network);
                  result = network.readVersion();
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
        return new OStorageOperationResult<ORecordVersion>(iVersion);

      } catch (OModificationOperationProhibitedException mope) {
        handleDBFreeze();
      } catch (Exception e) {
        handleException(lastNetworkUsed, "Error on update record " + iRid, e);

      }
    } while (true);
  }

  public OStorageOperationResult<Boolean> deleteRecord(final ORecordId iRid, final ORecordVersion iVersion, int iMode,
      final ORecordCallback<Boolean> iCallback) {
    checkConnection();

    if (iMode == 1 && iCallback == null)
      // ASYNCHRONOUS MODE NO ANSWER
      iMode = 2;

    OChannelBinaryClient network = null;
    do {
      try {
        network = beginRequest(OChannelBinaryProtocol.REQUEST_RECORD_DELETE);
        return new OStorageOperationResult<Boolean>(deleteRecord(iRid, iVersion, iMode, iCallback, network));
      } catch (OModificationOperationProhibitedException mope) {
        handleDBFreeze();
      } catch (Exception e) {
        handleException(network, "Error on delete record " + iRid, e);

      }
    } while (true);
  }

  @Override
  public boolean cleanOutRecord(ORecordId recordId, ORecordVersion recordVersion, int iMode, ORecordCallback<Boolean> callback) {
    checkConnection();

    if (iMode == 1 && callback == null)
      // ASYNCHRONOUS MODE NO ANSWER
      iMode = 2;

    OChannelBinaryClient network = null;
    do {
      try {
        network = beginRequest(OChannelBinaryProtocol.REQUEST_RECORD_CLEAN_OUT);
        return deleteRecord(recordId, recordVersion, iMode, callback, network);
      } catch (OModificationOperationProhibitedException mope) {
        handleDBFreeze();
      } catch (Exception e) {
        handleException(network, "Error on clean out record " + recordId, e);

      }
    } while (true);
  }

  @Override
  public void backup(OutputStream out, Map<String, Object> options, Callable<Object> callable) throws IOException {
    throw new UnsupportedOperationException("backup");
  }

  @Override
  public void restore(InputStream in, Map<String, Object> options, Callable<Object> callable) throws IOException {
    throw new UnsupportedOperationException("restore");
  }

  public long count(final int iClusterId) {
    return count(new int[] { iClusterId });
  }

  @Override
  public long count(int iClusterId, boolean countTombstones) {
    return count(new int[] { iClusterId }, countTombstones);
  }

  public OClusterPosition[] getClusterDataRange(final int iClusterId) {
    checkConnection();

    OChannelBinaryClient network = null;
    do {
      try {
        try {
          network = beginRequest(OChannelBinaryProtocol.REQUEST_DATACLUSTER_DATARANGE);

          network.writeShort((short) iClusterId);

        } finally {
          endRequest(network);
        }

        try {
          beginResponse(network);
          return new OClusterPosition[] { network.readClusterPosition(), network.readClusterPosition() };
        } finally {
          endResponse(network);
        }

      } catch (Exception e) {
        handleException(network, "Error on getting last entry position count in cluster: " + iClusterId, e);

      }
    } while (true);
  }

  @Override
  public OPhysicalPosition[] higherPhysicalPositions(int iClusterId, OPhysicalPosition iClusterPosition) {
    checkConnection();

    OChannelBinaryClient network = null;
    do {
      try {

        try {
          network = beginRequest(OChannelBinaryProtocol.REQUEST_POSITIONS_HIGHER);
          network.writeInt(iClusterId);
          network.writeClusterPosition(iClusterPosition.clusterPosition);

        } finally {
          endRequest(network);
        }

        try {
          beginResponse(network);
          final int positionsCount = network.readInt();

          if (positionsCount == 0) {
            return new OPhysicalPosition[0];
          } else {
            return readPhysicalPositions(network, positionsCount);
          }

        } finally {
          endResponse(network);
        }

      } catch (Exception e) {
        handleException(network, "Error on retrieving higher positions after " + iClusterPosition.clusterPosition, e);
      }
    } while (true);
  }

  @Override
  public OPhysicalPosition[] ceilingPhysicalPositions(int clusterId, OPhysicalPosition physicalPosition) {
    checkConnection();

    OChannelBinaryClient network = null;
    do {
      try {

        try {
          network = beginRequest(OChannelBinaryProtocol.REQUEST_POSITIONS_CEILING);
          network.writeInt(clusterId);
          network.writeClusterPosition(physicalPosition.clusterPosition);

        } finally {
          endRequest(network);
        }

        try {
          beginResponse(network);
          final int positionsCount = network.readInt();

          if (positionsCount == 0) {
            return new OPhysicalPosition[0];
          } else {
            return readPhysicalPositions(network, positionsCount);
          }

        } finally {
          endResponse(network);
        }

      } catch (Exception e) {
        handleException(network, "Error on retrieving ceiling positions after " + physicalPosition.clusterPosition, e);
      }
    } while (true);
  }

  private OPhysicalPosition[] readPhysicalPositions(OChannelBinaryClient network, int positionsCount) throws IOException {
    final OPhysicalPosition[] physicalPositions = new OPhysicalPosition[positionsCount];

    for (int i = 0; i < physicalPositions.length; i++) {
      final OPhysicalPosition position = new OPhysicalPosition();

      position.clusterPosition = network.readClusterPosition();
      position.dataSegmentId = network.readInt();
      position.dataSegmentPos = network.readLong();
      position.recordSize = network.readInt();
      position.recordVersion = network.readVersion();

      physicalPositions[i] = position;
    }
    return physicalPositions;
  }

  @Override
  public OPhysicalPosition[] lowerPhysicalPositions(int iClusterId, OPhysicalPosition physicalPosition) {
    checkConnection();

    OChannelBinaryClient network = null;
    do {
      try {

        try {
          network = beginRequest(OChannelBinaryProtocol.REQUEST_POSITIONS_LOWER);
          network.writeInt(iClusterId);
          network.writeClusterPosition(physicalPosition.clusterPosition);

        } finally {
          endRequest(network);
        }

        try {
          beginResponse(network);

          final int positionsCount = network.readInt();

          if (positionsCount == 0) {
            return new OPhysicalPosition[0];
          } else {
            return readPhysicalPositions(network, positionsCount);
          }

        } finally {
          endResponse(network);
        }

      } catch (Exception e) {
        handleException(network, "Error on retrieving lower positions after " + physicalPosition.clusterPosition, e);

      }
    } while (true);
  }

  @Override
  public OPhysicalPosition[] floorPhysicalPositions(int clusterId, OPhysicalPosition physicalPosition) {
    checkConnection();

    OChannelBinaryClient network = null;
    do {
      try {

        try {
          network = beginRequest(OChannelBinaryProtocol.REQUEST_POSITIONS_FLOOR);
          network.writeInt(clusterId);
          network.writeClusterPosition(physicalPosition.clusterPosition);

        } finally {
          endRequest(network);
        }

        try {
          beginResponse(network);

          final int positionsCount = network.readInt();

          if (positionsCount == 0) {
            return new OPhysicalPosition[0];
          } else {
            return readPhysicalPositions(network, positionsCount);
          }

        } finally {
          endResponse(network);
        }

      } catch (Exception e) {
        handleException(network, "Error on retrieving floor positions after " + physicalPosition.clusterPosition, e);
      }
    } while (true);
  }

  public long getSize() {
    checkConnection();

    OChannelBinaryClient network = null;
    do {
      try {
        try {

          network = beginRequest(OChannelBinaryProtocol.REQUEST_DB_SIZE);

        } finally {
          endRequest(network);
        }

        try {
          beginResponse(network);
          return network.readLong();
        } finally {
          endResponse(network);
        }

      } catch (Exception e) {
        handleException(network, "Error on read database size", e);

      }
    } while (true);
  }

  @Override
  public long countRecords() {
    checkConnection();

    OChannelBinaryClient network = null;
    do {
      try {
        try {

          network = beginRequest(OChannelBinaryProtocol.REQUEST_DB_COUNTRECORDS);

        } finally {
          endRequest(network);
        }

        try {
          beginResponse(network);
          return network.readLong();
        } finally {
          endResponse(network);
        }

      } catch (Exception e) {
        handleException(network, "Error on read database record count", e);

      }
    } while (true);
  }

  public long count(final int[] iClusterIds) {
    return count(iClusterIds, false);
  }

  public long count(final int[] iClusterIds, boolean countTombstones) {
    checkConnection();

    OChannelBinaryClient network = null;
    do {
      try {
        try {
          network = beginRequest(OChannelBinaryProtocol.REQUEST_DATACLUSTER_COUNT);

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

      } catch (Exception e) {
        handleException(network, "Error on read record count in clusters: " + Arrays.toString(iClusterIds), e);

      }
    } while (true);
  }

  /**
   * Execute the command remotely and get the results back.
   */
  public Object command(final OCommandRequestText iCommand) {
    checkConnection();

    if (!(iCommand instanceof OSerializableStream))
      throw new OCommandExecutionException("Cannot serialize the command to be executed to the server side.");

    OSerializableStream command = iCommand;
    Object result = null;

    final ODatabaseRecord database = ODatabaseRecordThreadLocal.INSTANCE.get();

    OChannelBinaryClient network = null;
    do {

      OStorageRemoteThreadLocal.INSTANCE.get().commandExecuting = true;
      try {

        final OCommandRequestText aquery = iCommand;

        final boolean asynch = iCommand instanceof OCommandRequestAsynch && ((OCommandRequestAsynch) iCommand).isAsynchronous();

        try {
          network = beginRequest(OChannelBinaryProtocol.REQUEST_COMMAND);

          network.writeByte((byte) (asynch ? 'a' : 's')); // ASYNC / SYNC
          network.writeBytes(OStreamSerializerAnyStreamable.INSTANCE.toStream(command));

        } finally {
          endRequest(network);
        }

        try {
          beginResponse(network);

          if (asynch) {
            byte status;

            // ASYNCH: READ ONE RECORD AT TIME
            while ((status = network.readByte()) > 0) {
              final ORecordInternal<?> record = (ORecordInternal<?>) OChannelBinaryProtocol.readIdentifiable(network);
              if (record == null)
                continue;

              switch (status) {
              case 1:
                // PUT AS PART OF THE RESULT SET. INVOKE THE LISTENER
                try {
                  if (!aquery.getResultListener().result(record)) {
                    // EMPTY THE INPUT CHANNEL
                    while (network.in.available() > 0)
                      network.in.read();

                    break;
                  }
                } catch (Throwable t) {
                  // ABSORBE ALL THE USER EXCEPTIONS
                  t.printStackTrace();
                }
                database.getLevel1Cache().updateRecord(record);
                break;

              case 2:
                // PUT IN THE CLIENT LOCAL CACHE
                database.getLevel1Cache().updateRecord(record);
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
              if (result instanceof ORecord<?>)
                database.getLevel1Cache().updateRecord((ORecordInternal<?>) result);
              break;

            case 'l':
              final int tot = network.readInt();
              final Collection<OIdentifiable> list = new ArrayList<OIdentifiable>(tot);
              for (int i = 0; i < tot; ++i) {
                final OIdentifiable resultItem = OChannelBinaryProtocol.readIdentifiable(network);
                if (resultItem instanceof ORecord<?>)
                  database.getLevel1Cache().updateRecord((ORecordInternal<?>) resultItem);
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
              byte status;
              while ((status = network.readByte()) > 0) {
                final ORecordInternal<?> record = (ORecordInternal<?>) OChannelBinaryProtocol.readIdentifiable(network);
                if (record != null && status == 2)
                  // PUT IN THE CLIENT LOCAL CACHE
                  database.getLevel1Cache().updateRecord(record);
              }
            }
          }
          break;
        } finally {
          if (aquery.getResultListener() != null) {
            aquery.getResultListener().end();
          }
          endResponse(network);
        }

      } catch (OModificationOperationProhibitedException mope) {
        handleDBFreeze();
      } catch (Exception e) {
        handleException(network, "Error on executing command: " + iCommand, e);

      } finally {
        OStorageRemoteThreadLocal.INSTANCE.get().commandExecuting = false;
      }
    } while (true);

    return result;
  }

  public void commit(final OTransaction iTx, Runnable callback) {
    checkConnection();

    final List<ORecordOperation> committedEntries = new ArrayList<ORecordOperation>();
    OChannelBinaryClient network = null;
    do {
      try {
        OStorageRemoteThreadLocal.INSTANCE.get().commandExecuting = true;

        try {
          network = beginRequest(OChannelBinaryProtocol.REQUEST_TX_COMMIT);

          network.writeInt(iTx.getId());
          network.writeByte((byte) (iTx.isUsingLog() ? 1 : 0));

          final List<ORecordOperation> tmpEntries = new ArrayList<ORecordOperation>();

          if (iTx.getCurrentRecordEntries().iterator().hasNext()) {
            while (iTx.getCurrentRecordEntries().iterator().hasNext()) {
              for (ORecordOperation txEntry : iTx.getCurrentRecordEntries())
                tmpEntries.add(txEntry);

              iTx.clearRecordEntries();

              if (tmpEntries.size() > 0) {
                for (ORecordOperation txEntry : tmpEntries) {
                  commitEntry(network, txEntry);
                  committedEntries.add(txEntry);
                }
                tmpEntries.clear();
              }
            }
          } else if (committedEntries.size() > 0) {
            for (ORecordOperation txEntry : committedEntries)
              commitEntry(network, txEntry);
          }

          // END OF RECORD ENTRIES
          network.writeByte((byte) 0);

          // SEND INDEX ENTRIES
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
              rop.getRecord().getRecordVersion().copyFrom(network.readVersion());
          }

          committedEntries.clear();
        } finally {
          endResponse(network);
        }

        // SET ALL THE RECORDS AS UNDIRTY
        for (ORecordOperation txEntry : iTx.getAllRecordEntries())
          txEntry.getRecord().unload();

        // UPDATE THE CACHE ONLY IF THE ITERATOR ALLOWS IT. USE THE STRATEGY TO ALWAYS REMOVE ALL THE RECORDS SINCE THEY COULD BE
        // CHANGED AS CONTENT IN CASE OF TREE AND GRAPH DUE TO CROSS REFERENCES
        OTransactionAbstract.updateCacheFromEntries(iTx, iTx.getAllRecordEntries(), false);

        break;

      } catch (OModificationOperationProhibitedException mope) {
        handleDBFreeze();
      } catch (Exception e) {
        handleException(network, "Error on commit", e);

      } finally {
        OStorageRemoteThreadLocal.INSTANCE.get().commandExecuting = false;

      }
    } while (true);
  }

  public void rollback(OTransaction iTx) {
  }

  public int getClusterIdByName(final String iClusterName) {
    checkConnection();

    if (iClusterName == null)
      return -1;

    if (Character.isDigit(iClusterName.charAt(0)))
      return Integer.parseInt(iClusterName);

    final OCluster cluster = clusterMap.get(iClusterName.toLowerCase());
    if (cluster == null)
      return -1;

    return cluster.getId();
  }

  public String getClusterTypeByName(final String iClusterName) {
    checkConnection();

    if (iClusterName == null)
      return null;

    final OCluster cluster = clusterMap.get(iClusterName.toLowerCase());
    if (cluster == null)
      return null;

    return cluster.getType();
  }

  public int getDefaultClusterId() {
    return defaultClusterId;
  }

  public int addCluster(final String iClusterType, final String iClusterName, final String iLocation,
      final String iDataSegmentName, boolean forceListBased, final Object... iArguments) {
    return addCluster(iClusterType, iClusterName, -1, iLocation, iDataSegmentName, forceListBased, iArguments);
  }

  public int addCluster(String iClusterType, String iClusterName, int iRequestedId, String iLocation, String iDataSegmentName,
      boolean forceListBased, Object... iParameters) {
    checkConnection();

    OChannelBinaryClient network = null;
    do {
      try {
        try {
          network = beginRequest(OChannelBinaryProtocol.REQUEST_DATACLUSTER_ADD);

          network.writeString(iClusterType.toString());
          network.writeString(iClusterName);
          if (network.getSrvProtocolVersion() >= 10 || iClusterType.equalsIgnoreCase("PHYSICAL"))
            network.writeString(iLocation);
          if (network.getSrvProtocolVersion() >= 10)
            network.writeString(iDataSegmentName);
          else
            network.writeInt(-1);

          if (network.getSrvProtocolVersion() >= 18)
            network.writeShort((short) iRequestedId);
        } finally {
          endRequest(network);
        }

        try {
          beginResponse(network);
          final int clusterId = network.readShort();

          final OClusterRemote cluster = new OClusterRemote();

          cluster.setType(iClusterType);
          cluster.configure(this, clusterId, iClusterName.toLowerCase(), null, 0);

          if (clusters.length <= clusterId)
            clusters = Arrays.copyOf(clusters, clusterId + 1);
          clusters[cluster.getId()] = cluster;
          clusterMap.put(cluster.getName().toLowerCase(), cluster);

          return clusterId;
        } finally {
          endResponse(network);
        }
      } catch (OModificationOperationProhibitedException mphe) {
        handleDBFreeze();
      } catch (Exception e) {
        handleException(network, "Error on add new cluster", e);
      }
    } while (true);
  }

  public boolean dropCluster(final int iClusterId, final boolean iTruncate) {
    checkConnection();

    OChannelBinaryClient network = null;
    do {
      try {
        try {
          network = beginRequest(OChannelBinaryProtocol.REQUEST_DATACLUSTER_DROP);

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

          getLevel2Cache().freeCluster(iClusterId);
          return true;
        }
        return false;

      } catch (OModificationOperationProhibitedException mope) {
        handleDBFreeze();
      } catch (Exception e) {
        handleException(network, "Error on removing of cluster", e);

      }
    } while (true);
  }

  public int addDataSegment(final String iDataSegmentName) {
    return addDataSegment(iDataSegmentName, null);
  }

  public int addDataSegment(final String iSegmentName, final String iLocation) {
    checkConnection();

    OChannelBinaryClient network = null;
    do {
      try {
        try {
          network = beginRequest(OChannelBinaryProtocol.REQUEST_DATASEGMENT_ADD);

          network.writeString(iSegmentName).writeString(iLocation);

        } finally {
          endRequest(network);
        }

        try {
          beginResponse(network);
          return network.readInt();
        } finally {
          endResponse(network);
        }

      } catch (OModificationOperationProhibitedException mphe) {
        handleDBFreeze();
      } catch (Exception e) {
        handleException(network, "Error on add new data segment", e);
      }
    } while (true);
  }

  public boolean dropDataSegment(final String iSegmentName) {
    checkConnection();

    OChannelBinaryClient network = null;
    do {
      try {
        try {
          network = beginRequest(OChannelBinaryProtocol.REQUEST_DATASEGMENT_DROP);

          network.writeString(iSegmentName);

        } finally {
          endRequest(network);
        }

        try {
          beginResponse(network);
          return network.readByte() == 1;
        } finally {
          endResponse(network);
        }

      } catch (OModificationOperationProhibitedException mope) {
        handleDBFreeze();
      } catch (Exception e) {
        handleException(network, "Error on remove data segment", e);
      }
    } while (true);
  }

  public void synch() {
  }

  public String getPhysicalClusterNameById(final int iClusterId) {
    lock.acquireSharedLock();
    try {

      if (iClusterId >= clusters.length)
        return null;

      final OCluster cluster = clusters[iClusterId];
      return cluster != null ? cluster.getName() : null;

    } finally {
      lock.releaseSharedLock();
    }
  }

  public int getClusterMap() {
    return clusterMap.size();
  }

  public Collection<OCluster> getClusterInstances() {
    lock.acquireSharedLock();
    try {

      return Arrays.asList(clusters);

    } finally {
      lock.releaseSharedLock();
    }
  }

  public OCluster getClusterById(int iClusterId) {
    lock.acquireSharedLock();
    try {

      if (iClusterId == ORID.CLUSTER_ID_INVALID)
        // GET THE DEFAULT CLUSTER
        iClusterId = defaultClusterId;

      return clusters[iClusterId];

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
   * Handles exceptions. In case of IO errors retries to reconnect until the configured retry times has reached.
   * 
   * @param message
   * @param exception
   */
  protected void handleException(final OChannelBinaryClient iNetwork, final String message, final Exception exception) {
    if (exception instanceof OTimeoutException)
      // TIMEOUT, AVOID LOOP, RE-THROW IT
      throw (OTimeoutException) exception;
    else if (exception instanceof OException)
      // RE-THROW IT
      throw (OException) exception;
    else if (!(exception instanceof IOException))
      throw new OStorageException(message, exception);

    if (status != STATUS.OPEN)
      // STORAGE CLOSED: DON'T HANDLE RECONNECTION
      return;

    OLogManager.instance().warn(this, "Caught I/O errors from %s (local socket=%s), trying to reconnect (error: %s)", iNetwork,
        iNetwork.socket.getLocalSocketAddress(), exception);
    try {
      iNetwork.close();
    } catch (Exception e) {
      // IGNORE ANY EXCEPTION
    }

    final long lostConnectionTime = System.currentTimeMillis();

    final int currentMaxRetry;
    final int currentRetryDelay;
    synchronized (clusterConfiguration) {
      if (!clusterConfiguration.isEmpty()) {
        // IN CLUSTER: NO RETRY AND 0 SLEEP TIME BETWEEN NODES
        currentMaxRetry = 1;
        currentRetryDelay = 0;
      } else {
        currentMaxRetry = connectionRetry;
        currentRetryDelay = connectionRetryDelay;
      }
    }

    for (int retry = 0; retry < currentMaxRetry; ++retry) {
      // WAIT THE DELAY BEFORE TO RETRY
      if (currentRetryDelay > 0)
        try {
          Thread.sleep(currentRetryDelay);
        } catch (InterruptedException e) {
          // THREAD INTERRUPTED: RETURN EXCEPTION
          Thread.currentThread().interrupt();
          break;
        }

      try {
        if (OLogManager.instance().isDebugEnabled())
          OLogManager.instance()
              .debug(this, "Retrying to connect to remote server #" + (retry + 1) + "/" + currentMaxRetry + "...");

        // FORCE RESET OF THREAD DATA (SERVER URL + SESSION ID)
        setSessionId(null, -1);

        if (createConnectionPool() == 0)
          // NO CONNECTION!
          break;

        // REACQUIRE DB SESSION ID
        openRemoteDatabase();

        OLogManager.instance().warn(this,
            "Connection re-acquired transparently after %dms and %d retries: no errors will be thrown at application level",
            System.currentTimeMillis() - lostConnectionTime, retry + 1);

        // RECONNECTED!
        return;

      } catch (Throwable t) {
        // DO NOTHING BUT CONTINUE IN THE LOOP
      }
    }

    // RECONNECTION FAILED: THROW+LOG THE ORIGINAL EXCEPTION
    throw new OStorageException(message, exception);
  }

  protected OChannelBinaryClient openRemoteDatabase() throws IOException {
    minPool = OGlobalConfiguration.CLIENT_CHANNEL_MIN_POOL.getValueAsInteger();
    maxPool = OGlobalConfiguration.CLIENT_CHANNEL_MAX_POOL.getValueAsInteger();
    connectionDbType = ODatabaseDocument.TYPE;

    if (connectionOptions != null && connectionOptions.size() > 0) {
      if (connectionOptions.containsKey(PARAM_MIN_POOL))
        minPool = Integer.parseInt(connectionOptions.get(PARAM_MIN_POOL).toString());
      if (connectionOptions.containsKey(PARAM_MAX_POOL))
        maxPool = Integer.parseInt(connectionOptions.get(PARAM_MAX_POOL).toString());
      if (connectionOptions.containsKey(PARAM_DB_TYPE))
        connectionDbType = connectionOptions.get(PARAM_DB_TYPE).toString();
    }

    boolean availableConnections = true;
    OChannelBinaryClient network = null;

    while (availableConnections) {
      try {
        network = getAvailableNetwork();
        try {
          network.writeByte(OChannelBinaryProtocol.REQUEST_DB_OPEN);
          network.writeInt(getSessionId());

          // @SINCE 1.0rc8
          sendClientInfo(network);

          network.writeString(name);

          if (network.getSrvProtocolVersion() >= 8)
            network.writeString(connectionDbType);

          network.writeString(connectionUserName);
          network.writeString(connectionUserPassword);

        } finally {
          endRequest(network);
        }

        final int sessionId;

        try {
          beginResponse(network);
          sessionId = network.readInt();
          setSessionId(network.getServerURL(), sessionId);

          OLogManager.instance().debug(this, "Client connected to %s with session id=%d", network.getServerURL(), sessionId);

          readDatabaseInformation(network);

          // READ CLUSTER CONFIGURATION
          updateClusterConfiguration(network.readBytes());

          // read OrientDB release info
          if (network.getSrvProtocolVersion() >= 14)
            network.readString();

          status = STATUS.OPEN;

          return network;

        } finally {
          endResponse(network);
        }
      } catch (Exception e) {
        handleException(network, "Cannot create a connection to remote server address(es): " + serverURLs, e);
      }

      networkPoolLock.lock();
      try {
        availableConnections = !networkPool.isEmpty();
      } finally {
        networkPoolLock.unlock();
      }

    }

    throw new OStorageException("Cannot create a connection to remote server address(es): " + serverURLs);
  }

  protected void sendClientInfo(OChannelBinaryClient network) throws IOException {
    if (network.getSrvProtocolVersion() >= 7) {
      // @COMPATIBILITY 1.0rc8
      network.writeString(DRIVER_NAME).writeString(OConstants.ORIENT_VERSION)
          .writeShort((short) OChannelBinaryProtocol.CURRENT_PROTOCOL_VERSION).writeString(clientId);
    }
  }

  /**
   * Parse the URL in the following formats:<br/>
   */
  protected void parseServerURLs() {
    int dbPos = url.indexOf('/');
    if (dbPos == -1) {
      // SHORT FORM
      addHost(url);
      name = url;
    } else {
      name = url.substring(url.lastIndexOf("/") + 1);
      for (String host : url.substring(0, dbPos).split(ADDRESS_SEPARATOR))
        addHost(host);
    }

    if (serverURLs.size() == 1 && OGlobalConfiguration.NETWORK_BINARY_DNS_LOADBALANCING_ENABLED.getValueAsBoolean()) {
      // LOOK FOR LOAD BALANCING DNS TXT RECORD
      final String primaryServer = serverURLs.get(0);

      try {
        final Hashtable<String, String> env = new Hashtable<String, String>();
        env.put("java.naming.factory.initial", "com.sun.jndi.dns.DnsContextFactory");
        env.put("com.sun.jndi.ldap.connect.timeout",
            OGlobalConfiguration.NETWORK_BINARY_DNS_LOADBALANCING_TIMEOUT.getValueAsString());
        final DirContext ictx = new InitialDirContext(env);
        final String hostName = primaryServer.indexOf(":") == -1 ? primaryServer : primaryServer.substring(0,
            primaryServer.indexOf(":"));
        final Attributes attrs = ictx.getAttributes(hostName, new String[] { "TXT" });
        final Attribute attr = attrs.get("TXT");
        if (attr != null) {
          String configuration = (String) attr.get();
          if (configuration.startsWith(""))
            configuration = configuration.substring(1, configuration.length() - 1);
          if (configuration != null) {
            final String[] parts = configuration.split(" ");
            for (String part : parts) {
              if (part.startsWith("s=")) {
                addHost(part.substring("s=".length()));
              }
            }
          }
        }
      } catch (NamingException e) {
      }
    }
  }

  /**
   * Registers the remote server with port.
   */
  protected String addHost(String host) {
    if (host.startsWith("localhost"))
      host = "127.0.0.1" + host.substring("localhost".length());

    // REGISTER THE REMOTE SERVER+PORT
    if (host.indexOf(":") == -1)
      host += ":" + getDefaultPort();

    if (!serverURLs.contains(host))
      serverURLs.add(host);

    return host;
  }

  protected String getDefaultHost() {
    return DEFAULT_HOST;
  }

  protected int getDefaultPort() {
    return DEFAULT_PORT;
  }

  protected OChannelBinaryClient createNetworkConnection() throws IOException, UnknownHostException {
    final String currentServerURL = getServerURL();

    if (currentServerURL != null) {
      // TRY WITH CURRENT URL IF ANY
      try {
        return connect(currentServerURL);

      } catch (Exception e) {
        OLogManager.instance().debug(this, "Error on connecting to %s", e, currentServerURL);
      }
    }

    for (int serverIdx = 0; serverIdx < serverURLs.size(); ++serverIdx) {
      final String server = serverURLs.get(serverIdx);

      try {
        final OChannelBinaryClient ch = connect(server);

        if (serverIdx > 0) {
          // UPDATE SERVER LIST WITH THE REACHABLE ONE AS HEAD TO SPEED UP FURTHER CONNECTIONS
          serverURLs.remove(serverIdx);
          serverURLs.add(0, server);
          OLogManager.instance().debug(this, "New server list priority: %s...", serverURLs);
        }

        return ch;

      } catch (Exception e) {
        OLogManager.instance().debug(this, "Error on connecting to %s", e, server);
      }
    }

    // ERROR, NO URL IS REACHABLE
    final StringBuilder buffer = new StringBuilder();
    for (String server : serverURLs) {
      if (buffer.length() > 0)
        buffer.append(',');
      buffer.append(server);
    }

    throw new OIOException("Cannot connect to any configured remote nodes: " + buffer);
  }

  protected OChannelBinaryClient connect(final String server) throws IOException {
    OLogManager.instance().debug(this, "Trying to connect to the remote host %s...", server);

    final int sepPos = server.indexOf(":");
    final String remoteHost = server.substring(0, sepPos);
    final int remotePort = Integer.parseInt(server.substring(sepPos + 1));

    final OChannelBinaryClient ch = new OChannelBinaryClient(remoteHost, remotePort, clientConfiguration,
        OChannelBinaryProtocol.CURRENT_PROTOCOL_VERSION, asynchEventListener);

    // REGISTER MYSELF AS LISTENER TO REMOVE THE CHANNEL FROM THE POOL IN CASE OF CLOSING
    ch.registerListener(this);

    // REGISTER IT IN THE POOL
    networkPool.add(ch);
    return ch;
  }

  protected void checkConnection() {
    // lock.acquireSharedLock();
    //
    // try {
    // synchronized (networkPool) {
    //
    // if (networkPool.size() == 0)
    // throw new ODatabaseException("Connection is closed");
    // }
    //
    // } finally {
    // lock.releaseSharedLock();
    // }
  }

  /**
   * Acquire a network channel from the pool. Don't lock the write stream since the connection usage is exclusive.
   * 
   * @param iCommand
   * @return
   * @throws IOException
   */
  protected OChannelBinaryClient beginRequest(final byte iCommand) throws IOException {
    final OChannelBinaryClient network = getAvailableNetwork();

    network.writeByte(iCommand);
    network.writeInt(getSessionId());

    return network;
  }

  protected OChannelBinaryClient getAvailableNetwork() throws IOException, UnknownHostException {
    // FIND THE FIRST FREE CHANNEL AVAILABLE

    OChannelBinaryClient network = null;

    int beginCursor = networkPoolCursor;
    while (network == null) {
      networkPoolLock.lock();
      try {
        if (networkPoolCursor < 0)
          networkPoolCursor = 0;
        else if (networkPoolCursor >= networkPool.size())
          // RESTART FROM THE BEGINNING
          networkPoolCursor = 0;

        if (networkPool.size() == 0) {
          createConnectionPool();
          networkPoolCursor = 0;
        }

        if (networkPool.size() == 0)
          throw new ONetworkProtocolException("Connection pool closed");

        network = networkPool.get(networkPoolCursor);
        networkPoolCursor++;

        final String serverURL = getServerURL();

        if (serverURL == null || network.getServerURL().equals(serverURL)) {
          if (network.getLockWrite().tryAcquireLock())
            // WAS UNLOCKED! USE THIS
            break;
        }

        network = null;

        if (beginCursor >= networkPool.size())
          // THE POOL HAS BEEN REDUCED: RSTART FROM CURRENT POSITION
          beginCursor = networkPoolCursor;

        if (networkPoolCursor == beginCursor) {
          // COMPLETE ROUND AND NOT FREE CONNECTIONS FOUND

          if (networkPool.size() < maxPool) {
            // CREATE NEW CONNECTION
            network = createNetworkConnection();
            network.getLockWrite().lock();

          } else {
            OLogManager.instance().info(this,
                "Network connection pool is full (max=%d): increase max size to avoid such bottleneck on connections", maxPool);

            removeDeadConnections();

            final long startToWait = System.currentTimeMillis();

            // TEMPORARY UNLOCK
            networkPoolLock.unlock();
            try {
              synchronized (networkPool) {
                networkPool.wait(5000);
              }
            } catch (InterruptedException e) {
              // THREAD INTERRUPTED: RETURN EXCEPTION
              Thread.currentThread().interrupt();
              throw new OStorageException("Cannot acquire a connection because the thread has been interrupted");
            } finally {
              networkPoolLock.lock();
            }

            Orient
                .instance()
                .getProfiler()
                .stopChrono("system.network.connectionPool.waitingTime", "Waiting for a free connection from the pool of channels",
                    startToWait);
          }
        }

      } finally {
        networkPoolLock.unlock();
      }
    }
    return network;
  }

  private void removeDeadConnections() {
    // FREE DEAD CONNECTIONS
    int removedDeadConnections = 0;
    for (OChannelBinaryClient n : new ArrayList<OChannelBinaryClient>(networkPool)) {
      if (n != null && !n.isConnected())
        try {
          n.close();
        } catch (Exception e) {
        }
      networkPool.remove(n);
      removedDeadConnections++;
    }

    OLogManager.instance().debug(this, "Found and removed %d dead connections from the network pool", removedDeadConnections);
  }

  /**
   * Ends the request and unlock the write lock
   */
  public void endRequest(final OChannelBinaryClient iNetwork) throws IOException {
    if (iNetwork == null)
      return;

    try {
      iNetwork.flush();
    } catch (IOException e) {
      try {
        iNetwork.close();
      } catch (Exception e2) {
      } finally {
        networkPoolLock.lock();
        try {
          networkPool.remove(iNetwork);
        } finally {
          networkPoolLock.unlock();
        }
      }
      throw e;
    } finally {

      iNetwork.releaseWriteLock();

      networkPoolLock.lock();
      try {
        synchronized (networkPool) {
          networkPool.notifyAll();
        }
      } finally {
        networkPoolLock.unlock();
      }
    }
  }

  /**
   * Starts listening the response.
   */
  protected void beginResponse(final OChannelBinaryClient iNetwork) throws IOException {
    iNetwork.beginResponse(getSessionId());
  }

  /**
   * End response reached: release the channel in the pool to being reused
   */
  public void endResponse(final OChannelBinaryClient iNetwork) {
    iNetwork.endResponse();
  }

  public boolean isPermanentRequester() {
    return false;
  }

  protected void getResponse(final OChannelBinaryClient iNetwork) throws IOException {
    try {
      beginResponse(iNetwork);
    } finally {
      endResponse(iNetwork);
    }
  }

  @SuppressWarnings("unchecked")
  public void updateClusterConfiguration(final byte[] obj) {
    if (obj == null)
      return;

    // UPDATE IT
    synchronized (clusterConfiguration) {
      clusterConfiguration.fromStream(obj);

      final List<ODocument> members = clusterConfiguration.field("members");
      if (members != null) {
        serverURLs.clear();

        parseServerURLs();

        for (ODocument m : members)
          if (m != null && !serverURLs.contains((String) m.field("name"))) {
            for (Map<String, Object> listener : ((Collection<Map<String, Object>>) m.field("listeners"))) {
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

  private void commitEntry(final OChannelBinaryClient iNetwork, final ORecordOperation txEntry) throws IOException {
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
      throw new OTransactionException("Error on transaction commit", e);
    }

    iNetwork.writeByte((byte) 1);
    iNetwork.writeByte(txEntry.type);
    iNetwork.writeRID(txEntry.getRecord().getIdentity());
    iNetwork.writeByte(txEntry.getRecord().getRecordType());

    switch (txEntry.type) {
    case ORecordOperation.CREATED:
      iNetwork.writeBytes(stream);
      break;

    case ORecordOperation.UPDATED:
      iNetwork.writeVersion(txEntry.getRecord().getRecordVersion());
      iNetwork.writeBytes(stream);
      break;

    case ORecordOperation.DELETED:
      iNetwork.writeVersion(txEntry.getRecord().getRecordVersion());
      break;
    }
  }

  protected int createConnectionPool() throws IOException, UnknownHostException {
    networkPoolLock.lock();
    try {

      if (networkPool.isEmpty())
        // ALWAYS CREATE THE FIRST CONNECTION
        createNetworkConnection();

      // CREATE THE MINIMUM POOL
      for (int i = networkPool.size(); i < minPool; ++i)
        createNetworkConnection();

      return networkPool.size();

    } finally {
      networkPoolLock.unlock();
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

  private void readDatabaseInformation(final OChannelBinaryClient network) throws IOException {
    // @COMPATIBILITY 1.0rc8
    final int tot = network.getSrvProtocolVersion() >= 7 ? network.readShort() : network.readInt();

    clusters = new OCluster[tot];
    clusterMap.clear();

    for (int i = 0; i < tot; ++i) {
      final OClusterRemote cluster = new OClusterRemote();
      String clusterName = network.readString();
      if (clusterName != null)
        clusterName = clusterName.toLowerCase();
      final int clusterId = network.readShort();
      final String clusterType = network.readString();
      final int dataSegmentId = network.getSrvProtocolVersion() >= 12 ? (int) network.readShort() : 0;

      cluster.setType(clusterType);
      cluster.configure(this, clusterId, clusterName, null, dataSegmentId);

      if (clusterId >= clusters.length)
        clusters = Arrays.copyOf(clusters, clusterId + 1);
      clusters[clusterId] = cluster;
      clusterMap.put(clusterName, cluster);
    }

    defaultClusterId = clusterMap.get(CLUSTER_DEFAULT_NAME).getId();
  }

  @Override
  public String getURL() {
    return OEngineRemote.NAME + ":" + url;
  }

  public String getClientId() {
    return clientId;
  }

  public int getDataSegmentIdByName(final String iName) {
    if (iName == null)
      return 0;

    throw new UnsupportedOperationException("getDataSegmentIdByName()");
  }

  public ODataSegment getDataSegmentById(final int iDataSegmentId) {
    throw new UnsupportedOperationException("getDataSegmentById()");
  }

  public int getClusters() {
    return clusterMap.size();
  }

  public void setDefaultClusterId(int defaultClusterId) {
    this.defaultClusterId = defaultClusterId;
  }

  @Override
  public String getType() {
    return OEngineRemote.NAME;
  }

  @Override
  public void onChannelClose(final OChannel iChannel) {
    networkPoolLock.lock();
    try {

      networkPool.remove(iChannel);

    } finally {
      networkPoolLock.unlock();
    }
  }

  private boolean deleteRecord(final ORecordId iRid, ORecordVersion iVersion, int iMode, final ORecordCallback<Boolean> iCallback,
      final OChannelBinaryClient network) throws IOException {
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
