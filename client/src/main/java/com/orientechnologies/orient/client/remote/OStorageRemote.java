/*
 * Copyright 1999-2010 Luca Garulli (l.garulli--at--orientechnologies.com)
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
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.io.OIOException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.util.OPair;
import com.orientechnologies.orient.client.config.OClientConfiguration;
import com.orientechnologies.orient.client.dictionary.ODictionaryClient;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.OCommandRequestAsynch;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.config.OStorageConfiguration;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.dictionary.ODictionary;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordFactory;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.ORecordSchemaAware;
import com.orientechnologies.orient.core.serialization.OSerializableStream;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializerAnyRuntime;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializerAnyStreamable;
import com.orientechnologies.orient.core.storage.OCluster;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.OStorageAbstract;
import com.orientechnologies.orient.core.storage.impl.local.OClusterLocal;
import com.orientechnologies.orient.core.tx.OTransaction;
import com.orientechnologies.orient.core.tx.OTransactionEntry;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryClient;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;

/**
 * This object is bound to each remote ODatabase instances.
 */
@SuppressWarnings("unchecked")
public class OStorageRemote extends OStorageAbstract {
  private static final String             DEFAULT_HOST      = "localhost";
  private static final String[]           DEFAULT_PORTS     = new String[] { "2424" };
  private static final String             ADDRESS_SEPARATOR = ";";
  private String                          userName;
  private String                          userPassword;
  protected List<OPair<String, String[]>> serverURLs        = new ArrayList<OPair<String, String[]>>();
  private final OClientConfiguration      clientConfiguration;
  protected OChannelBinaryClient          network;
  protected String                        sessionId;
  protected final Map<String, Integer>    clustersIds       = new HashMap<String, Integer>();
  protected final Map<String, String>     clustersTypes     = new HashMap<String, String>();
  protected int                           defaultClusterId;
  protected int                           retry             = 0;

  public OStorageRemote(final String iURL, final String iMode) throws IOException {
    super(iURL, iURL, iMode);
    configuration = new OStorageConfiguration(this);
    clientConfiguration = new OClientConfiguration();
  }

  public void open(final int iRequesterId, final String iUserName, final String iUserPassword) {
    boolean locked = acquireExclusiveLock();

    try {
      userName = iUserName;
      userPassword = iUserPassword;

      openRemoteDatabase();
      addUser();

      configuration.load();

      cache.addUser();

      Orient.instance().registerStorage(this);

    } catch (Exception e) {
      close();
      if (e instanceof OException)
        throw (OException) e;
      else
        throw new OStorageException("Can't open the remote storage: " + name, e);
    } finally {

      releaseExclusiveLock(locked);
    }
  }

  public void create() {
    throw new UnsupportedOperationException(
        "Can't create a database in a remote server. Please use the console or the OServerAdmin class.");
  }

  public boolean exists() {
    checkConnection();

    do {
      boolean locked = acquireExclusiveLock();

      try {
        network.writeByte(OChannelBinaryProtocol.DB_EXIST);
        network.flush();

        readStatus();
        return network.readByte() == 1;
      } catch (Exception e) {
        if (handleException("Error on checking if the database exists", e))
          break;

      } finally {
        releaseExclusiveLock(locked);
      }
    } while (true);
    return false;
  }

  public void close() {
    boolean locked = acquireExclusiveLock();

    try {
      network.writeByte(OChannelBinaryProtocol.DB_CLOSE);
      network.out.flush();

      network.socket.close();

      cache.removeUser();
      cache.clear();

      open = false;

      Orient.instance().unregisterStorage(this);

    } catch (Exception e) {

    } finally {
      releaseExclusiveLock(locked);
    }
  }

  public Set<String> getClusterNames() {
    checkConnection();

    boolean locked = acquireSharedLock();

    try {
      return clustersIds.keySet();

    } finally {
      releaseSharedLock(locked);
    }
  }

  public long createRecord(final int iClusterId, final byte[] iContent, final byte iRecordType) {
    checkConnection();

    do {
      boolean locked = acquireExclusiveLock();

      try {
        network.writeByte(OChannelBinaryProtocol.RECORD_CREATE);
        network.writeShort((short) iClusterId);
        network.writeBytes(iContent);
        network.writeByte(iRecordType);
        network.flush();

        readStatus();
        return network.readLong();
      } catch (Exception e) {
        if (handleException("Error on create record in cluster: " + iClusterId, e))
          break;

      } finally {
        releaseExclusiveLock(locked);
      }
    } while (true);
    return -1;
  }

  public ORawBuffer readRecord(final ODatabaseRecord<?> iDatabase, final int iRequesterId, final int iClusterId,
      final long iPosition, final String iFetchPlan) {
    checkConnection();

    if (OStorageRemoteThreadLocal.INSTANCE.get())
      // PENDING NETWORK OPERATION, CAN'T EXECUTE IT NOW
      return null;

    do {
      boolean locked = acquireExclusiveLock();

      try {
        network.writeByte(OChannelBinaryProtocol.RECORD_LOAD);
        network.writeShort((short) iClusterId);
        network.writeLong(iPosition);
        network.writeString(iFetchPlan != null ? iFetchPlan : "");
        network.flush();

        readStatus();

        if (network.readByte() == 0)
          return null;

        final ORawBuffer buffer = new ORawBuffer(network.readBytes(), network.readInt(), network.readByte());

        while (network.readByte() == 2) {
          ORecordInternal<?> record = readRecordFromNetwork(iDatabase);
          // PUT IN THE CLIENT LOCAL CACHE
          cache.pushRecord(record.getIdentity().toString(),
              new ORawBuffer(record.toStream(), record.getVersion(), record.getRecordType()));
        }

        return buffer;

      } catch (Exception e) {
        if (handleException("Error on read record: " + iClusterId + ":" + iPosition, e))
          break;

      } finally {
        releaseExclusiveLock(locked);
      }
    } while (true);
    return null;
  }

  public int updateRecord(final int iRequesterId, final int iClusterId, final long iPosition, final byte[] iContent,
      final int iVersion, final byte iRecordType) {
    checkConnection();

    do {
      boolean locked = acquireExclusiveLock();

      try {
        network.writeByte(OChannelBinaryProtocol.RECORD_UPDATE);
        network.writeShort((short) iClusterId);
        network.writeLong(iPosition);
        network.writeBytes(iContent);
        network.writeInt(iVersion);
        network.writeByte(iRecordType);
        network.flush();

        readStatus();

        return network.readInt();

      } catch (Exception e) {
        if (handleException("Error on update record: " + iClusterId + ":" + iPosition, e))
          break;

      } finally {
        releaseExclusiveLock(locked);
      }
    } while (true);

    return -1;
  }

  public boolean deleteRecord(final int iRequesterId, final int iClusterId, final long iPosition, final int iVersion) {
    checkConnection();

    do {
      boolean locked = acquireExclusiveLock();

      try {
        network.writeByte(OChannelBinaryProtocol.RECORD_DELETE);
        network.writeShort((short) iClusterId);
        network.writeLong(iPosition);
        network.writeInt(iVersion);
        network.flush();

        readStatus();

        return network.readByte() == '1';
      } catch (Exception e) {
        if (handleException("Error on delete record: " + iClusterId + ":" + iPosition, e))
          break;

      } finally {
        releaseExclusiveLock(locked);
      }
    } while (true);
    return false;
  }

  public long count(final int iClusterId) {
    return count(new int[] { iClusterId });
  }

  public long[] getClusterDataRange(final int iClusterId) {
    checkConnection();

    do {
      boolean locked = acquireExclusiveLock();

      try {
        network.writeByte(OChannelBinaryProtocol.CLUSTER_DATARANGE);
        network.writeShort((short) iClusterId);
        network.flush();

        readStatus();
        return new long[] { network.readLong(), network.readLong() };
      } catch (Exception e) {
        if (handleException("Error on getting last entry position count in cluster: " + iClusterId, e))
          break;

      } finally {
        releaseExclusiveLock(locked);
      }
    } while (true);
    return null;
  }

  public long count(final int[] iClusterIds) {
    checkConnection();

    do {
      boolean locked = acquireExclusiveLock();

      try {
        network.writeByte(OChannelBinaryProtocol.CLUSTER_COUNT);
        network.writeShort((short) iClusterIds.length);
        for (int i = 0; i < iClusterIds.length; ++i)
          network.writeShort((short) iClusterIds[i]);
        network.flush();

        readStatus();
        return network.readLong();
      } catch (Exception e) {
        if (handleException("Error on read record count in clusters: " + iClusterIds, e))
          break;

      } finally {
        releaseExclusiveLock(locked);
      }
    } while (true);
    return -1;
  }

  public long count(final String iClassName) {
    checkConnection();

    do {
      boolean locked = acquireExclusiveLock();

      try {
        network.writeByte(OChannelBinaryProtocol.COUNT);
        network.writeString(iClassName);
        network.flush();

        readStatus();
        return network.readLong();
      } catch (Exception e) {
        if (handleException("Error on executing count on class: " + iClassName, e))
          break;

      } finally {
        releaseExclusiveLock(locked);
      }
    } while (true);
    return -1;
  }

  /**
   * Execute the command remotely and get the results back.
   */
  public Object command(final OCommandRequestText iCommand) {
    checkConnection();

    if (!(iCommand instanceof OSerializableStream))
      throw new OCommandExecutionException("Can't serialize the command to being executed to the server side.");

    OSerializableStream command = iCommand;

    Object result = null;

    do {
      boolean locked = acquireExclusiveLock();

      OStorageRemoteThreadLocal.INSTANCE.set(Boolean.TRUE);

      try {
        final OCommandRequestText aquery = (OCommandRequestText) iCommand;

        final boolean asynch = iCommand instanceof OCommandRequestAsynch;

        network.writeByte(OChannelBinaryProtocol.COMMAND);
        network.writeByte((byte) (asynch ? 'a' : 's')); // ASYNC / SYNC
        network.writeBytes(OStreamSerializerAnyStreamable.INSTANCE.toStream(command));
        network.flush();

        readStatus();

        if (asynch) {
          byte status;

          // ASYNCH: READ ONE RECORD AT TIME
          while ((status = network.readByte()) > 0) {
            ORecordSchemaAware<?> record = (ORecordSchemaAware<?>) readRecordFromNetwork(iCommand.getDatabase());
            if (record == null)
              break;

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
              break;

            case 2:
              // PUT IN THE CLIENT LOCAL CACHE
              cache.pushRecord(record.getIdentity().toString(),
                  new ORawBuffer(record.toStream(), record.getVersion(), record.getRecordType()));
            }
          }
        } else {
          final byte type = network.readByte();
          switch (type) {
          case 'n':
            result = null;
            break;

          case 'r':
            result = readRecordFromNetwork(iCommand.getDatabase());
            break;

          case 'a':
            result = OStreamSerializerAnyRuntime.INSTANCE.fromStream(network.readBytes());
            break;
          }
        }
        break;

      } catch (Exception e) {
        if (handleException("Error on executing command: " + iCommand, e))
          break;

      } finally {
        OStorageRemoteThreadLocal.INSTANCE.set(Boolean.FALSE);

        releaseExclusiveLock(locked);
      }
    } while (true);

    return result;
  }

  public void commit(final int iRequesterId, final OTransaction<?> iTx) {
    checkConnection();

    do {
      boolean locked = acquireExclusiveLock();

      try {
        network.writeByte(OChannelBinaryProtocol.TX_COMMIT);
        network.writeInt(iTx.getId());
        network.writeInt(iTx.size());

        for (OTransactionEntry<? extends ORecord<?>> txEntry : iTx.getEntries()) {
          if (txEntry.status == OTransactionEntry.LOADED)
            // JUMP LOADED OBJECTS
            continue;

          network.writeByte(txEntry.status);
          network.writeShort((short) txEntry.record.getIdentity().getClusterId());
          network.writeByte(txEntry.record.getRecordType());

          switch (txEntry.status) {
          case OTransactionEntry.CREATED:
            network.writeString(txEntry.clusterName);
            network.writeBytes(txEntry.record.toStream());
            break;

          case OTransactionEntry.UPDATED:
            network.writeLong(txEntry.record.getIdentity().getClusterPosition());
            network.writeInt(txEntry.record.getVersion());
            network.writeBytes(txEntry.record.toStream());
            break;

          case OTransactionEntry.DELETED:
            network.writeLong(txEntry.record.getIdentity().getClusterPosition());
            network.writeInt(txEntry.record.getVersion());
            break;
          }
        }
        network.flush();

        readStatus();
        break;
      } catch (Exception e) {
        if (handleException("Error on commit", e))
          break;

      } finally {
        releaseExclusiveLock(locked);
      }
    } while (true);
  }

  public int getClusterIdByName(final String iClusterName) {
    checkConnection();

    if (iClusterName == null)
      return -1;

    if (Character.isDigit(iClusterName.charAt(0)))
      return Integer.parseInt(iClusterName);

    boolean locked = acquireSharedLock();

    try {
      final Integer id = clustersIds.get(iClusterName.toLowerCase());
      if (id == null)
        return -1;

      return id;

    } finally {
      releaseSharedLock(locked);
    }
  }

  public String getClusterTypeByName(final String iClusterName) {
    checkConnection();

    if (iClusterName == null)
      return null;

    boolean locked = acquireSharedLock();

    try {
      return clustersTypes.get(iClusterName.toLowerCase());

    } finally {
      releaseSharedLock(locked);
    }
  }

  public int getDefaultClusterId() {
    return defaultClusterId;
  }

  public int addCluster(final String iClusterName, final OStorage.CLUSTER_TYPE iClusterType, final Object... iArguments) {
    checkConnection();

    do {
      boolean locked = acquireExclusiveLock();

      try {
        network.writeByte(OChannelBinaryProtocol.CLUSTER_ADD);
        network.writeString(iClusterType.toString());
        network.writeString(iClusterName);

        if (OClusterLocal.TYPE.equals(iClusterType)) {
          // FIEL PATH + START SIZE
          network.writeString(iArguments.length > 0 ? (String) iArguments[0] : "").writeInt(
              iArguments.length > 0 ? (Integer) iArguments[1] : -1);
        } else {
          // PHY CLUSTER ID
          network.writeInt(iArguments.length > 0 ? (Integer) iArguments[0] : -1);
        }

        network.flush();

        readStatus();

        int clusterId = network.readShort();
        clustersIds.put(iClusterName.toLowerCase(), clusterId);
        clustersTypes.put(iClusterName.toLowerCase(), iClusterType.toString());
        return clusterId;
      } catch (Exception e) {
        if (handleException("Error on add new cluster", e))
          break;

      } finally {
        releaseExclusiveLock(locked);
      }
    } while (true);
    return 0;
  }

  public boolean removeCluster(final int iClusterId) {
    checkConnection();

    do {
      boolean locked = acquireExclusiveLock();

      try {
        network.writeByte(OChannelBinaryProtocol.CLUSTER_REMOVE);
        network.writeShort((short) iClusterId);

        network.flush();

        readStatus();

        if (network.readByte() == '1') {
          // REMOVE THE CLUSTER LOCALLY
          for (Entry<String, Integer> entry : clustersIds.entrySet())
            if (entry.getValue() != null && entry.getValue().intValue() == iClusterId) {
              clustersIds.remove(entry.getKey());
              clustersTypes.remove(entry.getKey());
              break;
            }

          return true;
        }
        return false;
      } catch (Exception e) {
        if (handleException("Error on removing of cluster", e))
          break;

      } finally {
        releaseExclusiveLock(locked);
      }
    } while (true);
    return false;
  }

  public int addDataSegment(final String iDataSegmentName) {
    return addDataSegment(iDataSegmentName, null);
  }

  public int addDataSegment(final String iSegmentName, final String iSegmentFileName) {
    checkConnection();

    do {
      boolean locked = acquireExclusiveLock();

      try {
        network.writeByte(OChannelBinaryProtocol.DATASEGMENT_ADD);
        network.writeString(iSegmentName).writeString(iSegmentFileName);
        network.flush();

        readStatus();
        return network.readShort();
      } catch (Exception e) {
        if (handleException("Error on add new data segment", e))
          break;

      } finally {
        releaseExclusiveLock(locked);
      }
    } while (true);
    return 0;
  }

  public String getSessionId() {
    boolean locked = acquireSharedLock();

    try {
      return sessionId;

    } finally {
      releaseSharedLock(locked);
    }
  }

  public <REC extends ORecordInternal<?>> REC dictionaryPut(final ODatabaseRecord<REC> iDatabase, final String iKey,
      final ORecordInternal<?> iRecord) {
    checkConnection();

    do {
      boolean locked = acquireExclusiveLock();

      try {
        network.writeByte(OChannelBinaryProtocol.DICTIONARY_PUT);
        network.writeString(iKey);
        network.writeByte(iRecord.getRecordType());
        network.writeString(iRecord.getIdentity().toString());
        network.flush();

        readStatus();

        return (REC) readRecordFromNetwork(iDatabase);

      } catch (Exception e) {
        if (handleException("Error on insert record with key: " + iKey, e))
          break;

      } finally {
        releaseExclusiveLock(locked);
      }
    } while (true);
    return null;
  }

  public <REC extends ORecordInternal<?>> REC dictionaryLookup(ODatabaseRecord<REC> iDatabase, final String iKey) {
    checkConnection();

    do {
      boolean locked = acquireExclusiveLock();

      try {
        network.writeByte(OChannelBinaryProtocol.DICTIONARY_LOOKUP);
        network.writeString(iKey);
        network.flush();

        readStatus();

        return (REC) readRecordFromNetwork(iDatabase);

      } catch (Exception e) {
        if (handleException("Error on lookup record with key: " + iKey, e))
          break;

      } finally {
        releaseExclusiveLock(locked);
      }
    } while (true);
    return null;
  }

  public <REC extends ORecordInternal<?>> REC dictionaryRemove(ODatabaseRecord<REC> iDatabase, Object iKey) {
    checkConnection();

    do {
      boolean locked = acquireExclusiveLock();

      try {
        network.writeByte(OChannelBinaryProtocol.DICTIONARY_REMOVE);
        network.writeString(iKey.toString());
        network.flush();

        readStatus();

        return (REC) readRecordFromNetwork(iDatabase);

      } catch (Exception e) {
        if (handleException("Error on lookup record with key: " + iKey, e))
          break;

      } finally {
        releaseExclusiveLock(locked);
      }
    } while (true);
    return null;
  }

  public int dictionarySize(final ODatabaseRecord<?> iDatabase) {
    checkConnection();

    do {
      boolean locked = acquireExclusiveLock();

      try {
        network.writeByte(OChannelBinaryProtocol.DICTIONARY_SIZE);
        network.flush();

        readStatus();
        return network.readInt();
      } catch (Exception e) {
        if (handleException("Error on getting size of database's dictionary", e))
          break;

      } finally {
        releaseExclusiveLock(locked);
      }
    } while (true);
    return -1;
  }

  public ODictionary<?> createDictionary(final ODatabaseRecord<?> iDatabase) throws Exception {
    return new ODictionaryClient<Object>(iDatabase, this);
  }

  public Set<String> dictionaryKeys() {
    checkConnection();

    do {
      boolean locked = acquireExclusiveLock();

      try {
        network.writeByte(OChannelBinaryProtocol.DICTIONARY_KEYS);
        network.flush();

        readStatus();
        return network.readStringSet();
      } catch (Exception e) {
        if (handleException("Error on getting keys of database's dictionary", e))
          break;

      } finally {
        releaseExclusiveLock(locked);
      }
    } while (true);
    return null;
  }

  public void synch() {
  }

  public String getPhysicalClusterNameById(final int iClusterId) {
    for (Entry<String, Integer> clusterEntry : clustersIds.entrySet()) {
      if (clusterEntry.getValue().intValue() == iClusterId)
        return clusterEntry.getKey();
    }
    return null;
  }

  public Collection<OCluster> getClusters() {
    throw new UnsupportedOperationException("getClusters()");
  }

  public OCluster getClusterById(final int iId) {
    throw new UnsupportedOperationException("getClusterById()");
  }

  protected void readStatus() throws IOException {
    final byte result = network.readByte();

    if (result == OChannelBinaryProtocol.ERROR) {
      StringBuilder buffer = new StringBuilder();
      boolean moreDetails = false;
      String rootClassName = null;

      do {
        final String excClassName = network.readString();
        final String excMessage = network.readString();

        if (!moreDetails) {
          // FIRST ONE: TAKE AS ROOT CLASS/MSG
          rootClassName = excClassName;
        } else {
          // DETAIL: APPEND AS STRING SINCE EXCEPTIONS DON'T ALLOW TO BE REBUILT PROGRAMMATICALLY
          buffer.append("\n-> ");
          buffer.append(excClassName);
          buffer.append(": ");
        }
        buffer.append(excMessage);

        // READ IF MORE DETAILS ARE COMING
        moreDetails = network.readByte() == 1;

      } while (moreDetails);

      throw createException(rootClassName, buffer.toString());
    }
  }

  protected boolean handleException(final String iMessage, final Exception iException) {
    if (iException instanceof OException)
      // RE-THROW IT
      throw (OException) iException;

    if (!(iException instanceof IOException))
      throw new OStorageException(iMessage, iException);

    if (retry < clientConfiguration.connectionRetry) {
      // WAIT THE DELAY BEFORE TO RETRY
      try {
        Thread.sleep(clientConfiguration.connectionRetryDelay);
      } catch (InterruptedException e) {
      }

      try {
        if (OLogManager.instance().isDebugEnabled())
          OLogManager.instance().debug(this,
              "Retrying to connect to remote server #" + retry + "/" + clientConfiguration.connectionRetry + "...");

        openRemoteDatabase();

        retry = 0;

        OLogManager.instance().info(this,
            "Connection re-acquired in transparent way: no errors will be thrown at application level");

        return true;
      } catch (Throwable t) {
        ++retry;
      }
    } else {
      retry = 0;

      // RECONNECTION FAILED: THROW+LOG THE ORIGINAL EXCEPTION
      throw new OStorageException(iMessage, iException);
    }
    return false;
  }

  protected void openRemoteDatabase() throws IOException {
    // CONNECT TO THE SERVER
    parseServerURLs();
    createNetworkConnection();

    network.out.writeByte(OChannelBinaryProtocol.DB_OPEN);
    network.writeString(name).writeString(userName).writeString(userPassword);
    network.flush();

    readStatus();

    sessionId = network.readString();
    OLogManager.instance().debug(null, "Client connected with session id: " + sessionId);

    int tot = network.readInt();
    String clusterName;
    for (int i = 0; i < tot; ++i) {
      clusterName = network.readString().toLowerCase();
      clustersIds.put(clusterName, network.readInt());
      clustersTypes.put(clusterName, network.readString());
    }

    defaultClusterId = clustersIds.get(OStorage.CLUSTER_DEFAULT_NAME);

    open = true;
  }

  /**
   * Parse the URL in the following formats:<br/>
   * <ul>
   * <li>
   * 
   * <pre>
   * <protocol>:<>
   * </pre>
   * 
   * </li>
   * <li>
   * 
   * <pre>
   * <db-sename>
   * </pre>
   * 
   * , to connect to the localhost, default port 2424</li>
   * </ul>
   */
  protected void parseServerURLs() {
    String remoteHost;
    String[] remotePorts;

    int dbPos = url.indexOf("/");
    if (dbPos == -1) {
      // SHORT FORM
      name = url;
      remoteHost = getDefaultHost();
      remotePorts = getDefaultPort();
    } else {
      name = url.substring(dbPos + 1);

      int startPos = 0;
      int endPos = 0;

      while (endPos < dbPos) {
        if (url.indexOf(ADDRESS_SEPARATOR, startPos) > -1)
          endPos = url.indexOf(ADDRESS_SEPARATOR, startPos);
        else
          endPos = dbPos;

        int posRemotePort = url.indexOf(":", startPos);

        if (posRemotePort != -1 && posRemotePort < endPos) {
          remoteHost = url.substring(startPos, posRemotePort);
          remotePorts = url.substring(posRemotePort + 1, endPos).split("_");
          startPos = endPos + 1;
        } else {
          remoteHost = url.substring(startPos, endPos);
          remotePorts = getDefaultPort();
          startPos = endPos + 1;
        }

        // REGISTER THE REMOTE SERVER+PORT
        serverURLs.add(new OPair<String, String[]>(remoteHost, remotePorts));
      }
    }
  }

  protected String getDefaultHost() {
    return DEFAULT_HOST;
  }

  protected String[] getDefaultPort() {
    return DEFAULT_PORTS;
  }

  protected void createNetworkConnection() throws IOException, UnknownHostException {
    int port;

    for (OPair<String, String[]> server : serverURLs) {
      port = Integer.parseInt(server.getValue()[server.getValue().length - 1]);

      OLogManager.instance().debug(this, "Trying to connect to the remote host %s:%d...", server.getKey(), port);
      try {
        network = new OChannelBinaryClient(server.getKey(), port, clientConfiguration.connectionTimeout);
        return;
      } catch (Exception e) {
      }
    }

    final StringBuilder buffer = new StringBuilder();
    for (OPair<String, String[]> server : serverURLs) {
      if (buffer.length() > 0)
        buffer.append(',');
      buffer.append(server.getKey());
    }

    throw new OIOException("Can't connect to any configured remote nodes: " + buffer);
  }

  protected void checkConnection() {
    if (network == null)
      throw new ODatabaseException("Connection is closed");
  }

  private ORecordInternal<?> readRecordFromNetwork(final ODatabaseRecord<?> iDatabase) throws IOException {
    final int classId = network.readShort();
    if (classId == OChannelBinaryProtocol.RECORD_NULL)
      return null;

    ORecordInternal<?> record = ORecordFactory.newInstance(network.readByte());

    if (record instanceof ORecordSchemaAware<?>)
      ((ORecordSchemaAware<?>) record).fill(iDatabase, classId, network.readShort(), network.readLong(), network.readInt());
    else
      // DISCARD CLASS ID
      record.fill(iDatabase, network.readShort(), network.readLong(), network.readInt());

    record.fromStream(network.readBytes());

    return record;
  }

  private RuntimeException createException(final String iClassName, final String iMessage) {
    RuntimeException rootException = null;
    Constructor<?> c = null;
    try {
      final Class<RuntimeException> excClass = (Class<RuntimeException>) Class.forName(iClassName);
      c = excClass.getConstructor(String.class);
    } catch (Exception e) {
      // UNABLE TO REPRODUCE THE SAME SERVER-SIZE EXCEPTION: THROW A STORAGE EXCEPTION
      rootException = new OStorageException(iMessage, null);
    }

    if (c != null)
      try {
        rootException = (RuntimeException) c.newInstance(iMessage);
      } catch (InstantiationException e) {
      } catch (IllegalAccessException e) {
      } catch (InvocationTargetException e) {
      }

    return rootException;
  }
}
