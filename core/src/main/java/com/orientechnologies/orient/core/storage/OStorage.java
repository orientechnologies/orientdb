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
package com.orientechnologies.orient.core.storage;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

import com.orientechnologies.common.concur.resource.OSharedContainer;
import com.orientechnologies.common.concur.resource.OSharedResourceAdaptiveExternal;
import com.orientechnologies.orient.core.cache.OLevel2RecordCache;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.config.OStorageConfiguration;
import com.orientechnologies.orient.core.id.OClusterPosition;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.tx.OTransaction;
import com.orientechnologies.orient.core.util.OBackupable;
import com.orientechnologies.orient.core.version.ORecordVersion;

/**
 * This is the gateway interface between the Database side and the storage. Provided implementations are: Local, Remote and Memory.
 * 
 * @author Luca Garulli
 * @see com.orientechnologies.orient.core.storage.impl.local.OStorageLocal
 * @see com.orientechnologies.orient.core.storage.impl.memory.OStorageMemory
 */
public interface OStorage extends OBackupable, OSharedContainer {
  public static final String DATA_DEFAULT_NAME    = "default";
  public static final String CLUSTER_DEFAULT_NAME = "default";

  public enum CLUSTER_TYPE {
    PHYSICAL, MEMORY
  }

  public enum SIZE {
    TINY, MEDIUM, LARGE, HUGE
  }

  public enum STATUS {
    CLOSED, OPEN, CLOSING
  }

  public void open(String iUserName, String iUserPassword, final Map<String, Object> iProperties);

  public void create(Map<String, Object> iProperties);

  public boolean exists();

  public void reload();

  public void delete();

  public void close();

  public void close(boolean iForce);

  public boolean isClosed();

  /**
   * Returns the level1 cache. Cannot be null.
   * 
   * @return Current cache.
   */
  public OLevel2RecordCache getLevel2Cache();

  public OSharedResourceAdaptiveExternal getLock();

  // CRUD OPERATIONS
  public OStorageOperationResult<OPhysicalPosition> createRecord(int iDataSegmentId, ORecordId iRecordId, byte[] iContent,
      ORecordVersion iRecordVersion, byte iRecordType, int iMode, ORecordCallback<OClusterPosition> iCallback);

  public OStorageOperationResult<ORawBuffer> readRecord(ORecordId iRid, String iFetchPlan, boolean iIgnoreCache,
      ORecordCallback<ORawBuffer> iCallback, boolean loadTombstones);

  public OStorageOperationResult<ORecordVersion> updateRecord(ORecordId iRecordId, byte[] iContent, ORecordVersion iVersion,
      byte iRecordType, int iMode, ORecordCallback<ORecordVersion> iCallback);

  public OStorageOperationResult<Boolean> deleteRecord(ORecordId iRecordId, ORecordVersion iVersion, int iMode,
      ORecordCallback<Boolean> iCallback);

  public boolean updateReplica(final int dataSegmentId, final ORecordId rid, final byte[] content,
      final ORecordVersion recordVersion, final byte recordType) throws IOException;

  public ORecordMetadata getRecordMetadata(final ORID rid);

  public boolean cleanOutRecord(ORecordId recordId, ORecordVersion recordVersion, int iMode, ORecordCallback<Boolean> callback);

  // TX OPERATIONS
  public void commit(OTransaction iTx, Runnable callback);

  // TX OPERATIONS
  public void rollback(OTransaction iTx);

  // MISC
  public OStorageConfiguration getConfiguration();

  public int getClusters();

  public Set<String> getClusterNames();

  public OCluster getClusterById(int iId);

  public Collection<? extends OCluster> getClusterInstances();

  /**
   * Add a new cluster into the storage.
   * 
   * 
   * @param iClusterType
   *          Cluster type. Type depends by the implementation.
   * @param iClusterName
   *          name of the cluster
   * @param iLocation
   *          Location where to store the cluster
   * @param iDataSegmentName
   *          Name of the data-segment to use. null means 'default'
   * @param forceListBased
   * @param iParameters
   */
  public int addCluster(String iClusterType, String iClusterName, String iLocation, String iDataSegmentName,
      boolean forceListBased, Object... iParameters);

  /**
   * Add a new cluster into the storage.
   * 
   * 
   * @param iClusterType
   *          Cluster type. Type depends by the implementation.
   * @param iClusterName
   *          name of the cluster
   * @param iRequestedId
   *          requested id of the cluster
   * @param iLocation
   *          Location where to store the cluster
   * @param iDataSegmentName
   *          Name of the data-segment to use. null means 'default'
   * @param forceListBased
   * @param iParameters
   */
  public int addCluster(String iClusterType, String iClusterName, int iRequestedId, String iLocation, String iDataSegmentName,
      boolean forceListBased, Object... iParameters);

  public boolean dropCluster(String iClusterName, final boolean iTruncate);

  /**
   * Drops a cluster.
   * 
   * @param iId
   * @return true if has been removed, otherwise false
   */
  public boolean dropCluster(int iId, final boolean iTruncate);

  /**
   * Add a new data segment in the default segment directory and with filename equals to the cluster name.
   */
  public int addDataSegment(String iDataSegmentName);

  public int addDataSegment(String iSegmentName, String iDirectory);

  public long count(int iClusterId);

  public long count(int iClusterId, boolean countTombstones);

  public long count(int[] iClusterIds);

  public long count(int[] iClusterIds, boolean countTombstones);

  /**
   * Returns the size of the database.
   */
  public long getSize();

  /**
   * Returns the total number of records.
   */
  public long countRecords();

  public int getDefaultClusterId();

  public void setDefaultClusterId(final int defaultClusterId);

  public int getClusterIdByName(String iClusterName);

  public String getClusterTypeByName(String iClusterName);

  public String getPhysicalClusterNameById(int iClusterId);

  public boolean checkForRecordValidity(OPhysicalPosition ppos);

  public String getName();

  public String getURL();

  public long getVersion();

  public void synch();

  public int getUsers();

  public int addUser();

  public int removeUser();

  /**
   * Execute the command request and return the result back.
   */
  public Object command(OCommandRequestText iCommand);

  /**
   * Returns a pair of long values telling the begin and end positions of data in the requested cluster. Useful to know the range of
   * the records.
   * 
   * @param currentClusterId
   *          Cluster id
   */
  public OClusterPosition[] getClusterDataRange(int currentClusterId);

  public <V> V callInLock(Callable<V> iCallable, boolean iExclusiveLock);

  public <V> V callInRecordLock(Callable<V> iCallable, ORID rid, boolean iExclusiveLock);

  public ODataSegment getDataSegmentById(int iDataSegmentId);

  public int getDataSegmentIdByName(String iDataSegmentName);

  public boolean dropDataSegment(String iName);

  OPhysicalPosition[] higherPhysicalPositions(int clusterId, OPhysicalPosition physicalPosition);

  OPhysicalPosition[] lowerPhysicalPositions(int clusterId, OPhysicalPosition physicalPosition);

  OPhysicalPosition[] ceilingPhysicalPositions(int clusterId, OPhysicalPosition physicalPosition);

  OPhysicalPosition[] floorPhysicalPositions(int clusterId, OPhysicalPosition physicalPosition);

  /**
   * Returns the current storage's status
   * 
   * @return
   */
  public STATUS getStatus();

  /**
   * Returns the storage's type.
   * 
   * @return
   */
  public String getType();

  public void checkForClusterPermissions(final String iClusterName);

  public OStorage getUnderlying();

  public boolean isDistributed();
}
