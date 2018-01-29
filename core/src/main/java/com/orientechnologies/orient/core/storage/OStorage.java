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
package com.orientechnologies.orient.core.storage;

import com.orientechnologies.common.concur.resource.OSharedContainer;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.core.config.OStorageConfiguration;
import com.orientechnologies.orient.core.conflict.ORecordConflictStrategy;
import com.orientechnologies.orient.core.db.record.OCurrentStorageComponentsFactory;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.storage.ridbag.sbtree.OSBTreeCollectionManager;
import com.orientechnologies.orient.core.tx.OTransactionInternal;
import com.orientechnologies.orient.core.util.OBackupable;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;

/**
 * This is the gateway interface between the Database side and the storage. Provided implementations are: Local, Remote and Memory.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 * @see com.orientechnologies.orient.core.storage.impl.memory.ODirectMemoryStorage
 */

public interface OStorage extends OBackupable, OSharedContainer {
  String CLUSTER_DEFAULT_NAME = "default";

  enum STATUS {
    CLOSED, OPEN, CLOSING, @Deprecated OPENING
  }

  enum LOCKING_STRATEGY {
    NONE, DEFAULT, SHARED_LOCK, EXCLUSIVE_LOCK,

    @SuppressWarnings("DeprecatedIsStillUsed") @Deprecated KEEP_SHARED_LOCK,

    @SuppressWarnings("DeprecatedIsStillUsed") @Deprecated KEEP_EXCLUSIVE_LOCK
  }

  void open(String iUserName, String iUserPassword, final OContextConfiguration contextConfiguration);

  void create(OContextConfiguration contextConfiguration) throws IOException;

  boolean exists();

  void reload();

  void delete();

  void close();

  void close(boolean iForce, boolean onDelete);

  boolean isClosed();

  // CRUD OPERATIONS
  OStorageOperationResult<OPhysicalPosition> createRecord(ORecordId iRecordId, byte[] iContent, int iRecordVersion,
      byte iRecordType, int iMode, ORecordCallback<Long> iCallback);

  OStorageOperationResult<ORawBuffer> readRecord(ORecordId iRid, String iFetchPlan, boolean iIgnoreCache, boolean prefetchRecords,
      ORecordCallback<ORawBuffer> iCallback);

  OStorageOperationResult<ORawBuffer> readRecordIfVersionIsNotLatest(ORecordId rid, String fetchPlan, boolean ignoreCache,
      int recordVersion) throws ORecordNotFoundException;

  OStorageOperationResult<Integer> updateRecord(ORecordId iRecordId, boolean updateContent, byte[] iContent, int iVersion,
      byte iRecordType, int iMode, ORecordCallback<Integer> iCallback);

  /**
   * Resurrects a record that was previously deleted, with a new content.
   */
  OStorageOperationResult<Integer> recyclePosition(ORecordId iRecordId, byte[] iContent, int iVersion, byte iRecordType);

  OStorageOperationResult<Boolean> deleteRecord(ORecordId iRecordId, int iVersion, int iMode, ORecordCallback<Boolean> iCallback);

  ORecordMetadata getRecordMetadata(final ORID rid);

  boolean cleanOutRecord(ORecordId recordId, int recordVersion, int iMode, ORecordCallback<Boolean> callback);

  // TX OPERATIONS
  List<ORecordOperation> commit(OTransactionInternal iTx);

  // TX OPERATIONS
  void rollback(OTransactionInternal iTx);

  // MISC
  OStorageConfiguration getConfiguration();

  int getClusters();

  Set<String> getClusterNames();

  OCluster getClusterById(int iId);

  Collection<? extends OCluster> getClusterInstances();

  /**
   * Add a new cluster into the storage.
   *
   * @param iClusterName name of the cluster
   */
  int addCluster(String iClusterName, Object... iParameters);

  /**
   * Add a new cluster into the storage.
   *  @param iClusterName name of the cluster
   * @param iRequestedId requested id of the cluster
   */
  int addCluster(String iClusterName, int iRequestedId, Object... iParameters);

  boolean dropCluster(String iClusterName, final boolean iTruncate);

  /**
   * Drops a cluster.
   *
   * @param iId id of the cluster to delete
   *
   * @return true if has been removed, otherwise false
   */
  boolean dropCluster(int iId, final boolean iTruncate);

  long count(int iClusterId);

  long count(int iClusterId, boolean countTombstones);

  long count(int[] iClusterIds);

  long count(int[] iClusterIds, boolean countTombstones);

  /**
   * Returns the size of the database.
   */
  long getSize();

  /**
   * Returns the total number of records.
   */
  long countRecords();

  int getDefaultClusterId();

  void setDefaultClusterId(final int defaultClusterId);

  int getClusterIdByName(String iClusterName);

  String getPhysicalClusterNameById(int iClusterId);

  boolean checkForRecordValidity(OPhysicalPosition ppos);

  String getName();

  String getURL();

  long getVersion();

  /**
   * @return Version of product release under which storage was created.
   */
  String getCreatedAtVersion();

  void synch();

  /**
   * Execute the command request and return the result back.
   */
  Object command(OCommandRequestText iCommand);

  /**
   * Returns a pair of long values telling the begin and end positions of data in the requested cluster. Useful to know the range of
   * the records.
   *
   * @param currentClusterId Cluster id
   */
  long[] getClusterDataRange(int currentClusterId);

  <V> V callInLock(Callable<V> iCallable, boolean iExclusiveLock);

  OPhysicalPosition[] higherPhysicalPositions(int clusterId, OPhysicalPosition physicalPosition);

  OPhysicalPosition[] lowerPhysicalPositions(int clusterId, OPhysicalPosition physicalPosition);

  OPhysicalPosition[] ceilingPhysicalPositions(int clusterId, OPhysicalPosition physicalPosition);

  OPhysicalPosition[] floorPhysicalPositions(int clusterId, OPhysicalPosition physicalPosition);

  /**
   * Returns the current storage's status
   */
  STATUS getStatus();

  /**
   * Returns the storage's type.
   */
  String getType();

  OStorage getUnderlying();

  boolean isRemote();

  boolean isDistributed();

  boolean isAssigningClusterIds();

  OSBTreeCollectionManager getSBtreeCollectionManager();

  OCurrentStorageComponentsFactory getComponentsFactory();

  OStorageOperationResult<Boolean> hideRecord(ORecordId recordId, int mode, ORecordCallback<Boolean> callback);

  OCluster getClusterByName(String clusterName);

  ORecordConflictStrategy getConflictStrategy();

  void setConflictStrategy(ORecordConflictStrategy iResolver);

  /**
   * @return Backup file name
   */
  String incrementalBackup(String backupDirectory);

  void restoreFromIncrementalBackup(String filePath);

  /**
   * This method is called in {@link com.orientechnologies.orient.core.Orient#shutdown()} method. For most of the storages it means
   * that storage will be merely closed, but sometimes additional operations are need to be taken in account.
   */
  void shutdown();
}
