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

import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.core.config.OStorageConfigurationUpdateListener;
import com.orientechnologies.orient.core.conflict.ORecordConflictStrategy;
import com.orientechnologies.orient.core.db.OrientDBInternal;
import com.orientechnologies.orient.core.db.record.OCurrentStorageComponentsFactory;
import com.orientechnologies.orient.core.db.tool.ODatabaseExport;
import com.orientechnologies.orient.core.db.tool.ODatabaseImport;
import com.orientechnologies.orient.core.exception.OInvalidIndexEngineIdException;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OIndexMetadata;
import com.orientechnologies.orient.core.index.engine.IndexEngineValuesTransformer;
import com.orientechnologies.orient.core.index.engine.OBaseIndexEngine;
import com.orientechnologies.orient.core.storage.cluster.OPaginatedCluster;
import com.orientechnologies.orient.core.storage.impl.local.OBackgroundNewDelta;
import com.orientechnologies.orient.core.storage.impl.local.OIndexEngineCallback;
import com.orientechnologies.orient.core.storage.ridbag.sbtree.OBonsaiCollectionPointer;
import com.orientechnologies.orient.core.storage.ridbag.sbtree.OSBTreeCollectionManager;
import com.orientechnologies.orient.core.transaction.ODatabaseId;
import com.orientechnologies.orient.core.transaction.OTransactionId;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.stream.Stream;

/**
 * This is the gateway interface between the Database side and the storage. Provided implementations
 * are: Local, Remote and Memory.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 * @see com.orientechnologies.orient.core.storage.memory.ODirectMemoryStorage
 */
public interface OStorage extends OStorageInfo {
  public String CLUSTER_DEFAULT_NAME = "default";

  public enum STATUS {
    CLOSED,
    OPEN,
    MIGRATION,
    CLOSING,
    @Deprecated
    OPENING,
  }

  public enum LOCKING_STRATEGY {
    NONE,
    DEFAULT,
    SHARED_LOCK,
    EXCLUSIVE_LOCK,

    @SuppressWarnings("DeprecatedIsStillUsed")
    @Deprecated
    KEEP_SHARED_LOCK,

    @SuppressWarnings("DeprecatedIsStillUsed")
    @Deprecated
    KEEP_EXCLUSIVE_LOCK
  }

  void create(OContextConfiguration contextConfiguration, ODatabaseId id);

  boolean exists();

  void reload();

  void delete();

  void close();

  boolean isClosed();

  ORawBuffer readRecord(ORecordId iRid);

  ORawBuffer readRecordIfVersionIsNotLatest(ORecordId rid, int recordVersion)
      throws ORecordNotFoundException;

  OStorageOperationResult<Boolean> deleteRecord(ORecordId iRecordId, int iVersion);

  ORecordMetadata getRecordMetadata(final ORID rid);

  boolean cleanOutRecord(ORecordId recordId, int recordVersion);

  // TX OPERATIONS
  void commit(OStorageTransaction iTx);

  Set<String> getClusterNames();

  Collection<? extends OClusterInfo> getClusterInstances();

  /**
   * Add a new cluster into the storage.
   *
   * @param iClusterName name of the cluster
   */
  int addCluster(String iClusterName);

  /**
   * Add a new cluster into the storage.
   *
   * @param iClusterName name of the cluster
   * @param iRequestedId requested id of the cluster
   */
  int addCluster(String iClusterName, int iRequestedId);

  boolean dropCluster(String iClusterName);

  String getClusterName(final int clusterId);

  boolean setClusterAttribute(final int id, OCluster.ATTRIBUTES attribute, Object value);

  /**
   * Drops a cluster.
   *
   * @param iId id of the cluster to delete
   * @return true if has been removed, otherwise false
   */
  boolean dropCluster(int iId);

  String getClusterNameById(final int clusterId);

  long getClusterRecordsSizeById(final int clusterId);

  long getClusterRecordsSizeByName(final String clusterName);

  String getClusterRecordConflictStrategy(final int clusterId);

  String getClusterEncryption(final int clusterId);

  boolean isSystemCluster(final int clusterId);

  long getLastClusterPosition(final int clusterId);

  long getClusterNextPosition(final int clusterId);

  OPaginatedCluster.RECORD_STATUS getRecordStatus(final ORID rid);

  long count(int iClusterId);

  long count(int[] iClusterIds);

  /** Returns the size of the database. */
  long getSize();

  /** Returns the total number of records. */
  long countRecords();

  void setDefaultClusterId(final int defaultClusterId);

  int getClusterIdByName(String iClusterName);

  String getPhysicalClusterNameById(int iClusterId);

  String getName();

  long getVersion();

  /** @return Version of product release under which storage was created. */
  String getCreatedAtVersion();

  void synch();

  /**
   * Returns a pair of long values telling the begin and end positions of data in the requested
   * cluster. Useful to know the range of the records.
   *
   * @param currentClusterId Cluster id
   */
  long[] getClusterDataRange(int currentClusterId);

  OPhysicalPosition[] higherPhysicalPositions(int clusterId, OPhysicalPosition physicalPosition);

  OPhysicalPosition[] lowerPhysicalPositions(int clusterId, OPhysicalPosition physicalPosition);

  OPhysicalPosition[] ceilingPhysicalPositions(int clusterId, OPhysicalPosition physicalPosition);

  OPhysicalPosition[] floorPhysicalPositions(int clusterId, OPhysicalPosition physicalPosition);

  /** Returns the current storage's status */
  STATUS getStatus();

  /** Returns the storage's type. */
  String getType();

  boolean isAssigningClusterIds();

  OSBTreeCollectionManager getSBtreeCollectionManager();

  OCurrentStorageComponentsFactory getComponentsFactory();

  ORecordConflictStrategy getRecordConflictStrategy();

  void setConflictStrategy(ORecordConflictStrategy iResolver);

  /** @return Backup file name */
  String incrementalBackup(String backupDirectory, OCallable<Void, Void> started)
      throws UnsupportedOperationException;

  boolean supportIncremental();

  void fullIncrementalBackup(OutputStream stream) throws UnsupportedOperationException;

  void restoreFromIncrementalBackup(String filePath);

  void restoreFullIncrementalBackup(InputStream stream) throws UnsupportedOperationException;

  /**
   * This method is called in {@link com.orientechnologies.orient.core.Orient#shutdown()} method.
   * For most of the storages it means that storage will be merely closed, but sometimes additional
   * operations are need to be taken in account.
   */
  void shutdown();

  void setSchemaRecordId(String schemaRecordId);

  void setDateFormat(String dateFormat);

  void setTimeZone(TimeZone timeZoneValue);

  void setLocaleLanguage(String locale);

  void setCharset(String charset);

  void setIndexMgrRecordId(String indexMgrRecordId);

  void setDateTimeFormat(String dateTimeFormat);

  void setLocaleCountry(String localeCountry);

  void setClusterSelection(String clusterSelection);

  void setMinimumClusters(int minimumClusters);

  void setValidation(boolean validation);

  void removeProperty(String property);

  void setProperty(String property, String value);

  void setRecordSerializer(String recordSerializer, int version);

  void clearProperties();

  int[] getClustersIds(Set<String> filterClusters);

  default boolean isIcrementalBackupRunning() {
    return false;
  }

  OrientDBInternal getContext();

  Optional<byte[]> getLastMetadata();

  void open(OContextConfiguration configurations);

  void setConfigurationUpdateListener(
      final OStorageConfigurationUpdateListener storageConfigurationUpdateListener);

  void startDDL();

  void endDDL();

  void fireConfigurationUpdateNotifications();

  void pauseConfigurationUpdateNotifications();

  void preallocateRids(final OAllocationTransaction clientTx);

  void commitPreAllocated(final OStorageTransaction clientTx);

  void acquireWriteLock(final ORID rid, long timeout);

  void releaseWriteLock(final ORID rid);

  void acquireReadLock(final ORID rid, long timeout);

  void releaseReadLock(final ORID rid);

  boolean wereDataRestoredAfterOpen();

  boolean wereNonTxOperationsPerformedInPreviousOpen();

  boolean hasIndexRangeQuerySupport(int indexId) throws OInvalidIndexEngineIdException;

  int addIndexEngine(
      final OIndexMetadata indexMetadata, final Map<String, String> engineProperties);

  int loadIndexEngine(final OIndexMetadata indexMetadata);

  void clearIndex(final int indexId) throws OInvalidIndexEngineIdException;

  void deleteIndexEngine(int indexId) throws OInvalidIndexEngineIdException;

  Stream<ORawPair<Object, ORID>> getIndexStream(
      int indexId, final IndexEngineValuesTransformer valuesTransformer)
      throws OInvalidIndexEngineIdException;

  <T> T callIndexEngine(
      final boolean readOperation, int indexId, final OIndexEngineCallback<T> callback)
      throws OInvalidIndexEngineIdException;

  Stream<Object> getIndexKeyStream(int indexId) throws OInvalidIndexEngineIdException;

  OBaseIndexEngine getIndexEngine(int indexId) throws OInvalidIndexEngineIdException;

  void removeIndexValuesContainer(OIndexMetadata im);

  Stream<ORawPair<Object, ORID>> iterateIndexEntriesBetween(
      int indexId,
      final Object rangeFrom,
      final boolean fromInclusive,
      final Object rangeTo,
      final boolean toInclusive,
      final boolean ascSortOrder,
      final IndexEngineValuesTransformer transformer)
      throws OInvalidIndexEngineIdException;

  Stream<ORawPair<Object, ORID>> iterateIndexEntriesMajor(
      int indexId,
      final Object fromKey,
      final boolean isInclusive,
      final boolean ascSortOrder,
      final IndexEngineValuesTransformer transformer)
      throws OInvalidIndexEngineIdException;

  Stream<ORawPair<Object, ORID>> iterateIndexEntriesMinor(
      int indexId,
      final Object toKey,
      final boolean isInclusive,
      final boolean ascSortOrder,
      final IndexEngineValuesTransformer transformer)
      throws OInvalidIndexEngineIdException;

  Stream<ORID> getIndexValues(int indexId, final Object key) throws OInvalidIndexEngineIdException;

  long getIndexSize(int indexId, final IndexEngineValuesTransformer transformer)
      throws OInvalidIndexEngineIdException;

  Stream<ORawPair<Object, ORID>> getIndexDescStream(
      int indexId, final IndexEngineValuesTransformer valuesTransformer)
      throws OInvalidIndexEngineIdException;

  int getVersionForKey(final String indexName, final Object key);

  void metadataOnly(byte[] metadata);

  boolean isMemory();

  Optional<OBackgroundNewDelta> extractTransactionsFromWal(
      List<OTransactionId> transactionsMetadata);

  void backupTransactions(OutputStream output, List<OTransactionId> transactionsMetadata);

  boolean check(final boolean verbose, final OCommandOutputListener listener);

  OBinarySerializer<?> resolveObjectSerializer(final byte serializerId);

  OBonsaiCollectionPointer createSBTree(int clusterId, UUID ownerUUID);

  boolean isDeleted(final ORID rid);

  void incrementalSync(OutputStream dest, Runnable started);

  Optional<Path> getPath();

  ODatabaseId getDatabaseId();

  int getIndexApiVersion(int indexId);

  /**
   * Executes a backup of the database. During the backup the database will be frozen in read-only
   * mode.
   *
   * @param out OutputStream used to write the backup content. Use a FileOutputStream to make the
   *     backup persistent on disk
   * @param options Backup options as Map<String, Object> object
   * @param callable Callback to execute when the database is locked
   * @param iListener Listener called for backup messages
   * @param compressionLevel ZIP Compression level between 1 (the minimum) and 9 (maximum). The
   *     bigger is the compression, the smaller will be the final backup content, but will consume
   *     more CPU and time to execute
   * @param bufferSize Buffer size in bytes, the bigger is the buffer, the more efficient will be
   *     the compression
   * @throws IOException
   * @see ODatabaseExport
   */
  List<String> backup(
      OutputStream out,
      Map<String, Object> options,
      Callable<Object> callable,
      OCommandOutputListener iListener,
      int compressionLevel,
      int bufferSize)
      throws IOException;

  /**
   * Executes a restore of a database backup. During the restore the database will be frozen in
   * read-only mode.
   *
   * @param in InputStream used to read the backup content. Use a FileInputStream to read a backup
   *     on a disk
   * @param options Backup options as Map<String, Object> object
   * @param callable Callback to execute when the database is locked
   * @param iListener Listener called for backup messages
   * @throws IOException
   * @see ODatabaseImport
   */
  void restore(
      InputStream in,
      Map<String, Object> options,
      Callable<Object> callable,
      OCommandOutputListener iListener)
      throws IOException;
}
