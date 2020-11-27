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

import com.orientechnologies.orient.core.config.OStorageClusterConfiguration;
import com.orientechnologies.orient.core.conflict.ORecordConflictStrategy;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.storage.impl.local.OClusterBrowsePage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;
import java.io.IOException;

public interface OCluster {

  enum ATTRIBUTES {
    NAME,
    CONFLICTSTRATEGY,
    STATUS,
    @Deprecated
    ENCRYPTION
  }

  void configure(int iId, String iClusterName) throws IOException;

  void configure(OStorage iStorage, OStorageClusterConfiguration iConfig) throws IOException;

  void create(OAtomicOperation atomicOperation) throws IOException;

  void open(OAtomicOperation atomicOperation) throws IOException;

  void close() throws IOException;

  void close(boolean flush) throws IOException;

  void delete(OAtomicOperation atomicOperation) throws IOException;

  void setClusterName(String name);

  void setRecordConflictStrategy(String conflictStrategy);

  void setEncryption(String encryptionName, String encryptionKey);

  String encryption();

  long getTombstonesCount();

  /**
   * Allocates a physical position pointer on the storage for generate an id without a content.
   *
   * @param recordType the type of record of which allocate the position.
   * @return the allocated position.
   */
  OPhysicalPosition allocatePosition(final byte recordType, final OAtomicOperation atomicOperation)
      throws IOException;

  /**
   * Creates a new record in the cluster.
   *
   * @param content the content of the record.
   * @param recordVersion the current version
   * @param recordType the type of the record
   * @param allocatedPosition the eventual allocated position or null if there is no allocated
   *     position.
   * @return the position where the record si created.
   */
  OPhysicalPosition createRecord(
      byte[] content,
      int recordVersion,
      byte recordType,
      OPhysicalPosition allocatedPosition,
      OAtomicOperation atomicOperation);

  boolean deleteRecord(OAtomicOperation atomicOperation, long clusterPosition);

  void updateRecord(
      long clusterPosition,
      byte[] content,
      int recordVersion,
      byte recordType,
      OAtomicOperation atomicOperation);

  ORawBuffer readRecord(long clusterPosition, boolean prefetchRecords) throws IOException;

  ORawBuffer readRecordIfVersionIsNotLatest(long clusterPosition, int recordVersion)
      throws IOException, ORecordNotFoundException;

  boolean exists();

  /**
   * Fills and return the PhysicalPosition object received as parameter with the physical position
   * of logical record iPosition
   */
  OPhysicalPosition getPhysicalPosition(OPhysicalPosition iPPosition) throws IOException;

  /**
   * Check if a rid is existent and deleted or not existent return true only if delete flag is set.
   */
  boolean isDeleted(OPhysicalPosition iPPosition) throws IOException;

  long getEntries();

  long getFirstPosition() throws IOException;

  long getLastPosition() throws IOException;

  long getNextPosition() throws IOException;

  String getFileName();

  int getId();

  void synch() throws IOException;

  String getName();

  /** Returns the size of the records contained in the cluster in bytes. */
  long getRecordsSize() throws IOException;

  String compression();

  boolean isSystemCluster();

  OPhysicalPosition[] higherPositions(OPhysicalPosition position) throws IOException;

  OPhysicalPosition[] ceilingPositions(OPhysicalPosition position) throws IOException;

  OPhysicalPosition[] lowerPositions(OPhysicalPosition position) throws IOException;

  OPhysicalPosition[] floorPositions(OPhysicalPosition position) throws IOException;

  ORecordConflictStrategy getRecordConflictStrategy();

  /**
   * Acquires exclusive lock in the active atomic operation running on the current thread for this
   * cluster.
   */
  void acquireAtomicExclusiveLock();

  OClusterBrowsePage nextPage(long lastPosition) throws IOException;

  int getBinaryVersion();
}
