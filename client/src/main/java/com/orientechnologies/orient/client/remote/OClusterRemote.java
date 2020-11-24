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

import com.orientechnologies.orient.core.config.OStorageClusterConfiguration;
import com.orientechnologies.orient.core.conflict.ORecordConflictStrategy;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.storage.OCluster;
import com.orientechnologies.orient.core.storage.OPhysicalPosition;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.impl.local.OClusterBrowsePage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;

/**
 * Remote cluster implementation
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OClusterRemote implements OCluster {
  private String name;
  private int id;

  /*
   * (non-Javadoc)
   *
   * @see com.orientechnologies.orient.core.storage.OCluster#configure(com.orientechnologies.orient.core.storage.OStorage, int,
   * java.lang.String, java.lang.String, int, java.lang.Object[])
   */
  public void configure(int iId, String iClusterName) {
    id = iId;
    name = iClusterName;
  }

  /*
   * (non-Javadoc)
   *
   * @see com.orientechnologies.orient.core.storage.OCluster#configure(com.orientechnologies.orient.core.storage.OStorage,
   * com.orientechnologies.orient.core.config.OStorageClusterConfiguration)
   */
  public void configure(OStorage iStorage, OStorageClusterConfiguration iConfig) {
    id = iConfig.getId();
    name = iConfig.getName();
  }

  /*
   * (non-Javadoc)
   *
   * @see com.orientechnologies.orient.core.storage.OCluster#create(int)
   */
  public void create(OAtomicOperation atomicOperation) {}

  /*
   * (non-Javadoc)
   *
   * @see com.orientechnologies.orient.core.storage.OCluster#open()
   */
  public void open(OAtomicOperation atomicOperation) {}

  public void close() {}

  @Override
  public void close(boolean flush) {}

  @Override
  public OPhysicalPosition allocatePosition(byte recordType, OAtomicOperation atomicOperation) {
    throw new UnsupportedOperationException("allocatePosition");
  }

  @Override
  public OPhysicalPosition createRecord(
      byte[] content,
      int recordVersion,
      byte recordType,
      OPhysicalPosition allocatedPosition,
      OAtomicOperation atomicOperation) {
    throw new UnsupportedOperationException("createRecord");
  }

  @Override
  public boolean deleteRecord(OAtomicOperation atomicOperation, long clusterPosition) {
    throw new UnsupportedOperationException("deleteRecord");
  }

  @Override
  public void updateRecord(
      long clusterPosition,
      byte[] content,
      int recordVersion,
      byte recordType,
      OAtomicOperation atomicOperation) {
    throw new UnsupportedOperationException("updateRecord");
  }

  @Override
  public ORawBuffer readRecord(long clusterPosition, boolean prefetchRecords) {
    throw new UnsupportedOperationException("readRecord");
  }

  @Override
  public ORawBuffer readRecordIfVersionIsNotLatest(long clusterPosition, int recordVersion)
      throws ORecordNotFoundException {
    throw new UnsupportedOperationException("readRecordIfVersionIsNotLatest");
  }

  @Override
  public void setClusterName(final String name) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setRecordConflictStrategy(final String conflictStrategy) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setEncryption(final String encryptionName, final String encryptionKey) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean exists() {
    throw new UnsupportedOperationException("exists");
  }

  public void delete(OAtomicOperation atomicOperation) {}

  public Object set(ATTRIBUTES iAttribute, Object iValue) {
    return null;
  }

  @Override
  public String encryption() {
    throw new UnsupportedOperationException("encryption");
  }

  public OPhysicalPosition getPhysicalPosition(OPhysicalPosition iPPosition) {
    return null;
  }

  public long getEntries() {
    return 0;
  }

  @Override
  public long getTombstonesCount() {
    throw new UnsupportedOperationException("getTombstonesCount()");
  }

  @Override
  public long getFirstPosition() {
    return 0;
  }

  @Override
  public long getLastPosition() {
    return 0;
  }

  @Override
  public long getNextPosition() {
    return 0;
  }

  @Override
  public String getFileName() {
    throw new UnsupportedOperationException("getFileName()");
  }

  public int getId() {
    return id;
  }

  public void synch() {}

  public String getName() {
    return name;
  }

  public long getRecordsSize() {
    throw new UnsupportedOperationException("getRecordsSize()");
  }

  @Override
  public boolean isSystemCluster() {
    return false;
  }

  @Override
  public OPhysicalPosition[] higherPositions(OPhysicalPosition position) {
    throw new UnsupportedOperationException("higherPositions()");
  }

  @Override
  public OPhysicalPosition[] lowerPositions(OPhysicalPosition position) {
    throw new UnsupportedOperationException("lowerPositions()");
  }

  @Override
  public OPhysicalPosition[] ceilingPositions(OPhysicalPosition position) {
    throw new UnsupportedOperationException("ceilingPositions()");
  }

  @Override
  public OPhysicalPosition[] floorPositions(OPhysicalPosition position) {
    throw new UnsupportedOperationException("floorPositions()");
  }

  @Override
  public boolean isDeleted(OPhysicalPosition iPPosition) {
    throw new UnsupportedOperationException("isDeleted()");
  }

  @Override
  public String compression() {
    throw new UnsupportedOperationException("compression()");
  }

  @Override
  public ORecordConflictStrategy getRecordConflictStrategy() {
    return null;
  }

  @Override
  public void acquireAtomicExclusiveLock() {
    throw new UnsupportedOperationException("remote cluster doesn't support atomic locking");
  }

  @Override
  public OClusterBrowsePage nextPage(long lastPosition) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getBinaryVersion() {
    throw new UnsupportedOperationException(
        "Operation is not supported for given cluster implementation");
  }
}
