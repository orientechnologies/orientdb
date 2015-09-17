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
package com.orientechnologies.orient.core.storage;

import com.orientechnologies.common.concur.lock.OModificationLock;
import com.orientechnologies.orient.core.config.OStorageClusterConfiguration;
import com.orientechnologies.orient.core.conflict.ORecordConflictStrategy;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.version.ORecordVersion;

import java.io.IOException;

public interface OCluster {

  enum ATTRIBUTES {
    NAME, USE_WAL, RECORD_GROW_FACTOR, RECORD_OVERFLOW_GROW_FACTOR, COMPRESSION, CONFLICTSTRATEGY, STATUS
  }

  void configure(OStorage iStorage, int iId, String iClusterName, Object... iParameters) throws IOException;

  void configure(OStorage iStorage, OStorageClusterConfiguration iConfig) throws IOException;

  void create(int iStartSize) throws IOException;

  void open() throws IOException;

  void close() throws IOException;

  void close(boolean flush) throws IOException;

  void delete() throws IOException;

  OModificationLock getExternalModificationLock();

  Object set(ATTRIBUTES iAttribute, Object iValue) throws IOException;

  void convertToTombstone(long iPosition) throws IOException;

  long getTombstonesCount();

  boolean hasTombstonesSupport();

  /**
   * Truncates the cluster content. All the entries will be removed.
   *
   * @throws IOException
   */
  void truncate() throws IOException;

  OPhysicalPosition createRecord(byte[] content, ORecordVersion recordVersion, byte recordType) throws IOException;

  boolean deleteRecord(long clusterPosition) throws IOException;

  void updateRecord(long clusterPosition, byte[] content, ORecordVersion recordVersion, byte recordType) throws IOException;

  ORawBuffer readRecord(long clusterPosition) throws IOException;

  ORawBuffer readRecordIfVersionIsNotLatest(long clusterPosition, ORecordVersion recordVersion) throws IOException,
      ORecordNotFoundException;

  boolean exists();

  /**
   * Fills and return the PhysicalPosition object received as parameter with the physical position of logical record iPosition
   *
   * @throws IOException
   */
  OPhysicalPosition getPhysicalPosition(OPhysicalPosition iPPosition) throws IOException;

  long getEntries();

  long getFirstPosition() throws IOException;

  long getLastPosition() throws IOException;

  String getFileName();

  int getId();

  void synch() throws IOException;

  String getName();

  /**
   * Returns the size of the records contained in the cluster in bytes.
   *
   * @return
   */
  long getRecordsSize() throws IOException;

  boolean useWal();

  float recordGrowFactor();

  float recordOverflowGrowFactor();

  String compression();

  boolean isHashBased();

  boolean isSystemCluster();

  OClusterEntryIterator absoluteIterator();

  OPhysicalPosition[] higherPositions(OPhysicalPosition position) throws IOException;

  OPhysicalPosition[] ceilingPositions(OPhysicalPosition position) throws IOException;

  OPhysicalPosition[] lowerPositions(OPhysicalPosition position) throws IOException;

  OPhysicalPosition[] floorPositions(OPhysicalPosition position) throws IOException;

  /**
   * Hides records content by putting tombstone on the records position but does not delete record itself.
   * <p>
   * This method is used in case of record content itself is broken and cannot be read or deleted. So it is emergence method.
   *
   * @param position
   *          Position of record in cluster
   * @return false if record does not exist.
   * @throws java.lang.UnsupportedOperationException
   *           In case current version of cluster does not support given operation.
   */
  boolean hideRecord(long position) throws IOException;

  ORecordConflictStrategy getRecordConflictStrategy();
}
