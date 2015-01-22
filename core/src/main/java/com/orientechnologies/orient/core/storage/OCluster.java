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
import com.orientechnologies.orient.core.version.ORecordVersion;

import java.io.IOException;

public interface OCluster {

  public static enum ATTRIBUTES {
    NAME, USE_WAL, RECORD_GROW_FACTOR, RECORD_OVERFLOW_GROW_FACTOR, COMPRESSION, CONFLICTSTRATEGY, STATUS
  }

  public void configure(OStorage iStorage, int iId, String iClusterName, Object... iParameters) throws IOException;

  public void configure(OStorage iStorage, OStorageClusterConfiguration iConfig) throws IOException;

  public void create(int iStartSize) throws IOException;

  public void open() throws IOException;

  public void close() throws IOException;

  public void close(boolean flush) throws IOException;

  public void delete() throws IOException;

  public OModificationLock getExternalModificationLock();

  public Object set(ATTRIBUTES iAttribute, Object iValue) throws IOException;

  public void convertToTombstone(long iPosition) throws IOException;

  public long getTombstonesCount();

  public boolean hasTombstonesSupport();

  /**
   * Truncates the cluster content. All the entries will be removed.
   * 
   * @throws IOException
   */
  public void truncate() throws IOException;

  public OPhysicalPosition createRecord(byte[] content, ORecordVersion recordVersion, byte recordType) throws IOException;

  public boolean deleteRecord(long clusterPosition) throws IOException;

  public void updateRecord(long clusterPosition, byte[] content, ORecordVersion recordVersion, byte recordType) throws IOException;

  public ORawBuffer readRecord(long clusterPosition) throws IOException;

  public boolean exists();

  /**
   * Fills and return the PhysicalPosition object received as parameter with the physical position of logical record iPosition
   * 
   * @throws IOException
   */
  public OPhysicalPosition getPhysicalPosition(OPhysicalPosition iPPosition) throws IOException;

  public long getEntries();

  public long getFirstPosition() throws IOException;

  public long getLastPosition() throws IOException;

  public int getId();

  public void synch() throws IOException;

  public void setSoftlyClosed(boolean softlyClosed) throws IOException;

  public boolean wasSoftlyClosed() throws IOException;

  public String getName();

  /**
   * Returns the size of the records contained in the cluster in bytes.
   * 
   * @return
   */
  public long getRecordsSize() throws IOException;

  public boolean useWal();

  public float recordGrowFactor();

  public float recordOverflowGrowFactor();

  public String compression();

  public boolean isHashBased();

  public OClusterEntryIterator absoluteIterator();

  public OPhysicalPosition[] higherPositions(OPhysicalPosition position) throws IOException;

  public OPhysicalPosition[] ceilingPositions(OPhysicalPosition position) throws IOException;

  public OPhysicalPosition[] lowerPositions(OPhysicalPosition position) throws IOException;

  public OPhysicalPosition[] floorPositions(OPhysicalPosition position) throws IOException;

  /**
   * Hides records content by putting tombstone on the records position but does not delete record itself.
   * 
   * This method is used in case of record content itself is broken and can not be read or deleted. So it is emergence method.
   * 
   * @param position
   *          Position of record in cluster
   * @throws java.lang.UnsupportedOperationException
   *           In case current version of cluster does not support given operation.
   * 
   * @return false if record does not exist.
   */
  public boolean hideRecord(long position) throws IOException;

  public ORecordConflictStrategy getRecordConflictStrategy();
}
