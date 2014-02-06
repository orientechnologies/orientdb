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

import com.orientechnologies.common.concur.lock.OModificationLock;
import com.orientechnologies.orient.core.config.OStorageClusterConfiguration;
import com.orientechnologies.orient.core.id.OClusterPosition;
import com.orientechnologies.orient.core.version.ORecordVersion;

/**
 * Handle the table to resolve logical address to physical address.<br/>
 * <br/>
 * Record structure:<br/>
 * <br/>
 * +---------------------------------------------+<br/>
 * | DATA SEGMENT........ | DATA OFFSET......... |<br/>
 * | 2 bytes = max 2^15-1 | 4 bytes = max 2^31-1 |<br/>
 * +---------------------------------------------+<br/>
 * = 6 bytes<br/>
 */
public interface OCluster {

  public static enum ATTRIBUTES {
    NAME, DATASEGMENT, USE_WAL, RECORD_GROW_FACTOR, RECORD_OVERFLOW_GROW_FACTOR, COMPRESSION
  }

  public void configure(OStorage iStorage, int iId, String iClusterName, final String iLocation, int iDataSegmentId,
      Object... iParameters) throws IOException;

  public void configure(OStorage iStorage, OStorageClusterConfiguration iConfig) throws IOException;

  public void create(int iStartSize) throws IOException;

  public void open() throws IOException;

  public void close() throws IOException;

  public void close(boolean flush) throws IOException;

  public void delete() throws IOException;

  public OModificationLock getExternalModificationLock();

  public void set(ATTRIBUTES iAttribute, Object iValue) throws IOException;

  public void convertToTombstone(OClusterPosition iPosition) throws IOException;

  public long getTombstonesCount();

  public boolean hasTombstonesSupport();

  /**
   * Truncates the cluster content. All the entries will be removed.
   * 
   * @throws IOException
   */
  public void truncate() throws IOException;

  public String getType();

  public int getDataSegmentId();

  public OPhysicalPosition createRecord(byte[] content, ORecordVersion recordVersion, byte recordType) throws IOException;

  public boolean deleteRecord(OClusterPosition clusterPosition) throws IOException;

  public void updateRecord(OClusterPosition clusterPosition, byte[] content, ORecordVersion recordVersion, byte recordType)
      throws IOException;

  public ORawBuffer readRecord(OClusterPosition clusterPosition) throws IOException;

  public boolean exists();

  /**
   * Adds a new entry.
   */
  public boolean addPhysicalPosition(OPhysicalPosition iPPosition) throws IOException;

  /**
   * Fills and return the PhysicalPosition object received as parameter with the physical position of logical record iPosition
   * 
   * @throws IOException
   */
  public OPhysicalPosition getPhysicalPosition(OPhysicalPosition iPPosition) throws IOException;

  /**
   * Updates position in data segment (usually on defrag).
   */

  public void updateDataSegmentPosition(OClusterPosition iPosition, int iDataSegmentId, long iDataPosition) throws IOException;

  /**
   * Removes the Logical Position entry.
   */
  public void removePhysicalPosition(OClusterPosition iPosition) throws IOException;

  public void updateRecordType(OClusterPosition iPosition, final byte iRecordType) throws IOException;

  public void updateVersion(OClusterPosition iPosition, ORecordVersion iVersion) throws IOException;

  public long getEntries();

  public OClusterPosition getFirstPosition() throws IOException;

  public OClusterPosition getLastPosition() throws IOException;

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
}
