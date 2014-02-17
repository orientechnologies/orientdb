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

import com.orientechnologies.common.concur.lock.OModificationLock;
import com.orientechnologies.orient.core.config.OStorageClusterConfiguration;
import com.orientechnologies.orient.core.id.OClusterPosition;
import com.orientechnologies.orient.core.id.OClusterPositionFactory;
import com.orientechnologies.orient.core.storage.OCluster;
import com.orientechnologies.orient.core.storage.OClusterEntryIterator;
import com.orientechnologies.orient.core.storage.OPhysicalPosition;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.version.ORecordVersion;

/**
 * Remote cluster implementation
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 */
public class OClusterRemote implements OCluster {
  private String name;
  private int    id;
  private int    dataSegmentId;
  private String type;

  /*
   * (non-Javadoc)
   * 
   * @see com.orientechnologies.orient.core.storage.OCluster#configure(com.orientechnologies.orient.core.storage.OStorage, int,
   * java.lang.String, java.lang.String, int, java.lang.Object[])
   */
  public void configure(OStorage iStorage, int iId, String iClusterName, String iLocation, int iDataSegmentId,
      Object... iParameters) throws IOException {
    id = iId;
    name = iClusterName;
    dataSegmentId = iDataSegmentId;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.orientechnologies.orient.core.storage.OCluster#configure(com.orientechnologies.orient.core.storage.OStorage,
   * com.orientechnologies.orient.core.config.OStorageClusterConfiguration)
   */
  public void configure(OStorage iStorage, OStorageClusterConfiguration iConfig) throws IOException {
    id = iConfig.getId();
    name = iConfig.getName();
    dataSegmentId = iConfig.getDataSegmentId();
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.orientechnologies.orient.core.storage.OCluster#create(int)
   */
  public void create(int iStartSize) throws IOException {

  }

  /*
   * (non-Javadoc)
   * 
   * @see com.orientechnologies.orient.core.storage.OCluster#open()
   */
  public void open() throws IOException {
  }

  public void close() throws IOException {
  }

  @Override
  public OModificationLock getExternalModificationLock() {
    throw new UnsupportedOperationException("getExternalModificationLock");
  }

  @Override
  public void close(boolean flush) throws IOException {
  }

  @Override
  public OPhysicalPosition createRecord(byte[] content, ORecordVersion recordVersion, byte recordType) throws IOException {
    throw new UnsupportedOperationException("createRecord");
  }

  @Override
  public boolean deleteRecord(OClusterPosition clusterPosition) throws IOException {
    throw new UnsupportedOperationException("deleteRecord");
  }

  @Override
  public void updateRecord(OClusterPosition clusterPosition, byte[] content, ORecordVersion recordVersion, byte recordType)
      throws IOException {
    throw new UnsupportedOperationException("updateRecord");
  }

  @Override
  public ORawBuffer readRecord(OClusterPosition clusterPosition) throws IOException {
    throw new UnsupportedOperationException("readRecord");
  }

  @Override
  public boolean exists() {
    throw new UnsupportedOperationException("exists");
  }

  public void delete() throws IOException {
  }

  public void set(ATTRIBUTES iAttribute, Object iValue) throws IOException {
  }

  public void truncate() throws IOException {
  }

  public String getType() {
    return type;
  }

  public int getDataSegmentId() {
    return dataSegmentId;
  }

  public boolean addPhysicalPosition(OPhysicalPosition iPPosition) throws IOException {
    return false;
  }

  public OPhysicalPosition getPhysicalPosition(OPhysicalPosition iPPosition) throws IOException {
    return null;
  }

  public void updateDataSegmentPosition(OClusterPosition iPosition, int iDataSegmentId, long iDataPosition) throws IOException {
  }

  public void removePhysicalPosition(OClusterPosition iPosition) throws IOException {
  }

  public void updateRecordType(OClusterPosition iPosition, byte iRecordType) throws IOException {
  }

  public void updateVersion(OClusterPosition iPosition, ORecordVersion iVersion) throws IOException {
  }

  public long getEntries() {
    return 0;
  }

  @Override
  public long getTombstonesCount() {
    throw new UnsupportedOperationException("getTombstonesCount()");
  }

  @Override
  public void convertToTombstone(OClusterPosition iPosition) throws IOException {
    throw new UnsupportedOperationException("convertToTombstone()");
  }

  @Override
  public boolean hasTombstonesSupport() {
    throw new UnsupportedOperationException("hasTombstonesSupport()");
  }

  public OClusterPosition getFirstPosition() {
    return OClusterPositionFactory.INSTANCE.valueOf(0);
  }

  public OClusterPosition getLastPosition() {
    return OClusterPositionFactory.INSTANCE.valueOf(0);
  }

  public int getId() {
    return id;
  }

  public void synch() throws IOException {
  }

  public void setSoftlyClosed(boolean softlyClosed) throws IOException {
  }

  @Override
  public boolean wasSoftlyClosed() throws IOException {
    return true;
  }

  public String getName() {
    return name;
  }

  public long getRecordsSize() {
    throw new UnsupportedOperationException("getRecordsSize()");
  }

  public boolean isHashBased() {
    return false;
  }

  public OClusterEntryIterator absoluteIterator() {
    throw new UnsupportedOperationException("getRecordsSize()");
  }

  public void setType(String type) {
    this.type = type;
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
  public OPhysicalPosition[] ceilingPositions(OPhysicalPosition position) throws IOException {
    throw new UnsupportedOperationException("ceilingPositions()");
  }

  @Override
  public OPhysicalPosition[] floorPositions(OPhysicalPosition position) throws IOException {
    throw new UnsupportedOperationException("floorPositions()");
  }

  @Override
  public boolean useWal() {
    throw new UnsupportedOperationException("useWal()");
  }

  @Override
  public float recordGrowFactor() {
    throw new UnsupportedOperationException("recordGrowFactor()");
  }

  @Override
  public float recordOverflowGrowFactor() {
    throw new UnsupportedOperationException("recordOverflowGrowFactor()");
  }

  @Override
  public String compression() {
    throw new UnsupportedOperationException("compression()");
  }

}
