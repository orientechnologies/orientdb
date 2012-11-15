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

import com.orientechnologies.orient.core.config.OStorageClusterConfiguration;
import com.orientechnologies.orient.core.storage.OCluster;
import com.orientechnologies.orient.core.storage.OClusterEntryIterator;
import com.orientechnologies.orient.core.storage.OPhysicalPosition;
import com.orientechnologies.orient.core.storage.OStorage;

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

  public void updateDataSegmentPosition(long iPosition, int iDataSegmentId, long iDataPosition) throws IOException {
  }

  public void removePhysicalPosition(long iPosition) throws IOException {
  }

  public void updateRecordType(long iPosition, byte iRecordType) throws IOException {
  }

  public void updateVersion(long iPosition, int iVersion) throws IOException {
  }

  public long getEntries() {
    return 0;
  }

  public long getFirstEntryPosition() {
    return 0;
  }

  public long getLastEntryPosition() {
    return 0;
  }

  public void lock() {
  }

  public void unlock() {
  }

  public int getId() {
    return id;
  }

  public void synch() throws IOException {
  }

  public void setSoftlyClosed(boolean softlyClosed) throws IOException {
  }

  public String getName() {
    return name;
  }

  public long getRecordsSize() {
    throw new UnsupportedOperationException("getRecordsSize()");
  }

  public boolean generatePositionBeforeCreation() {
    return false;
  }

  @Override
  public OPhysicalPosition[] getPositionsByEntryPos(long entryPosition) throws IOException {
    return new OPhysicalPosition[0];
  }

  public OClusterEntryIterator absoluteIterator() {
    throw new UnsupportedOperationException("getRecordsSize()");
  }

  public void setType(String type) {
    this.type = type;
  }
}
