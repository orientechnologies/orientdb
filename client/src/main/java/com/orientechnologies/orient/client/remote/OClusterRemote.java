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
import com.orientechnologies.orient.core.storage.OClusterPositionIterator;
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

  /*
   * (non-Javadoc)
   * 
   * @see com.orientechnologies.orient.core.storage.OCluster#close()
   */
  public void close() throws IOException {
    // TODO Auto-generated method stub

  }

  /*
   * (non-Javadoc)
   * 
   * @see com.orientechnologies.orient.core.storage.OCluster#delete()
   */
  public void delete() throws IOException {
    // TODO Auto-generated method stub

  }

  /*
   * (non-Javadoc)
   * 
   * @see com.orientechnologies.orient.core.storage.OCluster#set(com.orientechnologies.orient.core.storage.OCluster.ATTRIBUTES,
   * java.lang.Object)
   */
  public void set(ATTRIBUTES iAttribute, Object iValue) throws IOException {
    // TODO Auto-generated method stub

  }

  /*
   * (non-Javadoc)
   * 
   * @see com.orientechnologies.orient.core.storage.OCluster#truncate()
   */
  public void truncate() throws IOException {
    // TODO Auto-generated method stub

  }

  /*
   * (non-Javadoc)
   * 
   * @see com.orientechnologies.orient.core.storage.OCluster#getType()
   */
  public String getType() {
    return type;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.orientechnologies.orient.core.storage.OCluster#getDataSegmentId()
   */
  public int getDataSegmentId() {
    return dataSegmentId;
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * com.orientechnologies.orient.core.storage.OCluster#addPhysicalPosition(com.orientechnologies.orient.core.storage.OPhysicalPosition
   * )
   */
  public void addPhysicalPosition(OPhysicalPosition iPPosition) throws IOException {
    // TODO Auto-generated method stub

  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * com.orientechnologies.orient.core.storage.OCluster#getPhysicalPosition(com.orientechnologies.orient.core.storage.OPhysicalPosition
   * )
   */
  public OPhysicalPosition getPhysicalPosition(OPhysicalPosition iPPosition) throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.orientechnologies.orient.core.storage.OCluster#updateDataSegmentPosition(long, int, long)
   */
  public void updateDataSegmentPosition(long iPosition, int iDataSegmentId, long iDataPosition) throws IOException {
    // TODO Auto-generated method stub

  }

  /*
   * (non-Javadoc)
   * 
   * @see com.orientechnologies.orient.core.storage.OCluster#removePhysicalPosition(long)
   */
  public void removePhysicalPosition(long iPosition) throws IOException {
    // TODO Auto-generated method stub

  }

  /*
   * (non-Javadoc)
   * 
   * @see com.orientechnologies.orient.core.storage.OCluster#updateRecordType(long, byte)
   */
  public void updateRecordType(long iPosition, byte iRecordType) throws IOException {
    // TODO Auto-generated method stub

  }

  /*
   * (non-Javadoc)
   * 
   * @see com.orientechnologies.orient.core.storage.OCluster#updateVersion(long, int)
   */
  public void updateVersion(long iPosition, int iVersion) throws IOException {
    // TODO Auto-generated method stub

  }

  /*
   * (non-Javadoc)
   * 
   * @see com.orientechnologies.orient.core.storage.OCluster#getEntries()
   */
  public long getEntries() {
    // TODO Auto-generated method stub
    return 0;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.orientechnologies.orient.core.storage.OCluster#getFirstEntryPosition()
   */
  public long getFirstEntryPosition() {
    // TODO Auto-generated method stub
    return 0;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.orientechnologies.orient.core.storage.OCluster#getLastEntryPosition()
   */
  public long getLastEntryPosition() {
    // TODO Auto-generated method stub
    return 0;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.orientechnologies.orient.core.storage.OCluster#lock()
   */
  public void lock() {
    // TODO Auto-generated method stub

  }

  /*
   * (non-Javadoc)
   * 
   * @see com.orientechnologies.orient.core.storage.OCluster#unlock()
   */
  public void unlock() {
    // TODO Auto-generated method stub

  }

  /*
   * (non-Javadoc)
   * 
   * @see com.orientechnologies.orient.core.storage.OCluster#getId()
   */
  public int getId() {
    return id;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.orientechnologies.orient.core.storage.OCluster#synch()
   */
  public void synch() throws IOException {
  }

  public void setSoftlyClosed(boolean softlyClosed) throws IOException {
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.orientechnologies.orient.core.storage.OCluster#getName()
   */
  public String getName() {
    return name;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.orientechnologies.orient.core.storage.OCluster#getRecordsSize()
   */
  public long getRecordsSize() {
    // TODO Auto-generated method stub
    return 0;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.orientechnologies.orient.core.storage.OCluster#absoluteIterator()
   */
  public OClusterPositionIterator absoluteIterator() {
    // TODO Auto-generated method stub
    return null;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.orientechnologies.orient.core.storage.OCluster#absoluteIterator(long, long)
   */
  public OClusterPositionIterator absoluteIterator(long iBeginRange, long iEndRange) throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  public void setType(String type) {
    this.type = type;
  }
}
