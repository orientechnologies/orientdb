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
package com.orientechnologies.orient.server.distributed;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import com.orientechnologies.orient.core.cache.OLevel2RecordCache;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.config.OStorageConfiguration;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.storage.OCluster;
import com.orientechnologies.orient.core.storage.ODataSegment;
import com.orientechnologies.orient.core.storage.OPhysicalPosition;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import com.orientechnologies.orient.core.storage.ORecordCallback;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.tx.OTransaction;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager.EXECUTION_MODE;
import com.orientechnologies.orient.server.task.OCreateRecordDistributedTask;
import com.orientechnologies.orient.server.task.ODeleteRecordDistributedTask;
import com.orientechnologies.orient.server.task.OReadRecordDistributedTask;
import com.orientechnologies.orient.server.task.OUpdateRecordDistributedTask;

/**
 * Distributed storage implementation that routes to the owner node the request.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class ODistributedStorage implements OStorage {
  protected ODistributedServerManager dManager;
  protected OStorage                  wrapped;
  protected boolean                   eventuallyConsistent = true;
  protected EXECUTION_MODE            createRecordMode     = EXECUTION_MODE.SYNCHRONOUS;
  protected EXECUTION_MODE            updateRecordMode     = EXECUTION_MODE.SYNCHRONOUS;
  protected EXECUTION_MODE            deleteRecordMode     = EXECUTION_MODE.SYNCHRONOUS;

  public ODistributedStorage(final ODistributedServerManager iCluster, final OStorage wrapped) {
    this.dManager = iCluster;
    this.wrapped = wrapped;
  }

  public OPhysicalPosition createRecord(int iDataSegmentId, ORecordId iRecordId, byte[] iContent, int iRecordVersion,
      byte iRecordType, int iMode, ORecordCallback<Long> iCallback) {
    Object result = null;

    try {
      result = dManager.routeOperation2Node(iRecordId,
          new OCreateRecordDistributedTask(dManager.getLocalNodeId(), wrapped.getName(), createRecordMode, iRecordId, iContent,
              iRecordVersion, iRecordType));
    } catch (ExecutionException e) {
      throw new OStorageException("Cannot route CREATE_RECORD operation to the distributed node", e);
    }

    return (OPhysicalPosition) result;
  }

  public ORawBuffer readRecord(ORecordId iRid, String iFetchPlan, boolean iIgnoreCache, ORecordCallback<ORawBuffer> iCallback) {
    if (eventuallyConsistent || dManager.isLocalNodeOwner(iRid))
      return wrapped.readRecord(iRid, iFetchPlan, iIgnoreCache, iCallback);

    try {
      return (ORawBuffer) dManager.routeOperation2Node(iRid,
          new OReadRecordDistributedTask(dManager.getLocalNodeId(), wrapped.getName(), iRid));
    } catch (ExecutionException e) {
      throw new OStorageException("Cannot route READ_RECORD operation to the distributed node", e);
    }
  }

  public int updateRecord(ORecordId iRecordId, byte[] iContent, int iVersion, byte iRecordType, int iMode,
      ORecordCallback<Integer> iCallback) {
    Object result = null;

    if (!dManager.isLocalNodeOwner(iRecordId))
      try {
        result = dManager.routeOperation2Node(iRecordId,
            new OUpdateRecordDistributedTask(dManager.getLocalNodeId(), wrapped.getName(), updateRecordMode, iRecordId, iContent,
                iVersion, iRecordType));
      } catch (ExecutionException e) {
        throw new OStorageException("Cannot route UPDATE_RECORD operation to the distributed node", e);
      }

    // UPDATE LOCALLY
    return (Integer) result;
  }

  public boolean deleteRecord(ORecordId iRecordId, int iVersion, int iMode, ORecordCallback<Boolean> iCallback) {
    Object result = null;

    if (!dManager.isLocalNodeOwner(iRecordId))
      try {
        result = dManager.routeOperation2Node(iRecordId,
            new ODeleteRecordDistributedTask(dManager.getLocalNodeId(), wrapped.getName(), updateRecordMode, iRecordId, iVersion));
      } catch (ExecutionException e) {
        throw new OStorageException("Cannot route UPDATE_RECORD operation to the distributed node", e);
      }

    // DELETE LOCALLY
    return (Boolean) result;
  }

  public boolean existsResource(String iName) {
    return wrapped.existsResource(iName);
  }

  public <T> T removeResource(String iName) {
    return wrapped.removeResource(iName);
  }

  public <T> T getResource(String iName, Callable<T> iCallback) {
    return wrapped.getResource(iName, iCallback);
  }

  public void open(String iUserName, String iUserPassword, Map<String, Object> iProperties) {
    wrapped.open(iUserName, iUserPassword, iProperties);
  }

  public void create(Map<String, Object> iProperties) {
    wrapped.create(iProperties);
  }

  public boolean exists() {
    return wrapped.exists();
  }

  public void reload() {
    wrapped.reload();
  }

  public void delete() {
    wrapped.delete();
  }

  public void close() {
    wrapped.close();
  }

  public void close(boolean iForce) {
    wrapped.close(iForce);
  }

  public boolean isClosed() {
    return wrapped.isClosed();
  }

  public OLevel2RecordCache getLevel2Cache() {
    return wrapped.getLevel2Cache();
  }

  public void commit(OTransaction iTx) {
    wrapped.commit(iTx);
  }

  public void rollback(OTransaction iTx) {
    wrapped.rollback(iTx);
  }

  public OStorageConfiguration getConfiguration() {
    return wrapped.getConfiguration();
  }

  public int getClusters() {
    return wrapped.getClusters();
  }

  public Set<String> getClusterNames() {
    return wrapped.getClusterNames();
  }

  public OCluster getClusterById(int iId) {
    return wrapped.getClusterById(iId);
  }

  public Collection<? extends OCluster> getClusterInstances() {
    return wrapped.getClusterInstances();
  }

  public int addCluster(String iClusterType, String iClusterName, String iLocation, String iDataSegmentName, Object... iParameters) {
    return wrapped.addCluster(iClusterType, iClusterName, iLocation, iDataSegmentName, iParameters);
  }

  public boolean dropCluster(String iClusterName) {
    return wrapped.dropCluster(iClusterName);
  }

  public boolean dropCluster(int iId) {
    return wrapped.dropCluster(iId);
  }

  public int addDataSegment(String iDataSegmentName) {
    return wrapped.addDataSegment(iDataSegmentName);
  }

  public int addDataSegment(String iSegmentName, String iDirectory) {
    return wrapped.addDataSegment(iSegmentName, iDirectory);
  }

  public long count(int iClusterId) {
    return wrapped.count(iClusterId);
  }

  public long count(int[] iClusterIds) {
    return wrapped.count(iClusterIds);
  }

  public long getSize() {
    return wrapped.getSize();
  }

  public long countRecords() {
    return wrapped.countRecords();
  }

  public int getDefaultClusterId() {
    return wrapped.getDefaultClusterId();
  }

  public void setDefaultClusterId(int defaultClusterId) {
    wrapped.setDefaultClusterId(defaultClusterId);
  }

  public int getClusterIdByName(String iClusterName) {
    return wrapped.getClusterIdByName(iClusterName);
  }

  public String getClusterTypeByName(String iClusterName) {
    return wrapped.getClusterTypeByName(iClusterName);
  }

  public String getPhysicalClusterNameById(int iClusterId) {
    return wrapped.getPhysicalClusterNameById(iClusterId);
  }

  public boolean checkForRecordValidity(OPhysicalPosition ppos) {
    return wrapped.checkForRecordValidity(ppos);
  }

  public String getName() {
    return wrapped.getName();
  }

  public String getURL() {
    return wrapped.getURL();
  }

  public long getVersion() {
    return wrapped.getVersion();
  }

  public void synch() {
    wrapped.synch();
  }

  public int getUsers() {
    return wrapped.getUsers();
  }

  public int addUser() {
    return wrapped.addUser();
  }

  public int removeUser() {
    return wrapped.removeUser();
  }

  public Object command(OCommandRequestText iCommand) {
    return wrapped.command(iCommand);
  }

  public long[] getClusterDataRange(int currentClusterId) {
    return wrapped.getClusterDataRange(currentClusterId);
  }

  public <V> V callInLock(Callable<V> iCallable, boolean iExclusiveLock) {
    return wrapped.callInLock(iCallable, iExclusiveLock);
  }

  public ODataSegment getDataSegmentById(int iDataSegmentId) {
    return wrapped.getDataSegmentById(iDataSegmentId);
  }

  public int getDataSegmentIdByName(String iDataSegmentName) {
    return wrapped.getDataSegmentIdByName(iDataSegmentName);
  }

  public boolean dropDataSegment(String iName) {
    return wrapped.dropDataSegment(iName);
  }

  public STATUS getStatus() {
    return wrapped.getStatus();
  }
}
