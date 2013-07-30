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
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

import com.orientechnologies.common.concur.resource.OSharedResourceAdaptiveExternal;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.cache.OLevel2RecordCache;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.config.OStorageConfiguration;
import com.orientechnologies.orient.core.id.OClusterPosition;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.OCluster;
import com.orientechnologies.orient.core.storage.ODataSegment;
import com.orientechnologies.orient.core.storage.OPhysicalPosition;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import com.orientechnologies.orient.core.storage.ORecordCallback;
import com.orientechnologies.orient.core.storage.ORecordMetadata;
import com.orientechnologies.orient.core.storage.OStorageOperationResult;
import com.orientechnologies.orient.core.storage.OStorageProxy;
import com.orientechnologies.orient.core.tx.OTransaction;
import com.orientechnologies.orient.core.version.ORecordVersion;
import com.orientechnologies.orient.core.version.OVersionFactory;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryClient;
import com.orientechnologies.orient.enterprise.channel.binary.ORemoteServerEventListener;

/**
 * Wrapper of OStorageRemote that maintains the sessionId. It's bound to the ODatabase and allow to use the shared OStorageRemote.
 */
@SuppressWarnings("unchecked")
public class OStorageRemoteThread implements OStorageProxy {
  private static AtomicInteger sessionSerialId = new AtomicInteger(-1);

  private final OStorageRemote delegate;
  private int                  sessionId;

  public OStorageRemoteThread(final OStorageRemote iSharedStorage) {
    delegate = iSharedStorage;
    sessionId = sessionSerialId.decrementAndGet();
  }

  public OStorageRemoteThread(final OStorageRemote iSharedStorage, final int iSessionId) {
    delegate = iSharedStorage;
    sessionId = iSessionId;
  }

  public void open(final String iUserName, final String iUserPassword, final Map<String, Object> iOptions) {
    delegate.setSessionId(sessionId);
    delegate.open(iUserName, iUserPassword, iOptions);
    sessionId = delegate.getSessionId();
  }

  public void create(final Map<String, Object> iOptions) {
    delegate.setSessionId(sessionId);
    delegate.create(iOptions);
    sessionId = delegate.getSessionId();
  }

  public void close(boolean iForce) {
    delegate.setSessionId(sessionId);
    delegate.close(iForce);
    Orient.instance().unregisterStorage(this);
  }

  public boolean dropCluster(final String iClusterName, final boolean iTruncate) {
    delegate.setSessionId(sessionId);
    return delegate.dropCluster(iClusterName, iTruncate);
  }

  public int getUsers() {
    delegate.setSessionId(sessionId);
    return delegate.getUsers();
  }

  public int addUser() {
    delegate.setSessionId(sessionId);
    return delegate.addUser();
  }

  public OSharedResourceAdaptiveExternal getLock() {
    delegate.setSessionId(sessionId);
    return delegate.getLock();
  }

  public void setSessionId(final int iSessionId) {
    sessionId = iSessionId;
    delegate.setSessionId(iSessionId);
  }

  public void reload() {
    delegate.setSessionId(sessionId);
    delegate.reload();
  }

  public boolean exists() {
    delegate.setSessionId(sessionId);
    return delegate.exists();
  }

  public int removeUser() {
    delegate.setSessionId(sessionId);
    return delegate.removeUser();
  }

  public void close() {
    delegate.setSessionId(sessionId);
    delegate.close();
  }

  public void delete() {
    delegate.setSessionId(sessionId);
    delegate.delete();
    Orient.instance().unregisterStorage(this);
  }

  public Set<String> getClusterNames() {
    delegate.setSessionId(sessionId);
    return delegate.getClusterNames();
  }

  public OStorageOperationResult<OPhysicalPosition> createRecord(final int iDataSegmentId, final ORecordId iRid,
      final byte[] iContent, ORecordVersion iRecordVersion, final byte iRecordType, final int iMode,
      ORecordCallback<OClusterPosition> iCallback) {
    delegate.setSessionId(sessionId);
    return delegate.createRecord(iDataSegmentId, iRid, iContent, OVersionFactory.instance().createVersion(), iRecordType, iMode,
        iCallback);
  }

  public OStorageOperationResult<ORawBuffer> readRecord(final ORecordId iRid, final String iFetchPlan, boolean iIgnoreCache,
      ORecordCallback<ORawBuffer> iCallback, boolean loadTombstones) {
    delegate.setSessionId(sessionId);
    return delegate.readRecord(iRid, iFetchPlan, iIgnoreCache, null, loadTombstones);
  }

  public OStorageOperationResult<ORecordVersion> updateRecord(final ORecordId iRid, final byte[] iContent,
      final ORecordVersion iVersion, final byte iRecordType, final int iMode, ORecordCallback<ORecordVersion> iCallback) {
    delegate.setSessionId(sessionId);
    return delegate.updateRecord(iRid, iContent, iVersion, iRecordType, iMode, iCallback);
  }

  public OStorageOperationResult<Boolean> deleteRecord(final ORecordId iRid, final ORecordVersion iVersion, final int iMode,
      ORecordCallback<Boolean> iCallback) {
    delegate.setSessionId(sessionId);
    return delegate.deleteRecord(iRid, iVersion, iMode, iCallback);
  }

  @Override
  public boolean updateReplica(int dataSegmentId, ORecordId rid, byte[] content, ORecordVersion recordVersion, byte recordType)
      throws IOException {
    delegate.setSessionId(sessionId);
    return delegate.updateReplica(dataSegmentId, rid, content, recordVersion, recordType);
  }

  @Override
  public ORecordMetadata getRecordMetadata(ORID rid) {
    delegate.setSessionId(sessionId);
    return delegate.getRecordMetadata(rid);
  }

  @Override
  public <V> V callInRecordLock(Callable<V> iCallable, ORID rid, boolean iExclusiveLock) {
    delegate.setSessionId(sessionId);
    return delegate.callInRecordLock(iCallable, rid, iExclusiveLock);
  }

  @Override
  public boolean cleanOutRecord(ORecordId recordId, ORecordVersion recordVersion, int iMode, ORecordCallback<Boolean> callback) {
    delegate.setSessionId(sessionId);
    return delegate.cleanOutRecord(recordId, recordVersion, iMode, callback);
  }

  public long count(final int iClusterId) {
    delegate.setSessionId(sessionId);
    return delegate.count(iClusterId);
  }

  @Override
  public long count(int iClusterId, boolean countTombstones) {
    delegate.setSessionId(sessionId);
    return delegate.count(iClusterId, countTombstones);
  }

  @Override
  public long count(int[] iClusterIds, boolean countTombstones) {
    delegate.setSessionId(sessionId);
    return delegate.count(iClusterIds, countTombstones);
  }

  public String toString() {
    delegate.setSessionId(sessionId);
    return delegate.toString();
  }

  public OClusterPosition[] getClusterDataRange(final int iClusterId) {
    delegate.setSessionId(sessionId);
    return delegate.getClusterDataRange(iClusterId);
  }

  @Override
  public OPhysicalPosition[] higherPhysicalPositions(int currentClusterId, OPhysicalPosition physicalPosition) {
    delegate.setSessionId(sessionId);
    return delegate.higherPhysicalPositions(currentClusterId, physicalPosition);
  }

  @Override
  public OPhysicalPosition[] lowerPhysicalPositions(int currentClusterId, OPhysicalPosition physicalPosition) {
    delegate.setSessionId(sessionId);
    return delegate.lowerPhysicalPositions(currentClusterId, physicalPosition);
  }

  @Override
  public OPhysicalPosition[] ceilingPhysicalPositions(int clusterId, OPhysicalPosition physicalPosition) {
    delegate.setSessionId(sessionId);
    return delegate.ceilingPhysicalPositions(clusterId, physicalPosition);
  }

  @Override
  public OPhysicalPosition[] floorPhysicalPositions(int clusterId, OPhysicalPosition physicalPosition) {
    delegate.setSessionId(sessionId);
    return delegate.floorPhysicalPositions(clusterId, physicalPosition);
  }

  public long getSize() {
    delegate.setSessionId(sessionId);
    return delegate.getSize();
  }

  public long countRecords() {
    delegate.setSessionId(sessionId);
    return delegate.countRecords();
  }

  public long count(final int[] iClusterIds) {
    delegate.setSessionId(sessionId);
    return delegate.count(iClusterIds);
  }

  public Object command(final OCommandRequestText iCommand) {
    delegate.setSessionId(sessionId);
    return delegate.command(iCommand);
  }

  public void commit(final OTransaction iTx) {
    delegate.setSessionId(sessionId);
    delegate.commit(iTx);
  }

  public void rollback(OTransaction iTx) {
    delegate.setSessionId(sessionId);
    delegate.rollback(iTx);
  }

  public int getClusterIdByName(final String iClusterName) {
    delegate.setSessionId(sessionId);
    return delegate.getClusterIdByName(iClusterName);
  }

  public String getClusterTypeByName(final String iClusterName) {
    delegate.setSessionId(sessionId);
    return delegate.getClusterTypeByName(iClusterName);
  }

  public int getDefaultClusterId() {
    delegate.setSessionId(sessionId);
    return delegate.getDefaultClusterId();
  }

  public void setDefaultClusterId(final int defaultClusterId) {
    delegate.setSessionId(sessionId);
    delegate.setDefaultClusterId(defaultClusterId);
  }

  public int addCluster(final String iClusterType, final String iClusterName, final String iLocation,
      final String iDataSegmentName, boolean forceListBased, final Object... iArguments) {
    delegate.setSessionId(sessionId);
    return delegate.addCluster(iClusterType, iClusterName, iLocation, iDataSegmentName, false, iArguments);
  }

  public int addCluster(String iClusterType, String iClusterName, int iRequestedId, String iLocation, String iDataSegmentName,
      boolean forceListBased, Object... iParameters) {
    delegate.setSessionId(sessionId);
    return delegate.addCluster(iClusterType, iClusterName, iRequestedId, iLocation, iDataSegmentName, forceListBased, iParameters);
  }

  public boolean dropCluster(final int iClusterId, final boolean iTruncate) {
    delegate.setSessionId(sessionId);
    return delegate.dropCluster(iClusterId, iTruncate);
  }

  public ODataSegment getDataSegmentById(final int iDataSegmentId) {
    return delegate.getDataSegmentById(iDataSegmentId);
  }

  public int getDataSegmentIdByName(final String iDataSegmentName) {
    return delegate.getDataSegmentIdByName(iDataSegmentName);
  }

  public int addDataSegment(final String iDataSegmentName) {
    delegate.setSessionId(sessionId);
    return delegate.addDataSegment(iDataSegmentName);
  }

  public int addDataSegment(final String iSegmentName, final String iSegmentFileName) {
    delegate.setSessionId(sessionId);
    return delegate.addDataSegment(iSegmentName, iSegmentFileName);
  }

  public boolean dropDataSegment(final String iSegmentName) {
    delegate.setSessionId(sessionId);
    return delegate.dropDataSegment(iSegmentName);
  }

  public void synch() {
    delegate.setSessionId(sessionId);
    delegate.synch();
  }

  public String getPhysicalClusterNameById(final int iClusterId) {
    delegate.setSessionId(sessionId);
    return delegate.getPhysicalClusterNameById(iClusterId);
  }

  public int getClusters() {
    delegate.setSessionId(sessionId);
    return delegate.getClusterMap();
  }

  public Collection<OCluster> getClusterInstances() {
    delegate.setSessionId(sessionId);
    return delegate.getClusterInstances();
  }

  public OCluster getClusterById(final int iId) {
    delegate.setSessionId(sessionId);
    return delegate.getClusterById(iId);
  }

  public long getVersion() {
    delegate.setSessionId(sessionId);
    return delegate.getVersion();
  }

  public boolean isPermanentRequester() {
    delegate.setSessionId(sessionId);
    return delegate.isPermanentRequester();
  }

  public void updateClusterConfiguration(final byte[] iContent) {
    delegate.setSessionId(sessionId);
    delegate.updateClusterConfiguration(iContent);
  }

  public OStorageConfiguration getConfiguration() {
    delegate.setSessionId(sessionId);
    return delegate.getConfiguration();
  }

  public boolean isClosed() {
    return delegate.isClosed();
  }

  public boolean checkForRecordValidity(final OPhysicalPosition ppos) {
    delegate.setSessionId(sessionId);
    return delegate.checkForRecordValidity(ppos);
  }

  public String getName() {
    delegate.setSessionId(sessionId);
    return delegate.getName();
  }

	@Override
  public boolean isHashClustersAreUsed() {
    delegate.setSessionId(sessionId);
    return delegate.isHashClustersAreUsed();
  }

  public String getURL() {
    return delegate.getURL();
  }

  public void beginResponse(final OChannelBinaryClient iNetwork) throws IOException {
    delegate.setSessionId(sessionId);
    delegate.beginResponse(iNetwork);
  }

  public OLevel2RecordCache getLevel2Cache() {
    return delegate.getLevel2Cache();
  }

  public boolean existsResource(final String iName) {
    return delegate.existsResource(iName);
  }

  public synchronized <T> T getResource(final String iName, final Callable<T> iCallback) {
    return (T) delegate.getResource(iName, iCallback);
  }

  public <T> T removeResource(final String iName) {
    return (T) delegate.removeResource(iName);
  }

  public ODocument getClusterConfiguration() {
    return delegate.getClusterConfiguration();
  }

  public void closeChannel(final OChannelBinaryClient network) {
    delegate.closeChannel(network);
  }

  protected void handleException(final String iMessage, final Exception iException) {
    delegate.handleException(iMessage, iException);
  }

  public <V> V callInLock(final Callable<V> iCallable, final boolean iExclusiveLock) {
    return delegate.callInLock(iCallable, iExclusiveLock);
  }

  public ORemoteServerEventListener getRemoteServerEventListener() {
    return delegate.getAsynchEventListener();
  }

  public void setRemoteServerEventListener(final ORemoteServerEventListener iListener) {
    delegate.setAsynchEventListener(iListener);
  }

  public void removeRemoteServerEventListener() {
    delegate.removeRemoteServerEventListener();
  }

  public static int getNextConnectionId() {
    return sessionSerialId.decrementAndGet();
  }

  @Override
  public void checkForClusterPermissions(final String iClusterName) {
    delegate.checkForClusterPermissions(iClusterName);
  }

  public STATUS getStatus() {
    return delegate.getStatus();
  }

  @Override
  public String getType() {
    return delegate.getType();
  }

}
