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
package com.orientechnologies.orient.client.remote;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.config.OStorageConfiguration;
import com.orientechnologies.orient.core.conflict.ORecordConflictStrategy;
import com.orientechnologies.orient.core.db.record.OCurrentStorageComponentsFactory;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ridbag.sbtree.OSBTreeCollectionManager;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.OCluster;
import com.orientechnologies.orient.core.storage.OPhysicalPosition;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import com.orientechnologies.orient.core.storage.ORecordCallback;
import com.orientechnologies.orient.core.storage.ORecordMetadata;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.OStorageOperationResult;
import com.orientechnologies.orient.core.storage.OStorageProxy;
import com.orientechnologies.orient.core.tx.OTransaction;
import com.orientechnologies.orient.core.version.ORecordVersion;
import com.orientechnologies.orient.core.version.OVersionFactory;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryAsynchClient;
import com.orientechnologies.orient.enterprise.channel.binary.ORemoteServerEventListener;

/**
 * Wrapper of OStorageRemote that maintains the sessionId. It's bound to the ODatabase and allow to use the shared OStorageRemote.
 */
@SuppressWarnings("unchecked")
public class OStorageRemoteThread implements OStorageProxy {
  private static AtomicInteger sessionSerialId = new AtomicInteger(-1);

  private final OStorageRemote delegate;
  private String               serverURL;
  private int                  sessionId;
  private byte[]               token;

  public OStorageRemoteThread(final OStorageRemote iSharedStorage) {
    delegate = iSharedStorage;
    serverURL = null;
    sessionId = sessionSerialId.decrementAndGet();
  }

  public OStorageRemoteThread(final OStorageRemote iSharedStorage, final int iSessionId) {
    delegate = iSharedStorage;
    serverURL = null;
    sessionId = iSessionId;
  }

  public static int getNextConnectionId() {
    return sessionSerialId.decrementAndGet();
  }

  public void open(final String iUserName, final String iUserPassword, final Map<String, Object> iOptions) {
    pushSession();
    try {
      delegate.open(iUserName, iUserPassword, iOptions);
    } finally {
      popSession();
    }
  }

  @Override
  public boolean isDistributed() {
    return delegate.isDistributed();
  }

  @Override
  public Class<? extends OSBTreeCollectionManager> getCollectionManagerClass() {
    return delegate.getCollectionManagerClass();
  }

  public void create(final Map<String, Object> iOptions) {
    pushSession();
    try {
      delegate.create(iOptions);
    } finally {
      popSession();
    }
  }

  public void close(boolean iForce, boolean onDelete) {
    pushSession();
    try {
      delegate.close(iForce, false);
      Orient.instance().unregisterStorage(this);
    } finally {
      popSession();
    }
  }

  public boolean dropCluster(final String iClusterName, final boolean iTruncate) {
    pushSession();
    try {
      return delegate.dropCluster(iClusterName, iTruncate);
    } finally {
      popSession();
    }
  }

  public int getUsers() {
    pushSession();
    try {
      return delegate.getUsers();
    } finally {
      popSession();
    }
  }

  public int addUser() {
    pushSession();
    try {
      return delegate.addUser();
    } finally {
      popSession();
    }
  }

  public void setSessionId(final String iServerURL, final int iSessionId, byte[] iToken) {
    serverURL = iServerURL;
    sessionId = iSessionId;
    token = iToken;
    delegate.setSessionId(serverURL, iSessionId, iToken);
  }

  public void reload() {
    pushSession();
    try {
      delegate.reload();
    } finally {
      popSession();
    }
  }

  public boolean exists() {
    pushSession();
    try {
      return delegate.exists();
    } finally {
      popSession();
    }
  }

  public int removeUser() {
    pushSession();
    try {
      return delegate.removeUser();
    } finally {
      popSession();
    }
  }

  public void close() {
    pushSession();
    try {
      delegate.close();

      Orient.instance().unregisterStorage(this);
    } finally {
      popSession();
    }
  }

  public void delete() {
    pushSession();
    try {
      delegate.delete();
      Orient.instance().unregisterStorage(this);
    } finally {
      popSession();
    }
  }

  @Override
  public OStorage getUnderlying() {
    return delegate;
  }

  @Override
  public boolean isRemote() {
    return true;
  }

  public Set<String> getClusterNames() {
    pushSession();
    try {
      return delegate.getClusterNames();
    } finally {
      popSession();
    }
  }

  @Override
  public List<String> backup(OutputStream out, Map<String, Object> options, final Callable<Object> callable, final OCommandOutputListener iListener, int compressionLevel, int bufferSize) throws IOException {
    throw new UnsupportedOperationException("backup");
  }

  @Override
  public void restore(InputStream in, Map<String, Object> options, final Callable<Object> callable,
      final OCommandOutputListener iListener) throws IOException {
    throw new UnsupportedOperationException("restore");
  }

  public OStorageOperationResult<OPhysicalPosition> createRecord(final ORecordId iRid, final byte[] iContent,
      ORecordVersion iRecordVersion, final byte iRecordType, final int iMode, ORecordCallback<Long> iCallback) {
    pushSession();
    try {
      return delegate.createRecord(iRid, iContent, OVersionFactory.instance().createVersion(), iRecordType, iMode, iCallback);
    } finally {
      popSession();
    }
  }

  public OStorageOperationResult<ORawBuffer> readRecord(final ORecordId iRid, final String iFetchPlan, boolean iIgnoreCache,
      ORecordCallback<ORawBuffer> iCallback) {
    pushSession();
    try {
      return delegate.readRecord(iRid, iFetchPlan, iIgnoreCache, null);
    } finally {
      popSession();
    }
  }

  @Override
  public OStorageOperationResult<ORawBuffer> readRecordIfVersionIsNotLatest(ORecordId rid, String fetchPlan, boolean ignoreCache,
      ORecordVersion recordVersion) throws ORecordNotFoundException {
    pushSession();
    try {
      return delegate.readRecordIfVersionIsNotLatest(rid, fetchPlan, ignoreCache, recordVersion);
    } finally {
      popSession();
    }
  }

  public OStorageOperationResult<ORecordVersion> updateRecord(final ORecordId iRid, boolean updateContent, final byte[] iContent,
      final ORecordVersion iVersion, final byte iRecordType, final int iMode, ORecordCallback<ORecordVersion> iCallback) {
    pushSession();
    try {
      return delegate.updateRecord(iRid, updateContent, iContent, iVersion, iRecordType, iMode, iCallback);
    } finally {
      popSession();
    }
  }

  public OStorageOperationResult<Boolean> deleteRecord(final ORecordId iRid, final ORecordVersion iVersion, final int iMode,
      ORecordCallback<Boolean> iCallback) {
    pushSession();
    try {
      return delegate.deleteRecord(iRid, iVersion, iMode, iCallback);
    } finally {
      popSession();
    }
  }

  @Override
  public OStorageOperationResult<Boolean> hideRecord(ORecordId recordId, int mode, ORecordCallback<Boolean> callback) {
    pushSession();
    try {
      return delegate.hideRecord(recordId, mode, callback);
    } finally {
      popSession();
    }
  }

  @Override
  public OCluster getClusterByName(String clusterName) {
    return delegate.getClusterByName(clusterName);
  }

  @Override
  public ORecordConflictStrategy getConflictStrategy() {
    throw new UnsupportedOperationException("getConflictStrategy");
  }

  @Override
  public void setConflictStrategy(ORecordConflictStrategy iResolver) {
    throw new UnsupportedOperationException("setConflictStrategy");
  }

  @Override
  public ORecordMetadata getRecordMetadata(ORID rid) {
    pushSession();
    try {
      return delegate.getRecordMetadata(rid);
    } finally {
      popSession();
    }
  }

  @Override
  public boolean cleanOutRecord(ORecordId recordId, ORecordVersion recordVersion, int iMode, ORecordCallback<Boolean> callback) {
    pushSession();
    try {
      return delegate.cleanOutRecord(recordId, recordVersion, iMode, callback);
    } finally {
      popSession();
    }
  }

  public long count(final int iClusterId) {
    pushSession();
    try {
      return delegate.count(iClusterId);
    } finally {
      popSession();
    }
  }

  @Override
  public long count(int iClusterId, boolean countTombstones) {
    pushSession();
    try {
      return delegate.count(iClusterId, countTombstones);
    } finally {
      popSession();
    }
  }

  @Override
  public long count(int[] iClusterIds, boolean countTombstones) {
    pushSession();
    try {
      return delegate.count(iClusterIds, countTombstones);
    } finally {
      popSession();
    }
  }

  public String toString() {
    return delegate.toString();
  }

  public long[] getClusterDataRange(final int iClusterId) {
    pushSession();
    try {
      return delegate.getClusterDataRange(iClusterId);
    } finally {
      popSession();
    }
  }

  @Override
  public OPhysicalPosition[] higherPhysicalPositions(int currentClusterId, OPhysicalPosition physicalPosition) {
    pushSession();
    try {
      return delegate.higherPhysicalPositions(currentClusterId, physicalPosition);
    } finally {
      popSession();
    }
  }

  @Override
  public OPhysicalPosition[] lowerPhysicalPositions(int currentClusterId, OPhysicalPosition physicalPosition) {
    pushSession();
    try {
      return delegate.lowerPhysicalPositions(currentClusterId, physicalPosition);
    } finally {
      popSession();
    }
  }

  @Override
  public OPhysicalPosition[] ceilingPhysicalPositions(int clusterId, OPhysicalPosition physicalPosition) {
    pushSession();
    try {
      return delegate.ceilingPhysicalPositions(clusterId, physicalPosition);
    } finally {
      popSession();
    }
  }

  @Override
  public OPhysicalPosition[] floorPhysicalPositions(int clusterId, OPhysicalPosition physicalPosition) {
    pushSession();
    try {
      return delegate.floorPhysicalPositions(clusterId, physicalPosition);
    } finally {
      popSession();
    }
  }

  public long getSize() {
    pushSession();
    try {
      return delegate.getSize();
    } finally {
      popSession();
    }
  }

  public long countRecords() {
    pushSession();
    try {
      return delegate.countRecords();
    } finally {
      popSession();
    }
  }

  public long count(final int[] iClusterIds) {
    pushSession();
    try {
      return delegate.count(iClusterIds);
    } finally {
      popSession();
    }
  }

  public Object command(final OCommandRequestText iCommand) {
    pushSession();
    try {
      return delegate.command(iCommand);
    } finally {
      popSession();
    }
  }

  public void commit(final OTransaction iTx, Runnable callback) {
    pushSession();
    try {
      delegate.commit(iTx, null);
    } finally {
      popSession();
    }
  }

  public void rollback(OTransaction iTx) {
    pushSession();
    try {
      delegate.rollback(iTx);
    } finally {
      popSession();
    }
  }

  public int getClusterIdByName(final String iClusterName) {
    pushSession();
    try {
      return delegate.getClusterIdByName(iClusterName);
    } finally {
      popSession();
    }
  }

  public int getDefaultClusterId() {
    pushSession();
    try {
      return delegate.getDefaultClusterId();
    } finally {
      popSession();
    }
  }

  public void setDefaultClusterId(final int defaultClusterId) {
    pushSession();
    try {
      delegate.setDefaultClusterId(defaultClusterId);
    } finally {
      popSession();
    }
  }

  public int addCluster(final String iClusterName, boolean forceListBased, final Object... iArguments) {
    pushSession();
    try {
      return delegate.addCluster(iClusterName, false, iArguments);
    } finally {
      popSession();
    }
  }

  public int addCluster(String iClusterName, int iRequestedId, boolean forceListBased, Object... iParameters) {
    pushSession();
    try {
      return delegate.addCluster(iClusterName, iRequestedId, forceListBased, iParameters);
    } finally {
      popSession();
    }
  }

  public boolean dropCluster(final int iClusterId, final boolean iTruncate) {
    pushSession();
    try {
      return delegate.dropCluster(iClusterId, iTruncate);
    } finally {
      popSession();
    }
  }

  public void synch() {
    pushSession();
    try {
      delegate.synch();
    } finally {
      popSession();
    }
  }

  public String getPhysicalClusterNameById(final int iClusterId) {
    pushSession();
    try {
      return delegate.getPhysicalClusterNameById(iClusterId);
    } finally {
      popSession();
    }
  }

  public int getClusters() {
    pushSession();
    try {
      return delegate.getClusterMap();
    } finally {
      popSession();
    }
  }

  public Collection<OCluster> getClusterInstances() {
    pushSession();
    try {
      return delegate.getClusterInstances();
    } finally {
      popSession();
    }
  }

  public OCluster getClusterById(final int iId) {
    pushSession();
    try {
      return delegate.getClusterById(iId);
    } finally {
      popSession();
    }
  }

  public long getVersion() {
    pushSession();
    try {
      return delegate.getVersion();
    } finally {
      popSession();
    }
  }

  public boolean isPermanentRequester() {
    pushSession();
    try {
      return delegate.isPermanentRequester();
    } finally {
      popSession();
    }
  }

  public void updateClusterConfiguration(final String iCurrentURL, final byte[] iContent) {
    pushSession();
    try {
      delegate.updateClusterConfiguration(iCurrentURL, iContent);
    } finally {
      popSession();
    }
  }

  public OStorageConfiguration getConfiguration() {
    pushSession();
    try {
      return delegate.getConfiguration();
    } finally {
      popSession();
    }
  }

  public boolean isClosed() {
    return (sessionId < 0 && token == null) || delegate.isClosed();
  }

  public boolean checkForRecordValidity(final OPhysicalPosition ppos) {
    pushSession();
    try {
      return delegate.checkForRecordValidity(ppos);
    } finally {
      popSession();
    }
  }

  @Override
  public boolean isAssigningClusterIds() {
    return false;
  }

  public String getName() {
    pushSession();
    try {
      return delegate.getName();
    } finally {
      popSession();
    }
  }

  public String getURL() {
    return delegate.getURL();
  }

  public void beginResponse(final OChannelBinaryAsynchClient iNetwork) throws IOException {
    pushSession();
    try {
      delegate.beginResponse(iNetwork);
    } finally {
      popSession();
    }
  }

  @Override
  public OCurrentStorageComponentsFactory getComponentsFactory() {
    return delegate.getComponentsFactory();
  }

  @Override
  public long getLastOperationId() {
    return 0;
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

  @Override
  public void checkForClusterPermissions(final String iClusterName) {
    delegate.checkForClusterPermissions(iClusterName);
  }

  public STATUS getStatus() {
    return delegate.getStatus();
  }

  @Override
  public String getUserName() {
    return delegate.getUserName();
  }

  @Override
  public Object indexGet(final String iIndexName, final Object iKey, final String iFetchPlan) {
    return delegate.indexGet(iIndexName, iKey, iFetchPlan);
  }

  @Override
  public void indexPut(final String iIndexName, Object iKey, final OIdentifiable iValue) {
    delegate.indexPut(iIndexName, iKey, iValue);
  }

  @Override
  public boolean indexRemove(final String iIndexName, final Object iKey) {
    return delegate.indexRemove(iIndexName, iKey);
  }

  @Override
  public String getType() {
    return delegate.getType();
  }

  @Override
  public boolean equals(final Object iOther) {
    if (iOther instanceof OStorageRemoteThread)
      return iOther == this;

    if (iOther instanceof OStorageRemote)
      return iOther == delegate;

    return false;
  }

  protected void handleException(final OChannelBinaryAsynchClient iNetwork, final String iMessage, final Exception iException) {
    delegate.handleException(iNetwork, iMessage, iException);
  }

  protected void pushSession() {
    delegate.setSessionId(serverURL, sessionId, token);
  }

  protected void popSession() {
    serverURL = delegate.getServerURL();
    sessionId = delegate.getSessionId();
    token = delegate.getSessionToken();
    // delegate.clearSession();
  }
}
