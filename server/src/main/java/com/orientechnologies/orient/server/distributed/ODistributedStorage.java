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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

import com.orientechnologies.common.concur.ONeedRetryException;
import com.orientechnologies.common.concur.resource.OSharedResourceAdaptiveExternal;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.cache.OLevel2RecordCache;
import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.core.command.OCommandExecutor;
import com.orientechnologies.orient.core.command.OCommandManager;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.config.OStorageConfiguration;
import com.orientechnologies.orient.core.db.OScenarioThreadLocal;
import com.orientechnologies.orient.core.db.OScenarioThreadLocal.RUN_MODE;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.id.OClusterPosition;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.sql.OCommandExecutorSQLDelegate;
import com.orientechnologies.orient.core.storage.*;
import com.orientechnologies.orient.core.storage.impl.local.OFreezableStorage;
import com.orientechnologies.orient.core.tx.OTransaction;
import com.orientechnologies.orient.core.version.ORecordVersion;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.ODistributedRequest.EXECUTION_MODE;
import com.orientechnologies.orient.server.distributed.task.*;

/**
 * Distributed storage implementation that routes to the owner node the request.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 */
public class ODistributedStorage implements OStorage, OFreezableStorage {
  protected final OServer                   serverInstance;
  protected final ODistributedServerManager dManager;
  protected final OStorageEmbedded          wrapped;

  public ODistributedStorage(final OServer iServer, final OStorageEmbedded wrapped) {
    this.serverInstance = iServer;
    this.dManager = iServer.getDistributedManager();
    this.wrapped = wrapped;
  }

  @Override
  public boolean isDistributed() {
    return true;
  }

  public Object command(final OCommandRequestText iCommand) {
    if (OScenarioThreadLocal.INSTANCE.get() == RUN_MODE.RUNNING_DISTRIBUTED)
      // ALREADY DISTRIBUTED
      return wrapped.command(iCommand);

    final ODistributedConfiguration dConfig = dManager.getDatabaseConfiguration(getName());
    if (!dConfig.isReplicationActive(null))
      // DON'T REPLICATE
      return wrapped.command(iCommand);

    final OCommandExecutor executor = OCommandManager.instance().getExecutor(iCommand);

    executor.setProgressListener(iCommand.getProgressListener());
    executor.parse(iCommand);

    final OCommandExecutor exec = executor instanceof OCommandExecutorSQLDelegate ? ((OCommandExecutorSQLDelegate) executor)
        .getDelegate() : executor;

    boolean distribute = false;
    if (OScenarioThreadLocal.INSTANCE.get() != RUN_MODE.RUNNING_DISTRIBUTED)
      if (exec instanceof OCommandDistributedReplicateRequest)
        distribute = ((OCommandDistributedReplicateRequest) exec).isReplicated();

    if (!distribute)
      // DON'T REPLICATE
      return wrapped.executeCommand(iCommand, executor);

    try {
      // REPLICATE IT
      // final OAbstractRemoteTask task = exec instanceof OCommandExecutorSQLResultsetAbstract ? new OMapReduceCommandTask(
      // iCommand.getText()) : new OSQLCommandTask(iCommand.getText());
      final OAbstractRemoteTask task = new OSQLCommandTask(iCommand.getText());

      final Object result = dManager.sendRequest(getName(), null, task, EXECUTION_MODE.RESPONSE);

      if (result instanceof ONeedRetryException)
        throw (ONeedRetryException) result;
      else if (result instanceof Throwable)
        throw new ODistributedException("Error on execution distributed COMMAND", (Throwable) result);

      return result;

    } catch (ONeedRetryException e) {
      // PASS THROUGH
      throw e;
    } catch (Exception e) {
      handleDistributedException("Cannot route COMMAND operation to the distributed node", e);
      // UNREACHABLE
      return null;
    }
  }

  public OStorageOperationResult<OPhysicalPosition> createRecord(final int iDataSegmentId, final ORecordId iRecordId,
      final byte[] iContent, final ORecordVersion iRecordVersion, final byte iRecordType, final int iMode,
      final ORecordCallback<OClusterPosition> iCallback) {
    if (OScenarioThreadLocal.INSTANCE.get() == RUN_MODE.RUNNING_DISTRIBUTED)
      // ALREADY DISTRIBUTED
      return wrapped.createRecord(iDataSegmentId, iRecordId, iContent, iRecordVersion, iRecordType, iMode, iCallback);

    Object result = null;

    try {
      // ASSIGN DESTINATION NODE
      final String clusterName = getClusterNameByRID(iRecordId);

      final ODistributedConfiguration dConfig = dManager.getDatabaseConfiguration(getName());
      if (!dConfig.isReplicationActive(clusterName))
        // DON'T REPLICATE
        return wrapped.createRecord(iDataSegmentId, iRecordId, iContent, iRecordVersion, iRecordType, iMode, iCallback);

      // REPLICATE IT
      result = dManager.sendRequest(getName(), clusterName,
          new OCreateRecordTask(iRecordId, iContent, iRecordVersion, iRecordType), EXECUTION_MODE.RESPONSE);

      if (result instanceof ONeedRetryException)
        throw (ONeedRetryException) result;
      else if (result instanceof Throwable)
        throw new ODistributedException("Error on execution distributed CREATE_RECORD", (Throwable) result);

      iRecordId.clusterPosition = ((OPhysicalPosition) result).clusterPosition;
      return new OStorageOperationResult<OPhysicalPosition>((OPhysicalPosition) result);

    } catch (ONeedRetryException e) {
      // PASS THROUGH
      throw e;
    } catch (Exception e) {
      handleDistributedException("Cannot route CREATE_RECORD operation against %s to the distributed node", e, iRecordId);
      // UNREACHABLE
      return null;
    }
  }

  public OStorageOperationResult<ORawBuffer> readRecord(final ORecordId iRecordId, final String iFetchPlan,
      final boolean iIgnoreCache, final ORecordCallback<ORawBuffer> iCallback, boolean loadTombstones) {
    if (OScenarioThreadLocal.INSTANCE.get() == RUN_MODE.RUNNING_DISTRIBUTED)
      // ALREADY DISTRIBUTED
      return wrapped.readRecord(iRecordId, iFetchPlan, iIgnoreCache, iCallback, loadTombstones);

    try {
      final String clusterName = getClusterNameByRID(iRecordId);
      final ODistributedConfiguration dConfig = dManager.getDatabaseConfiguration(getName());
      if (!dConfig.isReplicationActive(clusterName))
        // DON'T REPLICATE
        return wrapped.readRecord(iRecordId, iFetchPlan, iIgnoreCache, iCallback, loadTombstones);

      final ODistributedPartitioningStrategy strategy = dManager.getPartitioningStrategy(dConfig.getPartitionStrategy(clusterName));
      final ODistributedPartition partition = strategy.getPartition(dManager, getName(), clusterName);
      if (partition.getNodes().contains(dManager.getLocalNodeName()))
        // LOCAL NODE OWNS THE DATA: GET IT LOCALLY BECAUSE IT'S FASTER
        return wrapped.readRecord(iRecordId, iFetchPlan, iIgnoreCache, iCallback, loadTombstones);

      // DISTRIBUTE IT
      final Object result = dManager.sendRequest(getName(), clusterName, new OReadRecordTask(iRecordId), EXECUTION_MODE.RESPONSE);

      if (result instanceof ONeedRetryException)
        throw (ONeedRetryException) result;
      else if (result instanceof Throwable)
        throw new ODistributedException("Error on execution distributed READ_RECORD", (Throwable) result);

      return new OStorageOperationResult<ORawBuffer>((ORawBuffer) result);

    } catch (ONeedRetryException e) {
      // PASS THROUGH
      throw e;
    } catch (Exception e) {
      handleDistributedException("Cannot route READ_RECORD operation against %s to the distributed node", e, iRecordId);
      // UNREACHABLE
      return null;
    }
  }

  public OStorageOperationResult<ORecordVersion> updateRecord(final ORecordId iRecordId, final byte[] iContent,
      final ORecordVersion iVersion, final byte iRecordType, final int iMode, final ORecordCallback<ORecordVersion> iCallback) {
    if (OScenarioThreadLocal.INSTANCE.get() == RUN_MODE.RUNNING_DISTRIBUTED)
      // ALREADY DISTRIBUTED
      return wrapped.updateRecord(iRecordId, iContent, iVersion, iRecordType, iMode, iCallback);

    try {
      final String clusterName = getClusterNameByRID(iRecordId);

      final ODistributedConfiguration dConfig = dManager.getDatabaseConfiguration(getName());
      if (!dConfig.isReplicationActive(clusterName))
        // DON'T REPLICATE
        return wrapped.updateRecord(iRecordId, iContent, iVersion, iRecordType, iMode, iCallback);

      // REPLICATE IT
      final Object result = dManager.sendRequest(getName(), clusterName, new OUpdateRecordTask(iRecordId, iContent, iVersion,
          iRecordType), EXECUTION_MODE.RESPONSE);

      if (result instanceof ONeedRetryException)
        throw (ONeedRetryException) result;
      else if (result instanceof Throwable)
        throw new ODistributedException("Error on execution distributed UPDATE_RECORD", (Throwable) result);

      // UPDATE LOCALLY
      return new OStorageOperationResult<ORecordVersion>((ORecordVersion) result);

    } catch (ONeedRetryException e) {
      // PASS THROUGH
      throw e;
    } catch (Exception e) {
      handleDistributedException("Cannot route UPDATE_RECORD operation against %s to the distributed node", e, iRecordId);
      // UNREACHABLE
      return null;
    }
  }

  public String getClusterNameByRID(final ORecordId iRid) {
    final OCluster cluster = getClusterById(iRid.clusterId);
    return cluster != null ? cluster.getName() : "*";
  }

  public OStorageOperationResult<Boolean> deleteRecord(final ORecordId iRecordId, final ORecordVersion iVersion, final int iMode,
      final ORecordCallback<Boolean> iCallback) {
    if (OScenarioThreadLocal.INSTANCE.get() == RUN_MODE.RUNNING_DISTRIBUTED)
      // ALREADY DISTRIBUTED
      return wrapped.deleteRecord(iRecordId, iVersion, iMode, iCallback);

    try {
      final String clusterName = getClusterNameByRID(iRecordId);

      final ODistributedConfiguration dConfig = dManager.getDatabaseConfiguration(getName());
      if (!dConfig.isReplicationActive(clusterName))
        // DON'T REPLICATE
        return wrapped.deleteRecord(iRecordId, iVersion, iMode, iCallback);

      // REPLICATE IT
      final Object result = dManager.sendRequest(getName(), clusterName, new ODeleteRecordTask(iRecordId, iVersion),
          EXECUTION_MODE.RESPONSE);

      if (result instanceof ONeedRetryException)
        throw (ONeedRetryException) result;
      else if (result instanceof Throwable)
        throw new ODistributedException("Error on execution distributed DELETE_RECORD", (Throwable) result);

      return new OStorageOperationResult<Boolean>(true);

    } catch (ONeedRetryException e) {
      // PASS THROUGH
      throw e;
    } catch (Exception e) {
      handleDistributedException("Cannot route DELETE_RECORD operation against %s to the distributed node", e, iRecordId);
      // UNREACHABLE
      return null;
    }
  }

  @Override
  public boolean updateReplica(int dataSegmentId, ORecordId rid, byte[] content, ORecordVersion recordVersion, byte recordType)
      throws IOException {
    return wrapped.updateReplica(dataSegmentId, rid, content, recordVersion, recordType);
  }

  @Override
  public ORecordMetadata getRecordMetadata(ORID rid) {
    return wrapped.getRecordMetadata(rid);
  }

  @Override
  public boolean cleanOutRecord(ORecordId recordId, ORecordVersion recordVersion, int iMode, ORecordCallback<Boolean> callback) {
    return wrapped.cleanOutRecord(recordId, recordVersion, iMode, callback);
  }

  public boolean existsResource(final String iName) {
    return wrapped.existsResource(iName);
  }

  @SuppressWarnings("unchecked")
  public <T> T removeResource(final String iName) {
    return (T) wrapped.removeResource(iName);
  }

  public <T> T getResource(final String iName, final Callable<T> iCallback) {
    return (T) wrapped.getResource(iName, iCallback);
  }

  public void open(final String iUserName, final String iUserPassword, final Map<String, Object> iProperties) {
    wrapped.open(iUserName, iUserPassword, iProperties);
  }

  public void create(final Map<String, Object> iProperties) {
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

  public void close(final boolean iForce) {
    wrapped.close(iForce);
  }

  public boolean isClosed() {
    return wrapped.isClosed();
  }

  public OLevel2RecordCache getLevel2Cache() {
    return wrapped.getLevel2Cache();
  }

  public void commit(final OTransaction iTx, final Runnable callback) {
    if (OScenarioThreadLocal.INSTANCE.get() == RUN_MODE.RUNNING_DISTRIBUTED)
      // ALREADY DISTRIBUTED
      wrapped.commit(iTx, callback);
    else {
      try {
        final ODistributedConfiguration dConfig = dManager.getDatabaseConfiguration(getName());
        if (!dConfig.isReplicationActive(null))
          // DON'T REPLICATE
          wrapped.commit(iTx, callback);
        else {
          final OTxTask txTask = new OTxTask();

          for (ORecordOperation op : iTx.getCurrentRecordEntries()) {
            final OAbstractRecordReplicatedTask task;

            final ORecordInternal<?> record = op.getRecord();

            switch (op.type) {
            case ORecordOperation.CREATED:
              task = new OCreateRecordTask((ORecordId) op.record.getIdentity(), record.toStream(), record.getRecordVersion(),
                  record.getRecordType());
              break;
            case ORecordOperation.UPDATED:
              task = new OUpdateRecordTask((ORecordId) op.record.getIdentity(), record.toStream(), record.getRecordVersion(),
                  record.getRecordType());
              break;
            case ORecordOperation.DELETED:
              task = new ODeleteRecordTask((ORecordId) op.record.getIdentity(), record.getRecordVersion());
              break;
            default:
              continue;
            }

            txTask.add(task);
          }

          // REPLICATE IT
          dManager.sendRequest(getName(), null, txTask, EXECUTION_MODE.RESPONSE);
        }
      } catch (Exception e) {
        handleDistributedException("Cannot route TX operation against distributed node", e);
      }
    }
  }

  public void rollback(final OTransaction iTx) {
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

  public int addCluster(final String iClusterType, final String iClusterName, final String iLocation,
      final String iDataSegmentName, boolean forceListBased, final Object... iParameters) {
    return wrapped.addCluster(iClusterType, iClusterName, iLocation, iDataSegmentName, false, iParameters);
  }

  public int addCluster(String iClusterType, String iClusterName, int iRequestedId, String iLocation, String iDataSegmentName,
      boolean forceListBased, Object... iParameters) {
    return wrapped.addCluster(iClusterType, iClusterName, iRequestedId, iLocation, iDataSegmentName, forceListBased, iParameters);
  }

  public boolean dropCluster(final String iClusterName, final boolean iTruncate) {
    return wrapped.dropCluster(iClusterName, iTruncate);
  }

  public boolean dropCluster(final int iId, final boolean iTruncate) {
    return wrapped.dropCluster(iId, iTruncate);
  }

  public int addDataSegment(final String iDataSegmentName) {
    return wrapped.addDataSegment(iDataSegmentName);
  }

  public int addDataSegment(final String iSegmentName, final String iDirectory) {
    return wrapped.addDataSegment(iSegmentName, iDirectory);
  }

  public long count(final int iClusterId) {
    return wrapped.count(iClusterId);
  }

  @Override
  public long count(int iClusterId, boolean countTombstones) {
    return wrapped.count(iClusterId, countTombstones);
  }

  public long count(final int[] iClusterIds) {
    return wrapped.count(iClusterIds);
  }

  @Override
  public long count(int[] iClusterIds, boolean countTombstones) {
    return wrapped.count(iClusterIds, countTombstones);
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

  public void setDefaultClusterId(final int defaultClusterId) {
    wrapped.setDefaultClusterId(defaultClusterId);
  }

  public int getClusterIdByName(String iClusterName) {
    return wrapped.getClusterIdByName(iClusterName);
  }

  public String getClusterTypeByName(final String iClusterName) {
    return wrapped.getClusterTypeByName(iClusterName);
  }

  public String getPhysicalClusterNameById(final int iClusterId) {
    return wrapped.getPhysicalClusterNameById(iClusterId);
  }

  public boolean checkForRecordValidity(final OPhysicalPosition ppos) {
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

  public OClusterPosition[] getClusterDataRange(final int currentClusterId) {
    return wrapped.getClusterDataRange(currentClusterId);
  }

  public <V> V callInLock(final Callable<V> iCallable, final boolean iExclusiveLock) {
    return wrapped.callInLock(iCallable, iExclusiveLock);
  }

  @Override
  public <V> V callInRecordLock(Callable<V> iCallable, ORID rid, boolean iExclusiveLock) {
    return wrapped.callInRecordLock(iCallable, rid, iExclusiveLock);
  }

  public ODataSegment getDataSegmentById(final int iDataSegmentId) {
    return wrapped.getDataSegmentById(iDataSegmentId);
  }

  public int getDataSegmentIdByName(final String iDataSegmentName) {
    return wrapped.getDataSegmentIdByName(iDataSegmentName);
  }

  public boolean dropDataSegment(final String iName) {
    return wrapped.dropDataSegment(iName);
  }

  public STATUS getStatus() {
    return wrapped.getStatus();
  }

  @Override
  public void checkForClusterPermissions(final String iClusterName) {
    wrapped.checkForClusterPermissions(iClusterName);
  }

  @Override
  public OPhysicalPosition[] higherPhysicalPositions(int currentClusterId, OPhysicalPosition entry) {
    return wrapped.higherPhysicalPositions(currentClusterId, entry);
  }

  @Override
  public OPhysicalPosition[] ceilingPhysicalPositions(int clusterId, OPhysicalPosition physicalPosition) {
    return wrapped.ceilingPhysicalPositions(clusterId, physicalPosition);
  }

  @Override
  public OPhysicalPosition[] floorPhysicalPositions(int clusterId, OPhysicalPosition physicalPosition) {
    return wrapped.floorPhysicalPositions(clusterId, physicalPosition);
  }

  @Override
  public OPhysicalPosition[] lowerPhysicalPositions(int currentClusterId, OPhysicalPosition entry) {
    return wrapped.lowerPhysicalPositions(currentClusterId, entry);
  }

  @Override
  public OSharedResourceAdaptiveExternal getLock() {
    return wrapped.getLock();
  }

  public OStorage getUnderlying() {
    return wrapped;
  }

  @Override
  public String getType() {
    return "distributed";
  }

  protected void handleDistributedException(final String iMessage, Exception e, Object... iParams) {
    OLogManager.instance().error(this, iMessage, e, iParams);
    final Throwable t = e.getCause();
    if (t != null) {
      if (t instanceof OException)
        throw (OException) t;
      else if (t.getCause() instanceof OException)
        throw (OException) t.getCause();
    }
    throw new OStorageException(String.format(iMessage, iParams), e);
  }

  @Override
  public void freeze(boolean throwException) {
    getFreezableStorage().freeze(throwException);
  }

  @Override
  public void release() {
    getFreezableStorage().release();
  }

  @Override
  public void backup(OutputStream out, Map<String, Object> options, Callable<Object> callable) throws IOException {
    wrapped.backup(out, options, callable);
  }

  @Override
  public void restore(InputStream in, Map<String, Object> options, Callable<Object> callable) throws IOException {
    wrapped.restore(in, options, callable);
  }

  private OFreezableStorage getFreezableStorage() {
    if (wrapped instanceof OFreezableStorage)
      return ((OFreezableStorage) wrapped);
    else
      throw new UnsupportedOperationException("Storage engine " + wrapped.getType() + " does not support freeze operation");
  }
}
