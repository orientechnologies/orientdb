/*
 * Copyright 2010-2012 Luca Garulli (l.garulli(at)orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.common.concur.ONeedRetryException;
import com.orientechnologies.common.concur.resource.OSharedResourceAdaptiveExternal;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.util.OPair;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.cache.OLevel2RecordCache;
import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.core.command.OCommandExecutor;
import com.orientechnologies.orient.core.command.OCommandManager;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.config.OStorageConfiguration;
import com.orientechnologies.orient.core.db.OScenarioThreadLocal;
import com.orientechnologies.orient.core.db.OScenarioThreadLocal.RUN_MODE;
import com.orientechnologies.orient.core.db.record.OCurrentStorageComponentsFactory;
import com.orientechnologies.orient.core.db.record.OPlaceholder;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.db.record.ridbag.sbtree.OSBTreeCollectionManager;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.exception.OTransactionException;
import com.orientechnologies.orient.core.id.OClusterPosition;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandExecutorSQLDelegate;
import com.orientechnologies.orient.core.sql.OCommandExecutorSQLSelect;
import com.orientechnologies.orient.core.sql.functions.OSQLFunctionRuntime;
import com.orientechnologies.orient.core.storage.*;
import com.orientechnologies.orient.core.storage.impl.local.OFreezableStorage;
import com.orientechnologies.orient.core.tx.OTransaction;
import com.orientechnologies.orient.core.version.ORecordVersion;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.ODistributedRequest.EXECUTION_MODE;
import com.orientechnologies.orient.server.distributed.task.OAbstractRecordReplicatedTask;
import com.orientechnologies.orient.server.distributed.task.OAbstractRemoteTask;
import com.orientechnologies.orient.server.distributed.task.OCreateRecordTask;
import com.orientechnologies.orient.server.distributed.task.ODeleteRecordTask;
import com.orientechnologies.orient.server.distributed.task.OReadRecordTask;
import com.orientechnologies.orient.server.distributed.task.OSQLCommandTask;
import com.orientechnologies.orient.server.distributed.task.OTxTask;
import com.orientechnologies.orient.server.distributed.task.OUpdateRecordTask;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimerTask;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Distributed storage implementation that routes to the owner node the request.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 */
public class ODistributedStorage implements OStorage, OFreezableStorage, OAutoshardedStorage {
  protected final OServer                                                   serverInstance;
  protected final ODistributedServerManager                                 dManager;
  protected final OStorageEmbedded                                          wrapped;

  protected final TimerTask                                                 purgeDeletedRecordsTask;
  protected final ConcurrentHashMap<ORecordId, OPair<Long, ORecordVersion>> deletedRecords  = new ConcurrentHashMap<ORecordId, OPair<Long, ORecordVersion>>();
  protected final AtomicLong                                                lastOperationId = new AtomicLong();

  public ODistributedStorage(final OServer iServer, final OStorageEmbedded wrapped) {
    this.serverInstance = iServer;
    this.dManager = iServer.getDistributedManager();
    this.wrapped = wrapped;

    purgeDeletedRecordsTask = new TimerTask() {
      @Override
      public void run() {
        final long now = System.currentTimeMillis();
        for (Iterator<Map.Entry<ORecordId, OPair<Long, ORecordVersion>>> it = deletedRecords.entrySet().iterator(); it.hasNext();) {
          final Map.Entry<ORecordId, OPair<Long, ORecordVersion>> entry = it.next();

          try {
            final ORecordId rid = entry.getKey();
            final long time = entry.getValue().getKey();
            final ORecordVersion version = entry.getValue().getValue();

            if (now - time > (OGlobalConfiguration.DISTRIBUTED_ASYNCH_RESPONSES_TIMEOUT.getValueAsLong() * 2)) {
              // DELETE RECORD
              final OStorageOperationResult<Boolean> result = wrapped.deleteRecord(rid, version, 0, null);
              if (result == null || !result.getResult())
                OLogManager.instance().error(this, "Error on deleting record %s v.%s", rid, version);
            }
          } finally {
            // OK, REMOVE IT
            it.remove();
          }
        }
      }
    };

    Orient
        .instance()
        .getTimer()
        .schedule(purgeDeletedRecordsTask, OGlobalConfiguration.DISTRIBUTED_PURGE_RESPONSES_TIMER_DELAY.getValueAsLong(),
            OGlobalConfiguration.DISTRIBUTED_PURGE_RESPONSES_TIMER_DELAY.getValueAsLong());
  }

  @Override
  public boolean isDistributed() {
    return true;
  }

  @Override
  public Class<? extends OSBTreeCollectionManager> getCollectionManagerClass() {
    return wrapped.getCollectionManagerClass();
  }

  public Object command(final OCommandRequestText iCommand) {
    if (OScenarioThreadLocal.INSTANCE.get() == RUN_MODE.RUNNING_DISTRIBUTED)
      // ALREADY DISTRIBUTED
      return wrapped.command(iCommand);

    final ODistributedConfiguration dbCfg = dManager.getDatabaseConfiguration(getName());
    if (!dbCfg.isReplicationActive(null))
      // DON'T REPLICATE
      return wrapped.command(iCommand);

    // if (dManager.getDatabaseStatus(dManager.getLocalNodeName(), getName()) != ODistributedServerManager.DB_STATUS.ONLINE)
    // // NODE OFFLINE, DON'T DISTRIBUTE
    // return wrapped.command(iCommand);

    final OCommandExecutor executor = OCommandManager.instance().getExecutor(iCommand);

    executor.setProgressListener(iCommand.getProgressListener());
    executor.parse(iCommand);

    final OCommandExecutor exec = executor instanceof OCommandExecutorSQLDelegate ? ((OCommandExecutorSQLDelegate) executor)
        .getDelegate() : executor;

    try {
      final OSQLCommandTask task = new OSQLCommandTask(iCommand);

      Object result;
      boolean replicated = false;
      if (OScenarioThreadLocal.INSTANCE.get() != RUN_MODE.RUNNING_DISTRIBUTED) {
        if (exec instanceof OCommandDistributedReplicateRequest)
          replicated = ((OCommandDistributedReplicateRequest) exec).isReplicated();
      }

      final Collection<String> involvedClusters = exec.getInvolvedClusters();
      final Collection<String> nodes;

      if (replicated) {
        // REPLICATE IT, GET ALL THE INVOLVED NODES
        task.setResultStrategy(OAbstractRemoteTask.RESULT_STRATEGY.ANY);

        nodes = dbCfg.getServers(involvedClusters);
        result = dManager.sendRequest(getName(), involvedClusters, nodes, task, EXECUTION_MODE.RESPONSE);
      } else {
        // SHARDED, GET ONLY ONE NODE PER INVOLVED CLUSTER
        task.setResultStrategy(OAbstractRemoteTask.RESULT_STRATEGY.UNION);

        nodes = dbCfg.getOneServerPerCluster(involvedClusters, dManager.getLocalNodeName());

        if (nodes.size() == 1 && nodes.iterator().next().equals(dManager.getLocalNodeName()))
          // LOCAL NODE, AVOID TO DISTRIBUTE IT
          return wrapped.command(iCommand);

        // TODO: OPTIMIZE FILTERING BY CHANGING TARGET PER CLUSTER INSTEAD OF LEAVING CLASS
        result = dManager.sendRequest(getName(), involvedClusters, nodes, task, EXECUTION_MODE.RESPONSE);

        if (result instanceof Map) {
          if (executor instanceof OCommandExecutorSQLDelegate
              && ((OCommandExecutorSQLDelegate) executor).getDelegate() instanceof OCommandExecutorSQLSelect) {
            final OCommandExecutorSQLSelect cmd = (OCommandExecutorSQLSelect) ((OCommandExecutorSQLDelegate) executor)
                .getDelegate();

            if (((Map<String, Object>) result).size() == 1)
              // USE THE COLLECTION DIRECTLY
              result = ((Map<String, Object>) result).values().iterator().next();
            else {
              if (cmd.isAnyFunctionAggregates()) {
                final Map<String, Object> proj = cmd.getProjections();

                final List<Object> list = new ArrayList<Object>();
                final ODocument doc = new ODocument();
                list.add(doc);

                boolean hasNonAggregates = false;
                for (Map.Entry<String, Object> p : proj.entrySet()) {
                  if (!(p.getValue() instanceof OSQLFunctionRuntime)) {
                    hasNonAggregates = true;
                    break;
                  }
                }

                if (hasNonAggregates) {
                  // MERGE NON AGGREGATED FIELDS
                  for (Map.Entry<String, Object> entry : ((Map<String, Object>) result).entrySet()) {
                    final List<Object> resultSet = (List<Object>) entry.getValue();

                    for (Object r : resultSet) {
                      if (r instanceof ODocument) {
                        final ODocument d = (ODocument) r;

                        for (Map.Entry<String, Object> p : proj.entrySet()) {
                          // WRITE THE FIELD AS IS
                          if (!(p.getValue() instanceof OSQLFunctionRuntime))
                            doc.field(p.getKey(), p.getValue());
                        }
                      }
                    }
                  }
                }

                final List<Object> toMerge = new ArrayList<Object>();

                // MERGE AGGREGATED FIELDS
                for (Map.Entry<String, Object> p : proj.entrySet()) {
                  if (p.getValue() instanceof OSQLFunctionRuntime) {
                    // MERGE RESULTS
                    final OSQLFunctionRuntime f = (OSQLFunctionRuntime) p.getValue();

                    toMerge.clear();
                    for (Map.Entry<String, Object> entry : ((Map<String, Object>) result).entrySet()) {
                      final List<Object> resultSet = (List<Object>) entry.getValue();

                      for (Object r : resultSet) {
                        if (r instanceof ODocument) {
                          final ODocument d = (ODocument) r;
                          toMerge.add(d.rawField(p.getKey()));
                        }
                      }

                    }

                    // WRITE THE FINAL MERGED RESULT
                    doc.field(p.getKey(), f.getFunction().mergeDistributedResult(toMerge));
                  }
                }

                result = list;
              } else {
                // MIX & FILTER RESULT SET AVOIDING DUPLICATES
                // TODO: ONCE OPTIMIZED (SEE ABOVE) AVOID TO FILTER HERE
                final Set<Object> set = new HashSet<Object>();
                for (Map.Entry<String, Object> entry : ((Map<String, Object>) result).entrySet()) {
                  set.addAll((Collection<?>) entry.getValue());
                }
                result = new ArrayList<Object>(set);
              }
            }
          }
        }
      }

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

      final ODistributedConfiguration dbCfg = dManager.getDatabaseConfiguration(getName());
      if (!dbCfg.isReplicationActive(clusterName) && dbCfg.getServers(clusterName) == null)
        // DON'T REPLICATE OR DISTRIBUTE
        return wrapped.createRecord(iDataSegmentId, iRecordId, iContent, iRecordVersion, iRecordType, iMode, iCallback);

      // REPLICATE IT
      final Collection<String> nodes = dbCfg.getServers(clusterName);
      result = dManager.sendRequest(getName(), Collections.singleton(clusterName), nodes, new OCreateRecordTask(iRecordId,
          iContent, iRecordVersion, iRecordType), EXECUTION_MODE.RESPONSE);

      if (result instanceof ONeedRetryException)
        throw (ONeedRetryException) result;
      else if (result instanceof Throwable)
        throw new ODistributedException("Error on execution distributed CREATE_RECORD", (Throwable) result);

      final OPlaceholder p = (OPlaceholder) result;

      iRecordId.copyFrom(p.getIdentity());
      return new OStorageOperationResult<OPhysicalPosition>(new OPhysicalPosition(p.getIdentity().getClusterPosition(),
          p.getRecordVersion()));

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
      final boolean iIgnoreCache, final ORecordCallback<ORawBuffer> iCallback, boolean loadTombstones,
      LOCKING_STRATEGY iLockingStrategy) {

    if (deletedRecords.get(iRecordId) != null)
      // DELETED
      throw new ORecordNotFoundException("Record " + iRecordId + " was not found");

    if (OScenarioThreadLocal.INSTANCE.get() == RUN_MODE.RUNNING_DISTRIBUTED)
      // ALREADY DISTRIBUTED
      return wrapped.readRecord(iRecordId, iFetchPlan, iIgnoreCache, iCallback, loadTombstones, LOCKING_STRATEGY.DEFAULT);

    try {
      final String clusterName = getClusterNameByRID(iRecordId);

      final ODistributedConfiguration dbCfg = dManager.getDatabaseConfiguration(getName());
      final Collection<String> serverList = dbCfg.getServers(clusterName);
      if (!dbCfg.isReplicationActive(clusterName) && serverList == null)
        // DON'T REPLICATE
        return wrapped.readRecord(iRecordId, iFetchPlan, iIgnoreCache, iCallback, loadTombstones, LOCKING_STRATEGY.DEFAULT);

      if (serverList.contains(dManager.getLocalNodeName()))
        // LOCAL NODE OWNS THE DATA: GET IT LOCALLY BECAUSE IT'S FASTER
        return wrapped.readRecord(iRecordId, iFetchPlan, iIgnoreCache, iCallback, loadTombstones, LOCKING_STRATEGY.DEFAULT);

      // DISTRIBUTE IT
      final Collection<String> nodes = dbCfg.getServers(clusterName);
      final Object result = dManager.sendRequest(getName(), Collections.singleton(clusterName), nodes, new OReadRecordTask(
          iRecordId), EXECUTION_MODE.RESPONSE);

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

  @Override
  public OStorageOperationResult<ORecordVersion> updateRecord(final ORecordId iRecordId, final byte[] iContent,
      final ORecordVersion iVersion, final byte iRecordType, final int iMode, final ORecordCallback<ORecordVersion> iCallback) {
    if (deletedRecords.get(iRecordId) != null)
      // DELETED
      throw new ORecordNotFoundException("Record " + iRecordId + " was not found");

    if (OScenarioThreadLocal.INSTANCE.get() == RUN_MODE.RUNNING_DISTRIBUTED)
      // ALREADY DISTRIBUTED
      return wrapped.updateRecord(iRecordId, iContent, iVersion, iRecordType, iMode, iCallback);

    try {
      final String clusterName = getClusterNameByRID(iRecordId);

      final ODistributedConfiguration dbCfg = dManager.getDatabaseConfiguration(getName());
      if (!dbCfg.isReplicationActive(clusterName) && dbCfg.getServers(clusterName) == null)
        // DON'T REPLICATE OR DISTRIBUTE
        return wrapped.updateRecord(iRecordId, iContent, iVersion, iRecordType, iMode, iCallback);

      // LOAD PREVIOUS CONTENT TO BE USED IN CASE OF UNDO
      final OStorageOperationResult<ORawBuffer> previousContent = readRecord(iRecordId, null, false, null, false,
          LOCKING_STRATEGY.DEFAULT);

      // REPLICATE IT
      final Collection<String> nodes = dbCfg.getServers(clusterName);
      final Object result = dManager.sendRequest(getName(), Collections.singleton(clusterName), nodes, new OUpdateRecordTask(
          iRecordId, previousContent.getResult().getBuffer(), previousContent.getResult().version, iContent, iVersion),
          EXECUTION_MODE.RESPONSE);

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

  @Override
  public OStorageOperationResult<Boolean> deleteRecord(final ORecordId iRecordId, final ORecordVersion iVersion, final int iMode,
      final ORecordCallback<Boolean> iCallback) {
    if (OScenarioThreadLocal.INSTANCE.get() == RUN_MODE.RUNNING_DISTRIBUTED)
      // ALREADY DISTRIBUTED
      return wrapped.deleteRecord(iRecordId, iVersion, iMode, iCallback);

    try {
      final String clusterName = getClusterNameByRID(iRecordId);

      final ODistributedConfiguration dbCfg = dManager.getDatabaseConfiguration(getName());
      if (!dbCfg.isReplicationActive(clusterName) && dbCfg.getServers(clusterName) == null)
        // DON'T REPLICATE OR DISTRIBUTE
        return wrapped.deleteRecord(iRecordId, iVersion, iMode, iCallback);

      // LOAD PREVIOUS CONTENT TO BE USED IN CASE OF UNDO
      final OStorageOperationResult<ORawBuffer> previousContent = readRecord(iRecordId, null, false, null, false,
          LOCKING_STRATEGY.DEFAULT);

      // REPLICATE IT
      final Collection<String> nodes = dbCfg.getServers(clusterName);

      final Object result = dManager.sendRequest(getName(), Collections.singleton(clusterName), nodes, new ODeleteRecordTask(
          iRecordId, iVersion), EXECUTION_MODE.RESPONSE);

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
  public OStorageOperationResult<Boolean> hideRecord(ORecordId recordId, int mode, ORecordCallback<Boolean> callback) {
    throw new UnsupportedOperationException();
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

  @Override
  public boolean existsResource(final String iName) {
    return wrapped.existsResource(iName);
  }

  public OCluster getClusterByName(final String iName){
    return wrapped.getClusterByName(iName);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> T removeResource(final String iName) {
    return (T) wrapped.removeResource(iName);
  }

  @Override
  public <T> T getResource(final String iName, final Callable<T> iCallback) {
    return (T) wrapped.getResource(iName, iCallback);
  }

  @Override
  public void open(final String iUserName, final String iUserPassword, final Map<String, Object> iProperties) {
    wrapped.open(iUserName, iUserPassword, iProperties);
  }

  @Override
  public void create(final Map<String, Object> iProperties) {
    wrapped.create(iProperties);
  }

  @Override
  public boolean exists() {
    return wrapped.exists();
  }

  @Override
  public void reload() {
    wrapped.reload();
  }

  @Override
  public void delete() {
    wrapped.delete();
  }

  @Override
  public void close() {
    wrapped.close();
  }

  @Override
  public void close(final boolean iForce, boolean onDelete) {
    wrapped.close(iForce, false);
  }

  @Override
  public boolean isClosed() {
    return wrapped.isClosed();
  }

  @Override
  public OLevel2RecordCache getLevel2Cache() {
    return wrapped.getLevel2Cache();
  }

  @Override
  public void commit(final OTransaction iTx, final Runnable callback) {
    if (OScenarioThreadLocal.INSTANCE.get() == RUN_MODE.RUNNING_DISTRIBUTED)
      // ALREADY DISTRIBUTED
      wrapped.commit(iTx, callback);
    else {
      try {
        final ODistributedConfiguration dbCfg = dManager.getDatabaseConfiguration(getName());
        if (!dbCfg.isReplicationActive(null))
          // DON'T REPLICATE
          wrapped.commit(iTx, callback);
        else {
          final OTxTask txTask = new OTxTask();
          final Set<String> involvedClusters = new HashSet<String>();

          for (ORecordOperation op : iTx.getCurrentRecordEntries()) {
            final OAbstractRecordReplicatedTask task;

            final ORecordInternal<?> record = op.getRecord();

            final ORecordId rid = (ORecordId) op.record.getIdentity();

            switch (op.type) {
            case ORecordOperation.CREATED:
              task = new OCreateRecordTask(rid, record.toStream(), record.getRecordVersion(), record.getRecordType());
              break;

            case ORecordOperation.UPDATED:
              // LOAD PREVIOUS CONTENT TO BE USED IN CASE OF UNDO
              final OStorageOperationResult<ORawBuffer> previousContent = wrapped.readRecord(rid, null, false, null, false,
                  LOCKING_STRATEGY.DEFAULT);

              if (previousContent.getResult() == null)
                // DELETED
                throw new OTransactionException("Cannot update record '" + rid + "' because has been deleted");

              task = new OUpdateRecordTask(rid, previousContent.getResult().getBuffer(), previousContent.getResult().version,
                  record.toStream(), record.getRecordVersion());
              break;

            case ORecordOperation.DELETED:
              task = new ODeleteRecordTask(rid, record.getRecordVersion());
              break;

            default:
              continue;
            }

            involvedClusters.add(getClusterNameByRID(rid));
            txTask.add(task);
          }

          final Collection<String> nodes = dbCfg.getServers(involvedClusters);

          // REPLICATE IT
          final Object result = dManager.sendRequest(getName(), involvedClusters, nodes, txTask, EXECUTION_MODE.RESPONSE);

          if (result instanceof List<?>) {
            final List<Object> list = (List<Object>) result;
            for (int i = 0; i < txTask.getTasks().size(); ++i) {
              final Object o = list.get(i);

              final OAbstractRecordReplicatedTask task = txTask.getTasks().get(i);

              if (task instanceof OCreateRecordTask) {
                final OCreateRecordTask t = (OCreateRecordTask) task;
                t.getRid().copyFrom(((OPlaceholder) o).getIdentity());
                t.getVersion().copyFrom(((OPlaceholder) o).getRecordVersion());

              } else if (task instanceof OUpdateRecordTask) {
                final OUpdateRecordTask t = (OUpdateRecordTask) task;
                t.getVersion().copyFrom((ORecordVersion) o);

              } else if (task instanceof ODeleteRecordTask) {

              }

            }
          } else if (result instanceof Throwable) {
            // EXCEPTION: LOG IT AND ADD AS NESTED EXCEPTION
            if (ODistributedServerLog.isDebugEnabled())
              ODistributedServerLog.debug(this, dManager.getLocalNodeName(), null, ODistributedServerLog.DIRECTION.NONE,
                  "distributed transaction error: %s", result, result.toString());

            if (result instanceof OTransactionException || result instanceof ONeedRetryException)
              throw (RuntimeException) result;

            throw new OTransactionException("Error on committing distributed transaction", (Throwable) result);
          } else {
            // UNKNOWN RESPONSE TYPE
            if (ODistributedServerLog.isDebugEnabled())
              ODistributedServerLog.debug(this, dManager.getLocalNodeName(), null, ODistributedServerLog.DIRECTION.NONE,
                  "distributed transaction error, received unknown response type: %s", result);

            throw new OTransactionException("Error on committing distributed transaction, received unknown response type " + result);
          }

        }
      } catch (Exception e) {
        handleDistributedException("Cannot route TX operation against distributed node", e);
      }
    }
  }

  @Override
  public void rollback(final OTransaction iTx) {
    wrapped.rollback(iTx);
  }

  @Override
  public OStorageConfiguration getConfiguration() {
    return wrapped.getConfiguration();
  }

  @Override
  public int getClusters() {
    return wrapped.getClusters();
  }

  @Override
  public Set<String> getClusterNames() {
    return wrapped.getClusterNames();
  }

  @Override
  public OCluster getClusterById(int iId) {
    return wrapped.getClusterById(iId);
  }

  @Override
  public Collection<? extends OCluster> getClusterInstances() {
    return wrapped.getClusterInstances();
  }

  @Override
  public int addCluster(final String iClusterType, final String iClusterName, final String iLocation,
      final String iDataSegmentName, boolean forceListBased, final Object... iParameters) {
    return wrapped.addCluster(iClusterType, iClusterName, iLocation, iDataSegmentName, false, iParameters);
  }

  @Override
  public int addCluster(String iClusterType, String iClusterName, int iRequestedId, String iLocation, String iDataSegmentName,
      boolean forceListBased, Object... iParameters) {
    return wrapped.addCluster(iClusterType, iClusterName, iRequestedId, iLocation, iDataSegmentName, forceListBased, iParameters);
  }

  public boolean dropCluster(final String iClusterName, final boolean iTruncate) {
    return wrapped.dropCluster(iClusterName, iTruncate);
  }

  @Override
  public boolean dropCluster(final int iId, final boolean iTruncate) {
    return wrapped.dropCluster(iId, iTruncate);
  }

  @Override
  public int addDataSegment(final String iDataSegmentName) {
    return wrapped.addDataSegment(iDataSegmentName);
  }

  @Override
  public int addDataSegment(final String iSegmentName, final String iDirectory) {
    return wrapped.addDataSegment(iSegmentName, iDirectory);
  }

  @Override
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

  @Override
  public long getSize() {
    return wrapped.getSize();
  }

  @Override
  public long countRecords() {
    return wrapped.countRecords();
  }

  @Override
  public int getDefaultClusterId() {
    return wrapped.getDefaultClusterId();
  }

  @Override
  public void setDefaultClusterId(final int defaultClusterId) {
    wrapped.setDefaultClusterId(defaultClusterId);
  }

  @Override
  public int getClusterIdByName(String iClusterName) {
    return wrapped.getClusterIdByName(iClusterName);
  }

  @Override
  public String getClusterTypeByName(final String iClusterName) {
    return wrapped.getClusterTypeByName(iClusterName);
  }

  @Override
  public String getPhysicalClusterNameById(final int iClusterId) {
    return wrapped.getPhysicalClusterNameById(iClusterId);
  }

  @Override
  public boolean checkForRecordValidity(final OPhysicalPosition ppos) {
    return wrapped.checkForRecordValidity(ppos);
  }

  @Override
  public String getName() {
    return wrapped.getName();
  }

  @Override
  public String getURL() {
    return wrapped.getURL();
  }

  @Override
  public long getVersion() {
    return wrapped.getVersion();
  }

  @Override
  public void synch() {
    wrapped.synch();
  }

  @Override
  public int getUsers() {
    return wrapped.getUsers();
  }

  @Override
  public int addUser() {
    return wrapped.addUser();
  }

  @Override
  public int removeUser() {
    return wrapped.removeUser();
  }

  @Override
  public OClusterPosition[] getClusterDataRange(final int currentClusterId) {
    return wrapped.getClusterDataRange(currentClusterId);
  }

  @Override
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
  public OCurrentStorageComponentsFactory getComponentsFactory() {
    return wrapped.getComponentsFactory();
  }

  @Override
  public String getType() {
    return "distributed";
  }

  @Override
  public void freeze(final boolean throwException) {
    getFreezableStorage().freeze(throwException);
  }

  @Override
  public void release() {
    getFreezableStorage().release();
  }

  @Override
  public void backup(final OutputStream out, final Map<String, Object> options, final Callable<Object> callable,
      final OCommandOutputListener iListener, final int compressionLevel, final int bufferSize) throws IOException {
    wrapped.backup(out, options, callable, iListener, compressionLevel, bufferSize);
  }

  @Override
  public void restore(final InputStream in, final Map<String, Object> options, final Callable<Object> callable,
      final OCommandOutputListener iListener) throws IOException {
    wrapped.restore(in, options, callable, iListener);
  }

  @Override
  public long getLastOperationId() {
    return lastOperationId.get();
  }

  public void setLastOperationId(final long lastOperationId) {
    this.lastOperationId.set(lastOperationId);
  }

  public void pushDeletedRecord(final ORecordId rid, final ORecordVersion version) {
    deletedRecords.putIfAbsent(rid, new OPair<Long, ORecordVersion>(System.currentTimeMillis(), version));
  }

  public boolean resurrectDeletedRecord(final ORecordId rid) {
    return deletedRecords.remove(rid) != null;
  }

  public String getClusterNameByRID(final ORecordId iRid) {
    final OCluster cluster = getClusterById(iRid.clusterId);
    return cluster != null ? cluster.getName() : "*";
  }

  @Override
  public String getStorageId() {
    return dManager.getLocalNodeName() + "." + getName();
  }

  protected void handleDistributedException(final String iMessage, final Exception e, final Object... iParams) {
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

  private OFreezableStorage getFreezableStorage() {
    if (wrapped instanceof OFreezableStorage)
      return ((OFreezableStorage) wrapped);
    else
      throw new UnsupportedOperationException("Storage engine " + wrapped.getType() + " does not support freeze operation");
  }
}
