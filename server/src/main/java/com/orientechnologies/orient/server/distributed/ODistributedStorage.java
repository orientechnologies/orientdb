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
package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.common.concur.ONeedRetryException;
import com.orientechnologies.common.concur.resource.OSharedResourceAdaptiveExternal;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.util.OPair;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.core.command.OCommandExecutor;
import com.orientechnologies.orient.core.command.OCommandManager;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.command.ODistributedCommand;
import com.orientechnologies.orient.core.command.script.OCommandScript;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.config.OStorageConfiguration;
import com.orientechnologies.orient.core.conflict.ORecordConflictStrategy;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.OScenarioThreadLocal;
import com.orientechnologies.orient.core.db.OScenarioThreadLocal.RUN_MODE;
import com.orientechnologies.orient.core.db.record.OCurrentStorageComponentsFactory;
import com.orientechnologies.orient.core.db.record.OPlaceholder;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.db.record.ridbag.sbtree.OSBTreeCollectionManager;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.exception.OTransactionException;
import com.orientechnologies.orient.core.exception.OValidationException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.clusterselection.OClusterSelectionStrategy;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandExecutorSQLDelegate;
import com.orientechnologies.orient.core.sql.OCommandExecutorSQLSelect;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.functions.OSQLFunctionRuntime;
import com.orientechnologies.orient.core.storage.OAutoshardedStorage;
import com.orientechnologies.orient.core.storage.OCluster;
import com.orientechnologies.orient.core.storage.OPhysicalPosition;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import com.orientechnologies.orient.core.storage.ORecordCallback;
import com.orientechnologies.orient.core.storage.ORecordMetadata;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.OStorageOperationResult;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.OFreezableStorage;
import com.orientechnologies.orient.core.tx.OTransaction;
import com.orientechnologies.orient.core.tx.OTransactionAbstract;
import com.orientechnologies.orient.core.tx.OTransactionInternal;
import com.orientechnologies.orient.core.tx.OTransactionRealAbstract;
import com.orientechnologies.orient.core.version.ORecordVersion;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.ODistributedRequest.EXECUTION_MODE;
import com.orientechnologies.orient.server.distributed.task.*;

import java.io.File;
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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Distributed storage implementation that routes to the owner node the request.
 *
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 */
public class ODistributedStorage implements OStorage, OFreezableStorage, OAutoshardedStorage {
  protected final OServer                                                   serverInstance;
  protected final ODistributedServerManager                                 dManager;
  protected final OAbstractPaginatedStorage                                 wrapped;

  protected final TimerTask                                                 purgeDeletedRecordsTask;
  protected final ConcurrentHashMap<ORecordId, OPair<Long, ORecordVersion>> deletedRecords  = new ConcurrentHashMap<ORecordId, OPair<Long, ORecordVersion>>();
  protected final AtomicLong                                                lastOperationId = new AtomicLong();

  protected final BlockingQueue<OAsynchDistributedOperation>                asynchronousOperationsQueue;
  protected final Thread                                                    asynchWorker;
  protected volatile boolean                                                running         = true;
  protected volatile File                                                   lastValidBackup = null;

  public ODistributedStorage(final OServer iServer, final OAbstractPaginatedStorage wrapped) {
    this.serverInstance = iServer;
    this.dManager = iServer.getDistributedManager();
    this.wrapped = wrapped;

    ODistributedServerLog.info(this, dManager != null ? dManager.getLocalNodeName() : "?", null,
        ODistributedServerLog.DIRECTION.NONE, "Installing distributed storage on database '%s'", wrapped.getName());

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

    Orient.instance().scheduleTask(purgeDeletedRecordsTask,
        OGlobalConfiguration.DISTRIBUTED_PURGE_RESPONSES_TIMER_DELAY.getValueAsLong(),
        OGlobalConfiguration.DISTRIBUTED_PURGE_RESPONSES_TIMER_DELAY.getValueAsLong());

    final int queueSize = OGlobalConfiguration.DISTRIBUTED_ASYNCH_QUEUE_SIZE.getValueAsInteger();
    if (queueSize <= 0)
      asynchronousOperationsQueue = new LinkedBlockingQueue<OAsynchDistributedOperation>();
    else
      asynchronousOperationsQueue = new LinkedBlockingQueue<OAsynchDistributedOperation>(queueSize);

    asynchWorker = new Thread() {
      @Override
      public void run() {
        while (running || !asynchronousOperationsQueue.isEmpty()) {
          try {
            final OAsynchDistributedOperation operation = asynchronousOperationsQueue.take();

            dManager.sendRequest(operation.getDatabaseName(), operation.getClusterNames(), operation.getNodes(),
                operation.getTask(), EXECUTION_MODE.NO_RESPONSE);

          } catch (InterruptedException e) {

            final int pendingMessages = asynchronousOperationsQueue.size();
            if (pendingMessages > 0)
              ODistributedServerLog.warn(this, dManager != null ? dManager.getLocalNodeName() : "?", null,
                  ODistributedServerLog.DIRECTION.NONE,
                  "Received shutdown signal, waiting for asynchronous queue is empty (pending msgs=%d)...", pendingMessages);

            Thread.interrupted();

          } catch (Throwable e) {
            // ASYNCH: IGNORE IT
            ODistributedServerLog.error(this, dManager != null ? dManager.getLocalNodeName() : "?", null,
                ODistributedServerLog.DIRECTION.OUT, "Error on executing asynch operation", e);
          }
        }
        ODistributedServerLog.warn(this, dManager != null ? dManager.getLocalNodeName() : "?", null,
            ODistributedServerLog.DIRECTION.NONE, "Shutdown asynchronous queue worker completed");
      }
    };
    asynchWorker.setName("OrientDB Distributed asynch ops node=" + getNodeId() + " db=" + getName());
    asynchWorker.start();
  }

  @Override
  public boolean isDistributed() {
    return true;
  }

  @Override
  public boolean isAssigningClusterIds() {
    return true;
  }

  @Override
  public Class<? extends OSBTreeCollectionManager> getCollectionManagerClass() {
    return wrapped.getCollectionManagerClass();
  }

  public Object command(final OCommandRequestText iCommand) {

    List<String> servers = (List<String>) iCommand.getContext().getVariable("servers");
    if (servers == null) {
      servers = new ArrayList<String>();
      iCommand.getContext().setVariable("servers", servers);
    }
    servers.add(dManager.getLocalNodeName());

    if (OScenarioThreadLocal.INSTANCE.get() == RUN_MODE.RUNNING_DISTRIBUTED)
      // ALREADY DISTRIBUTED
      return wrapped.command(iCommand);

    final ODistributedConfiguration dbCfg = dManager.getDatabaseConfiguration(getName());
    if (!dbCfg.isReplicationActive(null, dManager.getLocalNodeName()))
      // DON'T REPLICATE
      return wrapped.command(iCommand);

    final OCommandExecutor executor = OCommandManager.instance().getExecutor(iCommand);

    executor.setProgressListener(iCommand.getProgressListener());
    executor.parse(iCommand);

    final OCommandExecutor exec = executor instanceof OCommandExecutorSQLDelegate ? ((OCommandExecutorSQLDelegate) executor)
        .getDelegate() : executor;

    try {
      final OAbstractCommandTask task = iCommand instanceof OCommandScript ? new OScriptTask(iCommand) : new OSQLCommandTask(
          iCommand);

      Object result = null;
      OCommandDistributedReplicateRequest.DISTRIBUTED_EXECUTION_MODE executionMode = OCommandDistributedReplicateRequest.DISTRIBUTED_EXECUTION_MODE.LOCAL;
      if (OScenarioThreadLocal.INSTANCE.get() != RUN_MODE.RUNNING_DISTRIBUTED) {
        if (exec instanceof OCommandDistributedReplicateRequest)
          executionMode = ((OCommandDistributedReplicateRequest) exec).getDistributedExecutionMode();
      }

      switch (executionMode) {
      case LOCAL:
        return wrapped.command(iCommand);

      case REPLICATE: {
        // REPLICATE IT, GET ALL THE INVOLVED NODES
        final Collection<String> involvedClusters = exec.getInvolvedClusters();
        final Collection<String> nodes;

        task.setResultStrategy(OAbstractRemoteTask.RESULT_STRATEGY.ANY);

        nodes = dbCfg.getServers(involvedClusters);
        if (iCommand instanceof ODistributedCommand)
          nodes.removeAll(((ODistributedCommand) iCommand).nodesToExclude());

        if (nodes.isEmpty())
          // / NO NODE TO REPLICATE
          return null;

        result = dManager.sendRequest(getName(), involvedClusters, nodes, task, EXECUTION_MODE.RESPONSE);

        dManager.propagateSchemaChanges(ODatabaseRecordThreadLocal.INSTANCE.get());

        break;
      }

      case SHARDED: {
        // SHARDED, GET ONLY ONE NODE PER INVOLVED CLUSTER
        final Collection<String> involvedClusters = exec.getInvolvedClusters();
        final Collection<String> nodes;

        task.setResultStrategy(OAbstractRemoteTask.RESULT_STRATEGY.UNION);

        nodes = dbCfg.getOneServerPerCluster(involvedClusters, dManager.getLocalNodeName());

        if (iCommand instanceof ODistributedCommand)
          nodes.removeAll(((ODistributedCommand) iCommand).nodesToExclude());

        boolean executeLocally = false;
        if (exec.isIdempotent()) {
          // IDEMPOTENT: CHECK IF CAN WORK LOCALLY ONLY
          int maxReadQuorum;
          if (involvedClusters.isEmpty())
            maxReadQuorum = dbCfg.getReadQuorum(null);
          else {
            maxReadQuorum = 0;
            for (String cl : involvedClusters)
              maxReadQuorum = Math.max(maxReadQuorum, dbCfg.getReadQuorum(cl));
          }

          if (nodes.size() == 1 && nodes.iterator().next().equals(dManager.getLocalNodeName()) && maxReadQuorum <= 1)
            executeLocally = true;

        } else if (nodes.size() == 1 && nodes.iterator().next().equals(dManager.getLocalNodeName()))
          executeLocally = true;

        if (executeLocally)
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
                  final Object nodeResult = entry.getValue();
                  if (nodeResult instanceof Collection)
                    set.addAll((Collection<?>) nodeResult);
                  else if (nodeResult instanceof Exception)
                    // RECEIVED EXCEPTION
                    throw (Exception) nodeResult;
                }
                result = new ArrayList<Object>(set);
              }
            }
          }
        }
        break;
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

  public OStorageOperationResult<OPhysicalPosition> createRecord(final ORecordId iRecordId, final byte[] iContent,
      final ORecordVersion iRecordVersion, final byte iRecordType, final int iMode, final ORecordCallback<Long> iCallback) {
    resetLastValidBackup();

    if (OScenarioThreadLocal.INSTANCE.get() == RUN_MODE.RUNNING_DISTRIBUTED)
      // ALREADY DISTRIBUTED
      return wrapped.createRecord(iRecordId, iContent, iRecordVersion, iRecordType, iMode, iCallback);

    try {
      // ASSIGN DESTINATION NODE
      String clusterName = getClusterNameByRID(iRecordId);

      int clusterId = iRecordId.getClusterId();
      if (clusterId == ORID.CLUSTER_ID_INVALID)
        throw new IllegalArgumentException("Cluster not valid");

      final ODistributedConfiguration dbCfg = dManager.getDatabaseConfiguration(getName());
      List<String> nodes = dbCfg.getServers(clusterName, null);

      if (nodes.isEmpty()) {
        // DON'T REPLICATE OR DISTRIBUTE
        return (OStorageOperationResult<OPhysicalPosition>) ODistributedAbstractPlugin.runInDistributedMode(new Callable() {
          @Override
          public Object call() throws Exception {
            return wrapped.createRecord(iRecordId, iContent, iRecordVersion, iRecordType, iMode, iCallback);
          }
        });
      }

      String masterNode = nodes.get(0);
      if (!masterNode.equals(dManager.getLocalNodeName())) {
        final OCluster cl = getClusterByName(clusterName);
        final ODatabaseDocumentInternal db = ODatabaseRecordThreadLocal.INSTANCE.get();
        final OClass cls = db.getMetadata().getSchema().getClassByClusterId(cl.getId());
        String newClusterName = null;
        if (cls != null) {
          OClusterSelectionStrategy clSel = cls.getClusterSelection();
          if (!(clSel instanceof OLocalClusterStrategy)) {
            dManager.propagateSchemaChanges(db);
            clSel = cls.getClusterSelection();
          }

          newClusterName = getPhysicalClusterNameById(clSel.getCluster(cls, null));
          nodes = dbCfg.getServers(newClusterName, null);
          masterNode = nodes.get(0);
        }

        if (!masterNode.equals(dManager.getLocalNodeName()))
          throw new ODistributedException("Error on inserting into cluster '" + clusterName + "' where local node '"
              + dManager.getLocalNodeName() + "' is not the master of it, but it's '" + masterNode + "'");

        OLogManager.instance().warn(
            this,
            "Local node '" + dManager.getLocalNodeName() + "' is not the master for cluster '" + clusterName + "' (it's '"
                + masterNode + "'). Switching to a valid cluster of the same class: '" + newClusterName + "'");

        clusterName = newClusterName;
      }

      Boolean executionModeSynch = dbCfg.isExecutionModeSynchronous(clusterName);
      if (executionModeSynch == null)
        executionModeSynch = iMode == 0;

      if (executionModeSynch) {
        // SYNCHRONOUS CALL: REPLICATE IT
        final Object masterResult = dManager.sendRequest(getName(), Collections.singleton(clusterName), nodes,
            new OCreateRecordTask(iRecordId, iContent, iRecordVersion, iRecordType), EXECUTION_MODE.RESPONSE);

        if (masterResult instanceof ONeedRetryException)
          throw (ONeedRetryException) masterResult;
        else if (masterResult instanceof Throwable)
          throw new ODistributedException("Error on execution distributed CREATE_RECORD", (Throwable) masterResult);

        // COPY THE CLUSTER POS -> RID
        final OPlaceholder masterPlaceholder = (OPlaceholder) masterResult;
        iRecordId.copyFrom(masterPlaceholder.getIdentity());

        return new OStorageOperationResult<OPhysicalPosition>(new OPhysicalPosition(masterPlaceholder.getIdentity()
            .getClusterPosition(), masterPlaceholder.getRecordVersion()));
      }

      final OStorageOperationResult<OPhysicalPosition> localResult;

      localResult = (OStorageOperationResult<OPhysicalPosition>) ODistributedAbstractPlugin.runInDistributedMode(new Callable() {
        @Override
        public Object call() throws Exception {
          return wrapped.createRecord(iRecordId, iContent, iRecordVersion, iRecordType, iMode, iCallback);
        }
      });

      // ASYNCHRONOUSLY REPLICATE IT TO ALL THE OTHER NODES
      nodes.remove(dManager.getLocalNodeName());
      if (!nodes.isEmpty()) {
        asynchronousExecution(new OAsynchDistributedOperation(getName(), Collections.singleton(clusterName), nodes,
            new OCreateRecordTask(iRecordId, iContent, iRecordVersion, iRecordType)));
      }

      return localResult;

    } catch (ONeedRetryException e) {
      // PASS THROUGH
      throw e;
    } catch (Exception e) {
      handleDistributedException("Cannot route CREATE_RECORD operation for %s to the distributed node", e, iRecordId);
      // UNREACHABLE
      return null;
    }
  }

  public OStorageOperationResult<ORawBuffer> readRecord(final ORecordId iRecordId, final String iFetchPlan,
      final boolean iIgnoreCache, final ORecordCallback<ORawBuffer> iCallback) {

    if (deletedRecords.get(iRecordId) != null)
      // DELETED
      throw new ORecordNotFoundException("Record " + iRecordId + " was not found");

    try {
      final String clusterName = getClusterNameByRID(iRecordId);

      final ODistributedConfiguration dbCfg = dManager.getDatabaseConfiguration(getName());
      final List<String> nodes = dbCfg.getServers(clusterName, null);

      // CHECK IF LOCAL NODE OWNS THE DATA AND READ-QUORUM = 1: GET IT LOCALLY BECAUSE IT'S FASTER
      if (nodes.isEmpty() || nodes.contains(dManager.getLocalNodeName()) && dbCfg.getReadQuorum(clusterName) <= 1) {
        // DON'T REPLICATE
        return (OStorageOperationResult<ORawBuffer>) ODistributedAbstractPlugin.runInDistributedMode(new Callable() {
          @Override
          public Object call() throws Exception {
            return wrapped.readRecord(iRecordId, iFetchPlan, iIgnoreCache, iCallback);
          }
        });
      }

      // DISTRIBUTE IT
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
      handleDistributedException("Cannot route READ_RECORD operation for %s to the distributed node", e, iRecordId);
      // UNREACHABLE
      return null;
    }
  }

  @Override
  public OStorageOperationResult<ORecordVersion> updateRecord(final ORecordId iRecordId, final boolean updateContent,
      final byte[] iContent, final ORecordVersion iVersion, final byte iRecordType, final int iMode,
      final ORecordCallback<ORecordVersion> iCallback) {
    resetLastValidBackup();

    if (deletedRecords.get(iRecordId) != null)
      // DELETED
      throw new ORecordNotFoundException("Record " + iRecordId + " was not found");

    if (OScenarioThreadLocal.INSTANCE.get() == RUN_MODE.RUNNING_DISTRIBUTED)
      // ALREADY DISTRIBUTED
      return wrapped.updateRecord(iRecordId, updateContent, iContent, iVersion, iRecordType, iMode, iCallback);

    try {
      final String clusterName = getClusterNameByRID(iRecordId);

      final ODistributedConfiguration dbCfg = dManager.getDatabaseConfiguration(getName());
      final List<String> nodes = dbCfg.getServers(clusterName, null);

      if (nodes.isEmpty()) {

        // DON'T REPLICATE OR DISTRIBUTE
        return (OStorageOperationResult<ORecordVersion>) ODistributedAbstractPlugin.runInDistributedMode(new Callable() {
          @Override
          public Object call() throws Exception {
            return wrapped.updateRecord(iRecordId, updateContent, iContent, iVersion, iRecordType, iMode, iCallback);
          }
        });
      }

      Boolean executionModeSynch = dbCfg.isExecutionModeSynchronous(clusterName);
      if (executionModeSynch == null)
        executionModeSynch = iMode == 0;

      if (executionModeSynch) {
        // SYNCHRONOUS: LOAD PREVIOUS CONTENT TO BE USED IN CASE OF UNDO
        final OStorageOperationResult<ORawBuffer> previousContent = readRecord(iRecordId, null, false, null);

        // REPLICATE IT
        final Object result = dManager.sendRequest(getName(), Collections.singleton(clusterName), nodes, new OUpdateRecordTask(
            iRecordId, previousContent.getResult().getBuffer(), previousContent.getResult().version, iContent, iVersion,
            iRecordType), EXECUTION_MODE.RESPONSE);

        if (result instanceof ONeedRetryException)
          throw (ONeedRetryException) result;
        else if (result instanceof Throwable)
          throw new ODistributedException("Error on execution distributed UPDATE_RECORD", (Throwable) result);

        // UPDATE LOCALLY
        return new OStorageOperationResult<ORecordVersion>((ORecordVersion) result);
      }

      final OStorageOperationResult<ORecordVersion> localResult;
      localResult = (OStorageOperationResult<ORecordVersion>) ODistributedAbstractPlugin.runInDistributedMode(new Callable() {
        @Override
        public Object call() throws Exception {
          return wrapped.updateRecord(iRecordId, updateContent, iContent, iVersion, iRecordType, iMode, iCallback);
        }
      });

      nodes.remove(dManager.getLocalNodeName());
      if (!nodes.isEmpty()) {
        // LOAD PREVIOUS CONTENT TO BE USED IN CASE OF UNDO
        final OStorageOperationResult<ORawBuffer> previousContent = readRecord(iRecordId, null, false, null);

        asynchronousExecution(new OAsynchDistributedOperation(getName(), Collections.singleton(clusterName), nodes,
            new OUpdateRecordTask(iRecordId, previousContent.getResult().getBuffer(), previousContent.getResult().version,
                iContent, iVersion, iRecordType)));
      }

      return localResult;

    } catch (ONeedRetryException e) {
      // PASS THROUGH
      throw e;
    } catch (Exception e) {
      handleDistributedException("Cannot route UPDATE_RECORD operation for %s to the distributed node", e, iRecordId);
      // UNREACHABLE
      return null;
    }
  }

  @Override
  public OStorageOperationResult<Boolean> deleteRecord(final ORecordId iRecordId, final ORecordVersion iVersion, final int iMode,
      final ORecordCallback<Boolean> iCallback) {
    resetLastValidBackup();

    if (OScenarioThreadLocal.INSTANCE.get() == RUN_MODE.RUNNING_DISTRIBUTED)
      // ALREADY DISTRIBUTED
      return wrapped.deleteRecord(iRecordId, iVersion, iMode, iCallback);

    try {
      final String clusterName = getClusterNameByRID(iRecordId);

      final ODistributedConfiguration dbCfg = dManager.getDatabaseConfiguration(getName());
      final List<String> nodes = dbCfg.getServers(clusterName, null);

      if (nodes.isEmpty()) {
        // DON'T REPLICATE OR DISTRIBUTE
        return (OStorageOperationResult<Boolean>) ODistributedAbstractPlugin.runInDistributedMode(new Callable() {
          @Override
          public Object call() throws Exception {
            return wrapped.deleteRecord(iRecordId, iVersion, iMode, iCallback);
          }
        });
      }

      Boolean executionModeSynch = dbCfg.isExecutionModeSynchronous(clusterName);
      if (executionModeSynch == null)
        executionModeSynch = iMode == 0;

      if (executionModeSynch) {
        // REPLICATE IT
        final Object result = dManager.sendRequest(getName(), Collections.singleton(clusterName), nodes, new ODeleteRecordTask(
            iRecordId, iVersion), EXECUTION_MODE.RESPONSE);

        if (result instanceof ONeedRetryException)
          throw (ONeedRetryException) result;
        else if (result instanceof Throwable)
          throw new ODistributedException("Error on execution distributed DELETE_RECORD", (Throwable) result);

        return new OStorageOperationResult<Boolean>(true);
      }

      final OStorageOperationResult<Boolean> localResult;
      localResult = (OStorageOperationResult<Boolean>) ODistributedAbstractPlugin.runInDistributedMode(new Callable() {
        @Override
        public Object call() throws Exception {
          return wrapped.deleteRecord(iRecordId, iVersion, iMode, iCallback);
        }
      });

      nodes.remove(dManager.getLocalNodeName());
      if (!nodes.isEmpty())
        asynchronousExecution(new OAsynchDistributedOperation(getName(), Collections.singleton(clusterName), nodes,
            new ODeleteRecordTask(iRecordId, iVersion)));

      return localResult;

    } catch (ONeedRetryException e) {
      // PASS THROUGH
      throw e;
    } catch (Exception e) {
      handleDistributedException("Cannot route DELETE_RECORD operation for %s to the distributed node", e, iRecordId);
      // UNREACHABLE
      return null;
    }
  }

  @Override
  public OStorageOperationResult<Boolean> hideRecord(ORecordId recordId, int mode, ORecordCallback<Boolean> callback) {
    throw new UnsupportedOperationException();
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

  public OCluster getClusterByName(final String iName) {
    return wrapped.getClusterByName(iName);
  }

  @Override
  public ORecordConflictStrategy getConflictStrategy() {
    return getUnderlying().getConflictStrategy();
  }

  @Override
  public void setConflictStrategy(final ORecordConflictStrategy iResolver) {
    getUnderlying().setConflictStrategy(iResolver);
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
    close(false, false);
  }

  @Override
  public void close(final boolean iForce, boolean onDelete) {
    wrapped.close(iForce, onDelete);

    if (isClosed())
      shutdownAsynchronousWorker();
  }

  @Override
  public boolean isClosed() {
    return wrapped.isClosed();
  }

  @Override
  public void commit(final OTransaction iTx, final Runnable callback) {
    if (OScenarioThreadLocal.INSTANCE.get() == RUN_MODE.RUNNING_DISTRIBUTED) {
      // ALREADY DISTRIBUTED
      wrapped.commit(iTx, callback);
      return;
    }

    try {
      final ODistributedConfiguration dbCfg = dManager.getDatabaseConfiguration(getName());
      if (!dbCfg.isReplicationActive(null, dManager.getLocalNodeName())) {
        // DON'T REPLICATE
        ODistributedAbstractPlugin.runInDistributedMode(new Callable() {
          @Override
          public Object call() throws Exception {
            wrapped.commit(iTx, callback);
            return null;
          }
        });

        return;
      }

      OTransactionInternal.setStatus((OTransactionAbstract) iTx, OTransaction.TXSTATUS.BEGUN);

      final OTxTask txTask = new OTxTask();
      final Set<String> involvedClusters = new HashSet<String>();

      final Set<String> nodes = dbCfg.getServers(involvedClusters);

      Boolean executionModeSynch = dbCfg.isExecutionModeSynchronous(null);
      if (executionModeSynch == null)
        executionModeSynch = Boolean.TRUE;

      final List<ORecordOperation> tmpEntries = new ArrayList<ORecordOperation>();

      while (iTx.getCurrentRecordEntries().iterator().hasNext()) {
        for (ORecordOperation txEntry : iTx.getCurrentRecordEntries())
          tmpEntries.add(txEntry);

        iTx.clearRecordEntries();

        for (ORecordOperation op : tmpEntries) {
          final OAbstractRecordReplicatedTask task;

          final ORecord record = op.getRecord();

          final ORecordId rid = (ORecordId) record.getIdentity();

          switch (op.type) {
          case ORecordOperation.CREATED: {
            if (rid.isNew()) {
              // CREATE THE TASK PASSING THE RECORD OR A COPY BASED ON EXECUTION TYPE: IF ASYNCHRONOUS THE COPY PREVENT TO EARLY
              // ASSIGN CLUSTER IDS
              final ORecord rec = executionModeSynch ? record : record.copy();
              task = new OCreateRecordTask(rec);
              if (record instanceof ODocument)
                ((ODocument) record).validate();
              break;
            }
            // ELSE TREAT IT AS UPDATE: GO DOWN
          }

          case ORecordOperation.UPDATED: {
            if (record instanceof ODocument)
              ((ODocument) record).validate();

            // LOAD PREVIOUS CONTENT TO BE USED IN CASE OF UNDO
            final OStorageOperationResult<ORawBuffer> previousContent = wrapped.readRecord(rid, null, false, null);

            if (previousContent.getResult() == null)
              // DELETED
              throw new OTransactionException("Cannot update record '" + rid + "' because has been deleted");

            final ORecordVersion v = executionModeSynch ? record.getRecordVersion() : record.getRecordVersion().copy();

            task = new OUpdateRecordTask(rid, previousContent.getResult().getBuffer(), previousContent.getResult().version,
                record.toStream(), v, ORecordInternal.getRecordType(record));

            break;
          }

          case ORecordOperation.DELETED: {
            final ORecordVersion v = executionModeSynch ? record.getRecordVersion() : record.getRecordVersion().copy();
            task = new ODeleteRecordTask(rid, v);
            break;
          }

          default:
            continue;
          }

          involvedClusters.add(getClusterNameByRID(rid));
          txTask.add(task);
        }
      }

      OTransactionInternal.setStatus((OTransactionAbstract) iTx, OTransaction.TXSTATUS.COMMITTING);

      // if (executionModeSynch && !iTx.hasRecordCreation()) {
      if (executionModeSynch) {
        // SYNCHRONOUS CALL: REPLICATE IT
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

          // RESET DIRTY FLAGS TO AVOID CALLING AUTO-SAVE
          for (ORecordOperation op : tmpEntries) {
            final ORecord record = op.getRecord();
            if (record != null)
              ORecordInternal.unsetDirty(record);
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

        return;
      }

      // ASYNCH
      ODistributedAbstractPlugin.runInDistributedMode(new Callable() {
        @Override
        public Object call() throws Exception {
          ((OTransactionRealAbstract) iTx).restore();
          wrapped.commit(iTx, callback);
          return null;
        }
      });

      nodes.remove(dManager.getLocalNodeName());
      if (!nodes.isEmpty()) {
        if (executionModeSynch)
          dManager.sendRequest(getName(), involvedClusters, nodes, txTask, EXECUTION_MODE.RESPONSE);
        else
          // ASYNCHRONOUSLY REPLICATE IT TO ALL THE OTHER NODES
          asynchronousExecution(new OAsynchDistributedOperation(getName(), involvedClusters, nodes, txTask));
      }

    } catch (OValidationException e) {
      throw e;
    } catch (Exception e) {
      handleDistributedException("Cannot route TX operation against distributed node", e);
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
  public int addCluster(final String iClusterName, boolean forceListBased, final Object... iParameters) {
    for (int retry = 0; retry < 10; ++retry) {
      int clId = wrapped.addCluster(iClusterName, false, iParameters);

      if (OScenarioThreadLocal.INSTANCE.get() == RUN_MODE.DEFAULT) {

        final StringBuilder cmd = new StringBuilder("create cluster ");
        cmd.append(iClusterName);

        // EXECUTE THIS OUTSIDE LOCK TO AVOID DEADLOCKS
        OCommandSQL commandSQL = new OCommandSQL(cmd.toString());
        commandSQL.addExcludedNode(getNodeId());

        final Object result = command(commandSQL);
        if (result != null && ((Integer) result).intValue() != clId) {
          OLogManager.instance().warn(this,
              "Error on creating cluster on distributed nodes: ids are different (local=%d and remote=%d). Retrying %d/%d...",
              clId, ((Integer) result).intValue(), retry, 10);

          wrapped.dropCluster(clId, false);

          // REMOVE ON REMOTE NODES TOO
          cmd.setLength(0);
          cmd.append("drop cluster ");
          cmd.append(iClusterName);

          commandSQL = new OCommandSQL(cmd.toString());
          commandSQL.addExcludedNode(getNodeId());

          command(commandSQL);

          try {
            Thread.sleep(300);
          } catch (InterruptedException e) {
          }

          wrapped.reload();
          continue;
        }
      }
      return clId;
    }

    throw new ODistributedException("Error on creating cluster on distributed nodes: local and remote ids assigned are different");
  }

  @Override
  public int addCluster(String iClusterName, int iRequestedId, boolean forceListBased, Object... iParameters) {
    return wrapped.addCluster(iClusterName, iRequestedId, forceListBased, iParameters);
  }

  public boolean dropCluster(final String iClusterName, final boolean iTruncate) {
    return wrapped.dropCluster(iClusterName, iTruncate);
  }

  @Override
  public boolean dropCluster(final int iId, final boolean iTruncate) {
    return wrapped.dropCluster(iId, iTruncate);
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
  public long[] getClusterDataRange(final int currentClusterId) {
    return wrapped.getClusterDataRange(currentClusterId);
  }

  @Override
  public <V> V callInLock(final Callable<V> iCallable, final boolean iExclusiveLock) {
    return wrapped.callInLock(iCallable, iExclusiveLock);
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
    resetLastValidBackup();

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

  @Override
  public String getNodeId() {
    return dManager != null ? dManager.getLocalNodeName() : "?";
  }

  public void shutdownAsynchronousWorker() {
    running = false;
    asynchWorker.interrupt();
    try {
      asynchWorker.join();
    } catch (InterruptedException e) {
    }
    asynchronousOperationsQueue.clear();
  }

  public File getLastValidBackup() {
    return lastValidBackup;
  }

  public void setLastValidBackup(final File lastValidBackup) {
    this.lastValidBackup = lastValidBackup;
  }

  protected void asynchronousExecution(final OAsynchDistributedOperation iOperation) {
    asynchronousOperationsQueue.offer(iOperation);
  }

  protected void handleDistributedException(final String iMessage, final Exception e, final Object... iParams) {
    if (e != null) {
      if (e instanceof OException)
        throw (OException) e;
      else if (e.getCause() instanceof OException)
        throw (OException) e.getCause();
      else if (e.getCause() != null && e.getCause().getCause() instanceof OException)
        throw (OException) e.getCause().getCause();
    }

    OLogManager.instance().error(this, iMessage, e, iParams);
    throw new OStorageException(String.format(iMessage, iParams), e);
  }

  private OFreezableStorage getFreezableStorage() {
    if (wrapped instanceof OFreezableStorage)
      return ((OFreezableStorage) wrapped);
    else
      throw new UnsupportedOperationException("Storage engine " + wrapped.getType() + " does not support freeze operation");
  }

  private void resetLastValidBackup() {
    if (lastValidBackup != null)
      lastValidBackup = null;
  }
}
