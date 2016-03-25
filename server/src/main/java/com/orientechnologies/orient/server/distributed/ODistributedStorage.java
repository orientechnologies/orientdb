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
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.common.util.OPair;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.*;
import com.orientechnologies.orient.core.command.script.OCommandScript;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.config.OStorageConfiguration;
import com.orientechnologies.orient.core.conflict.ORecordConflictStrategy;
import com.orientechnologies.orient.core.db.*;
import com.orientechnologies.orient.core.db.OScenarioThreadLocal.RUN_MODE;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
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
import com.orientechnologies.orient.core.record.ORecordVersionHelper;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.replication.OAsyncReplicationError;
import com.orientechnologies.orient.core.replication.OAsyncReplicationOk;
import com.orientechnologies.orient.core.sql.OCommandExecutorSQLDelegate;
import com.orientechnologies.orient.core.sql.OCommandExecutorSQLSelect;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.functions.OSQLFunctionRuntime;
import com.orientechnologies.orient.core.storage.*;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.OFreezableStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OLocalPaginatedStorage;
import com.orientechnologies.orient.core.tx.OTransaction;
import com.orientechnologies.orient.core.tx.OTransactionAbstract;
import com.orientechnologies.orient.core.tx.OTransactionInternal;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.ODistributedRequest.EXECUTION_MODE;
import com.orientechnologies.orient.server.distributed.task.*;
import com.orientechnologies.orient.server.security.OSecurityServerUser;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Distributed storage implementation that routes to the owner node the request.
 *
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 */
public class ODistributedStorage implements OStorage, OFreezableStorage, OAutoshardedStorage {
  protected final OServer                                            serverInstance;
  protected final ODistributedServerManager                          dManager;
  protected final OAbstractPaginatedStorage                          wrapped;

  protected final TimerTask                                          purgeDeletedRecordsTask;
  protected final ConcurrentHashMap<ORecordId, OPair<Long, Integer>> deletedRecords        = new ConcurrentHashMap<ORecordId, OPair<Long, Integer>>();

  protected final BlockingQueue<OAsynchDistributedOperation>         asynchronousOperationsQueue;
  protected final Thread                                             asynchWorker;
  protected volatile boolean                                         running               = true;
  protected volatile File                                            lastValidBackup       = null;
  private ODistributedServerManager.DB_STATUS                        prevStatus;
  private final ReentrantReadWriteLock                               messageManagementLock = new ReentrantReadWriteLock();
  // ARRAY OF LOCKS FOR CONCURRENT OPERATIONS ON CLUSTERS
  private final Semaphore[]                                          clusterLocks;

  public ODistributedStorage(final OServer iServer, final OAbstractPaginatedStorage wrapped) {
    this.serverInstance = iServer;
    this.dManager = iServer.getDistributedManager();
    this.wrapped = wrapped;

    ODistributedServerLog.debug(this, dManager != null ? dManager.getLocalNodeName() : "?", null,
        ODistributedServerLog.DIRECTION.NONE, "Installing distributed storage on database '%s'", wrapped.getName());

    purgeDeletedRecordsTask = new TimerTask() {
      @Override
      public void run() {
        try {
          final long now = System.currentTimeMillis();
          for (Iterator<Map.Entry<ORecordId, OPair<Long, Integer>>> it = deletedRecords.entrySet().iterator(); it.hasNext();) {
            final Map.Entry<ORecordId, OPair<Long, Integer>> entry = it.next();

            try {
              final ORecordId rid = entry.getKey();
              final long time = entry.getValue().getKey();
              final int version = entry.getValue().getValue();

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
        } catch (Throwable e) {
          OLogManager.instance().debug(this, "Error on purge deleted record tasks for database %s", e, getName());
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
        while (running) { // || !asynchronousOperationsQueue.isEmpty()
          try {
            final OAsynchDistributedOperation operation = asynchronousOperationsQueue.take();

            ODistributedRequestId reqId = null;
            try {
              final ODistributedResponse dResponse = dManager.sendRequest(operation.getDatabaseName(), operation.getClusterNames(),
                  operation.getNodes(), operation.getTask(),
                  operation.getCallback() != null ? EXECUTION_MODE.RESPONSE : EXECUTION_MODE.NO_RESPONSE,
                  operation.getLocalResult(), operation.getAfterRequestCallback());

              reqId = dResponse.getRequestId();

              if (operation.getCallback() != null)
                operation.getCallback().call(new OPair<ODistributedRequestId, Object>(reqId, dResponse.getPayload()));

            } finally {
              // ASSURE IT'S CALLED ANYWAY
              if (operation.getAfterRequestCallback() != null)
                operation.getAfterRequestCallback().call(reqId);
            }

          } catch (InterruptedException e) {

            final int pendingMessages = asynchronousOperationsQueue.size();
            if (pendingMessages > 0)
              ODistributedServerLog.info(this, dManager != null ? dManager.getLocalNodeName() : "?", null,
                  ODistributedServerLog.DIRECTION.NONE,
                  "Received shutdown signal, waiting for asynchronous queue is empty (pending msgs=%d)...", pendingMessages);

            Thread.interrupted();

          } catch (Throwable e) {
            if (running)
              // ASYNC: IGNORE IT
              if (e instanceof ONeedRetryException)
                ODistributedServerLog.debug(this, dManager != null ? dManager.getLocalNodeName() : "?", null,
                    ODistributedServerLog.DIRECTION.OUT, "Error on executing asynchronous operation", e);
              else
                ODistributedServerLog.error(this, dManager != null ? dManager.getLocalNodeName() : "?", null,
                    ODistributedServerLog.DIRECTION.OUT, "Error on executing asynchronous operation", e);
          }
        }
        ODistributedServerLog.debug(this, dManager != null ? dManager.getLocalNodeName() : "?", null,
            ODistributedServerLog.DIRECTION.NONE, "Shutdown asynchronous queue worker for database '%s' completed",
            wrapped.getName());
      }
    };
    asynchWorker.setName("OrientDB Distributed asynch ops node=" + getNodeId() + " db=" + getName());
    asynchWorker.start();

    clusterLocks = new Semaphore[32];
    for (int i = 0; i < clusterLocks.length; ++i)
      clusterLocks[i] = new Semaphore(1);
  }

  @Override
  public boolean isDistributed() {
    return true;
  }

  @Override
  public boolean isAssigningClusterIds() {
    return true;
  }

  public Object command(final OCommandRequestText iCommand) {

    List<String> servers = (List<String>) iCommand.getContext().getVariable("servers");
    if (servers == null) {
      servers = new ArrayList<String>();
      iCommand.getContext().setVariable("servers", servers);
    }
    final String localNodeName = dManager.getLocalNodeName();

    servers.add(localNodeName);

    if (OScenarioThreadLocal.INSTANCE.get() == RUN_MODE.RUNNING_DISTRIBUTED)
      // ALREADY DISTRIBUTED
      return wrapped.command(iCommand);

    final ODistributedConfiguration dbCfg = dManager.getDatabaseConfiguration(getName());
    if (!dbCfg.isReplicationActive(null, localNodeName))
      // DON'T REPLICATE
      return wrapped.command(iCommand);

    final OCommandExecutor executor = OCommandManager.instance().getExecutor(iCommand);

    executor.setProgressListener(iCommand.getProgressListener());
    executor.parse(iCommand);

    final OCommandExecutor exec = executor instanceof OCommandExecutorSQLDelegate
        ? ((OCommandExecutorSQLDelegate) executor).getDelegate() : executor;

    if (!exec.isIdempotent())
      checkNodeIsMaster(localNodeName, dbCfg);

    try {
      Object result = null;
      OCommandDistributedReplicateRequest.DISTRIBUTED_EXECUTION_MODE executionMode = OCommandDistributedReplicateRequest.DISTRIBUTED_EXECUTION_MODE.LOCAL;
      OCommandDistributedReplicateRequest.DISTRIBUTED_RESULT_MGMT resultMgmt = OCommandDistributedReplicateRequest.DISTRIBUTED_RESULT_MGMT.CHECK_FOR_EQUALS;

      if (OScenarioThreadLocal.INSTANCE.get() != RUN_MODE.RUNNING_DISTRIBUTED) {
        if (exec instanceof OCommandDistributedReplicateRequest) {
          executionMode = ((OCommandDistributedReplicateRequest) exec).getDistributedExecutionMode();
          resultMgmt = ((OCommandDistributedReplicateRequest) exec).getDistributedResultManagement();
        }
      }

      switch (executionMode) {
      case LOCAL:
        // CALL IN DEFAULT MODE TO LET OWN COMMAND TO REDISTRIBUTE CHANGES (LIKE INSERT)
        return wrapped.command(iCommand);

      case REPLICATE:
        // REPLICATE IT, GET ALL THE INVOLVED NODES
        final Collection<String> involvedClusters = exec.getInvolvedClusters();

        if (resultMgmt == OCommandDistributedReplicateRequest.DISTRIBUTED_RESULT_MGMT.MERGE) {

          final Map<String, Collection<String>> nodeClusterMap = dbCfg.getServerClusterMap(involvedClusters, localNodeName);

          final Map<String, Object> results;

          if (nodeClusterMap.size() == 1 && nodeClusterMap.keySet().iterator().next().equals(localNodeName)) {
            // LOCAL NODE, AVOID TO DISTRIBUTE IT
            // CALL IN DEFAULT MODE TO LET OWN COMMAND TO REDISTRIBUTE CHANGES (LIKE INSERT)
            result = wrapped.command(iCommand);

            results = new HashMap<String, Object>(1);
            results.put(localNodeName, result);

          } else {
            // SELECT: SPLIT CLASSES/CLUSTER IF ANY
            results = executeOnServers(iCommand, involvedClusters, nodeClusterMap);
          }

          final OCommandExecutorSQLSelect select = exec instanceof OCommandExecutorSQLSelect ? (OCommandExecutorSQLSelect) exec
              : null;

          if (select != null && select.isAnyFunctionAggregates() && !select.hasGroupBy()) {
            result = mergeResultByAggregation(select, results);
          } else {
            // MIX & FILTER RESULT SET AVOIDING DUPLICATES
            // TODO: ONCE OPTIMIZED (SEE ABOVE) AVOID TO FILTER HERE

            result = exec.mergeResults(results);
          }

        } else {
          final OAbstractCommandTask task = iCommand instanceof OCommandScript ? new OScriptTask(iCommand)
              : new OSQLCommandTask(iCommand, new HashSet<String>());
          task.setResultStrategy(OAbstractRemoteTask.RESULT_STRATEGY.ANY);

          final Set<String> nodes = dbCfg.getServers(involvedClusters);

          if (iCommand instanceof ODistributedCommand)
            nodes.removeAll(((ODistributedCommand) iCommand).nodesToExclude());

          if (nodes.isEmpty())
            // / NO NODE TO REPLICATE
            return null;

          if (executeLocally(localNodeName, dbCfg, exec, involvedClusters, nodes))
            // LOCAL NODE, AVOID TO DISTRIBUTE IT
            // CALL IN DEFAULT MODE TO LET OWN COMMAND TO REDISTRIBUTE CHANGES (LIKE INSERT)
            return wrapped.command(iCommand);

          final ODistributedResponse dResponse = dManager.sendRequest(getName(), involvedClusters, nodes, task,
              EXECUTION_MODE.RESPONSE, null, null);

          result = dResponse.getPayload();

          if (exec.involveSchema())
            // UPDATE THE SCHEMA
            dManager.propagateSchemaChanges(ODatabaseRecordThreadLocal.INSTANCE.get());
        }

        break;
      }

      if (result instanceof ONeedRetryException)
        throw (ONeedRetryException) result;
      else if (result instanceof Exception)
        throw OException.wrapException(new ODistributedException("Error on execution distributed COMMAND"), (Exception) result);

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

  protected Map<String, Object> executeOnServers(final OCommandRequestText iCommand, final Collection<String> involvedClusters,
      final Map<String, Collection<String>> nodeClusterMap) {

    final Map<String, Object> results = new HashMap<String, Object>(nodeClusterMap.size());

    // EXECUTE DIFFERENT TASK ON EACH SERVER
    final List<String> nodes = new ArrayList<String>(1);
    for (Map.Entry<String, Collection<String>> c : nodeClusterMap.entrySet()) {
      final String nodeName = c.getKey();

      if (!dManager.isNodeAvailable(nodeName, getName())) {

        ODistributedServerLog.warn(this, dManager.getLocalNodeName(), nodeName, ODistributedServerLog.DIRECTION.OUT,
            "Node '%s' is involved in the command '%s' against database '%s', but the node is not active. Excluding it", nodeName,
            iCommand, wrapped.getName());

      } else {

        final OAbstractCommandTask task = iCommand instanceof OCommandScript ? new OScriptTask(iCommand)
            : new OSQLCommandTask(iCommand, c.getValue());
        task.setResultStrategy(OAbstractRemoteTask.RESULT_STRATEGY.ANY);

        nodes.clear();
        nodes.add(nodeName);

        results.put(nodeName,
            dManager.sendRequest(getName(), involvedClusters, nodes, task, EXECUTION_MODE.RESPONSE, null, null).getPayload());
      }
    }

    if (results.isEmpty())
      throw new ODistributedException("No active nodes found to execute command: " + iCommand);

    return results;
  }

  protected Object mergeResultByAggregation(final OCommandExecutorSQLSelect select, final Map<String, Object> iResults) {
    final List<Object> list = new ArrayList<Object>();
    final ODocument doc = new ODocument();
    list.add(doc);

    boolean hasNonAggregates = false;
    final Map<String, Object> proj = select.getProjections();
    for (Map.Entry<String, Object> p : proj.entrySet()) {
      if (!(p.getValue() instanceof OSQLFunctionRuntime)) {
        hasNonAggregates = true;
        break;
      }
    }

    if (hasNonAggregates) {
      // MERGE NON AGGREGATED FIELDS
      for (Map.Entry<String, Object> entry : iResults.entrySet()) {
        final List<Object> resultSet = (List<Object>) entry.getValue();

        for (Object r : resultSet) {
          if (r instanceof ODocument) {
            final ODocument d = (ODocument) r;

            for (Map.Entry<String, Object> p : proj.entrySet()) {
              // WRITE THE FIELD AS IS
              if (!(p.getValue() instanceof OSQLFunctionRuntime))
                doc.field(p.getKey(), ((ODocument) r).field(p.getKey()));
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
        for (Map.Entry<String, Object> entry : iResults.entrySet()) {
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

    return list;
  }

  protected boolean executeLocally(final String localNodeName, final ODistributedConfiguration dbCfg, final OCommandExecutor exec,
      final Collection<String> involvedClusters, final Collection<String> nodes) {
    boolean executeLocally = false;
    if (exec.isIdempotent()) {
      final int availableNodes = dManager.getAvailableNodes(nodes, getName());

      // IDEMPOTENT: CHECK IF CAN WORK LOCALLY ONLY
      int maxReadQuorum;
      if (involvedClusters.isEmpty())
        maxReadQuorum = dbCfg.getReadQuorum(null, availableNodes);
      else {
        maxReadQuorum = 0;
        for (String cl : involvedClusters)
          maxReadQuorum = Math.max(maxReadQuorum, dbCfg.getReadQuorum(cl, availableNodes));
      }

      if (nodes.size() == 1 && nodes.iterator().next().equals(localNodeName) && maxReadQuorum <= 1)
        executeLocally = true;

    } else if (nodes.size() == 1 && nodes.iterator().next().equals(localNodeName))
      executeLocally = true;

    return executeLocally;
  }

  public OStorageOperationResult<OPhysicalPosition> createRecord(final ORecordId iRecordId, final byte[] iContent,
      final int iRecordVersion, final byte iRecordType, final int iMode, final ORecordCallback<Long> iCallback) {
    resetLastValidBackup();

    if (OScenarioThreadLocal.INSTANCE.get() == RUN_MODE.RUNNING_DISTRIBUTED) {
      // ALREADY DISTRIBUTED
      return wrapped.createRecord(iRecordId, iContent, iRecordVersion, iRecordType, iMode, iCallback);
    }

    try {
      // ASSIGN DESTINATION NODE
      String clusterName = getClusterNameByRID(iRecordId);

      final int clusterId = iRecordId.getClusterId();
      if (clusterId == ORID.CLUSTER_ID_INVALID)
        throw new IllegalArgumentException("Cluster not valid");

      final String localNodeName = dManager.getLocalNodeName();

      final ODistributedConfiguration dbCfg = dManager.getDatabaseConfiguration(getName());

      checkNodeIsMaster(localNodeName, dbCfg);

      final List<String> nodes = dbCfg.getServers(clusterName, null);

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
      if (!masterNode.equals(localNodeName)) {
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

          OLogManager.instance().warn(this, "Local node '" + localNodeName + "' is not the master for cluster '" + clusterName
              + "' (it is '" + masterNode + "'). Reloading distributed configuration for database '" + getName() + "'");

          ((OLocalClusterStrategy) clSel).readConfiguration();

          newClusterName = getPhysicalClusterNameById(clSel.getCluster(cls, null));
          nodes.clear();
          nodes.addAll(dbCfg.getServers(newClusterName, null));
          masterNode = nodes.get(0);
        }

        if (!masterNode.equals(localNodeName))
          throw new ODistributedException("Error on inserting into cluster '" + clusterName + "' where local node '" + localNodeName
              + "' is not the master of it, but it is '" + masterNode + "'");

        OLogManager.instance().warn(this, "Local node '" + localNodeName + "' is not the master for cluster '" + clusterName
            + "' (it is '" + masterNode + "'). Switching to a valid cluster of the same class: '" + newClusterName + "'");

        // OVERWRITE CLUSTER
        clusterName = newClusterName;
        iRecordId.clusterId = db.getClusterIdByName(newClusterName);
      }

      final String finalClusterName = clusterName;

      // REMOVE CURRENT NODE BECAUSE IT HAS BEEN ALREADY EXECUTED LOCALLY
      nodes.remove(localNodeName);

      // FILTER ONLY AVAILABLE NODES
      dManager.getAvailableNodes(nodes, getName());

      Boolean executionModeSynch = dbCfg.isExecutionModeSynchronous(finalClusterName);
      if (executionModeSynch == null)
        executionModeSynch = iMode == 0;
      final boolean syncMode = executionModeSynch;

      // IN ANY CASE EXECUTE LOCALLY AND THEN DISTRIBUTE
      return (OStorageOperationResult<OPhysicalPosition>) executeRecordOperationInLock(syncMode, clusterId,
          new OCallable<Object, OCallable<Void, ODistributedRequestId>>() {

            @Override
            public Object call(OCallable<Void, ODistributedRequestId> unlockCallback) {
              final ODistributedDatabase ddb = acquireRecordLock(iRecordId);

              final OStorageOperationResult<OPhysicalPosition> localResult;
              try {
                localResult = (OStorageOperationResult<OPhysicalPosition>) ODistributedAbstractPlugin
                    .runInDistributedMode(new Callable() {

                  @Override
                  public Object call() throws Exception {
                    // USE THE DATABASE TO CALL HOOKS
                    final ODatabaseDocumentInternal db = ODatabaseRecordThreadLocal.INSTANCE.get();

                    final ORecord record = Orient.instance().getRecordFactoryManager().newInstance(iRecordType);
                    ORecordInternal.fill(record, iRecordId, iRecordVersion, iContent, true);
                    db.save(record);

                    final OPhysicalPosition ppos = new OPhysicalPosition(iRecordType);
                    ppos.clusterPosition = record.getIdentity().getClusterPosition();
                    ppos.recordVersion = record.getVersion();
                    return new OStorageOperationResult<OPhysicalPosition>(ppos);
                  }
                });
              } catch (RuntimeException e) {
                throw e;
              } catch (Exception e) {
                OException.wrapException(new ODistributedException("Cannot delete record " + iRecordId), e);
                // UNREACHABLE
                return null;

              } finally {
                // RELEASE LOCK
                ddb.unlockRecord(iRecordId);
              }

              // UPDATE RID WITH NEW POSITION
              iRecordId.clusterPosition = localResult.getResult().clusterPosition;

              final OPlaceholder localPlaceholder = new OPlaceholder(iRecordId, localResult.getResult().recordVersion);

              final OCreateRecordTask task = new OCreateRecordTask(iRecordId, iContent, iRecordVersion, iRecordType);

              if (!nodes.isEmpty())

          {
                if (syncMode) {

                  // SYNCHRONOUS CALL: REPLICATE IT
                  final ODistributedResponse dResponse = dManager.sendRequest(getName(), Collections.singleton(finalClusterName),
                      nodes, task, EXECUTION_MODE.RESPONSE, localPlaceholder, unlockCallback);

                  final Object payload = dResponse.getPayload();
                  if (payload != null) {
                    if (payload instanceof Exception) {
                      executeUndoOnLocalServer(dResponse.getRequestId(), task);
                      if (payload instanceof ONeedRetryException)
                        throw (ONeedRetryException) payload;

                      throw OException.wrapException(new ODistributedException("Error on execution distributed CREATE_RECORD"),
                          (Exception) payload);
                    }
                    // COPY THE CLUSTER POS -> RID
                    final OPlaceholder masterPlaceholder = (OPlaceholder) payload;
                    iRecordId.copyFrom(masterPlaceholder.getIdentity());

                    return new OStorageOperationResult<OPhysicalPosition>(new OPhysicalPosition(
                        masterPlaceholder.getIdentity().getClusterPosition(), masterPlaceholder.getVersion()));
                  }

                } else {
                  // ASYNCHRONOUSLY REPLICATE IT TO ALL THE OTHER NODES
                  asynchronousExecution(new OAsynchDistributedOperation(getName(), Collections.singleton(finalClusterName), nodes,
                      task, null, localPlaceholder, unlockCallback));
                }
              }
              return localResult;
            }
          });

    } catch (

    ONeedRetryException e)

    {
      // PASS THROUGH
      throw e;
    } catch (

    Exception e)

    {
      handleDistributedException("Cannot route CREATE_RECORD operation for %s to the distributed node", e, iRecordId);
      // UNREACHABLE
      return null;
    }

  }

  public OStorageOperationResult<ORawBuffer> readRecord(final ORecordId iRecordId, final String iFetchPlan,
      final boolean iIgnoreCache, final ORecordCallback<ORawBuffer> iCallback) {

    if (deletedRecords.get(iRecordId) != null)
      // DELETED
      throw new ORecordNotFoundException(iRecordId);

    try {
      final String clusterName = getClusterNameByRID(iRecordId);

      final ODistributedConfiguration dbCfg = dManager.getDatabaseConfiguration(getName());
      final List<String> nodes = dbCfg.getServers(clusterName, null);

      final int availableNodes = dManager.getAvailableNodes(nodes, getName());

      // CHECK IF LOCAL NODE OWNS THE DATA AND READ-QUORUM = 1: GET IT LOCALLY BECAUSE IT'S FASTER
      if (nodes.isEmpty() || nodes.contains(dManager.getLocalNodeName()) && dbCfg.getReadQuorum(clusterName, availableNodes) <= 1) {
        // DON'T REPLICATE
        return (OStorageOperationResult<ORawBuffer>) ODistributedAbstractPlugin.runInDistributedMode(new Callable() {
          @Override
          public Object call() throws Exception {
            return wrapped.readRecord(iRecordId, iFetchPlan, iIgnoreCache, iCallback);
          }
        });
      }

      // DISTRIBUTE IT
      final Object dResult = dManager.sendRequest(getName(), Collections.singleton(clusterName), nodes,
          new OReadRecordTask(iRecordId), EXECUTION_MODE.RESPONSE, null, null).getPayload();

      if (dResult instanceof ONeedRetryException)
        throw (ONeedRetryException) dResult;
      else if (dResult instanceof Exception)
        throw OException.wrapException(new ODistributedException("Error on execution distributed READ_RECORD"),
            (Exception) dResult);

      return new OStorageOperationResult<ORawBuffer>((ORawBuffer) dResult);

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
  public OStorageOperationResult<ORawBuffer> readRecordIfVersionIsNotLatest(final ORecordId rid, final String fetchPlan,
      final boolean ignoreCache, final int recordVersion) throws ORecordNotFoundException {

    if (deletedRecords.get(rid) != null)
      // DELETED
      throw new ORecordNotFoundException(rid);

    try {
      final String clusterName = getClusterNameByRID(rid);

      final ODistributedConfiguration dbCfg = dManager.getDatabaseConfiguration(getName());
      final List<String> nodes = dbCfg.getServers(clusterName, null);

      final int availableNodes = dManager.getAvailableNodes(nodes, getName());

      // CHECK IF LOCAL NODE OWNS THE DATA AND READ-QUORUM = 1: GET IT LOCALLY BECAUSE IT'S FASTER
      if (nodes.isEmpty() || nodes.contains(dManager.getLocalNodeName()) && dbCfg.getReadQuorum(clusterName, availableNodes) <= 1) {
        // DON'T REPLICATE
        return (OStorageOperationResult<ORawBuffer>) ODistributedAbstractPlugin.runInDistributedMode(new Callable() {
          @Override
          public Object call() throws Exception {
            return wrapped.readRecordIfVersionIsNotLatest(rid, fetchPlan, ignoreCache, recordVersion);
          }
        });
      }

      // DISTRIBUTE IT
      final Object result = dManager.sendRequest(getName(), Collections.singleton(clusterName), nodes,
          new OReadRecordIfNotLatestTask(rid, recordVersion), EXECUTION_MODE.RESPONSE, null, null).getPayload();

      if (result instanceof ONeedRetryException)
        throw (ONeedRetryException) result;
      else if (result instanceof Exception)
        throw OException.wrapException(new ODistributedException("Error on execution distributed READ_RECORD"), (Exception) result);

      return new OStorageOperationResult<ORawBuffer>((ORawBuffer) result);

    } catch (ONeedRetryException e) {
      // PASS THROUGH
      throw e;
    } catch (Exception e) {
      handleDistributedException("Cannot route READ_RECORD operation for %s to the distributed node", e, rid);
      // UNREACHABLE
      return null;
    }
  }

  @Override
  public OSBTreeCollectionManager getSBtreeCollectionManager() {
    return wrapped.getSBtreeCollectionManager();
  }

  @Override
  public OStorageOperationResult<Integer> updateRecord(final ORecordId iRecordId, final boolean updateContent,
      final byte[] iContent, final int iVersion, final byte iRecordType, final int iMode,
      final ORecordCallback<Integer> iCallback) {
    resetLastValidBackup();

    if (deletedRecords.get(iRecordId) != null)
      // DELETED
      throw new ORecordNotFoundException(iRecordId);

    if (OScenarioThreadLocal.INSTANCE.get() == RUN_MODE.RUNNING_DISTRIBUTED) {
      // ALREADY DISTRIBUTED
      return wrapped.updateRecord(iRecordId, updateContent, iContent, iVersion, iRecordType, iMode, iCallback);
    }

    try {
      final String clusterName = getClusterNameByRID(iRecordId);

      final ODistributedConfiguration dbCfg = dManager.getDatabaseConfiguration(getName());

      final String localNodeName = dManager.getLocalNodeName();

      checkNodeIsMaster(localNodeName, dbCfg);

      final List<String> nodes = dbCfg.getServers(clusterName, null);

      if (nodes.isEmpty()) {
        // DON'T REPLICATE OR DISTRIBUTE
        return (OStorageOperationResult<Integer>) ODistributedAbstractPlugin.runInDistributedMode(new Callable() {
          @Override
          public Object call() throws Exception {
            return wrapped.updateRecord(iRecordId, updateContent, iContent, iVersion, iRecordType, iMode, iCallback);
          }
        });
      }

      nodes.remove(localNodeName);

      // FILTER ONLY AVAILABLE NODES
      dManager.getAvailableNodes(nodes, getName());

      final int clusterId = iRecordId.clusterId;

      Boolean executionModeSynch = dbCfg.isExecutionModeSynchronous(clusterName);
      if (executionModeSynch == null)
        executionModeSynch = iMode == 0;
      final boolean syncMode = executionModeSynch;

      return (OStorageOperationResult<Integer>) executeRecordOperationInLock(syncMode, clusterId,
          new OCallable<Object, OCallable<Void, ODistributedRequestId>>() {

            @Override
            public Object call(OCallable<Void, ODistributedRequestId> unlockCallback) {
              final OUpdateRecordTask task = new OUpdateRecordTask(iRecordId, iContent, iVersion, iRecordType);

              final OStorageOperationResult<Integer> localResult;
              try {
                localResult = (OStorageOperationResult<Integer>) ODistributedAbstractPlugin.runInDistributedMode(new Callable() {

                  @Override
                  public Object call() throws Exception {
                    // USE THE DATABASE TO CALL HOOKS
                    final ODatabaseDocumentInternal db = ODatabaseRecordThreadLocal.INSTANCE.get();

                    final ORecord record = Orient.instance().getRecordFactoryManager().newInstance(iRecordType);
                    ORecordInternal.fill(record, iRecordId, iVersion, iContent, true);
                    db.save(record);

                    return new OStorageOperationResult<Integer>(record.getVersion());
                  }
                });
              } catch (RuntimeException e) {
                throw e;
              } catch (Exception e) {
                OException.wrapException(new ODistributedException("Cannot delete record " + iRecordId), e);
                // UNREACHABLE
                return null;
              }

              if (!nodes.isEmpty()) {
                if (syncMode) {
                  // REPLICATE IT
                  task.prepareUndoOperation();
                  final ODistributedResponse dResponse = dManager.sendRequest(getName(), Collections.singleton(clusterName), nodes,
                      task, EXECUTION_MODE.RESPONSE, localResult.getResult(), unlockCallback);

                  final Object payload = dResponse.getPayload();

                  if (payload instanceof Exception) {
                    executeUndoOnLocalServer(dResponse.getRequestId(), task);

                    if (payload instanceof ONeedRetryException)
                      throw (ONeedRetryException) payload;

                    throw OException.wrapException(new ODistributedException("Error on execution distributed UPDATE_RECORD"),
                        (Exception) payload);
                  }

                  // UPDATE LOCALLY
                  return new OStorageOperationResult<Integer>((Integer) payload);
                }

                // ASYNCHRONOUS CALL: EXECUTE LOCALLY AND THEN DISTRIBUTE
                asynchronousExecution(new OAsynchDistributedOperation(getName(), Collections.singleton(clusterName), nodes, task,
                    null, localResult.getResult(), unlockCallback));
              }
              return localResult;
            }
          });

    } catch (

    ONeedRetryException e) {
      // PASS THROUGH
      throw e;
    } catch (Exception e) {

      handleDistributedException("Cannot route UPDATE_RECORD operation for %s to the distributed node", e, iRecordId);
      // UNREACHABLE
      return null;
    }

  }

  @Override
  public OStorageOperationResult<Boolean> deleteRecord(final ORecordId iRecordId, final int iVersion, final int iMode,
      final ORecordCallback<Boolean> iCallback) {
    resetLastValidBackup();

    if (OScenarioThreadLocal.INSTANCE.get() == RUN_MODE.RUNNING_DISTRIBUTED) {
      // ALREADY DISTRIBUTED
      return wrapped.deleteRecord(iRecordId, iVersion, iMode, iCallback);
    }

    try {
      final String clusterName = getClusterNameByRID(iRecordId);

      final ODistributedConfiguration dbCfg = dManager.getDatabaseConfiguration(getName());

      final String localNodeName = dManager.getLocalNodeName();

      checkNodeIsMaster(localNodeName, dbCfg);

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

      nodes.remove(localNodeName);

      // FILTER ONLY AVAILABLE NODES
      dManager.getAvailableNodes(nodes, getName());

      final int clusterId = iRecordId.clusterId;

      Boolean executionModeSynch = dbCfg.isExecutionModeSynchronous(clusterName);
      if (executionModeSynch == null)
        executionModeSynch = iMode == 0;
      final boolean syncMode = executionModeSynch;

      return (OStorageOperationResult<Boolean>) executeRecordOperationInLock(syncMode, clusterId,
          new OCallable<Object, OCallable<Void, ODistributedRequestId>>() {

            @Override
            public Object call(OCallable<Void, ODistributedRequestId> unlockCallback) {
              final ODeleteRecordTask task = new ODeleteRecordTask(iRecordId, iVersion);
              task.prepareUndoOperation();

              final OStorageOperationResult<Boolean> localResult;
              try {
                localResult = (OStorageOperationResult<Boolean>) ODistributedAbstractPlugin.runInDistributedMode(new Callable() {

                  @Override
                  public Object call() throws Exception {
                    // USE THE DATABASE TO CALL HOOKS
                    final ODatabaseDocumentInternal db = ODatabaseRecordThreadLocal.INSTANCE.get();
                    try {
                      db.delete(iRecordId, iVersion);
                      return new OStorageOperationResult<Boolean>(true);
                    } catch (ORecordNotFoundException e) {
                      return new OStorageOperationResult<Boolean>(false);
                    }
                    // return wrapped.deleteRecord(iRecordId, iVersion, 0, null).getResult();
                  }
                });
              } catch (RuntimeException e) {
                throw e;
              } catch (Exception e) {
                OException.wrapException(new ODistributedException("Cannot delete record " + iRecordId), e);
                // UNREACHABLE
                return null;
              }

              if (!nodes.isEmpty()) {
                if (syncMode) {
                  // REPLICATE IT
                  final ODistributedResponse dResponse = dManager.sendRequest(getName(), Collections.singleton(clusterName), nodes,
                      task, EXECUTION_MODE.RESPONSE, localResult.getResult(), unlockCallback);

                  final Object payload = dResponse.getPayload();

                  if (payload instanceof Exception) {
                    executeUndoOnLocalServer(dResponse.getRequestId(), task);

                    if (payload instanceof ONeedRetryException)
                      throw (ONeedRetryException) payload;

                    throw OException.wrapException(new ODistributedException("Error on execution distributed DELETE_RECORD"),
                        (Exception) payload);
                  }

                  return new OStorageOperationResult<Boolean>(true);
                }

                // ASYNCHRONOUS CALL: EXECUTE LOCALLY AND THEN DISTRIBUTE
                if (!nodes.isEmpty())
                  asynchronousExecution(new OAsynchDistributedOperation(getName(), Collections.singleton(clusterName), nodes, task,
                      null, localResult.getResult(), unlockCallback));
              }
              return localResult;
            }
          });

    } catch (

    ONeedRetryException e) {
      // PASS THROUGH
      throw e;
    } catch (Exception e) {

      handleDistributedException("Cannot route DELETE_RECORD operation for %s to the distributed node", e, iRecordId);
      // UNREACHABLE
      return null;
    }

  }

  private Object executeRecordOperationInLock(final boolean iUnlockAtTheEnd, final int clusterId,
      final OCallable<Object, OCallable<Void, ODistributedRequestId>> callback) throws InterruptedException {
    messageManagementLock.readLock().lock();
    try {

      final int partition = clusterId % clusterLocks.length;
      clusterLocks[partition].acquire();
      final AtomicBoolean acquiredClusterLock = new AtomicBoolean(true);

      try {
        final OCallable<Void, ODistributedRequestId> unlockCallback = new OCallable<Void, ODistributedRequestId>() {
          @Override
          public Void call(final ODistributedRequestId req) {
            // UNLOCK AS SOON AS THE REQUEST IS SENT
            if (acquiredClusterLock.compareAndSet(true, false))
              clusterLocks[partition].release();
            return null;
          }
        };

        return callback.call(unlockCallback);

      } finally {
        if (iUnlockAtTheEnd)
          if (acquiredClusterLock.compareAndSet(true, false))
            clusterLocks[partition].release();
      }

    } finally {
      messageManagementLock.readLock().unlock();
    }
  }

  private Object executeOperationInLock(final boolean iExclusiveLock,
      final OCallable<Object, OCallable<Void, ODistributedRequestId>> callback) {
    if (iExclusiveLock)
      messageManagementLock.writeLock().lock();
    else
      messageManagementLock.readLock().lock();

    try {
      final OCallable<Void, ODistributedRequestId> unlockCallback = new OCallable<Void, ODistributedRequestId>() {
        @Override
        public Void call(final ODistributedRequestId req) {
          // UNLOCK AS SOON AS THE REQUEST IS SENT
          return null;
        }
      };

      return callback.call(unlockCallback);
    } finally {
      if (iExclusiveLock)
        messageManagementLock.writeLock().unlock();
      else
        messageManagementLock.readLock().unlock();
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
  public boolean cleanOutRecord(ORecordId recordId, final int recordVersion, int iMode, ORecordCallback<Boolean> callback) {
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
  public void incrementalBackup(final String backupDirectory) {
    wrapped.incrementalBackup(backupDirectory);
  }

  @Override
  public void restoreFromIncrementalBackup(final String filePath) {
    wrapped.restoreFromIncrementalBackup(filePath);
  }

  @Override
  public void close() {
    close(false, false);
  }

  @Override
  public void close(final boolean iForce, final boolean onDelete) {
    if (onDelete && wrapped instanceof OLocalPaginatedStorage) {
      // REMOVE distributed-config.json FILE to allow removal of directory
      final File dCfg = new File(
          ((OLocalPaginatedStorage) wrapped).getStoragePath() + "/" + getDistributedManager().FILE_DISTRIBUTED_DB_CONFIG);
      if (dCfg.exists())
        dCfg.delete();
    }

    wrapped.close(iForce, onDelete);
    if (isClosed())
      shutdownAsynchronousWorker();
  }

  @Override
  public boolean isClosed() {
    return wrapped.isClosed();
  }

  @Override
  public List<ORecordOperation> commit(final OTransaction iTx, final Runnable callback) {
    resetLastValidBackup();

    if (OScenarioThreadLocal.INSTANCE.get() == RUN_MODE.RUNNING_DISTRIBUTED) {
      // ALREADY DISTRIBUTED
      wrapped.commit(iTx, callback);
      return null;
    }

    final ODistributedConfiguration dbCfg = dManager.getDatabaseConfiguration(getName());

    final String localNodeName = dManager.getLocalNodeName();

    checkNodeIsMaster(localNodeName, dbCfg);

    try {
      if (!dbCfg.isReplicationActive(null, localNodeName)) {
        // DON'T REPLICATE
        ODistributedAbstractPlugin.runInDistributedMode(new Callable() {
          @Override
          public Object call() throws Exception {
            wrapped.commit(iTx, callback);
            return null;
          }
        });

        return null;
      }

      OTransactionInternal.setStatus((OTransactionAbstract) iTx, OTransaction.TXSTATUS.BEGUN);

      final OTxTask txTask = new OTxTask();
      final Set<String> involvedClusters = new HashSet<String>();

      // CREATE UNDO CONTENT FOR DISTRIBUTED 2-PHASE ROLLBACK
      final List<OAbstractRemoteTask> undoTasks = createUndoTasksFromTx(iTx);

      final int maxAutoRetry = OGlobalConfiguration.DISTRIBUTED_CONCURRENT_TX_MAX_AUTORETRY.getValueAsInteger();
      final int autoRetryDelay = OGlobalConfiguration.DISTRIBUTED_CONCURRENT_TX_AUTORETRY_DELAY.getValueAsInteger();

      Boolean executionModeSynch = dbCfg.isExecutionModeSynchronous(null);
      if (executionModeSynch == null)
        executionModeSynch = Boolean.TRUE;

      final boolean finalExecutionModeSynch = executionModeSynch;

      // SYNCHRONIZE THE ENTIRE BLOCK TO BE LINEARIZABLE WITH CREATE OPERATION
      final boolean exclusiveLock = iTx.hasRecordCreation();

      return (List<ORecordOperation>) executeOperationInLock(exclusiveLock,
          new OCallable<Object, OCallable<Void, ODistributedRequestId>>() {

            @Override
            public Object call(final OCallable<Void, ODistributedRequestId> unlockCallback) {
              try {
                final ODistributedDatabase ddb = acquireMultipleRecordLocks(iTx, maxAutoRetry, autoRetryDelay);

                try {
                  final List<ORecordOperation> uResult = (List<ORecordOperation>) ODistributedAbstractPlugin
                      .runInDistributedMode(new Callable() {
                    @Override
                    public Object call() throws Exception {
                      return wrapped.commit(iTx, callback);
                    }
                  });

                  // After commit force the clean of dirty managers due to possible copy and miss clean.
                  for (ORecordOperation ent : iTx.getAllRecordEntries()) {
                    ORecordInternal.getDirtyManager(ent.getRecord()).clear();
                  }

                  final OTxTaskResult localResult = new OTxTaskResult();
                  for (ORecordOperation op : uResult) {
                    final OAbstractRecordReplicatedTask task;

                    final ORecord record = op.getRecord();

                    switch (op.type) {
                    case ORecordOperation.CREATED:
                      task = new OCreateRecordTask(record);
                      localResult.results.add(new OPlaceholder((ORecordId) record.getIdentity(), record.getVersion()));
                      // ADD UNDO TASK ONCE YHE RID OS KNOWN
                      undoTasks.add(new ODeleteRecordTask(record));
                      break;

                    case ORecordOperation.UPDATED:
                      task = new OUpdateRecordTask(record);
                      localResult.results.add(record.getVersion());
                      break;

                    case ORecordOperation.DELETED:
                      task = new ODeleteRecordTask(record);
                      localResult.results.add(Boolean.TRUE);
                      break;

                    default:
                      continue;
                    }

                    involvedClusters.add(getClusterNameByRID((ORecordId) record.getIdentity()));
                    txTask.add(task);
                  }

                  txTask.setUndoTasks(undoTasks);

                  OTransactionInternal.setStatus((OTransactionAbstract) iTx, OTransaction.TXSTATUS.COMMITTING);

                  final Set<String> nodes = dbCfg.getServers(involvedClusters);

                  // REMOVE CURRENT NODE BECAUSE IT HAS BEEN ALREADY EXECUTED LOCALLY
                  nodes.remove(localNodeName);

                  // FILTER ONLY AVAILABLE NODES
                  dManager.getAvailableNodes(nodes, getName());

                  if (nodes.isEmpty())
                    // NO FURTHER NODES TO INVOLVE
                    return null;

                  if (finalExecutionModeSynch) {
                    // SYNCHRONOUS

                    // AUTO-RETRY IN CASE RECORDS ARE LOCKED
                    ODistributedResponse lastResult = null;
                    for (int retry = 1; retry <= maxAutoRetry; ++retry) {
                      // SYNCHRONOUS CALL: REPLICATE IT
                      lastResult = dManager.sendRequest(getName(), involvedClusters, nodes, txTask, EXECUTION_MODE.RESPONSE,
                          localResult, unlockCallback);
                      if (!processCommitResult(localNodeName, iTx, txTask, involvedClusters, uResult, nodes, autoRetryDelay,
                          lastResult.getRequestId(), lastResult)) {
                        // RETRY
                        continue;
                      }

                      return null;
                    }

                    if (ODistributedServerLog.isDebugEnabled())
                      ODistributedServerLog.debug(this, localNodeName, null, ODistributedServerLog.DIRECTION.NONE,
                          "Distributed transaction retries exceed maximum auto-retries (%d)", maxAutoRetry);

                    // ONLY CASE: ODistributedRecordLockedException MORE THAN AUTO-RETRY
                    throw (ODistributedRecordLockedException) lastResult.getPayload();
                  }

                  // ASYNC
                  if (!nodes.isEmpty()) {
                    // MANAGE REPLICATION CALLBACK
                    final OAsyncReplicationOk onAsyncReplicationOk = OExecutionThreadLocal.INSTANCE.get().onAsyncReplicationOk;
                    final OAsyncReplicationError onAsyncReplicationError = getAsyncReplicationError();

                    // ASYNCHRONOUSLY REPLICATE IT TO ALL THE OTHER NODES
                    asynchronousExecution(new OAsynchDistributedOperation(getName(), involvedClusters, nodes, txTask,
                        new OCallable<Object, OPair<ODistributedRequestId, Object>>() {
                      @Override
                      public Object call(final OPair<ODistributedRequestId, Object> iArgument) {
                        try {
                          final Object value = iArgument.getValue();

                          final ODistributedRequestId reqId = iArgument.getKey();

                          if (value instanceof OTxTaskResult) {
                            // SEND 2-PHASE DISTRIBUTED COMMIT TX
                            sendTxCompleted(localNodeName, involvedClusters, nodes, reqId, true);

                            if (onAsyncReplicationOk != null)
                              onAsyncReplicationOk.onAsyncReplicationOk();

                            return null;
                          } else if (value instanceof Exception) {
                            try {
                              executeUndoOnLocalServer(reqId, txTask);

                              if (ODistributedServerLog.isDebugEnabled())
                                ODistributedServerLog.debug(this, localNodeName, null, ODistributedServerLog.DIRECTION.NONE,
                                    "Async distributed transaction failed: %s", value);

                              // SEND 2-PHASE DISTRIBUTED ROLLBACK TX
                              sendTxCompleted(localNodeName, involvedClusters, nodes, reqId, false);

                              if (value instanceof RuntimeException)
                                throw (RuntimeException) value;
                              else
                                throw OException.wrapException(
                                    new OTransactionException("Error on execution async distributed transaction"),
                                    (Exception) value);

                            } finally {
                              if (onAsyncReplicationError != null)
                                onAsyncReplicationError.onAsyncReplicationError((Throwable) value, 0);
                            }
                          }

                          // UNKNOWN RESPONSE TYPE
                          if (ODistributedServerLog.isDebugEnabled())
                            ODistributedServerLog.debug(this, localNodeName, null, ODistributedServerLog.DIRECTION.NONE,
                                "Async distributed transaction error, received unknown response type: %s", iArgument);

                          throw new OTransactionException(
                              "Error on committing async distributed transaction, received unknown response type " + iArgument);

                        } finally {
                          try {
                            unlockCallback.call(iArgument.getKey());
                          } catch (Exception e) {
                            ODistributedServerLog.debug(this, localNodeName, null, ODistributedServerLog.DIRECTION.NONE,
                                "Error on unlocking Async distributed transaction", e);
                          }
                        }
                      }
                    }, localResult, unlockCallback));
                  }

                } finally {
                  // UNLOCK ALL THE RECORDS
                  for (ORecordOperation op : iTx.getAllRecordEntries()) {
                    ddb.unlockRecord(op.record);
                  }
                }
              } catch (RuntimeException e) {
                throw e;
              } catch (Exception e) {
                OException.wrapException(new ODistributedException("Cannot commit transaction"), e);
                // UNREACHABLE
              }
              return null;
            }
          });

    } catch (

    OValidationException e) {
      throw e;
    } catch (Exception e) {

      handleDistributedException("Cannot route TX operation against distributed node", e);
    }

    return null;

  }

  protected ODistributedDatabase acquireMultipleRecordLocks(final OTransaction iTx, final int maxAutoRetry,
      final int autoRetryDelay) throws InterruptedException {
    // ACQUIRE ALL THE LOCKS ON RECORDS ON LOCAL NODE BEFORE TO PROCEED
    final ODistributedDatabase ddb = dManager.getMessageService().getDatabase(getName());
    final ODistributedRequestId localReqId = new ODistributedRequestId(dManager.getLocalNodeId(),
        dManager.getNextMessageIdCounter());

    for (ORecordOperation op : iTx.getAllRecordEntries()) {
      boolean recordLocked = false;
      for (int retry = 1; retry <= maxAutoRetry; ++retry) {
        if (ddb.lockRecord(op.record, localReqId)) {
          recordLocked = true;
          break;
        }

        if (autoRetryDelay > -1)
          Thread.sleep(autoRetryDelay);
      }

      if (!recordLocked)
        throw new ODistributedRecordLockedException(op.record.getIdentity());
    }
    return ddb;
  }

  protected ODistributedDatabase acquireRecordLock(final ORecordId rid) {
    // ACQUIRE ALL THE LOCKS ON RECORDS ON LOCAL NODE BEFORE TO PROCEED
    final ODistributedDatabase ddb = dManager.getMessageService().getDatabase(getName());
    final ODistributedRequestId localReqId = new ODistributedRequestId(dManager.getLocalNodeId(),
        dManager.getNextMessageIdCounter());

    if (!(ddb.lockRecord(rid, localReqId)))
      throw new ODistributedRecordLockedException(rid);

    return ddb;
  }

  /**
   * Create undo content for distributed 2-phase rollback.
   * 
   * @param iTx
   *          Current transaction
   * @return List of remote undo tasks
   */
  protected List<OAbstractRemoteTask> createUndoTasksFromTx(final OTransaction iTx) {
    final List<OAbstractRemoteTask> undoTasks = new ArrayList<OAbstractRemoteTask>();
    for (ORecordOperation op : iTx.getAllRecordEntries()) {
      OAbstractRemoteTask undoTask = null;

      final ORecord record = op.getRecord();

      switch (op.type) {
      case ORecordOperation.CREATED:
        // CREATE UNDO TASK LATER ONCE THE RID HAS BEEN ASSIGNED
        break;

      case ORecordOperation.UPDATED: {
        // CREATE UNDO TASK WITH THE PREVIOUS RECORD CONTENT/VERSION
        final ORecord currentRecord = record.getIdentity().getRecord();
        if (currentRecord != null)
          undoTask = new OUpdateRecordTask(currentRecord, ORecordVersionHelper.clearRollbackMode(currentRecord.getVersion()));
        break;
      }

      case ORecordOperation.DELETED: {
        final ORecord currentRecord = record.getIdentity().getRecord();
        if (currentRecord != null)
          undoTask = new OResurrectRecordTask(currentRecord, ORecordVersionHelper.clearRollbackMode(currentRecord.getVersion()));
        break;
      }

      default:
        continue;
      }

      if (undoTask != null)
        undoTasks.add(undoTask);
    }
    return undoTasks;
  }

  protected boolean processCommitResult(final String localNodeName, final OTransaction iTx, final OTxTask txTask,
      final Set<String> involvedClusters, final Iterable<ORecordOperation> tmpEntries, final Collection<String> nodes,
      final int autoRetryDelay, final ODistributedRequestId reqId, final ODistributedResponse dResponse)
          throws InterruptedException {
    final Object result = dResponse.getPayload();

    if (result instanceof OTxTaskResult) {
      final OTxTaskResult txResult = ((OTxTaskResult) result);

      final List<Object> list = txResult.results;

      for (int i = 0; i < txTask.getTasks().size(); ++i) {
        final Object o = list.get(i);

        final OAbstractRecordReplicatedTask task = txTask.getTasks().get(i);

        if (task instanceof OCreateRecordTask) {
          final OCreateRecordTask t = (OCreateRecordTask) task;
          iTx.updateIdentityAfterCommit(t.getRid(), ((OPlaceholder) o).getIdentity());
          ORecordInternal.setVersion(iTx.getRecord(t.getRid()), ((OPlaceholder) o).getVersion());
        } else if (task instanceof OUpdateRecordTask) {
          final OUpdateRecordTask t = (OUpdateRecordTask) task;
          ORecordInternal.setVersion(iTx.getRecord(t.getRid()), (Integer) o);
        } else if (task instanceof ODeleteRecordTask) {

        }

      }

      // RESET DIRTY FLAGS TO AVOID CALLING AUTO-SAVE
      for (ORecordOperation op : tmpEntries) {
        final ORecord record = op.getRecord();
        if (record != null)
          ORecordInternal.unsetDirty(record);
      }

      sendTxCompleted(localNodeName, involvedClusters, nodes, reqId, true);

    } else if (result instanceof ODistributedRecordLockedException) {
      // AUTO RETRY
      if (autoRetryDelay > 0)
        Thread.sleep(autoRetryDelay);
      return false;

    } else if (result instanceof Exception) {
      // EXCEPTION: LOG IT AND ADD AS NESTED EXCEPTION
      if (ODistributedServerLog.isDebugEnabled())
        ODistributedServerLog.debug(this, localNodeName, null, ODistributedServerLog.DIRECTION.NONE,
            "distributed transaction error: %s", result, result.toString());

      executeUndoOnLocalServer(dResponse.getRequestId(), txTask);

      if (result instanceof OTransactionException || result instanceof ONeedRetryException)
        throw (RuntimeException) result;

      throw OException.wrapException(new OTransactionException("Error on committing distributed transaction"), (Exception) result);

    } else {
      // UNKNOWN RESPONSE TYPE
      if (ODistributedServerLog.isDebugEnabled())
        ODistributedServerLog.debug(this, localNodeName, null, ODistributedServerLog.DIRECTION.NONE,
            "distributed transaction error, received unknown response type: %s", result);

      throw new OTransactionException("Error on committing distributed transaction, received unknown response type " + result);
    }
    return true;
  }

  private void sendTxCompleted(final String localNodeName, final Set<String> involvedClusters, final Collection<String> nodes,
      final ODistributedRequestId reqId, final boolean success) {
    // SEND FINAL TX COMPLETE TASK TO UNLOCK RECORDS
    final Object completedResult = dManager
        .sendRequest(getName(), involvedClusters, nodes, new OCompletedTxTask(reqId, success), EXECUTION_MODE.RESPONSE, null, null)
        .getPayload();

    if (!(completedResult instanceof Boolean) || !((Boolean) completedResult).booleanValue()) {
      // EXCEPTION: LOG IT AND ADD AS NESTED EXCEPTION
      ODistributedServerLog.error(this, localNodeName, null, ODistributedServerLog.DIRECTION.NONE,
          "distributed transaction complete error: %s", completedResult);
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

        final StringBuilder cmd = new StringBuilder("create cluster `");
        cmd.append(iClusterName);
        cmd.append("`");

        // EXECUTE THIS OUTSIDE LOCK TO AVOID DEADLOCKS
        OCommandSQL commandSQL = new OCommandSQL(cmd.toString());
        commandSQL.addExcludedNode(getNodeId());

        final Object result = command(commandSQL);
        if (result != null && ((Integer) result).intValue() != clId) {
          ODistributedServerLog.warn(this, dManager.getLocalNodeName(), null, ODistributedServerLog.DIRECTION.NONE,
              "Error on creating cluster on distributed nodes: ids are different (local=%d and remote=%d). Retrying %d/%d...", clId,
              ((Integer) result).intValue(), retry, 10);

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

          wrapped.reload(); // TODO: RELOAD DOESN'T DO ANYTHING WHILE HERE IT'S NEEDED A WAY TO CLOSE/OPEN THE DB
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
    // TODO: SUPPORT SHARDING HERE
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

  public OServer getServer() {
    return serverInstance;
  }

  public ODistributedServerManager getDistributedManager() {
    return dManager;
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

  public OStorage getUnderlying() {
    return wrapped;
  }

  @Override
  public boolean isRemote() {
    return false;
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
    final String localNode = dManager.getLocalNodeName();

    prevStatus = dManager.getDatabaseStatus(localNode, getName());
    if (prevStatus == ODistributedServerManager.DB_STATUS.ONLINE)
      // SET STATUS = BACKUP
      dManager.setDatabaseStatus(localNode, getName(), ODistributedServerManager.DB_STATUS.BACKUP);

    getFreezableStorage().freeze(throwException);
  }

  @Override
  public void release() {
    final String localNode = dManager.getLocalNodeName();

    if (prevStatus == ODistributedServerManager.DB_STATUS.ONLINE)
      // RESTORE PREVIOUS STATUS
      dManager.setDatabaseStatus(localNode, getName(), ODistributedServerManager.DB_STATUS.ONLINE);

    getFreezableStorage().release();
  }

  @Override
  public List<String> backup(final OutputStream out, final Map<String, Object> options, final Callable<Object> callable,
      final OCommandOutputListener iListener, final int compressionLevel, final int bufferSize) throws IOException {
    final String localNode = dManager.getLocalNodeName();

    final ODistributedServerManager.DB_STATUS prevStatus = dManager.getDatabaseStatus(localNode, getName());
    if (prevStatus == ODistributedServerManager.DB_STATUS.ONLINE)
      // SET STATUS = BACKUP
      dManager.setDatabaseStatus(localNode, getName(), ODistributedServerManager.DB_STATUS.BACKUP);

    try {

      return wrapped.backup(out, options, callable, iListener, compressionLevel, bufferSize);

    } finally {
      if (prevStatus == ODistributedServerManager.DB_STATUS.ONLINE)
        // RESTORE PREVIOUS STATUS
        dManager.setDatabaseStatus(localNode, getName(), ODistributedServerManager.DB_STATUS.ONLINE);
    }
  }

  @Override
  public void restore(final InputStream in, final Map<String, Object> options, final Callable<Object> callable,
      final OCommandOutputListener iListener) throws IOException {
    wrapped.restore(in, options, callable, iListener);
  }

  public void pushDeletedRecord(final ORecordId rid, final int version) {
    resetLastValidBackup();

    deletedRecords.putIfAbsent(rid, new OPair<Long, Integer>(System.currentTimeMillis(), version));
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

  @Override
  public void shutdown() {
    close(true, false);
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

  protected void checkNodeIsMaster(final String localNodeName, final ODistributedConfiguration dbCfg) {
    final ODistributedConfiguration.ROLES nodeRole = dbCfg.getServerRole(localNodeName);
    if (nodeRole != ODistributedConfiguration.ROLES.MASTER)
      throw new ODistributedException("Cannot execute write operation on node '" + localNodeName + "' because is non master");
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

  protected OAsyncReplicationError getAsyncReplicationError() {
    if (OExecutionThreadLocal.INSTANCE.get().onAsyncReplicationError != null) {

      final OAsyncReplicationError subCallback = OExecutionThreadLocal.INSTANCE.get().onAsyncReplicationError;
      final ODatabaseDocumentTx currentDatabase = (ODatabaseDocumentTx) ODatabaseRecordThreadLocal.INSTANCE.get();
      final ODatabaseDocumentTx copyDatabase = currentDatabase.copy();
      currentDatabase.activateOnCurrentThread();

      return new OAsyncReplicationError() {
        @Override
        public ACTION onAsyncReplicationError(final Throwable iException, final int iRetry) {
          copyDatabase.activateOnCurrentThread();
          switch (subCallback.onAsyncReplicationError(iException, iRetry)) {
          case RETRY:
            break;

          case IGNORE:
          }

          return OAsyncReplicationError.ACTION.IGNORE;
        }
      };
    } else
      return null;
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
    throw OException.wrapException(new OStorageException(String.format(iMessage, iParams)), e);
  }

  private OFreezableStorage getFreezableStorage() {
    if (wrapped instanceof OFreezableStorage)
      return ((OFreezableStorage) wrapped);
    else
      throw new UnsupportedOperationException("Storage engine " + wrapped.getType() + " does not support freeze operation");
  }

  private void resetLastValidBackup() {
    if (lastValidBackup != null) {
      lastValidBackup = null;
    }
  }

  private void executeUndoOnLocalServer(final ODistributedRequestId reqId, final OAbstractReplicatedTask task) {
    final ORemoteTask undoTask = task.getUndoTask(reqId);
    if (undoTask != null) {
      ODatabaseDocumentTx database = (ODatabaseDocumentTx) ODatabaseRecordThreadLocal.INSTANCE.getIfDefined();
      final boolean databaseAlreadyDefined;

      if (database == null) {
        databaseAlreadyDefined = false;

        database = new ODatabaseDocumentTx(getURL());
        database.setProperty(ODatabase.OPTIONS.SECURITY.toString(), OSecurityServerUser.class);
        database.open("system", "system");
      } else
        databaseAlreadyDefined = true;

      try {

        undoTask.execute(reqId, dManager.getServerInstance(), dManager, database);

      } catch (Exception e) {
        ODistributedServerLog.error(this, dManager.getLocalNodeName(), null, ODistributedServerLog.DIRECTION.NONE,
            "Error on undo operation on local node (req=%s)", reqId);

      } finally {
        if (!databaseAlreadyDefined)
          database.close();
      }
    }
  }
}
