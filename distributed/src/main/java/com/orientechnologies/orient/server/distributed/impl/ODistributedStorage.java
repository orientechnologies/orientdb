/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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
 *  * For more information: http://orientdb.com
 *
 */
package com.orientechnologies.orient.server.distributed.impl;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.common.concur.ONeedRetryException;
import com.orientechnologies.common.concur.OOfflineNodeException;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.io.OIOException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.common.util.OPair;
import com.orientechnologies.orient.core.command.*;
import com.orientechnologies.orient.core.command.script.OCommandScript;
import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.config.OStorageConfiguration;
import com.orientechnologies.orient.core.conflict.ORecordConflictStrategy;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.OExecutionThreadLocal;
import com.orientechnologies.orient.core.db.OScenarioThreadLocal;
import com.orientechnologies.orient.core.db.OScenarioThreadLocal.RUN_MODE;
import com.orientechnologies.orient.core.db.record.OCurrentStorageComponentsFactory;
import com.orientechnologies.orient.core.db.record.OPlaceholder;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.exception.*;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.replication.OAsyncReplicationError;
import com.orientechnologies.orient.core.sql.OCommandExecutorSQLDelegate;
import com.orientechnologies.orient.core.sql.OCommandExecutorSQLSelect;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.functions.OSQLFunctionRuntime;
import com.orientechnologies.orient.core.storage.*;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.OFreezableStorageComponent;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OLocalPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.core.storage.ridbag.sbtree.OSBTreeCollectionManager;
import com.orientechnologies.orient.core.tx.OTransactionInternal;
import com.orientechnologies.orient.enterprise.channel.binary.ODistributedRedirectException;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.*;
import com.orientechnologies.orient.server.distributed.ODistributedRequest.EXECUTION_MODE;
import com.orientechnologies.orient.server.distributed.impl.metadata.OClassDistributed;
import com.orientechnologies.orient.server.distributed.impl.task.*;
import com.orientechnologies.orient.server.distributed.task.*;
import com.orientechnologies.orient.server.hazelcast.OHazelcastPlugin;

import java.io.*;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Distributed storage implementation that routes to the owner node the request.
 *
 * @author Luca Garulli (l.garulli--at--orientdb.com)
 */
public class ODistributedStorage implements OStorage, OFreezableStorageComponent, OAutoshardedStorage {
  private final    String                    name;
  private final    OServer                   serverInstance;
  private final    ODistributedServerManager dManager;
  private volatile OAbstractPaginatedStorage wrapped;

  private ODistributedServerManager.DB_STATUS prevStatus;
  private ODistributedDatabase                localDistributedDatabase;
  private ODistributedStorageEventListener    eventListener;

  private volatile ODistributedConfiguration distributedConfiguration;
  private volatile File                      lastValidBackup = null;

  public ODistributedStorage(final OServer iServer, final String dbName) {
    this.serverInstance = iServer;
    this.dManager = iServer.getDistributedManager();
    this.name = dbName;
  }

  public synchronized void replaceIfNeeded(final OAbstractPaginatedStorage wrapped) {
    if (this.wrapped != wrapped)
      this.wrapped = wrapped;
  }

  public synchronized void wrap(final OAbstractPaginatedStorage wrapped) {
    if (this.wrapped != null)
      // ALREADY WRAPPED
      return;

    this.wrapped = wrapped;
    this.wrapped.underDistributedStorage();
    this.localDistributedDatabase = dManager.getMessageService().getDatabase(getName());

    ODistributedServerLog
        .debug(this, dManager != null ? dManager.getLocalNodeName() : "?", null, ODistributedServerLog.DIRECTION.NONE,
            "Installing distributed storage on database '%s'", wrapped.getName());

    final int queueSize = getServer().getContextConfiguration()
        .getValueAsInteger(OGlobalConfiguration.DISTRIBUTED_ASYNCH_QUEUE_SIZE);

  }

  /**
   * Supported only in embedded storage. Use <code>SELECT FROM metadata:storage</code> instead.
   */
  @Override
  public String getCreatedAtVersion() {
    throw new UnsupportedOperationException("Supported only in embedded storage. Use 'SELECT FROM metadata:storage' instead.");
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
    if (isLocalEnv())
      // ALREADY DISTRIBUTED
      return wrapped.command(iCommand);

    List<String> servers = (List<String>) iCommand.getContext().getVariable("servers");
    if (servers == null) {
      servers = new ArrayList<String>();
      iCommand.getContext().setVariable("servers", servers);
    }
    final String localNodeName = dManager.getLocalNodeName();

    servers.add(localNodeName);

    final ODistributedConfiguration dbCfg = distributedConfiguration;
    if (!dbCfg.isReplicationActive(null, localNodeName))
      // DON'T REPLICATE
      return wrapped.command(iCommand);

    final OCommandExecutor executor = OCommandManager.instance().getExecutor(iCommand);

    executor.setProgressListener(iCommand.getProgressListener());
    executor.parse(iCommand);

    final OCommandExecutor exec = executor instanceof OCommandExecutorSQLDelegate ?
        ((OCommandExecutorSQLDelegate) executor).getDelegate() :
        executor;

    if (!exec.isIdempotent()) {
      resetLastValidBackup();
    }

    if (exec.isIdempotent() && !dManager.isNodeAvailable(dManager.getLocalNodeName(), getName())) {
      // SPECIAL CASE: NODE IS OFFLINE AND THE COMMAND IS IDEMPOTENT, EXECUTE IT LOCALLY ONLY
      ODistributedServerLog.warn(this, dManager.getLocalNodeName(), null, ODistributedServerLog.DIRECTION.NONE,
          "Node '%s' is %s, the command '%s' against database '%s' will be executed only on local server with the possibility to have partial result",
          dManager.getLocalNodeName(), dManager.getDatabaseStatus(dManager.getLocalNodeName(), getName()), iCommand,
          wrapped.getName());

      return wrapped.command(iCommand);
    }

    if (!exec.isIdempotent())
      checkNodeIsMaster(localNodeName, dbCfg, "Command '" + iCommand + "'");

    try {
      Object result = null;
      OCommandDistributedReplicateRequest.DISTRIBUTED_EXECUTION_MODE executionMode = OCommandDistributedReplicateRequest.DISTRIBUTED_EXECUTION_MODE.LOCAL;
      OCommandDistributedReplicateRequest.DISTRIBUTED_RESULT_MGMT resultMgmt = OCommandDistributedReplicateRequest.DISTRIBUTED_RESULT_MGMT.CHECK_FOR_EQUALS;
      boolean executeOnLocalNodeFirst = true;

      if (OScenarioThreadLocal.INSTANCE.getRunMode() != RUN_MODE.RUNNING_DISTRIBUTED) {
        if (exec instanceof OCommandDistributedReplicateRequest) {
          executionMode = ((OCommandDistributedReplicateRequest) exec).getDistributedExecutionMode();
          resultMgmt = ((OCommandDistributedReplicateRequest) exec).getDistributedResultManagement();
          executeOnLocalNodeFirst = ((OCommandDistributedReplicateRequest) exec).isDistributedExecutingOnLocalNodeFirst();
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
          if (!exec.isIdempotent() && dbCfg.isSharded())
            throw new ODistributedException("Cannot distribute the command '" + iCommand.getText()
                + "' because it is not idempotent and a map-reduce has been requested");

          final Map<String, Collection<String>> nodeClusterMap = dbCfg
              .getServerClusterMap(involvedClusters, localNodeName, exec.isIdempotent());

          final Map<String, Object> results;

          if (exec.isIdempotent() && nodeClusterMap.size() == 1 && nodeClusterMap.keySet().iterator().next()
              .equals(localNodeName)) {
            // LOCAL NODE, AVOID TO DISTRIBUTE IT
            // CALL IN DEFAULT MODE TO LET OWN COMMAND TO REDISTRIBUTE CHANGES (LIKE INSERT)
            result = wrapped.command(iCommand);

            results = new HashMap<String, Object>(1);
            results.put(localNodeName, result);

          } else {
            // SELECT: SPLIT CLASSES/CLUSTER IF ANY
            results = executeOnServers(iCommand, exec, involvedClusters, nodeClusterMap);
          }

          final OCommandExecutorSQLSelect select = exec instanceof OCommandExecutorSQLSelect ?
              (OCommandExecutorSQLSelect) exec :
              null;

          if (select != null && select.isAnyFunctionAggregates() && !select.hasGroupBy()) {
            result = mergeResultByAggregation(select, results);
          } else {
            // MIX & FILTER RESULT SET AVOIDING DUPLICATES
            // TODO: ONCE OPTIMIZED (SEE ABOVE) AVOID TO FILTER HERE

            result = exec.mergeResults(results);
          }

          if (result instanceof Throwable && results.containsKey(localNodeName))
            undoCommandOnLocalServer(iCommand);

        } else {
          final OAbstractCommandTask task = iCommand instanceof OCommandScript ?
              new OScriptTask(iCommand) :
              new OSQLCommandTask(iCommand, new HashSet<String>());
          task.setResultStrategy(OAbstractRemoteTask.RESULT_STRATEGY.ANY);

          final Set<String> nodes = dbCfg.getServers(involvedClusters);

          if (iCommand instanceof ODistributedCommand)
            nodes.removeAll(((ODistributedCommand) iCommand).nodesToExclude());

          if (executeOnlyLocally(localNodeName, dbCfg, exec, involvedClusters, nodes))
            // LOCAL NODE, AVOID TO DISTRIBUTE IT
            // CALL IN DEFAULT MODE TO LET OWN COMMAND TO REDISTRIBUTE CHANGES (LIKE INSERT)
            return wrapped.command(iCommand);

          final Object localResult;

          final boolean executedLocally = executeOnLocalNodeFirst && nodes.contains(localNodeName);

          if (exec.involveSchema())
            // EXECUTE THE COMMAND IN LOCK
            result = dManager
                .executeInDistributedDatabaseLock(getName(), 20000, dManager.getDatabaseConfiguration(getName()).modify(),
                    new OCallable<Object, OModifiableDistributedConfiguration>() {
                      @Override
                      public Object call(OModifiableDistributedConfiguration iArgument) {
                        return executeCommand(iCommand, localNodeName, involvedClusters, task, nodes, executedLocally);
                      }
                    });
          else
            result = executeCommand(iCommand, localNodeName, involvedClusters, task, nodes, executedLocally);
        }

        break;
      }

      if (result instanceof ONeedRetryException)
        throw (ONeedRetryException) result;
      else if (result instanceof RuntimeException)
        throw (RuntimeException) result;
      else if (result instanceof Exception)
        throw OException.wrapException(new ODistributedException("Error on execution distributed COMMAND"), (Exception) result);

      return result;

    } catch (OConcurrentModificationException e) {
      localDistributedDatabase.getDatabaseRepairer().enqueueRepairRecord((ORecordId) e.getRid());
      throw e;
    } catch (ONeedRetryException e) {
      // PASS THROUGH
      throw e;
    } catch (HazelcastInstanceNotActiveException e) {
      throw OException.wrapException(new OOfflineNodeException("Hazelcast instance is not available"), e);

    } catch (HazelcastException e) {
      throw OException.wrapException(new OOfflineNodeException("Hazelcast instance is not available"), e);

    } catch (Exception e) {
      handleDistributedException("Cannot route COMMAND operation to the distributed node", e);
      // UNREACHABLE
      return null;
    }
  }

  public void acquireDistributedExclusiveLock(final long timeout) {
    dManager.getLockManagerRequester().acquireExclusiveLock(getName(), dManager.getLocalNodeName(), timeout);
  }

  public void releaseDistributedExclusiveLock() {
    dManager.getLockManagerRequester().releaseExclusiveLock(getName(), dManager.getLocalNodeName());
  }

  protected Object executeCommand(final OCommandRequestText iCommand, String localNodeName, Collection<String> involvedClusters,
      OAbstractCommandTask task, Set<String> nodes, boolean executedLocally) {
    Object localResult;
    Object result;
    if (executedLocally) {
      // EXECUTE ON LOCAL NODE FIRST
      try {

        localResult = OScenarioThreadLocal.executeAsDistributed(new Callable() {
          @Override
          public Object call() throws Exception {
            return wrapped.command(iCommand);
          }
        });

      } catch (RuntimeException e) {
        throw e;
      } catch (Exception e) {
        throw OException.wrapException(new ODistributedException("Cannot execute command " + iCommand), e);
      }
      nodes.remove(localNodeName);
    } else
      localResult = null;

    if (!nodes.isEmpty()) {
      if (ODistributedServerLog.isDebugEnabled())
        ODistributedServerLog.debug(this, dManager.getLocalNodeName(), nodes.toString(), ODistributedServerLog.DIRECTION.OUT,
            "Sending command '%s' database '%s'", iCommand, wrapped.getName());

      final ODistributedResponse dResponse = dManager
          .sendRequest(getName(), involvedClusters, nodes, task, dManager.getNextMessageIdCounter(), EXECUTION_MODE.RESPONSE,
              localResult, null, null);

      result = dResponse.getPayload();

      if (executedLocally && result instanceof Throwable)
        undoCommandOnLocalServer(iCommand);

    } else
      result = localResult;
    return result;
  }

  protected void undoCommandOnLocalServer(final OCommandRequestText iCommand) {
    // UNDO LOCALLY
    OScenarioThreadLocal.executeAsDistributed(new Callable() {
      @Override
      public Object call() throws Exception {
        final OCommandExecutor executor = OCommandManager.instance().getExecutor(iCommand);

        // COPY THE CONTEXT FROM THE REQUEST
        executor.setContext(iCommand.getContext());
        executor.setProgressListener(iCommand.getProgressListener());
        executor.parse(iCommand);

        final String undoCommand = ((OCommandDistributedReplicateRequest) executor).getUndoCommand();
        if (undoCommand != null) {
          wrapped.command(new OCommandSQL(undoCommand));
        }
        return null;
      }
    });
  }

  protected Map<String, Object> executeOnServers(final OCommandRequestText iCommand, final OCommandExecutor exec,
      final Collection<String> involvedClusters, final Map<String, Collection<String>> nodeClusterMap) {

    final Map<String, Object> results = new HashMap<String, Object>(nodeClusterMap.size());

    // EXECUTE DIFFERENT TASK ON EACH SERVER
    final List<String> nodes = new ArrayList<String>(1);
    for (Map.Entry<String, Collection<String>> c : nodeClusterMap.entrySet()) {
      final String nodeName = c.getKey();

      if (!dManager.isNodeAvailable(nodeName, getName())) {

        ODistributedServerLog.debug(this, dManager.getLocalNodeName(), nodeName, ODistributedServerLog.DIRECTION.OUT,
            "Node '%s' is involved in the command '%s' against database '%s', but the node is not active. Excluding it", nodeName,
            iCommand, wrapped.getName());

      } else {

        final OAbstractCommandTask task = iCommand instanceof OCommandScript ?
            new OScriptTask(iCommand) :
            new OSQLCommandTask(iCommand, c.getValue());
        task.setResultStrategy(OAbstractRemoteTask.RESULT_STRATEGY.ANY);

        nodes.clear();
        nodes.add(nodeName);

        try {
          final ODistributedResponse response = dManager
              .sendRequest(getName(), involvedClusters, nodes, task, dManager.getNextMessageIdCounter(), EXECUTION_MODE.RESPONSE,
                  null, null, null);

          if (response != null) {
            if (!(response.getPayload() instanceof ODistributedOperationException))
              // IGNORE ODistributedOperationException EXCEPTION ON SINGLE NODE
              results.put(nodeName, response.getPayload());
          }

        } catch (Exception e) {
          ODistributedServerLog.debug(this, dManager.getLocalNodeName(), nodeName, ODistributedServerLog.DIRECTION.OUT,
              "Error on execution of command '%s' against server '%s', database '%s'", iCommand, nodeName, wrapped.getName());
        }
      }
    }

    if (results.isEmpty())
      throw new ODistributedException("No active nodes found to execute command: " + iCommand);

    return results;
  }

  protected Object mergeResultByAggregation(final OCommandExecutorSQLSelect select, final Map<String, Object> iResults) {
    List<Object> list = null;
    ODocument doc = null;
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
        if (resultSet != null) {
          if (list == null) {
            list = new ArrayList<Object>();
            doc = new ODocument();
            list.add(doc);
          }
          for (Object r : resultSet) {
            if (r instanceof ODocument) {
              final ODocument d = (ODocument) r;

              for (Map.Entry<String, Object> p : proj.entrySet()) {
                // WRITE THE FIELD AS IS
                if (!(p.getValue() instanceof OSQLFunctionRuntime))
                  doc.field(p.getKey(), (Object) d.field(p.getKey()));
              }
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
          if (resultSet != null) {
            if (list == null) {
              list = new ArrayList<Object>();
              doc = new ODocument();
              list.add(doc);
            }

            for (Object r : resultSet) {
              if (r instanceof ODocument) {
                final ODocument d = (ODocument) r;
                toMerge.add(d.rawField(p.getKey()));
              }
            }
          }
        }
        if (doc != null) {
          // WRITE THE FINAL MERGED RESULT
          doc.field(p.getKey(), f.getFunction().mergeDistributedResult(toMerge));
        }
      }
    }

    return list;
  }

  /**
   * Only idempotent commands that don't involve any other node can be executed locally.
   */
  protected boolean executeOnlyLocally(final String localNodeName, final ODistributedConfiguration dbCfg,
      final OCommandExecutor exec, final Collection<String> involvedClusters, final Collection<String> nodes) {
    boolean executeLocally = false;
    if (exec.isIdempotent()) {
      final int availableNodes = nodes.size();

      // IDEMPOTENT: CHECK IF CAN WORK LOCALLY ONLY
      int maxReadQuorum;
      if (involvedClusters.isEmpty())
        maxReadQuorum = dbCfg.getReadQuorum(null, availableNodes, localNodeName);
      else {
        maxReadQuorum = 0;
        for (String cl : involvedClusters)
          maxReadQuorum = Math.max(maxReadQuorum, dbCfg.getReadQuorum(cl, availableNodes, localNodeName));
      }

      if (nodes.contains(localNodeName) && maxReadQuorum <= 1)
        executeLocally = true;
    }

    return executeLocally;
  }

  public boolean isLocalEnv() {
    return localDistributedDatabase == null || dManager == null || distributedConfiguration == null || OScenarioThreadLocal.INSTANCE
        .isRunModeDistributed();
  }

  public OStorageOperationResult<ORawBuffer> readRecord(final ORecordId iRecordId, final String iFetchPlan,
      final boolean iIgnoreCache, final boolean prefetchRecords, final ORecordCallback<ORawBuffer> iCallback) {

    if (isLocalEnv()) {
      // ALREADY DISTRIBUTED
      return wrapped.readRecord(iRecordId, iFetchPlan, iIgnoreCache, prefetchRecords, iCallback);
    }

    final ORawBuffer memCopy = localDistributedDatabase.getRecordIfLocked(iRecordId);
    if (memCopy != null)
      return new OStorageOperationResult<ORawBuffer>(memCopy);

    try {
      final String clusterName = getClusterNameByRID(iRecordId);

      final ODistributedConfiguration dbCfg = distributedConfiguration;
      final List<String> nodes = dbCfg.getServers(clusterName, null);
      final int availableNodes = nodes.size();

      // CHECK IF LOCAL NODE OWNS THE DATA AND READ-QUORUM = 1: GET IT LOCALLY BECAUSE IT'S FASTER
      final String localNodeName = dManager.getLocalNodeName();

      if (nodes.isEmpty()
          || nodes.contains(dManager.getLocalNodeName()) && dbCfg.getReadQuorum(clusterName, availableNodes, localNodeName) <= 1) {
        // DON'T REPLICATE
        return (OStorageOperationResult<ORawBuffer>) OScenarioThreadLocal.executeAsDistributed(new Callable() {
          @Override
          public Object call() throws Exception {
            return wrapped.readRecord(iRecordId, iFetchPlan, iIgnoreCache, prefetchRecords, iCallback);
          }
        });
      }

      final OReadRecordTask task = ((OReadRecordTask) dManager.getTaskFactoryManager().getFactoryByServerNames(nodes)
          .createTask(OReadRecordTask.FACTORYID)).init(iRecordId);

      // DISTRIBUTE IT
      final ODistributedResponse response = dManager
          .sendRequest(getName(), Collections.singleton(clusterName), nodes, task, dManager.getNextMessageIdCounter(),
              EXECUTION_MODE.RESPONSE, null, null, null);
      final Object dResult = response != null ? response.getPayload() : null;

      if (dResult instanceof ONeedRetryException)
        throw (ONeedRetryException) dResult;
      else if (dResult instanceof Exception)
        throw OException
            .wrapException(new ODistributedException("Error on execution distributed read record"), (Exception) dResult);

      return new OStorageOperationResult<ORawBuffer>((ORawBuffer) dResult);

    } catch (ONeedRetryException e) {
      // PASS THROUGH
      throw e;
    } catch (HazelcastInstanceNotActiveException e) {
      throw OException.wrapException(new OOfflineNodeException("Hazelcast instance is not available"), e);

    } catch (HazelcastException e) {
      throw OException.wrapException(new OOfflineNodeException("Hazelcast instance is not available"), e);

    } catch (Exception e) {
      handleDistributedException("Cannot route read record operation for %s to the distributed node", e, iRecordId);
      // UNREACHABLE
      return null;
    }
  }

  @Override
  public OStorageOperationResult<ORawBuffer> readRecordIfVersionIsNotLatest(final ORecordId rid, final String fetchPlan,
      final boolean ignoreCache, final int recordVersion) throws ORecordNotFoundException {
    if (isLocalEnv()) {
      return wrapped.readRecordIfVersionIsNotLatest(rid, fetchPlan, ignoreCache, recordVersion);
    }
    final ORawBuffer memCopy = localDistributedDatabase.getRecordIfLocked(rid);
    if (memCopy != null)
      return new OStorageOperationResult<ORawBuffer>(memCopy);

    try {
      final String clusterName = getClusterNameByRID(rid);

      final ODistributedConfiguration dbCfg = distributedConfiguration;
      final List<String> nodes = dbCfg.getServers(clusterName, null);
      final int availableNodes = nodes.size();

      // CHECK IF LOCAL NODE OWNS THE DATA AND READ-QUORUM = 1: GET IT LOCALLY BECAUSE IT'S FASTER
      final String localNodeName = dManager.getLocalNodeName();

      if (nodes.isEmpty()
          || nodes.contains(dManager.getLocalNodeName()) && dbCfg.getReadQuorum(clusterName, availableNodes, localNodeName) <= 1) {
        // DON'T REPLICATE
        return (OStorageOperationResult<ORawBuffer>) OScenarioThreadLocal.executeAsDistributed(new Callable() {
          @Override
          public Object call() throws Exception {
            return wrapped.readRecordIfVersionIsNotLatest(rid, fetchPlan, ignoreCache, recordVersion);
          }
        });
      }

      final OReadRecordIfNotLatestTask task = (OReadRecordIfNotLatestTask) dManager.getTaskFactoryManager()
          .getFactoryByServerNames(nodes).createTask(OReadRecordIfNotLatestTask.FACTORYID);
      task.init(rid, recordVersion);

      // DISTRIBUTE IT
      final Object result = dManager
          .sendRequest(getName(), Collections.singleton(clusterName), nodes, task, dManager.getNextMessageIdCounter(),
              EXECUTION_MODE.RESPONSE, null, null, null).getPayload();

      if (result instanceof ONeedRetryException)
        throw (ONeedRetryException) result;
      else if (result instanceof Exception)
        throw OException.wrapException(new ODistributedException("Error on execution distributed read record"), (Exception) result);

      return new OStorageOperationResult<ORawBuffer>((ORawBuffer) result);

    } catch (ONeedRetryException e) {
      // PASS THROUGH
      throw e;
    } catch (HazelcastInstanceNotActiveException e) {
      throw OException.wrapException(new OOfflineNodeException("Hazelcast instance is not available"), e);

    } catch (HazelcastException e) {
      throw OException.wrapException(new OOfflineNodeException("Hazelcast instance is not available"), e);

    } catch (Exception e) {
      handleDistributedException("Cannot route read record operation for %s to the distributed node", e, rid);
      // UNREACHABLE
      return null;
    }
  }

  @Override
  public OStorageOperationResult<Boolean> deleteRecord(final ORecordId iRecordId, final int iVersion, final int iMode,
      final ORecordCallback<Boolean> iCallback) {
    // IF is a real delete should be with a tx
    return wrapped.deleteRecord(iRecordId, iVersion, iMode, iCallback);
  }

  @Override
  public OSBTreeCollectionManager getSBtreeCollectionManager() {
    return wrapped.getSBtreeCollectionManager();
  }


  @Override
  public OStorageOperationResult<Integer> recyclePosition(ORecordId iRecordId, byte[] iContent, int iVersion, byte iRecordType) {
    return wrapped.recyclePosition(iRecordId, iContent, iVersion, iRecordType);
  }

  public int getConfigurationUpdated() {
    return distributedConfiguration.getVersion();
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
  public void open(final String iUserName, final String iUserPassword, final OContextConfiguration iProperties) {
    wrapped.open(iUserName, iUserPassword, iProperties);
  }

  @Override
  public void create(final OContextConfiguration iProperties) throws IOException {
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
    if (wrapped instanceof OLocalPaginatedStorage)
      dropStorageFiles();

    wrapped.delete();
  }

  @Override
  public String incrementalBackup(final String backupDirectory) {
    return wrapped.incrementalBackup(backupDirectory);
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
    if (wrapped == null)
      return;

    if (onDelete && wrapped instanceof OLocalPaginatedStorage) {
      // REMOVE distributed-config.json and distributed-sync.json files to allow removal of directory
      dropStorageFiles();
    }

    wrapped.close(iForce, onDelete);

  }

  public void closeOnDrop() {
    if (wrapped == null)
      return;
    if (wrapped instanceof OLocalPaginatedStorage) {
      // REMOVE distributed-config.json and distributed-sync.json files to allow removal of directory
      dropStorageFiles();
    }

    serverInstance.getDatabases().forceDatabaseClose(getName());

  }

  @Override
  public boolean isClosed() {
    if (wrapped == null)
      return true;

    return wrapped.isClosed();
  }

  @Override
  public List<ORecordOperation> commit(final OTransactionInternal iTx) {
    return wrapped.commit(iTx);
  }

  @Override
  public void rollback(final OTransactionInternal iTx) {
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
  public int addCluster(final String iClusterName, final Object... iParameters) {
    for (int retry = 0; retry < 10; ++retry) {
      final AtomicInteger clId = new AtomicInteger();

      if (!isLocalEnv()) {

        final StringBuilder cmd = new StringBuilder("create cluster `");
        cmd.append(iClusterName);
        cmd.append("`");

        // EXECUTE THIS OUTSIDE LOCK TO AVOID DEADLOCKS
        Object result = null;
        try {
          result = dManager
              .executeInDistributedDatabaseLock(getName(), 20000, dManager.getDatabaseConfiguration(getName()).modify(),
                  new OCallable<Object, OModifiableDistributedConfiguration>() {
                    @Override
                    public Object call(OModifiableDistributedConfiguration iArgument) {
                      clId.set(wrapped.addCluster(iClusterName, iParameters));

                      final OCommandSQL commandSQL = new OCommandSQL(cmd.toString());
                      commandSQL.addExcludedNode(getNodeId());
                      return command(commandSQL);
                    }
                  });
        } catch (Exception e) {
          // RETRY
          wrapped.dropCluster(iClusterName, false);

          try {
            Thread.sleep(300);
          } catch (InterruptedException e2) {
          }

          continue;
        }

        if (result != null && ((Integer) result).intValue() != clId.get()) {
          ODistributedServerLog.warn(this, dManager.getLocalNodeName(), null, ODistributedServerLog.DIRECTION.NONE,
              "Error on creating cluster '%s' on distributed nodes: ids are different (local=%d and remote=%d). Local clusters %s. Retrying %d/%d...",
              iClusterName, clId.get(), ((Integer) result).intValue(), getClusterNames(), retry, 10);

          wrapped.dropCluster(clId.get(), false);

          // REMOVE ON REMOTE NODES TOO
          cmd.setLength(0);
          cmd.append("drop cluster ");
          cmd.append(iClusterName);

          final OCommandSQL commandSQL = new OCommandSQL(cmd.toString());
          commandSQL.addExcludedNode(getNodeId());
          command(commandSQL);

          try {
            Thread.sleep(300);
          } catch (InterruptedException e) {
          }

          wrapped.reload(); // TODO: RELOAD DOESN'T DO ANYTHING WHILE HERE IT'S NEEDED A WAY TO CLOSE/OPEN THE DB
          continue;
        }
      } else
        clId.set(wrapped.addCluster(iClusterName, iParameters));

      return clId.get();
    }

    throw new ODistributedException(
        "Error on creating cluster '" + iClusterName + "' on distributed nodes: local and remote ids assigned are different");
  }

  @Override
  public int addCluster(String iClusterName, int iRequestedId, Object... iParameters) {
    resetLastValidBackup();
    for (int retry = 0; retry < 10; ++retry) {
      final AtomicInteger clId = new AtomicInteger();

      if (!isLocalEnv()) {

        final StringBuilder cmd = new StringBuilder("create cluster `");
        cmd.append(iClusterName);
        cmd.append("`");
        cmd.append(" ID ");
        cmd.append(iRequestedId);

        // EXECUTE THIS OUTSIDE LOCK TO AVOID DEADLOCKS
        Object result = null;
        try {
          result = dManager
              .executeInDistributedDatabaseLock(getName(), 20000, dManager.getDatabaseConfiguration(getName()).modify(),
                  new OCallable<Object, OModifiableDistributedConfiguration>() {
                    @Override
                    public Object call(OModifiableDistributedConfiguration iArgument) {
                      clId.set(wrapped.addCluster(iClusterName, iRequestedId, iParameters));

                      final OCommandSQL commandSQL = new OCommandSQL(cmd.toString());
                      commandSQL.addExcludedNode(getNodeId());
                      return command(commandSQL);
                    }
                  });
        } catch (Exception e) {
          // RETRY
          wrapped.dropCluster(iClusterName, false);

          try {
            Thread.sleep(300);
          } catch (InterruptedException e2) {
          }

          continue;
        }

        if (result != null && ((Integer) result).intValue() != clId.get()) {
          ODistributedServerLog.warn(this, dManager.getLocalNodeName(), null, ODistributedServerLog.DIRECTION.NONE,
              "Error on creating cluster '%s' on distributed nodes: ids are different (local=%d and remote=%d). Local clusters %s. Retrying %d/%d...",
              iClusterName, clId.get(), ((Integer) result).intValue(), getClusterNames(), retry, 10);

          wrapped.dropCluster(clId.get(), false);

          // REMOVE ON REMOTE NODES TOO
          cmd.setLength(0);
          cmd.append("drop cluster ");
          cmd.append(iClusterName);

          final OCommandSQL commandSQL = new OCommandSQL(cmd.toString());
          commandSQL.addExcludedNode(getNodeId());
          command(commandSQL);

          try {
            Thread.sleep(300);
          } catch (InterruptedException e) {
          }

          wrapped.reload(); // TODO: RELOAD DOESN'T DO ANYTHING WHILE HERE IT'S NEEDED A WAY TO CLOSE/OPEN THE DB
          continue;
        }
      } else
        clId.set(wrapped.addCluster(iClusterName, iRequestedId, iParameters));
      return clId.get();
    }

    throw new ODistributedException(
        "Error on creating cluster '" + iClusterName + "' on distributed nodes: local and remote ids assigned are different");
  }

  public boolean dropCluster(final String iClusterName, final boolean iTruncate) {
    resetLastValidBackup();
    final AtomicBoolean clId = new AtomicBoolean();

    if (!isLocalEnv()) {

      final StringBuilder cmd = new StringBuilder();
      if (iTruncate) {
        cmd.append("truncate cluster `");
      } else {
        cmd.append("create cluster `");
      }
      cmd.append(iClusterName);
      cmd.append("`");

      // EXECUTE THIS OUTSIDE LOCK TO AVOID DEADLOCKS
      dManager.executeInDistributedDatabaseLock(getName(), 20000, dManager.getDatabaseConfiguration(getName()).modify(),
          new OCallable<Object, OModifiableDistributedConfiguration>() {
            @Override
            public Object call(OModifiableDistributedConfiguration iArgument) {
              clId.set(wrapped.dropCluster(iClusterName, iTruncate));

              final OCommandSQL commandSQL = new OCommandSQL(cmd.toString());
              commandSQL.addExcludedNode(getNodeId());
              return command(commandSQL);
            }
          });
      return clId.get();
    } else
      return wrapped.dropCluster(iClusterName, iTruncate);
  }

  @Override
  public boolean dropCluster(final int iId, final boolean iTruncate) {
    resetLastValidBackup();
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
    return name;
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

  public ODistributedStorageEventListener getEventListener() {
    return eventListener;
  }

  public void setEventListener(final ODistributedStorageEventListener eventListener) {
    this.eventListener = eventListener;
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

  public ODistributedConfiguration getDistributedConfiguration() {
    if (distributedConfiguration == null) {
      final Map<String, Object> map = dManager.getConfigurationMap();
      if (map == null)
        return null;

      ODocument doc = (ODocument) map.get(OHazelcastPlugin.CONFIG_DATABASE_PREFIX + getName());
      if (doc != null) {
        // DISTRIBUTED CFG AVAILABLE: COPY IT TO THE LOCAL DIRECTORY
        ODistributedServerLog.info(this, dManager.getLocalNodeName(), null, ODistributedServerLog.DIRECTION.NONE,
            "Downloaded configuration for database '%s' from the cluster", getName());
        setDistributedConfiguration(new OModifiableDistributedConfiguration(doc));
      } else {
        doc = loadDatabaseConfiguration(getDistributedConfigFile());
        if (doc == null) {
          // LOOK FOR THE STD FILE
          doc = loadDatabaseConfiguration(dManager.getDefaultDatabaseConfigFile());
          if (doc == null)
            throw new OConfigurationException(
                "Cannot load default distributed for database '" + getName() + "' config file: " + dManager
                    .getDefaultDatabaseConfigFile());

          // SAVE THE GENERIC FILE AS DATABASE FILE
          setDistributedConfiguration(new OModifiableDistributedConfiguration(doc));
        } else
          // JUST LOAD THE FILE IN MEMORY
          distributedConfiguration = new ODistributedConfiguration(doc);

        // LOADED FILE, PUBLISH IT IN THE CLUSTER
        dManager.updateCachedDatabaseConfiguration(getName(), new OModifiableDistributedConfiguration(doc), true);
      }
    }
    return distributedConfiguration;
  }

  public void setDistributedConfiguration(final OModifiableDistributedConfiguration distributedConfiguration) {
    if (this.distributedConfiguration == null || distributedConfiguration.getVersion() > this.distributedConfiguration
        .getVersion()) {
      this.distributedConfiguration = new ODistributedConfiguration(distributedConfiguration.getDocument().copy());

      // PRINT THE NEW CONFIGURATION
      final String cfgOutput = ODistributedOutput
          .formatClusterTable(dManager, getName(), distributedConfiguration, dManager.getTotalNodes(getName()));

      ODistributedServerLog.info(this, dManager.getLocalNodeName(), null, ODistributedServerLog.DIRECTION.NONE,
          "Setting new distributed configuration for database: %s (version=%d)%s\n", getName(),
          distributedConfiguration.getVersion(), cfgOutput);

      saveDatabaseConfiguration();
    }
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
  public boolean isFrozen() {
    return getFreezableStorage().isFrozen();
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
    // THIS CAUSES DEADLOCK
    // try {
    // return (List<String>) executeOperationInLock(new OCallable<Object, Void>() {
    // @Override
    // public Object call(Void iArgument) {
    final String localNode = dManager.getLocalNodeName();

    final ODistributedServerManager.DB_STATUS prevStatus = dManager.getDatabaseStatus(localNode, getName());
    if (prevStatus == ODistributedServerManager.DB_STATUS.ONLINE)
      // SET STATUS = BACKUP
      dManager.setDatabaseStatus(localNode, getName(), ODistributedServerManager.DB_STATUS.BACKUP);

    try {

      return wrapped.backup(out, options, callable, iListener, compressionLevel, bufferSize);

    } catch (IOException e) {
      throw OException.wrapException(new OIOException("Error on executing backup"), e);
    } finally {
      if (prevStatus == ODistributedServerManager.DB_STATUS.ONLINE)
        // RESTORE PREVIOUS STATUS
        dManager.setDatabaseStatus(localNode, getName(), ODistributedServerManager.DB_STATUS.ONLINE);
    }
    // }
    // });
    // } catch (InterruptedException e) {
    // Thread.currentThread().interrupt();
    // throw OException.wrapException(new OIOException("Backup interrupted"), e);
    // }
  }

  @Override
  public void restore(final InputStream in, final Map<String, Object> options, final Callable<Object> callable,
      final OCommandOutputListener iListener) throws IOException {
    wrapped.restore(in, options, callable, iListener);
  }

  public String getClusterNameByRID(final ORecordId iRid) {
    final OCluster cluster = getClusterById(iRid.getClusterId());
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

  protected void checkNodeIsMaster(final String localNodeName, final ODistributedConfiguration dbCfg, final String operation) {
    final ODistributedConfiguration.ROLES nodeRole = dbCfg.getServerRole(localNodeName);
    if (nodeRole != ODistributedConfiguration.ROLES.MASTER)
      throw new OWriteOperationNotPermittedException(
          "Cannot execute write operation (" + operation + ") on node '" + localNodeName + "' because is non a master");
  }

  public File getLastValidBackup() {
    return lastValidBackup;
  }

  public void setLastValidBackup(final File lastValidBackup) {
    this.lastValidBackup = lastValidBackup;
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

  private OFreezableStorageComponent getFreezableStorage() {
    if (wrapped instanceof OFreezableStorageComponent)
      return ((OFreezableStorageComponent) wrapped);
    else
      throw new UnsupportedOperationException("Storage engine " + wrapped.getType() + " does not support freeze operation");
  }

  public void resetLastValidBackup() {
    lastValidBackup = null;
  }

  protected void dropStorageFiles() {
    // REMOVE distributed-config.json and distributed-sync.json files to allow removal of directory
    final File dCfg = new File(
        ((OLocalPaginatedStorage) wrapped).getStoragePath() + "/" + getDistributedManager().FILE_DISTRIBUTED_DB_CONFIG);

    try {
      if (dCfg.exists()) {
        for (int i = 0; i < 10; ++i) {
          if (dCfg.delete())
            break;
          Thread.sleep(100);
        }
      }

      final File dCfg2 = new File(
          ((OLocalPaginatedStorage) wrapped).getStoragePath() + "/" + ODistributedDatabaseImpl.DISTRIBUTED_SYNC_JSON_FILENAME);
      if (dCfg2.exists()) {
        for (int i = 0; i < 10; ++i) {
          if (dCfg2.delete())
            break;
          Thread.sleep(100);
        }
      }
    } catch (InterruptedException e) {
      // IGNORE IT
    }
  }

  public ODocument loadDatabaseConfiguration(final File file) {
    if (!file.exists() || file.length() == 0)
      return null;

    ODistributedServerLog.info(this, dManager.getLocalNodeName(), null, ODistributedServerLog.DIRECTION.NONE,
        "Loaded configuration for database '%s' from disk: %s", getName(), file);

    FileInputStream f = null;
    try {
      f = new FileInputStream(file);
      final byte[] buffer = new byte[(int) file.length()];
      f.read(buffer);

      final ODocument doc = (ODocument) new ODocument().fromJSON(new String(buffer), "noMap");
      doc.field("version", 1);
      return doc;

    } catch (Exception e) {
      ODistributedServerLog.error(this, dManager.getLocalNodeName(), null, ODistributedServerLog.DIRECTION.NONE,
          "Error on loading distributed configuration file in: %s", e, file.getAbsolutePath());
    } finally {
      if (f != null)
        try {
          f.close();
        } catch (IOException e) {
        }
    }
    return null;
  }

  protected void saveDatabaseConfiguration() {
    // SAVE THE CONFIGURATION TO DISK
    FileOutputStream f = null;
    try {
      File file = getDistributedConfigFile();

      ODistributedServerLog.debug(this, dManager.getLocalNodeName(), null, ODistributedServerLog.DIRECTION.NONE,
          "Saving distributed configuration file for database '%s' to: %s", getName(), file);

      if (!file.exists()) {
        file.getParentFile().mkdirs();
        file.createNewFile();
      }

      f = new FileOutputStream(file);
      f.write(distributedConfiguration.getDocument().toJSON().getBytes());
      f.flush();
    } catch (Exception e) {
      ODistributedServerLog.error(this, dManager.getLocalNodeName(), null, ODistributedServerLog.DIRECTION.NONE,
          "Error on saving distributed configuration file", e);

    } finally {
      if (f != null)
        try {
          f.close();
        } catch (IOException e) {
        }
    }
  }

  protected File getDistributedConfigFile() {
    return new File(serverInstance.getDatabaseDirectory() + getName() + "/" + ODistributedServerManager.FILE_DISTRIBUTED_DB_CONFIG);
  }

  public ODistributedDatabase getLocalDistributedDatabase() {
    return localDistributedDatabase;
  }

  @Override
  public void setSchemaRecordId(String schemaRecordId) {
    wrapped.setSchemaRecordId(schemaRecordId);
  }

  @Override
  public void setDateFormat(String dateFormat) {
    wrapped.setDateFormat(dateFormat);
  }

  @Override
  public void setTimeZone(TimeZone timeZoneValue) {
    wrapped.setTimeZone(timeZoneValue);
  }

  @Override
  public void setLocaleLanguage(String locale) {
    wrapped.setLocaleLanguage(locale);
  }

  @Override
  public void setCharset(String charset) {
    wrapped.setCharset(charset);
  }

  @Override
  public void setIndexMgrRecordId(String indexMgrRecordId) {
    wrapped.setIndexMgrRecordId(indexMgrRecordId);
  }

  @Override
  public void setDateTimeFormat(String dateTimeFormat) {
    wrapped.setDateTimeFormat(dateTimeFormat);
  }

  @Override
  public void setLocaleCountry(String localeCountry) {
    wrapped.setLocaleCountry(localeCountry);
  }

  @Override
  public void setClusterSelection(String clusterSelection) {
    wrapped.setClusterSelection(clusterSelection);
  }

  @Override
  public void setMinimumClusters(int minimumClusters) {
    wrapped.setMinimumClusters(minimumClusters);
  }

  @Override
  public void setValidation(boolean validation) {
    wrapped.setValidation(validation);
  }

  @Override
  public void removeProperty(String property) {
    wrapped.removeProperty(property);
  }

  @Override
  public void setProperty(String property, String value) {
    wrapped.setProperty(property, value);
  }

  @Override
  public void setRecordSerializer(String recordSerializer, int version) {
    wrapped.setRecordSerializer(recordSerializer, version);
  }

  @Override
  public void clearProperties() {
    wrapped.clearProperties();
  }
}
