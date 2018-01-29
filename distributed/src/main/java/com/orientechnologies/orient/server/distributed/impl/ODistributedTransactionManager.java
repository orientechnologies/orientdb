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

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.common.concur.ONeedRetryException;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.common.util.OPair;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.OExecutionThreadLocal;
import com.orientechnologies.orient.core.db.OScenarioThreadLocal;
import com.orientechnologies.orient.core.db.record.OPlaceholder;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.exception.*;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.ORecordVersionHelper;
import com.orientechnologies.orient.core.replication.OAsyncReplicationError;
import com.orientechnologies.orient.core.replication.OAsyncReplicationOk;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OLocalPaginatedStorage;
import com.orientechnologies.orient.core.tx.OTransactionInternal;
import com.orientechnologies.orient.core.tx.OTransaction;
import com.orientechnologies.orient.core.tx.OTransactionAbstract;
import com.orientechnologies.orient.server.distributed.*;
import com.orientechnologies.orient.server.distributed.ODistributedRequest.EXECUTION_MODE;
import com.orientechnologies.orient.server.distributed.impl.task.*;
import com.orientechnologies.orient.server.distributed.task.*;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Distributed transaction manager.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class ODistributedTransactionManager {
  private final ODistributedServerManager dManager;
  private final ODistributedStorage       storage;
  private final ODistributedDatabase      localDistributedDatabase;

  private static final boolean SYNC_TX_COMPLETED = false;

  public ODistributedTransactionManager(final ODistributedStorage storage, final ODistributedServerManager manager,
      final ODistributedDatabase iDDatabase) {
    this.dManager = manager;
    this.storage = storage;
    this.localDistributedDatabase = iDDatabase;
  }

  public List<ORecordOperation> commit(final ODatabaseDocumentInternal database, final OTransactionInternal iTx, final ODistributedStorageEventListener eventListener) {
    final String localNodeName = dManager.getLocalNodeName();

    try {
      iTx.setStatus(OTransaction.TXSTATUS.BEGUN);

      final ODistributedConfiguration dbCfg = dManager.getDatabaseConfiguration(storage.getName());

      // CHECK THE LOCAL NODE IS THE OWNER OF THE CLUSTER IDS
      checkForClusterIds(iTx, localNodeName, dbCfg);

      Boolean executionModeSynch = dbCfg.isExecutionModeSynchronous(null);
      if (executionModeSynch == null)
        executionModeSynch = Boolean.TRUE;

      final ODistributedRequestId requestId = new ODistributedRequestId(dManager.getLocalNodeId(),
          dManager.getNextMessageIdCounter());

      ODistributedServerLog.debug(this, dManager.getLocalNodeName(), null, ODistributedServerLog.DIRECTION.NONE,
          "Starting distributed transaction on database '%s'... (reqId=%s thread=%d)", storage.getName(), requestId,
          Thread.currentThread().getId());

      final AtomicBoolean releaseContext = new AtomicBoolean(false);

      final AtomicBoolean lockReleased = new AtomicBoolean(true);
      final ODistributedTxContext ctx = localDistributedDatabase.registerTxContext(requestId);
      try {

        acquireMultipleRecordLocks(iTx, eventListener, ctx);
        lockReleased.set(false);

        // CREATE UNDO CONTENT AFTER LOCK FOR DISTRIBUTED 2-PHASE ROLLBACK
        final List<OAbstractRemoteTask> undoTasks = createUndoTasksFromTx(iTx, localDistributedDatabase, requestId);

        final List<ORecordOperation> uResult = (List<ORecordOperation>) OScenarioThreadLocal.executeAsDistributed(new Callable() {
          @Override
          public Object call() throws Exception {
            return storage.commit(iTx);
          }
        });

        try {
          localDistributedDatabase.getSyncConfiguration()
              .setLastLSN(localNodeName, ((OLocalPaginatedStorage) storage.getUnderlying()).getLSN(), true);
        } catch (IOException e) {
          ODistributedServerLog
              .debug(this, dManager != null ? dManager.getLocalNodeName() : "?", null, ODistributedServerLog.DIRECTION.NONE,
                  "Error on updating local LSN configuration for database '%s'", storage.getName());
        }

        final Set<String> involvedClusters = getInvolvedClusters(uResult);
        Set<String> nodes = getAvailableNodesButLocal(dbCfg, involvedClusters, localNodeName);

        final OTxTask txTask = !nodes.isEmpty() ? createTxTask(uResult, nodes) : null;

        // AFTER THE CREATION OF TASKS, REMOVE THE TX OBJECT FROM DATABASE TO AVOID UNDO OPERATIONS ARE "LOST IN TRANSACTION"
        database.setDefaultTransactionMode();

        // After commit force the clean of dirty managers due to possible copy and miss clean.
        for (ORecordOperation ent : iTx.getRecordOperations())
          ORecordInternal.getDirtyManager(ent.getRecord()).clear();

        if (nodes.isEmpty()) {
          // NO FURTHER NODES TO INVOLVE
          releaseContext.set(true);
          return null;
        }

        updateUndoTaskWithCreatedRecords(uResult, undoTasks);

        final OTxTaskResult localResult = createLocalTxResult(uResult);

        txTask.setLocalUndoTasks(undoTasks);

        try {
          txTask.setLastLSN(((OAbstractPaginatedStorage) storage.getUnderlying()).getLSN());

          iTx.setStatus(OTransaction.TXSTATUS.COMMITTING);

          if (executionModeSynch) {
            // SYNCHRONOUS CALL: REPLICATE IT
            final ODistributedResponse lastResult = dManager
                .sendRequest(storage.getName(), involvedClusters, nodes, txTask, requestId.getMessageId(), EXECUTION_MODE.RESPONSE,
                    localResult, null, new OCallable<Void, ODistributedResponseManager>() {
                      @Override
                      public Void call(final ODistributedResponseManager resp) {
                        // FINALIZE ONLY IF IT IS IN ASYNCH MODE
                        if (finalizeRequest(resp, localNodeName, involvedClusters, txTask)) {
                          if (lockReleased.compareAndSet(false, true)) {
                            localDistributedDatabase.popTxContext(requestId);
                            ctx.destroy();
                          }
                        }

                        // CONTEXT WILL BE RELEASED
                        releaseContext.set(true);
                        return null;
                      }
                    });

            if (ctx.isCanceled())
              throw new ODistributedOperationException("Transaction as been canceled because concurrent updates");

            if (lastResult == null)
              throw new OTransactionException("No response received from distributed servers");

            processCommitResult(localNodeName, iTx, txTask, involvedClusters, uResult, nodes, lastResult.getRequestId(),
                lastResult);

            ODistributedServerLog.debug(this, localNodeName, null, ODistributedServerLog.DIRECTION.NONE,
                "Distributed transaction succeeded. Tasks: %s", txTask.getTasks());

            // OK, DISTRIBUTED COMMIT SUCCEED
            return uResult;

          } else {
            // ASYNC, MANAGE REPLICATION CALLBACK
            final OCallable<Void, ODistributedRequestId> unlockCallback = new OCallable<Void, ODistributedRequestId>() {
              @Override
              public Void call(final ODistributedRequestId reqId) {
                // FREE THE CONTEXT
                if (lockReleased.compareAndSet(false, true)) {
                  localDistributedDatabase.popTxContext(requestId);
                  ctx.destroy();
                }
                return null;
              }
            };

            executeAsyncTx(nodes, localResult, involvedClusters, txTask, requestId.getMessageId(), localNodeName, unlockCallback);
          }
        } catch (Exception e) {
          ODistributedServerLog.debug(this, dManager.getLocalNodeName(), null, ODistributedServerLog.DIRECTION.NONE,
              "Error on executing transaction on database '%s', rollback... (reqId=%s err='%s')", storage.getName(), requestId, e);

          releaseContext.set(true);

          try {
            // ROLLBACK TX
            final OCompleted2pcTask task = (OCompleted2pcTask) dManager.getTaskFactoryManager().getFactoryByServerNames(nodes)
                .createTask(OCompleted2pcTask.FACTORYID);
            task.init(requestId, false, txTask.getPartitionKey());

            sendTxCompleted(localNodeName, involvedClusters, nodes, task);
          } finally {
            storage.executeUndoOnLocalServer(requestId, txTask);
          }

          if (e instanceof RuntimeException)
            throw (RuntimeException) e;
          else if (e instanceof InterruptedException)
            throw OException.wrapException(new ODistributedOperationException("Cannot commit transaction"), e);
          else
            throw OException.wrapException(new ODistributedException("Cannot commit transaction"), e);
        }

      } catch (RuntimeException e) {
        releaseContext.set(true);
        throw e;
      } catch (InterruptedException e) {
        releaseContext.set(true);
        throw OException.wrapException(new ODistributedOperationException("Cannot commit transaction"), e);
      } catch (Exception e) {
        releaseContext.set(true);
        throw OException.wrapException(new ODistributedException("Cannot commit transaction"), e);
      } finally {
        if (releaseContext.get() && lockReleased.compareAndSet(false, true)) {
          localDistributedDatabase.popTxContext(requestId);
          ctx.destroy();
        }
      }

    } catch (OValidationException e) {
      throw e;
    } catch (ODistributedRecordLockedException e) {
      throw e;
    } catch (OConcurrentCreateException e) {
      // REQUEST A REPAIR OF THE CLUSTER BECAUSE IS NOT ALIGNED
      localDistributedDatabase.getDatabaseRepairer().enqueueRepairCluster(e.getActualRid().getClusterId());
      throw e;
    } catch (OConcurrentModificationException e) {
      localDistributedDatabase.getDatabaseRepairer().enqueueRepairRecord((ORecordId) e.getRid());
      throw e;

    } catch (Exception e) {

      for (ORecordOperation op : iTx.getRecordOperations()) {
        if (op.type == ORecordOperation.CREATED) {
          final ORecordId lockEntireCluster = (ORecordId) op.getRID().copy();
          localDistributedDatabase.getDatabaseRepairer().enqueueRepairCluster(lockEntireCluster.getClusterId());
        }
        localDistributedDatabase.getDatabaseRepairer().enqueueRepairRecord((ORecordId) op.getRID());
      }

      storage.handleDistributedException("Cannot route TX operation against distributed node", e);
    }

    return null;
  }

  protected void checkForClusterIds(final OTransactionInternal iTx, final String localNodeName, final ODistributedConfiguration dbCfg) {
    for (ORecordOperation op : iTx.getRecordOperations()) {
      final ORecordId rid = (ORecordId) op.getRecord().getIdentity();
      switch (op.type) {
      case ORecordOperation.CREATED:
        final ORecordId newRid = rid.copy();
        if (rid.getClusterId() < 1) {
          final String clusterName = ((OTransactionAbstract) iTx).getClusterName(op.getRecord());
          if (clusterName != null) {
            newRid.setClusterId(ODatabaseRecordThreadLocal.instance().get().getClusterIdByName(clusterName));
            iTx.updateIdentityAfterCommit(rid, newRid);
          }
        }

        if (storage.checkForCluster(op.getRecord(), localNodeName, dbCfg) != null)
          iTx.updateIdentityAfterCommit(rid, newRid);

        break;
      }
    }
  }

  protected Set<String> getAvailableNodesButLocal(ODistributedConfiguration dbCfg, Set<String> involvedClusters,
      String localNodeName) {
    final Set<String> nodes = dbCfg.getServers(involvedClusters);

    // REMOVE CURRENT NODE BECAUSE IT HAS BEEN ALREADY EXECUTED LOCALLY
    nodes.remove(localNodeName);
    return nodes;
  }

  protected void executeAsyncTx(final Set<String> nodes, final OTxTaskResult localResult, final Set<String> involvedClusters,
      final OAbstractReplicatedTask txTask, final long messageId, final String localNodeName,
      final OCallable<Void, ODistributedRequestId> afterSendCallback) {

    final OAsyncReplicationOk onAsyncReplicationOk = OExecutionThreadLocal.INSTANCE.get().onAsyncReplicationOk;
    final OAsyncReplicationError onAsyncReplicationError = storage.getAsyncReplicationError();

    // ASYNCHRONOUSLY REPLICATE IT TO ALL THE OTHER NODES
    storage.asynchronousExecution(
        new OAsynchDistributedOperation(storage.getName(), involvedClusters, nodes, txTask, messageId, localResult,
            afterSendCallback, new OCallable<Object, OPair<ODistributedRequestId, Object>>() {
          @Override
          public Object call(final OPair<ODistributedRequestId, Object> iArgument) {
            try {
              final Object value = iArgument.getValue();

              final ODistributedRequestId reqId = iArgument.getKey();

              // REMOVE HERE THE CONTEXT TO AVOID A ROLLBACK AFTER SENDING THE FINAL 2PC COMMIT
              final ODistributedTxContext ctx = localDistributedDatabase.popTxContext(reqId);
              try {

                if (value instanceof OTxTaskResult) {
                  // SEND 2-PHASE DISTRIBUTED COMMIT TX
                  final OCompleted2pcTask task = (OCompleted2pcTask) dManager.getTaskFactoryManager().getFactoryByServerNames(nodes)
                      .createTask(OCompleted2pcTask.FACTORYID);
                  task.init(reqId, true, txTask.getPartitionKey());

                  sendTxCompleted(localNodeName, involvedClusters, nodes, task);

                  if (onAsyncReplicationOk != null)
                    onAsyncReplicationOk.onAsyncReplicationOk();

                  return null;
                } else if (value instanceof Exception) {
                  try {
                    storage.executeUndoOnLocalServer(reqId, txTask);

                    if (ODistributedServerLog.isDebugEnabled())
                      ODistributedServerLog.debug(this, localNodeName, null, ODistributedServerLog.DIRECTION.NONE,
                          "Async distributed transaction failed: %s", value);

                    // SEND 2-PHASE DISTRIBUTED ROLLBACK TX
                    final OCompleted2pcTask task = (OCompleted2pcTask) dManager.getTaskFactoryManager()
                        .getFactoryByServerNames(nodes).createTask(OCompleted2pcTask.FACTORYID);
                    task.init(reqId, false, txTask.getPartitionKey());

                    sendTxCompleted(localNodeName, involvedClusters, nodes, task);

                    if (value instanceof RuntimeException)
                      throw (RuntimeException) value;
                    else
                      throw OException.wrapException(new OTransactionException("Error on execution async distributed transaction"),
                          (Exception) value);

                  } finally {
                    if (onAsyncReplicationError != null)
                      onAsyncReplicationError.onAsyncReplicationError((Throwable) value, 0);
                  }
                }
              } finally {
                if (ctx != null)
                  ctx.destroy();
              }

              // UNKNOWN RESPONSE TYPE
              if (ODistributedServerLog.isDebugEnabled())
                ODistributedServerLog.debug(this, localNodeName, null, ODistributedServerLog.DIRECTION.NONE,
                    "Async distributed transaction error, received unknown response type: %s", iArgument);

              throw new OTransactionException(
                  "Error on committing async distributed transaction, received unknown response type " + iArgument);

            } finally {
              try {
                afterSendCallback.call(iArgument.getKey());
              } catch (Exception e) {
                ODistributedServerLog.debug(this, localNodeName, null, ODistributedServerLog.DIRECTION.NONE,
                    "Error on unlocking Async distributed transaction", e);
              }
            }
          }
        }));
  }

  protected void updateUndoTaskWithCreatedRecords(final List<ORecordOperation> uResult, final List<OAbstractRemoteTask> undoTasks) {
    for (ORecordOperation op : uResult) {
      final ORecord record = op.getRecord();
      switch (op.type) {
      case ORecordOperation.CREATED:
        // ADD UNDO TASK ONCE YHE RID OS KNOWN
        undoTasks.add(new OFixCreateRecordTask().init(record));
        break;
      }
    }
  }

  protected Set<String> getInvolvedClusters(final List<ORecordOperation> uResult) {
    final Set<String> involvedClusters = new HashSet<String>();
    for (ORecordOperation op : uResult) {
      final ORecord record = op.getRecord();
      involvedClusters.add(storage.getClusterNameByRID((ORecordId) record.getIdentity()));
    }
    return involvedClusters;
  }

  protected OTxTask createTxTask(final List<ORecordOperation> uResult, final Set<String> nodes) {
    final OTxTask txTask = (OTxTask) dManager.getTaskFactoryManager().getFactoryByServerNames(nodes).createTask(OTxTask.FACTORYID);

    for (ORecordOperation op : uResult) {
      final ORecord record = op.getRecord();

      final OAbstractRecordReplicatedTask task;

      switch (op.type) {
      case ORecordOperation.CREATED:
        task = (OCreateRecordTask) dManager.getTaskFactoryManager().getFactoryByServerNames(nodes)
            .createTask(OCreateRecordTask.FACTORYID);
        task.init(record);
        break;

      case ORecordOperation.UPDATED:
        task = (OUpdateRecordTask) dManager.getTaskFactoryManager().getFactoryByServerNames(nodes)
            .createTask(OUpdateRecordTask.FACTORYID);
        task.init(record);
        break;

      case ORecordOperation.DELETED:
        task = (ODeleteRecordTask) dManager.getTaskFactoryManager().getFactoryByServerNames(nodes)
            .createTask(ODeleteRecordTask.FACTORYID);
        task.init(record);
        break;

      default:
        continue;
      }

      txTask.add(task);
    }

    return txTask;
  }

  protected OTxTaskResult createLocalTxResult(final List<ORecordOperation> uResult) {

    final OTxTaskResult localResult = new OTxTaskResult();
    for (ORecordOperation op : uResult) {
      final ORecord record = op.getRecord();

      switch (op.type) {
      case ORecordOperation.CREATED:
        localResult.results.add(new OPlaceholder((ORecordId) record.getIdentity(), record.getVersion()));
        break;

      case ORecordOperation.UPDATED:
        localResult.results.add(record.getVersion());
        break;

      case ORecordOperation.DELETED:
        localResult.results.add(Boolean.TRUE);
        break;

      default:
        continue;
      }
    }
    return localResult;
  }

  private void sendTxCompleted(final String localNodeName, final Set<String> involvedClusters, final Collection<String> nodes,
      final OCompleted2pcTask task) {
    if (nodes.isEmpty())
      // NO ACTIVE NODES TO SEND THE REQUESTS
      return;

    try {
      // SEND FINAL TX COMPLETE TASK TO UNLOCK RECORDS
      ODistributedServerLog.debug(this, localNodeName, nodes.toString(), ODistributedServerLog.DIRECTION.OUT,
          "Sending distributed end of transaction success=%s reqId=%s waitForFinalResponse=%s fixTasks=%s", task.getSuccess(),
          task.getRequestId(), SYNC_TX_COMPLETED, task.getFixTasks());

      final ODistributedResponse response = dManager
          .sendRequest(storage.getName(), involvedClusters, nodes, task, dManager.getNextMessageIdCounter(),
              SYNC_TX_COMPLETED ? EXECUTION_MODE.NO_RESPONSE : EXECUTION_MODE.NO_RESPONSE, null, null, null);

      if (SYNC_TX_COMPLETED) {
        // WAIT FOR THE RESPONSE
        final Object result = response.getPayload();
        if (!(result instanceof Boolean) || !((Boolean) result).booleanValue()) {
          // EXCEPTION: LOG IT AND ADD AS NESTED EXCEPTION
          ODistributedServerLog
              .error(this, localNodeName, null, ODistributedServerLog.DIRECTION.NONE, "Distributed transaction complete error: %s",
                  response);

        }
      }
    } catch (ODistributedException e) {
      ODistributedServerLog.warn(this, localNodeName, nodes.toString(), ODistributedServerLog.DIRECTION.OUT,
          "Distributed transaction complete error: %s", e.toString());
      throw e;
    }
  }

  /**
   * Acquires lock in block by using an optimistic approach with retry & random delay. In case any record is locked, all the lock
   * acquired so far are released before to retry.
   *
   * @throws InterruptedException
   */
  protected void acquireMultipleRecordLocks(final OTransactionInternal iTx, final ODistributedStorageEventListener eventListener,
      final ODistributedTxContext reqContext) throws InterruptedException {
    final List<ORecordId> recordsToLock = new ArrayList<ORecordId>();
    for (ORecordOperation op : iTx.getRecordOperations()) {
      recordsToLock.add((ORecordId) op.record.getIdentity());
    }

    acquireMultipleRecordLocks(this, dManager, recordsToLock, eventListener, reqContext, -1);
  }

  public static void acquireMultipleRecordLocks(final Object iThis, final ODistributedServerManager dManager,
      final List<ORecordId> recordsToLock, final ODistributedStorageEventListener eventListener,
      final ODistributedTxContext reqContext, final long timeout) throws InterruptedException {

    // CREATE A SORTED LIST OF RID TO AVOID DEADLOCKS
    Collections.sort(recordsToLock);

    ORecordId lastRecordCannotLock = null;
    ODistributedRequestId lastLockHolder = null;

    final long begin = System.currentTimeMillis();

    final int maxAutoRetry = OGlobalConfiguration.DISTRIBUTED_CONCURRENT_TX_MAX_AUTORETRY.getValueAsInteger();
    final int autoRetryDelay = OGlobalConfiguration.DISTRIBUTED_CONCURRENT_TX_AUTORETRY_DELAY.getValueAsInteger();

    // ACQUIRE ALL THE LOCKS ON RECORDS ON LOCAL NODE BEFORE TO PROCEED
    for (int retry = 1; retry <= maxAutoRetry; ++retry) {
      lastRecordCannotLock = null;
      lastLockHolder = null;

      for (ORecordId rid : recordsToLock) {
        try {
          reqContext.lock(rid, timeout);
        } catch (ODistributedRecordLockedException e) {
          // LOCKED, UNLOCK ALL THE PREVIOUS LOCKED AND RETRY IN A WHILE
          lastRecordCannotLock = rid;
          lastLockHolder = e.getLockHolder();
          reqContext.unlock();

          if (autoRetryDelay > -1 && retry + 1 <= maxAutoRetry)
            Thread.sleep(autoRetryDelay / 2 + new Random().nextInt(autoRetryDelay));

          ODistributedServerLog.debug(iThis, dManager.getLocalNodeName(), null, ODistributedServerLog.DIRECTION.NONE,
              "Distributed transaction: %s cannot lock records %s because owned by %s (retry %d/%d, thread=%d)",
              reqContext.getReqId(), recordsToLock, lastLockHolder, retry, maxAutoRetry, Thread.currentThread().getId());

          break;
        }
      }

      if (lastRecordCannotLock == null) {
        if (eventListener != null)
          for (ORecordId rid : recordsToLock)
            try {
              eventListener.onAfterRecordLock(rid);
            } catch (Exception t) {
              // IGNORE IT
              ODistributedServerLog.error(iThis, dManager.getLocalNodeName(), null, ODistributedServerLog.DIRECTION.NONE,
                  "Caught exception during ODistributedStorageEventListener.onAfterRecordLock", t);
            }

        // LOCKED: EXIT FROM RETRY LOOP
        break;
      }
    }

    if (lastRecordCannotLock != null) {
      // localDistributedDatabase.dumpLocks();
      throw new ODistributedRecordLockedException(dManager.getLocalNodeName(), lastRecordCannotLock, lastLockHolder,
          System.currentTimeMillis() - begin);
    }
  }

  /**
   * Create undo content for distributed 2-phase rollback. This list of undo tasks is sent to all the nodes to revert a transaction
   * and it's also applied locally.
   *
   * @param iTx       Current transaction
   * @param database
   * @param requestId
   *
   * @return List of remote undo tasks
   */
  protected List<OAbstractRemoteTask> createUndoTasksFromTx(final OTransactionInternal iTx, final ODistributedDatabase database,
      final ODistributedRequestId requestId) {
    final List<OAbstractRemoteTask> undoTasks = new ArrayList<OAbstractRemoteTask>();
    for (final ORecordOperation op : iTx.getRecordOperations()) {
      OAbstractRemoteTask undoTask = null;

      final ORecord record = op.getRecord();

      switch (op.type) {
      case ORecordOperation.CREATED:
        // CREATE UNDO TASK LATER ONCE THE RID HAS BEEN ASSIGNED
        break;

      case ORecordOperation.UPDATED:
      case ORecordOperation.DELETED:
        // CREATE UNDO TASK WITH THE PREVIOUS RECORD CONTENT/VERSION
        final ORecordId rid = (ORecordId) record.getIdentity();

        final AtomicReference<ORecord> previousRecord = new AtomicReference<ORecord>();
        OScenarioThreadLocal.executeAsDefault(new Callable<Object>() {
          @Override
          public Object call() throws Exception {
            final ODatabaseDocumentInternal db = ODatabaseRecordThreadLocal.instance().get();
            final ORecordOperation txEntry = db.getTransaction().getRecordEntry(rid);

            final ORecord record;

            if (txEntry != null && txEntry.type == ORecordOperation.DELETED)
              // GET DELETED RECORD FROM TX
              record = txEntry.getRecord();
            else {
              final ORawBuffer loadedBuffer = database.getRecordIfLocked(rid);
              if (loadedBuffer != null) {
                // LOAD THE RECORD FROM THE STORAGE AVOIDING USING THE DB TO GET THE TRANSACTIONAL CHANGES
                record = Orient.instance().getRecordFactoryManager().newInstance(loadedBuffer.recordType);
                ORecordInternal.fill(record, rid, loadedBuffer.version, loadedBuffer.getBuffer(), false);
                previousRecord.set(record);
              } else
                // RECORD NOT FOUND ON LOCAL STORAGE, ASK TO DB BECAUSE IT COULD BE SHARDED AND RESIDE ON ANOTHER SERVER
                record = db.load(rid);
            }

            ODistributedServerLog.debug(this, dManager.getLocalNodeName(), null, ODistributedServerLog.DIRECTION.NONE,
                "Setting previous record for undo: %s (reqId=%s op=%s)", record, requestId, op);

            previousRecord.set(record);

            return null;
          }
        });

        if (previousRecord.get() == null)
          throw new ORecordNotFoundException(rid);

        if (op.type == ORecordOperation.UPDATED)
          undoTask = new OFixUpdateRecordTask()
              .init(previousRecord.get(), ORecordVersionHelper.clearRollbackMode(previousRecord.get().getVersion()));
        else
          undoTask = new OResurrectRecordTask().init(previousRecord.get());
        break;

      default:
        continue;
      }

      if (undoTask != null)
        undoTasks.add(undoTask);
    }
    return undoTasks;

  }

  protected void processCommitResult(final String localNodeName, final OTransactionInternal iTx, final OTxTask txTask,
      final Set<String> involvedClusters, final Iterable<ORecordOperation> tmpEntries, final Collection<String> nodes,
      final ODistributedRequestId reqId, final ODistributedResponse dResponse) throws InterruptedException {
    final Object result = dResponse.getPayload();

    if (result instanceof OTxTaskResult) {
      final OTxTaskResult txResult = (OTxTaskResult) result;

      final List<Object> list = txResult.results;

      for (int i = 0; i < txTask.getTasks().size(); ++i) {
        final Object o = list.get(i);

        final OAbstractRecordReplicatedTask task = txTask.getTasks().get(i);

        if (!OAbstract2pcTask.NON_LOCAL_CLUSTER.equals(o))
          if (task instanceof OCreateRecordTask) {
            final OCreateRecordTask t = (OCreateRecordTask) task;
            iTx.updateIdentityAfterCommit(t.getRid(), ((OPlaceholder) o).getIdentity());
            final ORecord rec = iTx.getRecord(t.getRid());
            if (rec != null)
              ORecordInternal.setVersion(rec, ((OPlaceholder) o).getVersion());
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

      if (ODistributedServerLog.isDebugEnabled())
        ODistributedServerLog
            .debug(this, localNodeName, null, ODistributedServerLog.DIRECTION.NONE, "Distributed transaction %s completed", reqId);

      // SEND THE 2PC ONLY TO THE SERVERS WITH THE SAME RESULT AS THE WINNING RESPONSE
      final Collection<String> conflictServers = dResponse.getDistributedResponseManager().getConflictServers();

      final Set<String> serverWithoutFollowup = dResponse.getDistributedResponseManager().getServersWithoutFollowup();

      final Collection<String> okNodes;
      if (conflictServers.isEmpty())
        okNodes = serverWithoutFollowup;
      else {
        okNodes = new ArrayList<String>(serverWithoutFollowup);
        okNodes.removeAll(conflictServers);
      }

      // EXCLUDE LOCAL SERVER
      okNodes.remove(localNodeName);

      if (!okNodes.isEmpty()) {
        for (String node : okNodes)
          dResponse.getDistributedResponseManager().addFollowupToServer(node);

        ODistributedServerLog.debug(this, localNodeName, okNodes.toString(), ODistributedServerLog.DIRECTION.OUT,
            "Distributed transaction %s sending 2pc: pending/conflicting servers are %s", reqId, conflictServers.toString());

        // DO NOT REMOVE THE CONTEXT WAITING FOR ANY PENDING RESPONSE
        final OCompleted2pcTask twopcTask = (OCompleted2pcTask) dManager.getTaskFactoryManager().getFactoryByServerNames(okNodes)
            .createTask(OCompleted2pcTask.FACTORYID);
        twopcTask.init(reqId, true, txTask.getPartitionKey());
        sendTxCompleted(localNodeName, involvedClusters, okNodes, twopcTask);
      }

    } else if (result instanceof ODistributedRecordLockedException) {
      if (ODistributedServerLog.isDebugEnabled())
        ODistributedServerLog.debug(this, localNodeName, nodes.toString(), ODistributedServerLog.DIRECTION.IN,
            "Distributed transaction %s error: record %s is locked by %s", reqId,
            ((ODistributedRecordLockedException) result).getRid(), ((ODistributedRecordLockedException) result).getLockHolder());

      throw (ODistributedRecordLockedException) result;

    } else if (result instanceof Exception) {
      // EXCEPTION: LOG IT AND ADD AS NESTED EXCEPTION
      if (ODistributedServerLog.isDebugEnabled())
        ODistributedServerLog.debug(this, localNodeName, nodes.toString(), ODistributedServerLog.DIRECTION.IN,
            "Distributed transaction %s received error: %s", reqId, result, result.toString());

      // LET TO THE CALLER TO UNDO IT
      if (result instanceof OTransactionException || result instanceof ONeedRetryException)
        throw (RuntimeException) result;

      if (result instanceof ORecordNotFoundException) {
        // REPAIR THE RECORD IMMEDIATELY
        localDistributedDatabase.getDatabaseRepairer().repairRecord((ORecordId) ((ORecordNotFoundException) result).getRid());

        throw (ORecordNotFoundException) result;
      }

      throw OException.wrapException(new OTransactionException("Error on committing distributed transaction"), (Exception) result);

    } else {
      // UNKNOWN RESPONSE TYPE
      if (ODistributedServerLog.isDebugEnabled())
        ODistributedServerLog.info(this, localNodeName, nodes.toString(), ODistributedServerLog.DIRECTION.IN,
            "Distributed transaction %s error, received unknown response type: %s", reqId, result);

      throw new OTransactionException("Error on committing distributed transaction, received unknown response type " + result);
    }
  }

  /**
   * Finalizes the request only if in asynchronous mode.
   */
  private boolean finalizeRequest(final ODistributedResponseManager resp, final String localNodeName,
      final Set<String> involvedClusters, final OTxTask txTask) {

    return resp.executeInLock(new OCallable<Boolean, ODistributedResponseManager>() {
      @Override
      public Boolean call(final ODistributedResponseManager resp) {
        if (resp.isSynchronousWaiting())
          // SYNCHRONOUS RESPONSE STILL PENDING, IT WILL MANAGE THE END OF THE RESPONSE
          return false;

        // CALLBACK IN CASE OF TIMEOUT AND COMPLETION OF RESPONSE
        final Set<String> serversToFollowup = resp.getServersWithoutFollowup();
        serversToFollowup.remove(dManager.getLocalNodeName());

        if (!serversToFollowup.isEmpty()) {
          final ODistributedResponse quorumResponse = resp.getQuorumResponse();

          if (ODistributedServerLog.isDebugEnabled())
            ODistributedServerLog.debug(this, localNodeName, serversToFollowup.toString(), ODistributedServerLog.DIRECTION.OUT,
                "Distributed transaction completed (quorum=%d winnerResponse=%s responses=%s reqId=%s), servers %s need a followup message",
                resp.getQuorum(), quorumResponse, resp.getRespondingNodes(), resp.getMessageId(), serversToFollowup);

          if (quorumResponse == null) {
            // NO QUORUM REACHED: SEND ROLLBACK TO ALL THE MISSING NODES
            try {
              final OCompleted2pcTask twopcTask = (OCompleted2pcTask) dManager.getTaskFactoryManager()
                  .getFactoryByServerNames(serversToFollowup).createTask(OCompleted2pcTask.FACTORYID);
              twopcTask.init(resp.getMessageId(), false, txTask.getPartitionKey());

              sendTxCompleted(localNodeName, involvedClusters, serversToFollowup, twopcTask);
              return true;

            } finally {
              // REPAIR RECORD: THIS IS NEEDED BECAUSE IN CASE OF LOCK A CONTINUE RETRY IS NEEDED

              // TODO: IN SOME CASES, LIKE WHEN THE RESPONSE IS LOCK EXCEPTION, THE REPAIR COULD BE AVOIDED
              final List<ORecordId> involvedRecords = txTask.getInvolvedRecords();

              ODistributedServerLog.debug(this, localNodeName, serversToFollowup.toString(), ODistributedServerLog.DIRECTION.OUT,
                  "Distributed transaction found servers %s not in quorum, schedule a repair records for %s (reqId=%s)",
                  serversToFollowup, involvedRecords, resp.getMessageId());

              localDistributedDatabase.getDatabaseRepairer().enqueueRepairRecords(involvedRecords);
            }
          }

          // QUORUM REACHED
          for (String s : serversToFollowup) {
            resp.addFollowupToServer(s);

            final Object serverResponse = resp.getResponseFromServer(s);

            if (quorumResponse.equals(serverResponse)) {
              // SEND COMMIT
              ODistributedServerLog.debug(this, localNodeName, s, ODistributedServerLog.DIRECTION.OUT,
                  "Sending 2pc message for distributed transaction (reqId=%s)", s, resp.getMessageId());

              final OCompleted2pcTask twopcTask = (OCompleted2pcTask) dManager.getTaskFactoryManager()
                  .getFactoryByServerNames(serversToFollowup).createTask(OCompleted2pcTask.FACTORYID);
              twopcTask.init(resp.getMessageId(), true, txTask.getPartitionKey());

              try {
                sendTxCompleted(localNodeName, involvedClusters, OMultiValue.getSingletonList(s), twopcTask);
              } catch (Exception e) {
                // SWALLOW THE EXCEPTION AND CONTINUE
                ODistributedServerLog.debug(this, localNodeName, s, ODistributedServerLog.DIRECTION.OUT,
                    "Error on sending 2pc message for distributed transaction (reqId=%s)", e, resp.getMessageId());
              }

            } else {
              // TRY TO FIX REMOTE NODE
              final Object serverResponsePayload = serverResponse instanceof ODistributedResponse ?
                  ((ODistributedResponse) serverResponse).getPayload() :
                  serverResponse;

              final ORemoteTask fixTask = txTask
                  .getFixTask(resp.getRequest(), null, serverResponsePayload, quorumResponse.getPayload(), s, dManager);

              if (fixTask != null) {
                // SEND THE FIX
                try {
                  sendTxCompleted(localNodeName, involvedClusters, OMultiValue.getSingletonList(s), (OCompleted2pcTask) fixTask);
                  return true;
                } catch (Exception t) {
                  // GO FOR ROLLBACK + REPAIR
                }
              }

              // NO FIX AVAILABLE, JUST ROLLBACK THE TX AND EXECUTE A REPAIR IMMEDIATELY
              final OCompleted2pcTask twopcTask = (OCompleted2pcTask) dManager.getTaskFactoryManager()
                  .getFactoryByServerNames(serversToFollowup).createTask(OCompleted2pcTask.FACTORYID);
              twopcTask.init(resp.getMessageId(), false, txTask.getPartitionKey());

              sendTxCompleted(localNodeName, involvedClusters, OMultiValue.getSingletonList(s), twopcTask);

              // SCHEDULE A REPAIR IN CASE THE FIX IS NOT EXECUTED
              final List<ORecordId> involvedRecords = txTask.getInvolvedRecords();

              ODistributedServerLog.debug(this, localNodeName, serversToFollowup.toString(), ODistributedServerLog.DIRECTION.OUT,
                  "Distributed transaction found servers %s not in quorum, schedule a repair records for %s (reqId=%s)",
                  serversToFollowup, involvedRecords, resp.getMessageId());

              localDistributedDatabase.getDatabaseRepairer().repairRecords(involvedRecords);
            }
          }
        }

        return true;
      }
    });
  }
}
