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
package com.orientechnologies.orient.server.distributed.impl;

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
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OPlaceholder;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.exception.OTransactionException;
import com.orientechnologies.orient.core.exception.OValidationException;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.ORecordVersionHelper;
import com.orientechnologies.orient.core.replication.OAsyncReplicationError;
import com.orientechnologies.orient.core.replication.OAsyncReplicationOk;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import com.orientechnologies.orient.core.storage.OStorageOperationResult;
import com.orientechnologies.orient.core.tx.OTransaction;
import com.orientechnologies.orient.core.tx.OTransactionAbstract;
import com.orientechnologies.orient.core.tx.OTransactionInternal;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.*;
import com.orientechnologies.orient.server.distributed.ODistributedRequest.EXECUTION_MODE;
import com.orientechnologies.orient.server.distributed.impl.task.*;
import com.orientechnologies.orient.server.distributed.task.*;

import java.util.*;
import java.util.concurrent.Callable;

/**
 * Distributed transaction manager.
 *
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 */
public class ODistributedTransactionManager {
  private final OServer                   serverInstance;
  private final ODistributedServerManager dManager;
  private final ODistributedStorage       storage;
  private final ODistributedDatabase      localDistributedDatabase;

  public ODistributedTransactionManager(final ODistributedStorage storage, final OServer iServer,
      final ODistributedDatabase iDDatabase) {
    this.serverInstance = iServer;
    this.dManager = iServer.getDistributedManager();
    this.storage = storage;
    this.localDistributedDatabase = iDDatabase;
  }

  public List<ORecordOperation> commit(final ODatabaseDocumentTx database, final OTransaction iTx, final Runnable callback,
      final ODistributedStorageEventListener eventListener) {
    final ODistributedConfiguration dbCfg = dManager.getDatabaseConfiguration(storage.getName());

    final String localNodeName = dManager.getLocalNodeName();

    try {
      OTransactionInternal.setStatus((OTransactionAbstract) iTx, OTransaction.TXSTATUS.BEGUN);

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

      final ODistributedRequestId requestId = new ODistributedRequestId(dManager.getLocalNodeId(),
          dManager.getNextMessageIdCounter());

      final ODistributedTxContext ctx = localDistributedDatabase.registerTxContext(requestId);

      return (List<ORecordOperation>) storage.executeOperationInLock(exclusiveLock, new OCallable<Object, Void>() {

        @Override
        public Object call(final Void nothing) {
          try {
            acquireMultipleRecordLocks(iTx, maxAutoRetry, autoRetryDelay, eventListener, ctx);

            final List<ORecordOperation> uResult = (List<ORecordOperation>) OScenarioThreadLocal
                .executeAsDistributed(new Callable() {
              @Override
              public Object call() throws Exception {
                return storage.commit(iTx, callback);
              }
            });

            // REMOVE THE TX OBJECT FROM DATABASE TO AVOID UND OPERATIONS ARE "LOST IN TRANSACTION"
            database.setDefaultTransactionMode();

            // After commit force the clean of dirty managers due to possible copy and miss clean.
            for (ORecordOperation ent : iTx.getAllRecordEntries()) {
              ORecordInternal.getDirtyManager(ent.getRecord()).clear();
            }

            final Set<String> involvedClusters = getInvolvedClusters(uResult);
            final Set<String> nodes = getAvailableNodesButLocal(dbCfg, involvedClusters, localNodeName);
            if (nodes.isEmpty()) {
              // NO FURTHER NODES TO INVOLVE
              ctx.destroy();
              return null;
            }

            updateUndoTaskWithCreatedRecords(uResult, undoTasks);

            final OTxTaskResult localResult = createLocalTxResult(uResult);

            final OTxTask txTask = createTxTask(uResult);
            txTask.setLocalUndoTasks(undoTasks);

            OTransactionInternal.setStatus((OTransactionAbstract) iTx, OTransaction.TXSTATUS.COMMITTING);

            if (finalExecutionModeSynch) {
              // SYNCHRONOUS, AUTO-RETRY IN CASE RECORDS ARE LOCKED
              ODistributedResponse lastResult = null;
              for (int retry = 1; retry <= maxAutoRetry; ++retry) {
                boolean isLastRetry = maxAutoRetry == retry;
                // SYNCHRONOUS CALL: REPLICATE IT
                lastResult = dManager.sendRequest(storage.getName(), involvedClusters, nodes, txTask, requestId.getMessageId(),
                    EXECUTION_MODE.RESPONSE, localResult, null);
                if (!processCommitResult(localNodeName, iTx, txTask, involvedClusters, uResult, nodes, autoRetryDelay,
                    lastResult.getRequestId(), lastResult, isLastRetry)) {

                  // RETRY
                  continue;
                }

                ODistributedServerLog.debug(this, localNodeName, null, ODistributedServerLog.DIRECTION.NONE,
                    "Distributed transaction succeeded. Tasks: %s", txTask.getTasks());

                // OK, DISTRIBUTED COMMIT SUCCEED
                return null;
              }

              // ONLY CASE: ODistributedRecordLockedException MORE THAN AUTO-RETRY
              ODistributedServerLog.debug(this, localNodeName, null, ODistributedServerLog.DIRECTION.NONE,
                  "Distributed transaction retries exceed maximum auto-retries (%d). Task: %s - Payload: %s - Tasks: %s",
                  maxAutoRetry, txTask, txTask.getPayload(), txTask.getTasks());

              // ROLLBACK TX
              storage.executeUndoOnLocalServer(requestId, txTask);
              sendTxCompleted(localNodeName, involvedClusters, nodes, lastResult.getRequestId(), false);

              throw (ODistributedRecordLockedException) lastResult.getPayload();

            } else {
              // ASYNC, MANAGE REPLICATION CALLBACK
              final OCallable<Void, ODistributedRequestId> unlockCallback = new OCallable<Void, ODistributedRequestId>() {
                @Override
                public Void call(final ODistributedRequestId reqId) {
                  // FREE THE CONTEXT
                  ctx.destroy();
                  return null;
                }
              };

              executeAsyncTx(nodes, localResult, involvedClusters, txTask, requestId.getMessageId(), localNodeName, unlockCallback);
            }

          } catch (RuntimeException e) {
            ctx.destroy();
            throw e;
          } catch (Exception e) {
            ctx.destroy();
            OException.wrapException(new ODistributedException("Cannot commit transaction"), e);
            // UNREACHABLE
          } finally {
            if (finalExecutionModeSynch)
              ctx.destroy();
          }
          return null;
        }
      });

    } catch (OValidationException e) {
      throw e;
    } catch (Exception e) {
      storage.handleDistributedException("Cannot route TX operation against distributed node", e);
    }

    return null;
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
    storage.asynchronousExecution(new OAsynchDistributedOperation(storage.getName(), involvedClusters, nodes, txTask, messageId,
        localResult, afterSendCallback, new OCallable<Object, OPair<ODistributedRequestId, Object>>() {
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
                  storage.executeUndoOnLocalServer(reqId, txTask);

                  if (ODistributedServerLog.isDebugEnabled())
                    ODistributedServerLog.debug(this, localNodeName, null, ODistributedServerLog.DIRECTION.NONE,
                        "Async distributed transaction failed: %s", value);

                  // SEND 2-PHASE DISTRIBUTED ROLLBACK TX
                  sendTxCompleted(localNodeName, involvedClusters, nodes, reqId, false);

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
        undoTasks.add(new ODeleteRecordTask(record));
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

  protected OTxTask createTxTask(final List<ORecordOperation> uResult) {
    final OTxTask txTask = new OTxTask();

    for (ORecordOperation op : uResult) {
      final ORecord record = op.getRecord();

      final OAbstractRecordReplicatedTask task;

      switch (op.type) {
      case ORecordOperation.CREATED:
        task = new OCreateRecordTask(record);
        break;

      case ORecordOperation.UPDATED:
        task = new OUpdateRecordTask(record);
        break;

      case ORecordOperation.DELETED:
        task = new ODeleteRecordTask(record);
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
      final ODistributedRequestId reqId, final boolean success) {
    // SEND FINAL TX COMPLETE TASK TO UNLOCK RECORDS
    final Object completedResult = dManager.sendRequest(storage.getName(), involvedClusters, nodes,
        new OCompletedTxTask(reqId, success), dManager.getNextMessageIdCounter(), EXECUTION_MODE.RESPONSE, null, null).getPayload();

    if (!(completedResult instanceof Boolean) || !((Boolean) completedResult).booleanValue()) {
      // EXCEPTION: LOG IT AND ADD AS NESTED EXCEPTION
      ODistributedServerLog.error(this, localNodeName, null, ODistributedServerLog.DIRECTION.NONE,
          "Distributed transaction complete error: %s", completedResult);
    }
  }

  /**
   * Acquires lock in block by using an optimistic approach with retry & random delay. In case any record is locked, all the lock
   * acquired so far are released before to retry.
   * 
   * @throws InterruptedException
   */
  protected void acquireMultipleRecordLocks(final OTransaction iTx, final int maxAutoRetry, final int autoRetryDelay,
      final ODistributedStorageEventListener eventListener, final ODistributedTxContext reqContext) throws InterruptedException {

    final List<ORecordId> recordsToLock = new ArrayList<ORecordId>();
    for (ORecordOperation op : iTx.getAllRecordEntries()) {
      recordsToLock.add((ORecordId) op.record.getIdentity());
    }

    ORecordId lastRecordCannotLock = null;
    ODistributedRequestId lockHolder = null;

    // ACQUIRE ALL THE LOCKS ON RECORDS ON LOCAL NODE BEFORE TO PROCEED
    for (int retry = 1; retry <= maxAutoRetry; ++retry) {
      lastRecordCannotLock = null;
      lockHolder = null;

      for (ORecordId rid : recordsToLock) {
        try {
          reqContext.lock(rid);
          if (eventListener != null) {
            try {
              eventListener.onAfterRecordLock(rid);
            } catch (Throwable t) {
              // IGNORE IT
              ODistributedServerLog.error(this, dManager.getLocalNodeName(), null, ODistributedServerLog.DIRECTION.NONE,
                  "Caught exception during ODistributedStorageEventListener.onAfterRecordLock", t);
            }
          }
        } catch (ODistributedRecordLockedException e) {
          // LOCKED, UNLOCK ALL AND RETRY IN A WHILE
          lastRecordCannotLock = rid;
          lockHolder = e.getLockHolder();

          reqContext.unlock();

          if (autoRetryDelay > -1 && retry + 1 <= maxAutoRetry)
            Thread.sleep(autoRetryDelay / 2 + new Random().nextInt(autoRetryDelay));

          ODistributedServerLog.debug(this, dManager.getLocalNodeName(), null, ODistributedServerLog.DIRECTION.NONE,
              "Distributed transaction: cannot lock records %s (retry %d/%d)", recordsToLock, retry, maxAutoRetry);

          break;
        }
      }

      if (lastRecordCannotLock == null)
        // LOCKED
        break;
    }

    if (lastRecordCannotLock != null)
      throw new ODistributedRecordLockedException(lastRecordCannotLock, lockHolder);
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

      case ORecordOperation.UPDATED:
      case ORecordOperation.DELETED:
        // CREATE UNDO TASK WITH THE PREVIOUS RECORD CONTENT/VERSION
        final ORecordId rid = (ORecordId) record.getIdentity();
        final OStorageOperationResult<ORawBuffer> loaded = ODatabaseRecordThreadLocal.INSTANCE.get().getStorage().getUnderlying()
            .readRecord(rid, null, true, null);

        if (loaded == null || loaded.getResult() == null)
          throw new ORecordNotFoundException(rid);

        final ORecord previousRecord = Orient.instance().getRecordFactoryManager().newInstance(loaded.getResult().recordType);
        ORecordInternal.fill(previousRecord, rid, loaded.getResult().version, loaded.getResult().getBuffer(), false);

        if (op.type == ORecordOperation.UPDATED)
          undoTask = new OUpdateRecordTask(previousRecord, ORecordVersionHelper.clearRollbackMode(previousRecord.getVersion()));
        else
          undoTask = new OResurrectRecordTask(previousRecord);
        break;

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
      final int autoRetryDelay, final ODistributedRequestId reqId, final ODistributedResponse dResponse, final boolean isLastRetry)
          throws InterruptedException {
    final Object result = dResponse.getPayload();

    if (result instanceof OTxTaskResult) {
      final OTxTaskResult txResult = (OTxTaskResult) result;

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

      if (ODistributedServerLog.isDebugEnabled())
        ODistributedServerLog.debug(this, localNodeName, null, ODistributedServerLog.DIRECTION.NONE,
            "Distributed transaction %s completed", reqId);

      sendTxCompleted(localNodeName, involvedClusters, nodes, reqId, true);

    } else if (result instanceof ODistributedRecordLockedException) {
      // AUTO RETRY

      if (ODistributedServerLog.isDebugEnabled())
        ODistributedServerLog.debug(this, localNodeName, null, ODistributedServerLog.DIRECTION.NONE,
            "Distributed transaction %s error: record %s is locked", reqId, ((ODistributedRecordLockedException) result).getRid());

      // ctx.unlock();

      // if this the the last retry (and it failed), we don't need to wait anymore
      if (autoRetryDelay > 0 && !isLastRetry)
        Thread.sleep(autoRetryDelay);

      // acquireMultipleRecordLocks(iTx, maxAutoRetry, autoRetryDelay, eventListener, ctx);

      // RETRY
      return false;

    } else if (result instanceof Exception) {
      // EXCEPTION: LOG IT AND ADD AS NESTED EXCEPTION
      if (ODistributedServerLog.isDebugEnabled())
        ODistributedServerLog.debug(this, localNodeName, null, ODistributedServerLog.DIRECTION.NONE,
            "Distributed transaction %s error: %s", reqId, result, result.toString());

      // ROLLBACK TX NOT TO SEND TX COMPLETED BECAUSE ALREADY UNDO
      storage.executeUndoOnLocalServer(dResponse.getRequestId(), txTask);

      if (result instanceof OTransactionException || result instanceof ONeedRetryException)
        throw (RuntimeException) result;

      throw OException.wrapException(new OTransactionException("Error on committing distributed transaction"), (Exception) result);

    } else {
      // UNKNOWN RESPONSE TYPE
      if (ODistributedServerLog.isDebugEnabled())
        ODistributedServerLog.info(this, localNodeName, null, ODistributedServerLog.DIRECTION.NONE,
            "Distributed transaction %s error, received unknown response type: %s", reqId, result);

      // ROLLBACK TX
      storage.executeUndoOnLocalServer(dResponse.getRequestId(), txTask);
      sendTxCompleted(localNodeName, involvedClusters, nodes, dResponse.getRequestId(), false);

      throw new OTransactionException("Error on committing distributed transaction, received unknown response type " + result);
    }
    return true;
  }
}
