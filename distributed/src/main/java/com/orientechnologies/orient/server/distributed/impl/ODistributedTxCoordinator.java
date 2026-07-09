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

import com.orientechnologies.common.concur.lock.OInterruptedException;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.log.OLogger;
import com.orientechnologies.orient.core.exception.OConcurrentCreateException;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.storage.ORecordDuplicatedException;
import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import com.orientechnologies.orient.core.tx.OTransaction;
import com.orientechnologies.orient.core.tx.OTransactionInternal;
import com.orientechnologies.orient.distributed.context.retryable.ORetryInfo;
import com.orientechnologies.orient.server.distributed.ODistributedDatabase;
import com.orientechnologies.orient.server.distributed.ODistributedRequestId;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.distributed.ODistributedTxContext;
import com.orientechnologies.orient.server.distributed.impl.lock.OLockGuard;
import com.orientechnologies.orient.server.distributed.impl.task.OLockKeySource;
import com.orientechnologies.orient.server.distributed.impl.task.OTransactionPhase1Task;
import com.orientechnologies.orient.server.distributed.impl.task.OTransactionPhase2Task;
import com.orientechnologies.orient.server.distributed.impl.task.transaction.OTransactionResultPayload;
import com.orientechnologies.orient.server.distributed.impl.task.transaction.OTxConcurrentCreation;
import com.orientechnologies.orient.server.distributed.impl.task.transaction.OTxConcurrentModification;
import com.orientechnologies.orient.server.distributed.impl.task.transaction.OTxException;
import com.orientechnologies.orient.server.distributed.impl.task.transaction.OTxInvalidSequential;
import com.orientechnologies.orient.server.distributed.impl.task.transaction.OTxKeyLockTimeout;
import com.orientechnologies.orient.server.distributed.impl.task.transaction.OTxRecordLockTimeout;
import com.orientechnologies.orient.server.distributed.impl.task.transaction.OTxSuccess;
import com.orientechnologies.orient.server.distributed.impl.task.transaction.OTxUniqueIndex;
import com.orientechnologies.orient.server.distributed.task.ODistributedKeyLockedException;
import com.orientechnologies.orient.server.distributed.task.ODistributedOperationException;
import com.orientechnologies.orient.server.distributed.task.ODistributedRecordLockedException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public class ODistributedTxCoordinator {
  private static final OLogger logger =
      OLogManager.instance().logger(ODistributedTxCoordinator.class);
  public static final String LOCAL_RESULT_SUCCESS = "OK";

  private final ODistributedServerManager dManager;
  private final ODistributedDatabaseImpl localDistributedDatabase;
  private ODistributedTxResponseManager responseManager;
  // ID and name of the node where this tx coordinator is running
  private final String nodeName;
  private final int maxRetries;
  private final int retryDelay;
  private final String dbName;

  public ODistributedTxCoordinator(
      final String dbName,
      final ODistributedServerManager manager,
      final ODistributedDatabase iDDatabase,
      String nodeName,
      int maxRetries,
      int retryDelay) {
    this.dManager = manager;
    this.dbName = dbName;
    this.localDistributedDatabase = (ODistributedDatabaseImpl) iDDatabase;
    this.nodeName = nodeName;
    this.maxRetries = maxRetries;
    this.retryDelay = retryDelay;
  }

  public void commit(final ODatabaseDocumentDistributed database, final OTransactionInternal iTx) {
    var retry = new ORetryInfo(maxRetries, retryDelay);
    do {
      final ODistributedRequestId requestId = database.getContext().nextRequestId();
      localDistributedDatabase.startOperation();
      var transactionSequence = database.getSharedContext().getTransactionSequence();
      try {
        Optional<OTransactionIdPromise> genId = transactionSequence.next();
        if (genId.isPresent()) {
          OTransactionIdPromise txId = genId.get();
          tryCommit(database, iTx, txId, requestId);
          return;
        } else {
          var nextWait = retry.nextRetry();
          if (nextWait.isPresent()) {
            try {
              Thread.sleep(nextWait.get());
            } catch (InterruptedException e) {
              OException.wrapException(new OInterruptedException(e.getMessage()), e);
            }
          } else {
            throw new ODistributedOperationException("Reached limit of retry to commit");
          }
        }
      } catch (OConcurrentCreateException
          | ODistributedRecordLockedException
          | ODistributedKeyLockedException
          | OInvalidSequentialException ex) {

        if (ex instanceof OConcurrentCreateException) {
          iTx.resetAllocatedIds();
        }

        // Nothing just retry
        var nextWait = retry.nextRetry();
        if (nextWait.isPresent()) {
          try {
            Thread.sleep(nextWait.get());
          } catch (InterruptedException e) {
            OException.wrapException(new OInterruptedException(e.getMessage()), e);
          }
        } else {
          destroyContext(requestId);
          throw ex;
        }

      } catch (RuntimeException | Error ex) {
        destroyContext(requestId);
        throw ex;
      } finally {
        localDistributedDatabase.endOperation();
      }
    } while (true);
  }

  private void destroyContext(final ODistributedRequestId requestId) {
    ODistributedTxContext context = localDistributedDatabase.getTxContext(requestId);
    if (context != null) {
      context.destroy();
    }
  }

  public void tryCommit(
      final ODatabaseDocumentDistributed database,
      final OTransactionInternal iTx,
      OTransactionIdPromise txId,
      final ODistributedRequestId requestId) {

    iTx.setStatus(OTransaction.TXSTATUS.BEGUN);

    OLocalKeySource keySource = new OLocalKeySource(txId.getId(), iTx, database);
    List<OLockGuard> guards = localDistributedDatabase.localLock(keySource);
    OTransactionResultPayload localResult;

    try {
      // This retry happen only the first time i try to lock on local server
      localResult =
          OTransactionPhase1Task.executeTransaction(requestId, txId, database, iTx, true, -1);
    } finally {
      localDistributedDatabase.localUnlock(guards);
    }

    if (localResult.getResponseType() == OTxRecordLockTimeout.ID) {
      localDistributedDatabase.popTxContext(requestId).destroy();
      throw new ODistributedRecordLockedException(
          nodeName, ((OTxRecordLockTimeout) localResult).getLockedId());
    }
    if (localResult.getResponseType() == OTxKeyLockTimeout.ID) {
      localDistributedDatabase.popTxContext(requestId).destroy();
      throw new ODistributedKeyLockedException(
          nodeName, ((OTxKeyLockTimeout) localResult).getKey());
    }
    Set<String> nodes = database.getContext().getAvailableNodeNotLocalNames(dbName);
    if (nodes.isEmpty()) {
      switch (localResult.getResponseType()) {
        case OTxSuccess.ID:
          // Success send ok
          localOk(requestId, database, keySource);
          break;
        case OTxException.ID:
          // Exception send ko and throws the exception
          localKo(requestId, database, keySource);
          throw ((OTxException) localResult).getException();
        case OTxUniqueIndex.ID:
          {
            // Unique index quorum error send ko and throw unique index exception
            localKo(requestId, database, keySource);
            ORID id = ((OTxUniqueIndex) localResult).getRecordId();
            String index = ((OTxUniqueIndex) localResult).getIndex();
            Object key = ((OTxUniqueIndex) localResult).getKey();
            throw new ORecordDuplicatedException(
                String.format(
                    "Cannot index record %s: found duplicated key '%s' in index '%s' ",
                    id, key, index),
                index,
                id,
                key);
          }
        case OTxConcurrentModification.ID:
          {
            // Concurrent modification exception quorum send ko and throw concurrent modification
            // exception
            localKo(requestId, database, keySource);
            ORID id = ((OTxConcurrentModification) localResult).getRecordId();
            int version = ((OTxConcurrentModification) localResult).getVersion();
            throw new OConcurrentModificationException(
                id,
                version,
                iTx.getRecordEntry(id).getRecord().getVersion(),
                iTx.getRecordEntry(id).getType());
          }
        case OTxRecordLockTimeout.ID:
          {
            throw new ODistributedRecordLockedException(
                nodeName, ((OTxRecordLockTimeout) localResult).getLockedId());
          }
        case OTxKeyLockTimeout.ID:
          {
            throw new ODistributedKeyLockedException(
                nodeName, ((OTxKeyLockTimeout) localResult).getKey());
          }
        case OTxInvalidSequential.ID:
          // This never happen in local only, keep the management anyway
          throw new OInvalidSequentialException();
      }
      return;
    }
    final OTransactionPhase1Task txTask = createTxPhase1Task(txId, iTx);

    final Set<String> sentNodes = new HashSet<>(nodes);

    iTx.setStatus(OTransaction.TXSTATUS.COMMITTING);
    // SYNCHRONOUS CALL: REPLICATE IT
    dManager.sendRequest(
        dbName,
        nodes,
        txTask,
        requestId,
        localResult,
        ((iRequest,
            iNodes,
            task,
            nodesConcurToTheQuorum,
            availableNodes,
            expectedResponses,
            quorum,
            groupByResponse,
            waitLocalNode) -> {
          responseManager =
              new ODistributedTxResponseManagerImpl(
                  txTask,
                  iNodes,
                  nodesConcurToTheQuorum,
                  availableNodes,
                  expectedResponses,
                  quorum);
          return responseManager;
        }));

    handleResponse(requestId, responseManager, sentNodes, database, iTx, txTask);
  }

  private void handleResponse(
      ODistributedRequestId requestId,
      ODistributedTxResponseManager responseManager,
      Set<String> nodes,
      ODatabaseDocumentDistributed database,
      OTransactionInternal iTx,
      OTransactionPhase1Task txTask) {

    if (responseManager.isQuorumReached()) {
      Optional<OTransactionResultPayload> result = responseManager.getDistributedTxFinalResponse();
      assert result.isPresent();
      OTransactionResultPayload resultPayload = result.get();
      switch (resultPayload.getResponseType()) {
        case OTxSuccess.ID:
          // Success send ok
          sendPhase2Task(nodes, createTxPhase2Task(requestId, txTask, true));
          localOk(requestId, database, txTask);
          break;
        case OTxException.ID:
          // Exception send ko and throws the exception
          sendPhase2Task(nodes, createTxPhase2Task(requestId, txTask, false));
          localKo(requestId, database, txTask);
          throw ((OTxException) resultPayload).getException();
        case OTxUniqueIndex.ID:
          {
            // Unique index quorum error send ko and throw unique index exception
            sendPhase2Task(nodes, createTxPhase2Task(requestId, txTask, false));
            localKo(requestId, database, txTask);
            ORID id = ((OTxUniqueIndex) resultPayload).getRecordId();
            String index = ((OTxUniqueIndex) resultPayload).getIndex();
            Object key = ((OTxUniqueIndex) resultPayload).getKey();
            throw new ORecordDuplicatedException(
                String.format(
                    "Cannot index record %s: found duplicated key '%s' in index '%s' ",
                    id, key, index),
                index,
                id,
                key);
          }
        case OTxConcurrentModification.ID:
          {
            // Concurrent modification exception quorum send ko and throw cuncurrent modification
            // exception
            sendPhase2Task(nodes, createTxPhase2Task(requestId, txTask, false));
            localKo(requestId, database, txTask);
            ORID id = ((OTxConcurrentModification) resultPayload).getRecordId();
            int version = ((OTxConcurrentModification) resultPayload).getVersion();
            throw new OConcurrentModificationException(
                id,
                version,
                iTx.getRecordEntry(id).getRecord().getVersion(),
                iTx.getRecordEntry(id).getType());
          }
        case OTxConcurrentCreation.ID:
          {
            sendPhase2Task(nodes, createTxPhase2Task(requestId, txTask, false));
            localKo(requestId, database, txTask);
            throw new OConcurrentCreateException(
                ((OTxConcurrentCreation) resultPayload).getExpectedRid(),
                ((OTxConcurrentCreation) resultPayload).getActualRid());
          }

        case OTxRecordLockTimeout.ID:
          sendPhase2Task(nodes, createTxPhase2Task(requestId, txTask, false));
          localKo(requestId, database, txTask);
          throw new ODistributedRecordLockedException(
              ((OTxRecordLockTimeout) resultPayload).getNode(),
              ((OTxRecordLockTimeout) resultPayload).getLockedId());
        case OTxKeyLockTimeout.ID:
          sendPhase2Task(nodes, createTxPhase2Task(requestId, txTask, false));
          localKo(requestId, database, txTask);
          throw new ODistributedKeyLockedException(
              ((OTxKeyLockTimeout) resultPayload).getNode(),
              ((OTxKeyLockTimeout) resultPayload).getKey());
        case OTxInvalidSequential.ID:
          sendPhase2Task(nodes, createTxPhase2Task(requestId, txTask, false));
          localKo(requestId, database, txTask);
          throw new OInvalidSequentialException();
      }

      for (OTransactionResultPayload txResult : responseManager.getAllResponses()) {
        if (txResult.getResponseType() == OTxException.ID) {
          logger.warn("One node on error", ((OTxException) txResult).getException());
        }
      }
    } else {
      List<OTransactionResultPayload> results = responseManager.getAllResponses();
      // If quorum is not reached is enough on a Lock timeout to trigger a deadlock retry.
      List<Exception> exceptions = new ArrayList<>();
      List<String> messages = new ArrayList<>();
      for (OTransactionResultPayload result : results) {
        String node = responseManager.getNodeNameFromPayload(result);
        switch (result.getResponseType()) {
          case OTxRecordLockTimeout.ID:
            sendPhase2Task(nodes, createTxPhase2Task(requestId, txTask, false));
            localKo(requestId, database, txTask);
            throw new ODistributedRecordLockedException(
                ((OTxRecordLockTimeout) result).getNode(),
                ((OTxRecordLockTimeout) result).getLockedId());
          case OTxKeyLockTimeout.ID:
            sendPhase2Task(nodes, createTxPhase2Task(requestId, txTask, false));
            localKo(requestId, database, txTask);
            throw new ODistributedKeyLockedException(
                ((OTxKeyLockTimeout) result).getNode(), ((OTxKeyLockTimeout) result).getKey());

          case OTxConcurrentCreation.ID:
            sendPhase2Task(nodes, createTxPhase2Task(requestId, txTask, false));
            localKo(requestId, database, txTask);
            throw new OConcurrentCreateException(
                ((OTxConcurrentCreation) result).getExpectedRid(),
                ((OTxConcurrentCreation) result).getActualRid());

          case OTxSuccess.ID:
            messages.add("node: " + node + " success");
            break;
          case OTxConcurrentModification.ID:
            sendPhase2Task(nodes, createTxPhase2Task(requestId, txTask, false));
            localKo(requestId, database, txTask);
            ORecordId recordId = ((OTxConcurrentModification) result).getRecordId();
            throw new OConcurrentModificationException(
                recordId,
                ((OTxConcurrentModification) result).getVersion(),
                iTx.getRecordEntry(recordId).getRecord().getVersion(),
                iTx.getRecordEntry(recordId).getType());
          case OTxException.ID:
            exceptions.add(((OTxException) result).getException());
            logger.debug("distributed exception", ((OTxException) result).getException());
            messages.add(
                String.format(
                    "exception (node " + node + "): '%s'",
                    ((OTxException) result).getException().getMessage()));
            break;
          case OTxUniqueIndex.ID:
            messages.add(
                String.format(
                    "unique index violation on index (node "
                        + node
                        + "):'%s' with key:'%s' and rid:'%s'",
                    ((OTxUniqueIndex) result).getIndex(),
                    ((OTxUniqueIndex) result).getKey(),
                    ((OTxUniqueIndex) result).getRecordId()));
            break;
          case OTxInvalidSequential.ID:
            sendPhase2Task(nodes, createTxPhase2Task(requestId, txTask, false));
            localKo(requestId, database, txTask);
            throw new OInvalidSequentialException();
        }
      }
      sendPhase2Task(nodes, createTxPhase2Task(requestId, txTask, false));
      localKo(requestId, database, txTask);

      ODistributedOperationException ex =
          new ODistributedOperationException(
              String.format(
                  "Request `%s` didn't reach the quorum of '%d', responses: [%s]",
                  requestId, responseManager.getQuorum(), String.join(",", messages)));
      for (Exception e : exceptions) {
        ex.addSuppressed(e);
      }
      throw ex;
    }
  }

  private void localKo(
      ODistributedRequestId requestId,
      ODatabaseDocumentDistributed database,
      OLockKeySource source) {
    List<OLockGuard> guards = localDistributedDatabase.localLock(source);
    try {
      database.rollback2pc(requestId);
    } finally {
      localDistributedDatabase.localUnlock(guards);
    }
  }

  private void localOk(
      ODistributedRequestId requestId,
      ODatabaseDocumentDistributed database,
      OLockKeySource source) {
    List<OLockGuard> guards = localDistributedDatabase.localLock(source);
    try {
      database.commit2pcLocal(requestId);
    } finally {
      localDistributedDatabase.localUnlock(guards);
    }
  }

  private void sendPhase2Task(Set<String> nodes, OTransactionPhase2Task task) {
    dManager.sendRequest(dbName, nodes, task);
  }

  protected OTransactionPhase1Task createTxPhase1Task(
      OTransactionIdPromise id, final OTransactionInternal transaction) {
    final OTransactionPhase1Task txTask = new OTransactionPhase1Task();
    txTask.init(id, transaction);
    return txTask;
  }

  private OTransactionPhase2Task createTxPhase2Task(
      ODistributedRequestId requestId, OTransactionPhase1Task txTask, boolean success) {
    return new OTransactionPhase2Task(
        requestId, success, txTask.getRids(), txTask.getUniqueKeys(), txTask.getPromise());
  }

  /** This is to be used only for testing! */
  public void setResponseManager(ODistributedTxResponseManager responseManager) {
    this.responseManager = responseManager;
  }
}
