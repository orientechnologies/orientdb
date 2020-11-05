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
import com.orientechnologies.orient.core.exception.OConcurrentCreateException;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.storage.ORecordDuplicatedException;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.tx.OTransaction;
import com.orientechnologies.orient.core.tx.OTransactionId;
import com.orientechnologies.orient.core.tx.OTransactionInternal;
import com.orientechnologies.orient.server.distributed.*;
import com.orientechnologies.orient.server.distributed.ODistributedRequest.EXECUTION_MODE;
import com.orientechnologies.orient.server.distributed.impl.lock.OLockGuard;
import com.orientechnologies.orient.server.distributed.impl.task.OLockKeySource;
import com.orientechnologies.orient.server.distributed.impl.task.OTransactionPhase1Task;
import com.orientechnologies.orient.server.distributed.impl.task.OTransactionPhase2Task;
import com.orientechnologies.orient.server.distributed.impl.task.transaction.*;
import com.orientechnologies.orient.server.distributed.task.ODistributedKeyLockedException;
import com.orientechnologies.orient.server.distributed.task.ODistributedOperationException;
import com.orientechnologies.orient.server.distributed.task.ODistributedRecordLockedException;
import java.util.*;

public class ODistributedTxCoordinator {
  public static final String LOCAL_RESULT_SUCCESS = "OK";

  private final ODistributedServerManager dManager;
  private final OStorage storage;
  private final ODistributedDatabase localDistributedDatabase;
  private ODistributedTxResponseManager responseManager;
  private final ODistributedMessageService messageService;
  // ID and name of the node where this tx coordinator is running
  private final int nodeId;
  private final String nodeName;
  private final int maxRetries;
  private final int retryDelay;

  public ODistributedTxCoordinator(
      final OStorage storage,
      final ODistributedServerManager manager,
      final ODistributedDatabase iDDatabase,
      ODistributedMessageService messageService,
      int nodeId,
      String nodeName,
      int maxRetries,
      int retryDelay) {
    this.dManager = manager;
    this.storage = storage;
    this.localDistributedDatabase = iDDatabase;
    this.messageService = messageService;
    this.nodeId = nodeId;
    this.nodeName = nodeName;
    this.maxRetries = maxRetries;
    this.retryDelay = retryDelay;
  }

  public void commit(
      final ODatabaseDocumentDistributed database,
      final OTransactionInternal iTx,
      Set<String> clusters) {
    ODistributedDatabaseImpl distributedDatabase =
        (ODistributedDatabaseImpl) messageService.getDatabase(database.getName());
    int count = 0;
    do {
      final ODistributedRequestId requestId =
          new ODistributedRequestId(nodeId, dManager.getNextMessageIdCounter());
      distributedDatabase.startOperation();
      try {
        Optional<OTransactionId> genId = distributedDatabase.nextId();
        if (genId.isPresent()) {
          OTransactionId txId = genId.get();
          tryCommit(database, iTx, txId, requestId, clusters);
          return;
        } else {
          try {
            Thread.sleep(new Random().nextInt(retryDelay));
          } catch (InterruptedException e) {
            OException.wrapException(new OInterruptedException(e.getMessage()), e);
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
        if (count > maxRetries) {
          destroyContext(requestId);
          throw ex;
        }
        try {
          Thread.sleep(new Random().nextInt(retryDelay));
        } catch (InterruptedException e) {
          OException.wrapException(new OInterruptedException(e.getMessage()), e);
        }

      } catch (RuntimeException | Error ex) {
        destroyContext(requestId);
        throw ex;
      } finally {
        distributedDatabase.endOperation();
      }
      count++;
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
      OTransactionId txId,
      final ODistributedRequestId requestId,
      Set<String> clusters) {

    iTx.setStatus(OTransaction.TXSTATUS.BEGUN);

    ODistributedDatabaseImpl sharedDb =
        (ODistributedDatabaseImpl) messageService.getDatabase(database.getName());

    OLocalKeySource keySource = new OLocalKeySource(txId, iTx, database);
    List<OLockGuard> guards = sharedDb.localLock(keySource);
    OTransactionResultPayload localResult;

    try {
      // This retry happen only the first time i try to lock on local server
      localResult =
          OTransactionPhase1Task.executeTransaction(requestId, txId, database, iTx, true, -1);
    } finally {
      sharedDb.localUnlock(guards);
    }

    if (localResult.getResponseType() == OTxRecordLockTimeout.ID) {
      sharedDb.popTxContext(requestId).destroy();
      throw new ODistributedRecordLockedException(
          nodeName, ((OTxRecordLockTimeout) localResult).getLockedId());
    }
    if (localResult.getResponseType() == OTxKeyLockTimeout.ID) {
      sharedDb.popTxContext(requestId).destroy();
      throw new ODistributedKeyLockedException(
          nodeName, ((OTxKeyLockTimeout) localResult).getKey());
    }

    Set<String> nodes = sharedDb.getAvailableNodesButLocal(clusters);
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
        storage.getName(),
        clusters,
        nodes,
        txTask,
        requestId.getMessageId(),
        EXECUTION_MODE.RESPONSE,
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

    handleResponse(requestId, responseManager, clusters, sentNodes, database, iTx, txTask);
  }

  private void handleResponse(
      ODistributedRequestId requestId,
      ODistributedTxResponseManager responseManager,
      Set<String> involvedClusters,
      Set<String> nodes,
      ODatabaseDocumentDistributed database,
      OTransactionInternal iTx,
      OTransactionPhase1Task txTask) {
    int[] involvedClustersIds = new int[involvedClusters.size()];
    int i = 0;
    for (String involvedCluster : involvedClusters) {
      involvedClustersIds[i++] = database.getClusterIdByName(involvedCluster);
    }

    if (responseManager.isQuorumReached()) {
      Optional<OTransactionResultPayload> result = responseManager.getDistributedTxFinalResponse();
      assert result.isPresent();
      OTransactionResultPayload resultPayload = result.get();
      switch (resultPayload.getResponseType()) {
        case OTxSuccess.ID:
          // Success send ok
          sendPhase2Task(involvedClusters, nodes, createTxPhase2Task(requestId, txTask, true));
          localOk(requestId, database, txTask);
          break;
        case OTxException.ID:
          // Exception send ko and throws the exception
          sendPhase2Task(involvedClusters, nodes, createTxPhase2Task(requestId, txTask, false));
          localKo(requestId, database, txTask);
          throw ((OTxException) resultPayload).getException();
        case OTxUniqueIndex.ID:
          {
            // Unique index quorum error send ko and throw unique index exception
            sendPhase2Task(involvedClusters, nodes, createTxPhase2Task(requestId, txTask, false));
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
            sendPhase2Task(involvedClusters, nodes, createTxPhase2Task(requestId, txTask, false));
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
            sendPhase2Task(involvedClusters, nodes, createTxPhase2Task(requestId, txTask, false));
            localKo(requestId, database, txTask);
            throw new OConcurrentCreateException(
                ((OTxConcurrentCreation) resultPayload).getExpectedRid(),
                ((OTxConcurrentCreation) resultPayload).getActualRid());
          }

        case OTxRecordLockTimeout.ID:
          sendPhase2Task(involvedClusters, nodes, createTxPhase2Task(requestId, txTask, false));
          localKo(requestId, database, txTask);
          throw new ODistributedRecordLockedException(
              ((OTxRecordLockTimeout) resultPayload).getNode(),
              ((OTxRecordLockTimeout) resultPayload).getLockedId());
        case OTxKeyLockTimeout.ID:
          sendPhase2Task(involvedClusters, nodes, createTxPhase2Task(requestId, txTask, false));
          localKo(requestId, database, txTask);
          throw new ODistributedKeyLockedException(
              ((OTxKeyLockTimeout) resultPayload).getNode(),
              ((OTxKeyLockTimeout) resultPayload).getKey());
        case OTxInvalidSequential.ID:
          sendPhase2Task(involvedClusters, nodes, createTxPhase2Task(requestId, txTask, false));
          localKo(requestId, database, txTask);
          throw new OInvalidSequentialException();
      }

      for (OTransactionResultPayload txResult : responseManager.getAllResponses()) {
        if (txResult.getResponseType() == OTxException.ID) {
          OLogManager.instance()
              .warn(this, "One node on error", ((OTxException) txResult).getException());
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
            sendPhase2Task(involvedClusters, nodes, createTxPhase2Task(requestId, txTask, false));
            localKo(requestId, database, txTask);
            throw new ODistributedRecordLockedException(
                ((OTxRecordLockTimeout) result).getNode(),
                ((OTxRecordLockTimeout) result).getLockedId());
          case OTxKeyLockTimeout.ID:
            sendPhase2Task(involvedClusters, nodes, createTxPhase2Task(requestId, txTask, false));
            localKo(requestId, database, txTask);
            throw new ODistributedKeyLockedException(
                ((OTxKeyLockTimeout) result).getNode(), ((OTxKeyLockTimeout) result).getKey());

          case OTxConcurrentCreation.ID:
            sendPhase2Task(involvedClusters, nodes, createTxPhase2Task(requestId, txTask, false));
            localKo(requestId, database, txTask);
            throw new OConcurrentCreateException(
                ((OTxConcurrentCreation) result).getExpectedRid(),
                ((OTxConcurrentCreation) result).getActualRid());

          case OTxSuccess.ID:
            messages.add("node: " + node + " success");
            break;
          case OTxConcurrentModification.ID:
            sendPhase2Task(involvedClusters, nodes, createTxPhase2Task(requestId, txTask, false));
            localKo(requestId, database, txTask);
            ORecordId recordId = ((OTxConcurrentModification) result).getRecordId();
            throw new OConcurrentModificationException(
                recordId,
                ((OTxConcurrentModification) result).getVersion(),
                iTx.getRecordEntry(recordId).getRecord().getVersion(),
                iTx.getRecordEntry(recordId).getType());
          case OTxException.ID:
            exceptions.add(((OTxException) result).getException());
            OLogManager.instance()
                .debug(this, "distributed exception", ((OTxException) result).getException());
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
            sendPhase2Task(involvedClusters, nodes, createTxPhase2Task(requestId, txTask, false));
            localKo(requestId, database, txTask);
            throw new OInvalidSequentialException();
        }
      }
      sendPhase2Task(involvedClusters, nodes, createTxPhase2Task(requestId, txTask, false));
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
    ODistributedDatabaseImpl dd =
        (ODistributedDatabaseImpl) messageService.getDatabase(database.getName());
    List<OLockGuard> guards = dd.localLock(source);
    try {
      database.rollback2pc(requestId);
    } finally {
      dd.localUnlock(guards);
    }
  }

  private void localOk(
      ODistributedRequestId requestId,
      ODatabaseDocumentDistributed database,
      OLockKeySource source) {
    ODistributedDatabaseImpl dd =
        (ODistributedDatabaseImpl) messageService.getDatabase(database.getName());
    List<OLockGuard> guards = dd.localLock(source);
    try {
      database.commit2pcLocal(requestId);
    } finally {
      dd.localUnlock(guards);
    }
  }

  private void sendPhase2Task(
      Set<String> involvedClusters, Set<String> nodes, OTransactionPhase2Task task) {
    dManager.sendRequest(
        storage.getName(),
        involvedClusters,
        nodes,
        task,
        dManager.getNextMessageIdCounter(),
        EXECUTION_MODE.RESPONSE,
        LOCAL_RESULT_SUCCESS);
  }

  protected OTransactionPhase1Task createTxPhase1Task(
      OTransactionId id, final OTransactionInternal transaction) {
    final OTransactionPhase1Task txTask = new OTransactionPhase1Task();
    txTask.init(id, transaction);
    return txTask;
  }

  private OTransactionPhase2Task createTxPhase2Task(
      ODistributedRequestId requestId, OTransactionPhase1Task txTask, boolean success) {
    return new OTransactionPhase2Task(
        requestId, success, txTask.getRids(), txTask.getUniqueKeys(), txTask.getTransactionId());
  }

  /** This is to be used only for testing! */
  void setResponseManager(ODistributedTxResponseManager responseManager) {
    this.responseManager = responseManager;
  }
}
