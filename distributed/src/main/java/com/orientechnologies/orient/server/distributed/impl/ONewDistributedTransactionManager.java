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
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.exception.OConcurrentCreateException;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.storage.ORecordDuplicatedException;
import com.orientechnologies.orient.core.storage.disk.OLocalPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.core.tx.OTransaction;
import com.orientechnologies.orient.core.tx.OTransactionId;
import com.orientechnologies.orient.core.tx.OTransactionInternal;
import com.orientechnologies.orient.server.distributed.*;
import com.orientechnologies.orient.server.distributed.ODistributedRequest.EXECUTION_MODE;
import com.orientechnologies.orient.server.distributed.impl.task.OTransactionPhase1Task;
import com.orientechnologies.orient.server.distributed.impl.task.OTransactionPhase2Task;
import com.orientechnologies.orient.server.distributed.impl.task.transaction.*;
import com.orientechnologies.orient.server.distributed.task.ODistributedKeyLockedException;
import com.orientechnologies.orient.server.distributed.task.ODistributedOperationException;
import com.orientechnologies.orient.server.distributed.task.ODistributedRecordLockedException;

import java.io.IOException;
import java.util.*;

import static com.orientechnologies.orient.core.config.OGlobalConfiguration.*;

/**
 * Distributed transaction manager.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class ONewDistributedTransactionManager {
  private final ODistributedServerManager dManager;
  private final ODistributedStorage       storage;
  private final ODistributedDatabase      localDistributedDatabase;

  private static final boolean                        SYNC_TX_COMPLETED = false;
  private              ONewDistributedResponseManager responseManager;

  public ONewDistributedTransactionManager(final ODistributedStorage storage, final ODistributedServerManager manager,
      final ODistributedDatabase iDDatabase) {
    this.dManager = manager;
    this.storage = storage;
    this.localDistributedDatabase = iDDatabase;
  }

  public List<ORecordOperation> commit(final ODatabaseDocumentDistributed database, final OTransactionInternal iTx,
      final ODistributedStorageEventListener eventListener) {
    int nretry = database.getConfiguration().getValueAsInteger(DISTRIBUTED_CONCURRENT_TX_MAX_AUTORETRY);
    int delay = database.getConfiguration().getValueAsInteger(DISTRIBUTED_CONCURRENT_TX_AUTORETRY_DELAY);
    ODistributedDatabaseImpl distributedDatabase = (ODistributedDatabaseImpl) dManager.getMessageService()
        .getDatabase(database.getName());
    int count = 0;

    do {
      final ODistributedRequestId requestId = new ODistributedRequestId(dManager.getLocalNodeId(),
          dManager.getNextMessageIdCounter());
      distributedDatabase.startOperation();
      try {
        Optional<OTransactionId> genId = dManager.getMessageService().getDatabase(database.getName()).nextId();
        if (genId.isPresent()) {
          OTransactionId txId = genId.get();
          return retriedCommit(database, iTx, txId, requestId);
        } else {
          try {
            Thread.sleep(new Random().nextInt(delay));
          } catch (InterruptedException e) {
            OException.wrapException(new OInterruptedException(e.getMessage()), e);
          }
        }
      } catch (OConcurrentCreateException | ODistributedRecordLockedException | ODistributedKeyLockedException | OInvalidSequentialException ex) {
        // Nothing just retry
        if (count > nretry) {
          ODistributedTxContext context = localDistributedDatabase.getTxContext(requestId);
          if (context != null) {
            context.destroy();
          }
          throw ex;
        }
        try {
          Thread.sleep(new Random().nextInt(delay));
        } catch (InterruptedException e) {
          OException.wrapException(new OInterruptedException(e.getMessage()), e);
        }

      } catch (RuntimeException | Error ex) {
        ODistributedTxContext context = localDistributedDatabase.getTxContext(requestId);
        if (context != null) {
          context.destroy();
        }
        throw ex;
      } finally {
        distributedDatabase.endOperation();
      }
      count++;
    } while (true);
  }

  public List<ORecordOperation> retriedCommit(final ODatabaseDocumentDistributed database, final OTransactionInternal iTx,
      OTransactionId txId, final ODistributedRequestId requestId) {
    final String localNodeName = dManager.getLocalNodeName();

    iTx.setStatus(OTransaction.TXSTATUS.BEGUN);

    final ODistributedConfiguration dbCfg = dManager.getDatabaseConfiguration(storage.getName());

    final Set<String> involvedClusters = getInvolvedClusters(iTx.getRecordOperations());
    Set<String> nodes = getAvailableNodesButLocal(dbCfg, involvedClusters, localNodeName);
    OTransactionResultPayload localResult;

    //This retry happen only the first time i try to lock on local server
    localResult = OTransactionPhase1Task.executeTransaction(requestId, txId, database, iTx, true, -1);
    if (localResult.getResponseType() == OTxRecordLockTimeout.ID) {
      dManager.getMessageService().getDatabase(database.getName()).popTxContext(requestId).destroy();
      int timeout = database.getConfiguration().getValueAsInteger(DISTRIBUTED_ATOMIC_LOCK_TIMEOUT);
      throw new ODistributedRecordLockedException(dManager.getLocalNodeName(), ((OTxRecordLockTimeout) localResult).getLockedId(),
          timeout);
    }
    if (localResult.getResponseType() == OTxKeyLockTimeout.ID) {
      dManager.getMessageService().getDatabase(database.getName()).popTxContext(requestId).destroy();
      int timeout = database.getConfiguration().getValueAsInteger(DISTRIBUTED_ATOMIC_LOCK_TIMEOUT);
      throw new ODistributedKeyLockedException(dManager.getLocalNodeName(), ((OTxKeyLockTimeout) localResult).getKey(), timeout);
    }

    final OTransactionPhase1Task txTask = !nodes.isEmpty() ? createTxTask(txId, iTx, nodes) : null;
    try {
      localDistributedDatabase.getSyncConfiguration()
          .setLastLSN(localNodeName, ((OLocalPaginatedStorage) storage.getUnderlying()).getLSN(), true);
    } catch (IOException e) {
      ODistributedServerLog
          .debug(this, dManager != null ? dManager.getLocalNodeName() : "?", null, ODistributedServerLog.DIRECTION.NONE,
              "Error on updating local LSN configuration for database '%s'", storage.getName());
    }
    if (nodes.isEmpty()) {
      switch (localResult.getResponseType()) {
      case OTxSuccess.ID:
        //Success send ok
        localOk(requestId, database);
        break;
      case OTxException.ID:
        //Exception send ko and throws the exception
        localKo(requestId, database);
        throw ((OTxException) localResult).getException();
      case OTxUniqueIndex.ID: {
        //Unique index quorum error send ko and throw unique index exception
        localKo(requestId, database);
        ORID id = ((OTxUniqueIndex) localResult).getRecordId();
        String index = ((OTxUniqueIndex) localResult).getIndex();
        Object key = ((OTxUniqueIndex) localResult).getKey();
        throw new ORecordDuplicatedException(
            String.format("Cannot index record %s: found duplicated key '%s' in index '%s' ", id, key, index), index, id, key);
      }
      case OTxConcurrentModification.ID: {
        //Concurrent modification exception quorum send ko and throw cuncurrent modification exception
        localKo(requestId, database);
        ORID id = ((OTxConcurrentModification) localResult).getRecordId();
        int version = ((OTxConcurrentModification) localResult).getVersion();
        throw new OConcurrentModificationException(id, version, iTx.getRecordEntry(id).getRecord().getVersion(),
            iTx.getRecordEntry(id).getType());
      }
      case OTxRecordLockTimeout.ID: {
        int timeout = database.getConfiguration().getValueAsInteger(DISTRIBUTED_ATOMIC_LOCK_TIMEOUT);
        throw new ODistributedRecordLockedException(dManager.getLocalNodeName(), ((OTxRecordLockTimeout) localResult).getLockedId(),
            timeout);
      }
      case OTxKeyLockTimeout.ID: {
        int timeout = database.getConfiguration().getValueAsInteger(DISTRIBUTED_ATOMIC_LOCK_TIMEOUT);
        throw new ODistributedKeyLockedException(dManager.getLocalNodeName(), ((OTxKeyLockTimeout) localResult).getKey(), timeout);
      }
      case OTxInvalidSequential.ID:
        //This never happen in local only, keep the management anyway
        throw new OInvalidSequentialException(((OTxInvalidSequential) localResult).getCurrent());

      }
      return null;
    }
    //TODO:check the lsn
    txTask.setLastLSN(getLsn());

    final Set sentNodes = new HashSet(nodes);

    iTx.setStatus(OTransaction.TXSTATUS.COMMITTING);
    // SYNCHRONOUS CALL: REPLICATE IT
    ((ODistributedAbstractPlugin) dManager)
        .sendRequest(storage.getName(), involvedClusters, nodes, txTask, requestId.getMessageId(), EXECUTION_MODE.RESPONSE,
            localResult, null, null,
            ((iRequest, iNodes, endCallback, task, nodesConcurToTheQuorum, availableNodes, expectedResponses, quorum, groupByResponse, waitLocalNode) -> {
              responseManager = new ONewDistributedResponseManager(txTask, iNodes, nodesConcurToTheQuorum, availableNodes,
                  expectedResponses, quorum);
              return responseManager;
            }));

    handleResponse(requestId, txId, responseManager, involvedClusters, sentNodes, database, iTx);

    // OK, DISTRIBUTED COMMIT SUCCEED
    //TODO:Get the list of result from local ok, if is needed otherwise remove the ruturn
    return null;

  }

  public OLogSequenceNumber getLsn() {
    return ((OAbstractPaginatedStorage) storage.getUnderlying()).getLSN();
  }

  private void handleResponse(ODistributedRequestId requestId, OTransactionId transactionId,
      ONewDistributedResponseManager responseManager, Set<String> involvedClusters, Set<String> nodes,
      ODatabaseDocumentDistributed database, OTransactionInternal iTx) {
    int timeout = database.getConfiguration().getValueAsInteger(DISTRIBUTED_ATOMIC_LOCK_TIMEOUT);
    int[] involvedClustersIds = new int[involvedClusters.size()];
    int i = 0;
    for (String involvedCluster : involvedClusters) {
      involvedClustersIds[i++] = database.getClusterIdByName(involvedCluster);
    }

    if (responseManager.isQuorumReached()) {
      List<OTransactionResultPayload> results = (List<OTransactionResultPayload>) responseManager.getGenericFinalResponse();
      assert results.size() > 0;
      OTransactionResultPayload resultPayload = results.get(0);
      switch (resultPayload.getResponseType()) {
      case OTxSuccess.ID:
        //Success send ok
        sendPhase2Task(involvedClusters, nodes, new OTransactionPhase2Task(requestId, true, involvedClustersIds, getLsn()));
        localOk(requestId, database);
        break;
      case OTxException.ID:
        //Exception send ko and throws the exception
        sendPhase2Task(involvedClusters, nodes, new OTransactionPhase2Task(requestId, false, involvedClustersIds, getLsn()));
        localKo(requestId, database);
        throw ((OTxException) resultPayload).getException();
      case OTxUniqueIndex.ID: {
        //Unique index quorum error send ko and throw unique index exception
        sendPhase2Task(involvedClusters, nodes, new OTransactionPhase2Task(requestId, false, involvedClustersIds, getLsn()));
        localKo(requestId, database);
        ORID id = ((OTxUniqueIndex) resultPayload).getRecordId();
        String index = ((OTxUniqueIndex) resultPayload).getIndex();
        Object key = ((OTxUniqueIndex) resultPayload).getKey();
        throw new ORecordDuplicatedException(
            String.format("Cannot index record %s: found duplicated key '%s' in index '%s' ", id, key, index), index, id, key);
      }
      case OTxConcurrentModification.ID: {
        //Concurrent modification exception quorum send ko and throw cuncurrent modification exception
        sendPhase2Task(involvedClusters, nodes, new OTransactionPhase2Task(requestId, false, involvedClustersIds, getLsn()));
        localKo(requestId, database);
        ORID id = ((OTxConcurrentModification) resultPayload).getRecordId();
        int version = ((OTxConcurrentModification) resultPayload).getVersion();
        throw new OConcurrentModificationException(id, version, iTx.getRecordEntry(id).getRecord().getVersion(),
            iTx.getRecordEntry(id).getType());
      }
      case OTxConcurrentCreation.ID: {
        sendPhase2Task(involvedClusters, nodes, new OTransactionPhase2Task(requestId, false, involvedClustersIds, getLsn()));
        localKo(requestId, database);
        throw new OConcurrentCreateException(((OTxConcurrentCreation) resultPayload).getExpectedRid(),
            ((OTxConcurrentCreation) resultPayload).getActualRid());
      }

      case OTxRecordLockTimeout.ID:
        sendPhase2Task(involvedClusters, nodes, new OTransactionPhase2Task(requestId, false, involvedClustersIds, getLsn()));
        localKo(requestId, database);
        throw new ODistributedRecordLockedException(((OTxRecordLockTimeout) resultPayload).getNode(),
            ((OTxRecordLockTimeout) resultPayload).getLockedId(), timeout);
      case OTxKeyLockTimeout.ID:
        sendPhase2Task(involvedClusters, nodes, new OTransactionPhase2Task(requestId, false, involvedClustersIds, getLsn()));
        localKo(requestId, database);
        throw new ODistributedKeyLockedException(((OTxKeyLockTimeout) resultPayload).getNode(),
            ((OTxKeyLockTimeout) resultPayload).getKey(), timeout);
      case OTxInvalidSequential.ID:
        sendPhase2Task(involvedClusters, nodes, new OTransactionPhase2Task(requestId, false, involvedClustersIds, getLsn()));
        localKo(requestId, database);
        throw new OInvalidSequentialException(((OTxInvalidSequential) resultPayload).getCurrent());
      }

    } else {
      List<OTransactionResultPayload> results = responseManager.getAllResponses();
      //If quorum is not reached is enough on a Lock timeout to trigger a deadlock retry.
      List<Exception> exceptions = new ArrayList<>();
      List<String> messages = new ArrayList<>();
      for (OTransactionResultPayload result : results) {
        String node = responseManager.getNodeNameFromPayload(result);
        switch (result.getResponseType()) {
        case OTxRecordLockTimeout.ID:
          sendPhase2Task(involvedClusters, nodes, new OTransactionPhase2Task(requestId, false, involvedClustersIds, getLsn()));
          localKo(requestId, database);
          throw new ODistributedRecordLockedException(((OTxRecordLockTimeout) result).getNode(),
              ((OTxRecordLockTimeout) result).getLockedId(), timeout);
        case OTxKeyLockTimeout.ID:
          sendPhase2Task(involvedClusters, nodes, new OTransactionPhase2Task(requestId, false, involvedClustersIds, getLsn()));
          localKo(requestId, database);
          throw new ODistributedKeyLockedException(((OTxKeyLockTimeout) result).getNode(), ((OTxKeyLockTimeout) result).getKey(),
              timeout);

        case OTxConcurrentCreation.ID:
          sendPhase2Task(involvedClusters, nodes, new OTransactionPhase2Task(requestId, false, involvedClustersIds, getLsn()));
          localKo(requestId, database);
          throw new OConcurrentCreateException(((OTxConcurrentCreation) result).getExpectedRid(),
              ((OTxConcurrentCreation) result).getActualRid());

        case OTxSuccess.ID:
          messages.add("node: " + node + " success");
          break;
        case OTxConcurrentModification.ID:
          sendPhase2Task(involvedClusters, nodes, new OTransactionPhase2Task(requestId, false, involvedClustersIds, getLsn()));
          localKo(requestId, database);
          ORecordId recordId = ((OTxConcurrentModification) result).getRecordId();
          throw new OConcurrentModificationException(recordId, iTx.getRecordEntry(recordId).getRecord().getVersion(),
              ((OTxConcurrentModification) result).getVersion(), iTx.getRecordEntry(recordId).getType());
        case OTxException.ID:
          exceptions.add(((OTxException) result).getException());
          OLogManager.instance().debug(this, "distributed exception", ((OTxException) result).getException());
          messages.add(String.format("exception (node " + node + "): '%s'", ((OTxException) result).getException().getMessage()));
          break;
        case OTxUniqueIndex.ID:
          messages.add(String.format("unique index violation on index (node " + node + "):'%s' with key:'%s' and rid:'%s'",
              ((OTxUniqueIndex) result).getIndex(), ((OTxUniqueIndex) result).getKey(), ((OTxUniqueIndex) result).getRecordId()));
          break;
        case OTxInvalidSequential.ID:
          sendPhase2Task(involvedClusters, nodes, new OTransactionPhase2Task(requestId, false, involvedClustersIds, getLsn()));
          localKo(requestId, database);
          throw new OInvalidSequentialException(((OTxInvalidSequential) result).getCurrent());
        }
      }
      sendPhase2Task(involvedClusters, nodes, new OTransactionPhase2Task(requestId, false, involvedClustersIds, getLsn()));
      localKo(requestId, database);

      ODistributedOperationException ex = new ODistributedOperationException(String
          .format("Request `%s` didn't reach the quorum of '%d', responses: [%s]", requestId, responseManager.getQuorum(),
              String.join(",", messages)));
      for (Exception e : exceptions) {
        ex.addSuppressed(e);
      }
      throw ex;
    }

  }

  private void localKo(ODistributedRequestId requestId, ODatabaseDocumentDistributed database) {
    database.rollback2pc(requestId);
  }

  private void localOk(ODistributedRequestId requestId, ODatabaseDocumentDistributed database) {
    database.commit2pcLocal(requestId);
  }

  private void sendPhase2Task(Set<String> involvedClusters, Set<String> nodes, OTransactionPhase2Task task) {
    dManager
        .sendRequest(storage.getName(), involvedClusters, nodes, task, dManager.getNextMessageIdCounter(), EXECUTION_MODE.RESPONSE,
            "OK", null, null);
  }

  protected void checkForClusterIds(final OTransactionInternal iTx) {
    for (ORecordOperation op : iTx.getRecordOperations()) {
      final ORecordId rid = (ORecordId) op.getRecord().getIdentity();
      switch (op.type) {
      case ORecordOperation.CREATED:
        assert rid.isPersistent();
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

  protected Set<String> getInvolvedClusters(final Iterable<ORecordOperation> uResult) {
    final Set<String> involvedClusters = new HashSet<String>();
    for (ORecordOperation op : uResult) {
      final ORecord record = op.getRecord();
      involvedClusters.add(storage.getClusterNameByRID((ORecordId) record.getIdentity()));
    }
    return involvedClusters;
  }

  protected OTransactionPhase1Task createTxTask(OTransactionId id, final OTransactionInternal transaction,
      final Set<String> nodes) {
    final OTransactionPhase1Task txTask = (OTransactionPhase1Task) dManager.getTaskFactoryManager().getFactoryByServerNames(nodes)
        .createTask(OTransactionPhase1Task.FACTORYID);
    txTask.init(id, transaction);
    return txTask;
  }

}
