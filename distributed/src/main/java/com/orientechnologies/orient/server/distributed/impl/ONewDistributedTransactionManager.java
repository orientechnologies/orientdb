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
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.storage.ORecordDuplicatedException;
import com.orientechnologies.orient.core.storage.disk.OLocalPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.core.tx.OTransaction;
import com.orientechnologies.orient.core.tx.OTransactionInternal;
import com.orientechnologies.orient.server.distributed.ODistributedConfiguration;
import com.orientechnologies.orient.server.distributed.ODistributedDatabase;
import com.orientechnologies.orient.server.distributed.ODistributedRequest.EXECUTION_MODE;
import com.orientechnologies.orient.server.distributed.ODistributedRequestId;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.distributed.impl.task.OTransactionPhase1Task;
import com.orientechnologies.orient.server.distributed.impl.task.OTransactionPhase2Task;
import com.orientechnologies.orient.server.distributed.impl.task.transaction.OTransactionResultPayload;
import com.orientechnologies.orient.server.distributed.impl.task.transaction.OTxConcurrentModification;
import com.orientechnologies.orient.server.distributed.impl.task.transaction.OTxException;
import com.orientechnologies.orient.server.distributed.impl.task.transaction.OTxLockTimeout;
import com.orientechnologies.orient.server.distributed.impl.task.transaction.OTxSuccess;
import com.orientechnologies.orient.server.distributed.impl.task.transaction.OTxUniqueIndex;
import com.orientechnologies.orient.server.distributed.task.ODistributedOperationException;
import com.orientechnologies.orient.server.distributed.task.ODistributedRecordLockedException;

import java.io.IOException;
import java.util.*;

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
    int nretry = database.getConfiguration().getValueAsInteger(OGlobalConfiguration.DISTRIBUTED_CONCURRENT_TX_MAX_AUTORETRY);
    int delay = database.getConfiguration().getValueAsInteger(OGlobalConfiguration.DISTRIBUTED_CONCURRENT_TX_AUTORETRY_DELAY);
    int count = 0;
    do {

      try {
        return retriedCommit(database, iTx);
      } catch (ODistributedRecordLockedException ex) {
        if (count == nretry) {
          throw ex;
        }
        int v = new Random().nextInt(1000);
        try {
          Thread.sleep(delay * count + v);
        } catch (InterruptedException e) {
          Thread.interrupted();
          return null;
        }
      }
      count++;
    } while (true);

  }

  public List<ORecordOperation> retriedCommit(final ODatabaseDocumentDistributed database, final OTransactionInternal iTx) {
    final String localNodeName = dManager.getLocalNodeName();

    iTx.setStatus(OTransaction.TXSTATUS.BEGUN);

    final ODistributedConfiguration dbCfg = dManager.getDatabaseConfiguration(storage.getName());

    // CHECK THE LOCAL NODE IS THE OWNER OF THE CLUSTER IDS
    checkForClusterIds(iTx);

    final ODistributedRequestId requestId = new ODistributedRequestId(dManager.getLocalNodeId(),
        dManager.getNextMessageIdCounter());

    final Set<String> involvedClusters = getInvolvedClusters(iTx.getRecordOperations());
    Set<String> nodes = getAvailableNodesButLocal(dbCfg, involvedClusters, localNodeName);
    final OTransactionPhase1Task txTask = !nodes.isEmpty() ? createTxTask(iTx, nodes) : null;
    OTransactionResultPayload localResult;
    int nretry = database.getConfiguration().getValueAsInteger(OGlobalConfiguration.DISTRIBUTED_CONCURRENT_TX_MAX_AUTORETRY);
    int delay = database.getConfiguration().getValueAsInteger(OGlobalConfiguration.DISTRIBUTED_CONCURRENT_TX_AUTORETRY_DELAY);
    int count = 0;
    do {
      localResult = OTransactionPhase1Task.executeTransaction(requestId, database, iTx, true, -1);
      if (count != 0) {
        int v = new Random().nextInt(1000);
        try {
          Thread.sleep(delay * count + v);
        } catch (InterruptedException e) {
          Thread.interrupted();
          return null;
        }
      }
      count++;
    } while (localResult.getResponseType() == OTxLockTimeout.ID && count < nretry);

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
        //TODO include all paramenter in response
        throw new OConcurrentModificationException(id, version, 0, 0);
      }
      case OTxLockTimeout.ID:
        throw new ODistributedRecordLockedException("DeadLock", new ORecordId(-1, -1), requestId,
            database.getConfiguration().getValueAsInteger(OGlobalConfiguration.DISTRIBUTED_ATOMIC_LOCK_TIMEOUT));
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

    handleResponse(requestId, responseManager, involvedClusters, sentNodes, database, iTx);

    // OK, DISTRIBUTED COMMIT SUCCEED
    //TODO:Get the list of result from local ok, if is needed otherwise remove the ruturn
    return null;

  }

  public OLogSequenceNumber getLsn() {
    return ((OAbstractPaginatedStorage) storage.getUnderlying()).getLSN();
  }

  private void handleResponse(ODistributedRequestId requestId, ONewDistributedResponseManager responseManager,
      Set<String> involvedClusters, Set<String> nodes, ODatabaseDocumentDistributed database, OTransactionInternal iTx) {

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
      case OTxLockTimeout.ID:
        sendPhase2Task(involvedClusters, nodes, new OTransactionPhase2Task(requestId, false, involvedClustersIds, getLsn()));
        throw new ODistributedRecordLockedException("DeadLock", new ORecordId(-1, -1), requestId,
            database.getConfiguration().getValueAsInteger(OGlobalConfiguration.DISTRIBUTED_ATOMIC_LOCK_TIMEOUT));
      }
    } else {
      List<OTransactionResultPayload> results = responseManager.getAllResponses();
      //If quorum is not reached is enough on a Lock timeout to trigger a deadlock retry.
      List<Exception> exceptions = new ArrayList<>();
      List<String> messages = new ArrayList<>();
      for (OTransactionResultPayload result : results) {
        switch (result.getResponseType()) {
        case OTxLockTimeout.ID:
          sendPhase2Task(involvedClusters, nodes, new OTransactionPhase2Task(requestId, false, involvedClustersIds, getLsn()));
          throw new ODistributedRecordLockedException("DeadLock", new ORecordId(-1, -1), requestId,
              database.getConfiguration().getValueAsInteger(OGlobalConfiguration.DISTRIBUTED_ATOMIC_LOCK_TIMEOUT));
        case OTxSuccess.ID:
          messages.add("success");
          break;
        case OTxConcurrentModification.ID:
          ORecordId recordId = ((OTxConcurrentModification) result).getRecordId();
          messages.add(String
              .format("concurrent modification record: %s database version: %d transaction version: %d", recordId.toString(),
                  ((OTxConcurrentModification) result).getVersion(), iTx.getRecordEntry(recordId).getRecord().getVersion()));
          break;
        case OTxException.ID:
          exceptions.add(((OTxException) result).getException());
          OLogManager.instance().debug(this, "distributed exception", ((OTxException) result).getException());
          messages.add(String.format("exception: '%s'", ((OTxException) result).getException().getMessage()));
          break;
        case OTxUniqueIndex.ID:
          messages.add(String
              .format("unique index violation on index:'$s' with key:'%s' and rid:'%s'", ((OTxUniqueIndex) result).getIndex(),
                  ((OTxUniqueIndex) result).getKey(), ((OTxUniqueIndex) result).getRecordId()));
          break;

        }
      }
      localKo(requestId, database);
      sendPhase2Task(involvedClusters, nodes, new OTransactionPhase2Task(requestId, false, involvedClustersIds, getLsn()));

      ODistributedOperationException ex = new ODistributedOperationException(
          String.format("quorum not reached, responses: [%s]", String.join(",", messages)));
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

  protected OTransactionPhase1Task createTxTask(final OTransactionInternal transaction, final Set<String> nodes) {
    final OTransactionPhase1Task txTask = (OTransactionPhase1Task) dManager.getTaskFactoryManager().getFactoryByServerNames(nodes)
        .createTask(OTransactionPhase1Task.FACTORYID);
    txTask.init(transaction);
    return txTask;
  }

}
