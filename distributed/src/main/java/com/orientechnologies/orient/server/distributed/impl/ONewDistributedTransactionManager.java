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

import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.exception.*;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.storage.ORecordDuplicatedException;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OLocalPaginatedStorage;
import com.orientechnologies.orient.core.tx.OTransaction;
import com.orientechnologies.orient.core.tx.OTransactionInternal;
import com.orientechnologies.orient.core.tx.OTransactionOptimistic;
import com.orientechnologies.orient.server.distributed.*;
import com.orientechnologies.orient.server.distributed.ODistributedRequest.EXECUTION_MODE;
import com.orientechnologies.orient.server.distributed.impl.task.*;
import com.orientechnologies.orient.server.distributed.impl.task.transaction.*;
import com.orientechnologies.orient.server.distributed.task.*;

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

  private static final boolean SYNC_TX_COMPLETED = false;
  private ONewDistributedResponseManager responseManager;

  public ONewDistributedTransactionManager(final ODistributedStorage storage, final ODistributedServerManager manager,
      final ODistributedDatabase iDDatabase) {
    this.dManager = manager;
    this.storage = storage;
    this.localDistributedDatabase = iDDatabase;
  }

  public List<ORecordOperation> commit(final ODatabaseDocumentDistributed database, final OTransactionOptimistic iTx,
      final ODistributedStorageEventListener eventListener) {
    final String localNodeName = dManager.getLocalNodeName();

    OTransactionInternal.setStatus(iTx, OTransaction.TXSTATUS.BEGUN);

    final ODistributedConfiguration dbCfg = dManager.getDatabaseConfiguration(storage.getName());

    // CHECK THE LOCAL NODE IS THE OWNER OF THE CLUSTER IDS
    checkForClusterIds(iTx);

    final ODistributedRequestId requestId = new ODistributedRequestId(dManager.getLocalNodeId(),
        dManager.getNextMessageIdCounter());

    final Set<String> involvedClusters = getInvolvedClusters(iTx.getAllRecordEntries());
    Set<String> nodes = getAvailableNodesButLocal(dbCfg, involvedClusters, localNodeName);
    final OTransactionPhase1Task txTask = !nodes.isEmpty() ? createTxTask(iTx, nodes) : null;

    OTransactionResultPayload localResult = OTransactionPhase1Task.executeTransaction(requestId, database, iTx);

    try {
      localDistributedDatabase.getSyncConfiguration()
          .setLastLSN(localNodeName, ((OLocalPaginatedStorage) storage.getUnderlying()).getLSN(), true);
    } catch (IOException e) {
      ODistributedServerLog
          .debug(this, dManager != null ? dManager.getLocalNodeName() : "?", null, ODistributedServerLog.DIRECTION.NONE,
              "Error on updating local LSN configuration for database '%s'", storage.getName());
    }

    if (nodes.isEmpty()) {
      // NO FURTHER NODES TO INVOLVE
      return null;
    }
    //TODO:check the lsn
    txTask.setLastLSN(((OAbstractPaginatedStorage) storage.getUnderlying()).getLSN());

    OTransactionInternal.setStatus(iTx, OTransaction.TXSTATUS.COMMITTING);

    // SYNCHRONOUS CALL: REPLICATE IT
    ((ODistributedAbstractPlugin) dManager)
        .sendRequest(storage.getName(), involvedClusters, nodes, txTask, requestId.getMessageId(), EXECUTION_MODE.RESPONSE,
            localResult, null, null,
            ((iRequest, iNodes, endCallback, task, nodesConcurToTheQuorum, availableNodes, expectedResponses, quorum, groupByResponse, waitLocalNode) -> {
              responseManager = new ONewDistributedResponseManager(txTask, iNodes, nodesConcurToTheQuorum, availableNodes,
                  expectedResponses, quorum);
              return responseManager;
            }));

    handleResponse(requestId, responseManager, involvedClusters, nodes, database);

    // OK, DISTRIBUTED COMMIT SUCCEED
    //TODO:Get the list of result from local ok, if is needed otherwise remove the ruturn
    return null;

  }

  private void handleResponse(ODistributedRequestId requestId, ONewDistributedResponseManager responseManager,
      Set<String> involvedClusters, Set<String> nodes, ODatabaseDocumentDistributed database) {
    if (responseManager.isQuorumReached()) {
      List<OTransactionResultPayload> results = (List<OTransactionResultPayload>) responseManager.getGenericFinalResponse();
      assert results.size() > 0;
      OTransactionResultPayload resultPayload = results.get(0);
      switch (resultPayload.getResponseType()) {
      case OTxSuccess.ID:
        //Success send ok
        localOk(requestId, database);
        sendPhase2Task(involvedClusters, nodes, new OTransactionPhase2Task(requestId, true));
        break;
      case OTxException.ID:
        //Exception send ko and throws the exception
        localKo(requestId, database);
        sendPhase2Task(involvedClusters, nodes, new OTransactionPhase2Task(requestId, false));
        throw ((OTxException) resultPayload).getException();
      case OTxUniqueIndex.ID: {
        //Unique index quorum error send ko and throw unique index exception
        localKo(requestId, database);
        sendPhase2Task(involvedClusters, nodes, new OTransactionPhase2Task(requestId, false));
        ORID id = ((OTxUniqueIndex) resultPayload).getRecordId();
        String index = ((OTxUniqueIndex) resultPayload).getIndex();
        Object key = ((OTxUniqueIndex) resultPayload).getKey();
        //TODO include all paramenter in response
        throw new ORecordDuplicatedException(
            String.format("Cannot index record %s: found duplicated key '%s' in index '%s' ", id, key, index), index, id);
      }
      case OTxConcurrentModification.ID: {
        //Concurrent modification exception quorum send ko and throw cuncurrent modification exception
        localKo(requestId, database);
        sendPhase2Task(involvedClusters, nodes, new OTransactionPhase2Task(requestId, false));
        ORID id = ((OTxConcurrentModification) resultPayload).getRecordId();
        int version = ((OTxConcurrentModification) resultPayload).getVersion();
        //TODO include all paramenter in response
        throw new OConcurrentModificationException(id, version, 0, 0);
      }
      case OTxLockTimeout.ID:
        //TODO: probably a retry
        break;
      }
    } else {
      localKo(requestId, database);
      sendPhase2Task(involvedClusters, nodes, new OTransactionPhase2Task(requestId, false));
      throw new ODistributedOperationException("quorum not reached");
    }

  }

  private void localKo(ODistributedRequestId requestId, ODatabaseDocumentDistributed database) {
    database.rollback2pc(requestId);

  }

  private void localOk(ODistributedRequestId requestId, ODatabaseDocumentDistributed database) {
    database.commit2pcLocal(requestId);
  }

  private void sendPhase2Task(Set<String> involvedClusters, Set<String> nodes, OTransactionPhase2Task task) {
    dManager.sendRequest(storage.getName(), involvedClusters, nodes, task, dManager.getNextMessageIdCounter(),
        EXECUTION_MODE.NO_RESPONSE, null, null, null);
  }

  protected void checkForClusterIds(final OTransaction iTx) {
    for (ORecordOperation op : iTx.getAllRecordEntries()) {
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

  protected OTransactionPhase1Task createTxTask(final OTransactionOptimistic uResult, final Set<String> nodes) {
    final OTransactionPhase1Task txTask = (OTransactionPhase1Task) dManager.getTaskFactoryManager().getFactoryByServerNames(nodes)
        .createTask(OTransactionPhase1Task.FACTORYID);
    txTask.init(uResult);
    return txTask;
  }

}
