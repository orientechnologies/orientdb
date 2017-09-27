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
import com.orientechnologies.common.concur.lock.OInterruptedException;
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
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.ORecordVersionHelper;
import com.orientechnologies.orient.core.replication.OAsyncReplicationError;
import com.orientechnologies.orient.core.replication.OAsyncReplicationOk;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import com.orientechnologies.orient.core.storage.ORecordDuplicatedException;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OLocalPaginatedStorage;
import com.orientechnologies.orient.core.tx.OTransaction;
import com.orientechnologies.orient.core.tx.OTransactionAbstract;
import com.orientechnologies.orient.core.tx.OTransactionInternal;
import com.orientechnologies.orient.core.tx.OTransactionOptimistic;
import com.orientechnologies.orient.server.distributed.*;
import com.orientechnologies.orient.server.distributed.ODistributedRequest.EXECUTION_MODE;
import com.orientechnologies.orient.server.distributed.impl.task.*;
import com.orientechnologies.orient.server.distributed.impl.task.transaction.*;
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
      final Runnable callback, final ODistributedStorageEventListener eventListener) {
    final String localNodeName = dManager.getLocalNodeName();

    try {
      OTransactionInternal.setStatus(iTx, OTransaction.TXSTATUS.BEGUN);

      final ODistributedConfiguration dbCfg = dManager.getDatabaseConfiguration(storage.getName());

      // CHECK THE LOCAL NODE IS THE OWNER OF THE CLUSTER IDS
      checkForClusterIds(iTx, localNodeName, dbCfg);

      final ODistributedRequestId requestId = new ODistributedRequestId(dManager.getLocalNodeId(),
          dManager.getNextMessageIdCounter());

      final AtomicBoolean releaseContext = new AtomicBoolean(false);

      final AtomicBoolean lockReleased = new AtomicBoolean(true);
      try {
        lockReleased.set(false);
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
          releaseContext.set(true);
          return null;
        }
        try {
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

          handleResponse(requestId, responseManager, involvedClusters, nodes);

          // OK, DISTRIBUTED COMMIT SUCCEED
          //TODO:Get the list of result from local ok, if is needed otherwise remove the ruturn
          return null;

        } catch (Throwable e) {
          ODistributedServerLog.debug(this, dManager.getLocalNodeName(), null, ODistributedServerLog.DIRECTION.NONE,
              "Error on executing transaction on database '%s', rollback... (reqId=%s err='%s')", storage.getName(), requestId, e);

          releaseContext.set(true);

          // ROLLBACK TX
          final OCompleted2pcTask task = (OCompleted2pcTask) dManager.getTaskFactoryManager().getFactoryByServerNames(nodes)
              .createTask(OCompleted2pcTask.FACTORYID);
          task.init(requestId, false, txTask.getPartitionKey());

          sendTxCompleted(localNodeName, involvedClusters, nodes, task);
          if (e instanceof RuntimeException)
            throw (RuntimeException) e;
          else if (e instanceof InterruptedException)
            throw OException.wrapException(new ODistributedOperationException("Cannot commit transaction"), e);
          else
            throw OException.wrapException(new ODistributedException("Cannot commit transaction"), e);
        }

      } catch (OInterruptedException e) {
        releaseContext.set(true);
        throw OException.wrapException(new ODistributedOperationException("Cannot commit transaction"), e);
      } catch (RuntimeException e) {
        releaseContext.set(true);
        throw e;
      } catch (Exception e) {
        releaseContext.set(true);
        throw OException.wrapException(new ODistributedException("Cannot commit transaction"), e);
      } finally {
        if (releaseContext.get() && lockReleased.compareAndSet(false, true)) {
          localDistributedDatabase.popTxContext(requestId);
          //ctx.destroy();
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

      for (ORecordOperation op : iTx.getAllRecordEntries()) {
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

  private void handleResponse(ODistributedRequestId requestId, ONewDistributedResponseManager responseManager,
      Set<String> involvedClusters, Set<String> nodes) {
    if (responseManager.isQuorumReached()) {
      List<OTransactionResultPayload> results = (List<OTransactionResultPayload>) responseManager.getGenericFinalResponse();
      assert results.size() > 0;
      OTransactionResultPayload resultPayload = results.get(0);
      switch (resultPayload.getResponseType()) {
      case OTxSuccess.ID:
        //Success send ok
        localOk(requestId);
        sendPhase2Task(involvedClusters, nodes, new OTransactionPhase2Task(requestId, true));
        break;
      case OTxException.ID:
        //Exception send ko and throws the exception
        localKo(requestId);
        sendPhase2Task(involvedClusters, nodes, new OTransactionPhase2Task(requestId, false));
        throw ((OTxException) resultPayload).getException();
      case OTxUniqueIndex.ID: {
        //Unique index quorum error send ko and throw unique index exception
        localKo(requestId);
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
        localKo(requestId);
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
      localKo(requestId);
      sendPhase2Task(involvedClusters, nodes, new OTransactionPhase2Task(requestId, false));
      throw new ODistributedOperationException("quorum not reached");
    }

  }

  private void localKo(ODistributedRequestId requestId) {

  }

  private void localOk(ODistributedRequestId requestId) {

  }

  private void sendPhase2Task(Set<String> involvedClusters, Set<String> nodes, OTransactionPhase2Task task) {
    dManager.sendRequest(storage.getName(), involvedClusters, nodes, task, dManager.getNextMessageIdCounter(),
        EXECUTION_MODE.NO_RESPONSE, null, null, null);
  }

  protected void checkForClusterIds(final OTransaction iTx, final String localNodeName, final ODistributedConfiguration dbCfg) {
    for (ORecordOperation op : iTx.getAllRecordEntries()) {
      final ORecordId rid = (ORecordId) op.getRecord().getIdentity();
      switch (op.type) {
      case ORecordOperation.CREATED:
        final ORecordId newRid = rid.copy();
        if (rid.getClusterId() < 1) {
          final String clusterName = ((OTransactionAbstract) iTx).getClusterName(op.getRecord());
          if (clusterName != null) {
            newRid.setClusterId(ODatabaseRecordThreadLocal.INSTANCE.get().getClusterIdByName(clusterName));
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

}
