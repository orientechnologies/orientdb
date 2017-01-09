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

import com.orientechnologies.common.concur.OOfflineNodeException;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.profiler.OAbstractProfiler;
import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.exception.OSecurityAccessException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.server.OSystemDatabase;
import com.orientechnologies.orient.server.distributed.*;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog.DIRECTION;
import com.orientechnologies.orient.server.distributed.impl.task.OUnreachableServerLocalTask;
import com.orientechnologies.orient.server.distributed.impl.task.OWaitForTask;
import com.orientechnologies.orient.server.distributed.task.OAbstractRemoteTask;
import com.orientechnologies.orient.server.distributed.task.ODistributedOperationException;
import com.orientechnologies.orient.server.distributed.task.ODistributedRecordLockedException;
import com.orientechnologies.orient.server.distributed.task.ORemoteTask;
import com.orientechnologies.orient.server.hazelcast.OHazelcastPlugin;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Distributed database implementation. There is one instance per database. Each node creates own instance to talk with each others.
 *
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 */
public class ODistributedDatabaseImpl implements ODistributedDatabase {
  public static final  String           DISTRIBUTED_SYNC_JSON_FILENAME = "distributed-sync.json";
  private static final String           NODE_LOCK_PREFIX               = "orientdb.reqlock.";
  private static final HashSet<Integer> ALL_QUEUES                     = new HashSet<Integer>();
  protected final ODistributedAbstractPlugin     manager;
  protected final ODistributedMessageServiceImpl msgService;
  protected final String                         databaseName;
  protected       ODistributedDatabaseRepairer   repairer;
  protected       ODistributedSyncConfiguration  syncConfiguration;
  protected ConcurrentHashMap<ORID, ODistributedLock> lockManager = new ConcurrentHashMap<ORID, ODistributedLock>(256);

  protected       ConcurrentHashMap<ODistributedRequestId, ODistributedTxContext> activeTxContexts = new ConcurrentHashMap<ODistributedRequestId, ODistributedTxContext>(
      64);
  protected final List<ODistributedWorker>                                        workerThreads    = new ArrayList<ODistributedWorker>();

  private          AtomicLong                            totalSentRequests     = new AtomicLong();
  private          AtomicLong                            totalReceivedRequests = new AtomicLong();
  private          TimerTask                             txTimeoutTask         = null;
  private          CountDownLatch                        waitForOnline         = new CountDownLatch(1);
  private volatile boolean                               running               = true;
  private          AtomicBoolean                         parsing               = new AtomicBoolean(true);
  private final    AtomicReference<ODistributedMomentum> filterByMomentum      = new AtomicReference<ODistributedMomentum>();

  private String localNodeName;

  public class ODistributedLock {
    final ODistributedRequestId reqId;
    final CountDownLatch        lock;
    final long                  acquiredOn;

    private ODistributedLock(final ODistributedRequestId reqId) {
      this.reqId = reqId;
      this.lock = new CountDownLatch(1);
      this.acquiredOn = System.currentTimeMillis();
    }
  }

  private class ODatabaseExclusiveDistributedLock {
    final String         server;
    final long           acquiredOn;
    final CountDownLatch lock;

    private ODatabaseExclusiveDistributedLock(final String server) {
      this.server = server;
      this.acquiredOn = System.currentTimeMillis();
      this.lock = new CountDownLatch(1);
    }
  }

  public ODistributedDatabaseImpl(final OHazelcastPlugin manager, final ODistributedMessageServiceImpl msgService,
      final String iDatabaseName, final ODistributedConfiguration cfg) {
    this.manager = manager;
    this.msgService = msgService;
    this.databaseName = iDatabaseName;
    this.localNodeName = manager.getLocalNodeName();

    // SELF REGISTERING ITSELF HERE BECAUSE IT'S NEEDED FURTHER IN THE CALL CHAIN
    final ODistributedDatabaseImpl prev = msgService.databases.put(iDatabaseName, this);
    if (prev != null) {
      // KILL THE PREVIOUS ONE
      prev.shutdown();
    }

    startAcceptingRequests();

    if (iDatabaseName.equals(OSystemDatabase.SYSTEM_DB_NAME))
      return;

    checkLocalNodeInConfiguration(cfg);

    startTxTimeoutTimerTask();

    repairer = new OConflictResolverDatabaseRepairer(manager, databaseName);

    Orient.instance().getProfiler()
        .registerHookValue("distributed.db." + databaseName + ".msgSent", "Number of replication messages sent from current node",
            OProfiler.METRIC_TYPE.COUNTER, new OAbstractProfiler.OProfilerHookValue() {
              @Override
              public Object getValue() {
                return totalSentRequests.get();
              }
            }, "distributed.db.*.msgSent");

    Orient.instance().getProfiler().registerHookValue("distributed.db." + databaseName + ".msgReceived",
        "Number of replication messages received from external nodes", OProfiler.METRIC_TYPE.COUNTER,
        new OAbstractProfiler.OProfilerHookValue() {
          @Override
          public Object getValue() {
            return totalReceivedRequests.get();
          }
        }, "distributed.db.*.msgReceived");

    Orient.instance().getProfiler()
        .registerHookValue("distributed.db." + databaseName + ".activeContexts", "Number of active distributed transactions",
            OProfiler.METRIC_TYPE.COUNTER, new OAbstractProfiler.OProfilerHookValue() {
              @Override
              public Object getValue() {
                return (long) activeTxContexts.size();
              }
            }, "distributed.db.*.activeContexts");

    Orient.instance().getProfiler()
        .registerHookValue("distributed.db." + databaseName + ".workerThreads", "Number of worker threads",
            OProfiler.METRIC_TYPE.COUNTER, new OAbstractProfiler.OProfilerHookValue() {
              @Override
              public Object getValue() {
                return (long) workerThreads.size();
              }
            }, "distributed.db.*.workerThreads");

    Orient.instance().getProfiler().registerHookValue("distributed.db." + databaseName + ".recordLocks", "Number of record locked",
        OProfiler.METRIC_TYPE.COUNTER, new OAbstractProfiler.OProfilerHookValue() {
          @Override
          public Object getValue() {
            return (long) lockManager.size();
          }
        }, "distributed.db.*.recordLocks");
  }

  public OLogSequenceNumber getLastLSN(final String server) {
    if (server == null)
      return null;
    return getSyncConfiguration().getLastLSN(server);
  }

  @Override
  public void waitForOnline() {
    try {
      waitForOnline.await();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      // IGNORE IT
    }
  }

  /**
   * Distributed requests against the available workers by using one queue per worker. This guarantee the sequence of the operations
   * against the same record cluster.
   */
  public void processRequest(final ODistributedRequest request) {
    if (!running)
      // DISCARD IT
      return;

    final ORemoteTask task = request.getTask();

    waitIsReady(task);

    if (!running)
      // DISCARD IT
      return;

    totalReceivedRequests.incrementAndGet();

    // final ODistributedMomentum lastMomentum = filterByMomentum.get();
    // if (lastMomentum != null && task instanceof OAbstractReplicatedTask) {
    // final OLogSequenceNumber taskLastLSN = ((OAbstractReplicatedTask) task).getLastLSN();
    //
    // final String sourceServer = manager.getNodeNameById(request.getId().getNodeId());
    // final OLogSequenceNumber lastLSNFromMomentum = lastMomentum.getLSN(sourceServer);
    //
    // if (taskLastLSN != null && lastLSNFromMomentum != null && taskLastLSN.compareTo(lastLSNFromMomentum) < 0) {
    // // SKIP REQUEST BECAUSE CONTAINS AN OLD LSN
    // final String msg = String.format("Skipped request %s on database '%s' because %s < current %s", request, databaseName,
    // taskLastLSN, lastLSNFromMomentum);
    // ODistributedServerLog.info(this, localNodeName, null, DIRECTION.NONE, msg);
    // ODistributedWorker.sendResponseBack(this, manager, request, new ODistributedException(msg));
    // return;
    // }
    // }

    final int[] partitionKeys = task.getPartitionKey();

    if (ODistributedServerLog.isDebugEnabled())
      ODistributedServerLog
          .debug(this, localNodeName, task.getNodeSource(), DIRECTION.IN, "Request %s on database '%s' partitionKeys=%s task=%s",
              request, databaseName, Arrays.toString(partitionKeys), task);

    if (partitionKeys.length > 1 || partitionKeys[0] == -1) {

      final Set<Integer> involvedWorkerQueues;
      if (partitionKeys.length > 1)
        involvedWorkerQueues = getInvolvedQueuesByPartitionKeys(partitionKeys);
      else
        // LOCK ALL THE QUEUES
        involvedWorkerQueues = ALL_QUEUES;

      // if (ODistributedServerLog.isDebugEnabled())
      ODistributedServerLog
          .debug(this, localNodeName, null, DIRECTION.NONE, "Request %s on database '%s' involvedQueues=%s", request, databaseName,
              involvedWorkerQueues);

      if (involvedWorkerQueues.size() == 1)
        // JUST ONE QUEUE INVOLVED: PROCESS IT IMMEDIATELY
        processRequest(involvedWorkerQueues.iterator().next(), request);
      else {
        // INVOLVING MULTIPLE QUEUES

        // if (ODistributedServerLog.isDebugEnabled())
        ODistributedServerLog.debug(this, localNodeName, null, DIRECTION.NONE,
            "Request %s on database '%s' waiting for all the previous requests to be completed", request, databaseName);

        // WAIT ALL THE INVOLVED QUEUES ARE FREE AND SYNCHRONIZED
        final CountDownLatch syncLatch = new CountDownLatch(involvedWorkerQueues.size());
        final ODistributedRequest syncRequest = new ODistributedRequest(manager.getTaskFactory(), request.getId().getNodeId(), -1,
            databaseName, new OSynchronizedTaskWrapper(syncLatch));
        for (int queue : involvedWorkerQueues)
          workerThreads.get(queue).processRequest(syncRequest);

        long taskTimeout = task.getDistributedTimeout();
        try {
          if (taskTimeout <= 0)
            syncLatch.await();
          else {
            // WAIT FOR COMPLETION. THE TIMEOUT IS MANAGED IN SMALLER CYCLES TO PROPERLY RECOGNIZE WHEN THE DB IS REMOVED
            final long start = System.currentTimeMillis();
            final long cycleTimeout = Math.min(taskTimeout, 2000);

            boolean locked = false;
            do {
              if (syncLatch.await(cycleTimeout, TimeUnit.MILLISECONDS)) {
                // DONE
                locked = true;
                break;
              }

              if (this.workerThreads.size() == 0)
                // DATABASE WAS SHUTDOWN
                break;

            } while (System.currentTimeMillis() - start < taskTimeout);

            if (!locked) {
              final String msg = String
                  .format("Cannot execute distributed request (%s) because all worker threads (%d) are busy (pending=%d)", request,
                      workerThreads.size(), syncLatch.getCount());
              ODistributedWorker.sendResponseBack(this, manager, request, new ODistributedOperationException(msg));
              return;
            }
          }
        } catch (InterruptedException e) {
          // IGNORE
          Thread.currentThread().interrupt();
          final String msg = String
              .format("Cannot execute distributed request (%s) because all worker threads (%d) are busy", request,
                  workerThreads.size());
          ODistributedWorker.sendResponseBack(this, manager, request, new ODistributedOperationException(msg));
          return;
        }

        // PUT THE TASK TO EXECUTE ONLY IN THE FIRST QUEUE AND PUT WAIT-FOR TASKS IN THE OTHERS. WHEN THE REAL TASK IS EXECUTED,
        // ALL THE OTHER TASKS WILL RETURN, SO THE QUEUES WILL BE BUSY DURING THE EXECUTION OF THE TASK. THIS AVOID CONCURRENT
        // EXECUTION FOR THE SAME PARTITION
        final CountDownLatch queueLatch = new CountDownLatch(1);

        int i = 0;
        for (int queue : involvedWorkerQueues) {
          final ODistributedRequest req;
          if (i++ == 0) {
            // USE THE FIRST QUEUE TO PROCESS THE REQUEST
            final String senderNodeName = manager.getNodeNameById(request.getId().getNodeId());
            request.setTask(new OSynchronizedTaskWrapper(queueLatch, senderNodeName, task));
            req = request;
          } else
            req = new ODistributedRequest(manager.getTaskFactory(), request.getId().getNodeId(), -1, databaseName,
                new OWaitForTask(queueLatch));

          workerThreads.get(queue).processRequest(req);
        }
      }
    } else if (partitionKeys.length > 1 || partitionKeys[0] == -2) {
      // ANY PARTITION: USE THE FIRST EMPTY IF ANY, OTHERWISE THE FIRST IN THE LIST
      boolean found = false;

      for (ODistributedWorker q : workerThreads) {
        if (q.isWaitingForNextRequest() && q.localQueue.isEmpty()) {
          q.processRequest(request);
          found = true;
          break;
        }
      }

      if (!found)
        // ALL THE THREADS ARE BUSY, SELECT THE FIRST EMPTY ONE
        for (ODistributedWorker q : workerThreads) {
          if (q.localQueue.isEmpty()) {
            q.processRequest(request);
            found = true;
            break;
          }
        }

      if (!found)
        // EXEC ON THE FIRST QUEUE
        workerThreads.get(0).processRequest(request);

    } else {
      processRequest(partitionKeys[0], request);
    }
  }

  public void waitIsReady(ORemoteTask task) {
    if (task.isNodeOnlineRequired())
      if (!parsing.get()) {
        // WAIT FOR PARSING REQUESTS
        while (!parsing.get()) {
          try {
            Thread.sleep(300);
          } catch (InterruptedException e) {
            break;
          }
        }
      }
  }

  protected Set<Integer> getInvolvedQueuesByPartitionKeys(final int[] partitionKeys) {
    final Set<Integer> involvedWorkerQueues = new HashSet<Integer>(partitionKeys.length);
    for (int pk : partitionKeys) {
      if (pk >= 0)
        involvedWorkerQueues.add(pk % workerThreads.size());
    }
    return involvedWorkerQueues;
  }

  protected void processRequest(final int partitionKey, final ODistributedRequest request) {
    if (workerThreads.isEmpty())
      throw new ODistributedException("There are no worker threads to process request " + request);

    final int partition = partitionKey % workerThreads.size();

    ODistributedServerLog.debug(this, localNodeName, request.getTask().getNodeSource(), DIRECTION.IN,
        "Request %s on database '%s' dispatched to the worker %d", request, databaseName, partition);

    workerThreads.get(partition).processRequest(request);
  }

  @Override
  public ODistributedResponse send2Nodes(final ODistributedRequest iRequest, final Collection<String> iClusterNames,
      Collection<String> iNodes, final ODistributedRequest.EXECUTION_MODE iExecutionMode, final Object localResult,
      final OCallable<Void, ODistributedRequestId> iAfterSentCallback) {
    boolean afterSendCallBackCalled = false;
    try {
      checkForServerOnline(iRequest);

      final String databaseName = iRequest.getDatabaseName();

      if (iNodes.isEmpty()) {
        ODistributedServerLog
            .error(this, localNodeName, null, DIRECTION.OUT, "No nodes configured for database '%s' request: %s", databaseName,
                iRequest);
        throw new ODistributedException("No nodes configured for partition '" + databaseName + "' request: " + iRequest);
      }

      final ODistributedConfiguration cfg = manager.getDatabaseConfiguration(databaseName);

      final ORemoteTask task = iRequest.getTask();

      final boolean checkNodesAreOnline = task.isNodeOnlineRequired();

      final Set<String> nodesConcurToTheQuorum = manager.getDistributedStrategy()
          .getNodesConcurInQuorum(manager, cfg, iRequest, iNodes, localResult);

      // AFTER COMPUTED THE QUORUM, REMOVE THE OFFLINE NODES TO HAVE THE LIST OF REAL AVAILABLE NODES
      final int availableNodes = checkNodesAreOnline ?
          manager.getNodesWithStatus(iNodes, databaseName, ODistributedServerManager.DB_STATUS.ONLINE,
              ODistributedServerManager.DB_STATUS.BACKUP, ODistributedServerManager.DB_STATUS.SYNCHRONIZING) :
          iNodes.size();

      final int expectedResponses = localResult != null ? availableNodes + 1 : availableNodes;

      final int quorum = calculateQuorum(task.getQuorumType(), iClusterNames, cfg, expectedResponses, nodesConcurToTheQuorum.size(),
          checkNodesAreOnline, localNodeName);

      final boolean groupByResponse = task.getResultStrategy() != OAbstractRemoteTask.RESULT_STRATEGY.UNION;

      final boolean waitLocalNode = waitForLocalNode(cfg, iClusterNames, iNodes);

      // CREATE THE RESPONSE MANAGER
      final ODistributedResponseManager currentResponseMgr = new ODistributedResponseManager(manager, iRequest, iNodes,
          nodesConcurToTheQuorum, expectedResponses, quorum, waitLocalNode, task.getSynchronousTimeout(expectedResponses),
          task.getTotalTimeout(availableNodes), groupByResponse);

      if (localResult != null)
        // COLLECT LOCAL RESULT
        currentResponseMgr.setLocalResult(localNodeName, localResult);

      // SORT THE NODE TO GUARANTEE THE SAME ORDER OF DELIVERY
      if (!(iNodes instanceof List))
        iNodes = new ArrayList<String>(iNodes);
      if (iNodes.size() > 1)
        Collections.sort((List<String>) iNodes);

      msgService.registerRequest(iRequest.getId().getMessageId(), currentResponseMgr);

      if (ODistributedServerLog.isDebugEnabled())
        ODistributedServerLog.debug(this, localNodeName, iNodes.toString(), DIRECTION.OUT, "Sending request %s...", iRequest);

      for (String node : iNodes) {
        // CATCH ANY EXCEPTION LOG IT AND IGNORE TO CONTINUE SENDING REQUESTS TO OTHER NODES
        try {
          final ORemoteServerController remoteServer = manager.getRemoteServer(node);

          remoteServer.sendRequest(iRequest);

        } catch (Throwable e) {
          currentResponseMgr.removeServerBecauseUnreachable(node);

          String reason = e.getMessage();
          if (e instanceof ODistributedException && e.getCause() instanceof IOException) {
            // CONNECTION ERROR: REMOVE THE CONNECTION
            reason = e.getCause().getMessage();
            manager.closeRemoteServer(node);

          } else if (e instanceof OSecurityAccessException) {
            // THE CONNECTION COULD BE STALE, CREATE A NEW ONE AND RETRY
            manager.closeRemoteServer(node);
            try {
              final ORemoteServerController remoteServer = manager.getRemoteServer(node);
              remoteServer.sendRequest(iRequest);
              continue;

            } catch (Throwable ex) {
              // IGNORE IT BECAUSE MANAGED BELOW
            }
          }

          if (!manager.isNodeAvailable(node))
            // NODE IS NOT AVAILABLE
            ODistributedServerLog.debug(this, localNodeName, node, ODistributedServerLog.DIRECTION.OUT,
                "Error on sending distributed request %s. The target node is not available. Active nodes: %s", e, iRequest,
                manager.getAvailableNodeNames(databaseName));
          else
            ODistributedServerLog.error(this, localNodeName, node, ODistributedServerLog.DIRECTION.OUT,
                "Error on sending distributed request %s (err=%s). Active nodes: %s", iRequest, reason,
                manager.getAvailableNodeNames(databaseName));
        }
      }

      if (ODistributedServerLog.isDebugEnabled())
        ODistributedServerLog.debug(this, localNodeName, iNodes.toString(), DIRECTION.OUT, "Sent request %s", iRequest);

      totalSentRequests.incrementAndGet();

      afterSendCallBackCalled = true;

      if (iAfterSentCallback != null)
        iAfterSentCallback.call(iRequest.getId());

      if (iExecutionMode == ODistributedRequest.EXECUTION_MODE.RESPONSE)
        return waitForResponse(iRequest, currentResponseMgr);

      return null;

    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw OException.wrapException(new ODistributedException(
          "Error on executing distributed request (" + iRequest + ") against database '" + databaseName + (iClusterNames != null ?
              "." + iClusterNames :
              "") + "' to nodes " + iNodes), e);
    } finally {
      if (iAfterSentCallback != null && !afterSendCallBackCalled)
        iAfterSentCallback.call(iRequest.getId());
    }
  }

  @Override
  public void setOnline() {
    ODistributedServerLog
        .info(this, localNodeName, null, DIRECTION.NONE, "Publishing ONLINE status for database %s.%s...", localNodeName,
            databaseName);

    // SET THE NODE.DB AS ONLINE
    manager.setDatabaseStatus(localNodeName, databaseName, ODistributedServerManager.DB_STATUS.ONLINE);

    waitForOnline.countDown();
  }

  @Override
  public boolean lockRecord(final OIdentifiable iRecord, final ODistributedRequestId iRequestId, final long timeout) {
    final ORID rid = iRecord.getIdentity();

    final ODistributedLock lock = new ODistributedLock(iRequestId);

    boolean newLock = true;

    ODistributedLock currentLock = lockManager.putIfAbsent(rid, lock);
    if (currentLock != null) {
      if (iRequestId.equals(currentLock.reqId)) {
        // SAME ID, ALREADY LOCKED
        ODistributedServerLog.debug(this, localNodeName, null, DIRECTION.NONE,
            "Distributed transaction: %s locked record %s in database '%s' owned by %s (thread=%d)", iRequestId, iRecord,
            databaseName, currentLock.reqId, Thread.currentThread().getId());
        currentLock = null;
        newLock = false;
      } else {
        // TRY TO RE-LOCK IT UNTIL TIMEOUT IS EXPIRED
        final long startTime = System.currentTimeMillis();
        do {
          try {
            if (timeout > 0) {
              if (!currentLock.lock.await(timeout, TimeUnit.MILLISECONDS))
                continue;
            } else
              currentLock.lock.await();

            currentLock = lockManager.putIfAbsent(rid, lock);

          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            break;
          }
        } while (currentLock != null && (timeout == 0 || System.currentTimeMillis() - startTime < timeout));
      }
    }

//    if (currentLock != null) {
//      // CHECK THE OWNER SERVER IS ONLINE. THIS AVOIDS ANY "WALKING DEAD" LOCKS
//      final String lockingNodeName = manager.getNodeNameById(currentLock.reqId.getNodeId());
//      if (lockingNodeName == null || !manager.isNodeAvailable(lockingNodeName)) {
//        ODistributedServerLog.info(this, localNodeName, null, DIRECTION.NONE,
//            "Distributed transaction: forcing unlock of record %s in database '%s' because the owner server '%s' is offline (reqId=%s ownerReqId=%s, thread=%d)",
//            iRecord.getIdentity(), databaseName, lockingNodeName, iRequestId, currentLock.reqId, Thread.currentThread().getId());
//
//        // FORCE THE UNLOCK AND LOCK OF CURRENT REQ-ID
//        lockManager.put(rid, lock);
//        currentLock = null;
//      }
//    }
//
    if (ODistributedServerLog.isDebugEnabled())
      if (currentLock == null) {
        ODistributedServerLog.debug(this, localNodeName, null, DIRECTION.NONE,
            "Distributed transaction: %s locked record %s in database '%s' (thread=%d)", iRequestId, iRecord, databaseName,
            Thread.currentThread().getId());
      } else {
        ODistributedServerLog.debug(this, localNodeName, null, DIRECTION.NONE,
            "Distributed transaction: %s cannot lock record %s in database '%s' owned by %s (thread=%d)", iRequestId, iRecord,
            databaseName, currentLock.reqId, Thread.currentThread().getId());
      }

    if (currentLock != null)
      throw new ODistributedRecordLockedException(manager.getLocalNodeName(), rid, currentLock.reqId, timeout);

    // DUMP STACK TRACE
    // OException.dumpStackTrace(String.format("Distributed transaction: %s locked record %s in database '%s' (thread=%d)",
    // iRequestId,
    // iRecord, databaseName, Thread.currentThread().getId()));

    return newLock;
  }

  @Override
  public void unlockRecord(final OIdentifiable iRecord, final ODistributedRequestId requestId) {
    if (requestId == null)
      return;

    final ODistributedLock owner = lockManager.remove(iRecord.getIdentity());
    if (owner != null) {
      if (!owner.reqId.equals(requestId)) {
        ODistributedServerLog.error(this, localNodeName, null, DIRECTION.NONE,
            "Distributed transaction: cannot unlock record %s in database '%s' because owner %s <> current %s (thread=%d)", iRecord,
            databaseName, owner.reqId, requestId, Thread.currentThread().getId());
        return;
      }

      // DUMP STACK TRACE
      // OException
      // .dumpStackTrace(String.format("Distributed transaction: %s unlocked record %s in database '%s' (owner=%s, thread=%d)",
      // requestId, iRecord, databaseName, owner != null ? owner.reqId : "null", Thread.currentThread().getId()));

      // NOTIFY ANY WAITERS
      owner.lock.countDown();
    }

    if (ODistributedServerLog.isDebugEnabled())
      ODistributedServerLog.debug(this, localNodeName, null, DIRECTION.NONE,
          "Distributed transaction: %s unlocked record %s in database '%s' (owner=%s, thread=%d)", requestId, iRecord, databaseName,
          owner != null ? owner.reqId : "null", Thread.currentThread().getId());
  }

  @Override
  public void unlockResourcesOfServer(final ODatabaseDocumentInternal database, final String serverName) {
    final int nodeLeftId = manager.getNodeIdByName(serverName);

    int rollbacks = 0;
    final Iterator<ODistributedTxContext> pendingReqIterator = activeTxContexts.values().iterator();
    while (pendingReqIterator.hasNext()) {
      final ODistributedTxContext pReq = pendingReqIterator.next();
      if (pReq != null && pReq.getReqId().getNodeId() == nodeLeftId) {
        try {
          pReq.rollback(database);
          rollbacks++;
        } catch (Throwable t) {
          // IGNORE IT
          ODistributedServerLog.error(this, manager.getLocalNodeName(), null, DIRECTION.NONE,
              "Distributed transaction: error on rolling back transaction (req=%s)", pReq.getReqId());
        }
        pReq.destroy();
        pendingReqIterator.remove();
      }
    }

    int recordLocks = 0;
    for (Map.Entry<ORID, ODistributedDatabaseImpl.ODistributedLock> entry : lockManager.entrySet()) {
      final ODistributedDatabaseImpl.ODistributedLock lock = entry.getValue();
      if (lock != null && lock.reqId != null && lock.reqId.getNodeId() == nodeLeftId) {
        OLogManager.instance().debug(this, "Unlocking record %s acquired with req=%s", entry.getKey(), lock.reqId);
        recordLocks++;
      }
    }

    ODistributedServerLog.info(this, localNodeName, null, DIRECTION.NONE,
        "Distributed transaction: rolled back %d transactions and %d single locks in database '%s' owned by server '%s'", rollbacks,
        recordLocks, databaseName, serverName);
  }

  @Override
  public ODistributedTxContext registerTxContext(final ODistributedRequestId reqId) {
    ODistributedTxContext ctx = new ODistributedTxContextImpl(this, reqId);

    final ODistributedTxContext prevCtx = activeTxContexts.putIfAbsent(reqId, ctx);
    if (prevCtx != null) {
      // ALREADY EXISTENT
      ODistributedServerLog.debug(this, localNodeName, null, DIRECTION.NONE,
          "Distributed transaction: repeating request %s in database '%s' (thread=%d)", reqId, databaseName,
          Thread.currentThread().getId());
      ctx = prevCtx;
    } else
      // REGISTERED
      ODistributedServerLog.debug(this, localNodeName, null, DIRECTION.NONE,
          "Distributed transaction: registered request %s in database '%s' (thread=%d)", reqId, databaseName,
          Thread.currentThread().getId());

    return ctx;
  }

  @Override
  public ODistributedTxContext popTxContext(final ODistributedRequestId requestId) {
    final ODistributedTxContext ctx = activeTxContexts.remove(requestId);
    ODistributedServerLog
        .debug(this, localNodeName, null, DIRECTION.NONE, "Distributed transaction: pop request %s for database %s -> %s",
            requestId, databaseName, ctx);
    return ctx;
  }

  @Override
  public ODistributedServerManager getManager() {
    return manager;
  }

  public boolean exists() {
    try {
      manager.getServerInstance().getStoragePath(databaseName);
      return true;
    } catch (OConfigurationException e) {
      return false;
    }
  }

  public ODistributedSyncConfiguration getSyncConfiguration() {
    if (syncConfiguration == null) {
      final String path = manager.getServerInstance().getDatabaseDirectory() + databaseName + "/" + DISTRIBUTED_SYNC_JSON_FILENAME;
      final File cfgFile = new File(path);
      try {
        syncConfiguration = new ODistributedSyncConfiguration(manager, databaseName, cfgFile);
      } catch (IOException e) {
        throw new ODistributedException("Cannot open database distributed sync configuration file: " + cfgFile);
      }
    }

    return syncConfiguration;
  }

  public void filterBeforeThisMomentum(final ODistributedMomentum momentum) {
    this.filterByMomentum.set(momentum);
  }

  @Override
  public void handleUnreachableNode(final String nodeName) {
    ODistributedServerLog.debug(this, manager.getLocalNodeName(), nodeName, DIRECTION.IN,
        "Distributed transaction: rolling back all the pending transactions coordinated by the unreachable server '%s'", nodeName);

    final OUnreachableServerLocalTask task = new OUnreachableServerLocalTask(nodeName);
    final ODistributedRequest rollbackRequest = new ODistributedRequest(manager.getTaskFactory(), manager.getLocalNodeId(),
        manager.getNextMessageIdCounter(), null, task);
    processRequest(rollbackRequest);
  }

  @Override
  public String getDatabaseName() {
    return databaseName;
  }

  @Override
  public ODatabaseDocumentTx getDatabaseInstance() {
    return manager.getServerInstance().openDatabase(databaseName, "internal", "internal", null, true);
  }

  @Override
  public long getReceivedRequests() {
    return totalReceivedRequests.get();
  }

  @Override
  public long getProcessedRequests() {
    long total = 0;
    for (ODistributedWorker workerThread : workerThreads) {
      total += workerThread.getProcessedRequests();
    }

    return total;
  }

  public void shutdown() {
    running = false;

    try {
      if (txTimeoutTask != null)
        txTimeoutTask.cancel();

      if (repairer != null)
        repairer.shutdown();

      // SEND THE SHUTDOWN TO ALL THE WORKER THREADS
      for (ODistributedWorker workerThread : workerThreads) {
        if (workerThread != null)
          workerThread.sendShutdown();
      }

      // WAIT A BIT FOR PROPER SHUTDOWN
      for (ODistributedWorker workerThread : workerThreads) {
        if (workerThread != null) {
          try {
            workerThread.join(2000);
          } catch (InterruptedException e) {
          }
        }
      }
      workerThreads.clear();

      // SAVE SYNC CONFIGURATION
      try {
        getSyncConfiguration().save();
      } catch (IOException e) {
        ODistributedServerLog
            .warn(this, localNodeName, null, DIRECTION.NONE, "Error on saving distributed LSN table for database '%s'",
                databaseName);
      }
      syncConfiguration = null;

      ODistributedServerLog.info(this, localNodeName, null, DIRECTION.NONE,
          "Shutting down distributed database manager '%s'. Pending objects: txs=%d locks=%d", databaseName,
          activeTxContexts.size(), lockManager.size());

      lockManager.clear();
      activeTxContexts.clear();

      Orient.instance().getProfiler().unregisterHookValue("distributed.db." + databaseName + ".msgSent");
      Orient.instance().getProfiler().unregisterHookValue("distributed.db." + databaseName + ".msgReceived");
      Orient.instance().getProfiler().unregisterHookValue("distributed.db." + databaseName + ".activeContexts");
      Orient.instance().getProfiler().unregisterHookValue("distributed.db." + databaseName + ".workerThreads");
      Orient.instance().getProfiler().unregisterHookValue("distributed.db." + databaseName + ".recordLocks");

    } finally {

      final ODistributedServerManager.DB_STATUS serverStatus = manager.getDatabaseStatus(manager.getLocalNodeName(), databaseName);
      if (serverStatus == ODistributedServerManager.DB_STATUS.ONLINE
          || serverStatus == ODistributedServerManager.DB_STATUS.SYNCHRONIZING) {
        try {
          manager.setDatabaseStatus(manager.getLocalNodeName(), databaseName, ODistributedServerManager.DB_STATUS.NOT_AVAILABLE);
        } catch (Throwable e) {
          // IGNORE IT
        }
      }

    }
  }

  protected void checkForServerOnline(final ODistributedRequest iRequest) throws ODistributedException {
    final ODistributedServerManager.NODE_STATUS srvStatus = manager.getNodeStatus();
    if (srvStatus == ODistributedServerManager.NODE_STATUS.OFFLINE
        || srvStatus == ODistributedServerManager.NODE_STATUS.SHUTTINGDOWN) {
      ODistributedServerLog
          .error(this, localNodeName, null, DIRECTION.OUT, "Local server is not online (status='%s'). Request %s will be ignored",
              srvStatus, iRequest);
      throw new OOfflineNodeException(
          "Local server is not online (status='" + srvStatus + "'). Request " + iRequest + " will be ignored");
    }
  }

  protected boolean waitForLocalNode(final ODistributedConfiguration cfg, final Collection<String> iClusterNames,
      final Collection<String> iNodes) {
    boolean waitLocalNode = false;
    if (iNodes.contains(localNodeName))
      if (iClusterNames == null || iClusterNames.isEmpty()) {
        // DEFAULT CLUSTER (*)
        if (cfg.isReadYourWrites(null))
          waitLocalNode = true;
      } else
        // BROWSE FOR ALL CLUSTER TO GET THE FIRST 'waitLocalNode'
        for (String clName : iClusterNames) {
          if (cfg.isReadYourWrites(clName)) {
            waitLocalNode = true;
            break;
          }
        }
    return waitLocalNode;
  }

  protected int calculateQuorum(final OCommandDistributedReplicateRequest.QUORUM_TYPE quorumType, Collection<String> clusterNames,
      final ODistributedConfiguration cfg, final int allAvailableNodes, final int masterAvailableNodes,
      final boolean checkNodesAreOnline, final String localNodeName) {

    int quorum = 1;

    if (clusterNames == null || clusterNames.isEmpty()) {
      clusterNames = new ArrayList<String>(1);
      clusterNames.add(null);
    }

    for (String cluster : clusterNames) {
      int clusterQuorum = 0;
      switch (quorumType) {
      case NONE:
        // IGNORE IT
        break;
      case READ:
        clusterQuorum = cfg.getReadQuorum(cluster, allAvailableNodes, localNodeName);
        break;
      case WRITE:
        clusterQuorum = cfg.getWriteQuorum(cluster, masterAvailableNodes, localNodeName);
        break;
      case ALL:
        clusterQuorum = allAvailableNodes;
        break;
      }

      quorum = Math.max(quorum, clusterQuorum);
    }

    if (quorum < 0)
      quorum = 0;

    if (checkNodesAreOnline && quorum > allAvailableNodes)
      throw new ODistributedException(
          "Quorum (" + quorum + ") cannot be reached on server '" + localNodeName + "' because it is major than available nodes ("
              + allAvailableNodes + ")");

    return quorum;
  }

  protected ODistributedResponse waitForResponse(final ODistributedRequest iRequest,
      final ODistributedResponseManager currentResponseMgr) throws InterruptedException {
    final long beginTime = System.currentTimeMillis();

    // WAIT FOR THE MINIMUM SYNCHRONOUS RESPONSES (QUORUM)
    if (!currentResponseMgr.waitForSynchronousResponses()) {
      final long elapsed = System.currentTimeMillis() - beginTime;

      if (elapsed > currentResponseMgr.getSynchTimeout()) {

        ODistributedServerLog.warn(this, localNodeName, null, DIRECTION.IN,
            "Timeout (%dms) on waiting for synchronous responses from nodes=%s responsesSoFar=%s request=(%s)", elapsed,
            currentResponseMgr.getExpectedNodes(), currentResponseMgr.getRespondingNodes(), iRequest);
      }
    }

    return currentResponseMgr.getFinalResponse();
  }

  protected void checkLocalNodeInConfiguration(final ODistributedConfiguration cfg) {
    manager.executeInDistributedDatabaseLock(databaseName, 0, cfg != null ? cfg.modify() : null,
        new OCallable<Void, OModifiableDistributedConfiguration>() {
          @Override
          public Void call(final OModifiableDistributedConfiguration lastCfg) {
            // GET LAST VERSION IN LOCK
            final List<String> foundPartition = lastCfg.addNewNodeInServerList(localNodeName);
            if (foundPartition != null) {
              ODistributedServerLog
                  .info(this, localNodeName, null, DIRECTION.NONE, "Adding node '%s' in partition: %s db=%s v=%d", localNodeName,
                      foundPartition, databaseName, lastCfg.getVersion());
            }
            return null;
          }
        });
  }

  protected String getLocalNodeName() {
    return localNodeName;
  }

  private void startAcceptingRequests() {
    // START ALL THE WORKER THREADS (CONFIGURABLE)
    final int totalWorkers = OGlobalConfiguration.DISTRIBUTED_DB_WORKERTHREADS.getValueAsInteger();
    if (totalWorkers < 1)
      throw new ODistributedException("Cannot create configured distributed workers (" + totalWorkers + ")");

    for (int i = 0; i < totalWorkers; ++i) {
      final ODistributedWorker workerThread = new ODistributedWorker(this, databaseName, i);
      workerThreads.add(workerThread);
      workerThread.start();

      ALL_QUEUES.add(i);
    }
  }

  @Override
  public void setLSN(final String sourceNodeName, final OLogSequenceNumber taskLastLSN, final boolean updateLastOperationTimestamp)
      throws IOException {
    if (taskLastLSN == null)
      return;

    final ODistributedSyncConfiguration cfg = getSyncConfiguration();
    cfg.setLastLSN(sourceNodeName, taskLastLSN, updateLastOperationTimestamp);
  }

  @Override
  public ODistributedDatabaseRepairer getDatabaseRepairer() {
    return repairer;
  }

  private void startTxTimeoutTimerTask() {
    txTimeoutTask = new TimerTask() {
      @Override
      public void run() {
        ODatabaseDocumentTx database = null;
        try {
          final long now = System.currentTimeMillis();
          final long timeout = OGlobalConfiguration.DISTRIBUTED_TX_EXPIRE_TIMEOUT.getValueAsLong();

          for (final Iterator<ODistributedTxContext> it = activeTxContexts.values().iterator(); it.hasNext(); ) {
            if (!isRunning())
              break;

            final ODistributedTxContext ctx = it.next();
            if (ctx != null) {
              final long started = ctx.getStartedOn();
              final long elapsed = now - started;
              if (elapsed > timeout) {
                // TRANSACTION EXPIRED, ROLLBACK IT

                if (database == null)
                  // GET THE DATABASE THE FIRST TIME
                  database = getDatabaseInstance();

                ODistributedServerLog.debug(this, localNodeName, null, DIRECTION.NONE,
                    "Distributed transaction %s on database '%s' is expired after %dms", ctx.getReqId(), databaseName, elapsed);

                try {
                  ctx.rollback(database);
                  ctx.destroy();
                } catch (Throwable t) {
                  ODistributedServerLog.info(this, localNodeName, null, DIRECTION.NONE,
                      "Error on rolling back distributed transaction %s on database '%s'", ctx.getReqId(), databaseName);
                } finally {
                  it.remove();
                }
              }
            }
          }
        } catch (Throwable t) {
          // CATCH EVERYTHING TO AVOID THE TIMER IS CANCELED
          ODistributedServerLog.info(this, localNodeName, null, DIRECTION.NONE,
              "Error on checking for expired distributed transaction on database '%s'", databaseName);
        } finally {
          if (database != null)
            database.close();
        }
      }
    };

    Orient.instance().scheduleTask(txTimeoutTask, OGlobalConfiguration.DISTRIBUTED_TX_EXPIRE_TIMEOUT.getValueAsLong(),
        OGlobalConfiguration.DISTRIBUTED_TX_EXPIRE_TIMEOUT.getValueAsLong() / 2);
  }

  private boolean isRunning() {
    return running;
  }

  public void suspend() {
    if (this.parsing.get())
      // RESET THE DATABASE
      for (ODistributedWorker w : workerThreads) {
        w.reset();
      }

    this.parsing.set(false);
  }

  public void resume() {
    this.parsing.set(true);
  }

  @Override
  public void dumpLocks() {
    OLogManager.instance().info(this, "Current locks database '%s' server '%s'", databaseName, manager.getLocalNodeName());
    for (Map.Entry<ORID, ODistributedLock> entry : lockManager.entrySet()) {
      OLogManager.instance()
          .info(this, "- %s = %s (count=%d)", entry.getKey(), entry.getValue().reqId, entry.getValue().lock.getCount());
    }
  }
}
