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

import com.orientechnologies.common.concur.OOfflineNodeException;
import com.orientechnologies.common.concur.lock.OSimpleLockManager;
import com.orientechnologies.common.concur.lock.OSimpleLockManagerImpl;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.profiler.OAbstractProfiler;
import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OSecurityAccessException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.OSystemDatabase;
import com.orientechnologies.orient.server.distributed.*;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog.DIRECTION;
import com.orientechnologies.orient.server.distributed.impl.task.ODistributedLockTask;
import com.orientechnologies.orient.server.distributed.impl.task.OUnreachableServerLocalTask;
import com.orientechnologies.orient.server.distributed.task.OAbstractRemoteTask;
import com.orientechnologies.orient.server.distributed.task.ODistributedRecordLockedException;
import com.orientechnologies.orient.server.distributed.task.ORemoteTask;
import com.orientechnologies.orient.server.hazelcast.OHazelcastPlugin;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static com.orientechnologies.orient.core.config.OGlobalConfiguration.DISTRIBUTED_ATOMIC_LOCK_TIMEOUT;

/**
 * Distributed database implementation. There is one instance per database. Each node creates own instance to talk with each
 * others.
 *
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 */
public class ODistributedDatabaseImpl implements ODistributedDatabase {
  public static final  String                                    DISTRIBUTED_SYNC_JSON_FILENAME = "distributed-sync.json";
  private static final HashSet<Integer>                          ALL_QUEUES                     = new HashSet<Integer>();
  protected final      ODistributedAbstractPlugin                manager;
  protected final      ODistributedMessageServiceImpl            msgService;
  protected final      String                                    databaseName;
  protected            ODistributedDatabaseRepairer              repairer;
  protected            ODistributedSyncConfiguration             syncConfiguration;
  protected            ConcurrentHashMap<ORID, ODistributedLock> lockManager                    = new ConcurrentHashMap<ORID, ODistributedLock>(
      256);

  protected       ConcurrentHashMap<ODistributedRequestId, ODistributedTxContext> activeTxContexts = new ConcurrentHashMap<ODistributedRequestId, ODistributedTxContext>(
      64);
  protected final List<ODistributedWorker>                                        workerThreads    = new ArrayList<ODistributedWorker>();
  protected       ODistributedWorker                                              lockThread;
  protected       ODistributedWorker                                              nowaitThread;

  private          AtomicLong                            totalSentRequests     = new AtomicLong();
  private          AtomicLong                            totalReceivedRequests = new AtomicLong();
  private          TimerTask                             txTimeoutTask         = null;
  private          CountDownLatch                        waitForOnline         = new CountDownLatch(1);
  private volatile boolean                               running               = true;
  private          AtomicBoolean                         parsing               = new AtomicBoolean(true);
  private final    AtomicReference<ODistributedMomentum> filterByMomentum      = new AtomicReference<ODistributedMomentum>();

  private final String                     localNodeName;
  private final OSimpleLockManager<ORID>   recordLockManager;
  private final OSimpleLockManager<Object> indexKeyLockManager;
  private       AtomicLong                 operationsRunnig = new AtomicLong(0);

  public OSimpleLockManager<ORID> getRecordLockManager() {
    return recordLockManager;
  }

  public OSimpleLockManager<Object> getIndexKeyLockManager() {
    return indexKeyLockManager;
  }

  public void startOperation() {
    waitDistributedIsReady();
    operationsRunnig.incrementAndGet();
  }

  public void endOperation() {
    operationsRunnig.decrementAndGet();
  }

  public class ODistributedLock {
    protected final    ODistributedRequestId reqId;
    protected final    CountDownLatch        lock;
    protected final    long                  acquiredOn;
    protected volatile ORawBuffer            record;

    private ODistributedLock(final ODistributedRequestId reqId) {
      this.reqId = reqId;
      this.lock = new CountDownLatch(1);
      this.acquiredOn = System.currentTimeMillis();
    }
  }

  public ODistributedDatabaseImpl(final OHazelcastPlugin manager, final ODistributedMessageServiceImpl msgService,
      final String iDatabaseName, final ODistributedConfiguration cfg, OServer server) {
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

    if (iDatabaseName.equals(OSystemDatabase.SYSTEM_DB_NAME)) {
      recordLockManager = null;
      indexKeyLockManager = null;
      return;
    }

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

    long timeout = manager.getServerInstance().getContextConfiguration().getValueAsLong(DISTRIBUTED_ATOMIC_LOCK_TIMEOUT);
    recordLockManager = new OSimpleLockManagerImpl<>(timeout);
    indexKeyLockManager = new OSimpleLockManagerImpl<>(timeout);
  }

  public OLogSequenceNumber getLastLSN(final String server) {
    if (server == null)
      return null;
    return getSyncConfiguration().getLastLSN(server);
  }

  @Override
  public void waitForOnline() {
    try {
      if (!databaseName.equalsIgnoreCase(OSystemDatabase.SYSTEM_DB_NAME))
        waitForOnline.await();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      // IGNORE IT
    }
  }

  public void reEnqueue(final int senderNodeId, final long msgSequence, final String databaseName, final ORemoteTask payload,
      int retryCount) {

    Orient.instance().scheduleTask(
        () -> processRequest(new ODistributedRequest(getManager(), senderNodeId, msgSequence, databaseName, payload), false),
        10 * retryCount, 0);
  }

  /**
   * Distributed requests against the available workers by using one queue per worker. This guarantee the sequence of the operations
   * against the same record cluster.
   */
  public synchronized void processRequest(final ODistributedRequest request, final boolean waitForAcceptingRequests) {
    if (!running) {
      throw new ODistributedException("Server is going down or is removing the database:'" + getDatabaseName() + "' discarding");
    }

    final ORemoteTask task = request.getTask();
    task.received(request, this);
    manager.messageReceived(request);

    if (waitForAcceptingRequests) {
      waitIsReady(task);

      if (!running) {
        throw new ODistributedException("Server is going down or is removing the database:'" + getDatabaseName() + "' discarding");
      }
    }

    totalReceivedRequests.incrementAndGet();

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

      manager.messagePartitionCalculate(request, involvedWorkerQueues);

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
        CountDownLatch started = new CountDownLatch(involvedWorkerQueues.size());
        OExecuteOnce once = new OExecuteOnce(started, task);
        // WAIT ALL THE INVOLVED QUEUES ARE FREE AND SYNCHRONIZED
        for (int queue : involvedWorkerQueues) {
          ODistributedWorker worker = workerThreads.get(queue);
          OWaitPartitionsReadyTask waitRequest = new OWaitPartitionsReadyTask(once);

          final ODistributedRequest syncRequest = new ODistributedRequest(null, request.getId().getNodeId(),
              request.getId().getMessageId(), databaseName, waitRequest);
          worker.processRequest(syncRequest);
        }

      }
    } else if (partitionKeys.length == 1 && partitionKeys[0] == -2) {
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

    } else if (partitionKeys.length == 1 && partitionKeys[0] == -3) {
      // SERVICE - LOCK
      ODistributedServerLog.debug(this, localNodeName, request.getTask().getNodeSource(), DIRECTION.IN,
          "Request %s on database '%s' dispatched to the lock worker", request, databaseName);

      lockThread.processRequest(request);

    } else if (partitionKeys.length == 1 && partitionKeys[0] == -4) {
      // SERVICE - FAST_NOLOCK
      ODistributedServerLog.debug(this, localNodeName, request.getTask().getNodeSource(), DIRECTION.IN,
          "Request %s on database '%s' dispatched to the nowait worker", request, databaseName);

      nowaitThread.processRequest(request);

    } else {
      processRequest(partitionKeys[0], request);
    }
  }

  public void waitIsReady(ORemoteTask task) {
    if (task.isNodeOnlineRequired())
      waitDistributedIsReady();
  }

  public void waitDistributedIsReady() {
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

    Set<Integer> partitions = new HashSet<>();
    partitions.add(partition);

    manager.messagePartitionCalculate(request, partitions);

    ODistributedServerLog.debug(this, localNodeName, request.getTask().getNodeSource(), DIRECTION.IN,
        "Request %s on database '%s' dispatched to the worker %d", request, databaseName, partition);

    workerThreads.get(partition).processRequest(request);
  }

  @Override
  public ODistributedResponse send2Nodes(final ODistributedRequest iRequest, final Collection<String> iClusterNames,
      Collection<String> iNodes, final ODistributedRequest.EXECUTION_MODE iExecutionMode, final Object localResult,
      final OCallable<Void, ODistributedRequestId> iAfterSentCallback,
      final OCallable<Void, ODistributedResponseManager> endCallback) {
    return send2Nodes(iRequest, iClusterNames, iNodes, iExecutionMode, localResult, iAfterSentCallback, endCallback,
        (iRequest1, iNodes1, endCallback1, task, nodesConcurToTheQuorum, availableNodes, expectedResponses, quorum, groupByResponse, waitLocalNode) -> {
          return new ODistributedResponseManagerImpl(manager, iRequest, iNodes, nodesConcurToTheQuorum, expectedResponses, quorum,
              waitLocalNode, adjustTimeoutWithLatency(iNodes, task.getSynchronousTimeout(expectedResponses), iRequest.getId()),
              adjustTimeoutWithLatency(iNodes, task.getTotalTimeout(availableNodes), iRequest.getId()), groupByResponse,
              endCallback);
        });
  }

  public ODistributedResponse send2Nodes(final ODistributedRequest iRequest, final Collection<String> iClusterNames,
      Collection<String> iNodes, final ODistributedRequest.EXECUTION_MODE iExecutionMode, final Object localResult,
      final OCallable<Void, ODistributedRequestId> iAfterSentCallback,
      final OCallable<Void, ODistributedResponseManager> endCallback, ODistributedResponseManagerFactory responseManagerFactory) {
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
          .getNodesConcurInQuorum(manager, cfg, iRequest, iNodes, databaseName, localResult);

      // AFTER COMPUTED THE QUORUM, REMOVE THE OFFLINE NODES TO HAVE THE LIST OF REAL AVAILABLE NODES
      final int availableNodes;
      if (checkNodesAreOnline) {
        availableNodes = manager.getNodesWithStatus(iNodes, databaseName, ODistributedServerManager.DB_STATUS.ONLINE,
            ODistributedServerManager.DB_STATUS.BACKUP, ODistributedServerManager.DB_STATUS.SYNCHRONIZING);
      } else {
        availableNodes = iNodes.size();
      }

      final int expectedResponses = localResult != null ? availableNodes + 1 : availableNodes;

      // all online masters
      int onlineMasters = manager.getOnlineNodes(databaseName).stream()
          .filter(f -> cfg.getServerRole(f) == ODistributedConfiguration.ROLES.MASTER).collect(Collectors.toSet()).size();

      final int quorum = calculateQuorum(task.getQuorumType(), iClusterNames, cfg, expectedResponses, nodesConcurToTheQuorum.size(),
          onlineMasters, checkNodesAreOnline, localNodeName);

      final boolean groupByResponse = task.getResultStrategy() != OAbstractRemoteTask.RESULT_STRATEGY.UNION;

      final boolean waitLocalNode = waitForLocalNode(cfg, iClusterNames, iNodes);

      // CREATE THE RESPONSE MANAGER
      final ODistributedResponseManager currentResponseMgr = responseManagerFactory
          .newResponseManager(iRequest, iNodes, endCallback, task, nodesConcurToTheQuorum, availableNodes, expectedResponses,
              quorum, groupByResponse, waitLocalNode);

      if (localResult != null && currentResponseMgr.setLocalResult(localNodeName, localResult)) {
        // COLLECT LOCAL RESULT ONLY
        return currentResponseMgr.getFinalResponse();
      }

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

        } catch (Exception e) {
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

            } catch (Exception ex) {
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

      if (currentResponseMgr.getExpectedNodes().isEmpty())
        // NO SERVER TO SEND A MESSAGE
        throw new ODistributedException(
            "No server active for distributed request (" + iRequest + ") against database '" + databaseName + (
                iClusterNames != null ? "." + iClusterNames : "") + "' to nodes " + iNodes);

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
      String names = iClusterNames != null ? "." + iClusterNames : "";
      throw OException.wrapException(new ODistributedException(
          "Error on executing distributed request (" + iRequest + ") against database '" + databaseName + names + "' to nodes " + iNodes), e);
    } finally {
      if (iAfterSentCallback != null && !afterSendCallBackCalled)
        iAfterSentCallback.call(iRequest.getId());
    }
  }

  public ODistributedResponseManager newResponseManager(ODistributedRequest iRequest, Collection<String> iNodes,
      OCallable<Void, ODistributedResponseManager> endCallback, ORemoteTask task, Set<String> nodesConcurToTheQuorum,
      int availableNodes, int expectedResponses, int quorum, boolean groupByResponse, boolean waitLocalNode) {
    return new ODistributedResponseManagerImpl(manager, iRequest, iNodes, nodesConcurToTheQuorum, expectedResponses, quorum,
        waitLocalNode, adjustTimeoutWithLatency(iNodes, task.getSynchronousTimeout(expectedResponses), iRequest.getId()),
        adjustTimeoutWithLatency(iNodes, task.getTotalTimeout(availableNodes), iRequest.getId()), groupByResponse, endCallback);
  }

  private long adjustTimeoutWithLatency(final Collection<String> iNodes, final long timeout,
      final ODistributedRequestId requestId) {
    long delta = 0;
    if (iNodes != null)
      for (String n : iNodes) {
        // UPDATE THE TIMEOUT WITH THE CURRENT SERVER LATENCY
        final long l = msgService.getCurrentLatency(n);
        delta = Math.max(delta, l);
      }

    if (delta > 500)
      ODistributedServerLog.debug(this, localNodeName, iNodes.toString(), DIRECTION.OUT,
          "Adjusted timeouts by adding +%dms because this is the maximum latency recorded against servers %s (reqId=%s)", delta,
          iNodes, requestId);

    return timeout + delta;
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
  public ORawBuffer getRecordIfLocked(final ORID rid) {
    final ODistributedLock currentLock = lockManager.get(rid);
    if (currentLock != null)
      return currentLock.record;
    return null;
  }

  @Override
  public void replaceRecordContentIfLocked(final ORID rid, final byte[] bytes) {
    final ODistributedLock currentLock = lockManager.get(rid);
    if (currentLock != null && currentLock.record != null)
      currentLock.record.buffer = bytes;
  }

  @Override
  public boolean lockRecord(final ORID rid, final ODistributedRequestId requestId, final long timeout) {
    final ODistributedLock lock = new ODistributedLock(requestId);

    ORawBuffer currentRecord = null;
    boolean newLock = true;

    ODistributedLock currentLock = lockManager.putIfAbsent(rid, lock);
    if (currentLock != null) {
      currentRecord = currentLock.record;

      if (requestId.equals(currentLock.reqId)) {
        // SAME ID, ALREADY LOCKED
        ODistributedServerLog.debug(this, localNodeName, null, DIRECTION.NONE,
            "Distributed transaction: %s locked record %s in database '%s' owned by %s (thread=%d)", requestId, rid, databaseName,
            currentLock.reqId, Thread.currentThread().getId());
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

    if (currentLock == null) {
      // SAVE CURRENT RECORD IN RAM FOR READ-ONLY ACCESS WHILE IT IS LOCKED
      if (currentRecord != null)
        // USE THE EXISTENT
        lock.record = currentRecord;
      else if (rid.isPersistent()) {
        final ODatabaseDocumentInternal db = ODatabaseRecordThreadLocal.instance().getIfDefined();
        if (db != null)
          lock.record = db.getStorage().getUnderlying().readRecord((ORecordId) rid, null, false, false, null).getResult();
      }

      if (ODistributedServerLog.isDebugEnabled())
        ODistributedServerLog
            .debug(this, localNodeName, null, DIRECTION.NONE, "Locked record %s in database '%s' (reqId=%s thread=%d)", rid,
                databaseName, requestId, Thread.currentThread().getId());
    } else {
      if (ODistributedServerLog.isDebugEnabled())
        ODistributedServerLog.debug(this, localNodeName, null, DIRECTION.NONE,
            "Cannot lock record %s in database '%s' owned by %s (reqId=%s thread=%d)", rid, databaseName, null, requestId,
            Thread.currentThread().getId());
    }

    if (currentLock != null)
      throw new ODistributedRecordLockedException(manager.getLocalNodeName(), rid, timeout);

    return newLock;
  }

  @Override
  public void unlockRecord(final OIdentifiable iRecord, final ODistributedRequestId requestId) {
    if (requestId == null)
      return;

    final ODistributedLock owner = lockManager.get(iRecord.getIdentity());
    if (owner != null) {
      if (!owner.reqId.equals(requestId)) {
        ODistributedServerLog.debug(this, localNodeName, null, DIRECTION.NONE,
            "Distributed transaction: cannot unlock record %s in database '%s' because owner %s <> current %s (thread=%d)", iRecord,
            databaseName, owner.reqId, requestId, Thread.currentThread().getId());
        return;
      }

      lockManager.remove(iRecord.getIdentity());

      // NOTIFY ANY WAITERS
      owner.lock.countDown();
    }

    if (ODistributedServerLog.isDebugEnabled())
      ODistributedServerLog.debug(this, localNodeName, null, DIRECTION.NONE,
          "Distributed transaction: %s unlocked record %s in database '%s' (owner=%s, thread=%d)", requestId, iRecord, databaseName,
          owner != null ? owner.reqId : "null", Thread.currentThread().getId());
  }

  @Override
  public boolean forceLockRecord(final ORID rid, final ODistributedRequestId requestId) {
    final ODistributedLock lock = new ODistributedLock(requestId);

    ORawBuffer currentRecord = null;

    boolean newLock = true;
    ODistributedLock currentLock = lockManager.put(rid, lock);
    if (currentLock != null) {
      currentRecord = currentLock.record;

      if (requestId.equals(currentLock.reqId)) {
        // SAME ID, ALREADY LOCKED
        ODistributedServerLog.debug(this, localNodeName, null, DIRECTION.NONE,
            "Distributed transaction: rid %s was already locked by %s in database '%s' owned by %s (thread=%d)", rid, requestId,
            databaseName, currentLock.reqId, Thread.currentThread().getId());
        currentLock = null;
        newLock = false;
      } else {
        if (currentLock.reqId.getNodeId() == requestId.getNodeId())
          // BOTH REQUESTS COME FROM THE SAME SERVER, AVOID TO CANCEL THE REQUEST BECAUSE IS THE ONLY CASE WHEN IT SHOULD WAIT FOR COMPLETION
          return lockRecord(rid, requestId, 0);

        ODistributedServerLog
            .debug(this, localNodeName, null, DIRECTION.NONE, "Canceling request %s in database '%s' (reqId=%s thread=%d)",
                currentLock.reqId, databaseName, requestId, Thread.currentThread().getId());

        // WAKE UP WAITERS OF PREVIOUS LOCK
        currentLock.lock.countDown();

        // CANCEL REQ-ID/TX THAT OWNED THE LOCK
        final ODistributedTxContext lockedCtx = activeTxContexts.get(currentLock.reqId);
        if (lockedCtx != null) {
          // CANCEL THE ENTIRE TX/CONTEXT/REQ-ID

          lockedCtx.cancel(manager, ODatabaseRecordThreadLocal.instance().get());

        } else {
          // ABORT SINGLE REQUEST
          final ODistributedResponseManager respMgr = manager.getMessageService().getResponseManager(requestId);
          if (respMgr != null) {
            respMgr.cancel();
          }
        }
      }
    }

    if (currentLock == null) {
      // SAVE CURRENT RECORD IN RAM FOR READ-ONLY ACCESS WHILE IT IS LOCKED
      if (currentRecord != null)
        // USE THE EXISTENT
        lock.record = currentRecord;
      else if (rid.isPersistent()) {
        // RELOAD IT
        final ODatabaseDocumentInternal db = ODatabaseRecordThreadLocal.instance().getIfDefined();
        if (db != null)
          lock.record = db.getStorage().getUnderlying().readRecord((ORecordId) rid, null, false, false, null).getResult();
      }

      if (newLock && ODistributedServerLog.isDebugEnabled())
        ODistributedServerLog
            .debug(this, localNodeName, null, DIRECTION.NONE, "Locked rid %s in database '%s' (reqId=%s thread=%d)", rid,
                databaseName, requestId, Thread.currentThread().getId());
    } else {
      if (ODistributedServerLog.isDebugEnabled())
        ODistributedServerLog.debug(this, localNodeName, null, DIRECTION.NONE,
            "Forced locking of rid %s in database '%s' owned by %s (reqId=%s thread=%d)", rid, databaseName, currentLock.reqId,
            requestId, Thread.currentThread().getId());
    }

    return newLock;
  }

  @Override
  public void unlockResourcesOfServer(final ODatabaseDocumentInternal database, final String serverName) {
    final int nodeLeftId = manager.getNodeIdByName(serverName);

    final Set<ORecordId> rids2Repair = new HashSet<ORecordId>();

    int rollbacks = 0;
    final Iterator<ODistributedTxContext> pendingReqIterator = activeTxContexts.values().iterator();
    while (pendingReqIterator.hasNext()) {
      final ODistributedTxContext pReq = pendingReqIterator.next();
      if (pReq != null && pReq.getReqId().getNodeId() == nodeLeftId) {

        ODistributedServerLog.debug(this, manager.getLocalNodeName(), null, DIRECTION.NONE,
            "Distributed transaction: rolling back transaction (req=%s)", pReq.getReqId());

        try {
          rids2Repair.addAll(pReq.rollback(database));
          rollbacks++;
        } catch (Exception t) {
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

    // REPAIR RECORDS OF TRANSACTION.
    getDatabaseRepairer().enqueueRepairRecords(rids2Repair);
  }

  @Override
  public ODistributedTxContext registerTxContext(final ODistributedRequestId reqId) {
    return registerTxContext(reqId, new ODistributedTxContextImpl(this, reqId));
  }

  @Override
  public ODistributedTxContext registerTxContext(final ODistributedRequestId reqId, ODistributedTxContext ctx) {
    final ODistributedTxContext prevCtx = activeTxContexts.put(reqId, ctx);
    if (prevCtx != ctx && prevCtx != null) {
      prevCtx.destroy();
    }
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
  public ODistributedTxContext getTxContext(final ODistributedRequestId requestId) {
    final ODistributedTxContext ctx = activeTxContexts.get(requestId);
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
    return manager.getServerInstance().existsDatabase(databaseName);
  }

  public ODistributedSyncConfiguration getSyncConfiguration() {
    if (syncConfiguration == null) {
      final String path = manager.getServerInstance().getDatabaseDirectory() + databaseName + "/" + DISTRIBUTED_SYNC_JSON_FILENAME;
      final File cfgFile = new File(path);
      try {
        syncConfiguration = new ODistributedSyncConfiguration(manager, databaseName, cfgFile);
      } catch (IOException e) {
        throw OException
            .wrapException(new ODistributedException("Cannot open database distributed sync configuration file: " + cfgFile), e);
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
    final ODistributedRequest rollbackRequest = new ODistributedRequest(null, manager.getLocalNodeId(),
        manager.getNextMessageIdCounter(), null, task);
    processRequest(rollbackRequest, false);
  }

  @Override
  public String getDatabaseName() {
    return databaseName;
  }

  @Override
  public ODatabaseDocumentInternal getDatabaseInstance() {
    return manager.getServerInstance().openDatabase(databaseName, "internal", "internal", null, true);
  }

  @Override
  public long getReceivedRequests() {
    return totalReceivedRequests.get();
  }

  @Override
  public long getProcessedRequests() {
    long total = 0;

    if (lockThread != null)
      total += lockThread.getProcessedRequests();

    if (nowaitThread != null)
      total += nowaitThread.getProcessedRequests();

    for (ODistributedWorker workerThread : workerThreads) {
      if (workerThread != null)
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
      if (lockThread != null)
        lockThread.sendShutdown();
      if (nowaitThread != null)
        nowaitThread.sendShutdown();
      for (ODistributedWorker workerThread : workerThreads) {
        if (workerThread != null)
          workerThread.sendShutdown();
      }

      // WAIT A BIT FOR PROPER SHUTDOWN
      if (lockThread != null)
        try {
          lockThread.join(2000);
        } catch (InterruptedException e) {
        }

      if (nowaitThread != null)
        try {
          nowaitThread.join(2000);
        } catch (InterruptedException e) {
        }

      for (ODistributedWorker workerThread : workerThreads) {
        if (workerThread != null) {
          try {
            workerThread.join(2000);
          } catch (InterruptedException e) {
          }
        }
      }
      lockThread = null;
      nowaitThread = null;
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
        } catch (Exception e) {
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
      final ODistributedConfiguration cfg, final int totalServers, final int totalMasterServers, int onlineMasters,
      final boolean checkNodesAreOnline, final String localNodeName) {

    int quorum = 1;

    if (clusterNames == null || clusterNames.isEmpty()) {
      clusterNames = new ArrayList<String>(1);
      clusterNames.add(null);
    }

    int totalServerInQuorum = totalServers;
    for (String cluster : clusterNames) {
      int clusterQuorum = 0;
      switch (quorumType) {
      case NONE:
        // IGNORE IT
        break;
      case READ:
        clusterQuorum = cfg.getReadQuorum(cluster, totalServers, localNodeName);
        break;
      case WRITE:
        clusterQuorum = cfg.getWriteQuorum(cluster, totalMasterServers, localNodeName);
        totalServerInQuorum = totalMasterServers;
        break;
      case WRITE_ALL_MASTERS:
        int cfgQuorum = cfg.getWriteQuorum(cluster, totalMasterServers, localNodeName);
        clusterQuorum = Math.max(cfgQuorum, onlineMasters);
        break;
      case ALL:
        clusterQuorum = totalServers;
        break;
      }

      quorum = Math.max(quorum, clusterQuorum);
    }

    if (quorum < 0)
      quorum = 0;

    if (checkNodesAreOnline && quorum > totalServerInQuorum)
      throw new ODistributedException(
          "Quorum (" + quorum + ") cannot be reached on server '" + localNodeName + "' database '" + databaseName
              + "' because it is major than available nodes (" + totalServerInQuorum + ")");

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

  @Override
  public void checkNodeInConfiguration(final ODistributedConfiguration cfg, final String serverName) {
    manager.executeInDistributedDatabaseLock(databaseName, 20000, cfg != null ? cfg.modify() : null,
        new OCallable<Void, OModifiableDistributedConfiguration>() {
          @Override
          public Void call(final OModifiableDistributedConfiguration lastCfg) {
            // GET LAST VERSION IN LOCK
            final List<String> foundPartition = lastCfg.addNewNodeInServerList(serverName);
            if (foundPartition != null) {
              ODistributedServerLog
                  .info(this, localNodeName, null, DIRECTION.NONE, "Adding node '%s' in partition: %s db=%s v=%d", serverName,
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
    int totalWorkers = OGlobalConfiguration.DISTRIBUTED_DB_WORKERTHREADS.getValueAsInteger();
    if (totalWorkers < 0)
      throw new ODistributedException("Cannot create configured distributed workers (" + totalWorkers + ")");
    else if (totalWorkers == 0) {
      // AUTOMATIC
      final int totalDatabases = manager.getManagedDatabases().size() + 1;

      final int cpus = Runtime.getRuntime().availableProcessors();

      if (cpus > 1)
        totalWorkers = cpus / totalDatabases;

      if (totalWorkers == 0)
        totalWorkers = 1;
    }

    lockThread = new ODistributedWorker(this, databaseName, -3, false) {
      protected void handleError(final ODistributedRequest iRequest, final Object responsePayload) {
        // CANNOT SEND MSG BACK, UNLOCK IT
        final ODistributedLockTask task = (ODistributedLockTask) iRequest.getTask();
        task.undo(manager);
      }
    };
    lockThread.start();

    nowaitThread = new ODistributedWorker(this, databaseName, -4, true);
    nowaitThread.start();

    for (int i = 0; i < totalWorkers; ++i) {
      final ODistributedWorker workerThread = new ODistributedWorker(this, databaseName, i, true);
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
        ODatabaseDocumentInternal database = null;
        try {
          final long now = System.currentTimeMillis();
          final long timeout = OGlobalConfiguration.DISTRIBUTED_TX_EXPIRE_TIMEOUT.getValueAsLong();

          final Set<ORecordId> rids2Repair = new HashSet<ORecordId>();

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

                if (database != null)
                  database.activateOnCurrentThread();

                try {
                  rids2Repair.addAll(ctx.cancel(manager, database));

                  if (ctx.getReqId().getNodeId() == manager.getLocalNodeId())
                    // REQUEST WAS ORIGINATED FROM CURRENT SERVER
                    msgService.timeoutRequest(ctx.getReqId().getMessageId());

                } catch (Exception t) {
                  ODistributedServerLog.info(this, localNodeName, null, DIRECTION.NONE,
                      "Error on rolling back distributed transaction %s on database '%s' (err=%s)", ctx.getReqId(), databaseName,
                      t);
                } finally {
                  it.remove();
                }
              }
            }
          }

          // CHECK INDIVIDUAL LOCKS TOO
          for (final Iterator<Map.Entry<ORID, ODistributedLock>> it = lockManager.entrySet().iterator(); it.hasNext(); ) {
            final Map.Entry<ORID, ODistributedLock> entry = it.next();

            final ODistributedLock lock = entry.getValue();
            if (lock != null) {
              final long elapsed = now - lock.acquiredOn;
              if (elapsed > timeout) {
                // EXPIRED
                ODistributedServerLog.debug(this, localNodeName, null, DIRECTION.NONE,
                    "Distributed lock on database '%s' record %s is expired after %dms", databaseName, entry.getKey(), elapsed);

                it.remove();
              }
            }
          }

          getDatabaseRepairer().enqueueRepairRecords(rids2Repair);

        } catch (Exception t) {
          // CATCH EVERYTHING TO AVOID THE TIMER IS CANCELED
          ODistributedServerLog.info(this, localNodeName, null, DIRECTION.NONE,
              "Error on checking for expired distributed transaction on database '%s'", databaseName);
        } finally {
          if (database != null) {
            database.activateOnCurrentThread();
            database.close();
          }
        }
      }
    };

//    Orient.instance().scheduleTask(txTimeoutTask, OGlobalConfiguration.DISTRIBUTED_TX_EXPIRE_TIMEOUT.getValueAsLong(),
//        OGlobalConfiguration.DISTRIBUTED_TX_EXPIRE_TIMEOUT.getValueAsLong() / 2);
  }

  private boolean isRunning() {
    return running;
  }

  public void suspend() {
    boolean parsing = this.parsing.get();
    if (parsing) {
      // RESET THE DATABASE
      if (lockThread != null)
        lockThread.reset();
      if (nowaitThread != null)
        nowaitThread.reset();
      for (ODistributedWorker w : workerThreads) {
        if (w != null)
          w.reset();
      }
    }

    this.parsing.set(false);
    if (parsing) {
      while (operationsRunnig.get() != 0) {
        try {
          Thread.sleep(300);
        } catch (InterruptedException e) {
          break;
        }
      }

      recordLockManager.reset();
      indexKeyLockManager.reset();
    }

  }

  public void resume() {
    this.parsing.set(true);
  }

  @Override
  public String dump() {
    final StringBuilder buffer = new StringBuilder(1024);

    buffer.append("\n\nDATABASE '" + databaseName + "' ON SERVER '" + manager.getLocalNodeName() + "'");

    buffer.append("\n- " + ODistributedOutput.formatRecordLocks(manager, databaseName));

    buffer.append("\n- MESSAGES IN QUEUES");
    buffer.append(" (" + (workerThreads != null ? workerThreads.size() : 0) + " WORKERS):");

    if (lockThread != null) {
      final ODistributedRequest processing = lockThread.getProcessing();
      final ArrayBlockingQueue<ODistributedRequest> queue = lockThread.localQueue;

      if (processing != null || !queue.isEmpty()) {
        buffer.append("\n - QUEUE LOCK EXECUTING: " + processing);
        int i = 0;
        for (ODistributedRequest m : queue) {
          if (m != null)
            buffer.append("\n  - " + i + " = " + m.toString());
        }
      }
    }

    if (nowaitThread != null) {
      final ODistributedRequest processing = nowaitThread.getProcessing();
      final ArrayBlockingQueue<ODistributedRequest> queue = nowaitThread.localQueue;

      if (processing != null || !queue.isEmpty()) {
        buffer.append("\n - QUEUE UNLOCK EXECUTING: " + processing);
        int i = 0;
        for (ODistributedRequest m : queue) {
          if (m != null)
            buffer.append("\n  - " + i + " = " + m.toString());
        }
      }
    }

    if (workerThreads != null) {
      for (ODistributedWorker t : workerThreads) {
        final ODistributedRequest processing = t.getProcessing();
        final ArrayBlockingQueue<ODistributedRequest> queue = t.localQueue;

        if (processing != null || !queue.isEmpty()) {
          buffer.append("\n  - QUEUE " + t.id + " EXECUTING: " + processing);
          int i = 0;
          for (ODistributedRequest m : queue) {
            if (m != null)
              buffer.append("\n   - " + (i++) + " = " + m.toString());
          }
        }
      }
    }

    return buffer.toString();
  }

  public ConcurrentHashMap<ODistributedRequestId, ODistributedTxContext> getActiveTxContexts() {
    return activeTxContexts;
  }

}
