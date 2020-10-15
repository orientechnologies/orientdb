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

import static com.orientechnologies.orient.core.config.OGlobalConfiguration.DISTRIBUTED_ATOMIC_LOCK_TIMEOUT;
import static com.orientechnologies.orient.core.config.OGlobalConfiguration.DISTRIBUTED_TRANSACTION_SEQUENCE_SET_SIZE;
import static com.orientechnologies.orient.server.distributed.ODistributedServerLog.DIRECTION.OUT;

import com.orientechnologies.common.concur.OOfflineNodeException;
import com.orientechnologies.common.concur.lock.OInterruptedException;
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
import com.orientechnologies.orient.core.exception.OSecurityAccessException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.core.tx.OTransactionId;
import com.orientechnologies.orient.core.tx.OTransactionSequenceStatus;
import com.orientechnologies.orient.core.tx.OTxMetadataHolder;
import com.orientechnologies.orient.core.tx.ValidationResult;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.OSystemDatabase;
import com.orientechnologies.orient.server.distributed.ODistributedConfiguration;
import com.orientechnologies.orient.server.distributed.ODistributedDatabase;
import com.orientechnologies.orient.server.distributed.ODistributedException;
import com.orientechnologies.orient.server.distributed.ODistributedRequest;
import com.orientechnologies.orient.server.distributed.ODistributedRequestId;
import com.orientechnologies.orient.server.distributed.ODistributedResponse;
import com.orientechnologies.orient.server.distributed.ODistributedResponseManager;
import com.orientechnologies.orient.server.distributed.ODistributedResponseManagerImpl;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog.DIRECTION;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.distributed.ODistributedSyncConfiguration;
import com.orientechnologies.orient.server.distributed.ODistributedTxContext;
import com.orientechnologies.orient.server.distributed.OModifiableDistributedConfiguration;
import com.orientechnologies.orient.server.distributed.ORemoteServerController;
import com.orientechnologies.orient.server.distributed.impl.lock.OFreezeGuard;
import com.orientechnologies.orient.server.distributed.impl.lock.OLockGuard;
import com.orientechnologies.orient.server.distributed.impl.lock.OLockManager;
import com.orientechnologies.orient.server.distributed.impl.lock.OLockManagerImpl;
import com.orientechnologies.orient.server.distributed.impl.task.OLockKeySource;
import com.orientechnologies.orient.server.distributed.impl.task.OUnreachableServerLocalTask;
import com.orientechnologies.orient.server.distributed.impl.task.transaction.OTransactionUniqueKey;
import com.orientechnologies.orient.server.distributed.task.OAbstractRemoteTask;
import com.orientechnologies.orient.server.distributed.task.ORemoteTask;
import com.orientechnologies.orient.server.hazelcast.OHazelcastPlugin;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * Distributed database implementation. There is one instance per database. Each node creates own
 * instance to talk with each others.
 *
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 */
public class ODistributedDatabaseImpl implements ODistributedDatabase {
  public static final String DISTRIBUTED_SYNC_JSON_FILENAME = "distributed-sync.json";
  private static final HashSet<Integer> ALL_QUEUES = new HashSet<Integer>();
  protected final ODistributedAbstractPlugin manager;
  protected final ODistributedMessageServiceImpl msgService;
  protected final String databaseName;
  protected ODistributedSyncConfiguration syncConfiguration;

  protected Map<ODistributedRequestId, ODistributedTxContext> activeTxContexts =
      new ConcurrentHashMap<>(64);
  private AtomicLong totalSentRequests = new AtomicLong();
  private AtomicLong totalReceivedRequests = new AtomicLong();
  private TimerTask txTimeoutTask = null;
  private CountDownLatch waitForOnline = new CountDownLatch(1);
  private volatile boolean running = true;
  private AtomicBoolean parsing = new AtomicBoolean(true);

  private final String localNodeName;
  private final OSimpleLockManager<ORID> recordLockManager;
  private final OSimpleLockManager<Object> indexKeyLockManager;
  private AtomicLong operationsRunnig = new AtomicLong(0);
  private ODistributedSynchronizedSequence sequenceManager;
  private final AtomicLong pending = new AtomicLong();
  private ThreadPoolExecutor requestExecutor;
  private OLockManager lockManager = new OLockManagerImpl();
  private Set<OTransactionId> inQueue = Collections.newSetFromMap(new ConcurrentHashMap<>());
  private OFreezeGuard freezeGuard;

  public static boolean sendResponseBack(
      final Object current,
      final ODistributedServerManager manager,
      final ODistributedRequestId iRequestId,
      Object responsePayload) {

    if (iRequestId.getMessageId() < 0)
      // INTERNAL MSG
      return true;

    final String local = manager.getLocalNodeName();

    final String sender = manager.getNodeNameById(iRequestId.getNodeId());

    final ODistributedResponse response =
        new ODistributedResponse(null, iRequestId, local, sender, responsePayload);

    // TODO: check if using remote channel for local node still makes sense
    //    if (!senderNodeName.equalsIgnoreCase(manager.getLocalNodeName()))
    try {
      // GET THE SENDER'S RESPONSE QUEUE
      final ORemoteServerController remoteSenderServer = manager.getRemoteServer(sender);

      ODistributedServerLog.debug(
          current, local, sender, OUT, "Sending response %s back (reqId=%s)", response, iRequestId);

      remoteSenderServer.sendResponse(response);

    } catch (Exception e) {
      ODistributedServerLog.debug(
          current,
          local,
          sender,
          OUT,
          "Error on sending response '%s' back (reqId=%s err=%s)",
          response,
          iRequestId,
          e.toString());
      return false;
    }

    return true;
  }

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

  public ODistributedDatabaseImpl(
      final OHazelcastPlugin manager,
      final ODistributedMessageServiceImpl msgService,
      final String iDatabaseName,
      final ODistributedConfiguration cfg,
      OServer server) {
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

    Orient.instance()
        .getProfiler()
        .registerHookValue(
            "distributed.db." + databaseName + ".msgSent",
            "Number of replication messages sent from current node",
            OProfiler.METRIC_TYPE.COUNTER,
            new OAbstractProfiler.OProfilerHookValue() {
              @Override
              public Object getValue() {
                return totalSentRequests.get();
              }
            },
            "distributed.db.*.msgSent");

    Orient.instance()
        .getProfiler()
        .registerHookValue(
            "distributed.db." + databaseName + ".msgReceived",
            "Number of replication messages received from external nodes",
            OProfiler.METRIC_TYPE.COUNTER,
            new OAbstractProfiler.OProfilerHookValue() {
              @Override
              public Object getValue() {
                return totalReceivedRequests.get();
              }
            },
            "distributed.db.*.msgReceived");

    Orient.instance()
        .getProfiler()
        .registerHookValue(
            "distributed.db." + databaseName + ".activeContexts",
            "Number of active distributed transactions",
            OProfiler.METRIC_TYPE.COUNTER,
            new OAbstractProfiler.OProfilerHookValue() {
              @Override
              public Object getValue() {
                return (long) activeTxContexts.size();
              }
            },
            "distributed.db.*.activeContexts");

    Orient.instance()
        .getProfiler()
        .registerHookValue(
            "distributed.db." + databaseName + ".workerThreads",
            "Number of worker threads",
            OProfiler.METRIC_TYPE.COUNTER,
            new OAbstractProfiler.OProfilerHookValue() {
              @Override
              public Object getValue() {
                return (long) requestExecutor.getPoolSize();
              }
            },
            "distributed.db.*.workerThreads");

    Orient.instance()
        .getProfiler()
        .registerHookValue(
            "distributed.db." + databaseName + ".recordLocks",
            "Number of record locked",
            OProfiler.METRIC_TYPE.COUNTER,
            new OAbstractProfiler.OProfilerHookValue() {
              @Override
              public Object getValue() {
                return (long) recordLockManager.size() + indexKeyLockManager.size();
              }
            },
            "distributed.db.*.recordLocks");

    long timeout =
        manager
            .getServerInstance()
            .getContextConfiguration()
            .getValueAsLong(DISTRIBUTED_ATOMIC_LOCK_TIMEOUT);
    int sequenceSize =
        manager
            .getServerInstance()
            .getContextConfiguration()
            .getValueAsInteger(DISTRIBUTED_TRANSACTION_SEQUENCE_SET_SIZE);
    recordLockManager = new OSimpleLockManagerImpl<>(timeout);
    indexKeyLockManager = new OSimpleLockManagerImpl<>(timeout);
    sequenceManager = new ODistributedSynchronizedSequence(localNodeName, sequenceSize);
  }

  @Override
  public void waitForOnline() {
    try {
      if (!databaseName.equalsIgnoreCase(OSystemDatabase.SYSTEM_DB_NAME)) waitForOnline.await();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      // IGNORE IT
    }
  }

  public void reEnqueue(
      final int senderNodeId,
      final long msgSequence,
      final String databaseName,
      final ORemoteTask payload,
      int retryCount,
      int autoRetryDelay) {
    pending.incrementAndGet();
    Orient.instance()
        .scheduleTask(
            () -> {
              try {
                processRequest(
                    new ODistributedRequest(
                        getManager(), senderNodeId, msgSequence, databaseName, payload),
                    false);
              } finally {
                pending.decrementAndGet();
              }
            },
            autoRetryDelay * retryCount,
            0);
  }

  /**
   * Distributed requests against the available workers by using one queue per worker. This
   * guarantee the sequence of the operations against the same record cluster.
   */
  public void processRequest(
      final ODistributedRequest request, final boolean waitForAcceptingRequests) {
    if (!running) {
      throw new ODistributedException(
          "Server is going down or is removing the database:'"
              + getDatabaseName()
              + "' discarding");
    }

    final ORemoteTask task = request.getTask();
    if (waitForAcceptingRequests) {
      waitIsReady(task);

      if (!running) {
        throw new ODistributedException(
            "Server is going down or is removing the database:'"
                + getDatabaseName()
                + "' discarding");
      }
    }
    synchronized (this) {
      task.received(request, this);
      manager.messageReceived(request);

      totalReceivedRequests.incrementAndGet();
      if (task instanceof OLockKeySource) {
        SortedSet<ORID> rids = ((OLockKeySource) task).getRids();
        SortedSet<OTransactionUniqueKey> uniqueKeys = ((OLockKeySource) task).getUniqueKeys();
        OTransactionId txId = ((OLockKeySource) task).getTransactionId();
        this.lockManager.lock(
            rids,
            uniqueKeys,
            txId,
            (guards) -> {
              try {
                this.requestExecutor.submit(
                    () -> {
                      try {
                        execute(request);
                      } finally {
                        this.lockManager.unlock(guards);
                      }
                    });
              } catch (RejectedExecutionException e) {
                task.finished(this);
                this.lockManager.unlock(guards);
                throw e;
              }
            });
      } else {
        try {
          this.requestExecutor.submit(
              () -> {
                execute(request);
              });
        } catch (RejectedExecutionException e) {
          task.finished(this);
          throw e;
        }
      }
    }
  }

  public void trackTransactions(OTransactionId id) {
    inQueue.add(id);
  }

  public void untrackTransactions(OTransactionId id) {
    inQueue.remove(id);
  }

  private void execute(ODistributedRequest request) {
    ORemoteTask task = request.getTask();
    try {
      manager.messageProcessStart(request);
      Object response;
      if (task.isUsingDatabase()) {
        try (ODatabaseDocumentInternal db =
            this.manager.getServerInstance().getDatabases().openNoAuthorization(databaseName)) {
          response = this.manager.executeOnLocalNode(request.getId(), task, db);
        }
      } else {
        response = this.manager.executeOnLocalNode(request.getId(), task, null);
      }
      if (task.hasResponse()) {
        sendResponseBack(this, this.manager, request.getId(), response);
      }
      manager.messageProcessEnd(request, response);
    } finally {
      task.finished(this);
    }
  }

  public void waitIsReady(ORemoteTask task) {
    if (task.isNodeOnlineRequired()) waitDistributedIsReady();
  }

  public void waitDistributedIsReady() {
    if (!parsing.get()) {
      // WAIT FOR PARSING REQUESTS
      while (!parsing.get() && running) {
        try {
          Thread.sleep(300);
        } catch (InterruptedException e) {
          break;
        }
      }
    }
  }

  @Override
  public ODistributedResponse send2Nodes(
      final ODistributedRequest iRequest,
      final Collection<String> iClusterNames,
      Collection<String> iNodes,
      final ODistributedRequest.EXECUTION_MODE iExecutionMode,
      final Object localResult) {
    return send2Nodes(
        iRequest,
        iClusterNames,
        iNodes,
        iExecutionMode,
        localResult,
        (iRequest1,
            iNodes1,
            task,
            nodesConcurToTheQuorum,
            availableNodes,
            expectedResponses,
            quorum,
            groupByResponse,
            waitLocalNode) -> {
          return new ODistributedResponseManagerImpl(
              manager,
              iRequest,
              iNodes,
              nodesConcurToTheQuorum,
              expectedResponses,
              quorum,
              waitLocalNode,
              adjustTimeoutWithLatency(
                  iNodes, task.getSynchronousTimeout(expectedResponses), iRequest.getId()),
              adjustTimeoutWithLatency(
                  iNodes, task.getTotalTimeout(availableNodes), iRequest.getId()),
              groupByResponse);
        });
  }

  public ODistributedResponse send2Nodes(
      final ODistributedRequest iRequest,
      final Collection<String> iClusterNames,
      Collection<String> iNodes,
      final ODistributedRequest.EXECUTION_MODE iExecutionMode,
      final Object localResult,
      ODistributedResponseManagerFactory responseManagerFactory) {
    try {
      checkForServerOnline(iRequest);

      final String databaseName = iRequest.getDatabaseName();

      if (iNodes.isEmpty()) {
        ODistributedServerLog.error(
            this,
            localNodeName,
            null,
            OUT,
            "No nodes configured for database '%s' request: %s",
            databaseName,
            iRequest);
        throw new ODistributedException(
            "No nodes configured for partition '" + databaseName + "' request: " + iRequest);
      }

      final ODistributedConfiguration cfg = manager.getDatabaseConfiguration(databaseName);

      final ORemoteTask task = iRequest.getTask();

      final boolean checkNodesAreOnline = task.isNodeOnlineRequired();

      final Set<String> nodesConcurToTheQuorum =
          manager
              .getDistributedStrategy()
              .getNodesConcurInQuorum(manager, cfg, iRequest, iNodes, databaseName, localResult);

      // AFTER COMPUTED THE QUORUM, REMOVE THE OFFLINE NODES TO HAVE THE LIST OF REAL AVAILABLE
      // NODES
      final int availableNodes;
      if (checkNodesAreOnline) {
        availableNodes =
            manager.getNodesWithStatus(
                iNodes,
                databaseName,
                ODistributedServerManager.DB_STATUS.ONLINE,
                ODistributedServerManager.DB_STATUS.BACKUP,
                ODistributedServerManager.DB_STATUS.SYNCHRONIZING);
      } else {
        availableNodes = iNodes.size();
      }

      final int expectedResponses = localResult != null ? availableNodes + 1 : availableNodes;

      // all online masters
      int onlineMasters =
          manager.getOnlineNodes(databaseName).stream()
              .filter(f -> cfg.getServerRole(f) == ODistributedConfiguration.ROLES.MASTER)
              .collect(Collectors.toSet())
              .size();

      final int quorum =
          calculateQuorum(
              task.getQuorumType(),
              iClusterNames,
              cfg,
              expectedResponses,
              nodesConcurToTheQuorum.size(),
              onlineMasters,
              checkNodesAreOnline,
              localNodeName);

      final boolean groupByResponse =
          task.getResultStrategy() != OAbstractRemoteTask.RESULT_STRATEGY.UNION;

      final boolean waitLocalNode = waitForLocalNode(cfg, iClusterNames, iNodes);

      // CREATE THE RESPONSE MANAGER
      final ODistributedResponseManager currentResponseMgr =
          responseManagerFactory.newResponseManager(
              iRequest,
              iNodes,
              task,
              nodesConcurToTheQuorum,
              availableNodes,
              expectedResponses,
              quorum,
              groupByResponse,
              waitLocalNode);

      if (localResult != null && currentResponseMgr.setLocalResult(localNodeName, localResult)) {
        // COLLECT LOCAL RESULT ONLY
        return currentResponseMgr.getFinalResponse();
      }

      // SORT THE NODE TO GUARANTEE THE SAME ORDER OF DELIVERY
      if (!(iNodes instanceof List)) iNodes = new ArrayList<String>(iNodes);
      if (iNodes.size() > 1) Collections.sort((List<String>) iNodes);

      msgService.registerRequest(iRequest.getId().getMessageId(), currentResponseMgr);

      if (ODistributedServerLog.isDebugEnabled())
        ODistributedServerLog.debug(
            this, localNodeName, iNodes.toString(), OUT, "Sending request %s...", iRequest);

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
            ODistributedServerLog.debug(
                this,
                localNodeName,
                node,
                OUT,
                "Error on sending distributed request %s. The target node is not available. Active nodes: %s",
                e,
                iRequest,
                manager.getAvailableNodeNames(databaseName));
          else
            ODistributedServerLog.error(
                this,
                localNodeName,
                node,
                OUT,
                "Error on sending distributed request %s (err=%s). Active nodes: %s",
                iRequest,
                reason,
                manager.getAvailableNodeNames(databaseName));
        }
      }

      if (currentResponseMgr.getExpectedNodes().isEmpty())
        // NO SERVER TO SEND A MESSAGE
        throw new ODistributedException(
            "No server active for distributed request ("
                + iRequest
                + ") against database '"
                + databaseName
                + (iClusterNames != null ? "." + iClusterNames : "")
                + "' to nodes "
                + iNodes);

      if (ODistributedServerLog.isDebugEnabled())
        ODistributedServerLog.debug(
            this, localNodeName, iNodes.toString(), OUT, "Sent request %s", iRequest);

      totalSentRequests.incrementAndGet();

      if (iExecutionMode == ODistributedRequest.EXECUTION_MODE.RESPONSE)
        return waitForResponse(iRequest, currentResponseMgr);

      return null;

    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      String names = iClusterNames != null ? "." + iClusterNames : "";
      throw OException.wrapException(
          new ODistributedException(
              "Error on executing distributed request ("
                  + iRequest
                  + ") against database '"
                  + databaseName
                  + names
                  + "' to nodes "
                  + iNodes),
          e);
    }
  }

  private long adjustTimeoutWithLatency(
      final Collection<String> iNodes, final long timeout, final ODistributedRequestId requestId) {
    long delta = 0;
    if (iNodes != null)
      for (String n : iNodes) {
        // UPDATE THE TIMEOUT WITH THE CURRENT SERVER LATENCY
        final long l = msgService.getCurrentLatency(n);
        delta = Math.max(delta, l);
      }

    if (delta > 500)
      ODistributedServerLog.debug(
          this,
          localNodeName,
          iNodes.toString(),
          OUT,
          "Adjusted timeouts by adding +%dms because this is the maximum latency recorded against servers %s (reqId=%s)",
          delta,
          iNodes,
          requestId);

    return timeout + delta;
  }

  @Override
  public void setOnline() {
    OAbstractPaginatedStorage storage =
        ((OAbstractPaginatedStorage) manager.getStorage(databaseName).getUnderlying());
    if (storage != null) {
      sequenceManager.fill(storage.getLastMetadata());
    }
    ODistributedServerLog.info(
        this,
        localNodeName,
        null,
        DIRECTION.NONE,
        "Publishing ONLINE status for database %s.%s...",
        localNodeName,
        databaseName);

    // SET THE NODE.DB AS ONLINE
    manager.setDatabaseStatus(
        localNodeName, databaseName, ODistributedServerManager.DB_STATUS.ONLINE);
    waitForOnline.countDown();
  }

  @Override
  public void unlockResourcesOfServer(
      final ODatabaseDocumentInternal database, final String serverName) {
    final int nodeLeftId = manager.getNodeIdByName(serverName);

    final Iterator<ODistributedTxContext> pendingReqIterator = activeTxContexts.values().iterator();
    while (pendingReqIterator.hasNext()) {
      final ODistributedTxContext pReq = pendingReqIterator.next();
      if (pReq != null && pReq.getReqId().getNodeId() == nodeLeftId) {

        ODistributedServerLog.debug(
            this,
            manager.getLocalNodeName(),
            null,
            DIRECTION.NONE,
            "Distributed transaction: rolling back transaction (req=%s)",
            pReq.getReqId());

        try {
          pReq.rollback(database);
          pReq.destroy();
        } catch (Exception | Error t) {
          // IGNORE IT
          ODistributedServerLog.error(
              this,
              manager.getLocalNodeName(),
              null,
              DIRECTION.NONE,
              "Distributed transaction: error on rolling back transaction (req=%s)",
              pReq.getReqId());
        }
        pendingReqIterator.remove();
      }
    }
  }

  public ValidationResult validate(OTransactionId id) {
    // this check should happen only of destination nodes
    return sequenceManager.validateTransactionId(id);
  }

  @Override
  public OTxMetadataHolder commit(OTransactionId id) {
    return sequenceManager.notifySuccess(id);
  }

  @Override
  public void rollback(OTransactionId id) {
    sequenceManager.notifyFailure(id);
  }

  @Override
  public ODistributedTxContext registerTxContext(
      final ODistributedRequestId reqId, ODistributedTxContext ctx) {
    final ODistributedTxContext prevCtx = activeTxContexts.put(reqId, ctx);
    if (prevCtx != ctx && prevCtx != null) {
      prevCtx.destroy();
    }
    return ctx;
  }

  @Override
  public Optional<OTransactionId> nextId() {
    return sequenceManager.next();
  }

  @Override
  public List<OTransactionId> missingTransactions(OTransactionSequenceStatus lastState) {
    return sequenceManager.missingTransactions(lastState);
  }

  @Override
  public ODistributedTxContext popTxContext(final ODistributedRequestId requestId) {
    final ODistributedTxContext ctx = activeTxContexts.remove(requestId);
    ODistributedServerLog.debug(
        this,
        localNodeName,
        null,
        DIRECTION.NONE,
        "Distributed transaction: pop request %s for database %s -> %s",
        requestId,
        databaseName,
        ctx);
    return ctx;
  }

  @Override
  public ODistributedTxContext getTxContext(final ODistributedRequestId requestId) {
    final ODistributedTxContext ctx = activeTxContexts.get(requestId);
    ODistributedServerLog.debug(
        this,
        localNodeName,
        null,
        DIRECTION.NONE,
        "Distributed transaction: pop request %s for database %s -> %s",
        requestId,
        databaseName,
        ctx);
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
      final String path =
          manager.getServerInstance().getDatabaseDirectory()
              + databaseName
              + "/"
              + DISTRIBUTED_SYNC_JSON_FILENAME;
      final File cfgFile = new File(path);
      try {
        syncConfiguration = new ODistributedSyncConfiguration(manager, databaseName, cfgFile);
      } catch (IOException e) {
        throw OException.wrapException(
            new ODistributedException(
                "Cannot open database distributed sync configuration file: " + cfgFile),
            e);
      }
    }

    return syncConfiguration;
  }

  @Override
  public void handleUnreachableNode(final String nodeName) {
    if (!running) {
      return;
    }
    ODistributedServerLog.debug(
        this,
        manager.getLocalNodeName(),
        nodeName,
        DIRECTION.IN,
        "Distributed transaction: rolling back all the pending transactions coordinated by the unreachable server '%s'",
        nodeName);

    final OUnreachableServerLocalTask task = new OUnreachableServerLocalTask(nodeName);
    final ODistributedRequest rollbackRequest =
        new ODistributedRequest(
            null, manager.getLocalNodeId(), manager.getNextMessageIdCounter(), null, task);
    processRequest(rollbackRequest, false);
  }

  @Override
  public String getDatabaseName() {
    return databaseName;
  }

  @Override
  public ODatabaseDocumentInternal getDatabaseInstance() {
    return manager
        .getServerInstance()
        .openDatabase(databaseName, "internal", "internal", null, true);
  }

  @Override
  public long getReceivedRequests() {
    return totalReceivedRequests.get();
  }

  @Override
  public long getProcessedRequests() {
    return requestExecutor.getCompletedTaskCount();
  }

  public void onDropShutdown() {
    // Drop is often called directly from the exeutor so it cannot wait itself to finish
    shutdown(false);
  }

  public void shutdown() {
    shutdown(true);
  }

  public void shutdown(boolean wait) {
    waitPending();
    running = false;

    try {
      if (txTimeoutTask != null) txTimeoutTask.cancel();
      requestExecutor.shutdown();
      if (wait) {
        try {
          requestExecutor.awaitTermination(1, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
        }
      }

      // SAVE SYNC CONFIGURATION
      try {
        getSyncConfiguration().save();
      } catch (IOException e) {
        ODistributedServerLog.warn(
            this,
            localNodeName,
            null,
            DIRECTION.NONE,
            "Error on saving distributed LSN table for database '%s'",
            databaseName);
      }
      syncConfiguration = null;

      activeTxContexts.clear();

      Orient.instance()
          .getProfiler()
          .unregisterHookValue("distributed.db." + databaseName + ".msgSent");
      Orient.instance()
          .getProfiler()
          .unregisterHookValue("distributed.db." + databaseName + ".msgReceived");
      Orient.instance()
          .getProfiler()
          .unregisterHookValue("distributed.db." + databaseName + ".activeContexts");
      Orient.instance()
          .getProfiler()
          .unregisterHookValue("distributed.db." + databaseName + ".workerThreads");
      Orient.instance()
          .getProfiler()
          .unregisterHookValue("distributed.db." + databaseName + ".recordLocks");

    } finally {

      final ODistributedServerManager.DB_STATUS serverStatus =
          manager.getDatabaseStatus(manager.getLocalNodeName(), databaseName);

      if (serverStatus == ODistributedServerManager.DB_STATUS.ONLINE
          || serverStatus == ODistributedServerManager.DB_STATUS.SYNCHRONIZING) {
        try {
          manager.setDatabaseStatus(
              manager.getLocalNodeName(),
              databaseName,
              ODistributedServerManager.DB_STATUS.NOT_AVAILABLE);
        } catch (Exception e) {
          // IGNORE IT
        }
      }
    }
  }

  private void waitPending() {
    while (pending.get() > 0) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }
    }
  }

  protected void checkForServerOnline(final ODistributedRequest iRequest)
      throws ODistributedException {
    final ODistributedServerManager.NODE_STATUS srvStatus = manager.getNodeStatus();
    if (srvStatus == ODistributedServerManager.NODE_STATUS.OFFLINE
        || srvStatus == ODistributedServerManager.NODE_STATUS.SHUTTINGDOWN) {
      ODistributedServerLog.error(
          this,
          localNodeName,
          null,
          OUT,
          "Local server is not online (status='%s'). Request %s will be ignored",
          srvStatus,
          iRequest);
      throw new OOfflineNodeException(
          "Local server is not online (status='"
              + srvStatus
              + "'). Request "
              + iRequest
              + " will be ignored");
    }
  }

  protected boolean waitForLocalNode(
      final ODistributedConfiguration cfg,
      final Collection<String> iClusterNames,
      final Collection<String> iNodes) {
    boolean waitLocalNode = false;
    if (iNodes.contains(localNodeName))
      if (iClusterNames == null || iClusterNames.isEmpty()) {
        // DEFAULT CLUSTER (*)
        if (cfg.isReadYourWrites(null)) waitLocalNode = true;
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

  protected int calculateQuorum(
      final OCommandDistributedReplicateRequest.QUORUM_TYPE quorumType,
      Collection<String> clusterNames,
      final ODistributedConfiguration cfg,
      final int totalServers,
      final int totalMasterServers,
      int onlineMasters,
      final boolean checkNodesAreOnline,
      final String localNodeName) {

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

    if (quorum < 0) quorum = 0;

    if (checkNodesAreOnline && quorum > totalServerInQuorum)
      throw new ODistributedException(
          "Quorum ("
              + quorum
              + ") cannot be reached on server '"
              + localNodeName
              + "' database '"
              + databaseName
              + "' because it is major than available nodes ("
              + totalServerInQuorum
              + ")");

    return quorum;
  }

  protected ODistributedResponse waitForResponse(
      final ODistributedRequest iRequest, final ODistributedResponseManager currentResponseMgr)
      throws InterruptedException {
    final long beginTime = System.currentTimeMillis();

    // WAIT FOR THE MINIMUM SYNCHRONOUS RESPONSES (QUORUM)
    if (!currentResponseMgr.waitForSynchronousResponses()) {
      final long elapsed = System.currentTimeMillis() - beginTime;

      if (elapsed > currentResponseMgr.getSynchTimeout()) {

        ODistributedServerLog.warn(
            this,
            localNodeName,
            null,
            DIRECTION.IN,
            "Timeout (%dms) on waiting for synchronous responses from nodes=%s responsesSoFar=%s request=(%s)",
            elapsed,
            currentResponseMgr.getExpectedNodes(),
            currentResponseMgr.getRespondingNodes(),
            iRequest);
      }
    }

    return currentResponseMgr.getFinalResponse();
  }

  @Override
  public void checkNodeInConfiguration(
      final ODistributedConfiguration cfg, final String serverName) {
    manager.executeInDistributedDatabaseLock(
        databaseName,
        20000,
        cfg != null ? cfg.modify() : null,
        new OCallable<Void, OModifiableDistributedConfiguration>() {
          @Override
          public Void call(final OModifiableDistributedConfiguration lastCfg) {
            // GET LAST VERSION IN LOCK
            final List<String> foundPartition = lastCfg.addNewNodeInServerList(serverName);
            if (foundPartition != null) {
              ODistributedServerLog.info(
                  this,
                  localNodeName,
                  null,
                  DIRECTION.NONE,
                  "Adding node '%s' in partition: %s db=%s v=%d",
                  serverName,
                  foundPartition,
                  databaseName,
                  lastCfg.getVersion());
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
      throw new ODistributedException(
          "Cannot create configured distributed workers (" + totalWorkers + ")");
    else if (totalWorkers == 0) {
      // AUTOMATIC
      final int totalDatabases = manager.getManagedDatabases().size() + 1;

      final int cpus = Runtime.getRuntime().availableProcessors();

      if (cpus > 1) totalWorkers = cpus / totalDatabases;

      if (totalWorkers == 0) totalWorkers = 1;
    }

    synchronized (this) {
      this.requestExecutor =
          new ThreadPoolExecutor(
              0,
              totalWorkers,
              1,
              TimeUnit.HOURS,
              new LinkedBlockingQueue<>(),
              new ThreadFactory() {
                @Override
                public Thread newThread(Runnable r) {
                  Thread thread = new Thread(r);
                  thread.setName(
                      "OrientDB DistributedWorker node="
                          + getLocalNodeName()
                          + " db="
                          + databaseName);
                  thread.setUncaughtExceptionHandler(
                      new Thread.UncaughtExceptionHandler() {
                        @Override
                        public void uncaughtException(Thread t, Throwable e) {
                          OLogManager.instance().error(t, "Exception in distributed executor", e);
                        }
                      });
                  return thread;
                }
              });
    }
  }

  @Override
  public void setLSN(
      final String sourceNodeName,
      final OLogSequenceNumber taskLastLSN,
      final boolean updateLastOperationTimestamp)
      throws IOException {
    if (taskLastLSN == null) return;

    final ODistributedSyncConfiguration cfg = getSyncConfiguration();
    cfg.setLastLSN(sourceNodeName, taskLastLSN, updateLastOperationTimestamp);
  }

  private void startTxTimeoutTimerTask() {
    txTimeoutTask =
        new TimerTask() {
          @Override
          public void run() {
            ODatabaseDocumentInternal database = null;
            try {
              final long now = System.currentTimeMillis();
              final long timeout =
                  OGlobalConfiguration.DISTRIBUTED_TX_EXPIRE_TIMEOUT.getValueAsLong();

              for (final Iterator<ODistributedTxContext> it = activeTxContexts.values().iterator();
                  it.hasNext(); ) {
                if (!isRunning()) break;

                final ODistributedTxContext ctx = it.next();
                if (ctx != null) {
                  final long started = ctx.getStartedOn();
                  final long elapsed = now - started;
                  if (elapsed > timeout) {
                    // TRANSACTION EXPIRED, ROLLBACK IT

                    if (database == null)
                      // GET THE DATABASE THE FIRST TIME
                      database = getDatabaseInstance();

                    ODistributedServerLog.debug(
                        this,
                        localNodeName,
                        null,
                        DIRECTION.NONE,
                        "Distributed transaction %s on database '%s' is expired after %dms",
                        ctx.getReqId(),
                        databaseName,
                        elapsed);

                    if (database != null) database.activateOnCurrentThread();

                    try {
                      ctx.cancel(manager, database);

                      if (ctx.getReqId().getNodeId() == manager.getLocalNodeId())
                        // REQUEST WAS ORIGINATED FROM CURRENT SERVER
                        msgService.timeoutRequest(ctx.getReqId().getMessageId());

                    } catch (Exception t) {
                      ODistributedServerLog.info(
                          this,
                          localNodeName,
                          null,
                          DIRECTION.NONE,
                          "Error on rolling back distributed transaction %s on database '%s' (err=%s)",
                          ctx.getReqId(),
                          databaseName,
                          t);
                    } finally {
                      it.remove();
                    }
                  }
                }
              }

            } catch (Exception t) {
              // CATCH EVERYTHING TO AVOID THE TIMER IS CANCELED
              ODistributedServerLog.info(
                  this,
                  localNodeName,
                  null,
                  DIRECTION.NONE,
                  "Error on checking for expired distributed transaction on database '%s'",
                  databaseName);
            } finally {
              if (database != null) {
                database.activateOnCurrentThread();
                database.close();
              }
            }
          }
        };
  }

  private boolean isRunning() {
    return running;
  }

  public void suspend() {
    boolean parsing = this.parsing.get();
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
    LinkedBlockingQueue<OFreezeGuard> latch = new LinkedBlockingQueue<OFreezeGuard>(1);
    this.lockManager.freeze(
        (guards) -> {
          try {
            latch.put(guards);
          } catch (InterruptedException e) {
            throw new OInterruptedException(e.getMessage());
          }
        });
    try {
      this.freezeGuard = latch.take();
    } catch (InterruptedException e) {
      throw new OInterruptedException(e.getMessage());
    }
  }

  public void resume() {
    this.parsing.set(true);
    if (this.freezeGuard != null) {
      this.freezeGuard.release();
    }
  }

  @Override
  public String dump() {
    final StringBuilder buffer = new StringBuilder(1024);

    buffer.append(
        "\n\nDATABASE '" + databaseName + "' ON SERVER '" + manager.getLocalNodeName() + "'");

    buffer.append("\n- MESSAGES IN QUEUES");
    buffer.append(" (" + (requestExecutor.getPoolSize()) + " WORKERS):");

    return buffer.toString();
  }

  public Map<ODistributedRequestId, ODistributedTxContext> getActiveTxContexts() {
    return activeTxContexts;
  }

  @Override
  public void validateStatus(OTransactionSequenceStatus status) {
    List<OTransactionId> res = sequenceManager.checkSelfStatus(status);
    res.removeAll(this.inQueue);
    if (!res.isEmpty()) {
      Orient.instance()
          .submit(
              () -> {
                manager.installDatabase(false, databaseName, true, true);
              });
    }
  }

  @Override
  public Optional<OTransactionSequenceStatus> status() {
    if (sequenceManager == null) {
      return Optional.empty();
    } else {
      return Optional.of(sequenceManager.currentStatus());
    }
  }

  @Override
  public void checkReverseSync(OTransactionSequenceStatus lastState) {
    List<OTransactionId> res = sequenceManager.checkSelfStatus(lastState);
    if (!res.isEmpty()) {
      new Thread(
              () -> {
                manager.installDatabase(false, databaseName, true, true);
              })
          .start();
    }
  }

  public List<OLockGuard> localLock(OLockKeySource keySource) {
    SortedSet<ORID> rids = keySource.getRids();
    SortedSet<OTransactionUniqueKey> uniqueKeys = keySource.getUniqueKeys();
    OTransactionId txId = keySource.getTransactionId();
    LinkedBlockingQueue<List<OLockGuard>> latch = new LinkedBlockingQueue<List<OLockGuard>>(1);
    this.lockManager.lock(
        rids,
        uniqueKeys,
        txId,
        (guards) -> {
          try {
            latch.put(guards);
          } catch (InterruptedException e) {
            throw new OInterruptedException(e.getMessage());
          }
        });
    try {
      return latch.take();
    } catch (InterruptedException e) {
      throw new OInterruptedException(e.getMessage());
    }
  }

  public void localUnlock(List<OLockGuard> guards) {
    this.lockManager.unlock(guards);
  }
}
