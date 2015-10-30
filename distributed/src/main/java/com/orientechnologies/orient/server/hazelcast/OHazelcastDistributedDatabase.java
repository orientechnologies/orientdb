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
package com.orientechnologies.orient.server.hazelcast;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;

import com.hazelcast.core.IQueue;
import com.orientechnologies.common.util.OPair;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.server.distributed.ODistributedConfiguration;
import com.orientechnologies.orient.server.distributed.ODistributedDatabase;
import com.orientechnologies.orient.server.distributed.ODistributedException;
import com.orientechnologies.orient.server.distributed.ODistributedRequest;
import com.orientechnologies.orient.server.distributed.ODistributedResponse;
import com.orientechnologies.orient.server.distributed.ODistributedResponseManager;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog.DIRECTION;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.distributed.task.OAbstractRemoteTask;
import com.orientechnologies.orient.server.distributed.task.OCreateRecordTask;
import com.orientechnologies.orient.server.distributed.task.ODeleteRecordTask;
import com.orientechnologies.orient.server.distributed.task.OFixTxTask;
import com.orientechnologies.orient.server.distributed.task.OResurrectRecordTask;
import com.orientechnologies.orient.server.distributed.task.OSQLCommandTask;
import com.orientechnologies.orient.server.distributed.task.OTxTask;
import com.orientechnologies.orient.server.distributed.task.OUpdateRecordTask;

/**
 * Hazelcast implementation of distributed peer. There is one instance per database. Each node creates own instance to talk with
 * each others.
 *
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 */
public class OHazelcastDistributedDatabase implements ODistributedDatabase {

  public static final String                          NODE_QUEUE_PREFIX          = "orientdb.node.";
  public static final String                          NODE_QUEUE_PENDING_POSTFIX = ".pending";
  private static final String                         NODE_LOCK_PREFIX           = "orientdb.reqlock.";
  protected final OHazelcastPlugin                    manager;
  protected final OHazelcastDistributedMessageService msgService;
  protected final String                              databaseName;
  protected final Lock                                requestLock;
  protected final int                                 numWorkers                 = 8;
  protected volatile boolean                          restoringMessages          = false;
  protected AtomicBoolean                             status                     = new AtomicBoolean(false);
  protected List<ODistributedWorker>                  workers                    = new ArrayList<ODistributedWorker>();
  protected AtomicLong                                waitForMessageId           = new AtomicLong(-1);
  protected ConcurrentHashMap<ORID, String>           lockManager                = new ConcurrentHashMap<ORID, String>();

  public OHazelcastDistributedDatabase(final OHazelcastPlugin manager, final OHazelcastDistributedMessageService msgService,
      final String iDatabaseName) {
    this.manager = manager;
    this.msgService = msgService;
    this.databaseName = iDatabaseName;

    this.requestLock = manager.getHazelcastInstance().getLock(NODE_LOCK_PREFIX + iDatabaseName);

    checkLocalNodeInConfiguration();

    // CREATE 2 QUEUES FOR GENERIC REQUESTS + INSERT ONLY
    msgService.getQueue(OHazelcastDistributedMessageService.getRequestQueueName(getLocalNodeName(), databaseName));
    msgService.getQueue(OHazelcastDistributedMessageService.getRequestQueueName(getLocalNodeName(),
        databaseName + OCreateRecordTask.SUFFIX_QUEUE_NAME));
  }

  @Override
  public ODistributedResponse send2Nodes(final ODistributedRequest iRequest, final Collection<String> iClusterNames,
      final Collection<String> iNodes, final ODistributedRequest.EXECUTION_MODE iExecutionMode) {
    checkForServerOnline(iRequest);

    final String databaseName = iRequest.getDatabaseName();

    if (iNodes.isEmpty()) {
      ODistributedServerLog.error(this, getLocalNodeName(), null, DIRECTION.OUT,
          "No nodes configured for database '%s' request: %s", databaseName, iRequest);
      throw new ODistributedException("No nodes configured for partition '" + databaseName + "' request: " + iRequest);
    }

    final ODistributedConfiguration cfg = manager.getDatabaseConfiguration(databaseName);

    // TODO: REALLY STILL MATTERS THE NUMBER OF THE QUEUES?
    final OPair<String, IQueue<ODistributedRequest>>[] reqQueues = getRequestQueues(databaseName, iNodes, iRequest.getTask());

    iRequest.setSenderNodeName(getLocalNodeName());

    int availableNodes;
    if (iRequest.getTask().isRequireNodeOnline()) {
      // CHECK THE ONLINE NODES
      availableNodes = 0;
      int i = 0;
      for (String node : iNodes) {
        if (reqQueues[i].getValue() != null && manager.isNodeAvailable(node, databaseName))
          availableNodes++;
        else {
          if (ODistributedServerLog.isDebugEnabled())
            ODistributedServerLog.debug(this, getLocalNodeName(), node, DIRECTION.OUT,
                "skip expected response from node '%s' for request %s because it's not online (queue=%s)", node, iRequest,
                reqQueues[i].getValue() != null);
        }
        ++i;
      }
    } else {
      // EXPECT ANSWER FROM ALL NODES WITH A QUEUE
      availableNodes = 0;
      for (OPair<String, IQueue<ODistributedRequest>> q : reqQueues)
        if (q.getValue() != null)
          availableNodes++;
    }

    final int quorum = calculateQuorum(iRequest, iClusterNames, cfg, availableNodes, iExecutionMode);

    final int queueSize = iNodes.size();
    int expectedSynchronousResponses = availableNodes;

    final boolean groupByResponse;
    if (iRequest.getTask().getResultStrategy() == OAbstractRemoteTask.RESULT_STRATEGY.UNION) {
      expectedSynchronousResponses = availableNodes;
      groupByResponse = false;
    } else {
      groupByResponse = true;
    }

    final boolean waitLocalNode = waitForLocalNode(cfg, iClusterNames, iNodes);

    // CREATE THE RESPONSE MANAGER
    final ODistributedResponseManager currentResponseMgr = new ODistributedResponseManager(manager, iRequest, iNodes,
        expectedSynchronousResponses, quorum, waitLocalNode, iRequest.getTask().getSynchronousTimeout(expectedSynchronousResponses),
        iRequest.getTask().getTotalTimeout(queueSize), groupByResponse);

    final long timeout = OGlobalConfiguration.DISTRIBUTED_QUEUE_TIMEOUT.getValueAsLong();

    final int queueMaxSize = OGlobalConfiguration.DISTRIBUTED_QUEUE_MAXSIZE.getValueAsInteger();

    try {
      requestLock.lock();
      try {
        // LOCK = ASSURE MESSAGES IN THE QUEUE ARE INSERTED SEQUENTIALLY AT CLUSTER LEVEL
        // BROADCAST THE REQUEST TO ALL THE NODE QUEUES

        // TODO: CAN I MOVE THIS OUTSIDE?
        iRequest.setId(msgService.getMessageIdCounter().getAndIncrement());

        if (ODistributedServerLog.isDebugEnabled())
          ODistributedServerLog.debug(this, getLocalNodeName(), iNodes.toString(), DIRECTION.OUT, "sending request %s", iRequest);

        // TODO: CAN I MOVE THIS OUTSIDE?
        msgService.registerRequest(iRequest.getId(), currentResponseMgr);

        for (OPair<String, IQueue<ODistributedRequest>> entry : reqQueues) {
          final IQueue queue = entry.getValue();

          if (queue != null) {
            if (queueMaxSize > 0 && queue.size() > queueMaxSize) {
              ODistributedServerLog.warn(this, getLocalNodeName(), iNodes.toString(), DIRECTION.OUT,
                  "queue has too many messages (%d), treating the node as in stall: trying to restart it...", queue.size());
              queue.clear();

              manager.unjoinNode(entry.getKey());

            } else {
              // SEND THE MESSAGE
              queue.offer(iRequest, timeout, TimeUnit.MILLISECONDS);
            }
          }
        }

      } finally {
        requestLock.unlock();
      }

      if (ODistributedServerLog.isDebugEnabled())
        ODistributedServerLog.debug(this, getLocalNodeName(), iNodes.toString(), DIRECTION.OUT, "sent request %s", iRequest);

      Orient.instance().getProfiler().updateCounter("distributed.db." + databaseName + ".msgSent",
          "Number of replication messages sent from current node", +1, "distributed.db.*.msgSent");

      return waitForResponse(iRequest, currentResponseMgr);

    } catch (Throwable e) {
      throw new ODistributedException("Error on executing distributed request (" + iRequest + ") against database '" + databaseName
          + (iClusterNames != null ? "." + iClusterNames : "") + "' to nodes " + iNodes, e);
    }
  }

  public boolean isRestoringMessages() {
    return restoringMessages;
  }

  public OHazelcastDistributedDatabase configureDatabase(final boolean iRestoreMessages, final boolean iUnqueuePendingMessages,
      Callable<Void> iCallback) {
    // CREATE A QUEUE PER DATABASE REQUESTS
    final String queueName = OHazelcastDistributedMessageService.getRequestQueueName(getLocalNodeName(), databaseName);
    final IQueue<ODistributedRequest> requestQueue = msgService.getQueue(queueName);

    final ODistributedWorker listenerThread = unqueuePendingMessages(iRestoreMessages, iUnqueuePendingMessages, queueName,
        requestQueue);

    workers.add(listenerThread);

    if (iCallback != null)
      try {
        iCallback.call();
      } catch (Exception e) {
        throw new ODistributedException(e);
      }

    setOnline();

    return this;
  }

  @Override
  public void setOnline() {
    if (status.compareAndSet(false, true)) {
      ODistributedServerLog.info(this, getLocalNodeName(), null, DIRECTION.NONE, "Publishing online status for database %s.%s...",
          getLocalNodeName(), databaseName);

      // SET THE NODE.DB AS ONLINE
      manager.setDatabaseStatus(getLocalNodeName(), databaseName, ODistributedServerManager.DB_STATUS.ONLINE);
    }
  }

  @Override
  public boolean lockRecord(final ORID iRecord, final String iNodeName) {
    final boolean locked = lockManager.putIfAbsent(iRecord, iNodeName) == null;

    if (ODistributedServerLog.isDebugEnabled())
      if (locked)
        ODistributedServerLog.debug(this, getLocalNodeName(), null, DIRECTION.NONE,
            "Distributed transaction: locked record %s in database '%s' owned by server '%s'", iRecord, databaseName, iNodeName);
      else
        ODistributedServerLog.debug(this, getLocalNodeName(), null, DIRECTION.NONE,
            "Distributed transaction: cannot lock record %s in database '%s' owned by server '%s'", iRecord, databaseName,
            iNodeName);

    return locked;
  }

  @Override
  public void unlockRecord(final ORID iRecord) {
    lockManager.remove(iRecord);

    if (ODistributedServerLog.isDebugEnabled())
      ODistributedServerLog.debug(this, getLocalNodeName(), null, DIRECTION.NONE,
          "Distributed transaction: unlocked record %s in database '%s'", iRecord, databaseName);
  }

  @Override
  public void unlockRecords(final String iNodeName) {
    int unlocked = 0;
    final Iterator<Map.Entry<ORID, String>> it = lockManager.entrySet().iterator();
    while (it.hasNext()) {
      final Map.Entry<ORID, String> v = it.next();
      if (v != null && iNodeName.equals(v.getValue())) {
        // FOUND: UNLOCK IT
        it.remove();
        unlocked++;
      }
    }

    if (ODistributedServerLog.isDebugEnabled())
      ODistributedServerLog.debug(this, getLocalNodeName(), null, DIRECTION.NONE,
          "Distributed transaction: unlocked %d locks in database '%s' owned by server '%s'", unlocked, databaseName, iNodeName);
  }

  public OHazelcastDistributedDatabase setWaitForMessage(final long iMessageId) {
    ODistributedServerLog.debug(this, getLocalNodeName(), null, DIRECTION.NONE,
        "waiting for message id %d (discard all previous ones if any)...", iMessageId);

    waitForMessageId.set(iMessageId);
    return this;
  }

  public void shutdown() {
    for (int i = 0; i < workers.size(); ++i)
      workers.get(i).shutdown();
  }

  protected ODistributedWorker unqueuePendingMessages(boolean iRestoreMessages, boolean iUnqueuePendingMessages, String queueName,
      IQueue<ODistributedRequest> requestQueue) {
    if (ODistributedServerLog.isDebugEnabled())
      ODistributedServerLog.debug(this, getLocalNodeName(), null, DIRECTION.NONE, "listening for incoming requests on queue: %s",
          queueName);

    // UNDO PREVIOUS MESSAGE IF ANY
    restoreMessagesBeforeFailure(iRestoreMessages);

    restoringMessages = msgService.checkForPendingMessages(requestQueue, queueName, iUnqueuePendingMessages);

    final ODistributedWorker listenerThread = new ODistributedWorker(this, requestQueue, databaseName, 0, restoringMessages);
    listenerThread.initDatabaseInstance();

    listenerThread.start();

    return listenerThread;
  }

  protected void checkForServerOnline(ODistributedRequest iRequest) throws ODistributedException {
    final ODistributedServerManager.NODE_STATUS srvStatus = manager.getNodeStatus();
    if (srvStatus == ODistributedServerManager.NODE_STATUS.OFFLINE
        || srvStatus == ODistributedServerManager.NODE_STATUS.SHUTTINGDOWN) {
      ODistributedServerLog.error(this, getLocalNodeName(), null, DIRECTION.OUT,
          "Local server is not online (status='%s'). Request %s will be ignored", srvStatus, iRequest);
      throw new ODistributedException(
          "Local server is not online (status='" + srvStatus + "'). Request " + iRequest + " will be ignored");
    }
  }

  protected boolean waitForLocalNode(final ODistributedConfiguration cfg, final Collection<String> iClusterNames,
      final Collection<String> iNodes) {
    boolean waitLocalNode = false;
    if (iNodes.contains(getLocalNodeName()))
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

  protected int calculateQuorum(final ODistributedRequest iRequest, final Collection<String> clusterNames,
      final ODistributedConfiguration cfg, final int iAvailableNodes, final ODistributedRequest.EXECUTION_MODE iExecutionMode) {

    if (iAvailableNodes == 0 && iExecutionMode == ODistributedRequest.EXECUTION_MODE.RESPONSE)
      throw new ODistributedException("Quorum cannot be reached because there are no nodes available");

    final String clusterName = clusterNames == null || clusterNames.isEmpty() ? null : clusterNames.iterator().next();

    int quorum = 0;

    final OCommandDistributedReplicateRequest.QUORUM_TYPE quorumType = iRequest.getTask().getQuorumType();

    switch (quorumType) {
    case NONE:
      // IGNORE IT
      break;
    case READ:
      quorum = cfg.getReadQuorum(clusterName);
      break;
    case WRITE:
      quorum = cfg.getWriteQuorum(clusterName);
      break;
    case ALL:
      quorum = iAvailableNodes;
      break;
    }

    if (quorum > iAvailableNodes) {
      final boolean failureAvailableNodesLessQuorum = cfg.getFailureAvailableNodesLessQuorum(clusterName);
      if (failureAvailableNodesLessQuorum)
        throw new ODistributedException(
            "Quorum cannot be reached because it is major than available nodes and failureAvailableNodesLessQuorum=true");
      else {
        // SET THE QUORUM TO THE AVAILABLE NODE SIZE
        ODistributedServerLog.debug(this, getLocalNodeName(), null, DIRECTION.NONE,
            "quorum less then available nodes, downgrade quorum to %d", iAvailableNodes);
        quorum = iAvailableNodes;
      }
    }

    return quorum;
  }

  protected ODistributedResponse waitForResponse(final ODistributedRequest iRequest,
      final ODistributedResponseManager currentResponseMgr) throws InterruptedException {
    if (iRequest.getExecutionMode() == ODistributedRequest.EXECUTION_MODE.NO_RESPONSE)
      return null;

    final long beginTime = System.currentTimeMillis();

    // WAIT FOR THE MINIMUM SYNCHRONOUS RESPONSES (QUORUM)
    if (!currentResponseMgr.waitForSynchronousResponses()) {
      ODistributedServerLog.warn(this, getLocalNodeName(), null, DIRECTION.IN,
          "timeout (%dms) on waiting for synchronous responses from nodes=%s responsesSoFar=%s request=%s",
          System.currentTimeMillis() - beginTime, currentResponseMgr.getExpectedNodes(), currentResponseMgr.getRespondingNodes(),
          iRequest);
    }

    return currentResponseMgr.getFinalResponse();
  }

  protected OPair<String, IQueue<ODistributedRequest>>[] getRequestQueues(final String iDatabaseName,
      final Collection<String> nodes, final OAbstractRemoteTask iTask) {
    final OPair<String, IQueue<ODistributedRequest>>[] queues = new OPair[nodes.size()];

    int i = 0;

    // GET ALL THE EXISTENT QUEUES
    for (String node : nodes) {
      final String queueName = OHazelcastDistributedMessageService.getRequestQueueName(node, iDatabaseName);
      final IQueue<ODistributedRequest> queue = msgService.getQueue(queueName);
      queues[i++] = new OPair<String, IQueue<ODistributedRequest>>(node, queue);
    }

    return queues;
  }

  /**
   * Composes the undo queue name based on node name.
   */
  protected String getPendingRequestMapName() {
    final StringBuilder buffer = new StringBuilder(128);
    buffer.append(NODE_QUEUE_PREFIX);
    buffer.append(getLocalNodeName());
    buffer.append(NODE_QUEUE_PENDING_POSTFIX);
    return buffer.toString();
  }

  protected String getLocalNodeName() {
    return manager.getLocalNodeName();
  }

  protected void restoreMessagesBeforeFailure(final boolean iRestoreMessages) {
    for (int i = 0; i < workers.size(); ++i)
      workers.get(i).restoreMessagesBeforeFailure(iRestoreMessages);
  }

  /**
   * Checks if last pending operation must be re-executed or not. In some circustamces the exception
   * OHotAlignmentNotPossibleExeption is raised because it's not possible to recover the database state.
   *
   * @throws OHotAlignmentNotPossibleExeption
   */
  protected void hotAlignmentError(final ODistributedRequest iLastPendingRequest, final String iMessage, final Object... iParams)
      throws OHotAlignmentNotPossibleExeption {
    final String msg = String.format(iMessage, iParams);

    ODistributedServerLog.warn(this, getLocalNodeName(), iLastPendingRequest.getSenderNodeName(), DIRECTION.IN, "- " + msg);
    throw new OHotAlignmentNotPossibleExeption(msg);
  }

  protected void checkLocalNodeInConfiguration() {
    final Lock lock = manager.getLock("orientdb." + databaseName + ".cfg");
    lock.lock();
    try {
      // GET LAST VERSION IN LOCK
      final ODistributedConfiguration cfg = manager.getDatabaseConfiguration(databaseName);

      boolean distribCfgDirty = false;

      final List<String> foundPartition = cfg.addNewNodeInServerList(getLocalNodeName());
      if (foundPartition != null) {
        // SET THE NODE.DB AS OFFLINE, READY TO BE SYNCHRONIZED
        manager.setDatabaseStatus(getLocalNodeName(), databaseName, ODistributedServerManager.DB_STATUS.SYNCHRONIZING);

        ODistributedServerLog.info(this, getLocalNodeName(), null, DIRECTION.NONE, "adding node '%s' in partition: db=%s %s",
            getLocalNodeName(), databaseName, foundPartition);

        distribCfgDirty = true;
      }

      // SELF ASSIGN CLUSTERS PREVIOUSLY ASSIGNED TO THIS LOCAL NODE (BY SUFFIX)
      final String suffix2Search = "_" + getLocalNodeName();
      for (String c : cfg.getClusterNames()) {
        if (c.endsWith(suffix2Search)) {
          // FOUND: ASSIGN TO LOCAL NODE
          final String currentMaster = cfg.getMasterServer(c);

          if (!getLocalNodeName().equals(currentMaster)) {
            ODistributedServerLog.warn(this, getLocalNodeName(), null, DIRECTION.NONE,
                "changing mastership of cluster '%s' from node '%s' to '%s'", c, currentMaster, getLocalNodeName());
            cfg.setMasterServer(c, getLocalNodeName());
            distribCfgDirty = true;
          }
        }
      }

      if (distribCfgDirty)
        manager.updateCachedDatabaseConfiguration(databaseName, cfg.serialize(), true, true);

    } finally {
      lock.unlock();
    }
  }

  protected void removeNodeInConfiguration(final String iNode, final boolean iForce) {
    final Lock lock = manager.getLock("orientdb." + databaseName + ".cfg");
    lock.lock();
    try {
      // GET LAST VERSION IN LOCK
      final ODistributedConfiguration cfg = manager.getDatabaseConfiguration(databaseName);

      if (!cfg.isHotAlignment()) {
        final List<String> foundPartition = cfg.removeNodeInServerList(iNode, iForce);
        if (foundPartition != null) {
          ODistributedServerLog.info(this, getLocalNodeName(), null, ODistributedServerLog.DIRECTION.NONE,
              "removing node '%s' in partitions: db=%s %s", iNode, databaseName, foundPartition);

          msgService.removeQueue(OHazelcastDistributedMessageService.getRequestQueueName(iNode, databaseName));
          msgService.removeQueue(
              OHazelcastDistributedMessageService.getRequestQueueName(iNode, databaseName + OCreateRecordTask.SUFFIX_QUEUE_NAME));
        }

        // CHANGED: RE-DEPLOY IT
        manager.updateCachedDatabaseConfiguration(databaseName, cfg.serialize(), true, true);
      }

    } catch (Exception e) {
      ODistributedServerLog.debug(this, getLocalNodeName(), null, ODistributedServerLog.DIRECTION.NONE,
          "unable to remove node or change mastership for '%s' in distributed configuration, db=%s", e, iNode, databaseName);

    } finally {
      lock.unlock();
    }
  }

  protected boolean checkIfOperationHasBeenExecuted(final ODistributedRequest lastPendingRequest, final OAbstractRemoteTask task) {
    boolean executeLastPendingRequest = false;

    // ASK FOR RECORD
    if (task instanceof ODeleteRecordTask) {
      // EXECUTE ONLY IF THE RECORD HASN'T BEEN DELETED YET
      executeLastPendingRequest = ((ODeleteRecordTask) task).getRid().getRecord() != null;
    } else if (task instanceof OUpdateRecordTask) {
      final ORecord rec = ((OUpdateRecordTask) task).getRid().getRecord();
      if (rec == null)
        ODistributedServerLog.warn(this, getLocalNodeName(), lastPendingRequest.getSenderNodeName(), DIRECTION.IN,
            "- cannot update deleted record %s, database could be not aligned", ((OUpdateRecordTask) task).getRid());
      else
        // EXECUTE ONLY IF VERSIONS DIFFER
        executeLastPendingRequest = !rec.getRecordVersion().equals(((OUpdateRecordTask) task).getVersion());
    } else if (task instanceof OCreateRecordTask) {
      // EXECUTE ONLY IF THE RECORD HASN'T BEEN CREATED YET
      executeLastPendingRequest = ((OCreateRecordTask) task).getRid().getRecord() == null;
    } else if (task instanceof OSQLCommandTask) {
      if (!task.isIdempotent()) {
        hotAlignmentError(lastPendingRequest, "Not able to assure last command has been completed before last crash. Command='%s'",
            ((OSQLCommandTask) task).getPayload());
      }
    } else if (task instanceof OResurrectRecordTask) {
      if (((OResurrectRecordTask) task).getRid().getRecord() == null)
        // ALREADY DELETED: CANNOT RESTORE IT
        hotAlignmentError(lastPendingRequest, "Not able to resurrect deleted record '%s'", ((OResurrectRecordTask) task).getRid());
    } else if (task instanceof OTxTask) {
      // CHECK EACH TX ITEM IF HAS BEEN COMMITTED
      for (OAbstractRemoteTask t : ((OTxTask) task).getTasks()) {
        executeLastPendingRequest = checkIfOperationHasBeenExecuted(lastPendingRequest, t);
        if (executeLastPendingRequest)
          // REPEAT THE ENTIRE TX
          return true;
      }
    } else if (task instanceof OFixTxTask) {
      // CHECK EACH FIX-TX ITEM IF HAS BEEN COMMITTED
      for (OAbstractRemoteTask t : ((OFixTxTask) task).getTasks()) {
        executeLastPendingRequest = checkIfOperationHasBeenExecuted(lastPendingRequest, t);
        if (executeLastPendingRequest)
          // REPEAT THE ENTIRE TX
          return true;
      }
    } else
      hotAlignmentError(lastPendingRequest, "Not able to assure last operation has been completed before last crash. Task='%s'",
          task);
    return executeLastPendingRequest;
  }
}
