/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.server.hazelcast;

import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.IMap;
import com.hazelcast.core.IQueue;
import com.hazelcast.spi.exception.DistributedObjectDestroyedException;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.OScenarioThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.server.config.OServerUserConfiguration;
import com.orientechnologies.orient.server.distributed.ODistributedAbstractPlugin;
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

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;

/**
 * Hazelcast implementation of distributed peer. There is one instance per database. Each node creates own instance to talk with
 * each others.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OHazelcastDistributedDatabase implements ODistributedDatabase {

  public static final String                          NODE_QUEUE_PREFIX          = "orientdb.node.";
  public static final String                          NODE_QUEUE_REQUEST_POSTFIX = ".request";
  public static final String                          NODE_QUEUE_UNDO_POSTFIX    = ".undo";
  protected final static Map<String, IQueue<?>>       queues                     = new HashMap<String, IQueue<?>>();
  private static final String                         NODE_LOCK_PREFIX           = "orientdb.reqlock.";
  protected final OHazelcastPlugin                    manager;
  protected final OHazelcastDistributedMessageService msgService;
  protected final String                              databaseName;
  protected final Lock                                requestLock;
  protected volatile ODatabaseDocumentTx              database;
  protected volatile boolean                          restoringMessages          = false;
  protected AtomicBoolean                             status                     = new AtomicBoolean(false);
  protected Object                                    waitForOnline              = new Object();
  protected Thread                                    listenerThread;
  protected AtomicLong                                waitForMessageId           = new AtomicLong(-1);

  public OHazelcastDistributedDatabase(final OHazelcastPlugin manager, final OHazelcastDistributedMessageService msgService,
      final String iDatabaseName) {
    this.manager = manager;
    this.msgService = msgService;
    this.databaseName = iDatabaseName;

    this.requestLock = manager.getHazelcastInstance().getLock(NODE_LOCK_PREFIX + iDatabaseName);

    checkLocalNodeInConfiguration();

    // CREATE THE QUEUE
    msgService.getQueue(OHazelcastDistributedMessageService.getRequestQueueName(manager.getLocalNodeName(), databaseName));
  }

  @Override
  public ODistributedResponse send2Nodes(final ODistributedRequest iRequest, final Collection<String> iClusterNames,
      final Collection<String> iNodes) {
    final String databaseName = iRequest.getDatabaseName();

    if (iNodes.isEmpty()) {
      ODistributedServerLog.error(this, getLocalNodeName(), null, DIRECTION.OUT,
          "No nodes configured for database '%s' request: %s", databaseName, iRequest);
      throw new ODistributedException("No nodes configured for partition '" + databaseName + "' request: " + iRequest);
    }

    final ODistributedConfiguration cfg = manager.getDatabaseConfiguration(databaseName);

    final IQueue<ODistributedRequest>[] reqQueues = getRequestQueues(databaseName, iNodes);

    int quorum = calculateQuorum(iRequest, iNodes, cfg, iNodes);

    iRequest.setSenderNodeName(manager.getLocalNodeName());

    int availableNodes;
    if (iRequest.getTask().isRequireNodeOnline()) {
      // CHECK THE ONLINE NODES
      availableNodes = 0;
      int i = 0;
      for (String node : iNodes) {
        if (reqQueues[i] != null && manager.isNodeAvailable(node, databaseName))
          availableNodes++;
        else {
          if (ODistributedServerLog.isDebugEnabled())
            ODistributedServerLog.debug(this, getLocalNodeName(), node, DIRECTION.OUT,
                "skip expected response from node '%s' for request %s because it's not online (queue=%s)", node, iRequest,
                reqQueues[i] != null);
        }
        ++i;
      }
    } else {
      // EXPECT ANSWER FROM ALL NODES WITH A QUEUE
      availableNodes = 0;
      for (IQueue<ODistributedRequest> q : reqQueues)
        if (q != null)
          availableNodes++;
    }

    final int queueSize = iNodes.size();
    final boolean groupByResponse;
    int expectedSynchronousResponses = quorum > 0 ? Math.min(quorum, availableNodes) : 1;

    if (iRequest.getTask().getResultStrategy() == OAbstractRemoteTask.RESULT_STRATEGY.UNION) {
      expectedSynchronousResponses = availableNodes;
      groupByResponse = false;
    } else
      groupByResponse = true;

    final boolean waitLocalNode = waitForLocalNode(cfg, iClusterNames, iNodes);

    // CREATE THE RESPONSE MANAGER
    final ODistributedResponseManager currentResponseMgr = new ODistributedResponseManager(manager, iRequest, iNodes,
        expectedSynchronousResponses, quorum, waitLocalNode,
        iRequest.getTask().getSynchronousTimeout(expectedSynchronousResponses), iRequest.getTask().getTotalTimeout(queueSize),
        groupByResponse);

    if (ODistributedServerLog.isDebugEnabled())
      ODistributedServerLog.debug(this, getLocalNodeName(), iNodes.toString(), DIRECTION.OUT, "sending request %s",
          iRequest.getTask());

    final long timeout = OGlobalConfiguration.DISTRIBUTED_QUEUE_TIMEOUT.getValueAsLong();

    try {
      requestLock.lock();
      try {
        iRequest.setId(msgService.getMessageIdCounter().getAndIncrement());

        msgService.registerRequest(iRequest.getId(), currentResponseMgr);

        // LOCK = ASSURE MESSAGES IN THE QUEUE ARE INSERTED SEQUENTIALLY AT CLUSTER LEVEL
        // BROADCAST THE REQUEST TO ALL THE NODE QUEUES
        for (IQueue<ODistributedRequest> queue : reqQueues) {
          if (queue != null)
            queue.offer(iRequest, timeout, TimeUnit.MILLISECONDS);
        }

      } finally {
        requestLock.unlock();
      }

      if (ODistributedServerLog.isDebugEnabled())
        ODistributedServerLog.debug(this, getLocalNodeName(), iNodes.toString(), DIRECTION.OUT, "sent request %s",
            iRequest.getTask());

      Orient
          .instance()
          .getProfiler()
          .updateCounter("distributed.replication." + databaseName + ".msgSent",
              "Number of replication messages sent from current node", +1, "distributed.replication.*.msgSent");

      return waitForResponse(iRequest, currentResponseMgr);

    } catch (Throwable e) {
      throw new ODistributedException("Error on sending distributed request against database '" + databaseName
          + (iClusterNames != null ? "." + iClusterNames : "") + "' to nodes " + iNodes, e);
    }
  }

  public boolean isRestoringMessages() {
    return restoringMessages;
  }

  public OHazelcastDistributedDatabase configureDatabase(final boolean iRestoreMessages, final boolean iUnqueuePendingMessages) {
    // CREATE A QUEUE PER DATABASE
    final String queueName = OHazelcastDistributedMessageService.getRequestQueueName(manager.getLocalNodeName(), databaseName);
    final IQueue<ODistributedRequest> requestQueue = msgService.getQueue(queueName);

    if (ODistributedServerLog.isDebugEnabled())
      ODistributedServerLog.debug(this, getLocalNodeName(), null, DIRECTION.NONE, "listening for incoming requests on queue: %s",
          queueName);

    // UNDO PREVIOUS MESSAGE IF ANY
    final IMap<Object, Object> undoMap = restoreMessagesBeforeFailure(iRestoreMessages);

    restoringMessages = msgService.checkForPendingMessages(requestQueue, queueName, iUnqueuePendingMessages);

    listenerThread = new Thread(new Runnable() {
      @Override
      public void run() {
        Thread.currentThread().setName("OrientDB Node Request " + queueName);
        while (!Thread.interrupted()) {
          if (restoringMessages && requestQueue.isEmpty()) {
            // END OF RESTORING MESSAGES, SET IT ONLINE
            restoringMessages = false;
            setOnline();
          }

          String senderNode = null;
          ODistributedRequest message = null;
          try {
            message = readRequest(requestQueue);

            // SAVE THE MESSAGE IN THE UNDO MAP IN CASE OF FAILURE
            undoMap.put(databaseName, message);

            if (message != null) {
              senderNode = message.getSenderNodeName();
              onMessage(message);
            }

            // OK: REMOVE THE UNDO BUFFER
            undoMap.remove(databaseName);

          } catch (InterruptedException e) {
            // EXIT CURRENT THREAD
            Thread.interrupted();
            break;
          } catch (DistributedObjectDestroyedException e) {
            Thread.interrupted();
            break;
          } catch (HazelcastInstanceNotActiveException e) {
            Thread.interrupted();
            break;

          } catch (Throwable e) {
            ODistributedServerLog.error(this, getLocalNodeName(), senderNode, DIRECTION.IN,
                "error on reading distributed request: %s", e, message != null ? message.getTask() : "-");
          }
        }

        ODistributedServerLog.debug(this, manager.getLocalNodeName(), null, DIRECTION.NONE,
            "end of reading requests for database %s", databaseName);
      }
    });
    listenerThread.start();

    return this;
  }

  public void initDatabaseInstance() {
    if (database == null) {
      // OPEN IT
      final OServerUserConfiguration replicatorUser = manager.getServerInstance().getUser(
          ODistributedAbstractPlugin.REPLICATOR_USER);
      database = (ODatabaseDocumentTx) manager.getServerInstance().openDatabase("document", databaseName, replicatorUser.name,
          replicatorUser.password);
    } else if (database.isClosed()) {
      // DATABASE CLOSED, REOPEN IT
      final OServerUserConfiguration replicatorUser = manager.getServerInstance().getUser(
          ODistributedAbstractPlugin.REPLICATOR_USER);
      database.open(replicatorUser.name, replicatorUser.password);
    }
  }

  @Override
  public void setOnline() {
    initDatabaseInstance();

    ODistributedServerLog.info(this, getLocalNodeName(), null, DIRECTION.NONE, "Publishing online status for database %s.%s...",
        manager.getLocalNodeName(), databaseName);

    // SET THE NODE.DB AS ONLINE
    manager.setDatabaseStatus(databaseName, ODistributedServerManager.DB_STATUS.ONLINE);

    status.set(true);

    ODistributedServerLog.info(this, getLocalNodeName(), null, DIRECTION.NONE,
        "Database %s.%s is online, waking up listeners on local node...", manager.getLocalNodeName(), databaseName);

    // WAKE UP ANY WAITERS
    synchronized (waitForOnline) {
      waitForOnline.notifyAll();
    }
  }

  public OHazelcastDistributedDatabase setWaitForMessage(final long iMessageId) {
    ODistributedServerLog.debug(this, manager.getLocalNodeName(), null, DIRECTION.NONE,
        "waiting for message id %d (discard all previous ones if any)...", iMessageId);

    waitForMessageId.set(iMessageId);
    return this;
  }

  public void shutdown() {
    if (listenerThread != null)
      listenerThread.interrupt();

    try {
      if (database != null)
        database.close();
    } catch (Exception e) {
    }
  }

  public ODatabaseDocumentTx getDatabase() {
    return database;
  }

  protected boolean waitForLocalNode(final ODistributedConfiguration cfg, final Collection<String> iClusterNames,
      final Collection<String> iNodes) {
    boolean waitLocalNode = false;
    if (iNodes.contains(manager.getLocalNodeName()))
      if (iClusterNames == null) {
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
      final ODistributedConfiguration cfg, final Collection<String> nodes) {
    final OAbstractRemoteTask.QUORUM_TYPE quorumType = iRequest.getTask().getQuorumType();

    final int queueSize = nodes.size();

    final String clusterName = clusterNames != null ? clusterNames.iterator().next() : null;

    int quorum = 0;
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
      quorum = queueSize;
      break;
    }

    if (quorum > queueSize) {
      final boolean failureAvailableNodesLessQuorum = cfg.getFailureAvailableNodesLessQuorum(clusterName);
      if (failureAvailableNodesLessQuorum)
        throw new ODistributedException(
            "Quorum cannot be reached because it is major than available nodes and 'failureAvailableNodesLessQuorum' settings is true");
      else {
        // SET THE QUORUM TO THE AVAILABLE NODE SIZE
        ODistributedServerLog.debug(this, getLocalNodeName(), nodes.toString(), DIRECTION.OUT,
            "quorum less then available nodes, downgrade quorum to %d", queueSize);
        quorum = queueSize;
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

  protected ODistributedRequest readRequest(final IQueue<ODistributedRequest> requestQueue) throws InterruptedException {
    // GET FROM DISTRIBUTED QUEUE. IF EMPTY WAIT FOR A MESSAGE
    ODistributedRequest req = requestQueue.take();

    while (waitForMessageId.get() > -1) {
      if (req != null) {
        if (req.getId() >= waitForMessageId.get()) {
          // ARRIVED, RESET IT
          ODistributedServerLog.debug(this, manager.getLocalNodeName(), req.getSenderNodeName(), DIRECTION.IN,
              "reached waited request %d on request=%s sourceNode=%s", waitForMessageId.get(), req, req.getSenderNodeName());

          waitForMessageId.set(-1);
          return req;
        } else {
          // SKIP IT
          ODistributedServerLog.debug(this, manager.getLocalNodeName(), req.getSenderNodeName(), DIRECTION.IN,
              "discarded request %d because waiting for %d request=%s sourceNode=%s", req.getId(), waitForMessageId, req,
              req.getSenderNodeName());

          // READ THE NEXT ONE
          req = requestQueue.take();
        }
      }
    }

    while (!restoringMessages && !status.get() && req.getTask().isRequireNodeOnline()) {
      // WAIT UNTIL THE NODE IS ONLINE
      synchronized (waitForOnline) {
        ODistributedServerLog.debug(this, manager.getLocalNodeName(), req.getSenderNodeName(), DIRECTION.OUT,
            "node is not online, request=%s sourceNode=%s must wait to be processed", req, req.getSenderNodeName());

        waitForOnline.wait(5000);
      }
    }

    if (ODistributedServerLog.isDebugEnabled())
      ODistributedServerLog.debug(this, manager.getLocalNodeName(), req.getSenderNodeName(), DIRECTION.OUT,
          "processing request=%s sourceNode=%s", req, req.getSenderNodeName());

    return req;
  }

  /**
   * Execute the remote call on the local node and send back the result
   */
  protected void onMessage(final ODistributedRequest iRequest) {
    OScenarioThreadLocal.INSTANCE.set(OScenarioThreadLocal.RUN_MODE.RUNNING_DISTRIBUTED);

    try {
      final OAbstractRemoteTask task = iRequest.getTask();

      if (ODistributedServerLog.isDebugEnabled())
        ODistributedServerLog.debug(this, manager.getLocalNodeName(), iRequest.getSenderNodeName(), DIRECTION.OUT,
            "received request: %s", iRequest);

      // EXECUTE IT LOCALLY
      final Serializable responsePayload;
      try {
        if (task.isRequiredOpenDatabase())
          initDatabaseInstance();

        ODatabaseRecordThreadLocal.INSTANCE.set(database);

        task.setNodeSource(iRequest.getSenderNodeName());

        responsePayload = manager.executeOnLocalNode(iRequest, database);

      } finally {
        if (database != null)
          database.getLevel1Cache().clear();
      }

      if (ODistributedServerLog.isDebugEnabled())
        ODistributedServerLog.debug(this, manager.getLocalNodeName(), iRequest.getSenderNodeName(), DIRECTION.OUT,
            "sending back response '%s' to request: %s", responsePayload, task);

      final OHazelcastDistributedResponse response = new OHazelcastDistributedResponse(iRequest.getId(),
          manager.getLocalNodeName(), iRequest.getSenderNodeName(), responsePayload);

      try {
        // GET THE SENDER'S RESPONSE QUEUE
        final IQueue<ODistributedResponse> queue = msgService.getQueue(OHazelcastDistributedMessageService
            .getResponseQueueName(iRequest.getSenderNodeName()));

        if (!queue.offer(response, OGlobalConfiguration.DISTRIBUTED_QUEUE_TIMEOUT.getValueAsLong(), TimeUnit.MILLISECONDS))
          throw new ODistributedException("Timeout on dispatching response to the thread queue " + iRequest.getSenderNodeName());

      } catch (Exception e) {
        throw new ODistributedException("Cannot dispatch response to the thread queue " + iRequest.getSenderNodeName(), e);
      }

    } finally {
      OScenarioThreadLocal.INSTANCE.set(OScenarioThreadLocal.RUN_MODE.DEFAULT);
    }
  }

  protected IQueue<ODistributedRequest>[] getRequestQueues(final String iDatabaseName, final Collection<String> nodes) {
    final IQueue<ODistributedRequest>[] queues = new IQueue[nodes.size()];

    int i = 0;
    for (String node : nodes) {
      // GET ALL THE EXISTENT QUEUES
      final IQueue<ODistributedRequest> queue = msgService.getQueue(OHazelcastDistributedMessageService.getRequestQueueName(node,
          iDatabaseName));

      queues[i++] = queue;
    }

    return queues;
  }

  /**
   * Composes the undo queue name based on node name.
   */
  protected String getUndoMapName(final String iDatabaseName) {
    final StringBuilder buffer = new StringBuilder();
    buffer.append(NODE_QUEUE_PREFIX);
    buffer.append(manager.getLocalNodeName());
    if (iDatabaseName != null) {
      buffer.append('.');
      buffer.append(iDatabaseName);
    }
    buffer.append(NODE_QUEUE_UNDO_POSTFIX);
    return buffer.toString();
  }

  protected String getLocalNodeName() {
    return manager.getLocalNodeName();
  }

  protected IMap<Object, Object> restoreMessagesBeforeFailure(final boolean iRestoreMessages) {
    final IMap<Object, Object> undoMap = manager.getHazelcastInstance().getMap(getUndoMapName(databaseName));
    final ODistributedRequest undoRequest = (ODistributedRequest) undoMap.remove(databaseName);
    if (undoRequest != null && iRestoreMessages) {
      ODistributedServerLog.warn(this, getLocalNodeName(), null, DIRECTION.NONE,
          "restore last replication message before the crash for database %s: %s", databaseName, undoRequest);

      try {
        initDatabaseInstance();
        onMessage(undoRequest);
      } catch (Throwable t) {
        ODistributedServerLog.error(this, getLocalNodeName(), null, DIRECTION.NONE,
            "error on executing restored message for database %s", t, databaseName);
      }

    }
    return undoMap;
  }

  protected void checkLocalNodeInConfiguration() {
    final ODistributedConfiguration cfg = manager.getDatabaseConfiguration(databaseName);

    final List<String> foundPartition = cfg.addNewNodeInServerList(manager.getLocalNodeName());
    if (foundPartition != null) {
      // SET THE NODE.DB AS OFFLINE
      manager.setDatabaseStatus(databaseName, ODistributedServerManager.DB_STATUS.OFFLINE);

      ODistributedServerLog.info(this, manager.getLocalNodeName(), null, DIRECTION.NONE, "adding node '%s' in partition: db=%s %s",
          manager.getLocalNodeName(), databaseName, foundPartition);

      manager.updateCachedDatabaseConfiguration(databaseName, cfg.serialize(), true, true);
    }
  }

  protected void removeNodeInConfiguration(final String iNode, final boolean iForce) {
    try {
      // GET DATABASE CFG
      final ODistributedConfiguration cfg = manager.getDatabaseConfiguration(databaseName);

      final List<String> foundPartition = cfg.removeNodeInServerList(iNode, iForce);
      if (foundPartition != null) {
        ODistributedServerLog.info(this, manager.getLocalNodeName(), null, ODistributedServerLog.DIRECTION.NONE,
            "removing node '%s' in partition: db=%s %s", iNode, databaseName, foundPartition);

        msgService.removeQueue(OHazelcastDistributedMessageService.getRequestQueueName(iNode, databaseName));
        manager.updateCachedDatabaseConfiguration(databaseName, cfg.serialize(), true, true);
      }
    } catch (Exception e) {
      ODistributedServerLog.debug(this, manager.getLocalNodeName(), null, ODistributedServerLog.DIRECTION.NONE,
          "unable to remove node '%s' in distributed configuration, db=%s", e, iNode, databaseName);
    }
  }
}
