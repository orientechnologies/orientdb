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

import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TimerTask;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

import com.hazelcast.core.IMap;
import com.hazelcast.core.IQueue;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.OScenarioThreadLocal;
import com.orientechnologies.orient.core.db.OScenarioThreadLocal.RUN_MODE;
import com.orientechnologies.orient.server.distributed.ODistributedConfiguration;
import com.orientechnologies.orient.server.distributed.ODistributedException;
import com.orientechnologies.orient.server.distributed.ODistributedMessageService;
import com.orientechnologies.orient.server.distributed.ODistributedPartition;
import com.orientechnologies.orient.server.distributed.ODistributedPartitioningStrategy;
import com.orientechnologies.orient.server.distributed.ODistributedRequest;
import com.orientechnologies.orient.server.distributed.ODistributedRequest.EXECUTION_MODE;
import com.orientechnologies.orient.server.distributed.ODistributedResponse;
import com.orientechnologies.orient.server.distributed.ODistributedResponseManager;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog.DIRECTION;

/**
 * Hazelcast implementation of distributed peer.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OHazelcastDistributedMessageService implements ODistributedMessageService {

  protected final OHazelcastPlugin                                                  manager;

  protected final Map<String, IQueue<?>>                                            queues                      = new HashMap<String, IQueue<?>>();
  protected final Map<String, Lock>                                                 requestDatabaseLock         = new HashMap<String, Lock>(
                                                                                                                    5);

  protected final IQueue<ODistributedResponse>                                      nodeResponseQueue;
  protected final ConcurrentHashMap<Long, ArrayBlockingQueue<ODistributedResponse>> internalThreadQueues;
  protected final ConcurrentHashMap<Long, ODistributedResponseManager>              responsesByRequestIds;
  protected final TimerTask                                                         asynchMessageManager;

  private static final String                                                       NODE_QUEUE_PREFIX           = "orientdb.node.";
  private static final String                                                       NODE_QUEUE_REQUEST_POSTFIX  = ".request";
  private static final String                                                       NODE_QUEUE_RESPONSE_POSTFIX = ".response";
  private static final String                                                       NODE_QUEUE_UNDO_POSTFIX     = ".undo";

  private static final String                                                       NODE_LOCK_PREFIX            = "orientdb.reqlock.";

  public OHazelcastDistributedMessageService(final OHazelcastPlugin manager) {
    this.manager = manager;
    this.internalThreadQueues = new ConcurrentHashMap<Long, ArrayBlockingQueue<ODistributedResponse>>();
    this.responsesByRequestIds = new ConcurrentHashMap<Long, ODistributedResponseManager>();

    // CREATE TASK THAT CHECK ASYNCHRONOUS MESSAGE RECEIVED
    asynchMessageManager = new TimerTask() {
      @Override
      public void run() {
        purgePendingMessages();
      }
    };
    Orient
        .instance()
        .getTimer()
        .schedule(asynchMessageManager, OGlobalConfiguration.DISTRIBUTED_PURGE_RESPONSES_TIMER_DELAY.getValueAsLong(),
            OGlobalConfiguration.DISTRIBUTED_PURGE_RESPONSES_TIMER_DELAY.getValueAsLong());

    // CREAT THE QUEUE
    final String queueName = getResponseQueueName(manager.getLocalNodeName());
    nodeResponseQueue = getQueue(queueName);

    if (ODistributedServerLog.isDebugEnabled())
      ODistributedServerLog.debug(this, getLocalNodeNameAndThread(), null, DIRECTION.NONE,
          "listening for incoming responses on queue: %s", queueName);

    checkForPendingMessages(nodeResponseQueue, queueName);

    // CREATE THREAD LISTENER AGAINST orientdb.node.<node>.response, ONE PER NODE, THEN DISPATCH THE MESSAGE INTERNALLY USING THE
    // THREAD ID
    new Thread(new Runnable() {
      @Override
      public void run() {
        while (!Thread.interrupted()) {
          String senderNode = null;
          ODistributedResponse message = null;
          try {
            message = nodeResponseQueue.take();

            if (message != null) {
              senderNode = message.getSenderNodeName();
              dispatchResponseToThread(message);
            }

          } catch (InterruptedException e) {
            // EXIT CURRENT THREAD
            Thread.interrupted();
            break;
          } catch (Throwable e) {
            ODistributedServerLog.error(this, manager.getLocalNodeName(), senderNode, DIRECTION.IN,
                "error on reading distributed response", e, message != null ? message.getPayload() : "-");
          }
        }
      }
    }).start();
  }

  @Override
  public ODistributedRequest createRequest() {
    return new OHazelcastDistributedRequest();
  }

  @Override
  public ODistributedResponse send(final ODistributedRequest iRequest) {
    final Thread currentThread = Thread.currentThread();
    final long threadId = currentThread.getId();

    final String databaseName = iRequest.getDatabaseName();
    final String clusterName = iRequest.getClusterName();

    final ODistributedConfiguration cfg = manager.getDatabaseConfiguration(databaseName);

    final ODistributedPartitioningStrategy strategy = manager.getPartitioningStrategy(cfg.getPartitionStrategy(clusterName));
    final ODistributedPartition partition = strategy.getPartition(manager, databaseName, clusterName);
    final Set<String> nodes = partition.getNodes();

    final IQueue<ODistributedRequest>[] reqQueues = getRequestQueues(databaseName, nodes);

    final int queueSize = nodes.size();
    final int quorum = iRequest.getPayload().isWriteOperation() ? cfg.getWriteQuorum(clusterName) : queueSize;

    iRequest.setSenderNodeName(manager.getLocalNodeName());
    iRequest.setSenderThreadId(threadId);

    // CREATE THE RESPONSE MANAGER
    final ODistributedResponseManager currentResponseMgr = new ODistributedResponseManager(iRequest.getId(), nodes, iRequest
        .getPayload().getTotalTimeout(queueSize));
    responsesByRequestIds.put(iRequest.getId(), currentResponseMgr);

    if (ODistributedServerLog.isDebugEnabled())
      ODistributedServerLog.debug(this, getLocalNodeNameAndThread(), nodes.toString(), DIRECTION.OUT, "request %s",
          iRequest.getPayload());

    final long timeout = OGlobalConfiguration.DISTRIBUTED_QUEUE_TIMEOUT.getValueAsLong();

    final Lock requestLock = getLock(databaseName);
    try {
      requestLock.lock();
      try {
        // LOCK = ASSURE MESSAGES IN THE QUEUE ARE INSERTED SEQUENTIALLY AT CLUSTER LEVEL
        // BROADCAST THE REQUEST TO ALL THE NODE QUEUES
        for (IQueue<ODistributedRequest> queue : reqQueues) {
          queue.offer(iRequest, timeout, TimeUnit.MILLISECONDS);
        }

      } finally {
        requestLock.unlock();
      }

      Orient.instance().getProfiler()
          .updateCounter("distributed.replication.msgSent", "Number of replication messages sent from current node", +1);

      return collectResponses(iRequest, quorum, nodes, currentResponseMgr);

    } catch (Throwable e) {
      throw new ODistributedException("Error on sending distributed request against " + databaseName
          + (clusterName != null ? ":" + clusterName : ""), e);
    }

  }

  protected ArrayBlockingQueue<ODistributedResponse> getInternalThreadQueue(final long threadId) {
    ArrayBlockingQueue<ODistributedResponse> responseQueue = internalThreadQueues.get(threadId);
    if (responseQueue == null) {
      // REGISTER THE INTERNAL THREAD'S RESPONSE QUEUE
      responseQueue = new ArrayBlockingQueue<ODistributedResponse>(
          OGlobalConfiguration.DISTRIBUTED_THREAD_QUEUE_SIZE.getValueAsInteger(), true);
      internalThreadQueues.put(threadId, responseQueue);
    }
    return responseQueue;
  }

  protected ODistributedResponse collectResponses(final ODistributedRequest iRequest, final int iQuorum, final Set<String> nodes,
      final ODistributedResponseManager currentResponseMgr) throws InterruptedException {
    if (iRequest.getExecutionMode() == EXECUTION_MODE.NO_RESPONSE)
      return null;

    final long beginTime = System.currentTimeMillis();

    int availableNodes = 0;
    for (String node : nodes) {
      if (manager.isNodeAvailable(node))
        availableNodes++;
      else {
        if (ODistributedServerLog.isDebugEnabled())
          ODistributedServerLog.debug(this, getLocalNodeNameAndThread(), node, DIRECTION.OUT,
              "skip listening of response because node '%s' is not online", node);
      }
    }

    final int expectedSynchronousResponses = Math.min(availableNodes, iQuorum);

    final long synchTimeout = iRequest.getPayload().getSynchronousTimeout(expectedSynchronousResponses);

    // WAIT FOR CURRENT NODE? IF IT'S CONFIGURED WAIT FOR IT
    boolean executeOnLocalNode = nodes.contains(manager.getLocalNodeName());
    boolean receivedCurrentNode = false;

    final ArrayBlockingQueue<ODistributedResponse> responseQueue = getInternalThreadQueue(iRequest.getSenderThreadId());

    // WAIT FOR THE MINIMUM SYNCHRONOUS RESPONSES (WRITE QUORUM)
    int receivedSynchResponses = 0;
    ODistributedResponse firstResponse = null;

    while ((executeOnLocalNode && !receivedCurrentNode) || currentResponseMgr.getReceivedResponses() < expectedSynchronousResponses) {
      long elapsed = System.currentTimeMillis() - beginTime;

      final ODistributedResponse currentResponse = responseQueue.poll(synchTimeout - elapsed, TimeUnit.MILLISECONDS);

      if (currentResponse != null) {
        // RESPONSE RECEIVED
        if (currentResponse.getRequestId() == iRequest.getId()) {
          // CURRENT REQUEST: COLLECT IT
          if (executeOnLocalNode && currentResponse.isExecutedOnLocalNode())
            receivedCurrentNode = true;
        } else {
          // IT REFERS TO ANOTHER REQUEST, DISCARD IT
          continue;
        }

        // PROCESS IT AS SYNCHRONOUS
        if (ODistributedServerLog.isDebugEnabled())
          ODistributedServerLog.debug(this, getLocalNodeNameAndThread(), currentResponse.getSenderNodeName(), DIRECTION.IN,
              "received response: %s", currentResponse);

        if (firstResponse == null)
          firstResponse = currentResponse;
        receivedSynchResponses++;

      } else {
        elapsed = System.currentTimeMillis() - beginTime;
        ODistributedServerLog.warn(this, getLocalNodeNameAndThread(), null, DIRECTION.IN,
            "timeout (%dms) on waiting for synchronous responses from nodes=%s responsesSoFar=%s request=%s", elapsed, nodes,
            currentResponseMgr.getRespondingNodes(), iRequest);
        break;
      }
    }

    if (!receivedCurrentNode)
      ODistributedServerLog.warn(this, getLocalNodeNameAndThread(), manager.getLocalNodeName(), DIRECTION.IN,
          "no response received from local node about message %d", iRequest.getId());

    if (receivedSynchResponses < iQuorum) {
      // UNDO REQUEST
      // TODO: UNDO
      iRequest.undo();
    }

    if (firstResponse == null)
      throw new ODistributedException("No response from connected nodes");

    return getResponse(iRequest, firstResponse, currentResponseMgr);
  }

  public void configureDatabase(final String iDatabaseName) {
    // CREATE A QUEUE PER DATABASE
    final String queueName = getRequestQueueName(manager.getLocalNodeName(), iDatabaseName);
    final IQueue<ODistributedRequest> requestQueue = getQueue(queueName);

    if (ODistributedServerLog.isDebugEnabled())
      ODistributedServerLog.debug(this, getLocalNodeNameAndThread(), null, DIRECTION.NONE,
          "listening for incoming requests on queue: %s", queueName);

    // UNDO PREVIOUS MESSAGE IF ANY
    final IMap<Object, Object> undoMap = restoreMessagesBeforeFailure(iDatabaseName);

    checkForPendingMessages(requestQueue, queueName);

    // CREATE THREAD LISTENER AGAINST orientdb.node.<node>.<db>.request, ONE PER NODE, THEN DISPATCH THE MESSAGE INTERNALLY USING
    // THE THREAD ID
    new Thread(new Runnable() {
      @Override
      public void run() {
        while (!Thread.interrupted()) {
          String senderNode = null;
          ODistributedRequest message = null;
          try {
            // TODO: ASSURE WE DON'T LOOSE THE MSG AT THIS POINT. HAZELCAST TX? OR PEEK? ARE NOT BLOCKING :-(
            message = requestQueue.take();

            // SAVE THE MESSAGE IN THE UNDO MAP IN CASE OF FAILURE
            undoMap.put(iDatabaseName, message);

            if (message != null) {
              senderNode = message.getSenderNodeName();
              onMessage(message);
            }

            // OK: REMOVE THE UNDO BUFFER
            undoMap.remove(iDatabaseName);

          } catch (InterruptedException e) {
            // EXIT CURRENT THREAD
            Thread.interrupted();
            break;

          } catch (Throwable e) {
            ODistributedServerLog.error(this, getLocalNodeNameAndThread(), senderNode, DIRECTION.IN,
                "error on reading distributed request: %s", e, message != null ? message.getPayload() : "-");
          }
        }
      }
    }).start();
  }

  /**
   * Execute the remote call on the local node and send back the result
   */
  protected void onMessage(final ODistributedRequest iRequest) {
    OScenarioThreadLocal.INSTANCE.set(RUN_MODE.RUNNING_DISTRIBUTED);

    try {
      if (ODistributedServerLog.isDebugEnabled())
        ODistributedServerLog.debug(this, manager.getLocalNodeName(),
            iRequest.getSenderNodeName() + ":" + iRequest.getSenderThreadId(), DIRECTION.IN, "request %s", iRequest.getPayload());

      // EXECUTE IT LOCALLY
      final Serializable responsePayload = manager.executeOnLocalNode(iRequest);

      if (ODistributedServerLog.isDebugEnabled())
        ODistributedServerLog.debug(this, manager.getLocalNodeName(),
            iRequest.getSenderNodeName() + ":" + iRequest.getSenderThreadId(), DIRECTION.OUT,
            "sending back response %s to request %s", responsePayload, iRequest.getPayload());

      final OHazelcastDistributedResponse response = new OHazelcastDistributedResponse(iRequest.getId(),
          manager.getLocalNodeName(), iRequest.getSenderNodeName(), iRequest.getSenderThreadId(), responsePayload);

      try {
        // GET THE SENDER'S RESPONSE QUEUE
        final IQueue<ODistributedResponse> queue = getQueue(getResponseQueueName(iRequest.getSenderNodeName()));

        if (!queue.offer(response, OGlobalConfiguration.DISTRIBUTED_QUEUE_TIMEOUT.getValueAsLong(), TimeUnit.MILLISECONDS))
          throw new ODistributedException("Timeout on dispatching response to the thread queue " + iRequest.getSenderNodeName()
              + ":" + iRequest.getSenderThreadId());

      } catch (Exception e) {
        throw new ODistributedException("Cannot dispatch response to the thread queue " + iRequest.getSenderNodeName() + ":"
            + iRequest.getSenderThreadId(), e);
      }

    } finally {
      OScenarioThreadLocal.INSTANCE.set(RUN_MODE.DEFAULT);
    }
  }

  protected void dispatchResponseToThread(final ODistributedResponse response) {
    Orient.instance().getProfiler()
        .updateCounter("distributed.replication.msgReceived", "Number of replication messages received in current node", +1);

    final long threadId = response.getSenderThreadId();

    final String targetLocalNodeThread = manager.getLocalNodeName() + ":" + threadId;

    if (ODistributedServerLog.isDebugEnabled())
      ODistributedServerLog.debug(this, manager.getLocalNodeName(), targetLocalNodeThread, DIRECTION.OUT,
          "- forward response to the internal thread");

    final ArrayBlockingQueue<ODistributedResponse> responseQueue = internalThreadQueues.get(threadId);

    if (responseQueue == null) {
      if (ODistributedServerLog.isDebugEnabled())
        ODistributedServerLog.debug(this, manager.getLocalNodeName(), targetLocalNodeThread, DIRECTION.OUT,
            "cannot dispatch response to the internal thread because the thread queue %d was not found: %s", threadId,
            response.getPayload());
      return;
    }

    processAsynchResponse(response);

    try {
      if (!responseQueue.offer(response, OGlobalConfiguration.DISTRIBUTED_QUEUE_TIMEOUT.getValueAsLong(), TimeUnit.MILLISECONDS))
        ODistributedServerLog.debug(this, manager.getLocalNodeName(), targetLocalNodeThread, DIRECTION.OUT,
            "timeout on dispatching response of message %d to the internal thread queue", response.getRequestId());
    } catch (Exception e) {
      ODistributedServerLog.error(this, manager.getLocalNodeName(), targetLocalNodeThread, DIRECTION.OUT,
          "error on dispatching response to the internal thread queue", e);
    }
  }

  @SuppressWarnings("unchecked")
  protected IQueue<ODistributedRequest>[] getRequestQueues(final String iDatabaseName, final Set<String> nodes) {
    final IQueue<ODistributedRequest>[] queues = new IQueue[nodes.size()];

    int i = 0;
    for (String node : nodes)
      queues[i++] = getQueue(getRequestQueueName(node, iDatabaseName));

    return queues;
  }

  public void shutdown() {
    internalThreadQueues.clear();

    asynchMessageManager.cancel();
    responsesByRequestIds.clear();

    if (nodeResponseQueue != null) {
      nodeResponseQueue.clear();
      nodeResponseQueue.destroy();
    }
  }

  /**
   * Composes the request queue name based on node name and database.
   */
  protected static String getRequestQueueName(final String iNodeName, final String iDatabaseName) {
    final StringBuilder buffer = new StringBuilder();
    buffer.append(NODE_QUEUE_PREFIX);
    buffer.append(iNodeName);
    if (iDatabaseName != null) {
      buffer.append('.');
      buffer.append(iDatabaseName);
    }
    buffer.append(NODE_QUEUE_REQUEST_POSTFIX);
    return buffer.toString();
  }

  /**
   * Composes the response queue name based on node name.
   */
  protected static String getResponseQueueName(final String iNodeName) {
    final StringBuilder buffer = new StringBuilder();
    buffer.append(NODE_QUEUE_PREFIX);
    buffer.append(iNodeName);
    buffer.append(NODE_QUEUE_RESPONSE_POSTFIX);
    return buffer.toString();
  }

  /**
   * Composes the undo queue name based on node name.
   */
  protected String getUndoMapName() {
    final StringBuilder buffer = new StringBuilder();
    buffer.append(NODE_QUEUE_PREFIX);
    buffer.append(manager.getLocalNodeName());
    buffer.append(NODE_QUEUE_UNDO_POSTFIX);
    return buffer.toString();
  }

  protected String getLocalNodeNameAndThread() {
    return manager.getLocalNodeName() + ":" + Thread.currentThread().getId();
  }

  protected void purgePendingMessages() {
    final long now = System.currentTimeMillis();

    for (Iterator<Entry<Long, ODistributedResponseManager>> it = responsesByRequestIds.entrySet().iterator(); it.hasNext();) {
      final Entry<Long, ODistributedResponseManager> item = it.next();

      final ODistributedResponseManager resp = item.getValue();

      final long timeElapsed = now - resp.getSentOn();

      if (timeElapsed > resp.getTotalTimeout()) {
        // EXPIRED, FREE IT!
        final List<String> missingNodes = resp.getMissingNodes();
        if (missingNodes.size() > 0) {
          ODistributedServerLog.warn(this, manager.getLocalNodeName(), null, DIRECTION.IN,
              "%d missed response(s) for message %d by nodes %s after %dms when timeout is %dms", missingNodes.size(),
              resp.getMessageId(), missingNodes, timeElapsed, resp.getTotalTimeout());
        }

        Orient.instance().getProfiler()
            .updateCounter("distributed.replication.timeouts", "Number of timeouts on replication messages responses", +1);

        it.remove();
      }
    }
  }

  protected IMap<Object, Object> restoreMessagesBeforeFailure(final String iDatabaseName) {
    final IMap<Object, Object> undoMap = manager.getHazelcastInstance().getMap(getUndoMapName());
    final ODistributedRequest undoRequest = (ODistributedRequest) undoMap.remove(iDatabaseName);
    if (undoRequest != null) {
      ODistributedServerLog.warn(this, getLocalNodeNameAndThread(), null, DIRECTION.NONE,
          "restore last replication message before the crash for database %s: %s", iDatabaseName, undoRequest);

      try {
        onMessage(undoRequest);
      } catch (Throwable t) {
        ODistributedServerLog.error(this, getLocalNodeNameAndThread(), null, DIRECTION.NONE,
            "error on executing restored message for database %s", t, iDatabaseName);
      }

    }
    return undoMap;
  }

  protected void checkForPendingMessages(final IQueue<?> iQueue, final String iQueueName) {
    final int queueSize = iQueue.size();
    if (queueSize > 0)
      ODistributedServerLog.warn(this, manager.getLocalNodeName(), null, DIRECTION.NONE,
          "found %d previous messages in queue %s, aligning the database...", queueSize, iQueueName);
  }

  protected ODistributedResponse getResponse(final ODistributedRequest iRequest, ODistributedResponse firstResponse,
      final ODistributedResponseManager currentResponseMgr) {
    switch (iRequest.getPayload().getResultStrategy()) {
    case FIRST_RESPONSE:
      return firstResponse;

    case MERGE:
      return currentResponseMgr.merge(new OHazelcastDistributedResponse(firstResponse.getRequestId(), null, firstResponse
          .getSenderNodeName(), firstResponse.getSenderThreadId(), null));
    }

    return firstResponse;
  }

  protected void processAsynchResponse(final ODistributedResponse response) {
    final long reqId = response.getRequestId();

    // GET ASYNCHRONOUS MSG MANAGER IF ANY
    final ODistributedResponseManager asynchMgr = responsesByRequestIds.get(reqId);
    if (asynchMgr == null) {
      if (ODistributedServerLog.isDebugEnabled())
        ODistributedServerLog.debug(this, getLocalNodeNameAndThread(), response.getSenderNodeName(), DIRECTION.OUT,
            "received response for message %d after the timeout", reqId);
    } else {
      if (asynchMgr.addResponse(response)) {
        // ALL RESPONSE RECEIVED, REMOVE THE RESPONSE MANAGER
        responsesByRequestIds.remove(reqId);
      }
    }
  }

  /**
   * Return the request lock per database. If not exists create and register it.
   */
  protected Lock getLock(final String iDatabase) {
    synchronized (requestDatabaseLock) {
      Lock lock = requestDatabaseLock.get(iDatabase);
      if (lock == null) {
        lock = manager.getHazelcastInstance().getLock(NODE_LOCK_PREFIX + iDatabase);
        requestDatabaseLock.put(iDatabase, lock);
      }
      return lock;
    }
  }

  /**
   * Return the queue. If not exists create and register it.
   */
  @SuppressWarnings("unchecked")
  protected <T> IQueue<T> getQueue(final String iQueueName) {
    synchronized (queues) {
      IQueue<T> queue = (IQueue<T>) queues.get(iQueueName);
      if (queue == null) {
        queue = manager.getHazelcastInstance().getQueue(iQueueName);
        queues.put(iQueueName, queue);
      }

      return manager.getHazelcastInstance().getQueue(iQueueName);
    }
  }

}
