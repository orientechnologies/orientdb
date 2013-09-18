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
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.hazelcast.core.IQueue;
import com.orientechnologies.orient.server.distributed.ODistributedConfiguration;
import com.orientechnologies.orient.server.distributed.ODistributedException;
import com.orientechnologies.orient.server.distributed.ODistributedMessageService;
import com.orientechnologies.orient.server.distributed.ODistributedPartition;
import com.orientechnologies.orient.server.distributed.ODistributedPartitioningStrategy;
import com.orientechnologies.orient.server.distributed.ODistributedRequest;
import com.orientechnologies.orient.server.distributed.ODistributedRequest.EXECUTION_MODE;
import com.orientechnologies.orient.server.distributed.ODistributedResponse;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog.DIRECTION;
import com.orientechnologies.orient.server.distributed.ODistributedThreadLocal;

/**
 * Hazelcast implementation of distributed peer.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OHazelcastDistributedMessageService implements ODistributedMessageService {

  protected final OHazelcastPlugin                                                  manager;

  protected final Lock                                                              requestLock                 = new ReentrantLock();

  protected final IQueue<ODistributedResponse>                                      nodeResponseQueue;
  protected final ConcurrentHashMap<Long, ArrayBlockingQueue<ODistributedResponse>> internalThreadQueues;

  protected static final long                                                       MAX_PUSH_TIMEOUT            = 3000;
  protected static final long                                                       MAX_PULL_TIMEOUT            = 5000;
  private static final Object                                                       NODE_QUEUE_PREFIX           = "orientdb.node.";
  private static final String                                                       NODE_QUEUE_REQUEST_POSTFIX  = "request";
  private static final String                                                       NODE_QUEUE_RESPONSE_POSTFIX = "response";

  public OHazelcastDistributedMessageService(final OHazelcastPlugin manager) {
    this.manager = manager;
    this.internalThreadQueues = new ConcurrentHashMap<Long, ArrayBlockingQueue<ODistributedResponse>>();

    // CREAT THE QUEUE
    final String queueName = getResponseQueueName(manager.getLocalNodeName());
    nodeResponseQueue = getNodeQueue(queueName);

    checkForPendingMessages(nodeResponseQueue, queueName);

    // CREATE THREAD LISTENER AGAINST orientdb.node.<node>.response, ONE PER NODE, THEN DISPATCH THE MESSAGE INTERNALLY USING THE
    // THREAD ID
    new Thread(new Runnable() {
      @Override
      public void run() {
        while (!Thread.interrupted()) {
          String senderNode = null;
          try {
            final ODistributedResponse message = nodeResponseQueue.take();

            if (message != null) {
              senderNode = message.getSenderNodeName();
              dispatchResponseToThread(message);
            }

          } catch (Throwable e) {
            ODistributedServerLog.error(this, manager.getLocalNodeName(), senderNode, DIRECTION.IN,
                "error on reading distributed response");
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
    final String databaseName = iRequest.getDatabaseName();
    final String clusterName = iRequest.getClusterName();

    final Thread currentThread = Thread.currentThread();

    final ODistributedConfiguration cfg = manager.getDatabaseConfiguration(databaseName);

    final ODistributedPartitioningStrategy strategy = manager.getStrategy(cfg.getPartitionStrategy(clusterName));
    final ODistributedPartition partition = strategy.getPartition(manager, databaseName, clusterName);
    final List<String> nodes = partition.getNodes();

    final IQueue<ODistributedRequest>[] reqQueues = getRequestQueues(databaseName, nodes);

    final int writeQuorum = cfg.getWriteQuorum(clusterName);
    final int queueSize = nodes.size();

    iRequest.setSenderNodeName(manager.getLocalNodeName());
    iRequest.setSenderThreadId(currentThread.getId());

    // REGISTER THE THREAD'S RESPONSE QUEUE
    final ArrayBlockingQueue<ODistributedResponse> responseQueue = new ArrayBlockingQueue<ODistributedResponse>(queueSize, true);
    internalThreadQueues.put(currentThread.getId(), responseQueue);

    try {
      requestLock.lock();
      try {
        // LOCK = ASSURE THE ID IS ALWAYS SEQUENTIAL

        iRequest.assignUniqueId(manager.getRunId(), manager.incrementDistributedSerial(databaseName));

        ODistributedServerLog.debug(this, getLocalNodeNameAndThread(), nodes.toString(), DIRECTION.OUT, "request %s",
            iRequest.getPayload());

        // BROADCAST THE REQUEST TO ALL THE NODE QUEUES
        for (IQueue<ODistributedRequest> queue : reqQueues) {
          queue.offer(iRequest, MAX_PUSH_TIMEOUT, TimeUnit.MILLISECONDS);
        }

      } finally {
        requestLock.unlock();
      }

      final ODistributedResponse firstResponse = collectResponses(iRequest, writeQuorum, nodes, responseQueue);

      return firstResponse;

    } catch (Exception e) {
      throw new ODistributedException("Error on sending distributed request against " + databaseName
          + (clusterName != null ? ":" + clusterName : ""), e);
    } finally {
      // UNREGISTER THE THREAD'S RESPONSE QUEUE
      internalThreadQueues.remove(currentThread.getId());
    }

  }

  private ODistributedResponse collectResponses(final ODistributedRequest iRequest, final int writeQuorum,
      final List<String> iNodes, final ArrayBlockingQueue<ODistributedResponse> responseQueue) throws InterruptedException {
    if (iRequest.getExecutionMode() == EXECUTION_MODE.NO_RESPONSE)
      return null;

    final int queueSize = iNodes.size();
    final ODistributedResponse[] responses = new ODistributedResponse[queueSize];

    // WAIT FOR THE MINIMUM SYNCHRONOUS RESPONSES (WRITE QUORUM)
    int receivedResponses = 0;
    ODistributedResponse firstResponse = null;

    int availableNodes = 0;
    for (String node : iNodes) {
      if (manager.isNodeAvailable(node))
        availableNodes++;
    }

    final int expectedSynchronousResponses = Math.min(availableNodes, Math.min(queueSize, writeQuorum));
    final long beginTime = System.currentTimeMillis();

    for (int i = 0; i < expectedSynchronousResponses; ++i) {
      final long elapsed = System.currentTimeMillis() - beginTime;

      responses[i] = responseQueue.poll(MAX_PULL_TIMEOUT - elapsed, TimeUnit.MILLISECONDS);
      if (responses[i] != null) {
        if (responses[i].getRequestId() != iRequest.getId()) {
          ODistributedServerLog.error(this, getLocalNodeNameAndThread(), responses[i].getSenderNodeName(), DIRECTION.OUT,
              "ignore message from another request (%d)", responses[i].getRequestId());
          // IGNORE
          --i;
          continue;
        }

        ODistributedServerLog.debug(this, getLocalNodeNameAndThread(), responses[i].getSenderNodeName(), DIRECTION.IN,
            "- received response: %s", responses[i]);

        if (firstResponse == null)
          firstResponse = responses[i];
        receivedResponses++;
      } else
        ODistributedServerLog.warn(this, getLocalNodeNameAndThread(), null, DIRECTION.IN,
            "- timeout (%dms) on response for request: %s", elapsed, iRequest);
    }

    if (queueSize > writeQuorum) {
      // CREATE A FUTURE THAT READ MORE ASYNCHRONOUS RESPONSES
      // TODO
    }

    // CHECK FOR CONFLICTS
    for (int i = 0; i < responses.length; ++i) {
      // TODO: CHECK FOR CONFLICTS
    }

    if (receivedResponses < writeQuorum) {
      // UNDO REQUEST
      // TODO: UNDO
      iRequest.undo();
    }
    return firstResponse;
  }

  public void configureDatabase(final String iDatabaseName) {
    // CREATE A QUEUE PER DATABASE
    final String queueName = getRequestQueueName(manager.getLocalNodeName(), iDatabaseName);
    final IQueue<ODistributedRequest> requestQueue = getNodeQueue(queueName);

    checkForPendingMessages(requestQueue, queueName);

    // CREATE THREAD LISTENER AGAINST orientdb.node.<node>.<db>.request, ONE PER NODE, THEN DISPATCH THE MESSAGE INTERNALLY USING
    // THE THREAD ID
    new Thread(new Runnable() {
      @Override
      public void run() {
        while (!Thread.interrupted()) {
          String senderNode = null;
          try {
            final ODistributedRequest message = requestQueue.take();

            if (message != null) {
              senderNode = message.getSenderNodeName();
              onMessage(message);
            }

          } catch (Throwable e) {
            ODistributedServerLog.error(this, getLocalNodeNameAndThread(), senderNode, DIRECTION.IN,
                "error on reading distributed request");
          }
        }
      }
    }).start();
  }

  private void checkForPendingMessages(final IQueue<?> iQueue, final String iQueueName) {
    final int queueSize = iQueue.size();
    if (queueSize > 0)
      ODistributedServerLog.warn(this, manager.getLocalNodeName(), null, DIRECTION.NONE, "found previous messages in queue %s",
          iQueueName);
  }

  /**
   * Execute the remote call on the local node and send back the result
   */
  protected void onMessage(final ODistributedRequest iRequest) {
    ODistributedThreadLocal.INSTANCE.set("local");
    try {
      ODistributedServerLog.debug(this, manager.getLocalNodeName(),
          iRequest.getSenderNodeName() + ":" + iRequest.getSenderThreadId(), DIRECTION.IN, "request %s", iRequest.getPayload());

      // EXECUTE IT LOCALLY
      final Serializable responsePayload = manager.executeOnLocalNode(iRequest);

      ODistributedServerLog.debug(this, manager.getLocalNodeName(),
          iRequest.getSenderNodeName() + ":" + iRequest.getSenderThreadId(), DIRECTION.OUT,
          "sending back response %s to request %s", responsePayload, iRequest.getPayload());

      final OHazelcastDistributedResponse response = new OHazelcastDistributedResponse(iRequest.getId(),
          iRequest.getSenderNodeName(), iRequest.getSenderThreadId(), responsePayload);

      try {
        // GET THE SENDER'S RESPONSE QUEUE
        final IQueue<ODistributedResponse> queue = getNodeQueue(getResponseQueueName(iRequest.getSenderNodeName()));

        if (!queue.offer(response, MAX_PUSH_TIMEOUT, TimeUnit.MILLISECONDS))
          throw new ODistributedException("Timeout on dispatching response to the thread queue " + iRequest.getSenderNodeName()
              + ":" + iRequest.getSenderThreadId());

      } catch (Exception e) {
        throw new ODistributedException("Cannot dispatch response to the thread queue " + iRequest.getSenderNodeName() + ":"
            + iRequest.getSenderThreadId(), e);
      }

    } finally {
      ODistributedThreadLocal.INSTANCE.set(null);
    }
  }

  protected void dispatchResponseToThread(final ODistributedResponse response) {
    final long threadId = response.getSenderThreadId();

    final String targetLocalNodeThread = manager.getLocalNodeName() + ":" + threadId;

    ODistributedServerLog.debug(this, manager.getLocalNodeName(), targetLocalNodeThread, DIRECTION.OUT,
        "- forward response to the internal thread");

    final ArrayBlockingQueue<ODistributedResponse> responseQueue = internalThreadQueues.get(threadId);

    if (responseQueue == null) {
      ODistributedServerLog.error(this, manager.getLocalNodeName(), targetLocalNodeThread, DIRECTION.OUT,
          "cannot dispatch response to the internal thread because the queue was not found (timeout?)");
      return;
    }

    try {
      if (!responseQueue.offer(response, MAX_PUSH_TIMEOUT, TimeUnit.MILLISECONDS))
        ODistributedServerLog.error(this, manager.getLocalNodeName(), targetLocalNodeThread, DIRECTION.OUT,
            "timeout on dispatching response to the internal thread queue");
    } catch (Exception e) {
      ODistributedServerLog.error(this, manager.getLocalNodeName(), targetLocalNodeThread, DIRECTION.OUT,
          "error on dispatching response to the internal thread queue", e);
    }
  }

  @SuppressWarnings("unchecked")
  protected IQueue<ODistributedRequest>[] getRequestQueues(final String iDatabaseName, final List<String> iNodes) {
    final IQueue<ODistributedRequest>[] queues = new IQueue[iNodes.size()];

    for (int i = 0; i < iNodes.size(); ++i)
      queues[i] = getNodeQueue(getRequestQueueName(iNodes.get(i), iDatabaseName));

    return queues;
  }

  protected <T> IQueue<T> getNodeQueue(final String iQueueName) {
    return manager.getHazelcastInstance().getQueue(iQueueName);
  }

  public void shutdown() {
    internalThreadQueues.clear();

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

  protected String getLocalNodeNameAndThread() {
    return manager.getLocalNodeName() + ":" + Thread.currentThread().getId();
  }

}
