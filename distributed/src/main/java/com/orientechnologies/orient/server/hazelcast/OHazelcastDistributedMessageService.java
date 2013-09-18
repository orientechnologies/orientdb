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

import com.hazelcast.core.IQueue;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;
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
public class OHazelcastDistributedMessageService implements ODistributedMessageService, MessageListener<ODistributedRequest> {

  protected final OHazelcastPlugin                                                  manager;
  protected final ConcurrentHashMap<Long, ArrayBlockingQueue<ODistributedResponse>> threadQueues;
  protected final IQueue<ODistributedResponse>                                      responseQueue;

  protected static final long                                                       MAX_PUSH_TIMEOUT = 3000;
  protected static final long                                                       MAX_PULL_TIMEOUT = 5000;

  public OHazelcastDistributedMessageService(final OHazelcastPlugin manager) {
    this.manager = manager;
    this.threadQueues = new ConcurrentHashMap<Long, ArrayBlockingQueue<ODistributedResponse>>();

    // CREAT THE QUEUE
    responseQueue = manager.getHazelcastInstance().getQueue("node." + manager.getLocalNodeName());
    responseQueue.clear();

    // DISPATCH THE RESPONSE TO THE RIGHT THREAD QUEUE
    new Thread(new Runnable() {
      @Override
      public void run() {
        while (!Thread.interrupted())
          try {
            notifyResponse(responseQueue.take());
          } catch (Throwable e) {
            ODistributedServerLog.error(this, manager.getLocalNodeName(), null, DIRECTION.IN,
                "error on reading distributed response");
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

    final ITopic<ODistributedRequest> topic = getRequestTopic(databaseName, clusterName);

    final Thread currentThread = Thread.currentThread();

    final ODistributedConfiguration cfg = manager.getDatabaseConfiguration(databaseName);

    final ODistributedPartitioningStrategy strategy = manager.getStrategy(cfg.getPartitionStrategy(clusterName));
    final ODistributedPartition partition = strategy.getPartition(manager, databaseName, clusterName);
    final List<String> nodes = partition.getNodes();

    final int writeQuorum = cfg.getWriteQuorum(clusterName);
    final int queueSize = nodes.size();

    iRequest.setSenderNodeName(manager.getLocalNodeName());
    iRequest.setSenderThreadId(currentThread.getId());

    // REGISTER THE THREAD'S RESPONSE QUEUE
    final ArrayBlockingQueue<ODistributedResponse> responseQueue = new ArrayBlockingQueue<ODistributedResponse>(queueSize, true);
    threadQueues.put(currentThread.getId(), responseQueue);
    try {
      final ODistributedResponse[] responses = new ODistributedResponse[queueSize];

      // ASSURE THE ID IS ALWAYS SEQUENTIAL
      synchronized (topic) {
        iRequest.assignUniqueId(manager.getRunId(), manager.incrementDistributedSerial(databaseName));
        // BROADCAST THE REQUEST
        topic.publish(iRequest);
      }

      if (iRequest.getExecutionMode() == EXECUTION_MODE.NO_RESPONSE)
        return null;

      // WAIT FOR THE MINIMUM SYNCHRONOUS RESPONSES (WRITE QUORUM)
      int receivedResponses = 0;
      ODistributedResponse firstResponse = null;
      final long beginTime = System.currentTimeMillis();
      final int expectedSynchronousResponses = Math.min(queueSize, writeQuorum);

      for (int i = 0; i < expectedSynchronousResponses; ++i) {
        final long elapsed = System.currentTimeMillis() - beginTime;

        responses[i] = responseQueue.poll(MAX_PULL_TIMEOUT - elapsed, TimeUnit.MILLISECONDS);
        if (responses[i] != null) {
          ODistributedServerLog.debug(this, manager.getLocalNodeName() + ":" + currentThread.getId(), null, DIRECTION.IN,
              "received response: %s", responses[i]);

          if (firstResponse == null)
            firstResponse = responses[i];
          receivedResponses++;
        } else
          ODistributedServerLog.warn(this, manager.getLocalNodeName(), null, DIRECTION.IN,
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

    } catch (Exception e) {
      throw new ODistributedException("Error on sending distributed request against " + databaseName
          + (clusterName != null ? ":" + clusterName : ""), e);
    } finally {
      // UNREGISTER THE THREAD'S RESPONSE QUEUE
      threadQueues.remove(currentThread.getId());
    }

  }

  /**
   * Execute the remote call on the local node and send back the result
   */
  @Override
  public void onMessage(final Message<ODistributedRequest> message) {
    final ODistributedRequest req = message.getMessageObject();

    // GET THE SENDER'S RESPONSE QUEUE
    final IQueue<ODistributedResponse> queue = getNodeQueue(req.getSenderNodeName());

    ODistributedThreadLocal.INSTANCE.set("local");
    try {
      ODistributedServerLog.debug(this, manager.getLocalNodeName(), req.getSenderNodeName() + ":" + req.getSenderThreadId(),
          DIRECTION.IN, "request %s", req.getPayload());

      // EXECUTE IT LOCALLY
      final Serializable responsePayload = manager.executeOnLocalNode(req);

      ODistributedServerLog.debug(this, manager.getLocalNodeName(), req.getSenderNodeName() + ":" + req.getSenderThreadId(),
          DIRECTION.OUT, "response to request %s: %s", req.getPayload(), responsePayload);

      final OHazelcastDistributedResponse response = new OHazelcastDistributedResponse(req.getSenderNodeName(),
          req.getSenderThreadId(), responsePayload);

      try {
        if (!queue.offer(response, MAX_PUSH_TIMEOUT, TimeUnit.MILLISECONDS))
          throw new ODistributedException("Timeout on dispatching response to the thread queue " + req.getSenderNodeName() + ":"
              + req.getSenderThreadId());
      } catch (Exception e) {
        throw new ODistributedException("Cannot dispatch response to the thread queue " + req.getSenderNodeName() + ":"
            + req.getSenderThreadId(), e);
      }

    } finally {
      ODistributedThreadLocal.INSTANCE.set(null);
    }
  }

  public void notifyResponse(final ODistributedResponse response) {
    final long threadId = response.getSenderThreadId();

    ODistributedServerLog.debug(this, manager.getLocalNodeName(), null, DIRECTION.IN, "- forward response -> thread %d", threadId);

    final ArrayBlockingQueue<ODistributedResponse> responseQueue = threadQueues.get(threadId);

    if (responseQueue == null) {
      ODistributedServerLog.error(this, manager.getLocalNodeName(), null, DIRECTION.IN,
          "cannot dispatch response to the thread %d because the queue was not found (timeout?)", threadId);
      return;
    }

    try {
      if (!responseQueue.offer(response, MAX_PUSH_TIMEOUT, TimeUnit.MILLISECONDS))
        ODistributedServerLog.error(this, manager.getLocalNodeName(), null, DIRECTION.IN,
            "timeout on dispatching response to the internal thread queue %d", threadId);
    } catch (Exception e) {
      ODistributedServerLog.error(this, manager.getLocalNodeName(), null, DIRECTION.IN,
          "error on dispatching response to the internal thread queue %d", e, threadId);
    }
  }

  public void configureDatabase(final String databaseName) {
    final ODistributedConfiguration cfg = manager.getDatabaseConfiguration(databaseName);

    // CREATE 1 TOPIC PER CONFIGURED CLUSTER
    for (String clusterName : cfg.getClusterNames()) {
      final ITopic<ODistributedRequest> topic = getRequestTopic(databaseName, clusterName);
      topic.addMessageListener(this);
    }

    // CREATE 1 TOPIC PER DATABASE
    final ITopic<ODistributedRequest> topic = getRequestTopic(databaseName, null);
    topic.addMessageListener(this);
  }

  public ITopic<ODistributedRequest> getRequestTopic(final String iDatabaseName, final String iClusterName) {
    if (iClusterName == null)
      // TOPIC = <DATABASE>
      return manager.getHazelcastInstance().getTopic(iDatabaseName);

    // TOPIC = <DATABASE>.<CLUSTER>
    final String cfgClusterName = manager.getDatabaseConfiguration(iDatabaseName).getClusterConfigurationName(iClusterName);
    final String topicName = iDatabaseName + "." + cfgClusterName;
    return manager.getHazelcastInstance().getTopic(topicName);
  }

  public IQueue<ODistributedResponse> getNodeQueue(final String iNodeName) {
    final String queueName = "node." + iNodeName;
    return manager.getHazelcastInstance().getQueue(queueName);
  }

  public void shutdown() {
    threadQueues.clear();

    if (responseQueue != null) {
      responseQueue.clear();
      responseQueue.destroy();
    }
  }
}
