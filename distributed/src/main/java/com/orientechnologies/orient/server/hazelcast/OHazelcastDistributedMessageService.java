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
import java.util.concurrent.Callable;
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
import com.orientechnologies.orient.server.distributed.ODistributedResponse;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog.DIRECTION;

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

  protected static final long                                                       MAX_PUSH_TIMEOUT = 1000;
  protected static final long                                                       MAX_PULL_TIMEOUT = 10000;

  public OHazelcastDistributedMessageService(final OHazelcastPlugin manager) {
    this.manager = manager;
    this.threadQueues = new ConcurrentHashMap<Long, ArrayBlockingQueue<ODistributedResponse>>();

    // CREAT THE QUEUE
    responseQueue = manager.getHazelcastInstance().getQueue("node." + manager.getLocalNodeName());
    responseQueue.clear();

    // DISPATCH THE RESPONSE TO THE RIGHT THREAD QUEUE
    new Runnable() {
      @Override
      public void run() {
        while (Thread.interrupted())
          try {
            notifyResponse(responseQueue.take());
          } catch (Exception e) {
            ODistributedServerLog
                .error(this, manager.getLocalNodeId(), null, DIRECTION.IN, "error on reading distributed response");
          }
      }
    }.run();
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

    final ODistributedConfiguration cfg = manager.getConfiguration(databaseName);

    final ODistributedPartitioningStrategy strategy = manager.getStrategy(cfg.getPartitionStrategy(clusterName));
    final ODistributedPartition partition = strategy.getPartition(databaseName, clusterName);
    final List<String> nodes = partition.getNodes();

    final int writeQuorum = cfg.getWriteQuorum(clusterName);
    final int queueSize = nodes.size();

    iRequest.setSenderThreadId(currentThread.getId());

    // REGISTER THE THREAD'S RESPONSE QUEUE
    final ArrayBlockingQueue<ODistributedResponse> responseQueue = new ArrayBlockingQueue<ODistributedResponse>(queueSize, true);
    threadQueues.put(currentThread.getId(), responseQueue);
    try {
      final ODistributedResponse[] responses = new ODistributedResponse[queueSize];

      // BROADCAST THE REQUEST
      topic.publish(iRequest);

      // WAIT FOR THE MINIMUM SYNCHRONOUS RESPONSES (WRITE QUORUM)
      int receivedResponses = 0;
      ODistributedResponse firstResponse = null;
      final long beginTime = System.currentTimeMillis();
      for (int i = 0; i < writeQuorum; ++i) {
        responses[i] = responseQueue.poll(MAX_PULL_TIMEOUT - (System.currentTimeMillis() - beginTime), TimeUnit.MILLISECONDS);
        if (responses[i] != null) {
          if (firstResponse == null)
            firstResponse = responses[i];
          receivedResponses++;
        }
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
        // ROLLBACK MESSAGES
        // TODO: ROLLBACK
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

  @Override
  public void onMessage(final Message<ODistributedRequest> message) {
    final ODistributedRequest req = message.getMessageObject();

    // GET THE SENDER'S RESPONSE QUEUE
    final IQueue<ODistributedResponse> queue = getNodeQueue(req.getSenderNodeName());

    final Object requestPayload = req.getPayload();

    final Serializable responsePayload;
    if (requestPayload instanceof Callable<?>)
      try {
        responsePayload = (Serializable) ((Callable<?>) requestPayload).call();
      } catch (Exception e1) {
        throw new ODistributedException("Error on executing payload " + requestPayload + " in request: " + req.getSenderNodeName()
            + ":" + req.getSenderThreadId(), e1);
      }
    else
      throw new ODistributedException("Invalid payload in request " + req);

    final OHazelcastDistributedResponse response = new OHazelcastDistributedResponse(req.getSenderNodeName(),
        req.getSenderThreadId(), responsePayload);

    try {
      queue.offer(response, MAX_PUSH_TIMEOUT, TimeUnit.MILLISECONDS);
      throw new ODistributedException("Timeout on dispatching response to the thread queue " + req.getSenderNodeName() + ":"
          + req.getSenderThreadId());
    } catch (Exception e) {
      throw new ODistributedException("Cannot dispatch response to the thread queue " + req.getSenderNodeName() + ":"
          + req.getSenderThreadId(), e);
    }
  }

  public void notifyResponse(final ODistributedResponse response) {
    final long threadId = response.getSenderThreadId();
    final ArrayBlockingQueue<ODistributedResponse> responseQueue = threadQueues.get(threadId);

    if (responseQueue == null)
      throw new ODistributedException("Cannot dispatch response to the thread " + threadId + " because the queue was not found");

    try {
      if (!responseQueue.offer(response, MAX_PUSH_TIMEOUT, TimeUnit.MILLISECONDS))
        throw new ODistributedException("Timeout on dispatching response to the thread " + threadId);
    } catch (Exception e) {
      throw new ODistributedException("Cannot dispatch response to the thread " + threadId, e);
    }
  }

  public void configureDatabase(final String databaseName) {
    final ODistributedConfiguration cfg = manager.getConfiguration(databaseName);
    for (String clusterName : cfg.getClusterNames()) {
      final ITopic<ODistributedRequest> topic = getRequestTopic(databaseName, clusterName);
      topic.addMessageListener(this);
    }
  }

  public ITopic<ODistributedRequest> getRequestTopic(final String iDatabaseName, final String iClusterName) {
    if (iClusterName == null)
      // TOPIC = <DATABASE>
      return manager.getHazelcastInstance().getTopic(iDatabaseName);

    // TOPIC = <DATABASE>.<CLUSTER>
    final String cfgClusterName = manager.getConfiguration(iDatabaseName).getClusterConfigurationName(iClusterName);
    final String topicName = iDatabaseName + "." + cfgClusterName;
    return manager.getHazelcastInstance().getTopic(topicName);
  }

  public IQueue<ODistributedResponse> getNodeQueue(final String iNodeName) {
    final String queueName = "node." + iNodeName;
    return manager.getHazelcastInstance().getQueue(queueName);
  }
}
