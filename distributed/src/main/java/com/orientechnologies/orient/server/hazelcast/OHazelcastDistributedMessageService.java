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

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TimerTask;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import com.hazelcast.core.IQueue;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.server.distributed.ODistributedMessageService;
import com.orientechnologies.orient.server.distributed.ODistributedRequest;
import com.orientechnologies.orient.server.distributed.ODistributedResponse;
import com.orientechnologies.orient.server.distributed.ODistributedResponseManager;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog.DIRECTION;

/**
 * Hazelcast implementation of distributed peer. There is one instance per database. Each node creates own instance to talk with
 * each others.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OHazelcastDistributedMessageService implements ODistributedMessageService {

  protected final OHazelcastPlugin                                                  manager;

  protected Map<String, OHazelcastDistributedDatabase>                              databases                   = new ConcurrentHashMap<String, OHazelcastDistributedDatabase>();

  protected final static Map<String, IQueue<?>>                                     queues                      = new HashMap<String, IQueue<?>>();

  protected final IQueue<ODistributedResponse>                                      nodeResponseQueue;
  protected final ConcurrentHashMap<Long, ArrayBlockingQueue<ODistributedResponse>> internalThreadQueues;
  protected final ConcurrentHashMap<Long, ODistributedResponseManager>              responsesByRequestIds;
  protected final TimerTask                                                         asynchMessageManager;

  public static final String                                                        NODE_QUEUE_PREFIX           = "orientdb.node.";
  public static final String                                                        NODE_QUEUE_REQUEST_POSTFIX  = ".request";
  public static final String                                                        NODE_QUEUE_RESPONSE_POSTFIX = ".response";
  public static final String                                                        NODE_QUEUE_UNDO_POSTFIX     = ".undo";

  public OHazelcastDistributedMessageService(final OHazelcastPlugin manager) {
    this.manager = manager;

    this.responsesByRequestIds = new ConcurrentHashMap<Long, ODistributedResponseManager>();
    this.internalThreadQueues = new ConcurrentHashMap<Long, ArrayBlockingQueue<ODistributedResponse>>();

    // CREAT THE QUEUE
    final String queueName = getResponseQueueName(manager.getLocalNodeName());
    nodeResponseQueue = getQueue(queueName);

    if (ODistributedServerLog.isDebugEnabled())
      ODistributedServerLog.debug(this, getLocalNodeNameAndThread(), null, DIRECTION.NONE,
          "listening for incoming responses on queue: %s", queueName);

    checkForPendingMessages(nodeResponseQueue, queueName);

    // CREATE TASK THAT CHECK ASYNCHRONOUS MESSAGE RECEIVED
    asynchMessageManager = new TimerTask() {
      @Override
      public void run() {
        purgePendingMessages();
      }
    };

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

  public OHazelcastDistributedDatabase getDatabase(final String iDatabaseName) {
    return databases.get(iDatabaseName);
  }

  @Override
  public ODistributedRequest createRequest() {
    return new OHazelcastDistributedRequest();
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

    if (processAsynchResponse(response))
      // DISPATCH THE RESPONSE TO THE INTERNAL THREAD TO BE WORKED SYNCHRONOUSLY
      try {
        if (!responseQueue.offer(response, OGlobalConfiguration.DISTRIBUTED_QUEUE_TIMEOUT.getValueAsLong(), TimeUnit.MILLISECONDS))
          ODistributedServerLog.debug(this, manager.getLocalNodeName(), targetLocalNodeThread, DIRECTION.OUT,
              "timeout on dispatching response of message %d to the internal thread queue", response.getRequestId());
      } catch (Exception e) {
        ODistributedServerLog.error(this, manager.getLocalNodeName(), targetLocalNodeThread, DIRECTION.OUT,
            "error on dispatching response to the internal thread queue", e);
      }
  }

  public void shutdown() {
    for (Entry<String, OHazelcastDistributedDatabase> m : databases.entrySet())
      m.getValue().shutdown();

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

  protected String getLocalNodeNameAndThread() {
    return manager.getLocalNodeName() + ":" + Thread.currentThread().getId();
  }

  protected void purgePendingMessages() {
    final long now = System.currentTimeMillis();

    final long timeout = OGlobalConfiguration.DISTRIBUTED_ASYNCH_RESPONSES_TIMEOUT.getValueAsLong();

    for (Iterator<Entry<Long, ODistributedResponseManager>> it = responsesByRequestIds.entrySet().iterator(); it.hasNext();) {
      final Entry<Long, ODistributedResponseManager> item = it.next();

      final ODistributedResponseManager resp = item.getValue();

      final long timeElapsed = now - resp.getSentOn();

      if (timeElapsed > timeout) {
        // EXPIRED, FREE IT!
        final List<String> missingNodes = resp.getMissingNodes();
        if (missingNodes.size() > 0) {
          ODistributedServerLog.warn(this, manager.getLocalNodeName(), null, DIRECTION.IN,
              "%d missed response(s) for message %d by nodes %s after %dms when timeout is %dms", missingNodes.size(),
              resp.getMessageId(), missingNodes, timeElapsed, timeout);
        }

        Orient.instance().getProfiler()
            .updateCounter("distributed.replication.timeouts", "Number of timeouts on replication messages responses", +1);

        it.remove();
      }
    }
  }

  protected void checkForPendingMessages(final IQueue<?> iQueue, final String iQueueName) {
    final int queueSize = iQueue.size();
    if (queueSize > 0)
      ODistributedServerLog.warn(this, manager.getLocalNodeName(), null, DIRECTION.NONE,
          "found %d previous messages in queue %s, aligning the database...", queueSize, iQueueName);
  }

  /**
   * Processes the response asynchronously.
   * 
   * @return true if the response should be sent to the internal thread queue, otherwise no
   */
  protected boolean processAsynchResponse(final ODistributedResponse response) {
    final long reqId = response.getRequestId();

    // GET ASYNCHRONOUS MSG MANAGER IF ANY
    final ODistributedResponseManager asynchMgr = responsesByRequestIds.get(reqId);
    if (asynchMgr == null) {
      if (ODistributedServerLog.isDebugEnabled())
        ODistributedServerLog.debug(this, getLocalNodeNameAndThread(), response.getSenderNodeName(), DIRECTION.OUT,
            "received response for message %d after the timeout", reqId);

      return false;
    }

    boolean processSynchronously = asynchMgr.waitForSynchronousResponses();

    if (asynchMgr.addResponse(response))
      // ALL RESPONSE RECEIVED, REMOVE THE RESPONSE MANAGER
      responsesByRequestIds.remove(reqId);

    return processSynchronously;
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

  public void registerRequest(final long id, final ODistributedResponseManager currentResponseMgr) {
    responsesByRequestIds.put(id, currentResponseMgr);
  }

  public OHazelcastDistributedDatabase registerDatabase(final String iDatabaseName) {
    final OHazelcastDistributedDatabase db = new OHazelcastDistributedDatabase(manager, this, iDatabaseName);
    databases.put(iDatabaseName, db);
    return db;
  }

}
