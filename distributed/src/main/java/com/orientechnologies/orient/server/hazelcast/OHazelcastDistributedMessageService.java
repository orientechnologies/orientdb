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

import com.hazelcast.core.IQueue;
import com.hazelcast.monitor.LocalQueueStats;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.distributed.ODistributedMessageService;
import com.orientechnologies.orient.server.distributed.ODistributedRequest;
import com.orientechnologies.orient.server.distributed.ODistributedResponse;
import com.orientechnologies.orient.server.distributed.ODistributedResponseManager;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog.DIRECTION;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Hazelcast implementation of distributed peer. There is one instance per database. Each node creates own instance to talk with
 * each others.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OHazelcastDistributedMessageService implements ODistributedMessageService {

  protected final OHazelcastPlugin                                     manager;

  protected Map<String, OHazelcastDistributedDatabase>                 databases                   = new ConcurrentHashMap<String, OHazelcastDistributedDatabase>();

  protected final static Map<String, IQueue<?>>                        queues                      = new HashMap<String, IQueue<?>>();

  protected final IQueue<ODistributedResponse>                         nodeResponseQueue;
  protected final ConcurrentHashMap<Long, ODistributedResponseManager> responsesByRequestIds;
  protected final TimerTask                                            asynchMessageManager;

  public static final String                                           NODE_QUEUE_PREFIX           = "orientdb.node.";
  public static final String                                           NODE_QUEUE_REQUEST_POSTFIX  = ".request";
  public static final String                                           NODE_QUEUE_RESPONSE_POSTFIX = ".response";
  public static final String                                           NODE_QUEUE_UNDO_POSTFIX     = ".undo";

  public OHazelcastDistributedMessageService(final OHazelcastPlugin manager) {
    this.manager = manager;

    this.responsesByRequestIds = new ConcurrentHashMap<Long, ODistributedResponseManager>();

    // CREAT THE QUEUE
    final String queueName = getResponseQueueName(manager.getLocalNodeName());
    nodeResponseQueue = getQueue(queueName);

    if (ODistributedServerLog.isDebugEnabled())
      ODistributedServerLog.debug(this, getLocalNodeNameAndThread(), null, DIRECTION.NONE,
          "listening for incoming responses on queue: %s", queueName);

    checkForPendingMessages(nodeResponseQueue, queueName, false);

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

  protected void dispatchResponseToThread(final ODistributedResponse response) {
    try {
      final long reqId = response.getRequestId();

      // GET ASYNCHRONOUS MSG MANAGER IF ANY
      final ODistributedResponseManager asynchMgr = responsesByRequestIds.get(reqId);
      if (asynchMgr == null) {
        if (ODistributedServerLog.isDebugEnabled())
          ODistributedServerLog.debug(this, manager.getLocalNodeName(), response.getExecutorNodeName(), DIRECTION.IN,
              "received response for message %d after the timeout (%dms)", reqId,
              OGlobalConfiguration.DISTRIBUTED_ASYNCH_RESPONSES_TIMEOUT.getValueAsLong());
      } else if (asynchMgr.addResponse(response))
        // ALL RESPONSE RECEIVED, REMOVE THE RESPONSE MANAGER
        responsesByRequestIds.remove(reqId);

    } finally {
      Orient.instance().getProfiler()
          .updateCounter("distributed.replication.msgReceived", "Number of replication messages received in current node", +1);

      Orient
          .instance()
          .getProfiler()
          .updateCounter("distributed.replication." + response.getExecutorNodeName() + ".msgReceived",
              "Number of replication messages received in current node from a node", +1, "distributed.replication.*.msgReceived");
    }
  }

  public void shutdown() {
    for (Entry<String, OHazelcastDistributedDatabase> m : databases.entrySet())
      m.getValue().shutdown();

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

        ODistributedServerLog.warn(this, manager.getLocalNodeName(), missingNodes.toString(), DIRECTION.IN,
            "%d missed response(s) for message %d by nodes %s after %dms when timeout is %dms", missingNodes.size(),
            resp.getMessageId(), missingNodes, timeElapsed, timeout);

        Orient
            .instance()
            .getProfiler()
            .updateCounter("distributed.replication." + resp.getDatabaseName() + ".timeouts",
                "Number of timeouts on replication messages responses", +1, "distributed.replication.*.timeouts");

        resp.timeout();
        it.remove();
      }
    }
  }

  protected boolean checkForPendingMessages(final IQueue<?> iQueue, final String iQueueName, final boolean iUnqueuePendingMessages) {
    final int queueSize = iQueue.size();
    if (queueSize > 0) {
      if (!iUnqueuePendingMessages) {
        ODistributedServerLog.warn(this, manager.getLocalNodeName(), null, DIRECTION.NONE,
            "found %d previous messages in queue %s, clearing them...", queueSize, iQueueName);
        iQueue.clear();
      } else {
        ODistributedServerLog.warn(this, manager.getLocalNodeName(), null, DIRECTION.NONE,
            "found %d previous messages in queue %s, aligning the database...", queueSize, iQueueName);
        return true;
      }
    }
    return false;
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

  /**
   * Remove the queue.
   */
  protected void removeQueue(final String iQueueName) {
    synchronized (queues) {
      queues.remove(iQueueName);
      IQueue<?> queue = manager.getHazelcastInstance().getQueue(iQueueName);
      queue.clear();
    }
  }

  public void registerRequest(final long id, final ODistributedResponseManager currentResponseMgr) {
    responsesByRequestIds.put(id, currentResponseMgr);
  }

  @Override
  public List<String> getManagedQueueNames() {
    List<String> queueNames = new ArrayList<String>();
    for (String q : manager.getHazelcastInstance().getConfig().getQueueConfigs().keySet()) {
      if (q.startsWith(NODE_QUEUE_PREFIX))
        queueNames.add(q);
    }
    return queueNames;
  }

  @Override
  public ODocument getQueueStats(final String iQueueName) {
    final IQueue<Object> queue = manager.getHazelcastInstance().getQueue(iQueueName);
    if (queue == null)
      throw new IllegalArgumentException("Queue '" + iQueueName + "' not found");

    final ODocument doc = new ODocument();

    doc.field("name", queue.getName());
    doc.field("partitionKey", queue.getPartitionKey());
    doc.field("serviceName", queue.getServiceName());

    doc.field("size", queue.size());
    doc.field("nextElement", queue.peek());

    final LocalQueueStats stats = queue.getLocalQueueStats();
    doc.field("minAge", stats.getMinAge());
    doc.field("maxAge", stats.getMaxAge());
    doc.field("avgAge", stats.getAvgAge());

    doc.field("backupItemCount", stats.getBackupItemCount());
    doc.field("emptyPollOperationCount", stats.getEmptyPollOperationCount());
    doc.field("offerOperationCount", stats.getOfferOperationCount());
    doc.field("eventOperationCount", stats.getEventOperationCount());
    doc.field("otherOperationsCount", stats.getOtherOperationsCount());
    doc.field("pollOperationCount", stats.getPollOperationCount());
    doc.field("emptyPollOperationCount", stats.getEmptyPollOperationCount());
    doc.field("ownedItemCount", stats.getOwnedItemCount());
    doc.field("rejectedOfferOperationCount", stats.getRejectedOfferOperationCount());

    return doc;
  }

  public OHazelcastDistributedDatabase registerDatabase(final String iDatabaseName) {
    final OHazelcastDistributedDatabase db = new OHazelcastDistributedDatabase(manager, this, iDatabaseName);
    databases.put(iDatabaseName, db);
    return db;
  }

  public Set<String> getDatabases() {
    return databases.keySet();
  }
}
