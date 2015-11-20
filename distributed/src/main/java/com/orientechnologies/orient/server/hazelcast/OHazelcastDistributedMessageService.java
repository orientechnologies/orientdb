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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

import com.hazelcast.collection.impl.queue.QueueService;
import com.hazelcast.config.QueueConfig;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.IQueue;
import com.hazelcast.monitor.LocalQueueStats;
import com.hazelcast.spi.exception.DistributedObjectDestroyedException;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.distributed.ODistributedMessageService;
import com.orientechnologies.orient.server.distributed.ODistributedResponse;
import com.orientechnologies.orient.server.distributed.ODistributedResponseManager;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog.DIRECTION;
import com.orientechnologies.orient.server.distributed.task.OAbstractRemoteTask;

/**
 * Hazelcast implementation of distributed peer. There is one instance per database. Each node creates own instance to talk with
 * each others.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OHazelcastDistributedMessageService implements ODistributedMessageService {

  public static final int                                              STATS_MAX_MESSAGES          = 20;
  public static final String                                           NODE_QUEUE_PREFIX           = "orientdb.node.";
  public static final String                                           NODE_QUEUE_REQUEST_POSTFIX  = ".request";
  public static final String                                           NODE_QUEUE_RESPONSE_POSTFIX = ".response";
  protected final OHazelcastPlugin                                     manager;
  protected final IQueue                                               nodeResponseQueue;
  protected final ConcurrentHashMap<Long, ODistributedResponseManager> responsesByRequestIds;
  protected final TimerTask                                            asynchMessageManager;
  protected Map<String, OHazelcastDistributedDatabase>                 databases                   = new ConcurrentHashMap<String, OHazelcastDistributedDatabase>();
  protected Thread                                                     responseThread;
  protected long[]                                                     responseTimeMetrics         = new long[10];
  protected int                                                        responseTimeMetricIndex     = 0;
  protected volatile boolean                                           running                     = true;

  public OHazelcastDistributedMessageService(final OHazelcastPlugin manager) {
    this.manager = manager;
    this.responsesByRequestIds = new ConcurrentHashMap<Long, ODistributedResponseManager>();

    // RESET ALL THE METRICS
    for (int i = 0; i < responseTimeMetrics.length; ++i)
      responseTimeMetrics[i] = -1;

    // CREAT THE QUEUE
    final String queueName = getResponseQueueName(manager.getLocalNodeName());
    nodeResponseQueue = getQueue(queueName);

    if (ODistributedServerLog.isDebugEnabled())
      ODistributedServerLog.debug(this, getLocalNodeNameAndThread(), null, DIRECTION.NONE,
          "listening for incoming responses on queue: %s", queueName);

    // TODO: CHECK IF SET TO TRUE (UNQUEUE MSG) WHEN HOT-ALIGNMENT = TRUE
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
    responseThread = new Thread(new Runnable() {
      @Override
      public void run() {
        Thread.currentThread().setName("OrientDB Node Response " + queueName);
        while (running) {
          String senderNode = null;
          ODistributedResponse message = null;
          try {
            message = (ODistributedResponse) nodeResponseQueue.take();

            if (message != null) {
              senderNode = message.getSenderNodeName();

              final long reqId = message.getRequestId();
              if (reqId < 0) {
                // REQUEST
                final OAbstractRemoteTask task = (OAbstractRemoteTask) message.getPayload();
                task.execute(manager.getServerInstance(), manager, null);
              } else {
                // RESPONSE
                final long responseTime = dispatchResponseToThread(message);

                if (responseTime > -1)
                  collectMetric(responseTime);
              }
            }

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
          } catch (HazelcastException e) {
            if (e.getCause() instanceof InterruptedException)
              Thread.interrupted();
            else
              ODistributedServerLog.error(this, manager.getLocalNodeName(), senderNode, DIRECTION.IN,
                  "error on reading distributed response", e, message != null ? message.getPayload() : "-");
          } catch (Throwable e) {
            ODistributedServerLog.error(this, manager.getLocalNodeName(), senderNode, DIRECTION.IN,
                "error on reading distributed response", e, message != null ? message.getPayload() : "-");
          }
        }

        ODistributedServerLog.debug(this, manager.getLocalNodeName(), null, DIRECTION.NONE, "end of reading responses");
      }
    });

    responseThread.setDaemon(true);
    responseThread.start();
  }

  /**
   * Composes the request queue name based on node name and database.
   */
  public static String getRequestQueueName(final String iNodeName, final String iDatabaseName) {
    final StringBuilder buffer = new StringBuilder(128);
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
    final StringBuilder buffer = new StringBuilder(128);
    buffer.append(NODE_QUEUE_PREFIX);
    buffer.append(iNodeName);
    buffer.append(NODE_QUEUE_RESPONSE_POSTFIX);
    return buffer.toString();
  }

  public OHazelcastDistributedDatabase getDatabase(final String iDatabaseName) {
    return databases.get(iDatabaseName);
  }

  public void shutdown() {
    running = false;

    if (responseThread != null) {
      responseThread.interrupt();
      if (!nodeResponseQueue.isEmpty())
        try {
          responseThread.join();
        } catch (InterruptedException e) {
        }
      responseThread = null;
    }

    for (Entry<String, OHazelcastDistributedDatabase> m : databases.entrySet())
      m.getValue().shutdown();

    asynchMessageManager.cancel();
    responsesByRequestIds.clear();

    if (nodeResponseQueue != null) {
      nodeResponseQueue.clear();
      nodeResponseQueue.destroy();
    }
  }

  public void registerRequest(final long id, final ODistributedResponseManager currentResponseMgr) {
    responsesByRequestIds.put(id, currentResponseMgr);
  }

  public void handleUnreachableNode(final String nodeName) {
    final Set<String> dbs = getDatabases();
    if (dbs != null)
      for (String dbName : dbs) {
        getDatabase(dbName).removeNodeInConfiguration(nodeName, false);
      }

    // REMOVE THE SERVER'S RESPONSE QUEUE
    // removeQueue(OHazelcastDistributedMessageService.getResponseQueueName(nodeName));

    for (ODistributedResponseManager r : responsesByRequestIds.values())
      r.notifyWaiters();
  }

  @Override
  public List<String> getManagedQueueNames() {
    final List<String> queueNames = new ArrayList<String>();
    for (DistributedObject d : manager.getHazelcastInstance().getDistributedObjects()) {
      if (d.getServiceName().equals(QueueService.SERVICE_NAME))
        queueNames.add(d.getName());
    }
    return queueNames;
  }

  public IAtomicLong getMessageIdCounter() {
    return manager.getHazelcastInstance().getAtomicLong("orientdb.requestId");
  }

  @Override
  public ODocument getQueueStats(final String iQueueName) {
    final IQueue queue = manager.getHazelcastInstance().getQueue(iQueueName);
    if (queue == null)
      throw new IllegalArgumentException("Queue '" + iQueueName + "' not found");

    final ODocument doc = new ODocument();

    doc.field("name", queue.getName());
    doc.field("partitionKey", queue.getPartitionKey());
    doc.field("serviceName", queue.getServiceName());

    doc.field("size", queue.size());
    // doc.field("nextElement", queue.peek());

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

    List<Object> nextMessages = new ArrayList<Object>(STATS_MAX_MESSAGES);
    for (Iterator<Object> it = queue.iterator(); it.hasNext();) {
      Object next = it.next();
      if (next != null)
        nextMessages.add(next.toString());

      if (nextMessages.size() >= STATS_MAX_MESSAGES)
        break;
    }

    doc.field("nextMessages", nextMessages);

    return doc;
  }

  public long getAverageResponseTime() {
    long total = 0;
    int involved = 0;
    for (long metric : responseTimeMetrics) {
      if (metric > -1) {
        total += metric;
        involved++;
      }
    }
    return total > 0 ? total / involved : 0;
  }

  public OHazelcastDistributedDatabase registerDatabase(final String iDatabaseName) {
    final OHazelcastDistributedDatabase db = new OHazelcastDistributedDatabase(manager, this, iDatabaseName);
    databases.put(iDatabaseName, db);
    return db;
  }

  public OHazelcastDistributedDatabase unregisterDatabase(final String iDatabaseName) {
    final OHazelcastDistributedDatabase db = databases.remove(iDatabaseName);
    if (db != null) {
      db.shutdown();
    }
    return db;
  }

  public Set<String> getDatabases() {
    return databases.keySet();
  }

  /**
   * Not synchronized, it's called when a message arrives
   * 
   * @param response
   */
  protected long dispatchResponseToThread(final ODistributedResponse response) {
    final long chrono = Orient.instance().getProfiler().startChrono();

    try {
      final long reqId = response.getRequestId();

      // GET ASYNCHRONOUS MSG MANAGER IF ANY
      final ODistributedResponseManager asynchMgr = responsesByRequestIds.get(reqId);
      if (asynchMgr == null) {
        if (ODistributedServerLog.isDebugEnabled())
          ODistributedServerLog.debug(this, manager.getLocalNodeName(), response.getExecutorNodeName(), DIRECTION.IN,
              "received response for message %d after the timeout (%dms)", reqId,
              OGlobalConfiguration.DISTRIBUTED_ASYNCH_RESPONSES_TIMEOUT.getValueAsLong());
      } else if (asynchMgr.collectResponse(response)) {
        // ALL RESPONSE RECEIVED, REMOVE THE RESPONSE MANAGER WITHOUT WAITING THE PURGE THREAD REMOVE THEM FOR TIMEOUT
        responsesByRequestIds.remove(reqId);

        // RETURN THE ASYNCH RESPONSE TIME
        return System.currentTimeMillis() - asynchMgr.getSentOn();
      }
    } finally {
      Orient.instance().getProfiler().stopChrono("distributed.node." + response.getExecutorNodeName() + ".latency",
          "Latency in ms from current node", chrono);

      Orient.instance().getProfiler().updateCounter("distributed.node.msgReceived",
          "Number of replication messages received in current node", +1, "distributed.node.msgReceived");

      Orient.instance().getProfiler().updateCounter("distributed.node." + response.getExecutorNodeName() + ".msgReceived",
          "Number of replication messages received in current node from a node", +1, "distributed.node.*.msgReceived");
    }

    return -1;
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
        // EXPIRED REQUEST, FREE IT!
        final List<String> missingNodes = resp.getMissingNodes();

        ODistributedServerLog.warn(this, manager.getLocalNodeName(), missingNodes.toString(), DIRECTION.IN,
            "%d missed response(s) for message %d by nodes %s after %dms when timeout is %dms", missingNodes.size(),
            resp.getMessageId(), missingNodes, timeElapsed, timeout);

        Orient.instance().getProfiler().updateCounter("distributed.db." + resp.getDatabaseName() + ".timeouts",
            "Number of messages in timeouts", +1, "distributed.db.*.timeouts");

        Orient.instance().getProfiler().updateCounter("distributed.node.timeouts", "Number of messages in timeouts", +1,
            "distributed.node.timeouts");

        resp.timeout();
        it.remove();
      }
    }
  }

  protected boolean checkForPendingMessages(final IQueue iQueue, final String iQueueName, final boolean iUnqueuePendingMessages) {
    final int queueSize = iQueue.size();
    if (queueSize > 0) {
      if (!iUnqueuePendingMessages) {
        ODistributedServerLog.warn(this, manager.getLocalNodeName(), null, DIRECTION.NONE,
            "found %d messages in queue %s, clearing them...", queueSize, iQueueName);
        iQueue.clear();
      } else {
        ODistributedServerLog.warn(this, manager.getLocalNodeName(), null, DIRECTION.NONE,
            "found %d messages in queue %s, aligning the database...", queueSize, iQueueName);
        return true;
      }
    } else
      ODistributedServerLog.info(this, manager.getLocalNodeName(), null, DIRECTION.NONE, "found no previous messages in queue %s",
          iQueueName);

    return false;
  }

  /**
   * Returns the queue. If not exists create and register it.
   */
  public <T> IQueue<T> getQueue(final String iQueueName) {
    // configureQueue(iQueueName, 0, 0);
    return (IQueue<T>) manager.getHazelcastInstance().getQueue(iQueueName);
  }

  protected void configureQueue(final String iQueueName, final int synchReplica, final int asynchReplica) {
    final QueueConfig queueCfg = manager.getHazelcastInstance().getConfig().getQueueConfig(iQueueName);
    queueCfg.setBackupCount(synchReplica);
    queueCfg.setAsyncBackupCount(asynchReplica);
  }

  /**
   * Removes the queue. Hazelcast doesn't allow to remove the queue, so now we just clear it.
   */
  protected void removeQueue(final String iQueueName) {
    final IQueue queue = manager.getHazelcastInstance().getQueue(iQueueName);
    if (queue != null) {
      ODistributedServerLog.info(this, manager.getLocalNodeName(), null, DIRECTION.NONE,
          "removing queue '%s' containing %d messages", iQueueName, queue.size());
      queue.clear();
    }
  }

  protected void collectMetric(final long iTime) {
    if (responseTimeMetricIndex >= responseTimeMetrics.length)
      responseTimeMetricIndex = 0;
    responseTimeMetrics[responseTimeMetricIndex++] = iTime;
  }
}
