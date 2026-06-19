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

import com.orientechnologies.common.profiler.OProfilerEntry;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.distributed.db.OrientDBDistributed;
import com.orientechnologies.orient.server.distributed.ODistributedMessageService;
import com.orientechnologies.orient.server.distributed.ODistributedResponse;
import com.orientechnologies.orient.server.distributed.ODistributedResponseManager;
import com.orientechnologies.orient.server.distributed.OLoggerDistributed;
import com.orientechnologies.orient.server.distributed.ONodeLatencies;
import com.orientechnologies.orient.server.distributed.ONodeMessages;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Message handler for matching distributed responses to requests of distributed peer. Each node
 * creates own instance to talk with each others.
 *
 * @author Luca Garulli (l.garulli--at--orientdb.com)
 */
public class ODistributedMessageServiceImpl implements ODistributedMessageService {
  private static final OLoggerDistributed logger =
      OLoggerDistributed.logger(ODistributedMessageServiceImpl.class);
  private final OrientDBDistributed context;
  private final ConcurrentHashMap<Long, ODistributedResponseManager> responsesByRequestIds;
  private final TimerTask asynchMessageManager;
  private long[] responseTimeMetrics = new long[10];
  private final Map<String, OProfilerEntry> latencies = new HashMap<String, OProfilerEntry>();
  private final Map<String, AtomicLong> messagesStats = new HashMap<String, AtomicLong>();

  public ODistributedMessageServiceImpl(final OrientDBDistributed context) {
    this.context = context;
    this.responsesByRequestIds = new ConcurrentHashMap<Long, ODistributedResponseManager>();

    // RESET ALL THE METRICS
    for (int i = 0; i < responseTimeMetrics.length; ++i) responseTimeMetrics[i] = -1;

    // CREATE TASK THAT CHECK ASYNCHRONOUS MESSAGE RECEIVED
    asynchMessageManager =
        new TimerTask() {
          @Override
          public void run() {
            purgePendingMessages();
          }
        };
  }

  public void shutdown() {
    asynchMessageManager.cancel();

    // CANCEL ALL THE PENDING REQUESTS
    for (ODistributedResponseManager req : responsesByRequestIds.values()) req.cancel();

    responsesByRequestIds.clear();

    latencies.clear();
    messagesStats.clear();
  }

  public void registerRequest(final long id, final ODistributedResponseManager currentResponseMgr) {
    responsesByRequestIds.put(id, currentResponseMgr);
  }

  public void handleUnreachableNode(final String nodeName) {
    // WAKE UP ALL THE WAITING RESPONSES
    for (ODistributedResponseManager r : responsesByRequestIds.values())
      r.removeServerBecauseUnreachable(nodeName);
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

  /** Not synchronized, it's called when a message arrives */
  public void dispatchResponseToThread(final ODistributedResponse response) {
    try {
      final long msgId = response.getRequestId().getMessageId();

      // GET ASYNCHRONOUS MSG MANAGER IF ANY
      final ODistributedResponseManager asynchMgr = responsesByRequestIds.get(msgId);
      if (asynchMgr != null && asynchMgr.collectResponse(response)) {
        // ALL RESPONSE RECEIVED, REMOVE THE RESPONSE MANAGER WITHOUT WAITING THE PURGE THREAD
        // REMOVE THEM FOR TIMEOUT
        responsesByRequestIds.remove(msgId);
      }
    } finally {
      Orient.instance()
          .getProfiler()
          .updateCounter(
              "distributed.node.msgReceived",
              "Number of replication messages received in current node",
              +1,
              "distributed.node.msgReceived");

      Orient.instance()
          .getProfiler()
          .updateCounter(
              "distributed.node." + response.getExecutorNodeName() + ".msgReceived",
              "Number of replication messages received in current node from a node",
              +1,
              "distributed.node.*.msgReceived");
    }
  }

  /** Removes a response context because in timeout. */
  public void timeoutRequest(final long msgId) {
    final ODistributedResponseManager asynchMgr = responsesByRequestIds.remove(msgId);
    if (asynchMgr != null) asynchMgr.timeout();
  }

  @Override
  public List<ONodeLatencies> getNodesLatencies() {
    List<ONodeLatencies> nodes = new ArrayList<>();
    synchronized (latencies) {
      for (Entry<String, OProfilerEntry> entry : latencies.entrySet())
        nodes.add(new ONodeLatencies(new ONodeId(entry.getKey()), entry.getValue().toSnapshot()));
    }
    return nodes;
  }

  @Override
  public List<ONodeMessages> getNodesMessages() {
    List<ONodeMessages> messages = new ArrayList<>();
    synchronized (messagesStats) {
      for (Map.Entry<String, AtomicLong> entry : messagesStats.entrySet())
        messages.add(new ONodeMessages(new ONodeId(entry.getKey()), entry.getValue().longValue()));
    }
    return messages;
  }

  @Override
  public ODocument getLatencies() {
    final ODocument doc = new ODocument();

    synchronized (latencies) {
      for (Entry<String, OProfilerEntry> entry : latencies.entrySet())
        doc.field(entry.getKey(), entry.getValue().toDocument(), OType.EMBEDDED);
    }

    return doc;
  }

  @Override
  public long getCurrentLatency(final String server) {
    synchronized (latencies) {
      final OProfilerEntry l = latencies.get(server);
      if (l != null) return (long) (l.getAverage() / 1000000);
    }
    // NOT FOUND
    return 0;
  }

  @Override
  public void updateLatency(final String server, final long sentOn) {
    // MANAGE THIS ASYNCHRONOUSLY
    synchronized (latencies) {
      OProfilerEntry latency = latencies.computeIfAbsent(server, OProfilerEntry::new);
      latency.resettableUpdate(System.nanoTime() - sentOn, 30000);
    }
  }

  protected void purgePendingMessages() {
    final long now = System.nanoTime();

    final long timeout = OGlobalConfiguration.DISTRIBUTED_ASYNCH_RESPONSES_TIMEOUT.getValueAsLong();

    for (Iterator<Entry<Long, ODistributedResponseManager>> it =
            responsesByRequestIds.entrySet().iterator();
        it.hasNext(); ) {
      final Entry<Long, ODistributedResponseManager> item = it.next();

      final ODistributedResponseManager resp = item.getValue();

      final long timeElapsed = (now - resp.getSentOn()) / 1000000;

      if (timeElapsed > timeout) {
        // EXPIRED REQUEST, FREE IT!
        final List<String> missingNodes = resp.getMissingNodes();

        logger.warnIn(
            context.getNodeName(),
            missingNodes.toString(),
            "%d missed response(s) for message %d by nodes %s after %dms when timeout is %dms",
            missingNodes.size(),
            resp.getMessageId(),
            missingNodes,
            timeElapsed,
            timeout);

        Orient.instance()
            .getProfiler()
            .updateCounter(
                "distributed.db." + resp.getDatabaseName() + ".timeouts",
                "Number of messages in timeouts",
                +1,
                "distributed.db.*.timeouts");

        Orient.instance()
            .getProfiler()
            .updateCounter(
                "distributed.node.timeouts",
                "Number of messages in timeouts",
                +1,
                "distributed.node.timeouts");

        resp.timeout();
        it.remove();
      } else if (resp.isFinished()) {
        it.remove();
      }
    }
  }

  @Override
  public ODocument getMessageStats() {
    final ODocument doc = new ODocument();

    synchronized (messagesStats) {
      for (Map.Entry<String, AtomicLong> entry : messagesStats.entrySet())
        doc.field(entry.getKey(), entry.getValue().longValue());
    }

    return doc;
  }

  @Override
  public void updateMessageStats(final String message) {
    // MANAGE THIS ASYNCHRONOUSLY
    synchronized (messagesStats) {
      AtomicLong counter = messagesStats.get(message);
      if (counter == null) {
        counter = new AtomicLong();
        messagesStats.put(message, counter);
      }
      counter.incrementAndGet();
    }
  }

  @Override
  public long getReceivedRequests() {
    long total = 0;
    Collection<ODistributedDatabaseImpl> dbs = context.getDistributedDatabases();
    for (ODistributedDatabaseImpl db : dbs) {
      total += db.getReceivedRequests();
    }

    return total;
  }

  @Override
  public long getProcessedRequests() {
    long total = 0;
    Collection<ODistributedDatabaseImpl> dbs = context.getDistributedDatabases();
    for (ODistributedDatabaseImpl db : dbs) {
      total += db.getProcessedRequests();
    }

    return total;
  }
}
