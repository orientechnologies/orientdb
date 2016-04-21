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
package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog.DIRECTION;
import com.orientechnologies.orient.server.hazelcast.OHazelcastPlugin;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Hazelcast implementation of distributed peer. There is one instance per database. Each node creates own instance to talk with
 * each others.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class ODistributedMessageServiceImpl implements ODistributedMessageService {

  protected final OHazelcastPlugin                                     manager;
  protected final ConcurrentHashMap<Long, ODistributedResponseManager> responsesByRequestIds;
  protected final TimerTask                                            asynchMessageManager;
  protected Map<String, ODistributedDatabaseImpl>                      databases               = new ConcurrentHashMap<String, ODistributedDatabaseImpl>();
  protected Thread                                                     responseThread;
  protected long[]                                                     responseTimeMetrics     = new long[10];
  protected int                                                        responseTimeMetricIndex = 0;
  protected volatile boolean                                           running                 = true;

  public ODistributedMessageServiceImpl(final OHazelcastPlugin manager) {
    this.manager = manager;
    this.responsesByRequestIds = new ConcurrentHashMap<Long, ODistributedResponseManager>();

    // RESET ALL THE METRICS
    for (int i = 0; i < responseTimeMetrics.length; ++i)
      responseTimeMetrics[i] = -1;

    // CREATE TASK THAT CHECK ASYNCHRONOUS MESSAGE RECEIVED
    asynchMessageManager = new TimerTask() {
      @Override
      public void run() {
        purgePendingMessages();
      }
    };
  }

  public ODistributedDatabaseImpl getDatabase(final String iDatabaseName) {
    return databases.get(iDatabaseName);
  }

  public void shutdown() {
    running = false;

    if (responseThread != null) {
      responseThread.interrupt();
      responseThread = null;
    }

    // SHUTDOWN ALL DATABASES
    for (Entry<String, ODistributedDatabaseImpl> m : databases.entrySet())
      m.getValue().shutdown();
    databases.clear();

    asynchMessageManager.cancel();
    responsesByRequestIds.clear();
  }

  public void registerRequest(final long id, final ODistributedResponseManager currentResponseMgr) {
    responsesByRequestIds.put(id, currentResponseMgr);
  }

  public void handleUnreachableNode(final String nodeName) {
    // WAKE UP ALL THE WAITING RESPONSES
    for (ODistributedResponseManager r : responsesByRequestIds.values())
      r.notifyWaiters();
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

  public ODistributedDatabaseImpl registerDatabase(final String iDatabaseName) {
    final ODistributedDatabaseImpl db = new ODistributedDatabaseImpl(manager, this, iDatabaseName);
    databases.put(iDatabaseName, db);
    return db;
  }

  public ODistributedDatabaseImpl unregisterDatabase(final String iDatabaseName) {
    final ODistributedDatabaseImpl db = databases.remove(iDatabaseName);
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
  public long dispatchResponseToThread(final ODistributedResponse response) {
    final long chrono = Orient.instance().getProfiler().startChrono();

    try {
      final long msgId = response.getRequestId().getMessageId();

      // GET ASYNCHRONOUS MSG MANAGER IF ANY
      final ODistributedResponseManager asynchMgr = responsesByRequestIds.get(msgId);
      if (asynchMgr == null) {
        if (ODistributedServerLog.isDebugEnabled())
          ODistributedServerLog.debug(this, manager.getLocalNodeName(), response.getExecutorNodeName(), DIRECTION.IN,
              "received response for message %d after the timeout (%dms)", msgId,
              OGlobalConfiguration.DISTRIBUTED_ASYNCH_RESPONSES_TIMEOUT.getValueAsLong());
      } else if (asynchMgr.collectResponse(response)) {
        // ALL RESPONSE RECEIVED, REMOVE THE RESPONSE MANAGER WITHOUT WAITING THE PURGE THREAD REMOVE THEM FOR TIMEOUT
        responsesByRequestIds.remove(msgId);

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
}
