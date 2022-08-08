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
package com.orientechnologies.orient.core.query.live;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;

/** @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com) */
public class OLiveQueryQueueThreadV2 extends Thread {

  private static final OLogManager logger = OLogManager.instance();

  private final OLiveQueryHookV2.OLiveQueryOps ops;
  private volatile boolean stopped = false;

  public OLiveQueryQueueThreadV2(OLiveQueryHookV2.OLiveQueryOps ops) {
    setName("LiveQueryQueueThreadV2");
    this.ops = ops;
    this.setDaemon(true);
  }

  public OLiveQueryQueueThreadV2 clone() {
    return new OLiveQueryQueueThreadV2(this.ops);
  }

  @Override
  public void run() {
    final int batchSize = OGlobalConfiguration.QUERY_REMOTE_RESULTSET_PAGE_SIZE.getValueAsInteger();
    final BlockingQueue<OLiveQueryHookV2.OLiveQueryOp> queue = ops.getQueue();

    long totalEventsServed = 0;
    while (!stopped) {
      final List<OLiveQueryHookV2.OLiveQueryOp> items = new ArrayList<>(batchSize);
      try {
        items.add(queue.take()); // Blocking wait for start of batch
        while (items.size() < batchSize) {
          final OLiveQueryHookV2.OLiveQueryOp next = queue.poll(); // Fill batch until queue empty
          if (next == null) {
            break;
          }
          items.add(next);
        }
      } catch (InterruptedException ignore) {
        Thread.currentThread().interrupt();
        continue;
      }

      for (OLiveQueryListenerV2 listener : ops.getSubscribers().values()) {
        try {
          listener.onLiveResults(items);
        } catch (Exception e) {
          OLogManager.instance().warn(this, "Error executing live query subscriber.", e);
        }

        totalEventsServed++;
        if (totalEventsServed > 0 && totalEventsServed % 100_000 == 0) {
          logger.info(
              this.getClass(),
              "LiveQuery events: %d served, %d in queue",
              totalEventsServed,
              queue.size());
        }
      }
    }
  }

  public void stopExecution() {
    this.stopped = true;
    this.interrupt();
  }
}
