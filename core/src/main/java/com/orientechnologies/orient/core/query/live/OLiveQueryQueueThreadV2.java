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

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
public class OLiveQueryQueueThreadV2 extends Thread {

  private final BlockingQueue<OLiveQueryHookV2.OLiveQueryOp>            queue;
  private final ConcurrentMap<Integer, OLiveQueryListenerV2> subscribers;
  private boolean                                          stopped = false;

  private OLiveQueryQueueThreadV2(BlockingQueue<OLiveQueryHookV2.OLiveQueryOp> queue, ConcurrentMap<Integer, OLiveQueryListenerV2> subscribers) {
    this.queue = queue;
    this.subscribers = subscribers;
  }

  public OLiveQueryQueueThreadV2() {
    this(new LinkedBlockingQueue<OLiveQueryHookV2.OLiveQueryOp>(), new ConcurrentHashMap<Integer, OLiveQueryListenerV2>());
    setName("LiveQueryQueueThreadV2");
    this.setDaemon(true);
  }

  public OLiveQueryQueueThreadV2 clone() {
    return new OLiveQueryQueueThreadV2(this.queue, this.subscribers);
  }

  @Override
  public void run() {
    while (!stopped) {
      OLiveQueryHookV2.OLiveQueryOp next = null;
      try {
        next = queue.take();
      } catch (InterruptedException e) {
        break;
      }
      if (next == null) {
        continue;
      }
      for (OLiveQueryListenerV2 listener : subscribers.values()) {
        try {
          listener.onLiveResult(next);
        } catch (Exception e) {
          OLogManager.instance().warn(this, "Error executing live query subscriber.", e);
        }

      }
    }
  }

  public void stopExecution() {
    this.stopped = true;
    this.interrupt();
  }

  public void enqueue(OLiveQueryHookV2.OLiveQueryOp item) {
    queue.offer(item);
  }

  public Integer subscribe(Integer id, OLiveQueryListenerV2 iListener) {
    subscribers.put(id, iListener);
    return id;
  }

  public void unsubscribe(Integer id) {
    OLiveQueryListenerV2 res = subscribers.remove(id);
    if (res != null) {
      res.onLiveResultEnd();
    }
  }

  public boolean hasListeners() {
    return !subscribers.isEmpty();
  }

  public boolean hasToken(Integer key) {
    return subscribers.containsKey(key);
  }
}
