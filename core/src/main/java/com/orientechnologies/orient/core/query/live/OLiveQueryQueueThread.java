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
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;

/** @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com) */
public class OLiveQueryQueueThread extends Thread {

  private final BlockingQueue<ORecordOperation> queue;
  private final ConcurrentMap<Integer, OLiveQueryListener> subscribers;
  private boolean stopped = false;

  private OLiveQueryQueueThread(
      BlockingQueue<ORecordOperation> queue,
      ConcurrentMap<Integer, OLiveQueryListener> subscribers) {
    this.queue = queue;
    this.subscribers = subscribers;
  }

  public OLiveQueryQueueThread() {
    this(
        new LinkedBlockingQueue<ORecordOperation>(),
        new ConcurrentHashMap<Integer, OLiveQueryListener>());
    setName("LiveQueryQueueThread");
    this.setDaemon(true);
  }

  public OLiveQueryQueueThread clone() {
    return new OLiveQueryQueueThread(this.queue, this.subscribers);
  }

  @Override
  public void run() {
    while (!stopped) {
      ORecordOperation next = null;
      try {
        next = queue.take();
      } catch (InterruptedException ignore) {
        break;
      }
      if (next == null) {
        continue;
      }
      for (OLiveQueryListener listener : subscribers.values()) {
        // TODO filter data
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

  public void enqueue(ORecordOperation item) {
    queue.offer(item);
  }

  public Integer subscribe(Integer id, OLiveQueryListener iListener) {
    subscribers.put(id, iListener);
    return id;
  }

  public void unsubscribe(Integer id) {
    OLiveQueryListener res = subscribers.remove(id);
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
