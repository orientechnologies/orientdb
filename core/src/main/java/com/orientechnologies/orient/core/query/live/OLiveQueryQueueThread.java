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
package com.orientechnologies.orient.core.query.live;

import com.orientechnologies.orient.core.db.record.ORecordOperation;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author Luigi Dell'Aquila
 */
public class OLiveQueryQueueThread extends Thread {

  private final BlockingQueue<ORecordOperation>  queue       = new LinkedBlockingQueue<ORecordOperation>();
  private final Map<Integer, OLiveQueryListener> subscribers = new ConcurrentHashMap<Integer, OLiveQueryListener>();
  private boolean                                stopped     = false;

  public OLiveQueryQueueThread() {
    setName("LiveQueryQueueThread");
    this.setDaemon(true);
  }

  @Override
  public void run() {
    while (!stopped) {
      ORecordOperation next = null;
      try {
        next = queue.take();
      } catch (InterruptedException e) {
        break;
      }
      if (next == null) {
        continue;
      }
      for (OLiveQueryListener listener : subscribers.values()) {
        // TODO filter data
        listener.onLiveResult(next);
      }
    }
  }

  public void stopExecution() {
    this.stopped = true;
    queue.notifyAll();
  }

  public void enqueue(ORecordOperation item) {
    queue.offer(item);
  }

  public Integer subscribe(Integer id, OLiveQueryListener iListener) {
    subscribers.put(id, iListener);
    return id;
  }

  public void unsubscribe(Integer id) {
    subscribers.remove(id);
  }
}
