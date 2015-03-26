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

  private BlockingQueue<ORecordOperation> queue       = new LinkedBlockingQueue<ORecordOperation>();

  private Map<Integer, OLiveQueryListener>   subscribers = new ConcurrentHashMap<Integer, OLiveQueryListener>();

  private boolean                         stopped     = false;

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
