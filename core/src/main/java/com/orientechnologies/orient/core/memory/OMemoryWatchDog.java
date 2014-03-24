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
package com.orientechnologies.orient.core.memory;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.SoftReference;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TimerTask;
import java.util.WeakHashMap;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.profiler.OAbstractProfiler.OProfilerHookValue;
import com.orientechnologies.common.profiler.OProfilerMBean.METRIC_TYPE;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;

/**
 * This memory warning system will call the listener when we exceed the percentage of available memory specified. There should only
 * be one instance of this object created, since the usage threshold can only be set to one number.
 */
public class OMemoryWatchDog extends Thread {
  private final Map<ListenerWrapper, Object> listeners    = new WeakHashMap<ListenerWrapper, Object>(128);
  private static long                        lastGC       = 0;
  private int                                alertTimes   = 0;
  protected final ReferenceQueue<Object>     monitorQueue = new ReferenceQueue<Object>();
  protected SoftReference<Object>            monitorRef   = new SoftReference<Object>(new Object(), monitorQueue);

  protected final MemoryMXBean               memBean;

  private long                               autoFreeCheckEveryMs;
  private long                               autoFreeHeapThreshold;

  /**
   * we want properties of both IdentityHashMap and WeakHashMap
   */
  public static class ListenerWrapper implements Listener {
    protected final Listener listener;

    private ListenerWrapper(Listener listener) {
      this.listener = listener;
    }

    public Listener getListener() {
      return listener;
    }

    @Override
    public void lowMemory(long iFreeMemory, long iFreeMemoryPercentage) {
      listener.lowMemory(iFreeMemory, iFreeMemoryPercentage);
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o)
        return true;
      if (o == null || getClass() != o.getClass())
        return false;
      final ListenerWrapper that = (ListenerWrapper) o;
      return listener == that.listener;
    }

    @Override
    public int hashCode() {
      return listener != null ? System.identityHashCode(listener) : 0;
    }
  }

  public static interface Listener {
    /**
     * Called when free memory is getting lower.
     * 
     * @param iFreeMemory
     *          Current used memory
     * @param iFreeMemoryPercentage
     *          Max memory
     */
    public void lowMemory(long iFreeMemory, long iFreeMemoryPercentage);
  }

  /**
   * Create the memory watch dog with the default memory threshold.
   * 
   * @param iThreshold
   */
  public OMemoryWatchDog() {
    super("OrientDB MemoryWatchDog");

    memBean = ManagementFactory.getMemoryMXBean();

    final String size = OGlobalConfiguration.MEMORY_AUTOFREE_HEAP_THRESHOLD.getValueAsString();
    autoFreeHeapThreshold = OFileUtils.getSizeAsNumber(size);

    setDaemon(true);
    start();
  }

  @Override
  public void run() {
    Orient
    .instance()
    .getProfiler()
    .registerHookValue("system.memory.alerts", "Number of alerts received by JVM to free memory resources",
        METRIC_TYPE.COUNTER, new OProfilerHookValue() {
      public Object getValue() {
        return alertTimes;
      }
    });
    Orient
    .instance()
    .getProfiler()
    .registerHookValue("system.memory.lastGC", "Date of last System.gc() invocation", METRIC_TYPE.STAT,
        new OProfilerHookValue() {
      public Object getValue() {
        return lastGC;
      }
    });

    autoFreeCheckEveryMs = OGlobalConfiguration.MEMORY_AUTOFREE_CHECK_EVERY.getValueAsLong();
    Orient.instance().getTimer().schedule(new TimerTask() {

      @Override
      public void run() {
        // CHECK MEMORY
        MemoryUsage heapMemory = memBean.getHeapMemoryUsage();
        final long usedHeap = heapMemory.getUsed();
        final long maxHeap = heapMemory.getMax();
        final int usedMemoryPer = (int) (usedHeap * 100 / maxHeap);

        if (OLogManager.instance().isDebugEnabled())
          OLogManager.instance().debug(this, "Checking if memory is lower than configured (%s): used %s of %s (%d%%)",
              OFileUtils.getSizeAsString(autoFreeHeapThreshold), OFileUtils.getSizeAsString(usedHeap),
              OFileUtils.getSizeAsString(maxHeap), usedMemoryPer);

        if (!isMemoryAvailable())
          // CALL LISTENER TO FREE MEMORY
          synchronized (listeners) {
            for (ListenerWrapper listener : listeners.keySet()) {
              try {
                listener.listener.lowMemory(maxHeap - usedHeap, 100 - usedMemoryPer);
              } catch (Exception e) {
                e.printStackTrace();
              }
            }
          }
      }
    }, autoFreeCheckEveryMs, autoFreeCheckEveryMs);

    while (true) {
      try {
        // WAITS FOR THE GC FREE
        monitorQueue.remove();

        if (Thread.interrupted())
          break;

        // GC is freeing memory!
        alertTimes++;
        MemoryUsage heapMemory = memBean.getHeapMemoryUsage();
        final long usedHeap = heapMemory.getUsed();
        final long maxHeap = heapMemory.getMax();
        final int usedMemoryPer = (int) (usedHeap * 100 / maxHeap);

        if (OLogManager.instance().isDebugEnabled())
          OLogManager.instance().debug(this, "Free memory is low %s of %s (%d%%), calling listeners to free memory...",
              OFileUtils.getSizeAsString(maxHeap - usedHeap), OFileUtils.getSizeAsString(maxHeap), 100 - usedMemoryPer);

        final long timer = Orient.instance().getProfiler().startChrono();

        synchronized (listeners) {
          for (ListenerWrapper listener : listeners.keySet()) {
            try {
              listener.listener.lowMemory(maxHeap - usedHeap, 100 - usedMemoryPer);
            } catch (Exception e) {
              e.printStackTrace();
            }
          }
        }

        Orient.instance().getProfiler().stopChrono("OMemoryWatchDog.freeResources", "WatchDog free resources", timer);

      } catch (InterruptedException e) {
        break;
      } catch (Exception e) {
      } finally {
        // RE-INSTANTIATE THE MONITOR REF
        monitorRef = new SoftReference<Object>(new Object(), monitorQueue);
      }
    }

    OLogManager.instance().debug(this, "[OMemoryWatchDog] shutdowning...");

    synchronized (listeners) {
      listeners.clear();
    }
    monitorRef = null;
  }

  public boolean isMemoryAvailable() {
    if (autoFreeHeapThreshold > 0)
      return getUsedHeapMemory() < autoFreeHeapThreshold;
    return getUsedHeapMemoryInPercentage() < autoFreeHeapThreshold * -1;
  }

  /**
   * Important : callers <b>must</b> reference (with a strong ref) the returned Listener, otherwise this listener will be discarded.
   * 
   * @return The Listener to reference with a strong ref..
   */
  public Listener addListener(final Listener listener) {
    ListenerWrapper wrapper = new ListenerWrapper(listener);
    synchronized (listeners) {
      listeners.put(wrapper, listener);
    }
    return wrapper;
  }

  public boolean removeListener(final Listener listener) {
    synchronized (listeners) {
      return listeners.remove(new ListenerWrapper(listener)) != null;
    }
  }

  public List<Listener> getListeners() {
    synchronized (listeners) {
      List<Listener> listenerList = new ArrayList<Listener>();
      for (ListenerWrapper wrapper : listeners.keySet()) {
        listenerList.add(wrapper.listener);
      }
      return listenerList;
    }
  }

  public static void freeMemoryForOptimization(final long iDelayTime) {
    freeMemory(iDelayTime, OGlobalConfiguration.JVM_GC_DELAY_FOR_OPTIMIZE.getValueAsLong());
  }

  public static void freeMemoryForResourceCleanup(final long iDelayTime) {
    freeMemory(iDelayTime, 0);
  }

  public long getUsedHeapMemory() {
    MemoryUsage heapMemory = memBean.getHeapMemoryUsage();
    return heapMemory.getUsed();
  }

  public int getUsedHeapMemoryInPercentage() {
    MemoryUsage heapMemory = memBean.getHeapMemoryUsage();
    return (int) (heapMemory.getUsed() * 100 / heapMemory.getMax());
  }

  private static void freeMemory(final long iDelayTime, final long minimalTimeAmount) {
    final long dateLastGC = System.currentTimeMillis();
    if (dateLastGC - lastGC > minimalTimeAmount * 1000) {
      lastGC = dateLastGC;
      System.gc();

      if (iDelayTime > 0)
        try {
          Thread.sleep(iDelayTime);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
    }
  }

}
