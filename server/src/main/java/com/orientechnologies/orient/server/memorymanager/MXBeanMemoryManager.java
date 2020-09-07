package com.orientechnologies.orient.server.memorymanager;

import com.orientechnologies.common.log.OLogManager;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryNotificationInfo;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.LockSupport;
import javax.management.ListenerNotFoundException;
import javax.management.Notification;
import javax.management.NotificationEmitter;
import javax.management.NotificationListener;
import javax.management.openmbean.CompositeData;

public final class MXBeanMemoryManager implements NotificationListener, MemoryManager {
  private final int memoryThreshold;
  private final int sleepInterval;

  private final ConcurrentLinkedQueue<String> poolsWithOverhead = new ConcurrentLinkedQueue<>();
  private volatile HashMap<String, MemoryPoolMXBean> memoryBeans;

  public MXBeanMemoryManager(final int memoryThreshold, final int sleepInterval) {
    this.memoryThreshold = memoryThreshold;
    this.sleepInterval = sleepInterval;

    assert sleepInterval >= 0;
    assert memoryThreshold >= 0 && memoryThreshold < 100;
  }

  @Override
  public void start() {
    if (memoryThreshold <= 0) {
      return;
    }

    final HashMap<String, MemoryPoolMXBean> beansByName = new HashMap<>();

    final List<MemoryPoolMXBean> mxBeans = ManagementFactory.getMemoryPoolMXBeans();
    for (final MemoryPoolMXBean mxBean : mxBeans) {
      if (mxBean.isUsageThresholdSupported() && mxBean.getType() == MemoryType.HEAP) {
        final long maxMemory = mxBean.getUsage().getMax();
        if (maxMemory > 0) {
          final long threshold = maxMemory * memoryThreshold / 100;
          mxBean.setUsageThreshold(threshold);

          final NotificationEmitter emitter = (NotificationEmitter) mxBean;
          emitter.addNotificationListener(this, null, null);

          beansByName.put(mxBean.getName(), mxBean);

          OLogManager.instance()
              .infoNoDb(
                  this,
                  "Memory usage threshold for memory pool '%s' is set to %d bytes",
                  mxBean.getName(),
                  threshold);
        }
      }
    }

    this.memoryBeans = beansByName;
  }

  @Override
  public void shutdown() {
    for (final MemoryPoolMXBean mxBean : memoryBeans.values()) {
      final NotificationEmitter emitter = (NotificationEmitter) mxBean;
      try {
        emitter.removeNotificationListener(this);
      } catch (final ListenerNotFoundException e) {
        throw new IllegalStateException(
            "Memory bean "
                + mxBean.getName()
                + " was processed by memory manager but manager was not added as a listener",
            e);
      }
    }
  }

  @Override
  public void checkAndWaitMemoryThreshold() {
    if (poolsWithOverhead.isEmpty()) {
      return;
    }

    while (!poolsWithOverhead.isEmpty()) {
      final String poolName = poolsWithOverhead.peek();
      final MemoryPoolMXBean bean = memoryBeans.get(poolName);

      while (bean.isUsageThresholdExceeded()) {
        LockSupport.parkNanos(sleepInterval * 1_000_000L);
      }

      poolsWithOverhead.poll();
    }
  }

  @Override
  public void handleNotification(final Notification notification, final Object handback) {
    final String notificationType = notification.getType();
    if (notificationType.equals(MemoryNotificationInfo.MEMORY_THRESHOLD_EXCEEDED)) {
      final CompositeData cd = (CompositeData) notification.getUserData();
      final MemoryNotificationInfo info = MemoryNotificationInfo.from(cd);
      poolsWithOverhead.add(info.getPoolName());
    }
  }
}
