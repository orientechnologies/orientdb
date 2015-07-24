package com.orientechnologies.common.concur.lock;

import com.orientechnologies.orient.core.OOrientListenerAbstract;
import com.orientechnologies.orient.core.Orient;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * * @author Andrey Lomakin (a.lomakin-at-orientechnologies.com)
 */
public class ODistributedCounter extends OOrientListenerAbstract {
  private static final int           HASH_INCREMENT = 0x61c88647;

  private static final AtomicInteger nextHashCode   = new AtomicInteger();
  private final AtomicBoolean        poolBusy       = new AtomicBoolean();
  private final int                  maxPartitions  = Runtime.getRuntime().availableProcessors() << 3;
  private final int                  MAX_RETRIES    = 8;

  private final ThreadLocal<Integer> threadHashCode = new ThreadHashCode();
  private volatile AtomicLong[]      counters       = new AtomicLong[2];

  public ODistributedCounter() {
    for (int i = 0; i < counters.length; i++) {
      counters[i] = new AtomicLong();
    }

    Orient.instance().registerWeakOrientStartupListener(this);
    Orient.instance().registerWeakOrientShutdownListener(this);
  }

  public void increment() {
    updateCounter(+1);
  }

  public void decrement() {
    updateCounter(-1);
  }

  private void updateCounter(int delta) {
    final int hashCode = threadHashCode.get();

    while (true) {
      final AtomicLong[] cts = counters;
      final int index = (cts.length - 1) & hashCode;

      AtomicLong counter = cts[index];

      if (counter == null) {
        if (!poolBusy.get() && poolBusy.compareAndSet(false, true)) {
          if (cts == counters) {
            counter = cts[index];

            if (counter == null)
              cts[index] = new AtomicLong();
          }

          poolBusy.set(false);
        }

        continue;
      } else {
        long v = counter.get();
        int retries = 0;

        if (cts.length < maxPartitions) {
          while (retries < MAX_RETRIES) {
            if (!counter.compareAndSet(v, v + delta)) {
              retries++;
              v = counter.get();
            } else {
              return;
            }
          }
        } else {
          counter.addAndGet(delta);
          return;
        }

        if (!poolBusy.get() && poolBusy.compareAndSet(false, true)) {
          if (cts == counters) {
            if (cts.length < maxPartitions) {
              counters = new AtomicLong[cts.length << 1];
              System.arraycopy(cts, 0, counters, 0, cts.length);
            }
          }

          poolBusy.set(false);
        }

        continue;
      }
    }
  }

  public boolean isEmpty() {
    long sum = 0;

    for (AtomicLong counter : counters)
      if (counter != null)
        sum += counter.get();

    return sum == 0;
  }

  private static int nextHashCode() {
    return nextHashCode.getAndAdd(HASH_INCREMENT);
  }

  private static class ThreadHashCode extends ThreadLocal<Integer> {
    @Override
    protected Integer initialValue() {
      return nextHashCode();
    }
  }
}
