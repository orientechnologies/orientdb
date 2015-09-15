package com.orientechnologies.common.concur.lock;

import com.orientechnologies.orient.core.OOrientListenerAbstract;
import com.orientechnologies.orient.core.Orient;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * * @author Andrey Lomakin (a.lomakin-at-orientechnologies.com)
 */
@SuppressFBWarnings(value = "VO_VOLATILE_REFERENCE_TO_ARRAY")
public class ODistributedCounter extends OOrientListenerAbstract {
  private static final int HASH_INCREMENT = 0x61c88647;
  private static final int MAX_RETRIES = 8;

  private static final AtomicInteger nextHashCode = new AtomicInteger();
  private final AtomicBoolean           isBusy         = new AtomicBoolean();
  private final int maxPartitions = Runtime.getRuntime().availableProcessors() << 3;


  private volatile ThreadLocal<Integer> threadHashCode = new ThreadHashCode();
  private volatile AtomicLong[] counters;

  public ODistributedCounter() {
    final AtomicLong[] cts = new AtomicLong[2];
    for (int i = 0; i < cts.length; i++) {
      cts[i] = new AtomicLong();
    }

    counters = cts;

    Orient.instance().registerWeakOrientStartupListener(this);
    Orient.instance().registerWeakOrientShutdownListener(this);
  }

  @Override
  public void onStartup() {
    if (threadHashCode == null)
      threadHashCode = new ThreadHashCode();
  }

  @Override
  public void onShutdown() {
    threadHashCode = null;
  }

  public void increment() {
    updateCounter(+1);
  }

  public void decrement() {
    updateCounter(-1);
  }

  public void add(long delta) {
    updateCounter(delta);
  }

  public void clear() {
    while (!isBusy.compareAndSet(false, true))
      ;

    final AtomicLong[] cts = new AtomicLong[counters.length];
    for (int i = 0; i < counters.length; i++) {
      cts[i] = new AtomicLong();
    }

    counters = cts;

    isBusy.set(false);
  }

  private void updateCounter(long delta) {
    final int hashCode = threadHashCode.get();

    while (true) {
      final AtomicLong[] cts = counters;
      final int index = (cts.length - 1) & hashCode;

      AtomicLong counter = cts[index];

      if (counter == null) {
        if (!isBusy.get() && isBusy.compareAndSet(false, true)) {
          if (cts == counters) {
            counter = cts[index];

            if (counter == null)
              cts[index] = new AtomicLong();
          }

          isBusy.set(false);
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

        if (!isBusy.get() && isBusy.compareAndSet(false, true)) {
          if (cts == counters) {
            if (cts.length < maxPartitions) {
              final AtomicLong[] newCts = new AtomicLong[cts.length << 1];
              System.arraycopy(cts, 0, newCts, 0, cts.length);
              counters = newCts;
            }
          }

          isBusy.set(false);
        }

        continue;
      }
    }
  }

  public boolean isEmpty() {
    return get() == 0;
  }

  public long get() {
    long sum = 0;

    for (AtomicLong counter : counters)
      if (counter != null)
        sum += counter.get();

    return sum;
  }

  private static int nextHashCode() {
    return nextHashCode.getAndAdd(HASH_INCREMENT);
  }

  private static final class ThreadHashCode extends ThreadLocal<Integer> {
    @Override
    protected Integer initialValue() {
      return nextHashCode();
    }
  }
}
