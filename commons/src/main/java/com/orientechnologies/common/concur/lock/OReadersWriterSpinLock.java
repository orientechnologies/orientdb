package com.orientechnologies.common.concur.lock;

import com.orientechnologies.common.types.OModifiableInteger;
import com.orientechnologies.common.util.MersenneTwister;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.AbstractOwnableSynchronizer;
import java.util.concurrent.locks.LockSupport;

/**
 * @author Andrey Lomakin <a href="mailto:lomakin.andrey@gmail.com">Andrey Lomakin</a>
 * @since 8/18/14
 */
public class OReadersWriterSpinLock extends AbstractOwnableSynchronizer {
  private static final int                      NCPU           = Runtime.getRuntime().availableProcessors();
  private static final int                      COUNTERS_SIZE  = 1 << (32 - Integer.numberOfLeadingZeros((NCPU << 2) - 1));
  private static final int                      MASK           = COUNTERS_SIZE - 1;

  private final MersenneTwister                 random         = new MersenneTwister();
  private final ThreadLocal<Integer>            threadHashCode = new ThreadLocal<Integer>() {
                                                                 @Override
                                                                 protected Integer initialValue() {
                                                                   return random.nextInt();
                                                                 }
                                                               };

  private final AtomicInteger                   writeState     = new AtomicInteger(0);

  private final PaddedCounter[]                 readCounters;

  private final long                            writeDelay;

  private final ThreadLocal<OModifiableInteger> lockHolds      = new ThreadLocal<OModifiableInteger>() {
                                                                 @Override
                                                                 protected OModifiableInteger initialValue() {
                                                                   return new OModifiableInteger();
                                                                 }
                                                               };

  public OReadersWriterSpinLock() {
    this(100 * 1000); // 0.1 ms
  }

  public OReadersWriterSpinLock(final long writeDelay) {
    this.writeDelay = writeDelay;

    final PaddedCounter[] counters = new PaddedCounter[COUNTERS_SIZE];
    for (int i = 0; i < counters.length; i++)
      counters[i] = new PaddedCounter();

    readCounters = counters;
  }

  public void acquireReadLock() {
    final OModifiableInteger lHolds = lockHolds.get();

    final int holds = lHolds.intValue();
    if (holds > 0) {
      // we have already acquire read lock
      lockHolds.get().increment();
      return;
    } else if (holds < 0) {
      // write lock is acquired before, do nothing
      return;
    }

    final PaddedCounter counter = readCounters[threadHashCode.get() & MASK];
    incrementCounter(counter);

    int wstate = writeState.get();

    while (wstate > 0) {
      decrementCounter(counter);

      while (writeState.get() == 1)
        LockSupport.parkNanos(writeDelay);

      incrementCounter(counter);

      wstate = writeState.get();
    }

    lHolds.increment();
    assert lHolds.intValue() == 1;
  }

  public void releaseReadLock() {
    final OModifiableInteger lHolds = lockHolds.get();
    final int holds = lHolds.intValue();
    if (holds > 1) {
      lockHolds.get().decrement();
      return;
    } else if (holds < 0) {
      // write lock was acquired before, do nothing
      return;
    }

    final PaddedCounter counter = readCounters[threadHashCode.get() & MASK];
    decrementCounter(counter);

    lHolds.decrement();
    assert lHolds.intValue() == 0;
  }

  private void incrementCounter(PaddedCounter counter) {
    long attempts = 0;

    long cnt = counter.get();
    while (!counter.compareAndSet(cnt, cnt + 1)) {
      attempts++;

      if (attempts > NCPU)
        LockSupport.parkNanos(1);

      cnt = counter.get();
    }
  }

  public void acquireWriteLock() {
    final OModifiableInteger lHolds = lockHolds.get();

    if (lHolds.intValue() < 0) {
      lHolds.decrement();
      return;
    }

    while (!writeState.compareAndSet(0, 1))
      LockSupport.parkNanos(writeDelay);

    boolean cnt = false;
    while (!cnt) {
      cnt = true;

      for (PaddedCounter counter : readCounters) {
        cnt = counter.get() == 0;

        if (!cnt)
          break;
      }

    }

    setExclusiveOwnerThread(Thread.currentThread());
    lHolds.decrement();
    assert lHolds.intValue() == -1;
  }

  public void releaseWriteLock() {
    final OModifiableInteger lHolds = lockHolds.get();

    if (lHolds.intValue() < -1) {
      lHolds.increment();
      return;
    }

    assert writeState.get() == 1;
    setExclusiveOwnerThread(null);
    writeState.set(0);

    lHolds.increment();
    assert lHolds.intValue() == 0;
  }

  private void decrementCounter(PaddedCounter counter) {
    long attempts = 0;

    long cnt = counter.get();
    assert cnt > 0;
    while (!counter.compareAndSet(cnt, cnt - 1)) {
      attempts++;
      if (attempts > NCPU)
        LockSupport.parkNanos(1);

      cnt = counter.get();
    }
  }

  void modPadding() {
    for (PaddedCounter counter : readCounters)
      counter.modPadding();
  }

  @Override
  public String toString() {
    modPadding();
    return "OReadersWriterSpinLock{" + "writeState=" + writeState + ", readCounters=" + Arrays.toString(readCounters) + '}';
  }

  private static final class PaddedCounter extends AtomicLong {
    private long p0 = 0, p1 = 1, p2 = 2, p3 = 3, p4 = 4, p5 = 5, p6 = 6;

    void modPadding() {
      Random random = new Random();
      p0 = random.nextLong();
      p1 = random.nextLong();
      p2 = random.nextLong();
      p3 = random.nextLong();
      p4 = random.nextLong();
      p5 = random.nextLong();
      p6 = random.nextLong();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o)
        return true;
      if (o == null || getClass() != o.getClass())
        return false;

      PaddedCounter that = (PaddedCounter) o;

      if (p0 != that.p0)
        return false;
      if (p1 != that.p1)
        return false;
      if (p2 != that.p2)
        return false;
      if (p3 != that.p3)
        return false;
      if (p4 != that.p4)
        return false;
      if (p5 != that.p5)
        return false;
      if (p6 != that.p6)
        return false;

      return true;
    }

    @Override
    public int hashCode() {
      int result = (int) (p0 ^ (p0 >>> 32));
      result = 31 * result + (int) (p1 ^ (p1 >>> 32));
      result = 31 * result + (int) (p2 ^ (p2 >>> 32));
      result = 31 * result + (int) (p3 ^ (p3 >>> 32));
      result = 31 * result + (int) (p4 ^ (p4 >>> 32));
      result = 31 * result + (int) (p5 ^ (p5 >>> 32));
      result = 31 * result + (int) (p6 ^ (p6 >>> 32));
      return result;
    }
  }
}