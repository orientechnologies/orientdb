package com.orientechnologies.common.concur.lock;

import com.orientechnologies.common.types.OModifiableInteger;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.AbstractOwnableSynchronizer;
import java.util.concurrent.locks.LockSupport;

/**
 * @author Andrey Lomakin <a href="mailto:lomakin.andrey@gmail.com">Andrey Lomakin</a>
 * @since 8/18/14
 */
public class OReadersWriterSpinLock extends AbstractOwnableSynchronizer {
  private final OThreadCountersHashTable threadCountersHashTable = new OThreadCountersHashTable();
  private final AtomicInteger                   writeState              = new AtomicInteger(0);
  private final long                            writeDelay;

  private final ThreadLocal<OModifiableInteger> lockHolds               = new ThreadLocal<OModifiableInteger>() {
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
  }

  public void acquireReadLock() {
    final OModifiableInteger lHolds = lockHolds.get();

    final int holds = lHolds.intValue();
    if (holds > 0) {
      // we have already acquire read lock
      lHolds.increment();
      return;
    } else if (holds < 0) {
      // write lock is acquired before, do nothing
      return;
    }

    threadCountersHashTable.increment();

    int wstate = writeState.get();

    while (wstate > 0) {
      threadCountersHashTable.decrement();

      while (writeState.get() == 1)
        LockSupport.parkNanos(writeDelay);

      threadCountersHashTable.increment();

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

    threadCountersHashTable.decrement();

    lHolds.decrement();
    assert lHolds.intValue() == 0;
  }

  public void acquireWriteLock() {
    final OModifiableInteger lHolds = lockHolds.get();

    if (lHolds.intValue() < 0) {
      lHolds.decrement();
      return;
    }

    while (!writeState.compareAndSet(0, 1))
      LockSupport.parkNanos(writeDelay);

    while (!threadCountersHashTable.isEmpty())
      ;

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
}