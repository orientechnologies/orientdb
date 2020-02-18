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

package com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations;

import com.orientechnologies.common.concur.lock.OInterruptedException;
import com.orientechnologies.common.concur.lock.OOneEntryPerKeyLockManager;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.OOrientListenerAbstract;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.storage.cache.OReadCache;
import com.orientechnologies.orient.core.storage.cache.OWriteCache;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurableComponent;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.ONonTxOperationPerformedWALRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitId;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWriteAheadLog;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.LockSupport;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 12/3/13
 */
public class OAtomicOperationsManager {
  private final LongAdder atomicOperationsCount = new LongAdder();

  private final AtomicInteger freezeRequests = new AtomicInteger();

  private final ConcurrentMap<Long, FreezeParameters> freezeParametersIdMap = new ConcurrentHashMap<>();
  private final AtomicLong                            freezeIdGen           = new AtomicLong();

  private final AtomicReference<WaitingListNode> waitingHead = new AtomicReference<>();
  private final AtomicReference<WaitingListNode> waitingTail = new AtomicReference<>();

  private static volatile ThreadLocal<OAtomicOperation> currentOperation = new ThreadLocal<>();

  private final boolean trackPageOperations;
  private final int     operationsCacheLimit;

  static {
    Orient.instance().registerListener(new OOrientListenerAbstract() {
      @Override
      public void onStartup() {
        if (currentOperation == null) {
          currentOperation = new ThreadLocal<>();
        }
      }

      @Override
      public void onShutdown() {
        currentOperation = null;
      }
    });
  }

  private final OAbstractPaginatedStorage          storage;
  private final OWriteAheadLog                     writeAheadLog;
  private final OOneEntryPerKeyLockManager<String> lockManager = new OOneEntryPerKeyLockManager<>(true, -1,
      OGlobalConfiguration.COMPONENTS_LOCK_CACHE.getValueAsInteger());
  private final OReadCache                         readCache;
  private final OWriteCache                        writeCache;

  public OAtomicOperationsManager(OAbstractPaginatedStorage storage, boolean trackPageOperations, int operationsCacheLimit) {
    this.storage = storage;
    this.writeAheadLog = storage.getWALInstance();
    this.readCache = storage.getReadCache();
    this.writeCache = storage.getWriteCache();

    this.trackPageOperations = trackPageOperations;
    this.operationsCacheLimit = operationsCacheLimit;
  }

  /**
   * @see #startAtomicOperation(String, boolean)
   */
  public OAtomicOperation startAtomicOperation(ODurableComponent durableComponent, boolean trackNonTxOperations)
      throws IOException {
    if (durableComponent != null) {
      return startAtomicOperation(durableComponent.getLockName(), trackNonTxOperations);
    }

    return startAtomicOperation((String) null, trackNonTxOperations);
  }

  /**
   * Starts atomic operation inside of current thread. If atomic operation has been already started, current atomic operation
   * instance will be returned. All durable components have to call this method at the beginning of any data modification
   * operation.
   * <p>In current implementation of atomic operation, each component which is participated in atomic operation is hold under
   * exclusive lock till atomic operation will not be completed (committed or rolled back).
   * <p>If other thread is going to read data from component it has to acquire read lock inside of atomic operation manager {@link
   * #acquireReadLock(ODurableComponent)}, otherwise data consistency will be compromised.
   * <p>Atomic operation may be delayed if start of atomic operations is prohibited by call of {@link
   * #freezeAtomicOperations(Class, String)} method. If mentioned above method is called then execution of current method will be
   * stopped till call of {@link #releaseAtomicOperations(long)} method or exception will be thrown. Concrete behaviour depends on
   * real values of parameters of {@link #freezeAtomicOperations(Class, String)} method.
   *
   * @param trackNonTxOperations If this flag set to <code>true</code> then special record {@link ONonTxOperationPerformedWALRecord}
   *                             will be added to WAL in case of atomic operation is started outside of active storage transaction.
   *                             During storage restore procedure this record is monitored and if given record is present then
   *                             rebuild of all indexes is performed.
   * @param lockName             Name of lock (usually name of component) which is going participate in atomic operation.
   * @return Instance of active atomic operation.
   */
  public OAtomicOperation startAtomicOperation(String lockName, boolean trackNonTxOperations) throws IOException {
    return startAtomicOperation(lockName, trackNonTxOperations, Optional.empty());
  }

  public OAtomicOperation startAtomicOperation(String lockName, boolean trackNonTxOperations, Optional<byte[]> metadata)
      throws IOException {
    OAtomicOperation operation = currentOperation.get();
    if (operation != null) {
      operation.incrementCounter();

      if (lockName != null) {
        acquireExclusiveLockTillOperationComplete(operation, lockName);
      }

      return operation;
    }

    atomicOperationsCount.increment();

    while (freezeRequests.get() > 0) {
      assert freezeRequests.get() >= 0;

      atomicOperationsCount.decrement();

      throwFreezeExceptionIfNeeded();

      final Thread thread = Thread.currentThread();

      addThreadInWaitingList(thread);

      if (freezeRequests.get() > 0) {
        LockSupport.park(this);
      }

      atomicOperationsCount.increment();
    }

    assert freezeRequests.get() >= 0;

    final OOperationUnitId unitId = OOperationUnitId.generateId();
    final OLogSequenceNumber lsn;
    if (metadata.isPresent()) {
      lsn = writeAheadLog.logAtomicOperationStartRecord(true, unitId, metadata.get());
    } else {
      lsn = writeAheadLog.logAtomicOperationStartRecord(true, unitId);
    }

    if (!trackPageOperations) {
      operation = new OAtomicOperationBinaryTracking(lsn, unitId, readCache, writeCache, storage.getId());
    } else {
      operation = new OAtomicOperationPageOperationsTracking(readCache, writeCache, writeAheadLog, unitId, operationsCacheLimit,
          lsn);
    }

    currentOperation.set(operation);

    if (trackNonTxOperations && storage.getStorageTransaction() == null) {
      writeAheadLog.log(new ONonTxOperationPerformedWALRecord());
    }

    if (lockName != null) {
      acquireExclusiveLockTillOperationComplete(operation, lockName);
    }

    try {
      storage.checkReadOnlyConditions();
    } catch (RuntimeException | Error e) {
      final Iterator<String> lockedObjectIterator = operation.lockedObjects().iterator();

      while (lockedObjectIterator.hasNext()) {
        final String lockedObject = lockedObjectIterator.next();
        lockedObjectIterator.remove();

        lockManager.releaseLock(this, lockedObject, OOneEntryPerKeyLockManager.LOCK.EXCLUSIVE);
      }

      throw e;
    }

    return operation;
  }

  public static void alarmClearOfAtomicOperation() {
    final OAtomicOperation current = currentOperation.get();

    if (current != null) {
      currentOperation.set(null);
    }
  }

  private void addThreadInWaitingList(Thread thread) {
    final WaitingListNode node = new WaitingListNode(thread);

    while (true) {
      final WaitingListNode last = waitingTail.get();

      if (waitingTail.compareAndSet(last, node)) {
        if (last == null) {
          waitingHead.set(node);
        } else {
          last.next = node;
          last.linkLatch.countDown();
        }

        break;
      }
    }
  }

  private WaitingListNode cutWaitingList() {
    while (true) {
      final WaitingListNode tail = waitingTail.get();
      final WaitingListNode head = waitingHead.get();

      if (tail == null) {
        return null;
      }

      //head is null but tail is not null we are in the middle of addition of item in the list
      if (head == null) {
        //let other thread to make it's work
        Thread.yield();
        continue;
      }

      if (head == tail) {
        return new WaitingListNode(head.item);
      }

      if (waitingHead.compareAndSet(head, tail)) {
        WaitingListNode node = head;

        node.waitTillAllLinksWillBeCreated();

        while (node.next != tail) {
          node = node.next;

          node.waitTillAllLinksWillBeCreated();
        }

        node.next = new WaitingListNode(tail.item);

        return head;
      }
    }
  }

  public long freezeAtomicOperations(Class<? extends OException> exceptionClass, String message) {

    final long id = freezeIdGen.incrementAndGet();

    freezeRequests.incrementAndGet();
    freezeParametersIdMap.put(id, new FreezeParameters(message, exceptionClass));

    while (atomicOperationsCount.sum() > 0) {
      Thread.yield();
    }

    return id;
  }

  public boolean isFrozen() {
    return freezeRequests.get() > 0;
  }

  public void releaseAtomicOperations(long id) {
    if (id >= 0) {
      final FreezeParameters freezeParameters = freezeParametersIdMap.remove(id);
      if (freezeParameters == null) {
        throw new IllegalStateException("Invalid value for freeze id " + id);
      }
    }

    final Map<Long, FreezeParameters> freezeParametersMap = new HashMap<>(freezeParametersIdMap);
    final long requests = freezeRequests.decrementAndGet();

    if (requests == 0) {
      for (Long freezeId : freezeParametersMap.keySet()) {
        freezeParametersIdMap.remove(freezeId);
      }

      WaitingListNode node = cutWaitingList();

      while (node != null) {
        LockSupport.unpark(node.item);
        node = node.next;
      }
    }
  }

  private void throwFreezeExceptionIfNeeded() {
    for (FreezeParameters freezeParameters : this.freezeParametersIdMap.values()) {
      if (freezeParameters.exceptionClass != null) {
        if (freezeParameters.message != null) {
          try {
            final Constructor<? extends OException> mConstructor = freezeParameters.exceptionClass.getConstructor(String.class);
            throw mConstructor.newInstance(freezeParameters.message);
          } catch (InstantiationException | IllegalAccessException | NoSuchMethodException | SecurityException | InvocationTargetException ie) {
            OLogManager.instance().error(this, "Can not create instance of exception " + freezeParameters.exceptionClass
                + " with message will try empty constructor instead", ie);
            throwFreezeExceptionWithoutMessage(freezeParameters);
          }
        } else {
          throwFreezeExceptionWithoutMessage(freezeParameters);
        }

      }
    }
  }

  private void throwFreezeExceptionWithoutMessage(FreezeParameters freezeParameters) {
    try {
      throw freezeParameters.exceptionClass.newInstance();
    } catch (InstantiationException | IllegalAccessException ie) {
      OLogManager.instance().error(this, "Can not create instance of exception " + freezeParameters.exceptionClass
          + " will park thread instead of throwing of exception", ie);
    }
  }

  public static OAtomicOperation getCurrentOperation() {
    return currentOperation.get();
  }

  /**
   * Ends the current atomic operation on this manager.
   *
   * @param rollback {@code true} to indicate a rollback, {@code false} for successful commit.
   * @return the LSN produced by committing the current operation or {@code null} if no commit was done.
   */
  public OLogSequenceNumber endAtomicOperation(boolean rollback) throws IOException {
    final OAtomicOperation operation = currentOperation.get();

    if (operation == null) {
      OLogManager.instance().error(this, "There is no atomic operation active", null);
      throw new ODatabaseException("There is no atomic operation active");
    }

    int counter = operation.getCounter();
    operation.decrementCounter();

    assert counter > 0;

    final OLogSequenceNumber lsn;
    try {
      if (rollback) {
        operation.rollbackInProgress();
      }

      if (counter == 1) {
        try {
          if (trackPageOperations) {
            lsn = operation.commitChanges(writeAheadLog);
          } else if (!operation.isRollbackInProgress()) {
            lsn = operation.commitChanges(writeAheadLog);
          } else {
            lsn = null;
          }
        } finally {
          final Iterator<String> lockedObjectIterator = operation.lockedObjects().iterator();

          while (lockedObjectIterator.hasNext()) {
            final String lockedObject = lockedObjectIterator.next();
            lockedObjectIterator.remove();

            lockManager.releaseLock(this, lockedObject, OOneEntryPerKeyLockManager.LOCK.EXCLUSIVE);
          }

          currentOperation.set(null);
        }
      } else {
        lsn = null;
      }
    } catch (Error e) {
      final OAbstractPaginatedStorage st = storage;
      if (st != null) {
        st.handleJVMError(e);
      }

      counter = 1;
      throw e;
    } finally {
      if (counter == 1) {
        atomicOperationsCount.decrement();
      }
    }

    return lsn;
  }

  public void ensureThatComponentsUnlocked() {
    final OAtomicOperation operation = currentOperation.get();
    if (operation != null) {
      final Iterator<String> lockedObjectIterator = operation.lockedObjects().iterator();

      while (lockedObjectIterator.hasNext()) {
        final String lockedObject = lockedObjectIterator.next();
        lockedObjectIterator.remove();

        lockManager.releaseLock(this, lockedObject, OOneEntryPerKeyLockManager.LOCK.EXCLUSIVE);
      }
    }
  }

  /**
   * Acquires exclusive lock with the given lock name in the given atomic operation.
   *
   * @param operation the atomic operation to acquire the lock in.
   * @param lockName  the lock name to acquire.
   */
  public void acquireExclusiveLockTillOperationComplete(OAtomicOperation operation, String lockName) {
    if (operation.containsInLockedObjects(lockName)) {
      return;
    }

    lockManager.acquireLock(lockName, OOneEntryPerKeyLockManager.LOCK.EXCLUSIVE);
    operation.addLockedObject(lockName);
  }

  /**
   * Acquires exclusive lock in the active atomic operation running on the current thread for the {@code durableComponent}.
   */
  public void acquireExclusiveLockTillOperationComplete(ODurableComponent durableComponent) {
    final OAtomicOperation operation = currentOperation.get();
    assert operation != null;
    acquireExclusiveLockTillOperationComplete(operation, durableComponent.getLockName());
  }

  public void acquireReadLock(ODurableComponent durableComponent) {
    assert durableComponent.getLockName() != null;

    lockManager.acquireLock(durableComponent.getLockName(), OOneEntryPerKeyLockManager.LOCK.SHARED);
  }

  public void releaseReadLock(ODurableComponent durableComponent) {
    assert durableComponent.getName() != null;
    assert durableComponent.getLockName() != null;

    lockManager.releaseLock(this, durableComponent.getLockName(), OOneEntryPerKeyLockManager.LOCK.SHARED);
  }

  private static final class FreezeParameters {
    private final String                      message;
    private final Class<? extends OException> exceptionClass;

    FreezeParameters(String message, Class<? extends OException> exceptionClass) {
      this.message = message;
      this.exceptionClass = exceptionClass;
    }
  }

  private static final class WaitingListNode {
    /**
     * Latch which indicates that all links are created between add and existing list elements.
     */
    private final CountDownLatch linkLatch = new CountDownLatch(1);

    private final    Thread          item;
    private volatile WaitingListNode next;

    WaitingListNode(Thread item) {
      this.item = item;
    }

    void waitTillAllLinksWillBeCreated() {
      try {
        linkLatch.await();
      } catch (InterruptedException e) {
        throw OException.wrapException(
            new OInterruptedException("Thread was interrupted while was waiting for completion of 'waiting linked list' operation"),
            e);
      }
    }
  }
}
