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
import com.orientechnologies.common.util.OPair;
import com.orientechnologies.orient.core.OOrientListenerAbstract;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.cache.OReadCache;
import com.orientechnologies.orient.core.storage.cache.OWriteCache;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OStorageTransaction;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurableComponent;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.ONonTxOperationPerformedWALRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitId;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWriteAheadLog;
import com.orientechnologies.orient.core.storage.impl.local.statistic.OPerformanceStatisticManager;
import com.orientechnologies.orient.core.tx.OTransaction;

import javax.management.*;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.LockSupport;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 12/3/13
 */
public class OAtomicOperationsManager implements OAtomicOperationsMangerMXBean {
  public static final String MBEAN_NAME = "com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations:type=OAtomicOperationsMangerMXBean";

  private volatile boolean trackAtomicOperations = OGlobalConfiguration.TX_TRACK_ATOMIC_OPERATIONS.getValueAsBoolean();

  private final AtomicBoolean mbeanIsRegistered = new AtomicBoolean();

  private final LongAdder atomicOperationsCount = new LongAdder();

  private final AtomicInteger freezeRequests = new AtomicInteger();

  private final ConcurrentMap<Long, FreezeParameters> freezeParametersIdMap = new ConcurrentHashMap<Long, FreezeParameters>();
  private final AtomicLong                            freezeIdGen           = new AtomicLong();

  private final AtomicReference<WaitingListNode> waitingHead = new AtomicReference<WaitingListNode>();
  private final AtomicReference<WaitingListNode> waitingTail = new AtomicReference<WaitingListNode>();

  private static volatile ThreadLocal<OAtomicOperation> currentOperation = new ThreadLocal<OAtomicOperation>();
  private final OPerformanceStatisticManager performanceStatisticManager;

  static {
    Orient.instance().registerListener(new OOrientListenerAbstract() {
      @Override
      public void onStartup() {
        if (currentOperation == null)
          currentOperation = new ThreadLocal<OAtomicOperation>();
      }

      @Override
      public void onShutdown() {
        currentOperation = null;
      }
    });
  }

  private final OAbstractPaginatedStorage storage;
  private final OWriteAheadLog            writeAheadLog;
  private final OOneEntryPerKeyLockManager<String> lockManager = new OOneEntryPerKeyLockManager<String>(true, -1,
      OGlobalConfiguration.COMPONENTS_LOCK_CACHE.getValueAsInteger());
  private final OReadCache  readCache;
  private final OWriteCache writeCache;

  private final Map<OOperationUnitId, OPair<String, StackTraceElement[]>> activeAtomicOperations = new ConcurrentHashMap<OOperationUnitId, OPair<String, StackTraceElement[]>>();

  public OAtomicOperationsManager(OAbstractPaginatedStorage storage) {
    this.storage = storage;
    this.writeAheadLog = storage.getWALInstance();
    this.readCache = storage.getReadCache();
    this.writeCache = storage.getWriteCache();
    this.performanceStatisticManager = storage.getPerformanceStatisticManager();

    performanceStatisticManager.registerComponent("atomic operation");
  }

  /**
   * @see #startAtomicOperation(String, boolean)
   */
  public OAtomicOperation startAtomicOperation(ODurableComponent durableComponent, boolean trackNonTxOperations)
      throws IOException {
    if (durableComponent != null)
      return startAtomicOperation(durableComponent.getLockName(), trackNonTxOperations);

    return startAtomicOperation((String) null, trackNonTxOperations);
  }

  /**
   * Starts atomic operation inside of current thread. If atomic operation has been already started, current atomic operation
   * instance will be returned. All durable components have to call this method at the beginning of any data modification
   * operation.
   * <p>
   * <p>In current implementation of atomic operation, each component which is participated in atomic operation is hold under
   * exclusive lock till atomic operation will not be completed (committed or rollbacked).
   * <p>
   * <p>If other thread is going to read data from component it has to acquire read lock inside of atomic operation manager {@link
   * #acquireReadLock(ODurableComponent)}, otherwise data consistency will be compromised.
   * <p>
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
   *
   * @return Instance of active atomic operation.
   */
  public OAtomicOperation startAtomicOperation(String lockName, boolean trackNonTxOperations) throws IOException {
    OAtomicOperation operation = currentOperation.get();
    if (operation != null) {
      operation.incrementCounter();

      if (lockName != null)
        acquireExclusiveLockTillOperationComplete(operation, lockName);

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

    final boolean useWal = useWal();
    final OOperationUnitId unitId = OOperationUnitId.generateId();
    final OLogSequenceNumber lsn = useWal ? writeAheadLog.logAtomicOperationStartRecord(true, unitId) : null;

    operation = new OAtomicOperation(lsn, unitId, readCache, writeCache, storage.getId(), performanceStatisticManager);
    currentOperation.set(operation);

    if (trackAtomicOperations) {
      final Thread thread = Thread.currentThread();
      activeAtomicOperations.put(unitId, new OPair<String, StackTraceElement[]>(thread.getName(), thread.getStackTrace()));
    }

    if (useWal && trackNonTxOperations && storage.getStorageTransaction() == null)
      writeAheadLog.log(new ONonTxOperationPerformedWALRecord());

    if (lockName != null)
      acquireExclusiveLockTillOperationComplete(operation, lockName);

    return operation;
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

      if (tail == null)
        return null;

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
      if (freezeParameters == null)
        throw new IllegalStateException("Invalid value for freeze id " + id);
    }

    final Map<Long, FreezeParameters> freezeParametersMap = new HashMap<Long, FreezeParameters>(freezeParametersIdMap);
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
        if (freezeParameters.message != null)
          try {
            final Constructor<? extends OException> mConstructor = freezeParameters.exceptionClass.getConstructor(String.class);
            throw mConstructor.newInstance(freezeParameters.message);
          } catch (InstantiationException ie) {
            OLogManager.instance().error(this, "Can not create instance of exception " + freezeParameters.exceptionClass
                + " with message will try empty constructor instead", ie);
            throwFreezeExceptionWithoutMessage(freezeParameters);
          } catch (IllegalAccessException iae) {
            OLogManager.instance().error(this, "Can not create instance of exception " + freezeParameters.exceptionClass
                + " with message will try empty constructor instead", iae);
            throwFreezeExceptionWithoutMessage(freezeParameters);
          } catch (NoSuchMethodException nsme) {
            OLogManager.instance().error(this, "Can not create instance of exception " + freezeParameters.exceptionClass
                + " with message will try empty constructor instead", nsme);
            throwFreezeExceptionWithoutMessage(freezeParameters);
          } catch (SecurityException se) {
            OLogManager.instance().error(this, "Can not create instance of exception " + freezeParameters.exceptionClass
                + " with message will try empty constructor instead", se);
            throwFreezeExceptionWithoutMessage(freezeParameters);
          } catch (InvocationTargetException ite) {
            OLogManager.instance().error(this, "Can not create instance of exception " + freezeParameters.exceptionClass
                + " with message will try empty constructor instead", ite);
            throwFreezeExceptionWithoutMessage(freezeParameters);
          }
        else {
          throwFreezeExceptionWithoutMessage(freezeParameters);
        }

      }
    }
  }

  private void throwFreezeExceptionWithoutMessage(FreezeParameters freezeParameters) {
    try {
      throw freezeParameters.exceptionClass.newInstance();
    } catch (InstantiationException ie) {
      OLogManager.instance().error(this, "Can not create instance of exception " + freezeParameters.exceptionClass
          + " will park thread instead of throwing of exception", ie);
    } catch (IllegalAccessException iae) {
      OLogManager.instance().error(this, "Can not create instance of exception " + freezeParameters.exceptionClass
          + " will park thread instead of throwing of exception", iae);
    }
  }

  public OAtomicOperation getCurrentOperation() {
    return currentOperation.get();
  }

  public OAtomicOperation endAtomicOperation(boolean rollback, Exception exception) throws IOException {
    final OAtomicOperation operation = currentOperation.get();
    assert operation != null;

    if (rollback) {
      operation.rollback(exception);
    }

    if (operation.isRollback() && !rollback) {
      final StringWriter writer = new StringWriter();
      writer.append("Atomic operation was rolled back by internal component");
      if (operation.getRollbackException() != null) {
        writer.append(", exception which caused this rollback is :\n");
        operation.getRollbackException().printStackTrace(new PrintWriter(writer));
        writer.append("\r\n");
      }

      atomicOperationsCount.decrement();

      final ONestedRollbackException nre = new ONestedRollbackException(writer.toString());
      throw OException.wrapException(nre, exception);
    }

    final int counter = operation.getCounter();
    assert counter > 0;

    if (counter == 1) {
      final boolean useWal = useWal();

      if (!operation.isRollback())
        operation.commitChanges(useWal ? writeAheadLog : null);

      if (useWal)
        writeAheadLog.logAtomicOperationEndRecord(operation.getOperationUnitId(), rollback, operation.getStartLSN(),
            operation.getMetadata());

      // We have to decrement the counter after the disk operations, otherwise, if they
      // fail, we will be unable to rollback the atomic operation later.
      operation.decrementCounter();
      currentOperation.set(null);

      if (trackAtomicOperations) {
        activeAtomicOperations.remove(operation.getOperationUnitId());
      }

      for (String lockObject : operation.lockedObjects())
        lockManager.releaseLock(this, lockObject, OOneEntryPerKeyLockManager.LOCK.EXCLUSIVE);

      atomicOperationsCount.decrement();
    } else
      operation.decrementCounter();

    return operation;
  }

  /**
   * Acquires exclusive lock with the given lock name in the given atomic operation.
   *
   * @param operation the atomic operation to acquire the lock in.
   * @param lockName  the lock name to acquire.
   */
  public void acquireExclusiveLockTillOperationComplete(OAtomicOperation operation, String lockName) {
    if (operation.containsInLockedObjects(lockName))
      return;

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

  public void registerMBean() {
    if (mbeanIsRegistered.compareAndSet(false, true)) {
      try {
        final MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        final ObjectName mbeanName = new ObjectName(getMBeanName());

        if (!server.isRegistered(mbeanName)) {
          server.registerMBean(this, mbeanName);
        } else {
          mbeanIsRegistered.set(false);
          OLogManager.instance().warn(this,
              "MBean with name %s has already registered. Probably your system was not shutdown correctly "
                  + "or you have several running applications which use OrientDB engine inside", mbeanName.getCanonicalName());
        }

      } catch (MalformedObjectNameException e) {
        throw OException.wrapException(new OStorageException("Error during registration of atomic manager MBean"), e);
      } catch (InstanceAlreadyExistsException e) {
        throw OException.wrapException(new OStorageException("Error during registration of atomic manager MBean"), e);
      } catch (MBeanRegistrationException e) {
        throw OException.wrapException(new OStorageException("Error during registration of atomic manager MBean"), e);
      } catch (NotCompliantMBeanException e) {
        throw OException.wrapException(new OStorageException("Error during registration of atomic manager MBean"), e);
      }
    }
  }

  private String getMBeanName() {
    return MBEAN_NAME + ",name=" + ObjectName.quote(storage.getName()) + ",id=" + storage.getId();
  }

  public void unregisterMBean() {
    if (mbeanIsRegistered.compareAndSet(true, false)) {
      try {
        final MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        final ObjectName mbeanName = new ObjectName(getMBeanName());
        server.unregisterMBean(mbeanName);
      } catch (MalformedObjectNameException e) {
        throw OException.wrapException(new OStorageException("Error during unregistration of atomic manager MBean"), e);
      } catch (InstanceNotFoundException e) {
        throw OException.wrapException(new OStorageException("Error during unregistration of atomic manager MBean"), e);
      } catch (MBeanRegistrationException e) {
        throw OException.wrapException(new OStorageException("Error during unregistration of atomic manager MBean"), e);
      }
    }
  }

  @Override
  public void trackAtomicOperations() {
    activeAtomicOperations.clear();
    trackAtomicOperations = true;
  }

  @Override
  public void doNotTrackAtomicOperations() {
    trackAtomicOperations = false;
    activeAtomicOperations.clear();
  }

  @Override
  public String dumpActiveAtomicOperations() {
    if (!trackAtomicOperations)
      activeAtomicOperations.clear();

    final StringWriter writer = new StringWriter();
    writer.append("List of active atomic operations: \r\n");
    writer.append("------------------------------------------------------------------------------------------------\r\n");
    for (Map.Entry<OOperationUnitId, OPair<String, StackTraceElement[]>> entry : activeAtomicOperations.entrySet()) {
      writer.append("Operation unit id :").append(entry.getKey().toString()).append("\r\n");
      writer.append("Started at thread : ").append(entry.getValue().getKey()).append("\r\n");
      writer.append("Stack trace of method which started this operation : \r\n");

      StackTraceElement[] stackTraceElements = entry.getValue().getValue();
      for (int i = 1; i < stackTraceElements.length; i++) {
        writer.append("\tat ").append(stackTraceElements[i].toString()).append("\r\n");
      }

      writer.append("\r\n\r\n");
    }
    writer.append("-------------------------------------------------------------------------------------------------\r\n");
    return writer.toString();
  }

  private static final class FreezeParameters {
    private final String                      message;
    private final Class<? extends OException> exceptionClass;

    public FreezeParameters(String message, Class<? extends OException> exceptionClass) {
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

    public WaitingListNode(Thread item) {
      this.item = item;
    }

    public void waitTillAllLinksWillBeCreated() {
      try {
        linkLatch.await();
      } catch (InterruptedException e) {
        throw new OInterruptedException(
            "Thread was interrupted while was waiting for completion of 'waiting linked list' operation");
      }
    }
  }

  private boolean useWal() {
    if (writeAheadLog == null)
      return false;

    final OStorageTransaction storageTransaction = storage.getStorageTransaction();
    if (storageTransaction == null)
      return true;

    final OTransaction clientTx = storageTransaction.getClientTx();
    return clientTx == null || clientTx.isUsingLog();

  }
}
