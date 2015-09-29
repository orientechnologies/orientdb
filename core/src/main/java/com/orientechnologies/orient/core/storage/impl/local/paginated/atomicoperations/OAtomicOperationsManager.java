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

package com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations;

import com.orientechnologies.common.concur.lock.ODistributedCounter;
import com.orientechnologies.common.concur.lock.OLockManager;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.util.OPair;
import com.orientechnologies.orient.core.OOrientListenerAbstract;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.storage.cache.OReadCache;
import com.orientechnologies.orient.core.storage.cache.OWriteCache;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurableComponent;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.ONonTxOperationPerformedWALRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitId;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWriteAheadLog;

import javax.management.InstanceAlreadyExistsException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientechnologies.com)
 * @since 12/3/13
 */
public class OAtomicOperationsManager implements OAtomicOperationsMangerMXBean {
  public static final String                                              MBEAN_NAME             = "com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations:type=OAtomicOperationsMangerMXBean";

  private volatile boolean                                                trackAtomicOperations  = OGlobalConfiguration.TX_TRACK_ATOMIC_OPERATIONS
                                                                                                     .getValueAsBoolean();

  private final AtomicBoolean                                             mbeanIsRegistered      = new AtomicBoolean();

  private final ODistributedCounter                                       atomicOperationsCount  = new ODistributedCounter();

  private final AtomicInteger                                             freezeRequests         = new AtomicInteger();

  private final ConcurrentMap<Long, FreezeParameters>                     freezeParametersIdMap  = new ConcurrentHashMap<Long, FreezeParameters>();
  private final AtomicLong                                                freezeIdGen            = new AtomicLong();

  private final AtomicReference<WaitingListNode>                          waitingHead            = new AtomicReference<WaitingListNode>();
  private final AtomicReference<WaitingListNode>                          waitingTail            = new AtomicReference<WaitingListNode>();

  private static volatile ThreadLocal<OAtomicOperation>                   currentOperation       = new ThreadLocal<OAtomicOperation>();

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

  private final OAbstractPaginatedStorage                                 storage;
  private final OWriteAheadLog                                            writeAheadLog;
  private final OLockManager<String>                                      lockManager            = new OLockManager<String>(true,
                                                                                                     -1);
  private final OReadCache                                                readCache;
  private final OWriteCache                                               writeCache;

  private final Map<OOperationUnitId, OPair<String, StackTraceElement[]>> activeAtomicOperations = new ConcurrentHashMap<OOperationUnitId, OPair<String, StackTraceElement[]>>();

  public OAtomicOperationsManager(OAbstractPaginatedStorage storage) {
    this.storage = storage;
    this.writeAheadLog = storage.getWALInstance();
    this.readCache = storage.getReadCache();
    this.writeCache = storage.getWriteCache();
  }

  public OAtomicOperation startAtomicOperation(ODurableComponent durableComponent, boolean rollbackOnlyMode) throws IOException {
    if (durableComponent != null)
      return startAtomicOperation(durableComponent.getFullName(), rollbackOnlyMode);

    return startAtomicOperation((String) null, rollbackOnlyMode);
  }

  public OAtomicOperation startAtomicOperation(String fullName, boolean rollbackOnlyMode) throws IOException {
    if (writeAheadLog == null)
      return null;

    OAtomicOperation operation = currentOperation.get();
    if (operation != null) {
      operation.incrementCounter();

      if (fullName != null)
        acquireExclusiveLockTillOperationComplete(fullName);

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

    if (!rollbackOnlyMode)
      lsn = writeAheadLog.logAtomicOperationStartRecord(true, unitId);
    else
      lsn = null;

    operation = new OAtomicOperation(lsn, unitId, readCache, writeCache, storage.getId(), rollbackOnlyMode);
    currentOperation.set(operation);

    if (trackAtomicOperations) {
      final Thread thread = Thread.currentThread();
      activeAtomicOperations.put(unitId, new OPair<String, StackTraceElement[]>(thread.getName(), thread.getStackTrace()));
    }

    if (storage.getStorageTransaction() == null)
      writeAheadLog.log(new ONonTxOperationPerformedWALRecord());

    if (fullName != null)
      acquireExclusiveLockTillOperationComplete(fullName);

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

      if (head == tail) {
        return new WaitingListNode(head.item);
      }

      if (waitingHead.compareAndSet(head, tail)) {
        WaitingListNode node = head;

        while (node.next != tail) {
          node = node.next;
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

    while (atomicOperationsCount.get() > 0) {
      Thread.yield();
    }

    assert atomicOperationsCount.get() == 0;

    return id;
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
            OLogManager.instance().error(
                this,
                "Can not create instance of exception " + freezeParameters.exceptionClass
                    + " with message will try empty constructor instead", ie);
            throwFreezeExceptionWithoutMessage(freezeParameters);
          } catch (IllegalAccessException iae) {
            OLogManager.instance().error(
                this,
                "Can not create instance of exception " + freezeParameters.exceptionClass
                    + " with message will try empty constructor instead", iae);
            throwFreezeExceptionWithoutMessage(freezeParameters);
          } catch (NoSuchMethodException nsme) {
            OLogManager.instance().error(
                this,
                "Can not create instance of exception " + freezeParameters.exceptionClass
                    + " with message will try empty constructor instead", nsme);
            throwFreezeExceptionWithoutMessage(freezeParameters);
          } catch (SecurityException se) {
            OLogManager.instance().error(
                this,
                "Can not create instance of exception " + freezeParameters.exceptionClass
                    + " with message will try empty constructor instead", se);
            throwFreezeExceptionWithoutMessage(freezeParameters);
          } catch (InvocationTargetException ite) {
            OLogManager.instance().error(
                this,
                "Can not create instance of exception " + freezeParameters.exceptionClass
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
      OLogManager.instance().error(
          this,
          "Can not create instance of exception " + freezeParameters.exceptionClass
              + " will park thread instead of throwing of exception", ie);
    } catch (IllegalAccessException iae) {
      OLogManager.instance().error(
          this,
          "Can not create instance of exception " + freezeParameters.exceptionClass
              + " will park thread instead of throwing of exception", iae);
    }
  }

  public OAtomicOperation getCurrentOperation() {
    return currentOperation.get();
  }

  public OAtomicOperation endAtomicOperation(boolean rollback, Exception exception) throws IOException {
    if (writeAheadLog == null)
      return null;

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

    final int counter = operation.decrementCounter();
    assert counter >= 0;

    if (counter == 0) {
      if (!operation.isRollback())
        operation.commitChanges(writeAheadLog);

      if (!operation.isRollbackOnlyMode())
        writeAheadLog.logAtomicOperationEndRecord(operation.getOperationUnitId(), rollback, operation.getStartLSN());

      currentOperation.set(null);

      if (trackAtomicOperations) {
        activeAtomicOperations.remove(operation.getOperationUnitId());
      }

      for (String lockObject : operation.lockedObjects())
        lockManager.releaseLock(this, lockObject, OLockManager.LOCK.EXCLUSIVE);

      atomicOperationsCount.decrement();
    }

    return operation;
  }

  private void acquireExclusiveLockTillOperationComplete(String fullName) {
    final OAtomicOperation operation = currentOperation.get();
    if (operation == null)
      return;

    if (operation.containsInLockedObjects(fullName))
      return;

    lockManager.acquireLock(fullName, OLockManager.LOCK.EXCLUSIVE);
    operation.addLockedObject(fullName);
  }

  public void acquireReadLock(ODurableComponent durableComponent) {
    if (writeAheadLog == null)
      return;

    assert durableComponent.getName() != null;
    assert durableComponent.getFullName() != null;

    lockManager.acquireLock(durableComponent.getFullName(), OLockManager.LOCK.SHARED);
  }

  public void releaseReadLock(ODurableComponent durableComponent) {
    if (writeAheadLog == null)
      return;

    assert durableComponent.getName() != null;
    assert durableComponent.getFullName() != null;

    lockManager.releaseLock(this, durableComponent.getFullName(), OLockManager.LOCK.SHARED);
  }

  public void registerMBean() {
    if (mbeanIsRegistered.compareAndSet(false, true)) {
      try {
        final MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        final ObjectName mbeanName = new ObjectName(getMBeanName());
        if (!server.isRegistered(mbeanName))
          server.registerMBean(this, mbeanName);

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
    private final Thread             item;
    private volatile WaitingListNode next;

    public WaitingListNode(Thread item) {
      this.item = item;
    }
  }
}
