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

import com.orientechnologies.common.concur.lock.OLockManager;
import com.orientechnologies.common.util.OPair;
import com.orientechnologies.orient.core.OOrientListenerAbstract;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.storage.cache.OReadCache;
import com.orientechnologies.orient.core.storage.OIdentifiableStorage;
import com.orientechnologies.orient.core.storage.cache.OWriteCache;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurableComponent;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.*;

import javax.management.*;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.String;
import java.lang.management.ManagementFactory;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientechnologies.com)
 * @since 12/3/13
 */
public class OAtomicOperationsManager implements OAtomicOperationsMangerMXBean {
  public static final String                                              MBEAN_NAME             = "com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations:type=OAtomicOperationsMangerMXBean";

  private volatile boolean                                                trackAtomicOperations  = OGlobalConfiguration.TX_TRACK_ATOMIC_OPERATIONS
                                                                                                     .getValueAsBoolean();
  private final AtomicBoolean                                             mbeanIsRegistered      = new AtomicBoolean();

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

      throw new ONestedRollbackException(writer.toString(), exception);
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
        server.registerMBean(this, mbeanName);
      } catch (MalformedObjectNameException e) {
        throw new OStorageException("Error during registration of atomic manager MBean.", e);
      } catch (InstanceAlreadyExistsException e) {
        throw new OStorageException("Error during registration of atomic manager MBean.", e);
      } catch (MBeanRegistrationException e) {
        throw new OStorageException("Error during registration of atomic manager MBean.", e);
      } catch (NotCompliantMBeanException e) {
        throw new OStorageException("Error during registration of atomic manager MBean.", e);
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
        throw new OStorageException("Error during unregistration of atomic manager MBean.", e);
      } catch (InstanceNotFoundException e) {
        throw new OStorageException("Error during unregistration of atomic manager MBean.", e);
      } catch (MBeanRegistrationException e) {
        throw new OStorageException("Error during unregistration of atomic manager MBean.", e);
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
}
