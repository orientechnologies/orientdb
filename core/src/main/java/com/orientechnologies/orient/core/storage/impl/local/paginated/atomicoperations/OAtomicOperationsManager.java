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
import com.orientechnologies.orient.core.OOrientListenerAbstract;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurableComponent;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.*;

import java.io.IOException;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientechnologies.com)
 * @since 12/3/13
 */
public class OAtomicOperationsManager {
  private static volatile ThreadLocal<OAtomicOperation>        currentOperation = new ThreadLocal<OAtomicOperation>();

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

  private final OAbstractPaginatedStorage                      storage;
  private final OWriteAheadLog                                 writeAheadLog;
  private final OLockManager<String, OAtomicOperationsManager> lockManager      = new OLockManager<String, OAtomicOperationsManager>(
                                                                                    true, -1);

  public OAtomicOperationsManager(OAbstractPaginatedStorage storage) {
    this.storage = storage;
    this.writeAheadLog = storage.getWALInstance();
  }

  public OAtomicOperation startAtomicOperation(ODurableComponent durableComponent, boolean rollbackOnlyMode) throws IOException {
    return startAtomicOperation(durableComponent.getFullName(), rollbackOnlyMode);
  }

  public OAtomicOperation startAtomicOperation(String component, boolean rollbackOnlyMode) throws IOException {
    if (writeAheadLog == null)
      return null;

    OAtomicOperation operation = currentOperation.get();
    if (operation != null) {
      operation.incrementCounter();

      if (component != null)
        acquireExclusiveLockTillOperationComplete(component);

      return operation;
    }

    final OOperationUnitId unitId = OOperationUnitId.generateId();

    final OLogSequenceNumber lsn;
    if (!rollbackOnlyMode)
      lsn = writeAheadLog.logAtomicOperationStartRecord(true, unitId);
    else
      lsn = null;

    operation = new OAtomicOperation(lsn, unitId, rollbackOnlyMode);
    currentOperation.set(operation);

    if (storage.getStorageTransaction() == null)
      writeAheadLog.log(new ONonTxOperationPerformedWALRecord());

    if (component != null)
      acquireExclusiveLockTillOperationComplete(component);

    return operation;
  }

  public OAtomicOperation getCurrentOperation() {
    return currentOperation.get();
  }

  public OAtomicOperation endAtomicOperation(boolean rollback) throws IOException {
    if (writeAheadLog == null)
      return null;

    final OAtomicOperation operation = currentOperation.get();
    assert operation != null;

    if (rollback)
      operation.rollback();

    if (operation.isRollback() && !rollback)
      throw new ONestedRollbackException("Atomic operation was rolled back by internal component");

    final int counter = operation.decrementCounter();
    assert counter >= 0;

    if (counter == 0) {
      if (!operation.isRollback())
        operation.commitChanges(storage.getDiskCache(), writeAheadLog);

      if (!operation.isRollbackOnlyMode())
        writeAheadLog.logAtomicOperationEndRecord(operation.getOperationUnitId(), rollback, operation.getStartLSN());

      currentOperation.set(null);

      for (String lockObject : operation.lockedObjects())
        lockManager.releaseLock(this, lockObject, OLockManager.LOCK.EXCLUSIVE);
    }

    return operation;
  }

  private void acquireExclusiveLockTillOperationComplete(String durableComponent) {
    final OAtomicOperation operation = currentOperation.get();
    if (operation == null)
      return;

    assert durableComponent != null;

    if (operation.containsInLockedObjects(durableComponent))
      return;

    lockManager.acquireLock(this, durableComponent, OLockManager.LOCK.EXCLUSIVE);
    operation.addLockedObject(durableComponent);
  }

  public void acquireReadLock(ODurableComponent durableComponent) {
    if (writeAheadLog == null)
      return;

    assert durableComponent.getName() != null;
    assert durableComponent.getFullName() != null;

    lockManager.acquireLock(this, durableComponent.getFullName(), OLockManager.LOCK.SHARED);
  }

  public void releaseReadLock(ODurableComponent durableComponent) {
    if (writeAheadLog == null)
      return;

    assert durableComponent.getName() != null;
    assert durableComponent.getFullName() != null;

    lockManager.releaseLock(this, durableComponent.getFullName(), OLockManager.LOCK.SHARED);
  }
}
