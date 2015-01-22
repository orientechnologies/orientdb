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
import com.orientechnologies.orient.core.OOrientShutdownListener;
import com.orientechnologies.orient.core.OOrientStartupListener;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
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
  private final OLockManager<Object, OAtomicOperationsManager> lockManager      = new OLockManager<Object, OAtomicOperationsManager>(
                                                                                    true, -1);

  public OAtomicOperationsManager(OAbstractPaginatedStorage storage) {
    this.storage = storage;
    this.writeAheadLog = storage.getWALInstance();
  }

  public OAtomicOperation startAtomicOperation() throws IOException {
    if (writeAheadLog == null)
      return null;

    OAtomicOperation operation = currentOperation.get();
    if (operation != null) {
      operation.incrementCounter();
      return operation;
    }

    final OOperationUnitId unitId = OOperationUnitId.generateId();
    final OLogSequenceNumber lsn = writeAheadLog.logAtomicOperationStartRecord(true, unitId);

    operation = new OAtomicOperation(lsn, unitId);
    currentOperation.set(operation);

    if (storage.getStorageTransaction() == null)
      writeAheadLog.log(new ONonTxOperationPerformedWALRecord());

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
      for (Object lockObject : operation.lockedObjects())
        lockManager.releaseLock(this, lockObject, OLockManager.LOCK.EXCLUSIVE);

      writeAheadLog.logAtomicOperationEndRecord(operation.getOperationUnitId(), rollback, operation.getStartLSN());
      currentOperation.set(null);
    }

    return operation;
  }

  public void lockTillOperationComplete(Object lockObject) {
    final OAtomicOperation operation = currentOperation.get();
    if (operation == null)
      return;

    if (operation.containsInLockedObjects(lockObject))
      return;

    lockManager.acquireLock(this, lockObject, OLockManager.LOCK.EXCLUSIVE);
    operation.addLockedObject(lockObject);
  }
}
