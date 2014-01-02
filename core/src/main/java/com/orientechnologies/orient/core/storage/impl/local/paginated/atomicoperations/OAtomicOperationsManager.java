/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations;

import com.orientechnologies.common.concur.lock.OLockManager;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.*;

import java.io.IOException;
import java.util.concurrent.ConcurrentMap;

/**
 * @author Andrey Lomakin <a href="mailto:lomakin.andrey@gmail.com">Andrey Lomakin</a>
 * @since 12/3/13
 */
public class OAtomicOperationsManager {
  private static final ThreadLocal<OAtomicOperation>           currentOperation = new ThreadLocal<OAtomicOperation>();
  private final OWriteAheadLog                                 writeAheadLog;
  private final OLockManager<Object, OAtomicOperationsManager> lockManager      = new OLockManager<Object, OAtomicOperationsManager>(
                                                                                    true, 300000);

  public OAtomicOperationsManager(OWriteAheadLog writeAheadLog) {
    this.writeAheadLog = writeAheadLog;
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
    final OLogSequenceNumber lsn = writeAheadLog.log(new OAtomicUnitStartRecord(true, unitId));

    operation = new OAtomicOperation(lsn, unitId);
    currentOperation.set(operation);

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

      writeAheadLog.log(new OAtomicUnitEndRecord(operation.getOperationUnitId(), rollback));
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
