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
import com.orientechnologies.common.concur.lock.OLockException;
import com.orientechnologies.common.concur.lock.OOneEntryPerKeyLockManager;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.function.TxConsumer;
import com.orientechnologies.common.function.TxFunction;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.OOrientListenerAbstract;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.exception.OStorageExistsException;
import com.orientechnologies.orient.core.storage.cache.OReadCache;
import com.orientechnologies.orient.core.storage.cache.OWriteCache;
import com.orientechnologies.orient.core.storage.impl.local.AtomicOperationIdGen;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.operationsfreezer.OperationsFreezer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurableComponent;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWriteAheadLog;

import java.io.IOException;
import java.util.Iterator;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 12/3/13
 */
public class OAtomicOperationsManager {
    private static volatile ThreadLocal<OAtomicOperation> currentOperation = new ThreadLocal<>();

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

    private final OAbstractPaginatedStorage storage;
    private final OWriteAheadLog writeAheadLog;
    private final OOneEntryPerKeyLockManager<String> lockManager = new OOneEntryPerKeyLockManager<>(true, -1,
            OGlobalConfiguration.COMPONENTS_LOCK_CACHE.getValueAsInteger());
    private final OReadCache readCache;
    private final OWriteCache writeCache;

    private final Object segmentLock = new Object();
    private final AtomicOperationIdGen idGen;

    private final boolean trackPageOperations;
    private final int operationsCacheLimit;

    private final OperationsFreezer atomicOperationsFreezer = new OperationsFreezer();
    private final OperationsFreezer componentOperationsFreezer = new OperationsFreezer();
    private final AtomicOperationsTable atomicOperationsTable;

    public OAtomicOperationsManager(OAbstractPaginatedStorage storage, boolean trackPageOperations, int operationsCacheLimit,
                                    AtomicOperationsTable atomicOperationsTable) {
        this.storage = storage;
        this.writeAheadLog = storage.getWALInstance();
        this.readCache = storage.getReadCache();
        this.writeCache = storage.getWriteCache();

        this.trackPageOperations = trackPageOperations;
        this.operationsCacheLimit = operationsCacheLimit;
        this.idGen = storage.getIdGen();
        this.atomicOperationsTable = atomicOperationsTable;
    }

    public OAtomicOperation startAtomicOperation(final byte[] metadata) throws IOException {
        OAtomicOperation operation = currentOperation.get();
        if (operation != null) {
            throw new OStorageExistsException("Atomic operation already started");
        }

        atomicOperationsFreezer.startOperation();

        final OLogSequenceNumber lsn;

        final long activeSegment;
        final long unitId;

        //transaction id and id of active segment should grow synchronously to maintain correct size of WAL
        synchronized (segmentLock) {
            unitId = idGen.nextId();
            activeSegment = writeAheadLog.activeSegment();
        }

        atomicOperationsTable.startOperation(unitId, activeSegment);
        if (metadata != null) {
            lsn = writeAheadLog.logAtomicOperationStartRecord(true, unitId, metadata);
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

        checkReadOnlyConditions(operation);

        return operation;
    }

    private void checkReadOnlyConditions(OAtomicOperation operation) {
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
    }

    public <T> T calculateInsideAtomicOperation(final byte[] metadata, final TxFunction<T> function) throws IOException {
        boolean rollback = false;
        final OAtomicOperation atomicOperation = startAtomicOperation(metadata);
        try {
            return function.accept(atomicOperation);
        } catch (Exception e) {
            rollback = true;
            throw OException.wrapException(
                    new OStorageException("Exception during execution of atomic operation inside of storage " + storage.getName()), e);
        } finally {
            endAtomicOperation(rollback);
        }
    }

    public void executeInsideAtomicOperation(final byte[] metadata, final TxConsumer consumer) throws IOException {
        boolean rollback = false;
        final OAtomicOperation atomicOperation = startAtomicOperation(metadata);
        try {
            consumer.accept(atomicOperation);
        } catch (Exception e) {
            rollback = true;
            throw OException.wrapException(
                    new OStorageException("Exception during execution of atomic operation inside of storage " + storage.getName()), e);
        } finally {
            endAtomicOperation(rollback);
        }
    }

    public void executeInsideComponentOperation(final OAtomicOperation atomicOperation, final ODurableComponent component,
                                                final TxConsumer consumer) {
        executeInsideComponentOperation(atomicOperation, component.getLockName(), consumer);
    }

    public void executeInsideComponentOperation(final OAtomicOperation atomicOperation, final String lockName,
                                                final TxConsumer consumer) {
        Objects.requireNonNull(atomicOperation);
        startComponentOperation(atomicOperation, lockName);
        try {
            consumer.accept(atomicOperation);
        } catch (Exception e) {
            throw OException.wrapException(
                    new OStorageException("Exception during execution of component operation inside of storage " + storage.getName()), e);
        } finally {
            endComponentOperation(atomicOperation);
        }
    }

    public boolean tryExecuteInsideComponentOperation(final OAtomicOperation atomicOperation, final ODurableComponent component,
                                                      final TxConsumer consumer) {
        return tryExecuteInsideComponentOperation(atomicOperation, component.getLockName(), consumer);
    }

    private boolean tryExecuteInsideComponentOperation(final OAtomicOperation atomicOperation, final String lockName,
                                                       final TxConsumer consumer) {
        Objects.requireNonNull(atomicOperation);
        final boolean result = tryStartComponentOperation(atomicOperation, lockName);
        if (!result) {
            return false;
        }

        try {
            consumer.accept(atomicOperation);
        } catch (Exception e) {
            throw OException.wrapException(
                    new OStorageException("Exception during execution of component operation inside of storage " + storage.getName()), e);
        } finally {
            endComponentOperation(atomicOperation);
        }

        return true;
    }

    public <T> T calculateInsideComponentOperation(final OAtomicOperation atomicOperation, final ODurableComponent component,
                                                   final TxFunction<T> function) {
        return calculateInsideComponentOperation(atomicOperation, component.getLockName(), function);
    }

    public <T> T calculateInsideComponentOperation(final OAtomicOperation atomicOperation, final String lockName,
                                                   final TxFunction<T> function) {
        Objects.requireNonNull(atomicOperation);
        startComponentOperation(atomicOperation, lockName);
        try {
            return function.accept(atomicOperation);
        } catch (Exception e) {
            throw OException.wrapException(
                    new OStorageException("Exception during execution of component operation inside of storage " + storage.getName()), e);
        } finally {
            endComponentOperation(atomicOperation);
        }
    }

    private void startComponentOperation(final OAtomicOperation atomicOperation, final String lockName) {
        acquireExclusiveLockTillOperationComplete(atomicOperation, lockName);
        checkReadOnlyConditions(atomicOperation);
        atomicOperation.incrementComponentOperations();

        componentOperationsFreezer.startOperation();
    }

    private void endComponentOperation(final OAtomicOperation atomicOperation) {
        atomicOperation.decrementComponentOperations();

        componentOperationsFreezer.endOperation();
    }

    public long freezeComponentOperations() {
        return componentOperationsFreezer.freezeOperations(null, null);
    }

    public void releaseComponentOperations(final long freezeId) {
        componentOperationsFreezer.releaseOperations(freezeId);
    }

    private boolean tryStartComponentOperation(final OAtomicOperation atomicOperation, final String lockName) {
        final boolean result = tryAcquireExclusiveLockTillOperationComplete(atomicOperation, lockName);
        if (!result) {
            return false;
        }

        checkReadOnlyConditions(atomicOperation);
        atomicOperation.incrementComponentOperations();
        return true;
    }

    private boolean tryAcquireExclusiveLockTillOperationComplete(OAtomicOperation operation, String lockName) {
        if (operation.containsInLockedObjects(lockName)) {
            return true;
        }

        try {
            lockManager.acquireLock(lockName, OOneEntryPerKeyLockManager.LOCK.EXCLUSIVE, 1);
        } catch (OLockException e) {
            return false;
        }
        checkReadOnlyConditions(operation);
        operation.addLockedObject(lockName);

        componentOperationsFreezer.startOperation();
        return true;
    }

    public static void alarmClearOfAtomicOperation() {
        final OAtomicOperation current = currentOperation.get();

        if (current != null) {
            currentOperation.set(null);
        }
    }

    public long freezeAtomicOperations(Class<? extends OException> exceptionClass, String message) {
        return atomicOperationsFreezer.freezeOperations(exceptionClass, message);
    }

    public void releaseAtomicOperations(long id) {
        atomicOperationsFreezer.releaseOperations(id);
    }

    public static OAtomicOperation getCurrentOperation() {
        return currentOperation.get();
    }

    /**
     * Ends the current atomic operation on this manager.
     *
     * @param rollback {@code true} to indicate a rollback, {@code false} for successful commit.
     */
    public void endAtomicOperation(boolean rollback) throws IOException {
        final OAtomicOperation operation = currentOperation.get();

        if (operation == null) {
            OLogManager.instance().error(this, "There is no atomic operation active", null);
            throw new ODatabaseException("There is no atomic operation active");
        }

        try {
            if (rollback) {
                operation.rollbackInProgress();
            }

            try {
                final OLogSequenceNumber lsn;
                if (trackPageOperations) {
                    lsn = operation.commitChanges(writeAheadLog);
                } else if (!operation.isRollbackInProgress()) {
                    lsn = operation.commitChanges(writeAheadLog);
                } else {
                    lsn = null;
                }

                final long operationId = operation.getOperationUnitId();
                if (rollback) {
                    atomicOperationsTable.rollbackOperation(operationId);
                } else {
                    atomicOperationsTable.commitOperation(operationId);
                    writeAheadLog.addEventAt(lsn, () -> atomicOperationsTable.persistOperation(operationId));
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

        } catch (Error e) {
            final OAbstractPaginatedStorage st = storage;
            if (st != null) {
                st.handleJVMError(e);
            }

            throw e;
        } finally {
            atomicOperationsFreezer.endOperation();
        }

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
        private final String message;
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

        private final Thread item;
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
