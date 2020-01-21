package com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations;

import com.orientechnologies.common.concur.collection.CASObjectArray;
import com.orientechnologies.common.concur.lock.ScalableRWLock;

import java.util.concurrent.ConcurrentHashMap;

public class AtomicOperationsTable {
  private static final OperationInformation ATOMIC_OPERATION_STATUS_PLACE_HOLDER = new OperationInformation(
      AtomicOperationStatus.NOT_STARTED, -1);

  private long                                 idOffset;
  private CASObjectArray<OperationInformation> table;

  private final ScalableRWLock compactionLock = new ScalableRWLock();

  private final    int     tableCompactionLimit;
  private volatile boolean scheduleTableCompaction;

  private final ConcurrentHashMap<Long, StackTraceElement[]> idTxMap = new ConcurrentHashMap<>();

  public AtomicOperationsTable(final int tableCompactionLimit, final long idOffset) {
    this.tableCompactionLimit = tableCompactionLimit;
    this.idOffset = idOffset;
    table = new CASObjectArray<>();
  }

  public void startOperation(final long operationId, final long segment) {
    if (scheduleTableCompaction) {
      compactTable();
    }

    compactionLock.sharedLock();
    try {
      final int itemIndex = (int) (operationId - idOffset);
      if (itemIndex < 0) {
        throw new IllegalStateException("Invalid state of table of atomic operations");
      }

      table.set(itemIndex, new OperationInformation(AtomicOperationStatus.IN_PROGRESS, segment),
          ATOMIC_OPERATION_STATUS_PLACE_HOLDER);

      scheduleTableCompactionIfNeeded();
      idTxMap.put(operationId, Thread.currentThread().getStackTrace());
    } finally {
      compactionLock.sharedUnlock();
    }
  }

  private void scheduleTableCompactionIfNeeded() {
    if (table.size() >= tableCompactionLimit) {
      if (!scheduleTableCompaction) {
        scheduleTableCompaction = true;
      }
    }
  }

  public void commitOperation(final long operationId) {
    if (scheduleTableCompaction) {
      compactTable();
    }

    compactionLock.sharedLock();
    try {
      final int itemIndex = (int) (operationId - idOffset);
      if (itemIndex < 0) {
        throw new IllegalStateException("Invalid state of table of atomic operations");
      }

      final OperationInformation currentInformation = table.get(itemIndex);
      if (currentInformation.status != AtomicOperationStatus.IN_PROGRESS) {
        throw new IllegalStateException("Invalid state of table of atomic operations");
      }

      if (!table.compareAndSet(itemIndex, currentInformation,
          new OperationInformation(AtomicOperationStatus.COMMITTED, currentInformation.segment))) {
        throw new IllegalStateException("Invalid state of table of atomic operations");
      }

      scheduleTableCompactionIfNeeded();

      idTxMap.remove(operationId);
    } finally {
      compactionLock.sharedUnlock();
    }
  }

  public void rollbackOperation(final long operationId) {
    if (scheduleTableCompaction) {
      compactTable();
    }

    compactionLock.sharedLock();
    try {
      final int itemIndex = (int) (operationId - idOffset);
      if (itemIndex < 0) {
        throw new IllegalStateException("Invalid state of table of atomic operations");
      }

      final OperationInformation currentInformation = table.get(itemIndex);
      if (currentInformation.status != AtomicOperationStatus.IN_PROGRESS) {
        throw new IllegalStateException("Invalid state of table of atomic operations");
      }

      if (!table.compareAndSet(itemIndex, currentInformation,
          new OperationInformation(AtomicOperationStatus.ROLLED_BACK, currentInformation.segment))) {
        throw new IllegalStateException("Invalid state of table of atomic operations");
      }

      scheduleTableCompactionIfNeeded();
      idTxMap.remove(operationId);
    } finally {
      compactionLock.sharedUnlock();
    }
  }

  public long getSegmentEarliestOperationInProgress() {
    compactionLock.sharedLock();
    try {

      final int size = table.size();
      for (int i = 0; i < size; i++) {
        final OperationInformation operationInformation = table.get(i);
        if (operationInformation.status == AtomicOperationStatus.IN_PROGRESS) {
          return operationInformation.segment;
        }
      }
    } finally {
      compactionLock.sharedUnlock();
    }

    return -1;
  }

  public void compactTable() {
    compactionLock.exclusiveLock();
    try {
      final CASObjectArray<OperationInformation> newTable = new CASObjectArray<>();
      final int tableSize = table.size();
      boolean addition = false;

      long newIdOffset = -1;
      for (int i = 0; i < tableSize; i++) {
        final OperationInformation operationInformation = table.get(i);
        if (!addition) {
          if (operationInformation.status == AtomicOperationStatus.IN_PROGRESS
              || operationInformation.status == AtomicOperationStatus.NOT_STARTED) {
            addition = true;

            newIdOffset = i + idOffset;
            newTable.add(operationInformation);
          }
        } else {
          newTable.add(operationInformation);
        }
      }

      if (newIdOffset < 0) {
        newIdOffset = idOffset + tableSize;
      }

      this.table = newTable;
      this.idOffset = newIdOffset;
    } finally {
      compactionLock.exclusiveUnlock();
    }
  }

  private static final class OperationInformation {
    private final AtomicOperationStatus status;
    private final long                  segment;

    private OperationInformation(AtomicOperationStatus status, long segment) {
      this.status = status;
      this.segment = segment;
    }
  }
}
