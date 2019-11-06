package com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations;

import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cache.OReadCache;
import com.orientechnologies.orient.core.storage.cache.OWriteCache;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitId;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWriteAheadLog;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.common.WriteableWALRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.PageOperationRecord;
import com.orientechnologies.orient.core.storage.index.sbtreebonsai.local.OBonsaiBucketPointer;

import java.io.IOException;
import java.util.*;

final class OAtomicOperationPageOperationsTracking implements OAtomicOperation {
  private final Map<String, OAtomicOperationMetadata<?>> metadata = new LinkedHashMap<>();

  private final OReadCache     readCache;
  private final OWriteCache    writeCache;
  private final OWriteAheadLog writeAheadLog;

  private final OOperationUnitId operationUnitId;

  private int startCounter = 1;

  private final Set<String> lockedObjects = new HashSet<>();

  private boolean rollbackInProgress;

  private List<OLogSequenceNumber>  pageOperationRefs        = new ArrayList<>();
  private List<PageOperationRecord> pageOperationCache       = new ArrayList<>();
  private long                      sizeSerializedOperations = 0;

  private final int operationsCacheLimit;

  /**
   * Pointers to ridbags deleted during current transaction. We can not reuse pointers if we delete ridbag and then  create new one
   * inside of the same transaction.
   */
  private final Set<OBonsaiBucketPointer> deletedBonsaiPointers = new HashSet<>();

  private final OLogSequenceNumber startLSN;

  private final Map<ORawPair<Integer, Integer>, Set<Integer>> deletedRecordPositions = new HashMap<>();

  OAtomicOperationPageOperationsTracking(OReadCache readCache, OWriteCache writeCache, OWriteAheadLog writeAheadLog,
      OOperationUnitId operationUnitId, int operationsCacheLimit, OLogSequenceNumber startLSN) {
    this.readCache = readCache;
    this.writeCache = writeCache;
    this.operationUnitId = operationUnitId;
    this.writeAheadLog = writeAheadLog;
    this.operationsCacheLimit = operationsCacheLimit;
    this.startLSN = startLSN;
  }

  @Override
  public OCacheEntry loadPageForWrite(long fileId, long pageIndex, boolean checkPinnedPages, int pageCount, boolean verifyChecksum)
      throws IOException {
    return readCache.loadForWrite(fileId, pageIndex, checkPinnedPages, writeCache, verifyChecksum, null);
  }

  @Override
  public OCacheEntry loadPageForRead(long fileId, long pageIndex, boolean checkPinnedPages, int pageCount) throws IOException {
    return readCache.loadForRead(fileId, pageIndex, checkPinnedPages, writeCache, true);
  }

  @Override
  public void releasePageFromRead(final OCacheEntry cacheEntry) {
    readCache.releaseFromRead(cacheEntry, writeCache);
  }

  @Override
  public void releasePageFromWrite(final OCacheEntry cacheEntry) throws IOException {
    final List<PageOperationRecord> pageOperationRecords = cacheEntry.getPageOperations();

    OLogSequenceNumber lastLSN = null;
    for (final PageOperationRecord pageOperationRecord : pageOperationRecords) {
      pageOperationRecord.setOperationUnitId(operationUnitId);
      pageOperationRecord.setFileId(cacheEntry.getFileId());
      pageOperationRecord.setPageIndex(cacheEntry.getPageIndex());

      final OLogSequenceNumber lsn = writeAheadLog.log(pageOperationRecord);
      pageOperationRefs.add(lsn);

      sizeSerializedOperations += pageOperationRecord.serializedSize();
      lastLSN = lsn;

      if (sizeSerializedOperations <= operationsCacheLimit) {
        pageOperationCache.add(pageOperationRecord);
      } else {
        pageOperationCache.clear();
      }

    }

    if (lastLSN != null) {
      ODurablePage.setPageLSN(lastLSN, cacheEntry);
      cacheEntry.setEndLSN(lastLSN);
    }

    readCache.releaseFromWrite(cacheEntry, writeCache, true);
  }

  @Override
  public OCacheEntry addPage(long fileId) throws IOException {
    return readCache.allocateNewPage(fileId, writeCache, null);
  }

  @Override
  public long filledUpTo(long fileId) {
    return writeCache.getFilledUpTo(fileId);
  }

  @Override
  public long addFile(String fileName) throws IOException {
    return readCache.addFile(fileName, writeCache);
  }

  @Override
  public long loadFile(String fileName) throws IOException {
    return writeCache.loadFile(fileName);
  }

  @Override
  public void deleteFile(long fileId) throws IOException {
    readCache.deleteFile(fileId, writeCache);
  }

  @Override
  public boolean isFileExists(String fileName) {
    return writeCache.exists(fileName);
  }

  @Override
  public String fileNameById(long fileId) {
    return writeCache.fileNameById(fileId);
  }

  @Override
  public void truncateFile(long fileId) throws IOException {
    readCache.truncateFile(fileId, writeCache);
  }

  @Override
  public int getCounter() {
    return startCounter;
  }

  @Override
  public void incrementCounter() {
    startCounter++;
  }

  @Override
  public void decrementCounter() {
    startCounter--;
  }

  @Override
  public void addDeletedRidBag(OBonsaiBucketPointer rootPointer) {
    deletedBonsaiPointers.add(rootPointer);
  }

  @Override
  public Set<OBonsaiBucketPointer> getDeletedBonsaiPointers() {
    return deletedBonsaiPointers;
  }

  @Override
  public boolean containsInLockedObjects(String lockName) {
    return lockedObjects.contains(lockName);
  }

  @Override
  public void addLockedObject(String lockName) {
    lockedObjects.add(lockName);
  }

  @Override
  public Iterable<String> lockedObjects() {
    return lockedObjects;
  }

  @Override
  public void rollbackInProgress() {
    rollbackInProgress = true;
  }

  @Override
  public boolean isRollbackInProgress() {
    return rollbackInProgress;
  }

  @Override
  public OOperationUnitId getOperationUnitId() {
    return operationUnitId;
  }

  @Override
  public OLogSequenceNumber commitChanges(OWriteAheadLog writeAheadLog) throws IOException {
    if (rollbackInProgress) {
      if (!pageOperationCache.isEmpty()) {
        for (int i = pageOperationCache.size() - 1; i >= 0; i--) {
          final PageOperationRecord pageOperationRecord = pageOperationCache.get(i);
          revertPageOperation(pageOperationRecord);
        }
      } else if (!pageOperationRefs.isEmpty()) {
        final int chunkSize = 1_000;
        final List<PageOperationRecord> chunkToRevert = new ArrayList<>();

        int startIndex = pageOperationRefs.size() - chunkSize;
        int endIndex = pageOperationRefs.size();

        if (startIndex < 0) {
          startIndex = 0;
        }

        while (true) {
          List<WriteableWALRecord> walRecords = writeAheadLog.read(pageOperationRefs.get(startIndex), chunkSize);

          int recordsRead = 0;
          while (true) {
            for (final WriteableWALRecord walRecord : walRecords) {
              final int index = recordsRead + startIndex;

              if (startIndex + recordsRead < endIndex) {
                if (walRecord.getLsn().equals(pageOperationRefs.get(index))) {
                  chunkToRevert.add((PageOperationRecord) walRecord);
                  recordsRead++;
                }
              } else {
                break;
              }
            }

            if (recordsRead < endIndex - startIndex) {
              walRecords = writeAheadLog.read(pageOperationRefs.get(recordsRead + startIndex), chunkSize);
            } else {
              break;
            }
          }

          Collections.reverse(chunkToRevert);

          for (final PageOperationRecord pageOperationRecord : chunkToRevert) {
            revertPageOperation(pageOperationRecord);
          }

          chunkToRevert.clear();

          if (startIndex == 0) {
            break;
          }

          endIndex = startIndex;
          startIndex = startIndex - recordsRead;

          if (startIndex < 0) {
            startIndex = 0;
          }
        }
      }
    }

    return writeAheadLog.logAtomicOperationEndRecord(getOperationUnitId(), rollbackInProgress, this.startLSN, getMetadata());
  }

  private void revertPageOperation(PageOperationRecord pageOperationRecord) throws IOException {
    OLogSequenceNumber lastLSN = null;
    final OCacheEntry cacheEntry = readCache
        .loadForWrite(pageOperationRecord.getFileId(), pageOperationRecord.getPageIndex(), false, writeCache, true, null);
    try {
      pageOperationRecord.undo(cacheEntry);

      final List<PageOperationRecord> rollbackOperationRecords = cacheEntry.getPageOperations();
      for (final PageOperationRecord rollbackOperationRecord : rollbackOperationRecords) {

        rollbackOperationRecord.setOperationUnitId(operationUnitId);
        rollbackOperationRecord.setFileId(cacheEntry.getFileId());
        rollbackOperationRecord.setPageIndex(cacheEntry.getPageIndex());

        lastLSN = writeAheadLog.log(rollbackOperationRecord);
      }

      if (lastLSN != null) {
        ODurablePage.setPageLSN(lastLSN, cacheEntry);
        cacheEntry.setEndLSN(lastLSN);
      }
    } finally {
      readCache.releaseFromWrite(cacheEntry, writeCache, true);
    }
  }

  /**
   * Add metadata with given key inside of atomic operation. If metadata with the same key insist inside of atomic operation it will
   * be overwritten.
   *
   * @param metadata Metadata to add.
   *
   * @see OAtomicOperationMetadata
   */
  @Override
  public void addMetadata(final OAtomicOperationMetadata<?> metadata) {
    this.metadata.put(metadata.getKey(), metadata);
  }

  /**
   * @param key Key of metadata which is looking for.
   *
   * @return Metadata by associated key or <code>null</code> if such metadata is absent.
   */
  @Override
  public OAtomicOperationMetadata<?> getMetadata(final String key) {
    return metadata.get(key);
  }

  /**
   * @return All keys and associated metadata contained inside of atomic operation
   */
  private Map<String, OAtomicOperationMetadata<?>> getMetadata() {
    return Collections.unmodifiableMap(metadata);
  }

  @Override
  public void addDeletedRecordPosition(int clusterId, int pageIndex, int recordPosition) {
    final ORawPair<Integer, Integer> key = new ORawPair<>(clusterId, pageIndex);
    final Set<Integer> recordPositions = deletedRecordPositions.computeIfAbsent(key, k -> new HashSet<>());
    recordPositions.add(recordPosition);
  }

  @Override
  public Set<Integer> getBookedRecordPositions(int clusterId, int pageIndex) {
    return deletedRecordPositions.getOrDefault(new ORawPair<>(clusterId, pageIndex), Collections.emptySet());
  }
}
