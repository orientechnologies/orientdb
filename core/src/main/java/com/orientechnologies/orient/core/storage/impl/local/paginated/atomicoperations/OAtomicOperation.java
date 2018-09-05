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

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.storage.cache.*;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.*;
import com.orientechnologies.orient.core.storage.impl.local.statistic.OPerformanceStatisticManager;
import com.orientechnologies.orient.core.storage.impl.local.statistic.OSessionStoragePerformanceStatistic;

import java.io.IOException;
import java.util.*;

/**
 * Note: all atomic operations methods are designed in context that all operations on single files will be wrapped in shared lock.
 *
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 12/3/13
 */
public class OAtomicOperation {

  private final int                storageId;
  private final OLogSequenceNumber startLSN;
  private final OOperationUnitId   operationUnitId;

  private int       startCounter;
  private boolean   rollback;
  private Exception rollbackException;

  private final Set<String>            lockedObjects        = new HashSet<>();
  private final Map<Long, FileChanges> fileChanges          = new HashMap<>();
  private final Map<String, Long>      newFileNamesId       = new HashMap<>();
  private final Set<Long>              deletedFiles         = new HashSet<>();
  private final Map<String, Long>      deletedFileNameIdMap = new HashMap<>();

  private final OReadCache  readCache;
  private final OWriteCache writeCache;

  private final OPerformanceStatisticManager performanceStatisticManager;

  private final Map<String, OAtomicOperationMetadata<?>> metadata = new LinkedHashMap<>();

  public OAtomicOperation(OLogSequenceNumber startLSN, OOperationUnitId operationUnitId, OReadCache readCache,
      OWriteCache writeCache, int storageId, OPerformanceStatisticManager performanceStatisticManager) {
    this.storageId = storageId;
    this.startLSN = startLSN;
    this.operationUnitId = operationUnitId;
    this.performanceStatisticManager = performanceStatisticManager;
    startCounter = 1;
    this.readCache = readCache;
    this.writeCache = writeCache;
  }

  public OLogSequenceNumber getStartLSN() {
    return startLSN;
  }

  public OOperationUnitId getOperationUnitId() {
    return operationUnitId;
  }

  public OCacheEntry loadPageForWrite(long fileId, long pageIndex, boolean checkPinnedPages, final int pageCount)
      throws IOException {
    assert pageCount > 0;

    fileId = checkFileIdCompatibility(fileId, storageId);

    if (deletedFiles.contains(fileId))
      throw new OStorageException("File with id " + fileId + " is deleted.");

    FileChanges changesContainer = fileChanges.computeIfAbsent(fileId, k -> new FileChanges());

    if (changesContainer.isNew) {
      if (pageIndex <= changesContainer.maxNewPageIndex) {
        OCacheEntryChanges pageChange = changesContainer.pageChangesMap.get(pageIndex);
        return pageChange;
      } else
        return null;
    } else {
      OCacheEntryChanges pageChangesContainer = changesContainer.pageChangesMap.get(pageIndex);

      if (checkChangesFilledUpTo(changesContainer, pageIndex)) {
        if (pageChangesContainer == null) {
          OCacheEntry delegate = readCache.loadForRead(fileId, pageIndex, checkPinnedPages, writeCache, pageCount, true);
          if (delegate != null) {
            pageChangesContainer = new OCacheEntryChanges();
            changesContainer.pageChangesMap.put(pageIndex, pageChangesContainer);
            pageChangesContainer.delegate = delegate;
            return pageChangesContainer;
          }
        } else {
          if (pageChangesContainer.isNew) {
            return pageChangesContainer;
          } else {
            // Need to load the page again from cache for locking reasons
            OCacheEntry delegate = readCache.loadForRead(fileId, pageIndex, checkPinnedPages, writeCache, pageCount, true);
            pageChangesContainer.delegate = delegate;
            return pageChangesContainer;
          }
        }
      }
    }
    return null;
  }

  public OCacheEntry loadPageForRead(long fileId, long pageIndex, boolean checkPinnedPages, final int pageCount)
      throws IOException {
    assert pageCount > 0;

    fileId = checkFileIdCompatibility(fileId, storageId);

    if (deletedFiles.contains(fileId))
      throw new OStorageException("File with id " + fileId + " is deleted.");

    FileChanges changesContainer = fileChanges.get(fileId);
    if (changesContainer == null) {
      return readCache.loadForRead(fileId, pageIndex, checkPinnedPages, writeCache, pageCount, true);
    }

    if (changesContainer.isNew) {
      if (pageIndex <= changesContainer.maxNewPageIndex) {
        OCacheEntryChanges pageChange = changesContainer.pageChangesMap.get(pageIndex);
        return pageChange;
      } else
        return null;
    } else {
      OCacheEntryChanges pageChangesContainer = changesContainer.pageChangesMap.get(pageIndex);

      if (checkChangesFilledUpTo(changesContainer, pageIndex)) {
        if (pageChangesContainer == null) {
          return readCache.loadForRead(fileId, pageIndex, checkPinnedPages, writeCache, pageCount, true);
        } else {
          if (pageChangesContainer.isNew) {
            return pageChangesContainer;
          } else {
            // Need to load the page again from cache for locking reasons
            OCacheEntry delegate = readCache.loadForRead(fileId, pageIndex, checkPinnedPages, writeCache, pageCount, true);
            pageChangesContainer.delegate = delegate;
            return pageChangesContainer;
          }
        }
      }
    }
    return null;
  }

  /**
   * Add metadata with given key inside of atomic operation. If metadata with the same key insist inside of atomic operation it will
   * be overwritten.
   *
   * @param metadata Metadata to add.
   *
   * @see OAtomicOperationMetadata
   */
  public void addMetadata(OAtomicOperationMetadata<?> metadata) {
    this.metadata.put(metadata.getKey(), metadata);
  }

  /**
   * @param key Key of metadata which is looking for.
   *
   * @return Metadata by associated key or <code>null</code> if such metadata is absent.
   */
  public OAtomicOperationMetadata<?> getMetadata(String key) {
    return metadata.get(key);
  }

  /**
   * @return All keys and associated metadata contained inside of atomic operation
   */
  public Map<String, OAtomicOperationMetadata<?>> getMetadata() {
    return Collections.unmodifiableMap(metadata);
  }

  public void pinPage(OCacheEntry cacheEntry) {
    if (deletedFiles.contains(cacheEntry.getFileId()))
      throw new OStorageException("File with id " + cacheEntry.getFileId() + " is deleted.");

    final FileChanges changesContainer = fileChanges.get(cacheEntry.getFileId());
    assert changesContainer != null;

    final OCacheEntryChanges pageChangesContainer = changesContainer.pageChangesMap.get(cacheEntry.getPageIndex());
    assert pageChangesContainer != null;

    pageChangesContainer.pinPage = true;
  }

  public OCacheEntry addPage(long fileId) {
    fileId = checkFileIdCompatibility(fileId, storageId);

    if (deletedFiles.contains(fileId))
      throw new OStorageException("File with id " + fileId + " is deleted.");

    final FileChanges changesContainer = fileChanges.get(fileId);
    assert changesContainer != null;

    final long filledUpTo = internalFilledUpTo(fileId, changesContainer);

    OCacheEntryChanges pageChangesContainer = changesContainer.pageChangesMap.get(filledUpTo);
    assert pageChangesContainer == null;

    pageChangesContainer = new OCacheEntryChanges();
    pageChangesContainer.isNew = true;

    changesContainer.pageChangesMap.put(filledUpTo, pageChangesContainer);
    changesContainer.maxNewPageIndex = filledUpTo;
    OCacheEntry delegate = new OCacheEntryImpl(fileId, filledUpTo, new OCachePointer(null, null, fileId, filledUpTo), false);
    pageChangesContainer.delegate = delegate;
    return pageChangesContainer;
  }

  public void releasePageFromRead(OCacheEntry cacheEntry) {
    if (cacheEntry instanceof OCacheEntryChanges) {
      releasePageFromWrite(cacheEntry);
    } else
      readCache.releaseFromRead(cacheEntry, writeCache);
  }

  public void releasePageFromWrite(OCacheEntry cacheEntry) {
    OCacheEntryChanges real = (OCacheEntryChanges) cacheEntry;
    if (deletedFiles.contains(cacheEntry.getFileId()))
      throw new OStorageException("File with id " + cacheEntry.getFileId() + " is deleted.");

    if (cacheEntry.getCachePointer().getBuffer() != null)
      readCache.releaseFromRead(real.getDelegate(), writeCache);
    else {
      assert real.isNew || !cacheEntry.isLockAcquiredByCurrentThread();
    }
  }

  public long filledUpTo(long fileId) {
    fileId = checkFileIdCompatibility(fileId, storageId);

    if (deletedFiles.contains(fileId))
      throw new OStorageException("File with id " + fileId + " is deleted.");

    FileChanges changesContainer = fileChanges.get(fileId);

    return internalFilledUpTo(fileId, changesContainer);
  }

  private long internalFilledUpTo(long fileId, FileChanges changesContainer) {
    if (changesContainer == null) {
      changesContainer = new FileChanges();
      fileChanges.put(fileId, changesContainer);
    } else if (changesContainer.isNew || changesContainer.maxNewPageIndex > -2) {
      return changesContainer.maxNewPageIndex + 1;
    } else if (changesContainer.truncate)
      return 0;

    return writeCache.getFilledUpTo(fileId);
  }

  /**
   * This check if a file was trimmed or trunked in the current atomic operation.
   *
   * @param changesContainer changes container to check
   * @param pageIndex        limit to check aginst the changes
   *
   * @return true if there are no changes or pageIndex still fit, false if the pageIndex do not fit anymore
   */
  private boolean checkChangesFilledUpTo(FileChanges changesContainer, long pageIndex) {
    if (changesContainer == null) {
      return true;
    } else if (changesContainer.isNew || changesContainer.maxNewPageIndex > -2) {
      return pageIndex < changesContainer.maxNewPageIndex + 1;
    } else if (changesContainer.truncate)
      return false;
    return true;
  }

  public long addFile(String fileName) {
    if (newFileNamesId.containsKey(fileName))
      throw new OStorageException("File with name " + fileName + " already exists.");

    final long fileId;
    final boolean isNew;

    if (deletedFileNameIdMap.containsKey(fileName)) {
      fileId = deletedFileNameIdMap.remove(fileName);
      deletedFiles.remove(fileId);

      isNew = false;
    } else {
      fileId = writeCache.bookFileId(fileName);
      isNew = true;
    }

    newFileNamesId.put(fileName, fileId);

    FileChanges fileChanges = new FileChanges();
    fileChanges.isNew = isNew;
    fileChanges.fileName = fileName;
    fileChanges.maxNewPageIndex = -1;

    this.fileChanges.put(fileId, fileChanges);

    return fileId;
  }

  public long loadFile(String fileName) throws IOException {
    Long fileId = newFileNamesId.get(fileName);

    if (fileId == null)
      fileId = writeCache.loadFile(fileName);

    this.fileChanges.computeIfAbsent(fileId, k -> new FileChanges());

    return fileId;
  }

  public void deleteFile(long fileId) {
    fileId = checkFileIdCompatibility(fileId, storageId);

    final FileChanges fileChanges = this.fileChanges.remove(fileId);
    if (fileChanges != null && fileChanges.fileName != null)
      newFileNamesId.remove(fileChanges.fileName);
    else {
      deletedFiles.add(fileId);
      final String f = writeCache.fileNameById(fileId);
      if (f != null)
        deletedFileNameIdMap.put(f, fileId);
    }
  }

  public boolean isFileExists(String fileName) {
    if (newFileNamesId.containsKey(fileName))
      return true;

    if (deletedFileNameIdMap.containsKey(fileName))
      return false;

    return writeCache.exists(fileName);
  }

  public boolean isFileExists(long fileId) {
    fileId = checkFileIdCompatibility(fileId, storageId);

    if (fileChanges.containsKey(fileId))
      return true;

    if (deletedFiles.contains(fileId))
      return false;

    return writeCache.exists(fileId);
  }

  public String fileNameById(long fileId) {
    fileId = checkFileIdCompatibility(fileId, storageId);

    FileChanges fileChanges = this.fileChanges.get(fileId);

    if (fileChanges != null && fileChanges.fileName != null)
      return fileChanges.fileName;

    if (deletedFiles.contains(fileId))
      throw new OStorageException("File with id " + fileId + " was deleted.");

    return writeCache.fileNameById(fileId);
  }

  public void truncateFile(long fileId) {
    fileId = checkFileIdCompatibility(fileId, storageId);

    FileChanges fileChanges = this.fileChanges.computeIfAbsent(fileId, k -> new FileChanges());

    fileChanges.pageChangesMap.clear();
    fileChanges.maxNewPageIndex = -1;

    if (fileChanges.isNew)
      return;

    fileChanges.truncate = true;
  }

  public void commitChanges(OWriteAheadLog writeAheadLog) throws IOException {
    final OSessionStoragePerformanceStatistic sessionStoragePerformanceStatistic = performanceStatisticManager
        .getSessionPerformanceStatistic();

    if (sessionStoragePerformanceStatistic != null) {
      sessionStoragePerformanceStatistic.startCommitTimer();
      sessionStoragePerformanceStatistic
          .startComponentOperation("atomic operation", OSessionStoragePerformanceStatistic.ComponentType.GENERAL);
    }

    try {
      if (writeAheadLog != null) {
        for (long deletedFileId : deletedFiles) {
          writeAheadLog.log(new OFileDeletedWALRecord(operationUnitId, deletedFileId));
          readCache.deleteFile(deletedFileId, writeCache);
        }

        for (Map.Entry<Long, FileChanges> fileChangesEntry : fileChanges.entrySet()) {
          final FileChanges fileChanges = fileChangesEntry.getValue();
          final long fileId = fileChangesEntry.getKey();

          if (fileChanges.isNew) {
            writeAheadLog.log(new OFileCreatedWALRecord(operationUnitId, fileChanges.fileName, fileId));
            readCache.addFile(fileChanges.fileName, newFileNamesId.get(fileChanges.fileName), writeCache);
          } else if (fileChanges.truncate) {
            OLogManager.instance().warn(this,
                "You performing truncate operation which is considered unsafe because can not be rolled back, "
                    + "as result data can be incorrectly restored after crash, this operation is not recommended to be used");
            readCache.truncateFile(fileId, writeCache);
          }

          Iterator<Map.Entry<Long, OCacheEntryChanges>> filePageChangesIterator = fileChanges.pageChangesMap.entrySet().iterator();
          while (filePageChangesIterator.hasNext()) {
            Map.Entry<Long, OCacheEntryChanges> filePageChangesEntry = filePageChangesIterator.next();

            if (filePageChangesEntry.getValue().changes.hasChanges()) {
              final long pageIndex = filePageChangesEntry.getKey();
              final OCacheEntryChanges filePageChanges = filePageChangesEntry.getValue();

              OCacheEntry cacheEntry = readCache.loadForWrite(fileId, pageIndex, true, writeCache, 1, true);
              if (cacheEntry == null) {
                assert filePageChanges.isNew;
                do {
                  if (cacheEntry != null)
                    readCache.releaseFromWrite(cacheEntry, writeCache);

                  cacheEntry = readCache.allocateNewPage(fileId, writeCache, true);
                } while (cacheEntry.getPageIndex() != pageIndex);
              }

              final OLogSequenceNumber originalPageLSN = ODurablePage
                  .getLogSequenceNumberFromPage(cacheEntry.getCachePointer().getBuffer());
              final OLogSequenceNumber changesLSN = writeAheadLog
                  .log(new OUpdatePageRecord(pageIndex, fileId, operationUnitId, filePageChanges.changes, originalPageLSN));

              try {
                ODurablePage durablePage = new ODurablePage(cacheEntry);
                durablePage.restoreChanges(filePageChanges.changes);
                durablePage.setLsn(changesLSN);

                if (filePageChanges.pinPage)
                  readCache.pinPage(cacheEntry);

              } finally {
                readCache.releaseFromWrite(cacheEntry, writeCache);
              }
            } else
              filePageChangesIterator.remove();
          }
        }
      } else {
        for (long deletedFileId : deletedFiles) {
          readCache.deleteFile(deletedFileId, writeCache);
        }

        for (Map.Entry<Long, FileChanges> fileChangesEntry : fileChanges.entrySet()) {
          final FileChanges fileChanges = fileChangesEntry.getValue();
          final long fileId = fileChangesEntry.getKey();

          if (fileChanges.isNew)
            readCache.addFile(fileChanges.fileName, newFileNamesId.get(fileChanges.fileName), writeCache);
          else if (fileChanges.truncate)
            readCache.truncateFile(fileId, writeCache);

          for (Map.Entry<Long, OCacheEntryChanges> filePageChangesEntry : fileChanges.pageChangesMap.entrySet()) {
            final OCacheEntryChanges filePageChanges = filePageChangesEntry.getValue();
            if (!filePageChanges.changes.hasChanges())
              continue;
            final long pageIndex = filePageChangesEntry.getKey();

            OCacheEntry cacheEntry = readCache.loadForWrite(fileId, pageIndex, true, writeCache, 1, true);
            if (cacheEntry == null) {
              assert filePageChanges.isNew;
              do {
                if (cacheEntry != null)
                  readCache.releaseFromWrite(cacheEntry, writeCache);

                cacheEntry = readCache.allocateNewPage(fileId, writeCache, true);
              } while (cacheEntry.getPageIndex() != pageIndex);
            }

            try {
              ODurablePage durablePage = new ODurablePage(cacheEntry);
              durablePage.restoreChanges(filePageChanges.changes);

              if (filePageChanges.pinPage)
                readCache.pinPage(cacheEntry);

            } finally {
              readCache.releaseFromWrite(cacheEntry, writeCache);
            }
          }
        }
      }
    } finally {
      if (sessionStoragePerformanceStatistic != null) {
        sessionStoragePerformanceStatistic.stopCommitTimer();
        sessionStoragePerformanceStatistic.completeComponentOperation();
      }
    }
  }

  void incrementCounter() {
    startCounter++;
  }

  void decrementCounter() {
    startCounter--;
  }

  int getCounter() {
    return startCounter;
  }

  void rollback(Exception e) {
    rollback = true;
    rollbackException = e;
  }

  Exception getRollbackException() {
    return rollbackException;
  }

  boolean isRollback() {
    return rollback;
  }

  void addLockedObject(String lockedObject) {
    lockedObjects.add(lockedObject);
  }

  boolean containsInLockedObjects(String objectToLock) {
    return lockedObjects.contains(objectToLock);
  }

  Iterable<String> lockedObjects() {
    return lockedObjects;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    OAtomicOperation operation = (OAtomicOperation) o;

    if (!operationUnitId.equals(operation.operationUnitId))
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    return operationUnitId.hashCode();
  }

  private static class FileChanges {
    private final Map<Long, OCacheEntryChanges> pageChangesMap  = new HashMap<>();
    private       long                          maxNewPageIndex = -2;
    private       boolean                       isNew           = false;
    private       boolean                       truncate        = false;
    private       String                        fileName        = null;
  }

  private int storageId(long fileId) {
    return (int) (fileId >>> 32);
  }

  private long composeFileId(long fileId, int storageId) {
    return (((long) storageId) << 32) | fileId;
  }

  private long checkFileIdCompatibility(long fileId, int storageId) {
    // indicates that storage has no it's own id.
    if (storageId == -1)
      return fileId;

    if (storageId(fileId) == 0) {
      return composeFileId(fileId, storageId);
    }

    return fileId;
  }
}
