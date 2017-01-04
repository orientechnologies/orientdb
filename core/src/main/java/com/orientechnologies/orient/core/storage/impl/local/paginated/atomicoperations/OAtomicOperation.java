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

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.storage.cache.*;
import com.orientechnologies.orient.core.storage.cache.pages.OLruPageCache;
import com.orientechnologies.orient.core.storage.cache.pages.OPageCache;
import com.orientechnologies.orient.core.storage.cache.pages.OPassthroughPageCache;
import com.orientechnologies.orient.core.storage.cache.pages.OTinyPageCache;
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
  private static final int PAGE_CACHE_SIZE = OGlobalConfiguration.TX_PAGE_CACHE_SIZE.getValueAsInteger();

  private final int                storageId;
  private final OLogSequenceNumber startLSN;
  private final OOperationUnitId   operationUnitId;

  private int       startCounter;
  private boolean   rollback;
  private Exception rollbackException;

  private Set<String>            lockedObjects        = new HashSet<String>();
  private Map<Long, FileChanges> fileChanges          = new HashMap<Long, FileChanges>();
  private Map<String, Long>      newFileNamesId       = new HashMap<String, Long>();
  private Set<Long>              deletedFiles         = new HashSet<Long>();
  private Map<String, Long>      deletedFileNameIdMap = new HashMap<String, Long>();

  private OReadCache  readCache;
  private OWriteCache writeCache;

  private final OPerformanceStatisticManager performanceStatisticManager;

  private final Map<String, OAtomicOperationMetadata<?>> metadata = new LinkedHashMap<String, OAtomicOperationMetadata<?>>();

  private final OPageCache pageCache;

  public OAtomicOperation(OLogSequenceNumber startLSN, OOperationUnitId operationUnitId, OReadCache readCache,
      OWriteCache writeCache, int storageId, OPerformanceStatisticManager performanceStatisticManager) {
    this.storageId = storageId;
    this.startLSN = startLSN;
    this.operationUnitId = operationUnitId;
    this.performanceStatisticManager = performanceStatisticManager;
    startCounter = 1;
    this.readCache = readCache;
    this.writeCache = writeCache;
    this.pageCache = createPageCache(readCache);
  }

  public OLogSequenceNumber getStartLSN() {
    return startLSN;
  }

  public OOperationUnitId getOperationUnitId() {
    return operationUnitId;
  }

  public OCacheEntry loadPage(long fileId, long pageIndex, boolean checkPinnedPages, final int pageCount) throws IOException {
    assert pageCount > 0;

    fileId = checkFileIdCompatibilty(fileId, storageId);

    if (deletedFiles.contains(fileId))
      throw new OStorageException("File with id " + fileId + " is deleted.");

    FileChanges changesContainer = fileChanges.get(fileId);

    if (changesContainer == null) {
      changesContainer = new FileChanges();
      fileChanges.put(fileId, changesContainer);
    }

    if (changesContainer.isNew) {
      if (pageIndex <= changesContainer.maxNewPageIndex) {
        OCacheEntryChanges pageChange = changesContainer.pageChangesMap.get(pageIndex);
        return pageChange;
      } else
        return null;
    } else {
      OCacheEntryChanges pageChangesContainer = changesContainer.pageChangesMap.get(pageIndex);

      final long filledUpTo = internalFilledUpTo(fileId, changesContainer);

      if (pageIndex < filledUpTo) {
        if (pageChangesContainer == null) {
          pageChangesContainer = new OCacheEntryChanges();
          changesContainer.pageChangesMap.put(pageIndex, pageChangesContainer);
          if (pageChangesContainer.isNew) {
            OCacheEntry delegate = new OCacheEntryImpl(fileId, pageIndex,
                new OCachePointer(null, null, fileId, pageIndex), false);
            pageChangesContainer.delegate = delegate;
          }
        }

        if (pageChangesContainer.isNew) {
          return pageChangesContainer;
        } else {
          OCacheEntry delegate = pageCache.loadPage(fileId, pageIndex, checkPinnedPages, writeCache, pageCount);
          pageChangesContainer.delegate = delegate;
          return pageChangesContainer;
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

  public void pinPage(OCacheEntry cacheEntry) throws IOException {
    if (deletedFiles.contains(cacheEntry.getFileId()))
      throw new OStorageException("File with id " + cacheEntry.getFileId() + " is deleted.");

    final FileChanges changesContainer = fileChanges.get(cacheEntry.getFileId());
    assert changesContainer != null;

    final OCacheEntryChanges pageChangesContainer = changesContainer.pageChangesMap.get(cacheEntry.getPageIndex());
    assert pageChangesContainer != null;

    pageChangesContainer.pinPage = true;
  }

  public OCacheEntry addPage(long fileId) throws IOException {
    fileId = checkFileIdCompatibilty(fileId, storageId);

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
    OCacheEntry delegate = new OCacheEntryImpl(fileId, filledUpTo,
        new OCachePointer(null, null, fileId, filledUpTo), false);
    pageChangesContainer.delegate = delegate;
    return pageChangesContainer;
  }

  public void releasePage(OCacheEntry cacheEntry) {
    OCacheEntryChanges real = (OCacheEntryChanges) cacheEntry;
    if (deletedFiles.contains(cacheEntry.getFileId()))
      throw new OStorageException("File with id " + cacheEntry.getFileId() + " is deleted.");

    if (cacheEntry.getCachePointer().getExclusiveBuffer() != null)
      pageCache.releasePage(real.getDelegate(), writeCache);
    else {
      assert real.isNew || !cacheEntry.isLockAcquiredByCurrentThread();
    }
  }

  public long filledUpTo(long fileId) throws IOException {
    fileId = checkFileIdCompatibilty(fileId, storageId);

    if (deletedFiles.contains(fileId))
      throw new OStorageException("File with id " + fileId + " is deleted.");

    FileChanges changesContainer = fileChanges.get(fileId);

    return internalFilledUpTo(fileId, changesContainer);
  }

  private long internalFilledUpTo(long fileId, FileChanges changesContainer) throws IOException {
    if (changesContainer == null) {
      changesContainer = new FileChanges();
      fileChanges.put(fileId, changesContainer);
    } else if (changesContainer.isNew || changesContainer.maxNewPageIndex > -2) {
      return changesContainer.maxNewPageIndex + 1;
    } else if (changesContainer.truncate)
      return 0;

    return writeCache.getFilledUpTo(fileId);
  }

  public long addFile(String fileName) throws IOException {
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

    FileChanges fileChanges = this.fileChanges.get(fileId);
    if (fileChanges == null) {
      fileChanges = new FileChanges();
      this.fileChanges.put(fileId, fileChanges);
    }

    return fileId;
  }

  public void deleteFile(long fileId) {
    fileId = checkFileIdCompatibilty(fileId, storageId);

    pageCache.releaseFilePages(fileId, writeCache);

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
    fileId = checkFileIdCompatibilty(fileId, storageId);

    if (fileChanges.containsKey(fileId))
      return true;

    if (deletedFiles.contains(fileId))
      return false;

    return writeCache.exists(fileId);
  }

  public String fileNameById(long fileId) {
    fileId = checkFileIdCompatibilty(fileId, storageId);

    FileChanges fileChanges = this.fileChanges.get(fileId);

    if (fileChanges != null && fileChanges.fileName != null)
      return fileChanges.fileName;

    if (deletedFiles.contains(fileId))
      throw new OStorageException("File with id " + fileId + " was deleted.");

    return writeCache.fileNameById(fileId);
  }

  public void truncateFile(long fileId) {
    fileId = checkFileIdCompatibilty(fileId, storageId);

    pageCache.releaseFilePages(fileId, writeCache);

    FileChanges fileChanges = this.fileChanges.get(fileId);

    if (fileChanges == null) {
      fileChanges = new FileChanges();
      this.fileChanges.put(fileId, fileChanges);
    }

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
        }

        for (Map.Entry<Long, FileChanges> fileChangesEntry : fileChanges.entrySet()) {
          final FileChanges fileChanges = fileChangesEntry.getValue();
          final long fileId = fileChangesEntry.getKey();

          if (fileChanges.isNew)
            writeAheadLog.log(new OFileCreatedWALRecord(operationUnitId, fileChanges.fileName, fileId));
          else if (fileChanges.truncate)
            writeAheadLog.log(new OFileTruncatedWALRecord(operationUnitId, fileId));
          Iterator<Map.Entry<Long, OCacheEntryChanges>> filePageChangesIterator = fileChanges.pageChangesMap.entrySet().iterator();
          while (filePageChangesIterator.hasNext()) {
            Map.Entry<Long, OCacheEntryChanges> filePageChangesEntry = filePageChangesIterator.next();
            //I assume new pages have everytime changes
            if (filePageChangesEntry.getValue().changes.hasChanges()) {
              final long pageIndex = filePageChangesEntry.getKey();
              final OCacheEntryChanges filePageChanges = filePageChangesEntry.getValue();

              filePageChanges.lsn = writeAheadLog
                  .log(new OUpdatePageRecord(pageIndex, fileId, operationUnitId, filePageChanges.changes));
            } else
              filePageChangesIterator.remove();
          }
        }
      }

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

          OCacheEntry cacheEntry = filePageChanges.isNew ? null : pageCache.purgePage(fileId, pageIndex, writeCache);
          if (cacheEntry == null) {
            cacheEntry = readCache.load(fileId, pageIndex, true, writeCache, 1);
            if (cacheEntry == null) {
              assert filePageChanges.isNew;
              do {
                if (cacheEntry != null)
                  readCache.release(cacheEntry, writeCache);

                cacheEntry = readCache.allocateNewPage(fileId, writeCache);
              } while (cacheEntry.getPageIndex() != pageIndex);
            }
          }

          cacheEntry.acquireExclusiveLock();
          try {
            ODurablePage durablePage = new ODurablePage(cacheEntry);
            durablePage.restoreChanges(filePageChanges.changes);

            if (writeAheadLog != null)
              durablePage.setLsn(filePageChanges.lsn);

            if (filePageChanges.pinPage)
              readCache.pinPage(cacheEntry);

          } finally {
            cacheEntry.releaseExclusiveLock();
            readCache.release(cacheEntry, writeCache);
          }
        }
      }
    } finally {
      pageCache.reset(writeCache);

      if (sessionStoragePerformanceStatistic != null) {
        sessionStoragePerformanceStatistic.stopCommitTimer();
        sessionStoragePerformanceStatistic.completeComponentOperation();
      }
    }
  }

  void incrementCounter() {
    startCounter++;
  }

  int decrementCounter() {
    startCounter--;
    return startCounter;
  }

  int getCounter() {
    return startCounter;
  }

  void rollback(Exception e) {
    pageCache.reset(writeCache);
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

  protected OPageCache createPageCache(OReadCache readCache) {
    if (PAGE_CACHE_SIZE > 8)
      return new OLruPageCache(readCache, PAGE_CACHE_SIZE, Math.max(PAGE_CACHE_SIZE / 8, 4));
    if (PAGE_CACHE_SIZE > 0)
      return new OTinyPageCache(readCache, PAGE_CACHE_SIZE);
    return new OPassthroughPageCache(readCache);
  }

  private static class FileChanges {
    private Map<Long, OCacheEntryChanges> pageChangesMap  = new HashMap<Long, OCacheEntryChanges>();
    private long                          maxNewPageIndex = -2;
    private boolean                       isNew           = false;
    private boolean                       truncate        = false;
    private String                        fileName        = null;
  }

  private int storageId(long fileId) {
    return (int) (fileId >>> 32);
  }

  private long composeFileId(long fileId, int storageId) {
    return (((long) storageId) << 32) | fileId;
  }

  private long checkFileIdCompatibilty(long fileId, int storageId) {
    // indicates that storage has no it's own id.
    if (storageId == -1)
      return fileId;

    if (storageId(fileId) == 0) {
      return composeFileId(fileId, storageId);
    }

    return fileId;
  }
}
