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

import com.orientechnologies.common.directmemory.ODirectMemoryPointer;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cache.OCachePointer;
import com.orientechnologies.orient.core.storage.cache.OReadCache;
import com.orientechnologies.orient.core.storage.cache.OWriteCache;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 *
 * Note: all atomic operations methods are designed in context that all operations on single files will be wrapped in shared lock.
 *
 * @author Andrey Lomakin (a.lomakin-at-orientechnologies.com)
 * @since 12/3/13
 */
public class OAtomicOperation {
  private final int                storageId;
  private final OLogSequenceNumber startLSN;
  private final OOperationUnitId   operationUnitId;

  private int                      startCounter;
  private boolean                  rollback;
  private Exception                rollbackException;

  private Set<String>              lockedObjects        = new HashSet<String>();
  private Map<Long, FileChanges>   fileChanges          = new HashMap<Long, FileChanges>();
  private Map<String, Long>        newFileNamesId       = new HashMap<String, Long>();
  private Set<Long>                deletedFiles         = new HashSet<Long>();
  private Map<String, Long>        deletedFileNameIdMap = new HashMap<String, Long>();
  private OReadCache               readCache;
  private OWriteCache              writeCache;

  private final boolean            rollbackOnlyMode;

  public OAtomicOperation(OLogSequenceNumber startLSN, OOperationUnitId operationUnitId, OReadCache readCache,
      OWriteCache writeCache, int storageId, boolean rollbackOnlyMode) {
    this.storageId = storageId;
    this.startLSN = startLSN;
    this.operationUnitId = operationUnitId;
    startCounter = 1;
    this.readCache = readCache;
    this.writeCache = writeCache;
    this.rollbackOnlyMode = rollbackOnlyMode;
  }

  public boolean isRollbackOnlyMode() {
    return rollbackOnlyMode;
  }

  public OLogSequenceNumber getStartLSN() {
    return startLSN;
  }

  public OOperationUnitId getOperationUnitId() {
    return operationUnitId;
  }

  public OCacheEntry loadPage(long fileId, long pageIndex, boolean checkPinnedPages) throws IOException {
    fileId = checkFileIdCompatibilty(fileId, storageId);

    if (deletedFiles.contains(fileId))
      throw new OStorageException("File with id " + fileId + " is deleted.");

    FileChanges changesContainer = fileChanges.get(fileId);

    if (changesContainer == null) {
      changesContainer = new FileChanges();
      fileChanges.put(fileId, changesContainer);
    }

    FilePageChanges pageChangesContainer = changesContainer.pageChangesMap.get(pageIndex);

    if (changesContainer.isNew) {
      if (pageIndex <= changesContainer.maxNewPageIndex)
        return new OCacheEntry(fileId, pageIndex, new OCachePointer((ODirectMemoryPointer) null, new OLogSequenceNumber(-1, -1),
            fileId, pageIndex), false);
      else
        return null;
    } else {
      final long filledUpTo = filledUpTo(fileId);

      if (pageIndex < filledUpTo) {
        if (pageChangesContainer == null) {
          pageChangesContainer = new FilePageChanges();
          changesContainer.pageChangesMap.put(pageIndex, pageChangesContainer);
        }

        if (pageChangesContainer.isNew)
          return new OCacheEntry(fileId, pageIndex, new OCachePointer((ODirectMemoryPointer) null, new OLogSequenceNumber(-1, -1),
              fileId, pageIndex), false);
        else
          return readCache.load(fileId, pageIndex, checkPinnedPages, writeCache);
      }
    }

    return null;
  }

  public void pinPage(OCacheEntry cacheEntry) throws IOException {
    if (deletedFiles.contains(cacheEntry.getFileId()))
      throw new OStorageException("File with id " + cacheEntry.getFileId() + " is deleted.");

    final FileChanges changesContainer = fileChanges.get(cacheEntry.getFileId());
    assert changesContainer != null;

    final FilePageChanges pageChangesContainer = changesContainer.pageChangesMap.get(cacheEntry.getPageIndex());
    assert pageChangesContainer != null;

    pageChangesContainer.pinPage = true;
  }

  public OCacheEntry addPage(long fileId) throws IOException {
    fileId = checkFileIdCompatibilty(fileId, storageId);

    if (deletedFiles.contains(fileId))
      throw new OStorageException("File with id " + fileId + " is deleted.");

    final long filledUpTo = filledUpTo(fileId);

    final FileChanges changesContainer = fileChanges.get(fileId);
    assert changesContainer != null;

    FilePageChanges pageChangesContainer = changesContainer.pageChangesMap.get(filledUpTo);
    assert pageChangesContainer == null;

    pageChangesContainer = new FilePageChanges();
    pageChangesContainer.isNew = true;

    changesContainer.pageChangesMap.put(filledUpTo, pageChangesContainer);
    changesContainer.maxNewPageIndex = filledUpTo;

    return new OCacheEntry(fileId, filledUpTo, new OCachePointer((ODirectMemoryPointer) null, new OLogSequenceNumber(-1, -1),
        fileId, filledUpTo), false);
  }

  public void releasePage(OCacheEntry cacheEntry) {
    if (deletedFiles.contains(cacheEntry.getFileId()))
      throw new OStorageException("File with id " + cacheEntry.getFileId() + " is deleted.");

    if (cacheEntry.getCachePointer().getDataPointer() != null)
      readCache.release(cacheEntry, writeCache);
  }

  public OWALChangesTree getChangesTree(long fileId, long pageIndex) {
    fileId = checkFileIdCompatibilty(fileId, storageId);

    if (deletedFiles.contains(fileId))
      throw new OStorageException("File with id " + fileId + " is deleted.");

    final FileChanges changesContainer = fileChanges.get(fileId);
    assert changesContainer != null;

    final FilePageChanges pageChangesContainer = changesContainer.pageChangesMap.get(pageIndex);
    assert pageChangesContainer != null;

    return pageChangesContainer.changesTree;
  }

  public long filledUpTo(long fileId) throws IOException {
    fileId = checkFileIdCompatibilty(fileId, storageId);

    if (deletedFiles.contains(fileId))
      throw new OStorageException("File with id " + fileId + " is deleted.");

    FileChanges changesContainer = fileChanges.get(fileId);

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

  public long openFile(String fileName) throws IOException {
    Long fileId = newFileNamesId.get(fileName);

    if (fileId == null)
      fileId = readCache.openFile(fileName, writeCache);

    FileChanges fileChanges = this.fileChanges.get(fileId);
    if (fileChanges == null) {
      fileChanges = new FileChanges();
      this.fileChanges.put(fileId, fileChanges);
    }

    return fileId;
  }

  public void openFile(long fileId) throws IOException {
    fileId = checkFileIdCompatibilty(fileId, storageId);

    if (deletedFiles.contains(fileId))
      throw new OStorageException("File with id " + fileId + " is deleted.");

    FileChanges changesContainer = fileChanges.get(fileId);
    if (changesContainer == null || !changesContainer.isNew)
      readCache.openFile(fileId, writeCache);
  }

  public void deleteFile(long fileId) {
    fileId = checkFileIdCompatibilty(fileId, storageId);

    final FileChanges fileChanges = this.fileChanges.remove(fileId);
    if (fileChanges != null && fileChanges.fileName != null)
      newFileNamesId.remove(fileChanges.fileName);
    else {
      deletedFiles.add(fileId);
      deletedFileNameIdMap.put(writeCache.fileNameById(fileId), fileId);
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
    if (!rollbackOnlyMode) {
      for (long deletedFileId : deletedFiles) {
        writeAheadLog.log(new OFileDeletedWALRecord(operationUnitId, startLSN, deletedFileId));
      }

      for (Map.Entry<Long, FileChanges> fileChangesEntry : fileChanges.entrySet()) {
        final FileChanges fileChanges = fileChangesEntry.getValue();
        final long fileId = fileChangesEntry.getKey();

        if (fileChanges.isNew)
          writeAheadLog.log(new OFileCreatedWALRecord(operationUnitId, fileChanges.fileName, fileId, startLSN));
        else if (fileChanges.truncate)
          writeAheadLog.log(new OFileTruncatedWALRecord(operationUnitId, startLSN, fileId));

        for (Map.Entry<Long, FilePageChanges> filePageChangesEntry : fileChanges.pageChangesMap.entrySet()) {
          final long pageIndex = filePageChangesEntry.getKey();
          final FilePageChanges filePageChanges = filePageChangesEntry.getValue();

          filePageChanges.lsn = writeAheadLog.log(new OUpdatePageRecord(pageIndex, fileId, operationUnitId,
              filePageChanges.changesTree, startLSN));
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

      for (Map.Entry<Long, FilePageChanges> filePageChangesEntry : fileChanges.pageChangesMap.entrySet()) {
        final long pageIndex = filePageChangesEntry.getKey();
        final FilePageChanges filePageChanges = filePageChangesEntry.getValue();

        OCacheEntry cacheEntry = readCache.load(fileId, pageIndex, true, writeCache);
        if (cacheEntry == null) {
          assert filePageChanges.isNew;
          do {
            if (cacheEntry != null)
              readCache.release(cacheEntry, writeCache);

            cacheEntry = readCache.allocateNewPage(fileId, writeCache);
          } while (cacheEntry.getPageIndex() != pageIndex);
        }

        cacheEntry.acquireExclusiveLock();
        try {
          ODurablePage durablePage = new ODurablePage(cacheEntry, null);
          durablePage.restoreChanges(filePageChanges.changesTree);

          if (!rollbackOnlyMode)
            durablePage.setLsn(filePageChanges.lsn);

          if (filePageChanges.pinPage)
            readCache.pinPage(cacheEntry);

          readCache.release(cacheEntry, writeCache);
        } finally {
          cacheEntry.releaseExclusiveLock();
        }
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
    private Map<Long, FilePageChanges> pageChangesMap  = new HashMap<Long, FilePageChanges>();
    private long                       maxNewPageIndex = -2;
    private boolean                    isNew           = false;
    private boolean                    truncate        = false;
    private String                     fileName        = null;
  }

  private static class FilePageChanges {
    private OWALChangesTree    changesTree = new OWALChangesTree();
    private OLogSequenceNumber lsn         = null;
    private boolean            isNew       = false;
    private boolean            pinPage     = false;
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
