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
import com.orientechnologies.orient.core.index.hashindex.local.cache.OCacheEntry;
import com.orientechnologies.orient.core.index.hashindex.local.cache.OCachePointer;
import com.orientechnologies.orient.core.index.hashindex.local.cache.ODiskCache;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitId;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWALChangesTree;

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
  private final OLogSequenceNumber        startLSN;
  private final OOperationUnitId          operationUnitId;

  private int                             startCounter;
  private boolean                         rollback;

  private Set<Object>                     lockedObjects    = new HashSet<Object>();
  private Map<Long, FileChangesContainer> fileChanges      = new HashMap<Long, FileChangesContainer>();
  private Map<String, Long>               newFileNamesId   = new HashMap<String, Long>();
  private Set<Long>                       deletedFiles     = new HashSet<Long>();
  private Set<String>                     deletedFileNames = new HashSet<String>();

  public OAtomicOperation(OLogSequenceNumber startLSN, OOperationUnitId operationUnitId) {
    this.startLSN = startLSN;
    this.operationUnitId = operationUnitId;
    startCounter = 1;
  }

  public OLogSequenceNumber getStartLSN() {
    return startLSN;
  }

  public OOperationUnitId getOperationUnitId() {
    return operationUnitId;
  }

  public OCacheEntry loadPage(long fileId, long pageIndex, ODiskCache diskCache, boolean checkPinnedPages) throws IOException {
    if (deletedFiles.contains(fileId))
      throw new OStorageException("File with id " + fileId + " is deleted.");

    FileChangesContainer changesContainer = fileChanges.get(fileId);

    if (changesContainer == null) {
      changesContainer = new FileChangesContainer();
      fileChanges.put(fileId, changesContainer);
    }

    FilePageChangesContainer pageChangesContainer = changesContainer.pageChangesMap.get(pageIndex);
    if (pageChangesContainer == null) {
      pageChangesContainer = new FilePageChangesContainer();
      changesContainer.pageChangesMap.put(pageIndex, pageChangesContainer);
    }

    if (changesContainer.isNew) {
      if (pageIndex <= changesContainer.maxNewPageIndex)
        return new OCacheEntry(fileId, pageIndex, new OCachePointer((ODirectMemoryPointer) null, new OLogSequenceNumber(-1, -1)),
            false);
      else
        return null;
    } else {
      final long filledUpTo = filledUpTo(fileId, diskCache);

      if (pageIndex < filledUpTo) {
        if (pageChangesContainer.isNew)
          return new OCacheEntry(fileId, pageIndex, new OCachePointer((ODirectMemoryPointer) null, new OLogSequenceNumber(-1, -1)),
              false);
        else
          return diskCache.load(fileId, pageIndex, checkPinnedPages);
      }
    }

    return null;
  }

  public void pinPage(OCacheEntry cacheEntry) throws IOException {
    if (deletedFiles.contains(cacheEntry.getFileId()))
      throw new OStorageException("File with id " + cacheEntry.getFileId() + " is deleted.");

    final FileChangesContainer changesContainer = fileChanges.get(cacheEntry.getFileId());
    assert changesContainer != null;

    final FilePageChangesContainer pageChangesContainer = changesContainer.pageChangesMap.get(cacheEntry.getPageIndex());
    assert pageChangesContainer != null;

    pageChangesContainer.pinPage = true;
  }

  public OCacheEntry addPage(long fileId, ODiskCache diskCache) throws IOException {
    if (deletedFiles.contains(fileId))
      throw new OStorageException("File with id " + fileId + " is deleted.");

    final long filledUpTo = filledUpTo(fileId, diskCache);

    final FileChangesContainer changesContainer = fileChanges.get(fileId);
    assert changesContainer != null;

    FilePageChangesContainer pageChangesContainer = changesContainer.pageChangesMap.get(filledUpTo);
    assert pageChangesContainer == null;

    pageChangesContainer = new FilePageChangesContainer();
    pageChangesContainer.isNew = true;

    changesContainer.pageChangesMap.put(filledUpTo, pageChangesContainer);
    changesContainer.maxNewPageIndex = filledUpTo;

    return new OCacheEntry(fileId, filledUpTo, new OCachePointer((ODirectMemoryPointer) null, new OLogSequenceNumber(-1, -1)),
        false);
  }

  public void releasePage(OCacheEntry cacheEntry, ODiskCache diskCache) {
    if (deletedFiles.contains(cacheEntry.getFileId()))
      throw new OStorageException("File with id " + cacheEntry.getFileId() + " is deleted.");

    if (cacheEntry.getCachePointer().getDataPointer() != null)
      diskCache.release(cacheEntry);
  }

  public OWALChangesTree getChangesTree(long fileId, long pageIndex) {
    if (deletedFiles.contains(fileId))
      throw new OStorageException("File with id " + fileId + " is deleted.");

    final FileChangesContainer changesContainer = fileChanges.get(fileId);
    assert changesContainer != null;

    final FilePageChangesContainer pageChangesContainer = changesContainer.pageChangesMap.get(pageIndex);
    assert pageChangesContainer != null;

    return pageChangesContainer.changesTree;
  }

  public long filledUpTo(long fileId, ODiskCache diskCache) throws IOException {
    if (deletedFiles.contains(fileId))
      throw new OStorageException("File with id " + fileId + " is deleted.");

    FileChangesContainer changesContainer = fileChanges.get(fileId);

    if (changesContainer == null) {
      changesContainer = new FileChangesContainer();
      fileChanges.put(fileId, changesContainer);
    } else if (changesContainer.isNew || changesContainer.maxNewPageIndex > -1)
      return changesContainer.maxNewPageIndex + 1;

    return diskCache.getFilledUpTo(fileId);
  }

  public long addFile(String fileName, ODiskCache diskCache) {
    if (newFileNamesId.containsKey(fileName))
      throw new OStorageException("File with name " + fileName + " already exists.");

    final long fileId = diskCache.bookFileId();
    newFileNamesId.put(fileName, fileId);

    FileChangesContainer fileChangesContainer = new FileChangesContainer();
    fileChangesContainer.isNew = true;
    fileChangesContainer.fileName = fileName;

    fileChanges.put(fileId, fileChangesContainer);
    deletedFileNames.remove(fileName);

    return fileId;
  }

  public long openFile(String fileName, ODiskCache diskCache) throws IOException {
    Long fileId = newFileNamesId.get(fileName);

    if (fileId == null)
      fileId = diskCache.openFile(fileName);

    FileChangesContainer fileChangesContainer = fileChanges.get(fileId);
    if (fileChangesContainer == null) {
      fileChangesContainer = new FileChangesContainer();
      fileChanges.put(fileId, fileChangesContainer);
    }

    return fileId;
  }

  public void openFile(long fileId, ODiskCache diskCache) throws IOException {
    if (deletedFiles.contains(fileId))
      throw new OStorageException("File with id " + fileId + " is deleted.");

    FileChangesContainer changesContainer = fileChanges.get(fileId);
    if (changesContainer == null || !changesContainer.isNew)
      diskCache.openFile(fileId);
  }

  public void deleteFile(long fileId, ODiskCache diskCache) {
    final FileChangesContainer fileChangesContainer = fileChanges.remove(fileId);
    if (fileChangesContainer != null && fileChangesContainer.fileName != null)
      newFileNamesId.remove(fileChangesContainer.fileName);
    else {
      deletedFiles.add(fileId);
      deletedFileNames.add(diskCache.fileNameById(fileId));
    }
  }

  public boolean isFileExists(String fileName, ODiskCache diskCache) {
    if (newFileNamesId.containsKey(fileName))
      return true;

    if (deletedFileNames.contains(fileName))
      return false;

    return diskCache.exists(fileName);
  }

  public String fileNameById(long fileId, ODiskCache diskCache) {
    FileChangesContainer fileChangesContainer = fileChanges.get(fileId);

    if (fileChangesContainer != null && fileChangesContainer.fileName != null)
      return fileChangesContainer.fileName;

    if (deletedFiles.contains(fileId))
      throw new OStorageException("File with id " + fileId + " was deleted.");

    return diskCache.fileNameById(fileId);
  }

  void incrementCounter() {
    startCounter++;
  }

  int decrementCounter() {
    startCounter--;
    return startCounter;
  }

  void rollback() {
    rollback = true;
  }

  boolean isRollback() {
    return rollback;
  }

  void addLockedObject(Object lockedObject) {
    lockedObjects.add(lockedObject);
  }

  boolean containsInLockedObjects(Object objectToLock) {
    return lockedObjects.contains(objectToLock);
  }

  Iterable<Object> lockedObjects() {
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

  private static class FileChangesContainer {
    private Map<Long, FilePageChangesContainer> pageChangesMap  = new HashMap<Long, FilePageChangesContainer>();
    private long                                maxNewPageIndex = -1;
    private boolean                             isNew           = false;
    private String                              fileName        = null;
  }

  private static class FilePageChangesContainer {
    private OWALChangesTree    changesTree = new OWALChangesTree();
    private OLogSequenceNumber lsn         = null;
    private boolean            isNew       = false;
    private boolean            pinPage     = false;
  }
}
