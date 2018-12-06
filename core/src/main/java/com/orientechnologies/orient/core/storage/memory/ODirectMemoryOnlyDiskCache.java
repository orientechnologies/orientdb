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

package com.orientechnologies.orient.core.storage.memory;

import com.orientechnologies.common.directmemory.OByteBufferPool;
import com.orientechnologies.common.directmemory.OPointer;
import com.orientechnologies.common.types.OModifiableBoolean;
import com.orientechnologies.common.util.OCommonConst;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.storage.cache.OAbstractWriteCache;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cache.OCacheEntryImpl;
import com.orientechnologies.orient.core.storage.cache.OCachePointer;
import com.orientechnologies.orient.core.storage.cache.OPageDataVerificationError;
import com.orientechnologies.orient.core.storage.cache.OReadCache;
import com.orientechnologies.orient.core.storage.cache.OWriteCache;
import com.orientechnologies.orient.core.storage.cache.local.OBackgroundExceptionListener;
import com.orientechnologies.orient.core.storage.impl.local.OLowDiskSpaceListener;
import com.orientechnologies.orient.core.storage.impl.local.OPageIsBrokenListener;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.core.storage.impl.local.statistic.OPerformanceStatisticManager;
import com.orientechnologies.orient.core.storage.impl.local.statistic.OSessionStoragePerformanceStatistic;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 6/24/14
 */
public final class ODirectMemoryOnlyDiskCache extends OAbstractWriteCache implements OReadCache, OWriteCache {
  private final Lock metadataLock = new ReentrantLock();

  private final Map<String, Integer> fileNameIdMap = new HashMap<>();
  private final Map<Integer, String> fileIdNameMap = new HashMap<>();

  private final ConcurrentMap<Integer, MemoryFile> files = new ConcurrentHashMap<>();

  private int counter = 0;

  private final int                          pageSize;
  private final int                          id;
  private final OPerformanceStatisticManager performanceStatisticManager;

  ODirectMemoryOnlyDiskCache(int pageSize, int id, OPerformanceStatisticManager performanceStatisticManager) {
    this.pageSize = pageSize;
    this.id = id;
    this.performanceStatisticManager = performanceStatisticManager;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public final Path getRootDirectory() {
    return null;
  }

  @Override
  public final long addFile(String fileName, OWriteCache writeCache) {
    metadataLock.lock();
    try {
      Integer fileId = fileNameIdMap.get(fileName);

      if (fileId == null) {
        counter++;
        final int id = counter;

        files.put(id, new MemoryFile(this.id, id));
        fileNameIdMap.put(fileName, id);

        fileId = id;

        fileIdNameMap.put(fileId, fileName);
      } else {
        throw new OStorageException(fileName + " already exists.");
      }

      return composeFileId(id, fileId);
    } finally {
      metadataLock.unlock();
    }
  }

  @Override
  public final long fileIdByName(String fileName) {
    metadataLock.lock();
    try {
      Integer fileId = fileNameIdMap.get(fileName);
      if (fileId != null) {
        return fileId;
      }
    } finally {
      metadataLock.unlock();
    }

    return -1;
  }

  @Override
  public final int internalFileId(long fileId) {
    return extractFileId(fileId);
  }

  @Override
  public final long externalFileId(int fileId) {
    return composeFileId(id, fileId);
  }

  @Override
  public final long bookFileId(String fileName) {
    metadataLock.lock();
    try {
      counter++;
      return composeFileId(id, counter);
    } finally {
      metadataLock.unlock();
    }
  }

  @Override
  public final void addBackgroundExceptionListener(OBackgroundExceptionListener listener) {
  }

  @Override
  public final void removeBackgroundExceptionListener(OBackgroundExceptionListener listener) {
  }

  @Override
  public final void checkCacheOverflow() {
  }

  @Override
  public final long addFile(String fileName, long fileId, OWriteCache writeCache) {
    int intId = extractFileId(fileId);

    metadataLock.lock();
    try {
      if (files.containsKey(intId)) {
        throw new OStorageException("File with id " + intId + " already exists.");
      }

      if (fileNameIdMap.containsKey(fileName)) {
        throw new OStorageException(fileName + " already exists.");
      }

      files.put(intId, new MemoryFile(id, intId));
      fileNameIdMap.put(fileName, intId);
      fileIdNameMap.put(intId, fileName);

      return composeFileId(id, intId);
    } finally {
      metadataLock.unlock();
    }
  }

  @Override
  public final OCacheEntry loadForWrite(long fileId, long pageIndex, boolean checkPinnedPages, OWriteCache writeCache,
      int pageCount, boolean verifyChecksums, OLogSequenceNumber startLSN) {

    final OCacheEntry cacheEntry = doLoad(fileId, pageIndex);

    if (cacheEntry == null) {
      return null;
    }

    cacheEntry.acquireExclusiveLock();

    return cacheEntry;
  }

  @Override
  public final OCacheEntry loadForRead(long fileId, long pageIndex, boolean checkPinnedPages, OWriteCache writeCache, int pageCount,
      boolean verifyChecksums) {

    final OCacheEntry cacheEntry = doLoad(fileId, pageIndex);

    if (cacheEntry == null) {
      return null;
    }

    cacheEntry.acquireSharedLock();

    return cacheEntry;
  }

  private OCacheEntry doLoad(long fileId, long pageIndex) {
    final OSessionStoragePerformanceStatistic sessionStoragePerformanceStatistic = performanceStatisticManager
        .getSessionPerformanceStatistic();

    if (sessionStoragePerformanceStatistic != null) {
      sessionStoragePerformanceStatistic.startPageReadFromCacheTimer();
    }

    try {
      int intId = extractFileId(fileId);

      final MemoryFile memoryFile = getFile(intId);
      final OCacheEntry cacheEntry = memoryFile.loadPage(pageIndex);
      if (cacheEntry == null) {
        return null;
      }

      //noinspection SynchronizationOnLocalVariableOrMethodParameter
      synchronized (cacheEntry) {
        cacheEntry.incrementUsages();
      }

      return cacheEntry;
    } finally {
      if (sessionStoragePerformanceStatistic != null) {
        sessionStoragePerformanceStatistic.stopPageReadFromCacheTimer();
      }
    }
  }

  @Override
  public final void pinPage(OCacheEntry cacheEntry, OWriteCache writeCache) {
  }

  @Override
  public final OCacheEntry allocateNewPage(long fileId, OWriteCache writeCache, boolean verifyChecksums,
      OLogSequenceNumber startLSN, boolean initPage) {
    final OSessionStoragePerformanceStatistic sessionStoragePerformanceStatistic = performanceStatisticManager
        .getSessionPerformanceStatistic();

    if (sessionStoragePerformanceStatistic != null) {
      sessionStoragePerformanceStatistic.startPageReadFromCacheTimer();
    }

    try {
      int intId = extractFileId(fileId);

      final MemoryFile memoryFile = getFile(intId);
      final OCacheEntry cacheEntry = memoryFile.addNewPage();

      //noinspection SynchronizationOnLocalVariableOrMethodParameter
      synchronized (cacheEntry) {
        cacheEntry.incrementUsages();
      }

      cacheEntry.acquireExclusiveLock();
      return cacheEntry;
    } finally {
      if (sessionStoragePerformanceStatistic != null) {
        sessionStoragePerformanceStatistic.stopPageReadFromCacheTimer();
      }
    }
  }

  private MemoryFile getFile(int fileId) {
    final MemoryFile memoryFile = files.get(fileId);

    if (memoryFile == null) {
      throw new OStorageException("File with id " + fileId + " does not exist");
    }

    return memoryFile;
  }

  @Override
  public final void releaseFromWrite(OCacheEntry cacheEntry, OWriteCache writeCache) {
    cacheEntry.releaseExclusiveLock();

    doRelease(cacheEntry);
  }

  @Override
  public final void releaseFromRead(OCacheEntry cacheEntry, OWriteCache writeCache) {
    cacheEntry.releaseSharedLock();

    doRelease(cacheEntry);
  }

  private static void doRelease(OCacheEntry cacheEntry) {
    //noinspection SynchronizationOnLocalVariableOrMethodParameter
    synchronized (cacheEntry) {
      cacheEntry.decrementUsages();
      assert cacheEntry.getUsagesCount() > 0 || cacheEntry.getCachePointer().getBuffer() == null || !cacheEntry
          .isLockAcquiredByCurrentThread();
    }
  }

  @Override
  public final long getFilledUpTo(long fileId) {
    int intId = extractFileId(fileId);

    final MemoryFile memoryFile = getFile(intId);
    return memoryFile.size();
  }

  @Override
  public final void flush(long fileId) {
  }

  @Override
  public final void close(long fileId, boolean flush) {
  }

  @Override
  public final void deleteFile(long fileId) {
    int intId = extractFileId(fileId);
    metadataLock.lock();
    try {
      final String fileName = fileIdNameMap.remove(intId);
      if (fileName == null) {
        return;
      }

      fileNameIdMap.remove(fileName);
      MemoryFile file = files.remove(intId);
      if (file != null) {
        file.clear();
      }
    } finally {
      metadataLock.unlock();
    }
  }

  @Override
  public final void renameFile(long fileId, String newFileName) {
    int intId = extractFileId(fileId);

    metadataLock.lock();
    try {
      String fileName = fileIdNameMap.get(intId);
      if (fileName == null) {
        return;
      }

      fileNameIdMap.remove(fileName);

      fileIdNameMap.put(intId, newFileName);
      fileNameIdMap.put(newFileName, intId);
    } finally {
      metadataLock.unlock();
    }
  }

  @Override
  public final void replaceFileContentWith(long fileId, Path newContentFile) {
    throw new UnsupportedOperationException("replacing file content is not supported for memory storage");
  }

  @Override
  public final void truncateFile(long fileId) {
    int intId = extractFileId(fileId);

    final MemoryFile file = getFile(intId);
    file.clear();
  }

  @Override
  public final void flush() {
  }

  @Override
  public final long[] close() {
    return new long[0];
  }

  @Override
  public final void clear() {
    delete();
  }

  @Override
  public final long[] delete() {
    metadataLock.lock();
    try {
      for (MemoryFile file : files.values()) {
        file.clear();
      }

      files.clear();
      fileIdNameMap.clear();
      fileNameIdMap.clear();
    } finally {
      metadataLock.unlock();
    }

    return new long[0];
  }

  @Override
  public final void deleteStorage(OWriteCache writeCache) {
    delete();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public final void closeStorage(OWriteCache writeCache) {
    //noinspection ResultOfMethodCallIgnored
    close();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public final void loadCacheState(OWriteCache writeCache) {
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public final void storeCacheState(OWriteCache writeCache) {
  }

  @Override
  public final OPageDataVerificationError[] checkStoredPages(OCommandOutputListener commandOutputListener) {
    return OCommonConst.EMPTY_PAGE_DATA_VERIFICATION_ARRAY;
  }

  @Override
  public final boolean exists(String name) {
    metadataLock.lock();
    try {
      final Integer fileId = fileNameIdMap.get(name);
      if (fileId == null) {
        return false;
      }

      final MemoryFile memoryFile = files.get(fileId);
      return memoryFile != null;
    } finally {
      metadataLock.unlock();
    }
  }

  @Override
  public final boolean exists(long fileId) {
    int intId = extractFileId(fileId);

    metadataLock.lock();
    try {
      final MemoryFile memoryFile = files.get(intId);
      return memoryFile != null;
    } finally {
      metadataLock.unlock();
    }
  }

  @Override
  public final String fileNameById(long fileId) {
    int intId = extractFileId(fileId);

    metadataLock.lock();
    try {
      return fileIdNameMap.get(intId);
    } finally {
      metadataLock.unlock();
    }
  }

  @Override
  public final String nativeFileNameById(long fileId) {
    return fileNameById(fileId);
  }

  private static final class MemoryFile {
    private final int id;
    private final int storageId;

    private final ReadWriteLock clearLock = new ReentrantReadWriteLock();

    private final ConcurrentSkipListMap<Long, OCacheEntry> content = new ConcurrentSkipListMap<>();

    private MemoryFile(int storageId, int id) {
      this.storageId = storageId;
      this.id = id;
    }

    private OCacheEntry loadPage(long index) {
      clearLock.readLock().lock();
      try {
        return content.get(index);
      } finally {
        clearLock.readLock().unlock();
      }
    }

    private OCacheEntry addNewPage() {
      clearLock.readLock().lock();
      try {
        OCacheEntry cacheEntry;

        long index;
        do {
          if (content.isEmpty()) {
            index = 0;
          } else {
            long lastIndex = content.lastKey();
            index = lastIndex + 1;
          }

          final OByteBufferPool bufferPool = OByteBufferPool.instance(null);
          final OPointer pointer = bufferPool.acquireDirect(true);

          final OCachePointer cachePointer = new OCachePointer(pointer, bufferPool, id, index);
          cachePointer.incrementReferrer();

          cacheEntry = new OCacheEntryImpl(composeFileId(storageId, id), index, cachePointer);

          OCacheEntry oldCacheEntry = content.putIfAbsent(index, cacheEntry);

          if (oldCacheEntry != null) {
            cachePointer.decrementReferrer();
            index = -1;
          }
        } while (index < 0);

        return cacheEntry;
      } finally {
        clearLock.readLock().unlock();
      }
    }

    private long size() {
      clearLock.readLock().lock();
      try {
        if (content.isEmpty()) {
          return 0;
        }

        try {
          return content.lastKey() + 1;
        } catch (NoSuchElementException ignore) {
          return 0;
        }

      } finally {
        clearLock.readLock().unlock();
      }
    }

    private long getUsedMemory() {
      return content.size();
    }

    private void clear() {
      boolean thereAreNotReleased = false;

      clearLock.writeLock().lock();
      try {
        for (OCacheEntry entry : content.values()) {
          //noinspection SynchronizationOnLocalVariableOrMethodParameter
          synchronized (entry) {
            thereAreNotReleased |= entry.getUsagesCount() > 0;
            entry.getCachePointer().decrementReferrer();
          }
        }

        content.clear();
      } finally {
        clearLock.writeLock().unlock();
      }

      if (thereAreNotReleased) {
        throw new IllegalStateException("Some cache entries were not released. Storage may be in invalid state.");
      }
    }
  }

  @Override
  public final long getUsedMemory() {
    long totalPages = 0;
    for (MemoryFile file : files.values()) {
      totalPages += file.getUsedMemory();
    }

    return totalPages * pageSize;
  }

  @Override
  public final boolean checkLowDiskSpace() {
    return true;
  }

  /**
   * Not implemented because has no sense
   */
  @Override
  public final void addPageIsBrokenListener(OPageIsBrokenListener listener) {
  }

  /**
   * Not implemented because has no sense
   */
  @Override
  public final void removePageIsBrokenListener(OPageIsBrokenListener listener) {
  }

  @Override
  public final void addLowDiskSpaceListener(OLowDiskSpaceListener listener) {
  }

  @Override
  public final void removeLowDiskSpaceListener(OLowDiskSpaceListener listener) {
  }

  @Override
  public final long loadFile(String fileName) {
    metadataLock.lock();
    try {
      Integer fileId = fileNameIdMap.get(fileName);

      if (fileId == null) {
        throw new OStorageException("File " + fileName + " does not exist.");
      }

      return composeFileId(id, fileId);
    } finally {
      metadataLock.unlock();
    }
  }

  @Override
  public final long addFile(String fileName) {
    return addFile(fileName, null);
  }

  @Override
  public final long addFile(String fileName, long fileId) {
    return addFile(fileName, fileId, null);
  }

  @Override
  public final void store(long fileId, long pageIndex, OCachePointer dataPointer) {
    throw new UnsupportedOperationException();
  }

  @Override
  public final void makeFuzzyCheckpoint(long segmentId) {
  }

  @Override
  public final void flushTillSegment(long segmentId) {
  }

  @Override
  public final Long getMinimalNotFlushedSegment() {
    throw new UnsupportedOperationException();
  }

  @Override
  public final void updateDirtyPagesTable(OCachePointer pointer, OLogSequenceNumber startLSN) {
  }

  @Override
  public final OCachePointer[] load(long fileId, long startPageIndex, int pageCount, boolean addNewPages, boolean initNewPage,
      OModifiableBoolean cacheHit, boolean verifyChecksums) {
    throw new UnsupportedOperationException();
  }

  @Override
  public final long getExclusiveWriteCachePagesSize() {
    return 0;
  }

  @Override
  public final void truncateFile(long fileId, OWriteCache writeCache) {
    truncateFile(fileId);
  }

  @Override
  public final int getId() {
    return id;
  }

  @Override
  public final Map<String, Long> files() {
    final Map<String, Long> result = new HashMap<>();

    metadataLock.lock();
    try {
      for (Map.Entry<String, Integer> entry : fileNameIdMap.entrySet()) {
        if (entry.getValue() > 0) {
          result.put(entry.getKey(), composeFileId(id, entry.getValue()));
        }
      }
    } finally {
      metadataLock.unlock();
    }

    return result;
  }

  /**
   * @inheritDoc
   */
  @Override
  public final int pageSize() {
    return pageSize;
  }

  /**
   * @inheritDoc
   */
  @Override
  public final boolean fileIdsAreEqual(long firsId, long secondId) {
    final int firstIntId = extractFileId(firsId);
    final int secondIntId = extractFileId(secondId);

    return firstIntId == secondIntId;
  }

  @Override
  public final String restoreFileById(long fileId) {
    return null;
  }

  @Override
  public final void closeFile(long fileId, boolean flush, OWriteCache writeCache) {
    close(fileId, flush);
  }

  @Override
  public final void deleteFile(long fileId, OWriteCache writeCache) {
    deleteFile(fileId);
  }
}
