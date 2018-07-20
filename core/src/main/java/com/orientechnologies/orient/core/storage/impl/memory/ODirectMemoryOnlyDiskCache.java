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

package com.orientechnologies.orient.core.storage.impl.memory;

import com.orientechnologies.common.directmemory.OByteBufferPool;
import com.orientechnologies.common.types.OModifiableBoolean;
import com.orientechnologies.common.util.OCommonConst;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.storage.cache.OAbstractWriteCache;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cache.OCachePointer;
import com.orientechnologies.orient.core.storage.cache.OPageDataVerificationError;
import com.orientechnologies.orient.core.storage.cache.OReadCache;
import com.orientechnologies.orient.core.storage.cache.OWriteCache;
import com.orientechnologies.orient.core.storage.cache.local.OBackgroundExceptionListener;
import com.orientechnologies.orient.core.storage.impl.local.OLowDiskSpaceListener;
import com.orientechnologies.orient.core.storage.impl.local.OPageIsBrokenListener;
import com.orientechnologies.orient.core.storage.impl.local.statistic.OPerformanceStatisticManager;
import com.orientechnologies.orient.core.storage.impl.local.statistic.OSessionStoragePerformanceStatistic;

import java.nio.ByteBuffer;
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
public class ODirectMemoryOnlyDiskCache extends OAbstractWriteCache implements OReadCache, OWriteCache {
  private final Lock metadataLock = new ReentrantLock();

  private final Map<String, Integer> fileNameIdMap = new HashMap<>();
  private final Map<Integer, String> fileIdNameMap = new HashMap<>();

  private final ConcurrentMap<Integer, MemoryFile> files = new ConcurrentHashMap<>();

  private int counter = 0;

  private final int                          pageSize;
  private final int                          id;
  private final OPerformanceStatisticManager performanceStatisticManager;

  ODirectMemoryOnlyDiskCache(final int pageSize, final int id, final OPerformanceStatisticManager performanceStatisticManager) {
    this.pageSize = pageSize;
    this.id = id;
    this.performanceStatisticManager = performanceStatisticManager;
  }

  @Override
  public OPerformanceStatisticManager getPerformanceStatisticManager() {
    return performanceStatisticManager;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Path getRootDirectory() {
    return null;
  }

  @Override
  public long addFile(final String fileName, final OWriteCache writeCache) {
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
  public long fileIdByName(final String fileName) {
    metadataLock.lock();
    try {
      final Integer fileId = fileNameIdMap.get(fileName);
      if (fileId != null)
        return fileId;
    } finally {
      metadataLock.unlock();
    }

    return -1;
  }

  @Override
  public int internalFileId(final long fileId) {
    return extractFileId(fileId);
  }

  @Override
  public long externalFileId(final int fileId) {
    return composeFileId(id, fileId);
  }

  @Override
  public long bookFileId(final String fileName) {
    metadataLock.lock();
    try {
      counter++;
      return composeFileId(id, counter);
    } finally {
      metadataLock.unlock();
    }
  }

  @Override
  public void addBackgroundExceptionListener(final OBackgroundExceptionListener listener) {
  }

  @Override
  public void removeBackgroundExceptionListener(final OBackgroundExceptionListener listener) {
  }

  @Override
  public void checkCacheOverflow() {
  }

  @Override
  public long addFile(final String fileName, final long fileId, final OWriteCache writeCache) {
    final int intId = extractFileId(fileId);

    metadataLock.lock();
    try {
      if (files.containsKey(intId))
        throw new OStorageException("File with id " + intId + " already exists.");

      if (fileNameIdMap.containsKey(fileName))
        throw new OStorageException(fileName + " already exists.");

      files.put(intId, new MemoryFile(id, intId));
      fileNameIdMap.put(fileName, intId);
      fileIdNameMap.put(intId, fileName);

      return composeFileId(id, intId);
    } finally {
      metadataLock.unlock();
    }
  }

  @Override
  public OCacheEntry loadForWrite(final long fileId, final long pageIndex, final boolean checkPinnedPages,
      final OWriteCache writeCache, final int pageCount, final boolean verifyChecksums) {

    final OCacheEntry cacheEntry = doLoad(fileId, pageIndex);

    if (cacheEntry == null)
      return null;

    cacheEntry.acquireExclusiveLock();

    return cacheEntry;
  }

  @Override
  public OCacheEntry loadForRead(final long fileId, final long pageIndex, final boolean checkPinnedPages,
      final OWriteCache writeCache, final int pageCount, final boolean verifyChecksums) {

    final OCacheEntry cacheEntry = doLoad(fileId, pageIndex);

    if (cacheEntry == null)
      return null;

    cacheEntry.acquireSharedLock();

    return cacheEntry;
  }

  private OCacheEntry doLoad(final long fileId, final long pageIndex) {
    final OSessionStoragePerformanceStatistic sessionStoragePerformanceStatistic = performanceStatisticManager
        .getSessionPerformanceStatistic();

    if (sessionStoragePerformanceStatistic != null) {
      sessionStoragePerformanceStatistic.startPageReadFromCacheTimer();
    }

    try {
      final int intId = extractFileId(fileId);

      final MemoryFile memoryFile = getFile(intId);
      final OCacheEntry cacheEntry = memoryFile.loadPage(pageIndex);
      if (cacheEntry == null)
        return null;

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
  public void pinPage(final OCacheEntry cacheEntry, OWriteCache writeCache) {
  }

  @Override
  public OCacheEntry allocateNewPage(final long fileId, final OWriteCache writeCache, final boolean verifyChecksums) {
    final OSessionStoragePerformanceStatistic sessionStoragePerformanceStatistic = performanceStatisticManager
        .getSessionPerformanceStatistic();

    if (sessionStoragePerformanceStatistic != null) {
      sessionStoragePerformanceStatistic.startPageReadFromCacheTimer();
    }

    try {
      final int intId = extractFileId(fileId);

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

  private MemoryFile getFile(final int fileId) {
    final MemoryFile memoryFile = files.get(fileId);

    if (memoryFile == null)
      throw new OStorageException("File with id " + fileId + " does not exist");

    return memoryFile;
  }

  @Override
  public void releaseFromWrite(final OCacheEntry cacheEntry, final OWriteCache writeCache) {
    cacheEntry.releaseExclusiveLock();

    doRelease(cacheEntry);
  }

  @Override
  public void releaseFromRead(final OCacheEntry cacheEntry, final OWriteCache writeCache) {
    cacheEntry.releaseSharedLock();

    doRelease(cacheEntry);
  }

  private static void doRelease(final OCacheEntry cacheEntry) {
    //noinspection SynchronizationOnLocalVariableOrMethodParameter
    synchronized (cacheEntry) {
      cacheEntry.decrementUsages();
      assert cacheEntry.getUsagesCount() > 0 || cacheEntry.getCachePointer().getBuffer() == null || !cacheEntry
          .isLockAcquiredByCurrentThread();
    }
  }

  @Override
  public long getFilledUpTo(final long fileId) {
    final int intId = extractFileId(fileId);

    final MemoryFile memoryFile = getFile(intId);
    return memoryFile.size();
  }

  @Override
  public void flush(final long fileId) {
  }

  @Override
  public void close(final long fileId, final boolean flush) {
  }

  @Override
  public void deleteFile(final long fileId) {
    final int intId = extractFileId(fileId);
    metadataLock.lock();
    try {
      final String fileName = fileIdNameMap.remove(intId);
      if (fileName == null)
        return;

      fileNameIdMap.remove(fileName);
      final MemoryFile file = files.remove(intId);
      if (file != null)
        file.clear();
    } finally {
      metadataLock.unlock();
    }
  }

  @Override
  public void renameFile(final long fileId, final String newFileName) {
    final int intId = extractFileId(fileId);

    metadataLock.lock();
    try {
      final String fileName = fileIdNameMap.get(intId);
      if (fileName == null)
        return;

      fileNameIdMap.remove(fileName);

      fileIdNameMap.put(intId, newFileName);
      fileNameIdMap.put(newFileName, intId);
    } finally {
      metadataLock.unlock();
    }
  }

  @Override
  public void replaceFileContentWith(final long fileId, final Path newContentFile) {
    throw new UnsupportedOperationException("replacing file content is not supported for memory storage");
  }

  @Override
  public void truncateFile(final long fileId) {
    final int intId = extractFileId(fileId);

    final MemoryFile file = getFile(intId);
    file.clear();
  }

  @Override
  public void flush() {
  }

  @Override
  public long[] close() {
    return new long[0];
  }

  @Override
  public void clear() {
    delete();
  }

  @Override
  public long[] delete() {
    metadataLock.lock();
    try {
      for (final MemoryFile file : files.values())
        file.clear();

      files.clear();
      fileIdNameMap.clear();
      fileNameIdMap.clear();
    } finally {
      metadataLock.unlock();
    }

    return new long[0];
  }

  @Override
  public void deleteStorage(final OWriteCache writeCache) {
    delete();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void closeStorage(final OWriteCache writeCache) {
    close();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void loadCacheState(final OWriteCache writeCache) {
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void storeCacheState(final OWriteCache writeCache) {
  }

  @Override
  public OPageDataVerificationError[] checkStoredPages(final OCommandOutputListener commandOutputListener) {
    return OCommonConst.EMPTY_PAGE_DATA_VERIFICATION_ARRAY;
  }

  @Override
  public boolean exists(final String name) {
    metadataLock.lock();
    try {
      final Integer fileId = fileNameIdMap.get(name);
      if (fileId == null)
        return false;

      final MemoryFile memoryFile = files.get(fileId);
      return memoryFile != null;
    } finally {
      metadataLock.unlock();
    }
  }

  @Override
  public boolean exists(final long fileId) {
    final int intId = extractFileId(fileId);

    metadataLock.lock();
    try {
      final MemoryFile memoryFile = files.get(intId);
      return memoryFile != null;
    } finally {
      metadataLock.unlock();
    }
  }

  @Override
  public String fileNameById(final long fileId) {
    final int intId = extractFileId(fileId);

    metadataLock.lock();
    try {
      return fileIdNameMap.get(intId);
    } finally {
      metadataLock.unlock();
    }
  }

  @Override
  public String nativeFileNameById(final long fileId) {
    return fileNameById(fileId);
  }

  private static final class MemoryFile {
    private final int id;
    private final int storageId;

    private final ReadWriteLock clearLock = new ReentrantReadWriteLock();

    private final ConcurrentSkipListMap<Long, OCacheEntry> content = new ConcurrentSkipListMap<>();

    private MemoryFile(final int storageId, final int id) {
      this.storageId = storageId;
      this.id = id;
    }

    private OCacheEntry loadPage(final long index) {
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
          if (content.isEmpty())
            index = 0;
          else {
            final long lastIndex = content.lastKey();
            index = lastIndex + 1;
          }

          final OByteBufferPool bufferPool = OByteBufferPool.instance();
          final ByteBuffer buffer = bufferPool.acquireDirect(true, 0);

          final OCachePointer cachePointer = new OCachePointer(buffer, bufferPool, id, index);
          cachePointer.incrementReferrer();

          cacheEntry = new OCacheEntry(composeFileId(storageId, id), index, cachePointer);

          final OCacheEntry oldCacheEntry = content.putIfAbsent(index, cacheEntry);

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
        if (content.isEmpty())
          return 0;

        try {
          return content.lastKey() + 1;
        } catch (final NoSuchElementException ignore) {
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
        for (final OCacheEntry entry : content.values()) {
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

      if (thereAreNotReleased)
        throw new IllegalStateException("Some cache entries were not released. Storage may be in invalid state.");
    }
  }

  @Override
  public long getUsedMemory() {
    long totalPages = 0;
    for (final MemoryFile file : files.values())
      totalPages += file.getUsedMemory();

    return totalPages * pageSize;
  }

  @Override
  public boolean checkLowDiskSpace() {
    return true;
  }

  /**
   * Not implemented because has no sense
   */
  @Override
  public void addPageIsBrokenListener(final OPageIsBrokenListener listener) {
  }

  /**
   * Not implemented because has no sense
   */
  @Override
  public void removePageIsBrokenListener(final OPageIsBrokenListener listener) {
  }

  @Override
  public void addLowDiskSpaceListener(final OLowDiskSpaceListener listener) {
  }

  @Override
  public void removeLowDiskSpaceListener(final OLowDiskSpaceListener listener) {
  }

  @Override
  public long loadFile(final String fileName) {
    metadataLock.lock();
    try {
      final Integer fileId = fileNameIdMap.get(fileName);

      if (fileId == null) {
        throw new OStorageException("File " + fileName + " does not exist.");
      }

      return composeFileId(id, fileId);
    } finally {
      metadataLock.unlock();
    }
  }

  @Override
  public long addFile(final String fileName) {
    return addFile(fileName, null);
  }

  @Override
  public long addFile(final String fileName, final long fileId) {
    return addFile(fileName, fileId, null);
  }

  @Override
  public void store(final long fileId, final long pageIndex, final OCachePointer dataPointer) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void makeFuzzyCheckpoint(final long segmentId) {
  }

  @Override
  public void flushTillSegment(final long segmentId) {
  }

  @Override
  public Long getMinimalNotFlushedSegment() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateDirtyPagesTable(final OCachePointer pointer) {
  }

  @Override
  public OCachePointer[] load(final long fileId, final long startPageIndex, final int pageCount, final boolean addNewPages,
      final OModifiableBoolean cacheHit, final boolean verifyChecksums) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getExclusiveWriteCachePagesSize() {
    return 0;
  }

  @Override
  public void truncateFile(final long fileId, final OWriteCache writeCache) {
    truncateFile(fileId);
  }

  @Override
  public int getId() {
    return id;
  }

  @Override
  public Map<String, Long> files() {
    final Map<String, Long> result = new HashMap<>();

    metadataLock.lock();
    try {
      for (final Map.Entry<String, Integer> entry : fileNameIdMap.entrySet()) {
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
  public int pageSize() {
    return pageSize;
  }

  /**
   * @inheritDoc
   */
  @Override
  public boolean fileIdsAreEqual(final long firsId, final long secondId) {
    final int firstIntId = extractFileId(firsId);
    final int secondIntId = extractFileId(secondId);

    return firstIntId == secondIntId;
  }


  @Override
  public String restoreFileById(final long fileId) {
    return null;
  }

  @Override
  public void closeFile(final long fileId, final boolean flush, final OWriteCache writeCache) {
    close(fileId, flush);
  }

  @Override
  public void deleteFile(final long fileId, final OWriteCache writeCache) {
    deleteFile(fileId);
  }
}
