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

package com.orientechnologies.orient.core.storage.cache.local.twoq;

import com.orientechnologies.common.concur.lock.*;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.types.OModifiableBoolean;
import com.orientechnologies.common.util.OPair;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.exception.OAllCacheEntriesAreUsedException;
import com.orientechnologies.orient.core.exception.OReadCacheException;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.storage.cache.*;
import com.orientechnologies.orient.core.storage.impl.local.statistic.OSessionStoragePerformanceStatistic;

import java.io.*;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.Lock;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 7/24/13
 */
public class O2QCache implements OReadCache {
  /**
   * Maximum percent of pinned pages which may be contained in this cache.
   */
  private static final int MAX_PERCENT_OF_PINED_PAGES = 50;

  /**
   * Minimum size of memory which may be allocated by cache (in pages). This parameter is used only if related flag is set in
   * constrictor of cache.
   */
  public static final int MIN_CACHE_SIZE = 256;

  private static final int MAX_CACHE_OVERFLOW = Runtime.getRuntime().availableProcessors() * 8;

  /**
   * File which contains stored state of disk cache after storage close.
   */
  static final String CACHE_STATE_FILE = "cache.stt";

  /**
   * Extension for file which contains stored state of disk cache after storage close.
   */
  public static final String CACHE_STATISTIC_FILE_EXTENSION = ".stt";

  private final LRUList am;
  private final LRUList a1out;
  private final LRUList a1in;
  private final int     pageSize;

  /**
   * Counts how much time we warned user that limit of amount of pinned pages is reached.
   */
  private final LongAdder pinnedPagesWarningCounter = new LongAdder();

  /**
   * Cache of value which is contained inside of {@link #pinnedPagesWarningCounter}. It is used to speed up calculation of warnings.
   */
  private volatile int pinnedPagesWarningsCache = 0;

  private final AtomicReference<MemoryData> memoryDataContainer = new AtomicReference<>();

  /**
   * Contains all pages in cache for given file.
   */
  private final ConcurrentMap<Long, Set<Long>> filePages;

  /**
   * Maximum percent of pinned pages which may be hold in this cache.
   *
   * @see com.orientechnologies.orient.core.config.OGlobalConfiguration#DISK_CACHE_PINNED_PAGES
   */
  private final int percentOfPinnedPages;

  private final OReadersWriterSpinLock                 cacheLock       = new OReadersWriterSpinLock();
  private final OPartitionedLockManager<Object>        fileLockManager = new OPartitionedLockManager<>(true);
  private final OPartitionedLockManager<PageKey>       pageLockManager = new OPartitionedLockManager<>();
  private final ConcurrentMap<PinnedPage, OCacheEntry> pinnedPages     = new ConcurrentHashMap<>();

  private final AtomicBoolean coldPagesRemovalInProgress = new AtomicBoolean();

  /**
   * @param readCacheMaxMemory   Maximum amount of direct memory which can allocated by disk cache in bytes.
   * @param pageSize             Cache page size in bytes.
   * @param checkMinSize         If this flat is set size of cache may be {@link #MIN_CACHE_SIZE} or bigger.
   * @param percentOfPinnedPages Maximum percent of pinned pages which may be hold by this cache.
   *
   * @see #MAX_PERCENT_OF_PINED_PAGES
   */
  public O2QCache(final long readCacheMaxMemory, final int pageSize, final boolean checkMinSize, final int percentOfPinnedPages) {
    if (percentOfPinnedPages > MAX_PERCENT_OF_PINED_PAGES)
      throw new IllegalArgumentException(
          "Percent of pinned pages cannot be more than " + percentOfPinnedPages + " but passed value is " + percentOfPinnedPages);

    this.percentOfPinnedPages = percentOfPinnedPages;

    cacheLock.acquireWriteLock();
    try {
      this.pageSize = pageSize;

      this.filePages = new ConcurrentHashMap<>();

      int normalizedSize = normalizeMemory(readCacheMaxMemory, pageSize);

      if (checkMinSize && normalizedSize < MIN_CACHE_SIZE)
        normalizedSize = MIN_CACHE_SIZE;

      final MemoryData memoryData = new MemoryData(normalizedSize, 0);
      this.memoryDataContainer.set(memoryData);

      am = new ConcurrentLRUList();
      a1out = new ConcurrentLRUList();
      a1in = new ConcurrentLRUList();
    } finally {
      cacheLock.releaseWriteLock();
    }
  }

  LRUList getAm() {
    return am;
  }

  @SuppressWarnings("SameParameterValue")
  boolean inPinnedPages(long fileId, long pageIndex) {
    return pinnedPages.containsKey(new PinnedPage(fileId, pageIndex));
  }

  LRUList getA1out() {
    return a1out;
  }

  LRUList getA1in() {
    return a1in;
  }

  @Override
  public long addFile(String fileName, OWriteCache writeCache) throws IOException {
    cacheLock.acquireWriteLock();
    try {
      long fileId = writeCache.addFile(fileName);
      Set<Long> oldPages = filePages.put(fileId, Collections.newSetFromMap(new ConcurrentHashMap<>()));
      assert oldPages == null || oldPages.isEmpty();
      return fileId;
    } finally {
      cacheLock.releaseWriteLock();
    }
  }

  @Override
  public long addFile(String fileName, long fileId, OWriteCache writeCache) throws IOException {
    fileId = OAbstractWriteCache.checkFileIdCompatibility(writeCache.getId(), fileId);

    cacheLock.acquireWriteLock();
    try {
      final long fid = writeCache.addFile(fileName, fileId);
      Set<Long> oldPages = filePages.put(fid, Collections.newSetFromMap(new ConcurrentHashMap<>()));
      assert oldPages == null || oldPages.isEmpty();

      return fid;
    } finally {
      cacheLock.releaseWriteLock();
    }
  }

  @Override
  public OCacheEntry loadForWrite(long fileId, long pageIndex, boolean checkPinnedPages, OWriteCache writeCache, int pageCount)
      throws IOException {
    final OCacheEntry cacheEntry = doLoad(fileId, pageIndex, checkPinnedPages, writeCache, pageCount);

    if (cacheEntry != null) {
      cacheEntry.acquireExclusiveLock();
      writeCache.updateDirtyPagesTable(cacheEntry.getCachePointer());
    }

    return cacheEntry;
  }

  @Override
  public void releaseFromWrite(OCacheEntry cacheEntry, OWriteCache writeCache) {
    cacheEntry.releaseExclusiveLock();

    CountDownLatch latch = null;

    Lock fileLock;
    Lock pageLock;
    cacheLock.acquireReadLock();
    try {
      fileLock = fileLockManager.acquireSharedLock(cacheEntry.getFileId());
      try {
        pageLock = pageLockManager.acquireExclusiveLock(new PageKey(cacheEntry.getFileId(), cacheEntry.getPageIndex()));
        try {
          cacheEntry.decrementUsages();

          assert cacheEntry.getUsagesCount() >= 0;
          assert cacheEntry.getUsagesCount() > 0 || !cacheEntry.isLockAcquiredByCurrentThread();

          if (cacheEntry.getUsagesCount() == 0 && cacheEntry.isDirty()) {
            final OSessionStoragePerformanceStatistic sessionStoragePerformanceStatistic = writeCache
                .getPerformanceStatisticManager().getSessionPerformanceStatistic();

            if (sessionStoragePerformanceStatistic != null) {
              sessionStoragePerformanceStatistic.startPageWriteInCacheTimer();
            }

            try {
              latch = writeCache.store(cacheEntry.getFileId(), cacheEntry.getPageIndex(), cacheEntry.getCachePointer());
            } finally {
              if (sessionStoragePerformanceStatistic != null) {
                sessionStoragePerformanceStatistic.stopPageWriteInCacheTimer();
              }
            }

            cacheEntry.clearDirty();
          }
        } finally {
          pageLock.unlock();
        }
      } finally {
        fileLock.unlock();
      }
    } finally {
      cacheLock.releaseReadLock();
    }

    if (latch != null) {
      try {
        latch.await();
      } catch (InterruptedException e) {
        Thread.interrupted();
        throw new OInterruptedException("File flush was interrupted");
      } catch (Exception e) {
        throw OException.wrapException(new OReadCacheException("File flush was abnormally terminated"), e);
      }
    }
  }

  @Override
  public OCacheEntry loadForRead(long fileId, long pageIndex, boolean checkPinnedPages, OWriteCache writeCache, int pageCount)
      throws IOException {
    final OCacheEntry cacheEntry = doLoad(fileId, pageIndex, checkPinnedPages, writeCache, pageCount);

    if (cacheEntry != null) {
      cacheEntry.acquireSharedLock();
    }

    return cacheEntry;
  }

  @Override
  public void releaseFromRead(OCacheEntry cacheEntry, OWriteCache writeCache) {
    cacheEntry.releaseSharedLock();

    doRelease(cacheEntry);
  }

  private void doRelease(OCacheEntry cacheEntry) {
    Lock fileLock;
    Lock pageLock;
    cacheLock.acquireReadLock();
    try {
      fileLock = fileLockManager.acquireSharedLock(cacheEntry.getFileId());
      try {
        pageLock = pageLockManager.acquireExclusiveLock(new PageKey(cacheEntry.getFileId(), cacheEntry.getPageIndex()));
        try {
          cacheEntry.decrementUsages();

          assert cacheEntry.getUsagesCount() >= 0;
          assert cacheEntry.getUsagesCount() > 0 || !cacheEntry.isLockAcquiredByCurrentThread();
        } finally {
          pageLock.unlock();
        }
      } finally {
        fileLock.unlock();
      }
    } finally {
      cacheLock.releaseReadLock();
    }
  }

  @Override
  public void pinPage(final OCacheEntry cacheEntry) throws IOException {
    Lock fileLock;
    Lock pageLock;

    MemoryData memoryData = memoryDataContainer.get();

    if ((100 * (memoryData.pinnedPages + 1)) / memoryData.maxSize > percentOfPinnedPages) {
      if (pinnedPagesWarningsCache < MAX_PERCENT_OF_PINED_PAGES) {
        pinnedPagesWarningCounter.increment();

        final long warnings = pinnedPagesWarningCounter.sum();
        if (warnings < MAX_PERCENT_OF_PINED_PAGES) {
          pinnedPagesWarningsCache = (int) warnings;

          OLogManager.instance().warn(this, "Maximum amount of pinned pages is reached, given page " + cacheEntry
              + " will not be marked as pinned which may lead to performance degradation. You may consider to increase the percent of pinned pages "
              + "by changing the property '" + OGlobalConfiguration.DISK_CACHE_PINNED_PAGES.getKey() + "'");
        }
      }

      return;
    }

    cacheLock.acquireReadLock();
    try {
      fileLock = fileLockManager.acquireSharedLock(cacheEntry.getFileId());
      final PageKey k = new PageKey(cacheEntry.getFileId(), cacheEntry.getPageIndex());
      try {
        pageLock = pageLockManager.acquireExclusiveLock(k);
        try {
          remove(cacheEntry.getFileId(), cacheEntry.getPageIndex());
          pinnedPages.put(new PinnedPage(cacheEntry.getFileId(), cacheEntry.getPageIndex()), cacheEntry);
        } finally {
          pageLock.unlock();
        }
      } finally {
        fileLock.unlock();
      }
    } finally {
      cacheLock.releaseReadLock();
    }

    MemoryData newMemoryData = new MemoryData(memoryData.maxSize, memoryData.pinnedPages + 1);

    while (!memoryDataContainer.compareAndSet(memoryData, newMemoryData)) {
      memoryData = memoryDataContainer.get();
      newMemoryData = new MemoryData(memoryData.maxSize, memoryData.pinnedPages + 1);
    }

    removeColdestPagesIfNeeded();
  }

  /**
   * Changes amount of memory which may be used by given cache. This method may consume many resources if amount of memory provided
   * in parameter is much less than current amount of memory.
   *
   * @param readCacheMaxMemory New maximum size of cache in bytes.
   *
   * @throws IllegalStateException In case of new size of disk cache is too small to hold existing pinned pages.
   */
  public void changeMaximumAmountOfMemory(final long readCacheMaxMemory) throws IllegalStateException {
    MemoryData memoryData;
    MemoryData newMemoryData;

    int newMemorySize = normalizeMemory(readCacheMaxMemory, pageSize);
    do {
      memoryData = memoryDataContainer.get();

      if (memoryData.maxSize == newMemorySize)
        return;

      if ((100 * memoryData.pinnedPages / newMemorySize) > percentOfPinnedPages) {
        throw new IllegalStateException("Cannot decrease amount of memory used by disk cache "
            + "because limit of pinned pages will be more than allowed limit " + percentOfPinnedPages);
      }

      newMemoryData = new MemoryData(newMemorySize, memoryData.pinnedPages);
    } while (!memoryDataContainer.compareAndSet(memoryData, newMemoryData));

    if (newMemorySize < memoryData.maxSize)
      removeColdestPagesIfNeeded();

    OLogManager.instance()
        .info(this, "Disk cache size was changed from " + memoryData.maxSize + " pages to " + newMemorySize + " pages");
  }

  private OCacheEntry doLoad(long fileId, long pageIndex, boolean checkPinnedPages, OWriteCache writeCache, int pageCount)
      throws IOException {
    final OSessionStoragePerformanceStatistic sessionStoragePerformanceStatistic = writeCache.getPerformanceStatisticManager()
        .getSessionPerformanceStatistic();

    if (sessionStoragePerformanceStatistic != null) {
      sessionStoragePerformanceStatistic.startPageReadFromCacheTimer();
    }

    try {
      fileId = OAbstractWriteCache.checkFileIdCompatibility(writeCache.getId(), fileId);

      final UpdateCacheResult cacheResult = doLoad(fileId, pageIndex, checkPinnedPages, false, writeCache, pageCount,
          sessionStoragePerformanceStatistic);
      if (cacheResult == null)
        return null;

      try {
        if (cacheResult.removeColdPages)
          removeColdestPagesIfNeeded();
      } catch (RuntimeException e) {
        assert !cacheResult.cacheEntry.isDirty();

        releaseFromWrite(cacheResult.cacheEntry, writeCache);
        throw e;
      }

      return cacheResult.cacheEntry;
    } finally {
      if (sessionStoragePerformanceStatistic != null) {
        sessionStoragePerformanceStatistic.stopPageReadFromCacheTimer();
      }
    }
  }

  private UpdateCacheResult doLoad(long fileId, long pageIndex, boolean checkPinnedPages, boolean addNewPages,
      OWriteCache writeCache, final int pageCount, final OSessionStoragePerformanceStatistic sessionStoragePerformanceStatistic)
      throws IOException {

    if (pageCount < 1)
      throw new IllegalArgumentException(
          "Amount of pages to load from cache should be not less than 1 but passed value is " + pageCount);

    boolean removeColdPages = false;
    OCacheEntry cacheEntry = null;

    Lock fileLock;
    Lock[] pageLocks;

    final OModifiableBoolean cacheHit = new OModifiableBoolean(false);

    cacheLock.acquireReadLock();
    try {
      fileLock = fileLockManager.acquireSharedLock(fileId);
      try {
        final PageKey[] pageKeys = new PageKey[pageCount];

        for (int i = 0; i < pageKeys.length; i++) {
          pageKeys[i] = new PageKey(fileId, pageIndex + i);

        }
        pageLocks = pageLockManager.acquireExclusiveLocksInBatch(pageKeys);
        try {
          if (checkPinnedPages)
            cacheEntry = pinnedPages.get(new PinnedPage(fileId, pageIndex));

          if (cacheEntry == null) {
            UpdateCacheResult cacheResult = updateCache(fileId, pageIndex, addNewPages, writeCache, pageCount, cacheHit);
            if (cacheResult == null)
              return null;

            cacheEntry = cacheResult.cacheEntry;
            removeColdPages = cacheResult.removeColdPages;
          } else {
            cacheHit.setValue(true);
          }

          cacheEntry.incrementUsages();
        } finally {
          for (Lock pageLock : pageLocks) {
            pageLock.unlock();
          }
        }
      } finally {
        fileLock.unlock();
      }
    } finally {
      cacheLock.releaseReadLock();
    }

    if (sessionStoragePerformanceStatistic != null)
      sessionStoragePerformanceStatistic.incrementPageAccessOnCacheLevel(cacheHit.getValue());

    return new UpdateCacheResult(removeColdPages, cacheEntry);
  }

  @Override
  public OCacheEntry allocateNewPage(long fileId, OWriteCache writeCache) throws IOException {
    final OSessionStoragePerformanceStatistic sessionStoragePerformanceStatistic = writeCache.getPerformanceStatisticManager()
        .getSessionPerformanceStatistic();

    if (sessionStoragePerformanceStatistic != null) {
      sessionStoragePerformanceStatistic.startPageReadFromCacheTimer();
    }

    try {
      fileId = OAbstractWriteCache.checkFileIdCompatibility(writeCache.getId(), fileId);

      UpdateCacheResult cacheResult;

      Lock fileLock;
      cacheLock.acquireReadLock();
      try {
        fileLock = fileLockManager.acquireExclusiveLock(fileId);
        try {
          final long filledUpTo = writeCache.getFilledUpTo(fileId);
          assert filledUpTo >= 0;
          cacheResult = doLoad(fileId, filledUpTo, false, true, writeCache, 1, sessionStoragePerformanceStatistic);
        } finally {
          fileLock.unlock();
        }
      } finally {
        cacheLock.releaseReadLock();
      }

      assert cacheResult != null;

      try {
        if (cacheResult.removeColdPages)
          removeColdestPagesIfNeeded();
      } catch (RuntimeException e) {
        assert !cacheResult.cacheEntry.isDirty();

        doRelease(cacheResult.cacheEntry);
        throw e;
      }

      final OCacheEntry cacheEntry = cacheResult.cacheEntry;

      if (cacheEntry != null) {
        cacheEntry.acquireExclusiveLock();
        writeCache.updateDirtyPagesTable(cacheEntry.getCachePointer());
      }

      return cacheResult.cacheEntry;
    } finally {
      if (sessionStoragePerformanceStatistic != null) {
        sessionStoragePerformanceStatistic.stopPageReadFromCacheTimer();
      }
    }
  }

  public void clear() {
    cacheLock.acquireWriteLock();
    try {
      clearCacheContent();
    } finally {
      cacheLock.releaseWriteLock();
    }
  }

  @Override
  public void truncateFile(long fileId, OWriteCache writeCache) throws IOException {
    Lock fileLock;
    fileId = OAbstractWriteCache.checkFileIdCompatibility(writeCache.getId(), fileId);

    cacheLock.acquireReadLock();
    try {
      fileLock = fileLockManager.acquireExclusiveLock(fileId);
      try {

        writeCache.truncateFile(fileId);

        clearFile(fileId);
      } finally {
        fileLock.unlock();
      }
    } finally {
      cacheLock.releaseReadLock();
    }
  }

  private void clearFile(long fileId) {
    final Set<Long> pageEntries = filePages.get(fileId);
    if (pageEntries == null || pageEntries.isEmpty()) {
      assert get(fileId, 0) == null;
      return;
    }

    for (Long pageIndex : pageEntries) {
      OCacheEntry cacheEntry = get(fileId, pageIndex);

      if (cacheEntry == null)
        cacheEntry = pinnedPages.get(new PinnedPage(fileId, pageIndex));

      if (cacheEntry != null) {
        if (cacheEntry.getUsagesCount() == 0) {
          cacheEntry = remove(fileId, pageIndex);

          if (cacheEntry == null) {
            MemoryData memoryData = memoryDataContainer.get();
            cacheEntry = pinnedPages.remove(new PinnedPage(fileId, pageIndex));

            MemoryData newMemoryData = new MemoryData(memoryData.maxSize, memoryData.pinnedPages - 1);

            while (!memoryDataContainer.compareAndSet(memoryData, newMemoryData)) {
              memoryData = memoryDataContainer.get();
              newMemoryData = new MemoryData(memoryData.maxSize, memoryData.pinnedPages - 1);
            }
          }

          final OCachePointer cachePointer = cacheEntry.getCachePointer();
          if (cachePointer != null) {
            cachePointer.decrementReadersReferrer();
            cacheEntry.clearCachePointer();
          }

        } else
          throw new OStorageException(
              "Page with index " + pageIndex + " for file with id " + fileId + " cannot be freed because it is used.");
      } else
        throw new OStorageException("Page with index " + pageIndex + " was  not found in cache for file with id " + fileId);
    }

    assert get(fileId, 0) == null;

    pageEntries.clear();
  }

  @Override
  public void closeFile(long fileId, boolean flush, OWriteCache writeCache) throws IOException {
    fileId = OAbstractWriteCache.checkFileIdCompatibility(writeCache.getId(), fileId);

    Lock fileLock;
    cacheLock.acquireReadLock();
    try {
      fileLock = fileLockManager.acquireExclusiveLock(fileId);
      try {
        writeCache.close(fileId, flush);

        clearFile(fileId);

      } finally {
        fileLock.unlock();
      }

    } finally {
      cacheLock.releaseReadLock();
    }
  }

  @Override
  public void deleteFile(long fileId, OWriteCache writeCache) throws IOException {
    fileId = OAbstractWriteCache.checkFileIdCompatibility(writeCache.getId(), fileId);

    Lock fileLock;

    cacheLock.acquireReadLock();
    try {
      fileLock = fileLockManager.acquireExclusiveLock(fileId);
      try {
        clearFile(fileId);
        filePages.remove(fileId);
        writeCache.deleteFile(fileId);
      } finally {
        fileLock.unlock();
      }
    } finally {
      cacheLock.releaseReadLock();
    }
  }

  /**
   * Performs following steps:
   * <ol>
   * <li>If flag {@link OGlobalConfiguration#STORAGE_KEEP_DISK_CACHE_STATE} is set to <code>true</code> saves state of all queues of
   * 2Q cache into file {@link #CACHE_STATE_FILE}.The only exception is pinned pages they need to pinned again.</li>
   * <li>Closes all files and flushes all data associated to them.</li>
   * </ol>
   *
   * @param writeCache Write cache all files of which should be closed. In terms of cache write cache = storage.
   */
  @Override
  public void closeStorage(OWriteCache writeCache) throws IOException {
    if (writeCache == null)
      return;

    cacheLock.acquireWriteLock();
    try {
      final long[] filesToClear = writeCache.close();

      for (long fileId : filesToClear)
        clearFile(fileId);

    } finally {
      cacheLock.releaseWriteLock();
    }
  }

  /**
   * Loads state of 2Q cache queues stored during storage close {@link #closeStorage(OWriteCache)} back into memory if flag
   * {@link OGlobalConfiguration#STORAGE_KEEP_DISK_CACHE_STATE} is set to <code>true</code>.
   * <p>
   * If maximum size of cache was decreased cache state will not be restored.
   *
   * @param writeCache Write cache is used to load pages back into cache if needed.
   *
   * @see #closeStorage(OWriteCache)
   */
  public void loadCacheState(final OWriteCache writeCache) {
    if (!OGlobalConfiguration.STORAGE_KEEP_DISK_CACHE_STATE.getValueAsBoolean()) {
      return;
    }

    cacheLock.acquireReadLock();
    try {
      final Path statePath = writeCache.getRootDirectory().resolve(CACHE_STATE_FILE);

      if (Files.exists(statePath)) {
        try (FileChannel channel = FileChannel.open(statePath, StandardOpenOption.READ)) {
          final InputStream stream = Channels.newInputStream(channel);
          final BufferedInputStream bufferedInputStream = new BufferedInputStream(stream, 64 * 1024);
          try (DataInputStream dataInputStream = new DataInputStream(bufferedInputStream)) {
            final long maxCacheSize = dataInputStream.readLong();
            final long currentMaxCacheSize = memoryDataContainer.get().maxSize;

            if (maxCacheSize > currentMaxCacheSize) {
              OLogManager.instance().info(this,
                  "Previous maximum cache size was %d current maximum cache size is %d. Cache state for storage %s will not be restored.",
                  maxCacheSize, currentMaxCacheSize, writeCache.getRootDirectory());
              return;
            }

            restoreQueue(writeCache, am, dataInputStream, true);
            restoreQueue(writeCache, a1in, dataInputStream, true);

            restoreQueue(writeCache, a1out, dataInputStream, false);
          }

        }
      }
    } catch (Exception e) {
      OLogManager.instance()
          .warn(this, "Cannot restore state of cache for storage placed under %s", writeCache.getRootDirectory(), e);
    } finally {
      cacheLock.releaseReadLock();
    }
  }

  /**
   * Following format is used to store queue state:
   * <p>
   * <ol>
   * <li>File id or -1 if end of queue is reached (int)</li>
   * <li>Page index (long), is absent if end of the queue is reached</li>
   * </ol>
   *
   * @param dataInputStream Stream of file which contains state of the cache.
   * @param queue           Queue, state of which should be restored.
   * @param loadPages       Indicates whether pages should be loaded from disk or only stubs should be added.
   * @param writeCache      Write cache is used to load data from disk if needed.
   */
  private void restoreQueue(OWriteCache writeCache, LRUList queue, DataInputStream dataInputStream, boolean loadPages)
      throws IOException {
    if (loadPages) {
      restoreQueueWithPageLoad(writeCache, queue, dataInputStream);
    } else {
      restoreQueueWithoutPageLoad(writeCache, queue, dataInputStream);
    }
  }

  /**
   * Restores queues state if it is NOT needed to load cache page from disk to cache.
   * <p>
   * Following format is used to store queue state:
   * <p>
   * <ol>
   * <li>File id or -1 if end of queue is reached (int)</li>
   * <li>Page index (long), is absent if end of the queue is reached</li>
   * </ol>
   *
   * @param dataInputStream Stream of file which contains state of the cache.
   * @param queue           Queue, state of which should be restored.
   * @param writeCache      Write cache is used to load data from disk if needed.
   */
  private void restoreQueueWithoutPageLoad(OWriteCache writeCache, LRUList queue, DataInputStream dataInputStream)
      throws IOException {
    int internalFileId = dataInputStream.readInt();
    while (internalFileId >= 0) {
      final long pageIndex = dataInputStream.readLong();
      try {
        final long fileId = writeCache.externalFileId(internalFileId);
        final OCacheEntry cacheEntry = new OCacheEntryImpl(fileId, pageIndex, null, false);

        Set<Long> pages = filePages.get(fileId);
        if (pages == null) {
          pages = new HashSet<>();

          Set<Long> op = filePages.putIfAbsent(fileId, pages);
          if (op != null) {
            pages = op;
          }
        }

        queue.putToMRU(cacheEntry);
        pages.add(cacheEntry.getPageIndex());
      } finally {
        internalFileId = dataInputStream.readInt();
      }
    }
  }

  /**
   * Restores queues state if it is needed to load cache page from disk to cache.
   * <p>
   * Following format is used to store queue state:
   * <p>
   * <ol>
   * <li>File id or -1 if end of queue is reached (int)</li>
   * <li>Page index (long), is absent if end of the queue is reached</li>
   * </ol>
   *
   * @param dataInputStream Stream of file which contains state of the cache.
   * @param queue           Queue, state of which should be restored.
   * @param writeCache      Write cache is used to load data from disk if needed.
   */
  private void restoreQueueWithPageLoad(OWriteCache writeCache, LRUList queue, DataInputStream dataInputStream) throws IOException {
    // used only for statistics, and there is passed merely as stub
    final OModifiableBoolean cacheHit = new OModifiableBoolean();

    // first step, we will create two tree maps to sort data by position in file and load them with maximum speed and
    // then to put data into the queue to restore position of entries in LRU list.

    final TreeMap<PageKey, OPair<Long, OCacheEntry>> filePositionMap = new TreeMap<>();
    final TreeMap<Long, OCacheEntry> queuePositionMap = new TreeMap<>();

    long position = 0;
    int internalFileId = dataInputStream.readInt();
    while (internalFileId >= 0) {
      final long pageIndex = dataInputStream.readLong();
      try {
        final long fileId = writeCache.externalFileId(internalFileId);
        final OCacheEntry entry = new OCacheEntryImpl(fileId, pageIndex, null, false);
        filePositionMap.put(new PageKey(fileId, pageIndex), new OPair<>(position, entry));
        queuePositionMap.put(position, entry);

        position++;
      } finally {
        internalFileId = dataInputStream.readInt();
      }
    }

    // second step: load pages sorted by position in file
    for (final Map.Entry<PageKey, OPair<Long, OCacheEntry>> entry : filePositionMap.entrySet()) {
      final PageKey pageKey = entry.getKey();
      final OPair<Long, OCacheEntry> pair = entry.getValue();

      final OCachePointer[] pointers = writeCache.load(pageKey.fileId, pageKey.pageIndex, 1, false, cacheHit);

      if (pointers.length == 0) {
        queuePositionMap.remove(pair.key);
        continue;
      }

      final OCacheEntry cacheEntry = pair.value;
      cacheEntry.setCachePointer(pointers[0]);
    }

    // third step: add pages according to their order in LRU queue
    for (final OCacheEntry cacheEntry : queuePositionMap.values()) {
      final long fileId = cacheEntry.getFileId();
      Set<Long> pages = filePages.get(fileId);
      if (pages == null) {
        pages = new HashSet<>();

        Set<Long> op = filePages.putIfAbsent(fileId, pages);
        if (op != null) {
          pages = op;
        }
      }

      queue.putToMRU(cacheEntry);
      pages.add(cacheEntry.getPageIndex());
    }
  }

  /**
   * Stores state of queues of 2Q cache inside of {@link #CACHE_STATE_FILE} file if flag
   * {@link OGlobalConfiguration#STORAGE_KEEP_DISK_CACHE_STATE} is set to <code>true</code>.
   * <p>
   * Following format is used to store queue state:
   * <p>
   * <ol>
   * <li>Max cache size, single item (long)</li>
   * <li>File id or -1 if end of queue is reached (int)</li>
   * <li>Page index (long), is absent if end of the queue is reached</li>
   * </ol>
   *
   * @param writeCache Write cache which manages files cache state of which is going to be stored.
   */
  public void storeCacheState(OWriteCache writeCache) {
    if (!OGlobalConfiguration.STORAGE_KEEP_DISK_CACHE_STATE.getValueAsBoolean()) {
      return;
    }

    if (writeCache == null)
      return;

    cacheLock.acquireWriteLock();
    try {
      final Path rootDirectory = writeCache.getRootDirectory();
      final Path stateFile = rootDirectory.resolve(CACHE_STATE_FILE);

      if (Files.exists(stateFile)) {
        Files.delete(stateFile);
      }

      final Set<Long> filesToStore = new HashSet<>(writeCache.files().values());

      try (final FileChannel channel = FileChannel.open(stateFile, StandardOpenOption.WRITE, StandardOpenOption.CREATE)) {
        final OutputStream channelStream = Channels.newOutputStream(channel);
        final BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(channelStream, 64 * 1024);

        try (DataOutputStream dataOutputStream = new DataOutputStream(bufferedOutputStream)) {
          dataOutputStream.writeLong(memoryDataContainer.get().maxSize);

          storeQueueState(writeCache, filesToStore, dataOutputStream, am);
          dataOutputStream.writeInt(-1);

          storeQueueState(writeCache, filesToStore, dataOutputStream, a1in);
          dataOutputStream.writeInt(-1);

          storeQueueState(writeCache, filesToStore, dataOutputStream, a1out);
          dataOutputStream.writeInt(-1);
        }
      }
    } catch (Exception e) {
      OLogManager.instance()
          .error(this, "Cannot store state of cache for storage placed under %s (error: %s)", writeCache.getRootDirectory(), e);
    } finally {
      cacheLock.releaseWriteLock();
    }
  }

  /**
   * Stores state of single queue to the {@link OutputStream} . Items are stored from least recently used to most recently used, so
   * in case of sequential read of data we will restore the same state of queue.
   * <p>
   * Not all queue items are stored, only ones which contains pages of selected files.
   * <p>
   * <p>
   * Following format is used to store queue state:
   * <p>
   * <ol>
   * <li>File id or -1 if end of queue is reached (int)</li>
   * <li>Page index (long), is absent if end of the queue is reached</li>
   * </ol>
   *
   * @param writeCache       Write cache is used to convert external file ids to internal ones.
   * @param filesToStore     List of files state of which should be stored.
   * @param dataOutputStream Output stream for state file
   * @param queue            Queue state of which should be stored
   */
  private static void storeQueueState(OWriteCache writeCache, Set<Long> filesToStore, DataOutputStream dataOutputStream,
      LRUList queue) throws IOException {
    final Iterator<OCacheEntry> queueIterator = queue.reverseIterator();

    while (queueIterator.hasNext()) {
      final OCacheEntry cacheEntry = queueIterator.next();

      final long fileId = cacheEntry.getFileId();
      if (filesToStore.contains(fileId)) {
        final int internalId = writeCache.internalFileId(fileId);
        dataOutputStream.writeInt(internalId);

        dataOutputStream.writeLong(cacheEntry.getPageIndex());
      }
    }
  }

  @Override
  public void deleteStorage(OWriteCache writeCache) throws IOException {
    cacheLock.acquireWriteLock();
    try {
      final long[] filesToClear = writeCache.delete();
      for (long fileId : filesToClear)
        clearFile(fileId);

      final Path rootDirectory = writeCache.getRootDirectory();
      final Path stateFile = rootDirectory.resolve(CACHE_STATE_FILE);

      if (Files.exists(stateFile)) {
        Files.delete(stateFile);
      }

    } finally {
      cacheLock.releaseWriteLock();
    }
  }

  private OCacheEntry get(long fileId, long pageIndex) {
    OCacheEntry cacheEntry = am.get(fileId, pageIndex);

    if (cacheEntry != null) {
      assert filePages.get(fileId) != null;
      assert filePages.get(fileId).contains(pageIndex);

      return cacheEntry;
    }

    cacheEntry = a1out.get(fileId, pageIndex);
    if (cacheEntry != null) {
      assert filePages.get(fileId) != null;
      assert filePages.get(fileId).contains(pageIndex);

      return cacheEntry;
    }

    cacheEntry = a1in.get(fileId, pageIndex);
    return cacheEntry;
  }

  private void clearCacheContent() {
    for (OCacheEntry cacheEntry : am)
      if (cacheEntry.getUsagesCount() == 0) {
        final OCachePointer cachePointer = cacheEntry.getCachePointer();
        cachePointer.decrementReadersReferrer();
        cacheEntry.clearCachePointer();
      } else
        throw new OStorageException("Page with index " + cacheEntry.getPageIndex() + " for file id " + cacheEntry.getFileId()
            + " is used and cannot be removed");

    for (OCacheEntry cacheEntry : a1in)
      if (cacheEntry.getUsagesCount() == 0) {
        final OCachePointer cachePointer = cacheEntry.getCachePointer();
        cachePointer.decrementReadersReferrer();
        cacheEntry.clearCachePointer();
      } else
        throw new OStorageException("Page with index " + cacheEntry.getPageIndex() + " for file id " + cacheEntry.getFileId()
            + " is used and cannot be removed");

    a1out.clear();
    am.clear();
    a1in.clear();

    for (Set<Long> pages : filePages.values())
      pages.clear();

    clearPinnedPages();
  }

  private void clearPinnedPages() {
    for (OCacheEntry pinnedEntry : pinnedPages.values()) {
      if (pinnedEntry.getUsagesCount() == 0) {
        final OCachePointer cachePointer = pinnedEntry.getCachePointer();
        cachePointer.decrementReadersReferrer();
        pinnedEntry.clearCachePointer();

        MemoryData memoryData = memoryDataContainer.get();
        MemoryData newMemoryData = new MemoryData(memoryData.maxSize, memoryData.pinnedPages - 1);

        while (!memoryDataContainer.compareAndSet(memoryData, newMemoryData)) {
          memoryData = memoryDataContainer.get();
          newMemoryData = new MemoryData(memoryData.maxSize, memoryData.pinnedPages - 1);
        }
      } else
        throw new OStorageException("Page with index " + pinnedEntry.getPageIndex() + " for file with id " + pinnedEntry.getFileId()
            + "cannot be freed because it is used.");
    }

    pinnedPages.clear();
  }

  @SuppressWarnings("SameReturnValue")
  private boolean entryIsInAmQueue(final long fileId, final long pageIndex, final OCacheEntry cacheEntry) {
    assert filePages.get(fileId) != null;
    assert filePages.get(fileId).contains(pageIndex);

    am.putToMRU(cacheEntry);

    return false;
  }

  @SuppressWarnings("SameReturnValue")
  private boolean entryWasInA1OutQueue(final long fileId, final long pageIndex, final OCachePointer dataPointer,
      final OCacheEntry cacheEntry) {
    assert filePages.get(fileId) != null;
    assert filePages.get(fileId).contains(pageIndex);

    assert dataPointer != null;
    assert cacheEntry.getCachePointer() == null;
    assert !cacheEntry.isDirty();

    cacheEntry.setCachePointer(dataPointer);

    am.putToMRU(cacheEntry);

    return true;
  }

  @SuppressWarnings("SameReturnValue")
  private boolean entryIsInA1InQueue(final long fileId, final long pageIndex) {
    assert filePages.get(fileId) != null;
    assert filePages.get(fileId).contains(pageIndex);

    return false;
  }

  private UpdateCacheResult entryIsAbsentInQueues(long fileId, long pageIndex, OCachePointer dataPointer) {
    OCacheEntry cacheEntry;
    cacheEntry = new OCacheEntryImpl(fileId, pageIndex, dataPointer, false);
    a1in.putToMRU(cacheEntry);

    Set<Long> pages = filePages.get(fileId);
    if (pages == null) {
      pages = Collections.newSetFromMap(new ConcurrentHashMap<Long, Boolean>());
      Set<Long> oldPages = filePages.putIfAbsent(fileId, pages);

      if (oldPages != null)
        pages = oldPages;
    }

    pages.add(pageIndex);
    return new UpdateCacheResult(true, cacheEntry);
  }

  private UpdateCacheResult updateCache(final long fileId, final long pageIndex, final boolean addNewPages, OWriteCache writeCache,
      final int pageCount, final OModifiableBoolean cacheHit) throws IOException {

    assert pageCount > 0;

    OCacheEntry cacheEntry = am.get(fileId, pageIndex);

    if (cacheEntry != null) {
      cacheHit.setValue(true);

      return new UpdateCacheResult(entryIsInAmQueue(fileId, pageIndex, cacheEntry), cacheEntry);
    }

    boolean removeColdPages;
    OCachePointer[] dataPointers = null;

    cacheEntry = a1out.remove(fileId, pageIndex);
    if (cacheEntry != null) {
      dataPointers = writeCache.load(fileId, pageIndex, pageCount, false, cacheHit);

      OCachePointer dataPointer = dataPointers[0];
      removeColdPages = entryWasInA1OutQueue(fileId, pageIndex, dataPointer, cacheEntry);
    } else {
      cacheEntry = a1in.get(fileId, pageIndex);

      if (cacheEntry != null) {
        removeColdPages = entryIsInA1InQueue(fileId, pageIndex);
        cacheHit.setValue(true);
      } else {
        dataPointers = writeCache.load(fileId, pageIndex, pageCount, addNewPages, cacheHit);

        if (dataPointers.length == 0)
          return null;

        OCachePointer dataPointer = dataPointers[0];
        final UpdateCacheResult ucr = entryIsAbsentInQueues(fileId, pageIndex, dataPointer);
        cacheEntry = ucr.cacheEntry;
        removeColdPages = ucr.removeColdPages;
      }
    }

    if (dataPointers != null) {
      for (int n = 1; n < dataPointers.length; n++) {
        removeColdPages = processFetchedPage(removeColdPages, dataPointers[n]);
      }
    }

    return new UpdateCacheResult(removeColdPages, cacheEntry);
  }

  private boolean processFetchedPage(boolean removeColdPages, OCachePointer dataPointer) {
    final long fileId = dataPointer.getFileId();
    final long pageIndex = dataPointer.getPageIndex();

    if (pinnedPages.containsKey(new PinnedPage(fileId, pageIndex))) {
      return removeColdPages;
    }

    OCacheEntry cacheEntry = am.get(fileId, pageIndex);
    if (cacheEntry != null) {
      final boolean rcp = entryIsInAmQueue(fileId, pageIndex, cacheEntry);
      removeColdPages = removeColdPages || rcp;
      dataPointer.decrementReadersReferrer();

      return removeColdPages;
    }

    cacheEntry = a1out.remove(fileId, pageIndex);
    if (cacheEntry != null) {
      final boolean rcp = entryWasInA1OutQueue(fileId, pageIndex, dataPointer, cacheEntry);
      removeColdPages = removeColdPages || rcp;
      return removeColdPages;
    }

    cacheEntry = a1in.get(fileId, pageIndex);
    if (cacheEntry != null) {
      final boolean rcp = entryIsInA1InQueue(fileId, pageIndex);
      removeColdPages = removeColdPages || rcp;

      dataPointer.decrementReadersReferrer();

      return removeColdPages;
    }

    final boolean rcp = entryIsAbsentInQueues(fileId, pageIndex, dataPointer).removeColdPages;
    removeColdPages = removeColdPages || rcp;
    return removeColdPages;
  }

  private void removeColdestPagesIfNeeded() {
    if (!coldPagesRemovalInProgress.compareAndSet(false, true))
      return;

    final MemoryData memoryData = this.memoryDataContainer.get();
    final boolean exclusiveCacheLock = (am.size() + a1in.size() - memoryData.get2QCacheSize()) > MAX_CACHE_OVERFLOW;

    if (exclusiveCacheLock)
      cacheLock.acquireWriteLock();
    else
      cacheLock.acquireReadLock();

    try {

      if (exclusiveCacheLock)
        removeColdPagesWithCacheLock();
      else
        removeColdPagesWithoutCacheLock();

    } finally {
      if (exclusiveCacheLock)
        cacheLock.releaseWriteLock();
      else
        cacheLock.releaseReadLock();

      coldPagesRemovalInProgress.set(false);
    }
  }

  private void removeColdPagesWithCacheLock() {
    final MemoryData memoryData = this.memoryDataContainer.get();
    while (am.size() + a1in.size() > memoryData.get2QCacheSize()) {
      if (a1in.size() > memoryData.K_IN) {
        OCacheEntry removedFromAInEntry = a1in.removeLRU();
        if (removedFromAInEntry == null) {
          throw new OAllCacheEntriesAreUsedException("All records in aIn queue in 2q cache are used!");
        } else {
          assert removedFromAInEntry.getUsagesCount() == 0;
          assert !removedFromAInEntry.isDirty();

          final OCachePointer cachePointer = removedFromAInEntry.getCachePointer();
          cachePointer.decrementReadersReferrer();
          removedFromAInEntry.clearCachePointer();

          a1out.putToMRU(removedFromAInEntry);
        }

        while (a1out.size() > memoryData.K_OUT) {
          OCacheEntry removedEntry = a1out.removeLRU();

          assert removedEntry.getUsagesCount() == 0;
          assert removedEntry.getCachePointer() == null;
          assert !removedEntry.isDirty();

          Set<Long> pageEntries = filePages.get(removedEntry.getFileId());
          pageEntries.remove(removedEntry.getPageIndex());
        }
      } else {
        OCacheEntry removedEntry = am.removeLRU();

        if (removedEntry == null) {
          throw new OAllCacheEntriesAreUsedException("All records in aIn queue in 2q cache are used!");
        } else {
          assert removedEntry.getUsagesCount() == 0;
          assert !removedEntry.isDirty();

          final OCachePointer cachePointer = removedEntry.getCachePointer();
          cachePointer.decrementReadersReferrer();
          removedEntry.clearCachePointer();

          Set<Long> pageEntries = filePages.get(removedEntry.getFileId());
          pageEntries.remove(removedEntry.getPageIndex());
        }
      }
    }
  }

  private void removeColdPagesWithoutCacheLock() {
    Lock fileLock;
    Lock pageLock;
    int iterationsCounter = 0;

    final MemoryData memoryData = this.memoryDataContainer.get();
    while (am.size() + a1in.size() > memoryData.get2QCacheSize() && iterationsCounter < 1000) {
      iterationsCounter++;

      if (a1in.size() > memoryData.K_IN) {
        OCacheEntry removedFromAInEntry = a1in.getLRU();
        if (removedFromAInEntry == null) {
          throw new OAllCacheEntriesAreUsedException("All records in aIn queue in 2q cache are used!");
        } else {
          fileLock = fileLockManager.acquireSharedLock(removedFromAInEntry.getFileId());
          try {
            final PageKey k = new PageKey(removedFromAInEntry.getFileId(), removedFromAInEntry.getPageIndex());
            pageLockManager.acquireExclusiveLock(k);
            try {
              if (a1in.get(removedFromAInEntry.getFileId(), removedFromAInEntry.getPageIndex()) == null)
                continue;

              if (removedFromAInEntry.getUsagesCount() > 0)
                continue;

              assert !removedFromAInEntry.isDirty();

              a1in.remove(removedFromAInEntry.getFileId(), removedFromAInEntry.getPageIndex());

              final OCachePointer cachePointer = removedFromAInEntry.getCachePointer();
              cachePointer.decrementReadersReferrer();
              removedFromAInEntry.clearCachePointer();

              if (OLogManager.instance().isDebugEnabled())
                OLogManager.instance().debug(this, "Moving page in disk cache from a1in to a1out area: %s", removedFromAInEntry);

              a1out.putToMRU(removedFromAInEntry);
            } finally {
              pageLockManager.releaseExclusiveLock(k);
            }
          } finally {
            fileLock.unlock();
          }
        }

        while (a1out.size() > memoryData.K_OUT) {
          OCacheEntry removedEntry = a1out.getLRU();
          fileLock = fileLockManager.acquireSharedLock(removedEntry.getFileId());
          try {
            final PageKey k = new PageKey(removedEntry.getFileId(), removedEntry.getPageIndex());
            pageLock = pageLockManager.acquireExclusiveLock(k);
            try {
              if (a1out.remove(removedEntry.getFileId(), removedEntry.getPageIndex()) == null)
                continue;

              assert removedEntry.getUsagesCount() == 0;
              assert removedEntry.getCachePointer() == null;
              assert !removedEntry.isDirty();

              Set<Long> pageEntries = filePages.get(removedEntry.getFileId());
              pageEntries.remove(removedEntry.getPageIndex());
            } finally {
              pageLock.unlock();
            }
          } finally {
            fileLock.unlock();
          }
        }
      } else {
        OCacheEntry removedEntry = am.getLRU();

        if (removedEntry == null) {
          throw new OAllCacheEntriesAreUsedException("All records in aIn queue in 2q cache are used!");
        } else {
          fileLock = fileLockManager.acquireSharedLock(removedEntry.getFileId());
          try {
            final PageKey k = new PageKey(removedEntry.getFileId(), removedEntry.getPageIndex());
            pageLock = pageLockManager.acquireExclusiveLock(k);
            try {
              if (am.get(removedEntry.getFileId(), removedEntry.getPageIndex()) == null)
                continue;

              if (removedEntry.getUsagesCount() > 0)
                continue;

              assert !removedEntry.isDirty();

              am.remove(removedEntry.getFileId(), removedEntry.getPageIndex());

              final OCachePointer cachePointer = removedEntry.getCachePointer();
              cachePointer.decrementReadersReferrer();
              removedEntry.clearCachePointer();

              Set<Long> pageEntries = filePages.get(removedEntry.getFileId());
              pageEntries.remove(removedEntry.getPageIndex());
            } finally {
              pageLock.unlock();
            }
          } finally {
            fileLock.unlock();
          }
        }
      }
    }
  }

  int getMaxSize() {
    return memoryDataContainer.get().maxSize;
  }

  @Override
  public long getUsedMemory() {
    return ((long) (am.size() + a1in.size())) * pageSize;
  }

  private OCacheEntry remove(long fileId, long pageIndex) {
    OCacheEntry cacheEntry = am.remove(fileId, pageIndex);
    if (cacheEntry != null) {
      if (cacheEntry.getUsagesCount() > 1)
        throw new IllegalStateException("Record cannot be removed because it is used!");
      return cacheEntry;
    }

    cacheEntry = a1out.remove(fileId, pageIndex);
    if (cacheEntry != null)
      return cacheEntry;

    cacheEntry = a1in.remove(fileId, pageIndex);
    if (cacheEntry != null && cacheEntry.getUsagesCount() > 1)
      throw new IllegalStateException("Record cannot be removed because it is used!");

    return cacheEntry;
  }

  private int normalizeMemory(long maxSize, int pageSize) {
    long tmpMaxSize = maxSize / pageSize;
    if (tmpMaxSize >= Integer.MAX_VALUE) {
      return Integer.MAX_VALUE;
    } else {
      return (int) tmpMaxSize;
    }
  }

  private static class PinnedPage implements Comparable<PinnedPage> {
    private final long fileId;
    private final long pageIndex;

    private PinnedPage(long fileId, long pageIndex) {
      this.fileId = fileId;
      this.pageIndex = pageIndex;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o)
        return true;
      if (o == null || getClass() != o.getClass())
        return false;

      PinnedPage that = (PinnedPage) o;

      if (fileId != that.fileId)
        return false;
      if (pageIndex != that.pageIndex)
        return false;

      return true;
    }

    @Override
    public String toString() {
      return "PinnedPage{" + "fileId=" + fileId + ", pageIndex=" + pageIndex + '}';
    }

    @Override
    public int hashCode() {
      int result = (int) (fileId ^ (fileId >>> 32));
      result = 31 * result + (int) (pageIndex ^ (pageIndex >>> 32));
      return result;
    }

    @Override
    public int compareTo(PinnedPage other) {
      if (fileId > other.fileId)
        return 1;
      if (fileId < other.fileId)
        return -1;

      if (pageIndex > other.pageIndex)
        return 1;
      if (pageIndex < other.pageIndex)
        return -1;

      return 0;
    }
  }

  private static final class PageKey implements Comparable<PageKey> {
    private final long fileId;
    private final long pageIndex;

    private PageKey(long fileId, long pageIndex) {
      this.fileId = fileId;
      this.pageIndex = pageIndex;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o)
        return true;
      if (o == null || getClass() != o.getClass())
        return false;

      PageKey pageKey = (PageKey) o;

      if (fileId != pageKey.fileId)
        return false;
      if (pageIndex != pageKey.pageIndex)
        return false;

      return true;
    }

    @Override
    public int compareTo(PageKey other) {
      if (fileId > other.fileId)
        return 1;
      if (fileId < other.fileId)
        return -1;

      if (pageIndex > other.pageIndex)
        return 1;
      if (pageIndex < other.pageIndex)
        return -1;

      return 0;
    }

    @Override
    public int hashCode() {
      int result = (int) (fileId ^ (fileId >>> 32));
      result = 31 * result + (int) (pageIndex ^ (pageIndex >>> 32));
      return result;
    }
  }

  private final static class UpdateCacheResult {
    private final boolean     removeColdPages;
    private final OCacheEntry cacheEntry;

    private UpdateCacheResult(boolean removeColdPages, OCacheEntry cacheEntry) {
      this.removeColdPages = removeColdPages;
      this.cacheEntry = cacheEntry;
    }
  }

  /**
   * That is immutable class which contains information about current memory limits for 2Q cache.
   * <p>
   * This class is needed to change all parameters atomically when cache memory limits are changed outside of 2Q cache.
   */
  private static final class MemoryData {
    /**
     * Max size for {@link O2QCache#a1in} queue in amount of pages
     */
    private final int K_IN;

    /**
     * Max size for {@link O2QCache#a1out} queue in amount of pages
     */
    private final int K_OUT;

    /**
     * Maximum size of memory consumed by 2Q cache in amount of pages.
     */
    private final int maxSize;

    /**
     * Memory consumed by pinned pages in amount of pages.
     */
    private final int pinnedPages;

    MemoryData(int maxSize, int pinnedPages) {
      K_IN = (maxSize - pinnedPages) >> 2;
      K_OUT = (maxSize - pinnedPages) >> 1;

      this.maxSize = maxSize;
      this.pinnedPages = pinnedPages;
    }

    /**
     * @return Maximum size of memory which may be consumed by all 2Q queues in amount of pages.
     */
    int get2QCacheSize() {
      return maxSize - pinnedPages;
    }
  }
}
