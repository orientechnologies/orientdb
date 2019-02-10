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

import com.orientechnologies.common.concur.lock.OInterruptedException;
import com.orientechnologies.common.concur.lock.OPartitionedLockManager;
import com.orientechnologies.common.concur.lock.OReadersWriterSpinLock;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.types.OModifiableBoolean;
import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.exception.OAllCacheEntriesAreUsedException;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.storage.cache.OAbstractWriteCache;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cache.OCacheEntryImpl;
import com.orientechnologies.orient.core.storage.cache.OCachePointer;
import com.orientechnologies.orient.core.storage.cache.OReadCache;
import com.orientechnologies.orient.core.storage.cache.OWriteCache;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 7/24/13
 */
public final class O2QCache implements OReadCache {
  /**
   * Maximum amount of times when we will show message that limit of pinned pages was exhausted.
   */
  private static final int MAX_AMOUNT_OF_WARNINGS_PINNED_PAGES = 10;

  /**
   * Maximum percent of pinned pages which may be contained in this cache.
   */
  private static final int MAX_PERCENT_OF_PINED_PAGES = 50;

  /**
   * Minimum size of memory which may be allocated by cache (in pages). This parameter is used only if related flag is set in
   * constrictor of cache.
   */
  public static final int MIN_CACHE_SIZE = 256;

  /**
   * File which contains stored state of disk cache after storage close.
   */
  public static final String CACHE_STATE_FILE = "cache.stt";

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
  private final AtomicInteger pinnedPagesWarningCounter = new AtomicInteger();

  private final AtomicReference<MemoryData> memoryDataContainer = new AtomicReference<>();

  /**
   * Maximum percent of pinned pages which may be hold in this cache.
   *
   * @see com.orientechnologies.orient.core.config.OGlobalConfiguration#DISK_CACHE_PINNED_PAGES
   */
  private final int percentOfPinnedPages;

  private final LongAdder cacheRequests = new LongAdder();
  private final LongAdder cacheHits     = new LongAdder();

  private final Lock                                   evictionLock    = new ReentrantLock();
  private final OReadersWriterSpinLock                 cacheLock       = new OReadersWriterSpinLock();
  private final OPartitionedLockManager<PageKey>       pageLockManager = new OPartitionedLockManager<>();
  private final ConcurrentMap<PinnedPage, OCacheEntry> pinnedPages     = new ConcurrentHashMap<>();

  /**
   * @param readCacheMaxMemory   Maximum amount of direct memory which can allocated by disk cache in bytes.
   * @param pageSize             Cache page size in bytes.
   * @param checkMinSize         If this flat is set size of cache may be {@link #MIN_CACHE_SIZE} or bigger.
   * @param percentOfPinnedPages Maximum percent of pinned pages which may be hold by this cache.
   *
   * @see #MAX_PERCENT_OF_PINED_PAGES
   */
  public O2QCache(final long readCacheMaxMemory, final int pageSize, final boolean checkMinSize, final int percentOfPinnedPages,
      final boolean printCacheStatistics, final int cacheStatisticsInterval) {
    if (percentOfPinnedPages > MAX_PERCENT_OF_PINED_PAGES) {
      throw new IllegalArgumentException(
          "Percent of pinned pages cannot be more than " + percentOfPinnedPages + " but passed value is " + percentOfPinnedPages);
    }

    this.percentOfPinnedPages = percentOfPinnedPages;

    cacheLock.acquireWriteLock();
    try {
      this.pageSize = pageSize;

      int normalizedSize = normalizeMemory(readCacheMaxMemory, pageSize);

      if (checkMinSize && normalizedSize < MIN_CACHE_SIZE) {
        normalizedSize = MIN_CACHE_SIZE;
      }

      final MemoryData memoryData = new MemoryData(normalizedSize, 0);
      this.memoryDataContainer.set(memoryData);

      am = new ConcurrentLRUList();
      a1out = new ConcurrentLRUList();
      a1in = new ConcurrentLRUList();

      if (printCacheStatistics) {
        Orient.instance().scheduleTask(new TimerTask() {
          @Override
          public void run() {
            final long cacheRequests = O2QCache.this.cacheRequests.sum();
            final long cacheHits = O2QCache.this.cacheHits.sum();

            final MemoryData memoryData = memoryDataContainer.get();

            OLogManager.instance().infoNoDb(this, "Read cache stat: cache hits %d percents, cache size is %d percent",
                cacheRequests > 0 ? 100 * cacheHits / cacheRequests : -1,
                100 * (am.size() + a1in.size() + memoryData.pinnedPages) / memoryData.maxSize);

            O2QCache.this.cacheRequests.add(-cacheRequests);
            O2QCache.this.cacheHits.add(-cacheHits);
          }
        }, cacheStatisticsInterval * 1_000L, cacheStatisticsInterval * 1_000L);
      }
    } finally {
      cacheLock.releaseWriteLock();
    }
  }

  LRUList getAm() {
    return am;
  }

  @SuppressWarnings("SameParameterValue")
  boolean inPinnedPages(final long fileId, final long pageIndex) {
    return pinnedPages.containsKey(new PinnedPage(fileId, pageIndex));
  }

  LRUList getA1out() {
    return a1out;
  }

  LRUList getA1in() {
    return a1in;
  }

  @Override
  public final long addFile(final String fileName, final OWriteCache writeCache) throws IOException {
    cacheLock.acquireWriteLock();
    try {
      return writeCache.addFile(fileName);
    } finally {
      cacheLock.releaseWriteLock();
    }
  }

  @Override
  public final long addFile(final String fileName, long fileId, final OWriteCache writeCache) throws IOException {
    fileId = OAbstractWriteCache.checkFileIdCompatibility(writeCache.getId(), fileId);

    cacheLock.acquireWriteLock();
    try {
      return writeCache.addFile(fileName, fileId);
    } finally {
      cacheLock.releaseWriteLock();
    }
  }

  @Override
  public final OCacheEntry loadForWrite(final long fileId, final long pageIndex, final boolean checkPinnedPages,
      final OWriteCache writeCache, final int pageCount, final boolean verifyChecksums, final OLogSequenceNumber startLSN)
      throws IOException {
    final OCacheEntry cacheEntry = doLoad(fileId, pageIndex, checkPinnedPages, writeCache, pageCount, verifyChecksums);

    if (cacheEntry != null) {
      cacheEntry.acquireExclusiveLock();
      writeCache.updateDirtyPagesTable(cacheEntry.getCachePointer(), startLSN);
    }

    return cacheEntry;
  }

  @Override
  public final void releaseFromWrite(final OCacheEntry cacheEntry, final OWriteCache writeCache) {
    final OCachePointer cachePointer = cacheEntry.getCachePointer();
    assert cachePointer != null;

    final Lock pageLock;
    cacheLock.acquireReadLock();
    try {
      pageLock = pageLockManager.acquireExclusiveLock(new PageKey(cacheEntry.getFileId(), cacheEntry.getPageIndex()));
      try {
        cacheEntry.decrementUsages();

        assert cacheEntry.getUsagesCount() >= 0;

        if (cacheEntry.getUsagesCount() == 0) {
          writeCache.store(cacheEntry.getFileId(), cacheEntry.getPageIndex(), cacheEntry.getCachePointer());
        }
      } finally {
        pageLock.unlock();
      }
    } finally {
      cacheLock.releaseReadLock();
    }

    //We need to release exclusive lock from cache pointer after we put it into the write cache so both "dirty pages" of write
    //cache and write cache itself will contain actual values simultaneously. But because cache entry can be cleared after we put it back to the
    //read cache we make copy of cache pointer before head.
    //
    //Following situation can happen, if we release exclusive lock before we put entry to the write cache.
    //1. Page is loaded for write, locked and related LSN is written to the "dirty pages" table.
    //2. Page lock is released.
    //3. Page is chosen to be flushed on disk and its entry removed from "dirty pages" table
    //4. Page is added to write cache as dirty
    //
    //So we have situation when page is added as dirty into the write cache but its related entry in "dirty pages" table is removed
    //it is treated as flushed during fuzzy checkpoint and portion of write ahead log which contains not flushed changes is removed.
    //This can lead to the data loss after restore and corruption of data structures
    cachePointer.releaseExclusiveLock();
  }

  @Override
  public final OCacheEntry loadForRead(final long fileId, final long pageIndex, final boolean checkPinnedPages,
      final OWriteCache writeCache, final int pageCount, final boolean verifyChecksums) throws IOException {
    return doLoad(fileId, pageIndex, checkPinnedPages, writeCache, pageCount, verifyChecksums);
  }

  @Override
  public final void releaseFromRead(final OCacheEntry cacheEntry, final OWriteCache writeCache) {
    doRelease(cacheEntry);
  }

  private void doRelease(final OCacheEntry cacheEntry) {
    final Lock pageLock;
    cacheLock.acquireReadLock();
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
      cacheLock.releaseReadLock();
    }
  }

  @Override
  public final void pinPage(final OCacheEntry cacheEntry, final OWriteCache writeCache) {
    final Lock pageLock;

    MemoryData memoryData = memoryDataContainer.get();

    if ((100 * (memoryData.pinnedPages + 1)) / memoryData.maxSize > percentOfPinnedPages) {
      if (pinnedPagesWarningCounter.get() < MAX_AMOUNT_OF_WARNINGS_PINNED_PAGES) {

        final long warnings = pinnedPagesWarningCounter.getAndIncrement();
        if (warnings < MAX_AMOUNT_OF_WARNINGS_PINNED_PAGES) {
          OLogManager.instance().warn(this, "Maximum amount of pinned pages is reached, given page " + cacheEntry
              + " will not be marked as pinned which may lead to performance degradation. You may consider to increase the percent of pinned pages "
              + "by changing the property '" + OGlobalConfiguration.DISK_CACHE_PINNED_PAGES.getKey() + "'");
        }
      }

      return;
    }

    cacheLock.acquireReadLock();
    try {
      final PageKey k = new PageKey(cacheEntry.getFileId(), cacheEntry.getPageIndex());
      pageLock = pageLockManager.acquireExclusiveLock(k);
      try {
        remove(cacheEntry.getFileId(), cacheEntry.getPageIndex());
        pinnedPages.put(new PinnedPage(cacheEntry.getFileId(), cacheEntry.getPageIndex()), cacheEntry);
      } finally {
        pageLock.unlock();
      }

      MemoryData newMemoryData = new MemoryData(memoryData.maxSize, memoryData.pinnedPages + 1);

      while (!memoryDataContainer.compareAndSet(memoryData, newMemoryData)) {
        memoryData = memoryDataContainer.get();
        newMemoryData = new MemoryData(memoryData.maxSize, memoryData.pinnedPages + 1);
      }

      removeColdestPagesIfNeeded(writeCache);
    } finally {
      cacheLock.releaseReadLock();
    }

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

    final int newMemorySize = normalizeMemory(readCacheMaxMemory, pageSize);
    do {
      memoryData = memoryDataContainer.get();

      if (memoryData.maxSize == newMemorySize) {
        return;
      }

      if ((100 * memoryData.pinnedPages / newMemorySize) > percentOfPinnedPages) {
        throw new IllegalStateException("Cannot decrease amount of memory used by disk cache "
            + "because limit of pinned pages will be more than allowed limit " + percentOfPinnedPages);
      }

      newMemoryData = new MemoryData(newMemorySize, memoryData.pinnedPages);
    } while (!memoryDataContainer.compareAndSet(memoryData, newMemoryData));

//    if (newMemorySize < memoryData.maxSize)
//      removeColdestPagesIfNeeded();

    OLogManager.instance()
        .info(this, "Disk cache size was changed from " + memoryData.maxSize + " pages to " + newMemorySize + " pages");
  }

  private OCacheEntry doLoad(long fileId, final long pageIndex, final boolean checkPinnedPages, final OWriteCache writeCache,
      final int pageCount, final boolean verifyChecksums) throws IOException {
    final OModifiableBoolean cacheHit = new OModifiableBoolean(false);
    fileId = OAbstractWriteCache.checkFileIdCompatibility(writeCache.getId(), fileId);

    final UpdateCacheResult cacheResult;
    cacheLock.acquireReadLock();
    try {
      cacheResult = doLoad(fileId, pageIndex, checkPinnedPages, writeCache, pageCount, verifyChecksums, cacheHit);
      if (cacheResult == null) {
        return null;
      }

      try {
        if (cacheResult.removeColdPages) {
          removeColdestPagesIfNeeded(writeCache);
        }
      } catch (final RuntimeException e) {
        releaseFromWrite(cacheResult.cacheEntry, writeCache);
        throw e;
      }

    } finally {
      cacheLock.releaseReadLock();
    }
    cacheRequests.increment();

    if (cacheHit.getValue()) {
      cacheHits.increment();
    }
    return cacheResult.cacheEntry;
  }

  private UpdateCacheResult doLoad(final long fileId, final long pageIndex, final boolean checkPinnedPages,
      final OWriteCache writeCache, final int pageCount, final boolean verifyChecksums, final OModifiableBoolean cacheHit)
      throws IOException {

    if (pageCount < 1) {
      throw new IllegalArgumentException(
          "Amount of pages to load from cache should be not less than 1 but passed value is " + pageCount);
    }

    boolean removeColdPages = false;
    OCacheEntry cacheEntry = null;

    final Lock[] pageLocks;

    final PageKey[] pageKeys = new PageKey[pageCount];

    for (int i = 0; i < pageKeys.length; i++) {
      pageKeys[i] = new PageKey(fileId, pageIndex + i);
    }

    if (checkPinnedPages) {
      cacheEntry = pinnedPages.get(new PinnedPage(fileId, pageIndex));

      if (cacheEntry != null) {
        cacheHit.setValue(true);
        cacheEntry.incrementUsages();
        return new UpdateCacheResult(false, cacheEntry);
      }
    }
    pageLocks = pageLockManager.acquireExclusiveLocksInBatch(pageKeys);
    try {
      if (checkPinnedPages) {
        cacheEntry = pinnedPages.get(new PinnedPage(fileId, pageIndex));
      }

      if (cacheEntry == null) {
        final UpdateCacheResult cacheResult = updateCache(fileId, pageIndex, writeCache, pageCount, cacheHit, verifyChecksums);
        if (cacheResult == null) {
          return null;
        }

        cacheEntry = cacheResult.cacheEntry;
        removeColdPages = cacheResult.removeColdPages;
      } else {
        cacheHit.setValue(true);
      }

      cacheEntry.incrementUsages();
    } finally {
      for (final Lock pageLock : pageLocks) {
        pageLock.unlock();
      }
    }

    return new UpdateCacheResult(removeColdPages, cacheEntry);
  }

  @Override
  public final OCacheEntry allocateNewPage(long fileId, final OWriteCache writeCache, final OLogSequenceNumber startLSN)
      throws IOException {
    fileId = OAbstractWriteCache.checkFileIdCompatibility(writeCache.getId(), fileId);

    UpdateCacheResult cacheResult;

    final OModifiableBoolean cacheHit = new OModifiableBoolean(false);
    cacheLock.acquireReadLock();
    try {
      final long pageIndex = writeCache.allocateNewPage(fileId);
      cacheResult = doLoad(fileId, pageIndex, false, writeCache, 1, false, cacheHit);

      assert cacheResult != null;
      try {
        if (cacheResult.removeColdPages) {
          removeColdestPagesIfNeeded(writeCache);
        }
      } catch (final RuntimeException e) {
        doRelease(cacheResult.cacheEntry);
        throw e;
      }
    } finally {
      cacheLock.releaseReadLock();
    }

    final OCacheEntry cacheEntry = cacheResult.cacheEntry;

    if (cacheEntry != null) {
      cacheEntry.acquireExclusiveLock();
      writeCache.updateDirtyPagesTable(cacheEntry.getCachePointer(), startLSN);
    }

    cacheRequests.increment();
    cacheHits.increment();

    return cacheResult.cacheEntry;
  }

  @Override
  public final void clear() {
    cacheLock.acquireWriteLock();
    try {
      clearCacheContent();
    } finally {
      cacheLock.releaseWriteLock();
    }
  }

  @Override
  public final void truncateFile(long fileId, final OWriteCache writeCache) throws IOException {
    fileId = OAbstractWriteCache.checkFileIdCompatibility(writeCache.getId(), fileId);

    cacheLock.acquireWriteLock();
    try {
      writeCache.truncateFile(fileId);
      clearFiles(writeCache, fileId);
    } finally {
      cacheLock.releaseWriteLock();
    }
  }

  private void clearFiles(final OWriteCache writeCache, final long... fileIds) {
    final Set<Long> filesToClear = new HashSet<>(fileIds.length);
    for (final long fileId : fileIds) {
      filesToClear.add(fileId);
    }

    clearPinnedPages(writeCache, filesToClear);

    clearPersistentQueue(filesToClear, am, writeCache);
    clearPersistentQueue(filesToClear, a1in, writeCache);

    clearGhostQueue(filesToClear);
  }

  private void clearGhostQueue(final Set<Long> filesToClear) {
    final Iterator<OCacheEntry> queueIterator = a1out.iterator();
    final Set<ORawPair<Long, Long>> pagesToRemove = new HashSet<>(1024);

    while (queueIterator.hasNext()) {
      final OCacheEntry cacheEntry = queueIterator.next();
      if (filesToClear.contains(cacheEntry.getFileId())) {
        if (cacheEntry.getUsagesCount() == 0) {
          pagesToRemove.add(new ORawPair<>(cacheEntry.getFileId(), cacheEntry.getPageIndex()));
        } else {
          throw new OStorageException("Page with index " + cacheEntry.getPageIndex() + " for file with id " + cacheEntry.getFileId()
              + " cannot be freed because it is used.");
        }
      }
    }

    for (final ORawPair<Long, Long> page : pagesToRemove) {
      final OCacheEntry cacheEntry = a1out.remove(page.getFirst(), page.getSecond());

      assert cacheEntry.getUsagesCount() == 0;
    }
  }

  private void clearPinnedPages(final OWriteCache writeCache, final Set<Long> filesToClear) {
    final Iterator<Map.Entry<PinnedPage, OCacheEntry>> pinnedPagesIterator = pinnedPages.entrySet().iterator();
    while (pinnedPagesIterator.hasNext()) {
      final Map.Entry<PinnedPage, OCacheEntry> entry = pinnedPagesIterator.next();
      final PinnedPage page = entry.getKey();
      if (filesToClear.contains(entry.getKey().fileId)) {
        final OCacheEntry cacheEntry = entry.getValue();

        if (cacheEntry.getUsagesCount() == 0) {
          pinnedPagesIterator.remove();

          MemoryData memoryData = memoryDataContainer.get();
          MemoryData newMemoryData = new MemoryData(memoryData.maxSize, memoryData.pinnedPages - 1);

          while (!memoryDataContainer.compareAndSet(memoryData, newMemoryData)) {
            memoryData = memoryDataContainer.get();
            newMemoryData = new MemoryData(memoryData.maxSize, memoryData.pinnedPages - 1);
          }

          final OCachePointer cachePointer = cacheEntry.getCachePointer();
          if (cachePointer != null) {
            cachePointer.decrementReadersReferrer();
            cacheEntry.clearCachePointer();

            try {
              writeCache.checkCacheOverflow();
            } catch (final InterruptedException e) {
              throw OException.wrapException(new OInterruptedException("Check of write cache overflow was interrupted"), e);
            }
          }
        } else {
          throw new OStorageException(
              "Page with index " + page.pageIndex + " for file with id " + page.fileId + " cannot be freed because it is used.");
        }
      }
    }
  }

  private static void clearPersistentQueue(final Set<Long> filesToClear, final LRUList queue, final OWriteCache writeCache) {
    final Iterator<OCacheEntry> queueIterator = queue.iterator();
    final Set<ORawPair<Long, Long>> pagesToRemove = new HashSet<>(1024);

    while (queueIterator.hasNext()) {
      final OCacheEntry cacheEntry = queueIterator.next();
      if (filesToClear.contains(cacheEntry.getFileId())) {
        if (cacheEntry.getUsagesCount() == 0) {
          pagesToRemove.add(new ORawPair<>(cacheEntry.getFileId(), cacheEntry.getPageIndex()));
        } else {
          throw new OStorageException("Page with index " + cacheEntry.getPageIndex() + " for file with id " + cacheEntry.getFileId()
              + " cannot be freed because it is used.");
        }
      }
    }

    for (final ORawPair<Long, Long> page : pagesToRemove) {
      final OCacheEntry cacheEntry = queue.remove(page.getFirst(), page.getSecond());

      assert cacheEntry.getUsagesCount() == 0;

      final OCachePointer cachePointer = cacheEntry.getCachePointer();
      if (cachePointer != null) {
        cachePointer.decrementReadersReferrer();
        cacheEntry.clearCachePointer();

        try {
          writeCache.checkCacheOverflow();
        } catch (final InterruptedException e) {
          throw OException.wrapException(new OInterruptedException("Check of write cache overflow was interrupted"), e);
        }
      }
    }
  }

  @Override
  public final void closeFile(long fileId, final boolean flush, final OWriteCache writeCache) {
    fileId = OAbstractWriteCache.checkFileIdCompatibility(writeCache.getId(), fileId);

    cacheLock.acquireWriteLock();
    try {
      writeCache.close(fileId, flush);
      clearFiles(writeCache, fileId);
    } finally {
      cacheLock.releaseWriteLock();
    }
  }

  @Override
  public final void deleteFile(long fileId, final OWriteCache writeCache) throws IOException {
    fileId = OAbstractWriteCache.checkFileIdCompatibility(writeCache.getId(), fileId);

    cacheLock.acquireWriteLock();
    try {
      clearFiles(writeCache, fileId);
      writeCache.deleteFile(fileId);
    } finally {
      cacheLock.releaseWriteLock();
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
  public final void closeStorage(final OWriteCache writeCache) throws IOException {
    if (writeCache == null) {
      return;
    }

    cacheLock.acquireWriteLock();
    try {
      final long[] filesToClear = writeCache.close();
      clearFiles(writeCache, filesToClear);
    } finally {
      cacheLock.releaseWriteLock();
    }
  }

  /**
   * Loads state of 2Q cache queues stored during storage close {@link #closeStorage(OWriteCache)} back into memory if flag
   * {@link OGlobalConfiguration#STORAGE_KEEP_DISK_CACHE_STATE} is set to <code>true</code>.
   * If maximum size of cache was decreased cache state will not be restored.
   *
   * @param writeCache Write cache is used to load pages back into cache if needed.
   *
   * @see #closeStorage(OWriteCache)
   */
  @Override
  public final void loadCacheState(final OWriteCache writeCache) {
  }

  /**
   * Stores state of queues of 2Q cache inside of {@link #CACHE_STATE_FILE} file if flag
   * {@link OGlobalConfiguration#STORAGE_KEEP_DISK_CACHE_STATE} is set to <code>true</code>.
   * Following format is used to store queue state:
   * <ol>
   * <li>Max cache size, single item (long)</li>
   * <li>File id or -1 if end of queue is reached (int)</li>
   * <li>Page index (long), is absent if end of the queue is reached</li>
   * </ol>
   *
   * @param writeCache Write cache which manages files cache state of which is going to be stored.
   */
  @Override
  public final void storeCacheState(final OWriteCache writeCache) {
    if (!OGlobalConfiguration.STORAGE_KEEP_DISK_CACHE_STATE.getValueAsBoolean()) {
      return;
    }

    if (writeCache == null) {
      return;
    }

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

        try (final DataOutputStream dataOutputStream = new DataOutputStream(bufferedOutputStream)) {
          dataOutputStream.writeLong(memoryDataContainer.get().maxSize);

          storeQueueState(writeCache, filesToStore, dataOutputStream, am);
          dataOutputStream.writeInt(-1);

          storeQueueState(writeCache, filesToStore, dataOutputStream, a1in);
          dataOutputStream.writeInt(-1);

          storeQueueState(writeCache, filesToStore, dataOutputStream, a1out);
          dataOutputStream.writeInt(-1);
        }
      }
    } catch (final Exception e) {
      OLogManager.instance()
          .error(this, "Cannot store state of cache for storage placed under %s", e, writeCache.getRootDirectory());
    } finally {
      cacheLock.releaseWriteLock();
    }
  }

  /**
   * Stores state of single queue to the {@link OutputStream} . Items are stored from least recently used to most recently used, so
   * in case of sequential read of data we will restore the same state of queue.
   * Not all queue items are stored, only ones which contains pages of selected files.
   * Following format is used to store queue state:
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
  private static void storeQueueState(final OWriteCache writeCache, final Set<Long> filesToStore,
      final DataOutputStream dataOutputStream, final LRUList queue) throws IOException {
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
  public final void deleteStorage(final OWriteCache writeCache) throws IOException {
    cacheLock.acquireWriteLock();
    try {
      final long[] filesToClear = writeCache.delete();
      clearFiles(writeCache, filesToClear);

      final Path rootDirectory = writeCache.getRootDirectory();
      final Path stateFile = rootDirectory.resolve(CACHE_STATE_FILE);

      if (Files.exists(stateFile)) {
        Files.delete(stateFile);
      }
    } finally {
      cacheLock.releaseWriteLock();
    }
  }

  private void clearCacheContent() {
    for (final OCacheEntry cacheEntry : am) {
      if (cacheEntry.getUsagesCount() == 0) {
        final OCachePointer cachePointer = cacheEntry.getCachePointer();
        cachePointer.decrementReadersReferrer();
        cacheEntry.clearCachePointer();
      } else {
        throw new OStorageException("Page with index " + cacheEntry.getPageIndex() + " for file id " + cacheEntry.getFileId()
            + " is used and cannot be removed");
      }
    }

    for (final OCacheEntry cacheEntry : a1in) {
      if (cacheEntry.getUsagesCount() == 0) {
        final OCachePointer cachePointer = cacheEntry.getCachePointer();
        cachePointer.decrementReadersReferrer();
        cacheEntry.clearCachePointer();
      } else {
        throw new OStorageException("Page with index " + cacheEntry.getPageIndex() + " for file id " + cacheEntry.getFileId()
            + " is used and cannot be removed");
      }
    }

    a1out.clear();
    am.clear();
    a1in.clear();

    clearPinnedPages();
  }

  private void clearPinnedPages() {
    for (final OCacheEntry pinnedEntry : pinnedPages.values()) {
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
      } else {
        throw new OStorageException("Page with index " + pinnedEntry.getPageIndex() + " for file with id " + pinnedEntry.getFileId()
            + "cannot be freed because it is used.");
      }
    }

    pinnedPages.clear();
  }

  @SuppressWarnings("SameReturnValue")
  private boolean entryIsInAmQueue(final OCacheEntry cacheEntry) {
    am.putToMRU(cacheEntry);

    return false;
  }

  @SuppressWarnings("SameReturnValue")
  private boolean entryWasInA1OutQueue(final OCachePointer dataPointer, final OCacheEntry cacheEntry) {
    assert dataPointer != null;
    assert cacheEntry.getCachePointer() == null;

    cacheEntry.setCachePointer(dataPointer);

    am.putToMRU(cacheEntry);

    return true;
  }

  private UpdateCacheResult entryIsAbsentInQueues(final long fileId, final long pageIndex, final OCachePointer dataPointer) {
    final OCacheEntry cacheEntry;
    cacheEntry = new OCacheEntryImpl(fileId, pageIndex, dataPointer);
    a1in.putToMRU(cacheEntry);
    return new UpdateCacheResult(true, cacheEntry);
  }

  private UpdateCacheResult updateCache(final long fileId, final long pageIndex, final OWriteCache writeCache, final int pageCount,
      final OModifiableBoolean cacheHit, final boolean verifyChecksums) throws IOException {

    assert pageCount > 0;

    OCacheEntry cacheEntry = am.get(fileId, pageIndex);

    if (cacheEntry != null) {
      cacheHit.setValue(true);

      return new UpdateCacheResult(entryIsInAmQueue(cacheEntry), cacheEntry);
    }

    boolean removeColdPages;
    OCachePointer[] dataPointers = null;

    cacheEntry = a1out.remove(fileId, pageIndex);
    if (cacheEntry != null) {
      dataPointers = writeCache.load(fileId, pageIndex, pageCount, cacheHit, verifyChecksums);

      final OCachePointer dataPointer = dataPointers[0];
      removeColdPages = entryWasInA1OutQueue(dataPointer, cacheEntry);
    } else {
      cacheEntry = a1in.get(fileId, pageIndex);

      if (cacheEntry != null) {
        removeColdPages = false;
        cacheHit.setValue(true);
      } else {
        dataPointers = writeCache.load(fileId, pageIndex, pageCount, cacheHit, verifyChecksums);

        if (dataPointers.length == 0) {
          return null;
        }

        final OCachePointer dataPointer = dataPointers[0];
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

  private boolean processFetchedPage(boolean removeColdPages, final OCachePointer dataPointer) {
    final long fileId = dataPointer.getFileId();
    final long pageIndex = dataPointer.getPageIndex();

    if (pinnedPages.containsKey(new PinnedPage(fileId, pageIndex))) {
      dataPointer.decrementReadersReferrer();

      return removeColdPages;
    }

    OCacheEntry cacheEntry = am.get(fileId, pageIndex);
    if (cacheEntry != null) {
      final boolean rcp = entryIsInAmQueue(cacheEntry);
      removeColdPages = removeColdPages || rcp;
      dataPointer.decrementReadersReferrer();

      return removeColdPages;
    }

    cacheEntry = a1out.remove(fileId, pageIndex);
    if (cacheEntry != null) {
      final boolean rcp = entryWasInA1OutQueue(dataPointer, cacheEntry);
      removeColdPages = removeColdPages || rcp;
      return removeColdPages;
    }

    cacheEntry = a1in.get(fileId, pageIndex);
    if (cacheEntry != null) {
      dataPointer.decrementReadersReferrer();

      return removeColdPages;
    }

    final boolean rcp = entryIsAbsentInQueues(fileId, pageIndex, dataPointer).removeColdPages;
    removeColdPages = removeColdPages || rcp;
    return removeColdPages;
  }

  private void removeColdestPagesIfNeeded(final OWriteCache writeCache) {
    final MemoryData memoryData = this.memoryDataContainer.get();
    final int cacheMaxSize = memoryData.get2QCacheSize();
    final int diff = am.size() + a1in.size() - cacheMaxSize;

    if (diff <= 0) {
      return;
    }

    if (diff < 0.05 * cacheMaxSize) {
      if (!evictionLock.tryLock()) {
        return;
      }
    } else {
      evictionLock.lock();
    }
    try {
      int counter = 0;
      //remove at once 5% of staled pages to decrease contention
      while (counter < diff) {
        try {
          writeCache.checkCacheOverflow();
        } catch (final InterruptedException e) {
          throw OException.wrapException(new OInterruptedException("Check of write cache overflow was interrupted"), e);
        }

        if (a1in.size() > memoryData.K_IN * 0.95) {
          OCacheEntry removedFromAInEntry = a1in.getLRU();
          if (removedFromAInEntry == null) {
            throw new OAllCacheEntriesAreUsedException("All records in aIn queue in 2q cache are used!");
          }

          {
            final Lock lock = pageLockManager
                .acquireExclusiveLock(new PageKey(removedFromAInEntry.getFileId(), removedFromAInEntry.getPageIndex()));
            try {
              removedFromAInEntry = a1in.get(removedFromAInEntry.getFileId(), removedFromAInEntry.getPageIndex());
              if (removedFromAInEntry != null && removedFromAInEntry.getUsagesCount() == 0) {
                a1in.remove(removedFromAInEntry.getFileId(), removedFromAInEntry.getPageIndex());
                final OCachePointer cachePointer = removedFromAInEntry.getCachePointer();
                cachePointer.decrementReadersReferrer();
                removedFromAInEntry.clearCachePointer();
                a1out.putToMRU(removedFromAInEntry);

                counter++;
              } else {
                continue;
              }
            } finally {
              lock.unlock();
            }
          }

          while (a1out.size() > memoryData.K_OUT) {
            OCacheEntry removedEntry = a1out.getLRU();
            if (removedEntry != null) {
              final Lock lock = pageLockManager
                  .acquireExclusiveLock(new PageKey(removedEntry.getFileId(), removedEntry.getPageIndex()));
              try {
                removedEntry = a1out.get(removedEntry.getFileId(), removedEntry.getPageIndex());
                if (removedEntry != null && removedEntry.getUsagesCount() == 0) {
                  a1out.remove(removedEntry.getFileId(), removedEntry.getPageIndex());
                }
              } finally {
                lock.unlock();
              }
            }
          }
        } else {
          OCacheEntry removedEntry = am.getLRU();
          if (removedEntry == null) {
            throw new OAllCacheEntriesAreUsedException("All records in aIn queue in 2q cache are used!");
          }

          final Lock lock = pageLockManager
              .acquireExclusiveLock(new PageKey(removedEntry.getFileId(), removedEntry.getPageIndex()));
          try {
            removedEntry = am.get(removedEntry.getFileId(), removedEntry.getPageIndex());
            if (removedEntry != null && removedEntry.getUsagesCount() == 0) {
              am.remove(removedEntry.getFileId(), removedEntry.getPageIndex());

              final OCachePointer cachePointer = removedEntry.getCachePointer();

              cachePointer.decrementReadersReferrer();
              removedEntry.clearCachePointer();

              counter++;
            }
          } finally {
            lock.unlock();
          }
        }
      }
    } finally {
      evictionLock.unlock();
    }
  }

  int getMaxSize() {
    return memoryDataContainer.get().maxSize;
  }

  @Override
  public final long getUsedMemory() {
    return ((long) (am.size() + a1in.size())) * pageSize;
  }

  private void remove(final long fileId, final long pageIndex) {
    OCacheEntry cacheEntry = am.remove(fileId, pageIndex);
    if (cacheEntry != null) {
      if (cacheEntry.getUsagesCount() > 1) {
        throw new IllegalStateException("Record cannot be removed because it is used!");
      }
      return;
    }

    cacheEntry = a1out.remove(fileId, pageIndex);
    if (cacheEntry != null) {
      return;
    }

    cacheEntry = a1in.remove(fileId, pageIndex);
    if (cacheEntry != null && cacheEntry.getUsagesCount() > 1) {
      throw new IllegalStateException("Record cannot be removed because it is used!");
    }

  }

  private static int normalizeMemory(final long maxSize, final int pageSize) {
    final long tmpMaxSize = maxSize / pageSize;
    if (tmpMaxSize >= Integer.MAX_VALUE) {
      return Integer.MAX_VALUE;
    } else {
      return (int) tmpMaxSize;
    }
  }

  private static final class PinnedPage implements Comparable<PinnedPage> {
    private final long fileId;
    private final long pageIndex;

    private PinnedPage(final long fileId, final long pageIndex) {
      this.fileId = fileId;
      this.pageIndex = pageIndex;
    }

    @Override
    public final boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      final PinnedPage that = (PinnedPage) o;

      if (fileId != that.fileId) {
        return false;
      }
      return pageIndex == that.pageIndex;
    }

    @Override
    public final String toString() {
      return "PinnedPage{" + "fileId=" + fileId + ", pageIndex=" + pageIndex + '}';
    }

    @Override
    public final int hashCode() {
      int result = (int) (fileId ^ (fileId >>> 32));
      result = 31 * result + (int) (pageIndex ^ (pageIndex >>> 32));
      return result;
    }

    @Override
    public final int compareTo(final PinnedPage other) {
      if (fileId > other.fileId) {
        return 1;
      }
      if (fileId < other.fileId) {
        return -1;
      }

      return Long.compare(pageIndex, other.pageIndex);

    }
  }

  private static final class PageKey implements Comparable<PageKey> {
    private final long fileId;
    private final long pageIndex;

    private PageKey(final long fileId, final long pageIndex) {
      this.fileId = fileId;
      this.pageIndex = pageIndex;
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      final PageKey pageKey = (PageKey) o;

      if (fileId != pageKey.fileId) {
        return false;
      }
      return pageIndex == pageKey.pageIndex;
    }

    @Override
    public int compareTo(final PageKey other) {
      if (fileId > other.fileId) {
        return 1;
      }
      if (fileId < other.fileId) {
        return -1;
      }

      return Long.compare(pageIndex, other.pageIndex);

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

    private UpdateCacheResult(final boolean removeColdPages, final OCacheEntry cacheEntry) {
      this.removeColdPages = removeColdPages;
      this.cacheEntry = cacheEntry;
    }
  }

  /**
   * That is immutable class which contains information about current memory limits for 2Q cache.
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

    MemoryData(final int maxSize, final int pinnedPages) {
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
