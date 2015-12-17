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

package com.orientechnologies.orient.core.storage.cache.local.twoq;

import com.orientechnologies.common.concur.lock.OInterruptedException;
import com.orientechnologies.common.concur.lock.ONewLockManager;
import com.orientechnologies.common.concur.lock.OReadersWriterSpinLock;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.exception.OAllCacheEntriesAreUsedException;
import com.orientechnologies.orient.core.exception.OReadCacheException;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.storage.cache.OAbstractWriteCache;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cache.OCachePointer;
import com.orientechnologies.orient.core.storage.cache.OReadCache;
import com.orientechnologies.orient.core.storage.cache.OWriteCache;
import com.orientechnologies.orient.core.storage.cache.local.OWOWCache;
import com.orientechnologies.orient.core.storage.impl.local.statistic.OSessionStoragePerformanceStatistic;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;
import com.orientechnologies.orient.core.storage.impl.local.statistic.OStoragePerformanceStatistic;

import javax.management.InstanceAlreadyExistsException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;

/**
 * @author Andrey Lomakin
 * @since 7/24/13
 */
public class O2QCache implements OReadCache, O2QCacheMXBean {
  /**
   * Maximum percent of pinned pages which may be contained in this cache.
   */
  public static final int MAX_PERCENT_OF_PINED_PAGES = 50;

  /**
   * Minimum size of memory which may be allocated by cache (in pages).
   * This parameter is used only if related flag is set in constrictor of cache.
   */
  public static final int MIN_CACHE_SIZE = 256;

  private static final int MAX_CACHE_OVERFLOW = Runtime.getRuntime().availableProcessors() * 8;

  private final LRUList am;
  private final LRUList a1out;
  private final LRUList a1in;
  private final int     pageSize;

  private final AtomicReference<MemoryData> memoryDataContainer = new AtomicReference<MemoryData>();

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
  private final ONewLockManager                        fileLockManager = new ONewLockManager(true);
  private final ONewLockManager<PageKey>               pageLockManager = new ONewLockManager<PageKey>();
  private final ConcurrentMap<PinnedPage, OCacheEntry> pinnedPages     = new ConcurrentHashMap<PinnedPage, OCacheEntry>();

  private final AtomicBoolean coldPagesRemovalInProgress = new AtomicBoolean();

  private final       AtomicBoolean mbeanIsRegistered = new AtomicBoolean();
  public static final String        MBEAN_NAME        = "com.orientechnologies.orient.core.storage.cache.local:type=O2QCacheMXBean";

  /**
   * @param readCacheMaxMemory   Maximum amount of direct memory which can allocated  by disk cache in bytes.
   * @param pageSize             Cache page size in bytes.
   * @param checkMinSize         If this flat is set size of cache may be {@link #MIN_CACHE_SIZE} or bigger.
   * @param percentOfPinnedPages Maximum percent of pinned pages which may be hold by this cache.
   * @see #MAX_PERCENT_OF_PINED_PAGES
   */
  public O2QCache(final long readCacheMaxMemory, final int pageSize, final boolean checkMinSize, final int percentOfPinnedPages) {
    if (percentOfPinnedPages > MAX_PERCENT_OF_PINED_PAGES)
      throw new IllegalArgumentException(
          "Percent of pinned pages can not be more than " + percentOfPinnedPages + " but passed value is " + percentOfPinnedPages);

    this.percentOfPinnedPages = percentOfPinnedPages;

    cacheLock.acquireWriteLock();
    try {
      this.pageSize = pageSize;

      this.filePages = new ConcurrentHashMap<Long, Set<Long>>();

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
      Set<Long> oldPages = filePages.put(fileId, Collections.newSetFromMap(new ConcurrentHashMap<Long, Boolean>()));
      assert oldPages == null || oldPages.isEmpty();
      return fileId;
    } finally {
      cacheLock.releaseWriteLock();
    }
  }

  @Override
  public long openFile(final String fileName, OWriteCache writeCache) throws IOException {
    cacheLock.acquireWriteLock();
    try {
      Long fileId = writeCache.isOpen(fileName);
      if (fileId != null)
        return fileId;

      fileId = writeCache.openFile(fileName);
      Set<Long> oldPages = filePages.put(fileId, Collections.newSetFromMap(new ConcurrentHashMap<Long, Boolean>()));
      assert oldPages == null || oldPages.isEmpty();

      return fileId;
    } finally {
      cacheLock.releaseWriteLock();
    }
  }

  @Override
  public long openFile(long fileId, OWriteCache writeCache) throws IOException {
    fileId = OAbstractWriteCache.checkFileIdCompatibility(writeCache.getId(), fileId);

    cacheLock.acquireReadLock();
    Lock fileLock;
    try {
      fileLock = fileLockManager.acquireExclusiveLock(fileId);
      try {
        if (writeCache.isOpen(fileId))
          return fileId;

        writeCache.openFile(fileId);
        Set<Long> oldPages = filePages.put(fileId, Collections.newSetFromMap(new ConcurrentHashMap<Long, Boolean>()));
        assert oldPages == null || oldPages.isEmpty();

      } finally {
        fileLockManager.releaseLock(fileLock);
      }
    } finally {
      cacheLock.releaseReadLock();
    }

    return fileId;
  }

  @Override
  public long openFile(String fileName, long fileId, OWriteCache writeCache) throws IOException {
    fileId = OAbstractWriteCache.checkFileIdCompatibility(writeCache.getId(), fileId);

    cacheLock.acquireWriteLock();
    try {
      Long existingFileId = writeCache.isOpen(fileName);

      if (existingFileId != null) {
        if (writeCache.fileIdsAreEqual(fileId, existingFileId))
          return fileId;

        throw new OStorageException(
            "File with given name already exists but has different id " + existingFileId + " vs. proposed " + fileId);
      }

      writeCache.openFile(fileName, fileId);
      Set<Long> oldPages = filePages.put(fileId, Collections.newSetFromMap(new ConcurrentHashMap<Long, Boolean>()));
      assert oldPages == null || oldPages.isEmpty();
    } finally {
      cacheLock.releaseWriteLock();
    }

    return fileId;
  }

  @Override
  public long addFile(String fileName, long fileId, OWriteCache writeCache) throws IOException {
    fileId = OAbstractWriteCache.checkFileIdCompatibility(writeCache.getId(), fileId);

    cacheLock.acquireWriteLock();
    try {
      final long fid = writeCache.addFile(fileName, fileId);
      Set<Long> oldPages = filePages.put(fid, Collections.newSetFromMap(new ConcurrentHashMap<Long, Boolean>()));
      assert oldPages == null || oldPages.isEmpty();

      return fid;
    } finally {
      cacheLock.releaseWriteLock();
    }
  }

  @Override
  public void pinPage(final OCacheEntry cacheEntry) throws IOException {
    Lock fileLock;
    Lock pageLock;

    MemoryData memoryData = memoryDataContainer.get();

    if ((100 * (memoryData.pinnedPages + 1)) / memoryData.maxSize > percentOfPinnedPages) {
      OLogManager.instance().warn(this, "Maximum amount of pinned pages is reached , given page " + cacheEntry +
          " will not be marked as pinned which may lead to performance degradation. You may consider to increase percent of pined pages "
          + "by changing of property " + OGlobalConfiguration.DISK_CACHE_PINNED_PAGES.getKey());

      return;
    }

    cacheLock.acquireReadLock();
    try {
      fileLock = fileLockManager.acquireSharedLock(cacheEntry.getFileId());
      try {
        pageLock = pageLockManager.acquireExclusiveLock(new PageKey(cacheEntry.getFileId(), cacheEntry.getPageIndex()));
        try {
          remove(cacheEntry.getFileId(), cacheEntry.getPageIndex());
          pinnedPages.put(new PinnedPage(cacheEntry.getFileId(), cacheEntry.getPageIndex()), cacheEntry);
        } finally {
          pageLockManager.releaseLock(pageLock);
        }
      } finally {
        fileLockManager.releaseLock(fileLock);
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
   * Changes amount of memory which may be used by given cache.
   * This method may consume many resources if amount of memory provided in parameter is much less than current amount of memory.
   *
   * @param readCacheMaxMemory New maximum size of cache in bytes.
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
        throw new IllegalStateException("Can not decrease amount of memory used by disk cache "
            + "because limit of pinned pages will be more than allowed limit " + percentOfPinnedPages);
      }

      newMemoryData = new MemoryData(newMemorySize, memoryData.pinnedPages);
    } while (!memoryDataContainer.compareAndSet(memoryData, newMemoryData));

    if (newMemorySize < memoryData.maxSize)
      removeColdestPagesIfNeeded();

    OLogManager.instance()
        .info(this, "Disk cache size was changed from " + memoryData.maxSize + " pages to " + newMemorySize + " pages");
  }

  @Override
  public OCacheEntry load(long fileId, final long pageIndex, final boolean checkPinnedPages, OWriteCache writeCache,
      final int pageCount, OStoragePerformanceStatistic storagePerformanceStatistic) throws IOException {

    final OSessionStoragePerformanceStatistic sessionStoragePerformanceStatistic = OSessionStoragePerformanceStatistic
        .getStatisticInstance();

    if (sessionStoragePerformanceStatistic != null) {
      sessionStoragePerformanceStatistic.startPageReadFromCacheTimer();
    }

    storagePerformanceStatistic.startPageReadFromCacheTimer();

    try {
      fileId = OAbstractWriteCache.checkFileIdCompatibility(writeCache.getId(), fileId);

      final UpdateCacheResult cacheResult = doLoad(fileId, pageIndex, checkPinnedPages, false, writeCache, pageCount,
          sessionStoragePerformanceStatistic, storagePerformanceStatistic);
      if (cacheResult == null)
        return null;

      try {
        if (cacheResult.removeColdPages)
          removeColdestPagesIfNeeded();
      } catch (RuntimeException e) {
        assert !cacheResult.cacheEntry.isDirty();

        release(cacheResult.cacheEntry, writeCache, storagePerformanceStatistic);
        throw e;
      }

      return cacheResult.cacheEntry;
    } finally {
      if (sessionStoragePerformanceStatistic != null) {
        sessionStoragePerformanceStatistic.stopPageReadFromCacheTimer();
      }

      storagePerformanceStatistic.stopPageReadFromCacheTimer();
    }
  }

  private UpdateCacheResult doLoad(long fileId, long pageIndex, boolean checkPinnedPages, boolean addNewPages,
      OWriteCache writeCache, final int pageCount, final OSessionStoragePerformanceStatistic sessionStoragePerformanceStatistic,
      final OStoragePerformanceStatistic storagePerformanceStatistic) throws IOException {

    if (pageCount < 1)
      throw new IllegalArgumentException(
          "Amount of pages to load from cache should be not less than 1 but passed value is " + pageCount);

    boolean removeColdPages = false;
    OCacheEntry cacheEntry = null;

    Lock fileLock;
    Lock[] pageLocks;

    if (sessionStoragePerformanceStatistic != null) {
      sessionStoragePerformanceStatistic.incrementPageAccessOnCacheLevel();
    }
    storagePerformanceStatistic.incrementPageAccessOnCacheLevel();

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
            UpdateCacheResult cacheResult = updateCache(fileId, pageIndex, addNewPages, writeCache, pageCount,
                sessionStoragePerformanceStatistic, storagePerformanceStatistic);
            if (cacheResult == null)
              return null;

            cacheEntry = cacheResult.cacheEntry;
            removeColdPages = cacheResult.removeColdPages;
          } else {
            if (sessionStoragePerformanceStatistic != null) {
              sessionStoragePerformanceStatistic.incrementCacheHit();
            }
            storagePerformanceStatistic.incrementCacheHit();
          }

          cacheEntry.incrementUsages();
        } finally {
          for (Lock pageLock : pageLocks) {
            pageLock.unlock();
          }
        }
      } finally {
        fileLockManager.releaseLock(fileLock);
      }
    } finally {
      cacheLock.releaseReadLock();
    }

    return new UpdateCacheResult(removeColdPages, cacheEntry);
  }

  @Override
  public OCacheEntry allocateNewPage(long fileId, OWriteCache writeCache, OStoragePerformanceStatistic storagePerformanceStatistic)
      throws IOException {
    final OSessionStoragePerformanceStatistic sessionStoragePerformanceStatistic = OSessionStoragePerformanceStatistic
        .getStatisticInstance();

    if (sessionStoragePerformanceStatistic != null) {
      sessionStoragePerformanceStatistic.startPageReadFromCacheTimer();
    }

    storagePerformanceStatistic.startPageReadFromCacheTimer();

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
          cacheResult = doLoad(fileId, filledUpTo, false, true, writeCache, 1, sessionStoragePerformanceStatistic,
              storagePerformanceStatistic);
        } finally {
          fileLockManager.releaseLock(fileLock);
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

        release(cacheResult.cacheEntry, writeCache, storagePerformanceStatistic);
        throw e;
      }

      return cacheResult.cacheEntry;
    } finally {
      if (sessionStoragePerformanceStatistic != null) {
        sessionStoragePerformanceStatistic.stopPageReadFromCacheTimer();
      }

      storagePerformanceStatistic.stopPageReadFromCacheTimer();
    }
  }

  @Override
  public void release(OCacheEntry cacheEntry, OWriteCache writeCache, OStoragePerformanceStatistic storagePerformanceStatistic) {
    Future<?> flushFuture = null;

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

          if (cacheEntry.getUsagesCount() == 0 && cacheEntry.isDirty()) {
            final OSessionStoragePerformanceStatistic sessionStoragePerformanceStatistic = OSessionStoragePerformanceStatistic
                .getStatisticInstance();

            if (sessionStoragePerformanceStatistic != null) {
              sessionStoragePerformanceStatistic.startPageWriteInCacheTimer();
            }

            storagePerformanceStatistic.startPageWriteToCacheTimer();
            try {
              flushFuture = writeCache.store(cacheEntry.getFileId(), cacheEntry.getPageIndex(), cacheEntry.getCachePointer());
            } finally {
              if (sessionStoragePerformanceStatistic != null) {
                sessionStoragePerformanceStatistic.stopPageWriteInCacheTimer();
              }

              storagePerformanceStatistic.stopPageWriteToCacheTimer();
            }

            cacheEntry.clearDirty();
          }
        } finally {
          pageLockManager.releaseLock(pageLock);
        }
      } finally {
        fileLockManager.releaseLock(fileLock);
      }
    } finally {
      cacheLock.releaseReadLock();
    }

    if (flushFuture != null) {
      try {
        flushFuture.get();
      } catch (InterruptedException e) {
        Thread.interrupted();
        throw new OInterruptedException("File flush was interrupted");
      } catch (Exception e) {
        throw OException.wrapException(new OReadCacheException("File flush was abnormally terminated"), e);
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
        fileLockManager.releaseLock(fileLock);
      }
    } finally {
      cacheLock.releaseReadLock();
    }
  }

  private void clearFile(long fileId) {
    final Set<Long> pageEntries = filePages.get(fileId);
    if (pageEntries == null || pageEntries.isEmpty()) {
      assert get(fileId, 0, true) == null;
      return;
    }

    for (Long pageIndex : pageEntries) {
      OCacheEntry cacheEntry = get(fileId, pageIndex, true);

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

    assert get(fileId, 0, true) == null;

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
        fileLockManager.releaseLock(fileLock);
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
        truncateFile(fileId, writeCache);

        filePages.remove(fileId);
        writeCache.deleteFile(fileId);
      } finally {
        fileLockManager.releaseLock(fileLock);
      }
    } finally {
      cacheLock.releaseReadLock();
    }
  }

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

  @Override
  public void deleteStorage(OWriteCache writeCache) throws IOException {
    cacheLock.acquireWriteLock();
    try {
      final long[] filesToClear = writeCache.delete();
      for (long fileId : filesToClear)
        clearFile(fileId);
    } finally {
      cacheLock.releaseWriteLock();
    }
  }

  public void registerMBean() {
    if (mbeanIsRegistered.compareAndSet(false, true)) {
      try {
        final MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        final ObjectName mbeanName = new ObjectName(MBEAN_NAME);
        server.registerMBean(this, mbeanName);
      } catch (MalformedObjectNameException e) {
        throw OException.wrapException(new OReadCacheException("Error during registration of read cache MBean"), e);
      } catch (InstanceAlreadyExistsException e) {
        throw OException.wrapException(new OReadCacheException("Error during registration of read cache MBean"), e);
      } catch (MBeanRegistrationException e) {
        throw OException.wrapException(new OReadCacheException("Error during registration of read cache MBean"), e);
      } catch (NotCompliantMBeanException e) {
        throw OException.wrapException(new OReadCacheException("Error during registration of read cache MBean"), e);
      }
    }
  }

  public void unregisterMBean() {
    if (mbeanIsRegistered.compareAndSet(true, false)) {
      try {
        final MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        final ObjectName mbeanName = new ObjectName(MBEAN_NAME);
        server.unregisterMBean(mbeanName);
      } catch (MalformedObjectNameException e) {
        throw OException.wrapException(new OReadCacheException("Error during unregistration of read cache MBean"), e);
      } catch (InstanceNotFoundException e) {
        throw OException.wrapException(new OReadCacheException("Error during unregistration of read cache MBean"), e);
      } catch (MBeanRegistrationException e) {
        throw OException.wrapException(new OReadCacheException("Error during unregistration of read cache MBean"), e);
      }
    }
  }

  @Override
  public int getA1InSize() {
    return a1in.size();
  }

  @Override
  public int getA1OutSize() {
    return a1out.size();
  }

  @Override
  public int getAmSize() {
    return am.size();
  }

  private OCacheEntry get(long fileId, long pageIndex, boolean useOutQueue) {
    OCacheEntry cacheEntry = am.get(fileId, pageIndex);

    if (cacheEntry != null) {
      assert filePages.get(fileId) != null;
      assert filePages.get(fileId).contains(pageIndex);

      return cacheEntry;
    }

    if (useOutQueue) {
      cacheEntry = a1out.get(fileId, pageIndex);
      if (cacheEntry != null) {
        assert filePages.get(fileId) != null;
        assert filePages.get(fileId).contains(pageIndex);

        return cacheEntry;
      }

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

  private boolean entryIsInAmQueue(final long fileId, final long pageIndex, final OCacheEntry cacheEntry) {
    assert filePages.get(fileId) != null;
    assert filePages.get(fileId).contains(pageIndex);

    am.putToMRU(cacheEntry);

    return false;
  }

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

  private boolean entryIsInA1InQueue(final long fileId, final long pageIndex) {
    assert filePages.get(fileId) != null;
    assert filePages.get(fileId).contains(pageIndex);

    return false;
  }

  private UpdateCacheResult entryIsAbsentInQueues(long fileId, long pageIndex, OCachePointer dataPointer) {
    OCacheEntry cacheEntry;
    cacheEntry = new OCacheEntry(fileId, pageIndex, dataPointer, false);
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
      final int pageCount, final OSessionStoragePerformanceStatistic sessionStoragePerformanceStatistic,
      final OStoragePerformanceStatistic storagePerformanceStatistic) throws IOException {

    assert pageCount > 0;

    OCacheEntry cacheEntry = am.get(fileId, pageIndex);

    if (cacheEntry != null) {
      if (sessionStoragePerformanceStatistic != null) {
        sessionStoragePerformanceStatistic.incrementCacheHit();
      }
      storagePerformanceStatistic.incrementCacheHit();

      return new UpdateCacheResult(entryIsInAmQueue(fileId, pageIndex, cacheEntry), cacheEntry);
    }

    boolean removeColdPages;
    OCachePointer[] dataPointers = null;

    cacheEntry = a1out.remove(fileId, pageIndex);
    if (cacheEntry != null) {
      dataPointers = writeCache.load(fileId, pageIndex, pageCount, false);

      OCachePointer dataPointer = dataPointers[0];
      removeColdPages = entryWasInA1OutQueue(fileId, pageIndex, dataPointer, cacheEntry);
    } else {
      cacheEntry = a1in.get(fileId, pageIndex);

      if (cacheEntry != null) {
        removeColdPages = entryIsInA1InQueue(fileId, pageIndex);
        if (sessionStoragePerformanceStatistic != null) {
          sessionStoragePerformanceStatistic.incrementCacheHit();
        }
        storagePerformanceStatistic.incrementCacheHit();
      } else {
        dataPointers = writeCache.load(fileId, pageIndex, pageCount, addNewPages);

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
            pageLock = pageLockManager
                .acquireExclusiveLock(new PageKey(removedFromAInEntry.getFileId(), removedFromAInEntry.getPageIndex()));
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
              pageLockManager.releaseLock(pageLock);
            }
          } finally {
            fileLockManager.releaseLock(fileLock);
          }
        }

        while (a1out.size() > memoryData.K_OUT) {
          OCacheEntry removedEntry = a1out.getLRU();
          fileLock = fileLockManager.acquireSharedLock(removedEntry.getFileId());
          try {
            pageLock = pageLockManager.acquireExclusiveLock(new PageKey(removedEntry.getFileId(), removedEntry.getPageIndex()));
            try {
              if (a1out.remove(removedEntry.getFileId(), removedEntry.getPageIndex()) == null)
                continue;

              assert removedEntry.getUsagesCount() == 0;
              assert removedEntry.getCachePointer() == null;
              assert !removedEntry.isDirty();

              Set<Long> pageEntries = filePages.get(removedEntry.getFileId());
              pageEntries.remove(removedEntry.getPageIndex());
            } finally {
              pageLockManager.releaseLock(pageLock);
            }
          } finally {
            fileLockManager.releaseLock(fileLock);
          }
        }
      } else {
        OCacheEntry removedEntry = am.getLRU();

        if (removedEntry == null) {
          throw new OAllCacheEntriesAreUsedException("All records in aIn queue in 2q cache are used!");
        } else {
          fileLock = fileLockManager.acquireSharedLock(removedEntry.getFileId());
          try {
            pageLock = pageLockManager.acquireExclusiveLock(new PageKey(removedEntry.getFileId(), removedEntry.getPageIndex()));
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
              pageLockManager.releaseLock(pageLock);
            }
          } finally {
            fileLockManager.releaseLock(fileLock);
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
    return ((long) (am.size() + a1in.size())) * (2 * ODurablePage.PAGE_PADDING + pageSize);
  }

  @Override
  public long getUsedMemoryInMB() {
    return getUsedMemory() / (1024 * 1024);
  }

  @Override
  public double getUsedMemoryInGB() {
    return Math.ceil((getUsedMemory() * 100) / (1024.0 * 1024 * 1024)) / 100;
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
    long tmpMaxSize = maxSize / (pageSize + 2 * OWOWCache.PAGE_PADDING);
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

  private static final class PageKey {
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
   * That is immutable class which contains information
   * about current memory limits for 2Q cache.
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

    public MemoryData(int maxSize, int pinnedPages) {
      K_IN = (maxSize - pinnedPages) >> 2;
      K_OUT = (maxSize - pinnedPages) >> 1;

      this.maxSize = maxSize;
      this.pinnedPages = pinnedPages;
    }

    /**
     * @return Maximum size of memory which may be consumed by all 2Q queues in amount of pages.
     */
    public int get2QCacheSize() {
      return maxSize - pinnedPages;
    }
  }
}
