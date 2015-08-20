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

package com.orientechnologies.orient.core.storage.cache.local;

import com.orientechnologies.common.concur.lock.ODistributedCounter;
import com.orientechnologies.common.concur.lock.ONewLockManager;
import com.orientechnologies.common.concur.lock.OReadersWriterSpinLock;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.exception.OAllCacheEntriesAreUsedException;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.storage.cache.OAbstractWriteCache;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cache.OCachePointer;
import com.orientechnologies.orient.core.storage.cache.OReadCache;
import com.orientechnologies.orient.core.storage.cache.OWriteCache;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;

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
import java.util.concurrent.locks.Lock;

/**
 * @author Andrey Lomakin
 * @since 7/24/13
 */
public class O2QCache implements OReadCache, O2QCacheMXBean {
  public static final int                              MIN_CACHE_SIZE             = 256;

  private static final int                             MAX_CACHE_OVERFLOW         = Runtime.getRuntime().availableProcessors() * 8;

  private final int                                    maxSize;
  private final int                                    K_IN;
  private final int                                    K_OUT;

  private final LRUList                                am;
  private final LRUList                                a1out;
  private final LRUList                                a1in;

  private final int                                    pageSize;

  /**
   * Contains all pages in cache for given file.
   */
  private final ConcurrentMap<Long, Set<Long>>         filePages;

  private final OReadersWriterSpinLock                 cacheLock                  = new OReadersWriterSpinLock();
  private final ONewLockManager                        fileLockManager            = new ONewLockManager(true);
  private final ONewLockManager<PageKey>               pageLockManager            = new ONewLockManager<PageKey>();
  private final ConcurrentMap<PinnedPage, OCacheEntry> pinnedPages                = new ConcurrentHashMap<PinnedPage, OCacheEntry>();

  private final AtomicBoolean                          coldPagesRemovalInProgress = new AtomicBoolean();
  private final ODistributedCounter                    cacheHitCounter            = new ODistributedCounter();
  private final ODistributedCounter                    cacheQueriesCounter        = new ODistributedCounter();

  private final AtomicBoolean                          mbeanIsRegistered          = new AtomicBoolean();
  public static final String                           MBEAN_NAME                 = "com.orientechnologies.orient.core.storage.cache.local:type=O2QCacheMXBean";

  public O2QCache(final long readCacheMaxMemory, final int pageSize, final boolean checkMinSize) {
    cacheLock.acquireWriteLock();
    try {
      this.pageSize = pageSize;

      this.filePages = new ConcurrentHashMap<Long, Set<Long>>();

      int normalizedSize = normalizeMemory(readCacheMaxMemory, pageSize);

      if (checkMinSize && normalizedSize < MIN_CACHE_SIZE)
        normalizedSize = MIN_CACHE_SIZE;

      maxSize = normalizedSize;

      K_IN = maxSize >> 2;
      K_OUT = maxSize >> 1;

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
  public void openFile(long fileId, OWriteCache writeCache) throws IOException {
    fileId = OAbstractWriteCache.checkFileIdCompatibility(writeCache.getId(), fileId);

    cacheLock.acquireReadLock();
    Lock fileLock;
    try {
      fileLock = fileLockManager.acquireExclusiveLock(fileId);
      try {
        if (writeCache.isOpen(fileId))
          return;

        writeCache.openFile(fileId);
        Set<Long> oldPages = filePages.put(fileId, Collections.newSetFromMap(new ConcurrentHashMap<Long, Boolean>()));
        assert oldPages == null || oldPages.isEmpty();

      } finally {
        fileLockManager.releaseLock(fileLock);
      }
    } finally {
      cacheLock.releaseReadLock();
    }
  }

  @Override
  public void openFile(String fileName, long fileId, OWriteCache writeCache) throws IOException {
    fileId = OAbstractWriteCache.checkFileIdCompatibility(writeCache.getId(), fileId);

    cacheLock.acquireWriteLock();
    try {
      Long existingFileId = writeCache.isOpen(fileName);

      if (existingFileId != null) {
        if (fileId == existingFileId)
          return;

        throw new OStorageException("File with given name already exists but has different id " + existingFileId + " vs. proposed "
            + fileId);
      }

      writeCache.openFile(fileName, fileId);
      Set<Long> oldPages = filePages.put(fileId, Collections.newSetFromMap(new ConcurrentHashMap<Long, Boolean>()));
      assert oldPages == null || oldPages.isEmpty();
    } finally {
      cacheLock.releaseWriteLock();
    }
  }

  @Override
  public void addFile(String fileName, long fileId, OWriteCache writeCache) throws IOException {
    fileId = OAbstractWriteCache.checkFileIdCompatibility(writeCache.getId(), fileId);

    cacheLock.acquireWriteLock();
    try {
      writeCache.addFile(fileName, fileId);
      Set<Long> oldPages = filePages.put(fileId, Collections.newSetFromMap(new ConcurrentHashMap<Long, Boolean>()));
      assert oldPages == null || oldPages.isEmpty();
    } finally {
      cacheLock.releaseWriteLock();
    }
  }

  @Override
  public void pinPage(final OCacheEntry cacheEntry) throws IOException {
    Lock fileLock;
    Lock pageLock;

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
  }

  @Override
  public OCacheEntry load(long fileId, final long pageIndex, final boolean checkPinnedPages, OWriteCache writeCache)
      throws IOException {
    fileId = OAbstractWriteCache.checkFileIdCompatibility(writeCache.getId(), fileId);

    final UpdateCacheResult cacheResult = doLoad(fileId, pageIndex, checkPinnedPages, false, writeCache);
    if (cacheResult == null)
      return null;

    try {
      if (cacheResult.removeColdPages)
        removeColdestPagesIfNeeded();
    } catch (RuntimeException e) {
      assert !cacheResult.cacheEntry.isDirty();

      release(cacheResult.cacheEntry, writeCache);
      throw e;
    }

    return cacheResult.cacheEntry;
  }

  private UpdateCacheResult doLoad(long fileId, long pageIndex, boolean checkPinnedPages, boolean addNewPages,
      OWriteCache writeCache) throws IOException {
    boolean removeColdPages = false;
    OCacheEntry cacheEntry = null;

    Lock fileLock;
    Lock pageLock;

    cacheQueriesCounter.increment();

    cacheLock.acquireReadLock();
    try {
      fileLock = fileLockManager.acquireSharedLock(fileId);
      try {
        pageLock = pageLockManager.acquireExclusiveLock(new PageKey(fileId, pageIndex));
        try {
          if (checkPinnedPages)
            cacheEntry = pinnedPages.get(new PinnedPage(fileId, pageIndex));

          if (cacheEntry == null) {
            UpdateCacheResult cacheResult = updateCache(fileId, pageIndex, addNewPages, writeCache);
            if (cacheResult == null)
              return null;

            cacheEntry = cacheResult.cacheEntry;
            removeColdPages = cacheResult.removeColdPages;
          } else {
            cacheHitCounter.increment();
          }

          cacheEntry.incrementUsages();
        } finally {
          pageLockManager.releaseLock(pageLock);
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
  public OCacheEntry allocateNewPage(long fileId, OWriteCache writeCache) throws IOException {
    fileId = OAbstractWriteCache.checkFileIdCompatibility(writeCache.getId(), fileId);

    UpdateCacheResult cacheResult;

    Lock fileLock;
    cacheLock.acquireReadLock();
    try {
      fileLock = fileLockManager.acquireExclusiveLock(fileId);
      try {
        final long filledUpTo = writeCache.getFilledUpTo(fileId);
        assert filledUpTo >= 0;
        cacheResult = doLoad(fileId, filledUpTo, false, true, writeCache);
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

      release(cacheResult.cacheEntry, writeCache);
      throw e;
    }

    return cacheResult.cacheEntry;
  }

  @Override
  public void release(OCacheEntry cacheEntry, OWriteCache writeCache) {
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
            flushFuture = writeCache.store(cacheEntry.getFileId(), cacheEntry.getPageIndex(), cacheEntry.getCachePointer());
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
        throw new OException("File flush was interrupted", e);
      } catch (Exception e) {
        throw new OException("File flush was abnormally terminated", e);
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
          if (cacheEntry == null)
            cacheEntry = pinnedPages.remove(new PinnedPage(fileId, pageIndex));

          final OCachePointer cachePointer = cacheEntry.getCachePointer();
          if (cachePointer != null) {
            cachePointer.decrementReadersReferrer();
            cacheEntry.clearCachePointer();
          }

        } else
          throw new OStorageException("Page with index " + pageIndex + " for file with id " + fileId
              + " cannot be freed because it is used.");
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
        throw new OStorageException("Error during registration of read cache MBean.", e);
      } catch (InstanceAlreadyExistsException e) {
        throw new OStorageException("Error during registration of read cache MBean.", e);
      } catch (MBeanRegistrationException e) {
        throw new OStorageException("Error during registration of read cache MBean.", e);
      } catch (NotCompliantMBeanException e) {
        throw new OStorageException("Error during registration of read cache MBean.", e);
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
        throw new OStorageException("Error during unregistration of read cache MBean.", e);
      } catch (InstanceNotFoundException e) {
        throw new OStorageException("Error during unregistration of read cache MBean.", e);
      } catch (MBeanRegistrationException e) {
        throw new OStorageException("Error during unregistration of read cache MBean.", e);
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

  @Override
  public double getCacheHits() {
    return (cacheHitCounter.get() * 100.0) / cacheQueriesCounter.get();
  }

  @Override
  public void clearCacheStatistics() {
    cacheHitCounter.clear();
    cacheQueriesCounter.clear();
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
    if (cacheEntry != null) {

    }
    return cacheEntry;
  }

  private void clearCacheContent() {
    for (OCacheEntry cacheEntry : am)
      if (cacheEntry.getUsagesCount() == 0) {
        final OCachePointer cachePointer = cacheEntry.getCachePointer();
        cachePointer.decrementReadersReferrer();
        cacheEntry.clearCachePointer();
      }

      else
        throw new OStorageException("Page with index " + cacheEntry.getPageIndex() + " for file id " + cacheEntry.getFileId()
            + " is used and cannot be removed");

    for (OCacheEntry cacheEntry : a1in)
      if (cacheEntry.getUsagesCount() == 0) {
        final OCachePointer cachePointer = cacheEntry.getCachePointer();
        cachePointer.decrementReadersReferrer();
        cacheEntry.clearCachePointer();
      }

      else
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
      } else
        throw new OStorageException("Page with index " + pinnedEntry.getPageIndex() + " for file with id "
            + pinnedEntry.getFileId() + "cannot be freed because it is used.");
    }

    pinnedPages.clear();
  }

  private UpdateCacheResult updateCache(final long fileId, final long pageIndex, final boolean addNewPages, OWriteCache writeCache)
      throws IOException {
    OCacheEntry cacheEntry = am.get(fileId, pageIndex);

    if (cacheEntry != null) {
      assert filePages.get(fileId) != null;
      assert filePages.get(fileId).contains(pageIndex);

      am.putToMRU(cacheEntry);
      cacheHitCounter.increment();

      return new UpdateCacheResult(false, cacheEntry);
    }

    cacheEntry = a1out.remove(fileId, pageIndex);
    if (cacheEntry != null) {
      assert filePages.get(fileId) != null;
      assert filePages.get(fileId).contains(pageIndex);

      OCachePointer dataPointer = writeCache.load(fileId, pageIndex, false);

      assert dataPointer != null;
      assert cacheEntry.getCachePointer() == null;
      assert !cacheEntry.isDirty();

      cacheEntry.setCachePointer(dataPointer);

      am.putToMRU(cacheEntry);

      return new UpdateCacheResult(true, cacheEntry);
    }

    cacheEntry = a1in.get(fileId, pageIndex);
    if (cacheEntry != null) {
      assert filePages.get(fileId) != null;
      assert filePages.get(fileId).contains(pageIndex);

      cacheHitCounter.increment();
      return new UpdateCacheResult(false, cacheEntry);
    }

    OCachePointer dataPointer = writeCache.load(fileId, pageIndex, addNewPages);
    if (dataPointer == null)
      return null;

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

  private void removeColdestPagesIfNeeded() throws IOException {
    if (!coldPagesRemovalInProgress.compareAndSet(false, true))
      return;

    final boolean exclusiveCacheLock = (am.size() + a1in.size() - maxSize) > MAX_CACHE_OVERFLOW;

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
    while (am.size() + a1in.size() > maxSize) {
      if (a1in.size() > K_IN) {
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

        while (a1out.size() > K_OUT) {
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

    while (am.size() + a1in.size() > maxSize && iterationsCounter < 1000) {
      iterationsCounter++;

      if (a1in.size() > K_IN) {
        OCacheEntry removedFromAInEntry = a1in.getLRU();
        if (removedFromAInEntry == null) {
          throw new OAllCacheEntriesAreUsedException("All records in aIn queue in 2q cache are used!");
        } else {
          fileLock = fileLockManager.acquireSharedLock(removedFromAInEntry.getFileId());
          try {
            pageLock = pageLockManager.acquireExclusiveLock(new PageKey(removedFromAInEntry.getFileId(), removedFromAInEntry
                .getPageIndex()));
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

              a1out.putToMRU(removedFromAInEntry);
            } finally {
              pageLockManager.releaseLock(pageLock);
            }
          } finally {
            fileLockManager.releaseLock(fileLock);
          }
        }

        while (a1out.size() > K_OUT) {
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
    return maxSize;
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
}
