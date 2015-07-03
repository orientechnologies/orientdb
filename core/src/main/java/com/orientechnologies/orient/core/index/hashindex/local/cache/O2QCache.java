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

package com.orientechnologies.orient.core.index.hashindex.local.cache;

import java.io.IOException;
import java.util.Collections;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;

import com.orientechnologies.common.concur.lock.ONewLockManager;
import com.orientechnologies.common.concur.lock.OReadersWriterSpinLock;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.exception.OAllCacheEntriesAreUsedException;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.storage.cache.OAbstractWriteCache;
import com.orientechnologies.orient.core.storage.cache.OWriteCache;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;

/**
 * @author Andrey Lomakin
 * @since 7/24/13
 */
public class O2QCache implements OReadCache {
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
    fileId = OAbstractWriteCache.checkFileIdCompatibility(fileId, writeCache.getId());

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
    fileId = OAbstractWriteCache.checkFileIdCompatibility(fileId, writeCache.getId());

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
    fileId = OAbstractWriteCache.checkFileIdCompatibility(fileId, writeCache.getId());

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
      fileLock = fileLockManager.acquireSharedLock(cacheEntry.fileId);
      try {
        pageLock = pageLockManager.acquireExclusiveLock(new PageKey(cacheEntry.fileId, cacheEntry.pageIndex));
        try {
          remove(cacheEntry.fileId, cacheEntry.pageIndex);
          pinnedPages.put(new PinnedPage(cacheEntry.fileId, cacheEntry.pageIndex), cacheEntry);
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
    fileId = OAbstractWriteCache.checkFileIdCompatibility(fileId, writeCache.getId());

    final UpdateCacheResult cacheResult = doLoad(fileId, pageIndex, checkPinnedPages, false, writeCache);
    if (cacheResult == null)
      return null;

    try {
      if (cacheResult.removeColdPages)
        removeColdestPagesIfNeeded();
    } catch (RuntimeException e) {
      assert !cacheResult.cacheEntry.isDirty;

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
          }

          cacheEntry.usagesCount++;
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
    fileId = OAbstractWriteCache.checkFileIdCompatibility(fileId, writeCache.getId());

    UpdateCacheResult cacheResult;

    Lock fileLock;
    cacheLock.acquireReadLock();
    try {
      fileLock = fileLockManager.acquireExclusiveLock(fileId);
      try {
        final long filledUpTo = writeCache.getFilledUpTo(fileId);
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
      assert !cacheResult.cacheEntry.isDirty;

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
      fileLock = fileLockManager.acquireSharedLock(cacheEntry.fileId);
      try {
        pageLock = pageLockManager.acquireExclusiveLock(new PageKey(cacheEntry.fileId, cacheEntry.pageIndex));
        try {
          cacheEntry.usagesCount--;

          assert cacheEntry.usagesCount >= 0;

          if (cacheEntry.usagesCount == 0 && cacheEntry.isDirty) {
            flushFuture = writeCache.store(cacheEntry.fileId, cacheEntry.pageIndex, cacheEntry.dataPointer);
            cacheEntry.isDirty = false;
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
    fileId = OAbstractWriteCache.checkFileIdCompatibility(fileId, writeCache.getId());

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
        if (cacheEntry.usagesCount == 0) {
          cacheEntry = remove(fileId, pageIndex);
          if (cacheEntry == null)
            cacheEntry = pinnedPages.remove(new PinnedPage(fileId, pageIndex));

          if (cacheEntry.dataPointer != null) {
            cacheEntry.dataPointer.decrementReadersReferrer();
            cacheEntry.dataPointer = null;
          }

        } else
          throw new OStorageException("Page with index " + pageIndex + " for file with id " + fileId
              + " can not be freed because it is used.");
      } else
        throw new OStorageException("Page with index " + pageIndex + " was  not found in cache for file with id " + fileId);
    }

    assert get(fileId, 0, true) == null;

    pageEntries.clear();
  }

  @Override
  public void closeFile(long fileId, boolean flush, OWriteCache writeCache) throws IOException {
    fileId = OAbstractWriteCache.checkFileIdCompatibility(fileId, writeCache.getId());

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
    fileId = OAbstractWriteCache.checkFileIdCompatibility(fileId, writeCache.getId());

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
      if (cacheEntry.usagesCount == 0) {
        cacheEntry.dataPointer.decrementReadersReferrer();
        cacheEntry.dataPointer = null;
      }

      else
        throw new OStorageException("Page with index " + cacheEntry.pageIndex + " for file id " + cacheEntry.fileId
            + " is used and can not be removed");

    for (OCacheEntry cacheEntry : a1in)
      if (cacheEntry.usagesCount == 0) {
        cacheEntry.dataPointer.decrementReadersReferrer();
        cacheEntry.dataPointer = null;
      }

      else
        throw new OStorageException("Page with index " + cacheEntry.pageIndex + " for file id " + cacheEntry.fileId
            + " is used and can not be removed");

    a1out.clear();
    am.clear();
    a1in.clear();

    for (Set<Long> pages : filePages.values())
      pages.clear();

    clearPinnedPages();
  }

  private void clearPinnedPages() {
    for (OCacheEntry pinnedEntry : pinnedPages.values()) {
      if (pinnedEntry.usagesCount == 0) {
        pinnedEntry.dataPointer.decrementReadersReferrer();
        pinnedEntry.dataPointer = null;
      } else
        throw new OStorageException("Page with index " + pinnedEntry.pageIndex + " for file with id " + pinnedEntry.fileId
            + "can not be freed because it is used.");
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

      return new UpdateCacheResult(false, cacheEntry);
    }

    cacheEntry = a1out.remove(fileId, pageIndex);
    if (cacheEntry != null) {
      assert filePages.get(fileId) != null;
      assert filePages.get(fileId).contains(pageIndex);

      OCachePointer dataPointer = writeCache.load(fileId, pageIndex, false);

      assert dataPointer != null;
      assert cacheEntry.dataPointer == null;
      assert !cacheEntry.isDirty;

      cacheEntry.dataPointer = dataPointer;

      am.putToMRU(cacheEntry);

      return new UpdateCacheResult(true, cacheEntry);
    }

    cacheEntry = a1in.get(fileId, pageIndex);
    if (cacheEntry != null) {
      assert filePages.get(fileId) != null;
      assert filePages.get(fileId).contains(pageIndex);

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
          assert removedFromAInEntry.usagesCount == 0;
          assert !removedFromAInEntry.isDirty;

          removedFromAInEntry.dataPointer.decrementReadersReferrer();
          removedFromAInEntry.dataPointer = null;

          a1out.putToMRU(removedFromAInEntry);
        }

        while (a1out.size() > K_OUT) {
          OCacheEntry removedEntry = a1out.removeLRU();

          assert removedEntry.usagesCount == 0;
          assert removedEntry.dataPointer == null;
          assert !removedEntry.isDirty;

          Set<Long> pageEntries = filePages.get(removedEntry.fileId);
          pageEntries.remove(removedEntry.pageIndex);
        }
      } else {
        OCacheEntry removedEntry = am.removeLRU();

        if (removedEntry == null) {
          throw new OAllCacheEntriesAreUsedException("All records in aIn queue in 2q cache are used!");
        } else {
          assert removedEntry.usagesCount == 0;
          assert !removedEntry.isDirty;

          removedEntry.dataPointer.decrementReadersReferrer();
          removedEntry.dataPointer = null;

          Set<Long> pageEntries = filePages.get(removedEntry.fileId);
          pageEntries.remove(removedEntry.pageIndex);
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
          fileLock = fileLockManager.acquireSharedLock(removedFromAInEntry.fileId);
          try {
            pageLock = pageLockManager.acquireExclusiveLock(new PageKey(removedFromAInEntry.fileId, removedFromAInEntry.pageIndex));
            try {
              if (a1in.get(removedFromAInEntry.fileId, removedFromAInEntry.pageIndex) == null)
                continue;

              if (removedFromAInEntry.usagesCount > 0)
                continue;

              assert !removedFromAInEntry.isDirty;

              a1in.remove(removedFromAInEntry.fileId, removedFromAInEntry.pageIndex);

              removedFromAInEntry.dataPointer.decrementReadersReferrer();
              removedFromAInEntry.dataPointer = null;

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
          fileLock = fileLockManager.acquireSharedLock(removedEntry.fileId);
          try {
            pageLock = pageLockManager.acquireExclusiveLock(new PageKey(removedEntry.fileId, removedEntry.pageIndex));
            try {
              if (a1out.remove(removedEntry.fileId, removedEntry.pageIndex) == null)
                continue;

              assert removedEntry.usagesCount == 0;
              assert removedEntry.dataPointer == null;
              assert !removedEntry.isDirty;

              Set<Long> pageEntries = filePages.get(removedEntry.fileId);
              pageEntries.remove(removedEntry.pageIndex);
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
          fileLock = fileLockManager.acquireSharedLock(removedEntry.fileId);
          try {
            pageLock = pageLockManager.acquireExclusiveLock(new PageKey(removedEntry.fileId, removedEntry.pageIndex));
            try {
              if (am.get(removedEntry.fileId, removedEntry.pageIndex) == null)
                continue;

              if (removedEntry.usagesCount > 0)
                continue;

              assert !removedEntry.isDirty;

              am.remove(removedEntry.fileId, removedEntry.pageIndex);

              removedEntry.dataPointer.decrementReadersReferrer();
              removedEntry.dataPointer = null;

              Set<Long> pageEntries = filePages.get(removedEntry.fileId);
              pageEntries.remove(removedEntry.pageIndex);
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

  private OCacheEntry remove(long fileId, long pageIndex) {
    OCacheEntry cacheEntry = am.remove(fileId, pageIndex);
    if (cacheEntry != null) {
      if (cacheEntry.usagesCount > 1)
        throw new IllegalStateException("Record cannot be removed because it is used!");
      return cacheEntry;
    }

    cacheEntry = a1out.remove(fileId, pageIndex);
    if (cacheEntry != null)
      return cacheEntry;

    cacheEntry = a1in.remove(fileId, pageIndex);
    if (cacheEntry != null && cacheEntry.usagesCount > 1)
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
