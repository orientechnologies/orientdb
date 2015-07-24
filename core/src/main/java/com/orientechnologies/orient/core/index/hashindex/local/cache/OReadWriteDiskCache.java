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

import com.orientechnologies.common.concur.lock.ONewLockManager;
import com.orientechnologies.common.concur.lock.OReadersWriterSpinLock;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.profiler.OAbstractProfiler.OProfilerHookValue;
import com.orientechnologies.common.profiler.OProfilerMBean;
import com.orientechnologies.common.profiler.OProfilerMBean.METRIC_TYPE;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.exception.OAllCacheEntriesAreUsedException;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.storage.impl.local.OLowDiskSpaceListener;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OLocalPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWriteAheadLog;

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

/**
 * @author Andrey Lomakin
 * @since 7/24/13
 */
public class OReadWriteDiskCache implements ODiskCache {
  public static final int                             MIN_CACHE_SIZE             = 256;

  private static final int                            MAX_CACHE_OVERFLOW         = Runtime.getRuntime().availableProcessors() * 8;

  private volatile int                                maxSize;
  private volatile int                                K_IN;
  private volatile int                                K_OUT;

  private final LRUList                               am;
  private final LRUList                               a1out;
  private final LRUList                               a1in;

  private final OWOWCache                             writeCache;
  private final int                                   pageSize;

  /**
   * Contains all pages in cache for given file.
   */
  private final ConcurrentMap<Long, Set<Long>>        filePages;

  private final OReadersWriterSpinLock                cacheLock                  = new OReadersWriterSpinLock();
  private final ONewLockManager                       fileLockManager            = new ONewLockManager(true);
  private final ONewLockManager<PageKey>              pageLockManager            = new ONewLockManager<PageKey>();
  private final NavigableMap<PinnedPage, OCacheEntry> pinnedPages                = new ConcurrentSkipListMap<PinnedPage, OCacheEntry>();

  private final String                                storageName;

  private final AtomicBoolean                         coldPagesRemovalInProgress = new AtomicBoolean();

  private static String                               METRIC_HITS;
  private static String                               METRIC_HITS_METADATA;
  private static String                               METRIC_MISSED;
  private static String                               METRIC_MISSED_METADATA;

  public OReadWriteDiskCache(final long readCacheMaxMemory, final long writeCacheMaxMemory, final int pageSize,
      final long writeGroupTTL, final int pageFlushInterval, final OLocalPaginatedStorage storageLocal,
      final OWriteAheadLog writeAheadLog, final boolean syncOnPageFlush, final boolean checkMinSize) {
    this(null, readCacheMaxMemory, writeCacheMaxMemory, pageSize, writeGroupTTL, pageFlushInterval, storageLocal, writeAheadLog,
        syncOnPageFlush, checkMinSize);
  }

  public OReadWriteDiskCache(final String storageName, final long readCacheMaxMemory, final long writeCacheMaxMemory,
      final int pageSize, final long writeGroupTTL, final int pageFlushInterval, final OLocalPaginatedStorage storageLocal,
      final OWriteAheadLog writeAheadLog, final boolean syncOnPageFlush, final boolean checkMinSize) {
    cacheLock.acquireWriteLock();
    try {
      this.storageName = storageName;
      this.pageSize = pageSize;

      initProfiler();

      this.filePages = new ConcurrentHashMap<Long, Set<Long>>();

      maxSize = normalizeMemory(readCacheMaxMemory, pageSize);
      if (checkMinSize && maxSize < MIN_CACHE_SIZE)
        maxSize = MIN_CACHE_SIZE;

      this.writeCache = new OWOWCache(syncOnPageFlush, pageSize, writeGroupTTL, writeAheadLog, pageFlushInterval, normalizeMemory(
          writeCacheMaxMemory, pageSize), storageLocal, checkMinSize);

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
  public long addFile(String fileName) throws IOException {
    cacheLock.acquireWriteLock();
    try {
      long fileId = writeCache.addFile(fileName);
      filePages.put(fileId, Collections.newSetFromMap(new ConcurrentHashMap<Long, Boolean>()));
      return fileId;
    } finally {
      cacheLock.releaseWriteLock();
    }
  }

  @Override
  public long openFile(final String fileName) throws IOException {
    cacheLock.acquireWriteLock();
    try {
      long fileId = writeCache.isOpen(fileName);
      if (fileId >= 0)
        return fileId;

      fileId = writeCache.openFile(fileName);
      filePages.put(fileId, Collections.newSetFromMap(new ConcurrentHashMap<Long, Boolean>()));

      return fileId;
    } finally {
      cacheLock.releaseWriteLock();
    }
  }

  @Override
  public long bookFileId(String fileName) throws IOException {
    return writeCache.bookFileId(fileName);
  }

  @Override
  public void openFile(final long fileId) throws IOException {
    cacheLock.acquireReadLock();
    Lock fileLock;
    try {
      fileLock = fileLockManager.acquireExclusiveLock(fileId);
      try {
        if (writeCache.isOpen(fileId))
          return;

        writeCache.openFile(fileId);
        filePages.put(fileId, Collections.newSetFromMap(new ConcurrentHashMap<Long, Boolean>()));

      } finally {
        fileLockManager.releaseLock(fileLock);
      }
    } finally {
      cacheLock.releaseReadLock();
    }
  }

  @Override
  public void openFile(String fileName, long fileId) throws IOException {
    cacheLock.acquireWriteLock();
    try {
      long existingFileId = writeCache.isOpen(fileName);

      if (fileId == existingFileId)
        return;
      else if (existingFileId >= 0)
        throw new OStorageException("File with given name already exists but has different id " + existingFileId + " vs. proposed "
            + fileId);

      writeCache.openFile(fileName, fileId);
      filePages.put(fileId, Collections.newSetFromMap(new ConcurrentHashMap<Long, Boolean>()));
    } finally {
      cacheLock.releaseWriteLock();
    }
  }

  @Override
  public void addFile(String fileName, long fileId) throws IOException {
    cacheLock.acquireWriteLock();
    try {
      writeCache.addFile(fileName, fileId);
      filePages.put(fileId, Collections.newSetFromMap(new ConcurrentHashMap<Long, Boolean>()));
    } finally {
      cacheLock.releaseWriteLock();
    }
  }

  @Override
  public boolean exists(final String fileName) {
    return writeCache.exists(fileName);
  }

  @Override
  public boolean exists(long fileId) {
    return writeCache.exists(fileId);
  }

  @Override
  public String fileNameById(long fileId) {
    return writeCache.fileNameById(fileId);
  }

  @Override
  public void lock() throws IOException {
    writeCache.lock();
  }

  @Override
  public void unlock() throws IOException {
    writeCache.unlock();
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
  public OCacheEntry load(final long fileId, final long pageIndex, final boolean checkPinnedPages) throws IOException {
    final UpdateCacheResult cacheResult = doLoad(fileId, pageIndex, checkPinnedPages, false);
    if (cacheResult == null)
      return null;

    try {
      if (cacheResult.removeColdPages)
        removeColdestPagesIfNeeded();
    } catch (RuntimeException e) {
      assert !cacheResult.cacheEntry.isDirty;

      release(cacheResult.cacheEntry);
      throw e;
    }

    return cacheResult.cacheEntry;
  }

  private UpdateCacheResult doLoad(long fileId, long pageIndex, boolean checkPinnedPages, boolean addNewPages) throws IOException {
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
            UpdateCacheResult cacheResult = updateCache(fileId, pageIndex, addNewPages);
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
  public OCacheEntry allocateNewPage(final long fileId) throws IOException {
    UpdateCacheResult cacheResult;

    Lock fileLock;
    cacheLock.acquireReadLock();
    try {
      fileLock = fileLockManager.acquireExclusiveLock(fileId);
      try {
        final long filledUpTo = getFilledUpTo(fileId);
        cacheResult = doLoad(fileId, filledUpTo, false, true);
      } finally {
        fileLockManager.releaseLock(fileLock);
      }
    } finally {
      cacheLock.releaseReadLock();
    }

    try {
      if (cacheResult.removeColdPages)
        removeColdestPagesIfNeeded();
    } catch (RuntimeException e) {
      assert !cacheResult.cacheEntry.isDirty;

      release(cacheResult.cacheEntry);
      throw e;
    }

    return cacheResult.cacheEntry;
  }

  @Override
  public void release(OCacheEntry cacheEntry) {
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

  @Override
  public long getFilledUpTo(long fileId) throws IOException {
    return writeCache.getFilledUpTo(fileId);
  }

  @Override
  public void flushFile(long fileId) throws IOException {
    writeCache.flush(fileId);
  }

  @Override
  public void closeFile(final long fileId) throws IOException {
    closeFile(fileId, true);
  }

  @Override
  public void closeFile(long fileId, boolean flush) throws IOException {
    if (!isOpen(fileId))
      return;

    Lock fileLock;
    cacheLock.acquireReadLock();
    try {
      fileLock = fileLockManager.acquireExclusiveLock(fileId);
      try {
        writeCache.close(fileId, flush);

        final Set<Long> pageIndexes = filePages.get(fileId);
        if (pageIndexes == null)
          return;

        for (Long pageIndex : pageIndexes) {
          OCacheEntry cacheEntry = get(fileId, pageIndex, true);
          if (cacheEntry == null)
            cacheEntry = pinnedPages.get(new PinnedPage(fileId, pageIndex));

          if (cacheEntry != null) {
            if (cacheEntry.dataPointer != null) {
              if (cacheEntry.usagesCount == 0) {
                cacheEntry = remove(fileId, pageIndex);

                if (cacheEntry == null)
                  cacheEntry = pinnedPages.remove(new PinnedPage(fileId, pageIndex));
              } else
                throw new OStorageException("Page with index " + pageIndex + " for file with id " + fileId
                    + " can not be freed because it is used.");

              cacheEntry.dataPointer.decrementReferrer();
              cacheEntry.dataPointer = null;
            }
          } else {
            throw new OStorageException("Page with index " + pageIndex + " for file with id " + fileId + " was not found in cache");
          }
        }

      } finally {
        fileLockManager.releaseLock(fileLock);
      }

    } finally {
      cacheLock.releaseReadLock();
    }
  }

  @Override
  public void deleteFile(long fileId) throws IOException {
    Lock fileLock;

    cacheLock.acquireReadLock();
    try {
      fileLock = fileLockManager.acquireExclusiveLock(fileId);
      try {
        if (isOpen(fileId))
          truncateFile(fileId);

        writeCache.deleteFile(fileId);
        filePages.remove(fileId);
      } finally {
        fileLockManager.releaseLock(fileLock);
      }
    } finally {
      cacheLock.releaseReadLock();
    }
  }

  @Override
  public void truncateFile(long fileId) throws IOException {
    Lock fileLock;

    cacheLock.acquireReadLock();
    try {
      fileLock = fileLockManager.acquireExclusiveLock(fileId);
      try {
        writeCache.truncateFile(fileId);

        final Set<Long> pageEntries = filePages.get(fileId);
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
                cacheEntry.dataPointer.decrementReferrer();
                cacheEntry.dataPointer = null;
              }

            }
          } else
            throw new OStorageException("Page with index " + pageIndex + " was  not found in cache for file with id " + fileId);
        }

        pageEntries.clear();
      } finally {
        fileLockManager.releaseLock(fileLock);
      }
    } finally {
      cacheLock.releaseReadLock();
    }
  }

  @Override
  public void renameFile(long fileId, String oldFileName, String newFileName) throws IOException {
    Lock fileLock;
    cacheLock.acquireReadLock();
    try {
      fileLock = fileLockManager.acquireExclusiveLock(fileId);
      try {
        writeCache.renameFile(fileId, oldFileName, newFileName);
      } finally {
        fileLockManager.releaseLock(fileLock);
      }
    } finally {
      cacheLock.releaseReadLock();
    }

  }

  @Override
  public void flushBuffer() throws IOException {
    writeCache.flush();
  }

  public void clear() throws IOException {
    writeCache.flush();

    cacheLock.acquireWriteLock();
    try {
      clearCacheContent();
    } finally {
      cacheLock.releaseWriteLock();
    }
  }

  private void clearCacheContent() {
    for (OCacheEntry cacheEntry : am)
      if (cacheEntry.usagesCount == 0) {
        cacheEntry.dataPointer.decrementReferrer();
        cacheEntry.dataPointer = null;
      }

      else
        throw new OStorageException("Page with index " + cacheEntry.pageIndex + " for file id " + cacheEntry.fileId
            + " is used and can not be removed");

    for (OCacheEntry cacheEntry : a1in)
      if (cacheEntry.usagesCount == 0) {
        cacheEntry.dataPointer.decrementReferrer();
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
        pinnedEntry.dataPointer.decrementReferrer();
        pinnedEntry.dataPointer = null;
      } else
        throw new OStorageException("Page with index " + pinnedEntry.pageIndex + " for file with id " + pinnedEntry.fileId
            + "can not be freed because it is used.");
    }

    pinnedPages.clear();
  }

  @Override
  public void close() throws IOException {
    cacheLock.acquireWriteLock();
    try {
      clear();
      writeCache.close();

      deinitProfiler();
    } finally {
      cacheLock.releaseWriteLock();
    }
  }

  @Override
  public boolean wasSoftlyClosed(long fileId) throws IOException {
    return writeCache.wasSoftlyClosed(fileId);
  }

  @Override
  public void setSoftlyClosed(long fileId, boolean softlyClosed) throws IOException {
    writeCache.setSoftlyClosed(fileId, softlyClosed);
  }

  @Override
  public void setSoftlyClosed(boolean softlyClosed) throws IOException {
    writeCache.setSoftlyClosed(softlyClosed);
  }

  @Override
  public boolean isOpen(long fileId) {
    return writeCache.isOpen(fileId);
  }

  @Override
  public void addLowDiskSpaceListener(OLowDiskSpaceListener listener) {
    writeCache.addLowDiskSpaceListener(listener);
  }

  @Override
  public void removeLowDiskSpaceListener(OLowDiskSpaceListener listener) {
    writeCache.removeLowDiskSpaceListener(listener);
  }

  private UpdateCacheResult updateCache(final long fileId, final long pageIndex, final boolean addNewPages) throws IOException {
    final OProfilerMBean profiler = storageName != null ? Orient.instance().getProfiler() : null;
    final long startTime = storageName != null ? System.currentTimeMillis() : 0;

    OCacheEntry cacheEntry = am.get(fileId, pageIndex);

    if (cacheEntry != null) {
      am.putToMRU(cacheEntry);

      if (profiler != null && profiler.isRecording())
        profiler.stopChrono(METRIC_HITS, "Requested item was found in Disk Cache", startTime, METRIC_HITS_METADATA);

      return new UpdateCacheResult(false, cacheEntry);
    }

    if (profiler != null && profiler.isRecording())
      profiler.stopChrono(METRIC_MISSED, "Requested item was not found in Disk Cache", startTime, METRIC_MISSED_METADATA);

    cacheEntry = a1out.remove(fileId, pageIndex);
    if (cacheEntry != null) {
      OCachePointer dataPointer = writeCache.load(fileId, pageIndex, false);

      assert dataPointer != null;
      assert cacheEntry.dataPointer == null;
      assert !cacheEntry.isDirty;

      cacheEntry.dataPointer = dataPointer;

      am.putToMRU(cacheEntry);

      return new UpdateCacheResult(true, cacheEntry);
    }

    cacheEntry = a1in.get(fileId, pageIndex);
    if (cacheEntry != null)
      return new UpdateCacheResult(false, cacheEntry);

    OCachePointer dataPointer = writeCache.load(fileId, pageIndex, addNewPages);
    if (dataPointer == null)
      return null;

    cacheEntry = new OCacheEntry(fileId, pageIndex, dataPointer, false);
    a1in.putToMRU(cacheEntry);

    Set<Long> pages = filePages.get(fileId);
    if (pages == null) {
      pages = Collections.newSetFromMap(new ConcurrentHashMap<Long, Boolean>());
      filePages.put(fileId, pages);
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
          increaseCacheSize();
        } else {
          assert removedFromAInEntry.usagesCount == 0;
          assert !removedFromAInEntry.isDirty;

          removedFromAInEntry.dataPointer.decrementReferrer();
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
          increaseCacheSize();
        } else {
          assert removedEntry.usagesCount == 0;
          assert !removedEntry.isDirty;

          removedEntry.dataPointer.decrementReferrer();
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
          increaseCacheSize();
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

              removedFromAInEntry.dataPointer.decrementReferrer();
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
              if (a1out.get(removedEntry.fileId, removedEntry.pageIndex) == null)
                continue;

              assert removedEntry.usagesCount == 0;
              assert removedEntry.dataPointer == null;
              assert !removedEntry.isDirty;

              a1out.remove(removedEntry.fileId, removedEntry.pageIndex);

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
          increaseCacheSize();
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

              removedEntry.dataPointer.decrementReferrer();
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

  private void increaseCacheSize() {
    String message = "All records in aIn queue in 2q cache are used!";
    OLogManager.instance().warn(this, message);
    if (OGlobalConfiguration.SERVER_CACHE_INCREASE_ON_DEMAND.getValueAsBoolean()) {
      OLogManager.instance().warn(this, "Cache size will be increased.");
      maxSize = (int) Math.ceil(maxSize * (1 + OGlobalConfiguration.SERVER_CACHE_INCREASE_STEP.getValueAsFloat()));
      K_IN = maxSize >> 2;
      K_OUT = maxSize >> 1;
    } else {
      throw new OAllCacheEntriesAreUsedException(message);
    }
  }

  @Override
  public OPageDataVerificationError[] checkStoredPages(OCommandOutputListener commandOutputListener) {
    return writeCache.checkStoredPages(commandOutputListener);
  }

  @Override
  public void delete() throws IOException {
    cacheLock.acquireWriteLock();
    try {
      writeCache.delete();

      clearCacheContent();

      deinitProfiler();
    } finally {
      cacheLock.releaseWriteLock();
    }
  }

  int getMaxSize() {
    return maxSize;
  }

  @Override
  public long getUsedMemory() {
    return (am.size() + a1in.size() + writeCache.getAllocatedPages()) * (2 * ODurablePage.PAGE_PADDING + pageSize);
  }

  @Override
  public void startFuzzyCheckpoints() {
    writeCache.startFuzzyCheckpoints();
  }

  @Override
  public boolean checkLowDiskSpace() {
    return writeCache.checkLowDiskSpace();
  }

  @Override
  public void makeFuzzyCheckpoint() {
    writeCache.makeFuzzyCheckpoint();
  }

  private OCacheEntry get(long fileId, long pageIndex, boolean useOutQueue) {
    OCacheEntry cacheEntry = am.get(fileId, pageIndex);

    if (cacheEntry != null)
      return cacheEntry;

    if (useOutQueue) {
      cacheEntry = a1out.get(fileId, pageIndex);
      if (cacheEntry != null)
        return cacheEntry;
    }

    cacheEntry = a1in.get(fileId, pageIndex);
    return cacheEntry;
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

  private class PinnedPage implements Comparable<PinnedPage> {
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

  private void initProfiler() {
    final OProfilerMBean profiler = Orient.instance().getProfiler();

    METRIC_HITS = profiler.getDatabaseMetric(storageName, "diskCache.hits");
    METRIC_HITS_METADATA = profiler.getDatabaseMetric(null, "diskCache.hits");
    METRIC_MISSED = profiler.getDatabaseMetric(storageName, "diskCache.missed");
    METRIC_MISSED_METADATA = profiler.getDatabaseMetric(null, "diskCache.missed");

    profiler.registerHookValue(profiler.getDatabaseMetric(storageName, "diskCache.totalMemory"), "Total memory used by Disk Cache",
        METRIC_TYPE.SIZE, new OProfilerHookValue() {
          @Override
          public Object getValue() {
            return (am.size() + a1in.size()) * pageSize;
          }
        }, profiler.getDatabaseMetric(null, "diskCache.totalMemory"));

    profiler.registerHookValue(profiler.getDatabaseMetric(storageName, "diskCache.maxMemory"), "Maximum memory used by Disk Cache",
        METRIC_TYPE.SIZE, new OProfilerHookValue() {
          @Override
          public Object getValue() {
            return maxSize * pageSize;
          }
        }, profiler.getDatabaseMetric(null, "diskCache.maxMemory"));
  }

  private void deinitProfiler() {
    final OProfilerMBean profiler = Orient.instance().getProfiler();
    profiler.unregisterHookValue(profiler.getDatabaseMetric(storageName, "diskCache.totalMemory"));
    profiler.unregisterHookValue(profiler.getDatabaseMetric(storageName, "diskCache.maxMemory"));
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
