/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.core.index.hashindex.local.cache;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.orientechnologies.common.concur.lock.OLockManager;
import com.orientechnologies.common.directmemory.ODirectMemory;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.config.OStorageSegmentConfiguration;
import com.orientechnologies.orient.core.storage.impl.local.OMultiFileSegment;
import com.orientechnologies.orient.core.storage.impl.local.OStorageLocal;

/**
 * @author Artem Loginov
 * @since 14.03.13
 */
public class O2QCache implements ODiskCache {
  private final int                          maxSize;
  private final int                          K_IN;
  private final int                          K_OUT;

  private final int                          pageSize;

  private LRUList                            am;
  private LRUList                            a1out;
  private LRUList                            a1in;

  private final ODirectMemory                directMemory;

  private final Map<Long, OMultiFileSegment> files;
  private final Map<FileLockKey, Long>       evictedPages;
  private final Map<Long, Set<Long>>         filesPages;

  // private final OLockManager<FileLockKey, Runnable> lockManager;
  private final Object                       syncObject;
  private final OStorageLocal                storageLocal;

  private final boolean                      syncOnPageFlush;
  private long                               fileCounter = 1;

  public O2QCache(long maxMemory, ODirectMemory directMemory, int pageSize, OStorageLocal storageLocal, boolean syncOnPageFlush) {
    this.directMemory = directMemory;
    this.pageSize = pageSize;
    this.storageLocal = storageLocal;
    this.syncOnPageFlush = syncOnPageFlush;
    this.files = new HashMap<Long, OMultiFileSegment>();
    this.filesPages = new HashMap<Long, Set<Long>>();

    this.evictedPages = new HashMap<FileLockKey, Long>();

    // this.lockManager = new OLockManager<FileLockKey, Runnable>(OGlobalConfiguration.ENVIRONMENT_CONCURRENT.getValueAsBoolean(),
    // OGlobalConfiguration.DISK_PAGE_CACHE_LOCK_TIMEOUT.getValueAsInteger());

    long tmpMaxSize = maxMemory / pageSize;
    if (tmpMaxSize >= Integer.MAX_VALUE) {
      maxSize = Integer.MAX_VALUE;
    } else {
      maxSize = (int) tmpMaxSize;
    }

    K_IN = maxSize >> 2;
    K_OUT = maxSize >> 1;

    am = new LRUList();
    a1out = new LRUList();
    a1in = new LRUList();

    syncObject = new Object();
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

  Map<FileLockKey, Long> getEvictedPages() {
    return evictedPages;
  }

  @Override
  public long openFile(OStorageSegmentConfiguration fileConfiguration, String fileExtension) throws IOException {
    synchronized (syncObject) {
      long fileId = fileCounter++;

      final OMultiFileSegment multiFileSegment = new OMultiFileSegment(storageLocal, fileConfiguration, fileExtension, pageSize);
      if (multiFileSegment.getFile(0).exists())
        multiFileSegment.open();
      else
        multiFileSegment.create(pageSize);

      files.put(fileId, multiFileSegment);

      filesPages.put(fileId, new HashSet<Long>());

      return fileId;
    }
  }

  @Override
  public long loadForWrite(long fileId, long pageIndex) throws IOException {
    synchronized (syncObject) {
      final LRUEntry lruEntry = updateCache(fileId, pageIndex, ODirectMemory.NULL_POINTER);
      lruEntry.usageCounter.incrementAndGet();
      // lockManager.acquireLock(Thread.currentThread(), new FileLockKey(fileId, pageIndex), OLockManager.LOCK.EXCLUSIVE);
      lruEntry.isDirty = true;

      return lruEntry.dataPointer;
    }
  }

  @Override
  public long allocateForWrite(long fileId, long pageIndex) throws IOException {
    synchronized (syncObject) {
      final OMultiFileSegment multiFileSegment = files.get(fileId);

      final long startPosition = pageIndex * pageSize;
      final long endPosition = startPosition + pageSize;

      if (multiFileSegment.getFilledUpTo() < endPosition)
        multiFileSegment.allocateSpaceContinuously((int) (endPosition - multiFileSegment.getFilledUpTo()));

      long dataPointer = directMemory.allocate(new byte[pageSize]);
      LRUEntry lruEntry = updateCache(fileId, pageIndex, dataPointer);
      lruEntry.usageCounter.incrementAndGet();

      return dataPointer;
    }
  }

  @Override
  public void cacheHit(long fileId, long pageIndex, long dataPointer) throws IOException {
    synchronized (syncObject) {
      updateCache(fileId, pageIndex, dataPointer);
    }
  }

  /**
   * This method does not affect cache statistic.
   * 
   * @param fileId
   *          identity of file. File marked by this identity will be used to find page in cache.
   * @param pageIndex
   *          index of page to search it in cache
   * @return address of page if it is already in cache. <code>ODirectMemory.NULL_POINTER</code> if page is absent in cache.
   */
  @Override
  public long getForWrite(long fileId, long pageIndex) {
    synchronized (syncObject) {

      // lockManager.acquireLock(Thread.currentThread(), new FileLockKey(fileId, pageIndex), OLockManager.LOCK.EXCLUSIVE);

      LRUEntry lruEntry = get(fileId, pageIndex);
      if (lruEntry != null) {
        lruEntry.usageCounter.incrementAndGet();
        return lruEntry.dataPointer;
      }

      return ODirectMemory.NULL_POINTER;
    }
  }

  @Override
  public void clearExternalManagementFlag(long fileId, long pageIndex) throws IOException {
    synchronized (syncObject) {
      LRUEntry lruEntry = am.get(fileId, pageIndex);

      if (lruEntry != null) {
        lruEntry.managedExternally = false;
        return;
      }

      lruEntry = a1out.get(fileId, pageIndex);
      if (lruEntry != null) {
        lruEntry.managedExternally = false;
        return;
      }

      lruEntry = a1in.get(fileId, pageIndex);
      if (lruEntry != null) {
        lruEntry.managedExternally = false;
      }
    }
  }

  @Override
  public long loadForRead(long fileId, long pageIndex) throws IOException {
    synchronized (syncObject) {
      final LRUEntry lruEntry = updateCache(fileId, pageIndex, ODirectMemory.NULL_POINTER);
      lruEntry.usageCounter.incrementAndGet();
      // lockManager.acquireLock(Thread.currentThread(), new FileLockKey(fileId, pageIndex), OLockManager.LOCK.SHARED);
      return lruEntry.dataPointer;
    }
  }

  @Override
  public void release(long fileId, long pageIndex) {
    LRUEntry lruEntry = get(fileId, pageIndex);
    if (lruEntry != null) {
      lruEntry.usageCounter.decrementAndGet();
    } else {
      // TODO replace with correct exception
      throw new RuntimeException("record should be released is already free!");
    }
  }

  @Override
  public long getFilledUpTo(long fileId) throws IOException {
    synchronized (syncObject) {
      return files.get(fileId).getFilledUpTo() / pageSize;
    }
  }

  @Override
  public void flushFile(long fileId) throws IOException {
    flushFile(fileId, false);
  }

  @Override
  public void flushFile(long fileId, boolean writeLock) throws IOException {
    OLockManager.LOCK lock = writeLock ? OLockManager.LOCK.EXCLUSIVE : OLockManager.LOCK.SHARED;

    synchronized (syncObject) {
      final Set<Long> pageIndexes = filesPages.get(fileId);

      Long[] sortedPageIndexes = new Long[pageIndexes.size()];
      sortedPageIndexes = pageIndexes.toArray(sortedPageIndexes);
      Arrays.sort(sortedPageIndexes);

      for (Long pageIndex : sortedPageIndexes) {
        LRUEntry lruEntry = get(fileId, pageIndex);
        if (lruEntry.isDirty && lruEntry.usageCounter.get() == 0) {
          flushData(fileId, lruEntry.pageIndex, lruEntry.dataPointer);
          lruEntry.isDirty = false;
        }
      }

      files.get(fileId).synch();
    }
  }

  @Override
  public void freePage(long fileId, long pageIndex) {
    synchronized (syncObject) {
      final Long dataPointer = evictedPages.remove(new FileLockKey(fileId, pageIndex));
      if (dataPointer != null) {
        directMemory.free(dataPointer);
        return;
      }

      LRUEntry lruEntry = remove(fileId, pageIndex);
      if (lruEntry != null)
        directMemory.free(lruEntry.dataPointer);
      filesPages.get(fileId).remove(pageIndex);
    }
  }

  @Override
  public void closeFile(long fileId) throws IOException {
    synchronized (syncObject) {
      if (!files.containsKey(fileId))
        return;

      final Set<Long> pageIndexes = filesPages.get(fileId);
      Long[] sortedPageIndexes = new Long[pageIndexes.size()];
      sortedPageIndexes = pageIndexes.toArray(sortedPageIndexes);
      Arrays.sort(sortedPageIndexes);

      for (Long pageIndex : sortedPageIndexes) {
        // lockManager.acquireLock(Thread.currentThread(), new FileLockKey(fileId, pageIndex), OLockManager.LOCK.EXCLUSIVE);
        // try {
        LRUEntry lruEntry = get(fileId, pageIndex);
        if (lruEntry == null || lruEntry.usageCounter.get() == 0) {
          lruEntry = remove(fileId, pageIndex);
          if (lruEntry != null && !lruEntry.managedExternally) {
            flushData(fileId, pageIndex, lruEntry.dataPointer);

            directMemory.free(lruEntry.dataPointer);
          }
        }
        // } finally {
        // lockManager.releaseLock(Thread.currentThread(), new FileLockKey(fileId, pageIndex), OLockManager.LOCK.EXCLUSIVE);
        // }
      }

      pageIndexes.clear();

      files.get(fileId).close();
    }

  }

  @Override
  public void deleteFile(long fileId) throws IOException {
    synchronized (syncObject) {
      if (!files.containsKey(fileId))
        return;

      truncateFile(fileId);
      files.get(fileId).delete();

      files.remove(fileId);
      filesPages.remove(fileId);
    }

  }

  @Override
  public void truncateFile(long fileId) throws IOException {
    synchronized (syncObject) {
      if (!files.containsKey(fileId))
        return;

      final Set<Long> pageEntries = filesPages.get(fileId);
      for (Long pageIndex : pageEntries) {
        LRUEntry lruEntry = get(fileId, pageIndex);
        if (lruEntry == null || lruEntry.usageCounter.get() == 0) {
          lruEntry = remove(fileId, pageIndex);
          if (lruEntry != null && !lruEntry.managedExternally && lruEntry.dataPointer != ODirectMemory.NULL_POINTER)
            directMemory.free(lruEntry.dataPointer);
        }
      }

      pageEntries.clear();
      files.get(fileId).truncate();
    }
  }

  @Override
  public void renameFile(long fileId, String oldFileName, String newFileName) throws IOException {
    synchronized (syncObject) {
      if (!files.containsKey(fileId))
        return;

      files.get(fileId).rename(oldFileName, newFileName);
    }

  }

  @Override
  public void flushBuffer() throws IOException {
    flushBuffer(false);
  }

  @Override
  public void flushBuffer(boolean writeLock) throws IOException {
    synchronized (syncObject) {
      for (LRUEntry entry : am) {
        if (entry.isDirty && entry.usageCounter.get() == 0) {
          flushData(entry.fileId, entry.pageIndex, entry.dataPointer);
          entry.isDirty = false;
        }
      }

      for (LRUEntry entry : a1in) {
        if (entry.isDirty && entry.usageCounter.get() == 0) {
          flushData(entry.fileId, entry.pageIndex, entry.dataPointer);
          entry.isDirty = false;
        }
      }

      flushEvictedPages();

      for (OMultiFileSegment multiFileSegment : files.values())
        multiFileSegment.synch();
    }
  }

  @Override
  public void clear() throws IOException {
    synchronized (syncObject) {
      flushBuffer(true);

      am.clear();
      a1in.clear();
      a1out.clear();
      for (Set<Long> fileEntries : filesPages.values())
        fileEntries.clear();
    }
  }

  @Override
  public void close() throws IOException {
    synchronized (syncObject) {
      clear();
      for (OMultiFileSegment multiFileSegment : files.values()) {
        multiFileSegment.synch();
        multiFileSegment.close();
      }
    }

  }

  @Override
  public boolean wasSoftlyClosed(long fileId) throws IOException {
    synchronized (syncObject) {
      OMultiFileSegment multiFileSegment = files.get(fileId);
      if (multiFileSegment == null)
        return false;

      return multiFileSegment.wasSoftlyClosedAtPreviousTime();
    }

  }

  @Override
  public void setSoftlyClosed(long fileId, boolean softlyClosed) throws IOException {
    synchronized (syncObject) {
      OMultiFileSegment multiFileSegment = files.get(fileId);
      if (multiFileSegment != null)
        multiFileSegment.setSoftlyClosed(softlyClosed);
    }
  }

  private LRUEntry updateCache(long fileId, long pageIndex, long dataPointer) throws IOException {
    LRUEntry lruEntry = am.get(fileId, pageIndex);
    if (lruEntry != null) {
      assert dataPointer == ODirectMemory.NULL_POINTER || lruEntry.dataPointer == dataPointer;
      assert lruEntry.managedExternally == (dataPointer != ODirectMemory.NULL_POINTER);

      lruEntry = am.putToMRU(fileId, pageIndex, lruEntry.dataPointer, lruEntry.isDirty, lruEntry.managedExternally);

      return lruEntry;
    }

    lruEntry = a1out.get(fileId, pageIndex);
    if (lruEntry != null) {
      assert dataPointer == ODirectMemory.NULL_POINTER || lruEntry.dataPointer == dataPointer;
      assert lruEntry.managedExternally == (dataPointer != ODirectMemory.NULL_POINTER);
      removeColdestPageIfNeeded();

      CacheResult cacheResult = cacheFileContent(fileId, pageIndex);
      lruEntry.dataPointer = cacheResult.dataPointer;
      lruEntry.isDirty = cacheResult.isDirty;

      lruEntry = am.putToMRU(fileId, pageIndex, lruEntry.dataPointer, lruEntry.isDirty, lruEntry.managedExternally);
      return lruEntry;
    }

    lruEntry = a1in.get(fileId, pageIndex);
    if (lruEntry != null) {
      assert dataPointer == ODirectMemory.NULL_POINTER || lruEntry.dataPointer == dataPointer;
      assert lruEntry.managedExternally == (dataPointer != ODirectMemory.NULL_POINTER);
      return lruEntry;
    }

    removeColdestPageIfNeeded();

    if (dataPointer == ODirectMemory.NULL_POINTER) {
      CacheResult cacheResult = cacheFileContent(fileId, pageIndex);
      lruEntry = a1in.putToMRU(fileId, pageIndex, cacheResult.dataPointer, cacheResult.isDirty, false);
    } else {
      lruEntry = a1in.putToMRU(fileId, pageIndex, dataPointer, false, true);
    }

    filesPages.get(fileId).add(pageIndex);

    return lruEntry;
  }

  private void removeColdestPageIfNeeded() throws IOException {
    if (am.size() + a1in.size() >= maxSize) {
      if (a1in.size() > K_IN) {
        LRUEntry removedFromAInEntry = a1in.removeLRU();
        assert removedFromAInEntry.usageCounter.get() == 0;
        evictFileContent(removedFromAInEntry.fileId, removedFromAInEntry.pageIndex, removedFromAInEntry.dataPointer,
            removedFromAInEntry.isDirty, removedFromAInEntry.managedExternally);
        assert removedFromAInEntry.usageCounter.get() == 0;
        a1out.putToMRU(removedFromAInEntry.fileId, removedFromAInEntry.pageIndex, ODirectMemory.NULL_POINTER, false,
            removedFromAInEntry.managedExternally);
        if (a1out.size() > K_OUT) {
          LRUEntry removedEntry = a1out.removeLRU();
          assert removedEntry.usageCounter.get() == 0;
          Set<Long> pageEntries = filesPages.get(removedEntry.fileId);
          pageEntries.remove(removedEntry.pageIndex);
        }
      } else {
        LRUEntry removedEntry = am.removeLRU();
        assert removedEntry.usageCounter.get() == 0;
        evictFileContent(removedEntry.fileId, removedEntry.pageIndex, removedEntry.dataPointer, removedEntry.isDirty,
            removedEntry.managedExternally);
        Set<Long> pageEntries = filesPages.get(removedEntry.fileId);
        pageEntries.remove(removedEntry.pageIndex);
      }
    }
  }

  private CacheResult cacheFileContent(long fileId, long pageIndex) throws IOException {
    FileLockKey key = new FileLockKey(fileId, pageIndex);
    if (evictedPages.containsKey(key))
      return new CacheResult(true, evictedPages.remove(key));

    final OMultiFileSegment multiFileSegment = files.get(fileId);
    final long startPosition = pageIndex * pageSize;
    final long endPosition = startPosition + pageSize;

    byte[] content = new byte[pageSize];
    if (multiFileSegment.getFilledUpTo() >= endPosition) {
      multiFileSegment.readContinuously(startPosition, content, content.length);
    } else {
      multiFileSegment.allocateSpaceContinuously((int) (endPosition - multiFileSegment.getFilledUpTo()));
    }

    long dataPointer;
    dataPointer = directMemory.allocate(content);

    return new CacheResult(false, dataPointer);
  }

  private void evictFileContent(long fileId, long pageIndex, long dataPointer, boolean isDirty, boolean mangedExternally)
      throws IOException {
    if (mangedExternally)
      return;

    if (isDirty) {
      if (evictedPages.size() >= OGlobalConfiguration.DISK_CACHE_WRITE_QUEUE_LENGTH.getValueAsInteger()) {
        flushEvictedPages();

        flushData(fileId, pageIndex, dataPointer);
        directMemory.free(dataPointer);
      } else {
        evictedPages.put(new FileLockKey(fileId, pageIndex), dataPointer);
      }
    } else {
      directMemory.free(dataPointer);
    }

  }

  @Override
  public void flushData(final long fileId, final long pageIndex, final long dataPointer) throws IOException {
    final byte[] content = directMemory.get(dataPointer, pageSize);

    final OMultiFileSegment multiFileSegment = files.get(fileId);

    multiFileSegment.writeContinuously(pageIndex * pageSize, content);

    if (syncOnPageFlush)
      multiFileSegment.synch();
  }

  private void flushEvictedPages() throws IOException {
    Map.Entry<FileLockKey, Long>[] sortedPages = evictedPages.entrySet().toArray(new Map.Entry[evictedPages.size()]);
    Arrays.sort(sortedPages, new Comparator<Map.Entry>() {
      @Override
      public int compare(Map.Entry entryOne, Map.Entry entryTwo) {
        FileLockKey fileLockKeyOne = (FileLockKey) entryOne.getKey();
        FileLockKey fileLockKeyTwo = (FileLockKey) entryTwo.getKey();
        return fileLockKeyOne.compareTo(fileLockKeyTwo);
      }
    });

    for (Map.Entry<FileLockKey, Long> entry : sortedPages) {
      long evictedDataPointer = entry.getValue();
      FileLockKey fileLockKey = entry.getKey();

      flushData(fileLockKey.fileId, fileLockKey.pageIndex, evictedDataPointer);

      directMemory.free(evictedDataPointer);
    }

    evictedPages.clear();
  }

  private static class CacheResult {
    private final boolean isDirty;
    private final long    dataPointer;

    private CacheResult(boolean dirty, long dataPointer) {
      isDirty = dirty;
      this.dataPointer = dataPointer;
    }
  }

  private LRUEntry get(long fileId, long pageIndex) {
    LRUEntry lruEntry = am.get(fileId, pageIndex);

    if (lruEntry != null) {
      return lruEntry;
    }

    lruEntry = a1in.get(fileId, pageIndex);
    return lruEntry;
  }

  private LRUEntry remove(long fileId, long pageIndex) {
    LRUEntry lruEntry = am.remove(fileId, pageIndex);
    if (lruEntry != null) {
      if (lruEntry.usageCounter.get() > 1)
        throw new RuntimeException("Record cannot be removed because it is used!");
      return lruEntry;
    }
    lruEntry = a1out.remove(fileId, pageIndex);
    if (lruEntry != null) {
      return lruEntry;
    }
    lruEntry = a1in.remove(fileId, pageIndex);
    if (lruEntry != null && lruEntry.usageCounter.get() > 1)
      throw new RuntimeException("Record cannot be removed because it is used!");
    return lruEntry;
  }

  private final class FileLockKey implements Comparable<FileLockKey> {
    private final long fileId;
    private final long pageIndex;

    private FileLockKey(long fileId, long pageIndex) {
      this.fileId = fileId;
      this.pageIndex = pageIndex;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o)
        return true;
      if (o == null || getClass() != o.getClass())
        return false;

      FileLockKey that = (FileLockKey) o;

      if (fileId != that.fileId)
        return false;
      if (pageIndex != that.pageIndex)
        return false;

      return true;
    }

    @Override
    public int hashCode() {
      int result = (int) (fileId ^ (fileId >>> 32));
      result = 31 * result + (int) (pageIndex ^ (pageIndex >>> 32));
      return result;
    }

    @Override
    public int compareTo(FileLockKey otherKey) {
      if (fileId > otherKey.fileId)
        return 1;
      if (fileId < otherKey.fileId)
        return -1;

      if (pageIndex > otherKey.pageIndex)
        return 1;
      if (pageIndex < otherKey.pageIndex)
        return -1;

      return 0;
    }
  }
}
