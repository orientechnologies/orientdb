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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

import com.orientechnologies.common.concur.lock.OLockManager;
import com.orientechnologies.common.directmemory.ODirectMemory;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.config.OStorageSegmentConfiguration;
import com.orientechnologies.orient.core.storage.impl.local.OMultiFileSegment;
import com.orientechnologies.orient.core.storage.impl.local.OStorageLocal;

/**
 * @author Andrey Lomakin
 * @since 25.02.13
 */
public class OLRUCache implements ODiskCache {
  private final int                                 maxSize;
  private final int                                 pageSize;

  private final LRUList                             lruList;
  private final ODirectMemory                       directMemory;

  private final Map<Long, OMultiFileSegment>        files;
  private final NavigableMap<FileLockKey, Long>     evictedPages;

  private final Map<Long, Set<Long>>                filesPages;

  private final OLockManager<FileLockKey, Runnable> lockManager;
  private final Object                              syncObject;
  private final OStorageLocal                       storageLocal;

  private final boolean                             syncOnPageFlush;
  private long                                      fileCounter = 1;

  public OLRUCache(long maxMemory, ODirectMemory directMemory, int pageSize, OStorageLocal storageLocal, boolean syncOnPageFlush) {
    this.directMemory = directMemory;
    this.pageSize = pageSize;
    this.storageLocal = storageLocal;
    this.syncOnPageFlush = syncOnPageFlush;
    this.files = new HashMap<Long, OMultiFileSegment>();
    this.filesPages = new HashMap<Long, Set<Long>>();

    this.evictedPages = new TreeMap<FileLockKey, Long>();

    this.lockManager = new OLockManager<FileLockKey, Runnable>(OGlobalConfiguration.ENVIRONMENT_CONCURRENT.getValueAsBoolean(),
        OGlobalConfiguration.DISK_PAGE_CACHE_LOCK_TIMEOUT.getValueAsInteger());

    long tmpMaxSize = maxMemory / pageSize;
    if (tmpMaxSize >= Integer.MAX_VALUE)
      maxSize = Integer.MAX_VALUE;
    else
      maxSize = (int) tmpMaxSize;

    lruList = new LRUList();

    syncObject = new Object();
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
      lockManager.acquireLock(Thread.currentThread(), new FileLockKey(fileId, pageIndex), OLockManager.LOCK.EXCLUSIVE);
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
      updateCache(fileId, pageIndex, dataPointer);
      lockManager.acquireLock(Thread.currentThread(), new FileLockKey(fileId, pageIndex), OLockManager.LOCK.EXCLUSIVE);

      return dataPointer;
    }
  }

  @Override
  public void cacheHit(long fileId, long pageIndex, long dataPointer) throws IOException {
    synchronized (syncObject) {
      updateCache(fileId, pageIndex, dataPointer);
    }
  }

  @Override
  public long getForWrite(long fileId, long pageIndex) {
    synchronized (syncObject) {
      lockManager.acquireLock(Thread.currentThread(), new FileLockKey(fileId, pageIndex), OLockManager.LOCK.EXCLUSIVE);

      LRUEntry lruEntry = lruList.get(fileId, pageIndex);
      if (lruEntry != null)
        return lruEntry.dataPointer;

      return ODirectMemory.NULL_POINTER;
    }
  }

  @Override
  public void clearExternalManagementFlag(long fileId, long pageIndex) {
    synchronized (syncObject) {
      LRUEntry lruEntry = lruList.get(fileId, pageIndex);
      if (lruEntry != null)
        lruEntry.managedExternally = false;
    }
  }

  @Override
  public long loadForRead(long fileId, long pageIndex) throws IOException {
    synchronized (syncObject) {
      final LRUEntry lruEntry = updateCache(fileId, pageIndex, ODirectMemory.NULL_POINTER);

      lockManager.acquireLock(Thread.currentThread(), new FileLockKey(fileId, pageIndex), OLockManager.LOCK.SHARED);
      return lruEntry.dataPointer;
    }
  }

  @Override
  public void release(long fileId, long pageIndex) {
    lockManager.releaseLock(Thread.currentThread(), new FileLockKey(fileId, pageIndex), OLockManager.LOCK.SHARED);
  }

  // @Override
  // public void releaseWriteLock(long fileId, long pageIndex) {
  // lockManager.releaseLock(Thread.currentThread(), new FileLockKey(fileId, pageIndex), OLockManager.LOCK.EXCLUSIVE);
  // }

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
        lockManager.acquireLock(Thread.currentThread(), new FileLockKey(fileId, pageIndex), lock);
        try {
          LRUEntry lruEntry = lruList.get(fileId, pageIndex);
          if (lruEntry.isDirty) {
            flushData(fileId, lruEntry.pageIndex, lruEntry.dataPointer);
            lruEntry.isDirty = false;
          }

        } finally {
          lockManager.releaseLock(Thread.currentThread(), new FileLockKey(fileId, pageIndex), lock);
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

      LRUEntry lruEntry = lruList.remove(fileId, pageIndex);
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
        lockManager.acquireLock(Thread.currentThread(), new FileLockKey(fileId, pageIndex), OLockManager.LOCK.EXCLUSIVE);
        try {
          LRUEntry lruEntry = lruList.remove(fileId, pageIndex);
          if (lruEntry != null && !lruEntry.managedExternally) {
            if (lruEntry.isDirty)
              flushData(fileId, pageIndex, lruEntry.dataPointer);

            directMemory.free(lruEntry.dataPointer);
          }
        } finally {
          lockManager.releaseLock(Thread.currentThread(), new FileLockKey(fileId, pageIndex), OLockManager.LOCK.EXCLUSIVE);
        }
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

      final Set<Long> pageIndexes = filesPages.get(fileId);
      for (Long pageIndex : pageIndexes) {
        lockManager.acquireLock(Thread.currentThread(), new FileLockKey(fileId, pageIndex), OLockManager.LOCK.EXCLUSIVE);
        try {
          LRUEntry lruEntry = lruList.remove(fileId, pageIndex);
          if (lruEntry != null && !lruEntry.managedExternally)
            directMemory.free(lruEntry.dataPointer);
        } finally {
          lockManager.releaseLock(Thread.currentThread(), new FileLockKey(fileId, pageIndex), OLockManager.LOCK.EXCLUSIVE);
        }
      }

      NavigableMap<FileLockKey, Long> fileEvictedPages = evictedPages.subMap(new FileLockKey(fileId, 0), true, new FileLockKey(
          fileId, Integer.MAX_VALUE), true);
      for (long pointer : fileEvictedPages.values())
        directMemory.free(pointer);

      fileEvictedPages.clear();
      pageIndexes.clear();
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
    OLockManager.LOCK lock = writeLock ? OLockManager.LOCK.EXCLUSIVE : OLockManager.LOCK.SHARED;

    synchronized (syncObject) {
      for (LRUEntry entry : lruList) {
        lockManager.acquireLock(Thread.currentThread(), new FileLockKey(entry.fileId, entry.pageIndex), lock);
        try {
          if (entry.isDirty) {
            flushData(entry.fileId, entry.pageIndex, entry.dataPointer);
            entry.isDirty = false;
          }
        } finally {
          lockManager.releaseLock(Thread.currentThread(), new FileLockKey(entry.fileId, entry.pageIndex), lock);
        }
      }
      for (OMultiFileSegment multiFileSegment : files.values())
        multiFileSegment.synch();
    }
  }

  @Override
  public void clear() throws IOException {
    synchronized (syncObject) {
      flushBuffer(true);

      lruList.clear();
      for (Set<Long> fileEntries : filesPages.values())
        fileEntries.clear();
    }
  }

  @Override
  public void close() throws IOException {
    synchronized (syncObject) {
      flushBuffer();
      for (OMultiFileSegment multiFileSegment : files.values())
        multiFileSegment.synch();
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
    LRUEntry lruEntry = lruList.get(fileId, pageIndex);
    if (lruEntry != null) {
      assert dataPointer == ODirectMemory.NULL_POINTER || lruEntry.dataPointer == dataPointer;
      assert lruEntry.managedExternally == (dataPointer != ODirectMemory.NULL_POINTER);

      lruEntry = lruList.putToMRU(fileId, pageIndex, lruEntry.dataPointer, lruEntry.isDirty, lruEntry.managedExternally);

      return lruEntry;
    }

    Set<Long> pageEntries = filesPages.get(fileId);
    if (lruList.size() > maxSize) {
      LRUEntry removedLRU = lruList.getLRU();
      FileLockKey fileLockKey = new FileLockKey(removedLRU.fileId, removedLRU.pageIndex);
      lockManager.acquireLock(Thread.currentThread(), fileLockKey, OLockManager.LOCK.EXCLUSIVE);
      try {
        evictFileContent(removedLRU.fileId, removedLRU.pageIndex, removedLRU.dataPointer, removedLRU.isDirty,
            removedLRU.managedExternally);
        lruList.removeLRU();

        pageEntries.remove(removedLRU.pageIndex);
      } finally {
        lockManager.releaseLock(Thread.currentThread(), fileLockKey, OLockManager.LOCK.EXCLUSIVE);
      }
    }

    if (dataPointer == ODirectMemory.NULL_POINTER) {
      CacheResult cacheResult = cacheFileContent(fileId, pageIndex);
      lruEntry = lruList.putToMRU(fileId, pageIndex, cacheResult.dataPointer, cacheResult.isDirty, false);
    } else
      lruEntry = lruList.putToMRU(fileId, pageIndex, dataPointer, false, true);

    filesPages.get(fileId).add(pageIndex);

    return lruEntry;
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
        for (Map.Entry<FileLockKey, Long> entry : evictedPages.entrySet()) {
          long evictedDataPointer = entry.getValue();
          FileLockKey fileLockKey = entry.getKey();

          flushData(fileLockKey.fileId, fileLockKey.pageIndex, evictedDataPointer);

          directMemory.free(evictedDataPointer);
        }

        flushData(fileId, pageIndex, dataPointer);
        directMemory.free(dataPointer);

        evictedPages.clear();
      } else
        evictedPages.put(new FileLockKey(fileId, pageIndex), dataPointer);
    } else {
      directMemory.free(dataPointer);
    }

    filesPages.get(fileId).remove(pageIndex);
  }

  @Override
  public void flushData(final long fileId, final long pageIndex, final long dataPointer) throws IOException {
    final byte[] content = directMemory.get(dataPointer, pageSize);

    final OMultiFileSegment multiFileSegment = files.get(fileId);

    multiFileSegment.writeContinuously(pageIndex * pageSize, content);

    if (syncOnPageFlush)
      multiFileSegment.synch();
  }

  private static class CacheResult {
    private final boolean isDirty;
    private final long    dataPointer;

    private CacheResult(boolean dirty, long dataPointer) {
      isDirty = dirty;
      this.dataPointer = dataPointer;
    }
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
