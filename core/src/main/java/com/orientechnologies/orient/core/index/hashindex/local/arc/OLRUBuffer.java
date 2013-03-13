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
package com.orientechnologies.orient.core.index.hashindex.local.arc;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
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
public class OLRUBuffer {
  private final int                                 maxSize;
  private final int                                 pageSize;

  private final LRUList                             lruList;
  private final ODirectMemory                       directMemory;

  private final Map<String, OMultiFileSegment>      files;
  private final SortedMap<FileLockKey, Long>        evictedItems;
  private final Map<String, Set<Long>>              filesPages;

  private final OLockManager<FileLockKey, Runnable> lockManager;
  private final Object                              syncObject;
  private final OStorageLocal                       storageLocal;

  private final boolean                             syncOnPageFlush;

  public OLRUBuffer(long maxMemory, ODirectMemory directMemory, int pageSize, OStorageLocal storageLocal, boolean syncOnPageFlush) {
    this.directMemory = directMemory;
    this.pageSize = pageSize;
    this.storageLocal = storageLocal;
    this.syncOnPageFlush = syncOnPageFlush;
    this.files = new HashMap<String, OMultiFileSegment>();
    this.filesPages = new HashMap<String, Set<Long>>();

    this.evictedItems = new TreeMap<FileLockKey, Long>();

    this.lockManager = new OLockManager<FileLockKey, Runnable>(OGlobalConfiguration.ENVIRONMENT_CONCURRENT.getValueAsBoolean(),
        1000);

    long tmpMaxSize = maxMemory / pageSize;
    if (tmpMaxSize >= Integer.MAX_VALUE)
      maxSize = Integer.MAX_VALUE;
    else
      maxSize = (int) tmpMaxSize;

    lruList = new LRUList();

    syncObject = new Object();
  }

  public void openFile(OStorageSegmentConfiguration fileConfiguration, String fileExtension) throws IOException {
    final String fullName = fileConfiguration.name + fileExtension;
    synchronized (syncObject) {
      if (!files.containsKey(fullName)) {
        final OMultiFileSegment multiFileSegment = new OMultiFileSegment(storageLocal, fileConfiguration, fileExtension, pageSize);
        if (multiFileSegment.getFile(0).exists())
          multiFileSegment.open();
        else
          multiFileSegment.create(pageSize);

        files.put(fullName, multiFileSegment);

        filesPages.put(fullName, new HashSet<Long>());
      }
    }
  }

  public long loadAndLockForWrite(String fileName, String fileExtension, long pageIndex) throws IOException {
    synchronized (syncObject) {
      final String fullName = fileName + fileExtension;
      final LRUEntry lruEntry = updateCache(fullName, pageIndex, ODirectMemory.NULL_POINTER);

      lockManager.acquireLock(Thread.currentThread(), new FileLockKey(fullName, pageIndex), OLockManager.LOCK.EXCLUSIVE);
      lruEntry.isDirty = true;

      return lruEntry.dataPointer;
    }
  }

  public long allocateAndLockForWrite(String fileName, String fileExtension, long pageIndex) throws IOException {
    synchronized (syncObject) {
      final String fullName = fileName + fileExtension;

      final OMultiFileSegment multiFileSegment = files.get(fullName);

      final long startPosition = pageIndex * pageSize;
      final long endPosition = startPosition + pageSize;

      if (multiFileSegment.getFilledUpTo() < endPosition)
        multiFileSegment.allocateSpaceContinuously((int) (endPosition - multiFileSegment.getFilledUpTo()));

      long dataPointer = directMemory.allocate(new byte[pageSize]);
      updateCache(fullName, pageIndex, dataPointer);
      lockManager.acquireLock(Thread.currentThread(), new FileLockKey(fullName, pageIndex), OLockManager.LOCK.EXCLUSIVE);

      return dataPointer;
    }
  }

  public void cacheHit(String fileName, String fileExtension, long pageIndex, long dataPointer) throws IOException {
    final String fullName = fileName + fileExtension;
    synchronized (syncObject) {
      updateCache(fullName, pageIndex, dataPointer);
    }
  }

  public long getAndLockForWrite(String fileName, String fileExtension, long pageIndex) {
    final String fullName = fileName + fileExtension;
    synchronized (syncObject) {
      lockManager.acquireLock(Thread.currentThread(), new FileLockKey(fullName, pageIndex), OLockManager.LOCK.EXCLUSIVE);

      LRUEntry lruEntry = lruList.get(fullName, pageIndex);
      if (lruEntry != null)
        return lruEntry.dataPointer;

      return ODirectMemory.NULL_POINTER;
    }
  }

  public void clearExternalManagementFlag(String fileName, String fileExtension, long pageIndex) {
    final String fullName = fileName + fileExtension;
    synchronized (syncObject) {
      LRUEntry lruEntry = lruList.get(fullName, pageIndex);
      if (lruEntry != null)
        lruEntry.managedExternally = false;
    }
  }

  public long loadAndLockForRead(String fileName, String fileExtension, long pageIndex) throws IOException {
    synchronized (syncObject) {
      final String fullName = fileName + fileExtension;

      final LRUEntry lruEntry = updateCache(fullName, pageIndex, ODirectMemory.NULL_POINTER);

      lockManager.acquireLock(Thread.currentThread(), new FileLockKey(fullName, pageIndex), OLockManager.LOCK.SHARED);
      return lruEntry.dataPointer;
    }
  }

  public void releaseReadLock(String fileName, String fileExtension, long pageIndex) {
    final String fullName = fileName + fileExtension;
    lockManager.releaseLock(Thread.currentThread(), new FileLockKey(fullName, pageIndex), OLockManager.LOCK.SHARED);
  }

  public void releaseWriteLock(String fileName, String fileExtension, long pageIndex) {
    final String fullName = fileName + fileExtension;
    lockManager.releaseLock(Thread.currentThread(), new FileLockKey(fullName, pageIndex), OLockManager.LOCK.EXCLUSIVE);
  }

  public long getFilledUpTo(String fileName, String fileExtension) throws IOException {
    synchronized (syncObject) {
      final String fullName = fileName + fileExtension;
      return files.get(fullName).getFilledUpTo() / pageSize;
    }
  }

  public void flushFile(String fileName, String fileExtension) throws IOException {
    flushFile(fileName, fileExtension, false);
  }

  public void flushFile(String fileName, String fileExtension, boolean writeLock) throws IOException {
    final String fullName = fileName + fileExtension;
    OLockManager.LOCK lock = writeLock ? OLockManager.LOCK.EXCLUSIVE : OLockManager.LOCK.SHARED;

    synchronized (syncObject) {
      final Set<Long> pageIndexes = filesPages.get(fullName);
      for (Long pageIndex : pageIndexes) {
        lockManager.acquireLock(Thread.currentThread(), new FileLockKey(fullName, pageIndex), lock);
        try {
          LRUEntry lruEntry = lruList.get(fullName, pageIndex);
          if (lruEntry.isDirty) {
            flushData(fullName, lruEntry.pageIndex, lruEntry.dataPointer);
            lruEntry.isDirty = false;
          }

        } finally {
          lockManager.releaseLock(Thread.currentThread(), new FileLockKey(fullName, pageIndex), lock);
        }
      }

      files.get(fullName).synch();
    }
  }

  public void clearDirtyFlag(String fileName, String fileExtension, long pageIndex) {
    String fullName = fileName + fileExtension;
    Long dataPointer = evictedItems.remove(new FileLockKey(fullName, pageIndex));
    if (dataPointer != null) {
      directMemory.free(dataPointer);
      return;
    }

    LRUEntry lruEntry = lruList.get(fileName, pageIndex);
    if (lruEntry != null)
      lruEntry.isDirty = false;
  }

  public void closeFile(String fileName, String fileExtension) throws IOException, InterruptedException {
    final String fullName = fileName + fileExtension;
    synchronized (syncObject) {
      if (!files.containsKey(fullName))
        return;

      final Set<Long> pageIndexes = filesPages.get(fullName);
      for (Long pageIndex : pageIndexes) {
        lockManager.acquireLock(Thread.currentThread(), new FileLockKey(fullName, pageIndex), OLockManager.LOCK.EXCLUSIVE);
        try {
          LRUEntry lruEntry = lruList.remove(fullName, pageIndex);
          if (lruEntry != null && !lruEntry.managedExternally) {
            flushData(fullName, pageIndex, lruEntry.dataPointer);

            directMemory.free(lruEntry.dataPointer);
          }
        } finally {
          lockManager.releaseLock(Thread.currentThread(), new FileLockKey(fullName, pageIndex), OLockManager.LOCK.EXCLUSIVE);
        }
      }

      pageIndexes.clear();

      files.get(fullName).close();
    }
  }

  public void deleteFile(String fileName, String fileExtension) throws IOException, InterruptedException {
    final String fullName = fileName + fileExtension;
    synchronized (syncObject) {
      if (!files.containsKey(fullName))
        return;

      truncateFile(fileName, fileExtension);
      files.get(fullName).delete();

      files.remove(fullName);
      filesPages.remove(fullName);
    }
  }

  public void truncateFile(String fileName, String fileExtension) throws IOException, InterruptedException {
    final String fullName = fileName + fileExtension;
    synchronized (syncObject) {
      if (!files.containsKey(fullName))
        return;

      final Set<Long> pageEntries = filesPages.get(fullName);
      for (Long pageIndex : pageEntries) {
        lockManager.acquireLock(Thread.currentThread(), new FileLockKey(fullName, pageIndex), OLockManager.LOCK.EXCLUSIVE);
        try {
          LRUEntry lruEntry = lruList.remove(fullName, pageIndex);
          if (lruEntry != null && !lruEntry.managedExternally)
            directMemory.free(lruEntry.dataPointer);
        } finally {
          lockManager.releaseLock(Thread.currentThread(), new FileLockKey(fullName, pageIndex), OLockManager.LOCK.EXCLUSIVE);
        }
      }

      pageEntries.clear();
      files.get(fullName).truncate();
    }
  }

  public void flushBuffer() throws IOException {
    flushBuffer(false);
  }

  public void flushBuffer(boolean writeLock) throws IOException {
    OLockManager.LOCK lock = writeLock ? OLockManager.LOCK.EXCLUSIVE : OLockManager.LOCK.SHARED;

    synchronized (syncObject) {
      for (LRUEntry entry : lruList) {
        lockManager.acquireLock(Thread.currentThread(), new FileLockKey(entry.fileName, entry.pageIndex), lock);
        try {
          if (entry.isDirty) {
            flushData(entry.fileName, entry.pageIndex, entry.dataPointer);
            entry.isDirty = false;
          }
        } finally {
          lockManager.releaseLock(Thread.currentThread(), new FileLockKey(entry.fileName, entry.pageIndex), lock);
        }
      }
      for (OMultiFileSegment multiFileSegment : files.values())
        multiFileSegment.synch();
    }
  }

  public void clear() throws IOException {
    synchronized (syncObject) {
      flushBuffer(true);

      lruList.clear();
      for (Set<Long> fileEntries : filesPages.values())
        fileEntries.clear();
    }
  }

  public void close() throws IOException {
    synchronized (syncObject) {
      clear();
      for (OMultiFileSegment multiFileSegment : files.values())
        multiFileSegment.synch();
    }
  }

  private LRUEntry updateCache(final String fileName, long pageIndex, long dataPointer) throws IOException {
    LRUEntry lruEntry = lruList.get(fileName, pageIndex);
    if (lruEntry != null) {
      assert dataPointer == ODirectMemory.NULL_POINTER || lruEntry.dataPointer == dataPointer;
      assert lruEntry.managedExternally == (dataPointer != ODirectMemory.NULL_POINTER);

      lruEntry = lruList.putToMRU(fileName, pageIndex, lruEntry.dataPointer, lruEntry.isDirty, lruEntry.managedExternally);

      return lruEntry;
    }

    Set<Long> pageEntries = filesPages.get(fileName);
    if (lruList.size() > maxSize) {
      LRUEntry removedLRU = lruList.getLRU();
      FileLockKey fileLockKey = new FileLockKey(removedLRU.fileName, removedLRU.pageIndex);
      lockManager.acquireLock(Thread.currentThread(), fileLockKey, OLockManager.LOCK.EXCLUSIVE);
      try {
        evictFileContent(removedLRU.fileName, removedLRU.pageIndex, removedLRU.dataPointer, removedLRU.isDirty,
            removedLRU.managedExternally);
        lruList.removeLRU();

        pageEntries.remove(removedLRU.pageIndex);
      } finally {
        lockManager.releaseLock(Thread.currentThread(), fileLockKey, OLockManager.LOCK.EXCLUSIVE);
      }
    }

    if (dataPointer == ODirectMemory.NULL_POINTER) {
      CacheResult cacheResult = cacheFileContent(fileName, pageIndex);
      lruEntry = lruList.putToMRU(fileName, pageIndex, cacheResult.dataPointer, cacheResult.isDirty, false);
    } else
      lruEntry = lruList.putToMRU(fileName, pageIndex, dataPointer, false, true);

    filesPages.get(fileName).add(pageIndex);

    return lruEntry;
  }

  private CacheResult cacheFileContent(String fileName, long pageIndex) throws IOException {
    FileLockKey key = new FileLockKey(fileName, pageIndex);
    if (evictedItems.containsKey(key))
      return new CacheResult(true, evictedItems.remove(key));

    final OMultiFileSegment multiFileSegment = files.get(fileName);
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

  private void evictFileContent(String fileName, long pageIndex, long dataPointer, boolean isDirty, boolean mangedExternally)
      throws IOException {
    if (mangedExternally)
      return;

    if (isDirty) {
      if (evictedItems.size() >= 1500) {
        for (Map.Entry<FileLockKey, Long> entry : evictedItems.entrySet()) {
          long evictedDataPointer = entry.getValue();
          flushData(entry.getKey().fileName, entry.getKey().pageIndex, evictedDataPointer);

          directMemory.free(evictedDataPointer);
        }

        flushData(fileName, pageIndex, dataPointer);
        directMemory.free(dataPointer);

        evictedItems.clear();
      } else
        evictedItems.put(new FileLockKey(fileName, pageIndex), dataPointer);
    } else {
      directMemory.free(dataPointer);
    }

    filesPages.get(fileName).remove(pageIndex);
  }

  public void flushData(final String fileName, final String fileExtension, final long pageIndex, final long dataPointer)
      throws IOException {
    flushData(fileName + fileExtension, pageIndex, dataPointer);
  }

  private void flushData(final String fileName, final long pageIndex, final long dataPointer) throws IOException {
    final byte[] content = directMemory.get(dataPointer, pageSize);

    final OMultiFileSegment multiFileSegment = files.get(fileName);

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
    private final String fileName;
    private final long   pageIndex;

    private FileLockKey(String fileName, long pageIndex) {
      this.fileName = fileName;
      this.pageIndex = pageIndex;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o)
        return true;
      if (o == null || getClass() != o.getClass())
        return false;

      FileLockKey fileKey = (FileLockKey) o;

      if (pageIndex != fileKey.pageIndex)
        return false;
      if (!fileName.equals(fileKey.fileName))
        return false;

      return true;
    }

    @Override
    public int hashCode() {
      int result = fileName.hashCode();
      result = 31 * result + (int) (pageIndex ^ (pageIndex >>> 32));
      return result;
    }

    @Override
    public int compareTo(FileLockKey otherKey) {
      int result = fileName.compareTo(otherKey.fileName);
      if (result != 0)
        return result;

      if (pageIndex > otherKey.pageIndex)
        return 1;
      if (pageIndex < otherKey.pageIndex)
        return -1;

      return 0;
    }
  }
}
