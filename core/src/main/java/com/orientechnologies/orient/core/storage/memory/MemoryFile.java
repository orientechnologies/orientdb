package com.orientechnologies.orient.core.storage.memory;

import com.orientechnologies.common.directmemory.OByteBufferPool;
import com.orientechnologies.common.directmemory.ODirectMemoryAllocator.Intention;
import com.orientechnologies.common.directmemory.OPointer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cache.OCacheEntryImpl;
import com.orientechnologies.orient.core.storage.cache.OCachePointer;
import com.orientechnologies.orient.core.storage.cache.OReadCache;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public final class MemoryFile {
  private final int id;
  private final int storageId;

  private final ReadWriteLock clearLock = new ReentrantReadWriteLock();

  private final ConcurrentSkipListMap<Long, OCacheEntry> content = new ConcurrentSkipListMap<>();

  public MemoryFile(final int storageId, final int id) {
    this.storageId = storageId;
    this.id = id;
  }

  public OCacheEntry loadPage(final long index) {
    clearLock.readLock().lock();
    try {
      return content.get(index);
    } finally {
      clearLock.readLock().unlock();
    }
  }

  public OCacheEntry addNewPage(OReadCache readCache) {
    clearLock.readLock().lock();
    try {
      OCacheEntry cacheEntry;

      long index;
      do {
        if (content.isEmpty()) {
          index = 0;
        } else {
          final long lastIndex = content.lastKey();
          index = lastIndex + 1;
        }

        final OByteBufferPool bufferPool = OByteBufferPool.instance(null);
        final OPointer pointer =
            bufferPool.acquireDirect(true, Intention.ADD_NEW_PAGE_IN_MEMORY_STORAGE);

        final OCachePointer cachePointer = new OCachePointer(pointer, bufferPool, id, (int) index);
        cachePointer.incrementReferrer();

        cacheEntry =
            new OCacheEntryImpl(
                ODirectMemoryOnlyDiskCache.composeFileId(storageId, id),
                (int) index,
                cachePointer,
                true,
                readCache);

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

  public long size() {
    clearLock.readLock().lock();
    try {
      if (content.isEmpty()) {
        return 0;
      }

      try {
        return content.lastKey() + 1;
      } catch (final NoSuchElementException ignore) {
        return 0;
      }

    } finally {
      clearLock.readLock().unlock();
    }
  }

  public long getUsedMemory() {
    return content.size();
  }

  public void clear() {
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

    if (thereAreNotReleased) {
      throw new IllegalStateException(
          "Some cache entries were not released. Storage may be in invalid state.");
    }
  }
}
