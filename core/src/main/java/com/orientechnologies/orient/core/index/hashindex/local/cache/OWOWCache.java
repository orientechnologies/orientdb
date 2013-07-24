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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.CRC32;

import com.orientechnologies.common.concur.lock.OLockManager;
import com.orientechnologies.common.directmemory.ODirectMemory;
import com.orientechnologies.common.directmemory.ODirectMemoryFactory;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.exception.OAllCacheEntriesAreUsedException;
import com.orientechnologies.orient.core.storage.fs.OFileClassic;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWriteAheadLog;

/**
 * @author Andrey Lomakin
 * @since 7/23/13
 */
public class OWOWCache {
  public static final long                                   MAGIC_NUMBER   = 0xFACB03FEL;

  private final ConcurrentSkipListMap<GroupKey, OWriteGroup> writeGroups    = new ConcurrentSkipListMap<GroupKey, OWriteGroup>();

  private final ConcurrentMap<Long, OFileClassic>            files;
  private final ODirectMemory                                directMemory   = ODirectMemoryFactory.INSTANCE.directMemory();
  private final boolean                                      syncOnPageFlush;
  private final int                                          pageSize;
  private final long                                         groupTTL;
  private final OWriteAheadLog                               writeAheadLog;

  private final AtomicInteger                                cacheSize      = new AtomicInteger();
  private final OLockManager<GroupKey, Thread>               lockManager    = new OLockManager<GroupKey, Thread>(true, 1000);

  private volatile int                                       cacheMaxSize;

  private final ScheduledExecutorService                     commitExecutor = Executors
                                                                                .newSingleThreadScheduledExecutor(new ThreadFactory() {
                                                                                  @Override
                                                                                  public Thread newThread(Runnable r) {
                                                                                    Thread thread = new Thread(r);
                                                                                    thread.setDaemon(true);
                                                                                    thread.setName("Write Cache Flush Task");
                                                                                    return thread;
                                                                                  }
                                                                                });

  public OWOWCache(ConcurrentMap<Long, OFileClassic> files, boolean syncOnPageFlush, int pageSize, long groupTTL,
      OWriteAheadLog writeAheadLog, long pageFlushInterval, int cacheMaxSize) {
    this.files = files;
    this.syncOnPageFlush = syncOnPageFlush;
    this.pageSize = pageSize;
    this.groupTTL = groupTTL;
    this.writeAheadLog = writeAheadLog;
    this.cacheMaxSize = cacheMaxSize;

    if (pageFlushInterval > 0)
      commitExecutor.scheduleWithFixedDelay(new PeriodicFlushTask(), pageFlushInterval, pageFlushInterval, TimeUnit.MILLISECONDS);
  }

  public void add(OCacheEntry cacheEntry) {
    final GroupKey groupKey = new GroupKey(cacheEntry.fileId, cacheEntry.pageIndex / 16);

    lockManager.acquireLock(Thread.currentThread(), groupKey, OLockManager.LOCK.EXCLUSIVE);
    try {
      writeGroups.putIfAbsent(groupKey, new OWriteGroup(System.currentTimeMillis()));
      OWriteGroup writeGroup = writeGroups.get(groupKey);

      int entryIndex = (int) (cacheEntry.pageIndex % 16);
      assert writeGroup.cacheEntries[entryIndex] == null || writeGroup.cacheEntries[entryIndex] == cacheEntry;

      if (writeGroup.cacheEntries[entryIndex] == null) {
        cacheEntry.acquireExclusiveLock();
        try {
          cacheEntry.inWriteCache = true;
          writeGroup.cacheEntries[entryIndex] = cacheEntry;
        } finally {
          cacheEntry.releaseExclusiveLock();
        }

        cacheSize.incrementAndGet();
      } else {
        assert cacheEntry.inWriteCache;
      }

      writeGroup.recencyBit = true;
    } finally {
      lockManager.releaseLock(Thread.currentThread(), groupKey, OLockManager.LOCK.EXCLUSIVE);
    }

    if (cacheSize.get() > cacheMaxSize) {
      Future future = commitExecutor.submit(new PeriodicFlushTask());
      try {
        future.get();
      } catch (InterruptedException e) {
        Thread.interrupted();
        throw new OException("File flush was interrupted", e);
      } catch (Exception e) {
        throw new OException("File flush was abnormally terminated", e);
      }
    }
  }

  public OCacheEntry load(long fileId, long pageIndex) {
    final GroupKey groupKey = new GroupKey(fileId, pageIndex / 16);
    lockManager.acquireLock(Thread.currentThread(), groupKey, OLockManager.LOCK.SHARED);
    try {
      final OWriteGroup writeGroup = writeGroups.get(groupKey);

      if (writeGroup == null)
        return null;

      final int entryIndex = (int) (pageIndex % 16);
      OCacheEntry cacheEntry = writeGroup.cacheEntries[entryIndex];
      if (cacheEntry == null)
        return null;

      cacheEntry.acquireExclusiveLock();
      try {
        cacheEntry.usageCounter++;
      } finally {
        cacheEntry.releaseExclusiveLock();
      }

      return cacheEntry;
    } finally {
      lockManager.releaseLock(Thread.currentThread(), groupKey, OLockManager.LOCK.SHARED);
    }
  }

  public OCacheEntry get(long fileId, long pageIndex) {
    final GroupKey groupKey = new GroupKey(fileId, pageIndex / 16);
    lockManager.acquireLock(Thread.currentThread(), groupKey, OLockManager.LOCK.SHARED);
    try {
      final OWriteGroup writeGroup = writeGroups.get(groupKey);

      if (writeGroup == null)
        return null;

      final int entryIndex = (int) (pageIndex % 16);
      OCacheEntry cacheEntry = writeGroup.cacheEntries[entryIndex];
      if (cacheEntry == null)
        return null;

      return cacheEntry;
    } finally {
      lockManager.releaseLock(Thread.currentThread(), groupKey, OLockManager.LOCK.SHARED);
    }
  }

  public void flush(long fileId) {
    Future<Void> future = commitExecutor.submit(new FileFlushTask(fileId));
    try {
      future.get();
    } catch (InterruptedException e) {
      Thread.interrupted();
      throw new OException("File flush was interrupted", e);
    } catch (Exception e) {
      throw new OException("File flush was abnormally terminated", e);
    }
  }

  public void clear(long fileId) {
    Future<Void> future = commitExecutor.submit(new FileClearTask(fileId));
    try {
      future.get();
    } catch (InterruptedException e) {
      Thread.interrupted();
      throw new OException("File data removal was interrupted", e);
    } catch (Exception e) {
      throw new OException("File data removal was abnormally terminated", e);
    }
  }

  public void flush() {
    for (long fileId : files.keySet())
      flush(fileId);
  }

  public void clear() {
    for (long fileId : files.keySet())
      flush(fileId);
  }

  public void stopFlush() {
    if (!commitExecutor.isShutdown()) {
      commitExecutor.shutdown();
      try {
        if (!commitExecutor.awaitTermination(5, TimeUnit.MINUTES))
          throw new OException("Background data flush task can not be stopped.");
      } catch (InterruptedException e) {
        OLogManager.instance().error(this, "Data flush thread was interrupted");

        Thread.interrupted();
        throw new OException("Data flush thread was interrupted", e);
      }
    }
  }

  private void flushEntry(OCacheEntry entry) throws IOException {
    if (entry.usageCounter > 0)
      throw new OException("Cache entry is used and can not be deleted. (page index : " + entry.pageIndex + " file id : "
          + entry.fileId + ")");

    if (writeAheadLog != null) {
      OLogSequenceNumber lsn = getLogSequenceNumberFromPage(entry.dataPointer);
      OLogSequenceNumber flushedLSN = writeAheadLog.getFlushedLSN();
      if (flushedLSN == null || flushedLSN.compareTo(lsn) < 0)
        writeAheadLog.flush();
    }

    final byte[] content = directMemory.get(entry.dataPointer, pageSize);
    OLongSerializer.INSTANCE.serializeNative(MAGIC_NUMBER, content, 0);

    final int crc32 = calculatePageCrc(content);
    OIntegerSerializer.INSTANCE.serializeNative(crc32, content, OLongSerializer.LONG_SIZE);
    final OFileClassic fileClassic = files.get(entry.fileId);

    fileClassic.write(entry.pageIndex * pageSize, content);

    if (syncOnPageFlush)
      fileClassic.synch();
  }

  private static int calculatePageCrc(byte[] pageData) {
    int systemSize = OLongSerializer.LONG_SIZE + OIntegerSerializer.INT_SIZE;

    final CRC32 crc32 = new CRC32();
    crc32.update(pageData, systemSize, pageData.length - systemSize);

    return (int) crc32.getValue();
  }

  private OLogSequenceNumber getLogSequenceNumberFromPage(long dataPointer) {
    final long position = OLongSerializer.INSTANCE.deserializeFromDirectMemory(directMemory, dataPointer
        + OLongSerializer.LONG_SIZE + (2 * OIntegerSerializer.INT_SIZE));
    final int segment = OIntegerSerializer.INSTANCE.deserializeFromDirectMemory(directMemory, dataPointer
        + OLongSerializer.LONG_SIZE + OIntegerSerializer.INT_SIZE);

    return new OLogSequenceNumber(segment, position);
  }

  private final class GroupKey implements Comparable<GroupKey> {
    private final long fileId;
    private final long groupIndex;

    private GroupKey(long fileId, long groupIndex) {
      this.fileId = fileId;
      this.groupIndex = groupIndex;
    }

    @Override
    public int compareTo(GroupKey other) {
      if (fileId > other.fileId)
        return 1;
      if (fileId < other.fileId)
        return -1;

      if (groupIndex > other.groupIndex)
        return 1;
      if (groupIndex < other.groupIndex)
        return -1;

      return 0;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o)
        return true;
      if (o == null || getClass() != o.getClass())
        return false;

      GroupKey groupKey = (GroupKey) o;

      if (fileId != groupKey.fileId)
        return false;
      if (groupIndex != groupKey.groupIndex)
        return false;

      return true;
    }

    @Override
    public int hashCode() {
      int result = (int) (fileId ^ (fileId >>> 32));
      result = 31 * result + (int) (groupIndex ^ (groupIndex >>> 32));
      return result;
    }

    @Override
    public String toString() {
      return "GroupKey{" + "fileId=" + fileId + ", groupIndex=" + groupIndex + '}';
    }
  }

  private final class PeriodicFlushTask implements Runnable {
    private GroupKey lastGroupKey = new GroupKey(0, -1);

    @Override
    public void run() {
      try {
        if (writeGroups.isEmpty())
          return;

        int writeGroupsToFlush;

        double threshold = ((double) cacheSize.get()) / cacheMaxSize;
        if (threshold > 0.8)
          writeGroupsToFlush = (int) (0.2 * writeGroups.size());
        else if (threshold > 0.9)
          writeGroupsToFlush = (int) (0.4 * writeGroups.size());
        else
          writeGroupsToFlush = 1;

        int flushedGroups = 0;

        flushedGroups = flushRing(writeGroupsToFlush, flushedGroups, false);

        if (flushedGroups < writeGroupsToFlush)
          flushedGroups = flushRing(writeGroupsToFlush, flushedGroups, true);

        if (flushedGroups < writeGroupsToFlush) {
          if (OGlobalConfiguration.SERVER_CACHE_INCREASE_ON_DEMAND.getValueAsBoolean()) {
            final long oldCacheMaxSize = cacheMaxSize;

            cacheMaxSize = (int) Math.ceil(cacheMaxSize * (1 + OGlobalConfiguration.SERVER_CACHE_INCREASE_STEP.getValueAsFloat()));
            OLogManager.instance().warn(this, "Write cache size is increased from %d to %d", oldCacheMaxSize, cacheMaxSize);
          } else {
            throw new OAllCacheEntriesAreUsedException("All records in write cache are used!");
          }
        }
      } catch (Exception e) {
        OLogManager.instance().error(this, "Exception during data flush.", e);
      }
    }

    private int flushRing(int writeGroupsToFlush, int flushedGroups, boolean forceFlush) throws IOException {
      NavigableMap<GroupKey, OWriteGroup> subMap = writeGroups.tailMap(lastGroupKey, false);

      if (!subMap.isEmpty()) {
        flushedGroups = iterateBySubRing(subMap, writeGroupsToFlush, 0, forceFlush);
        if (flushedGroups < writeGroupsToFlush) {
          subMap = writeGroups.headMap(subMap.firstKey(), false);
          flushedGroups = iterateBySubRing(subMap, writeGroupsToFlush, flushedGroups, forceFlush);
        }
      } else
        flushedGroups = iterateBySubRing(writeGroups, writeGroupsToFlush, flushedGroups, forceFlush);

      return flushedGroups;
    }

    private int iterateBySubRing(NavigableMap<GroupKey, OWriteGroup> subMap, int writeGroupsToFlush, int flushedWriteGroups,
        boolean forceFlush) throws IOException {
      Iterator<Map.Entry<GroupKey, OWriteGroup>> entriesIterator = subMap.entrySet().iterator();
      long currentTime = System.currentTimeMillis();

      while (entriesIterator.hasNext() && flushedWriteGroups < writeGroupsToFlush) {
        Map.Entry<GroupKey, OWriteGroup> entry = entriesIterator.next();
        final OWriteGroup group = entry.getValue();
        lockManager.acquireLock(Thread.currentThread(), entry.getKey(), OLockManager.LOCK.EXCLUSIVE);
        try {
          if (group.recencyBit && group.creationTime - currentTime < groupTTL || forceFlush)
            group.recencyBit = false;
          else {
            boolean canBeFlushed = true;
            final List<OCacheEntry> lockedEntries = new ArrayList<OCacheEntry>();

            for (final OCacheEntry cacheEntry : group.cacheEntries) {
              cacheEntry.acquireExclusiveLock();
              lockedEntries.add(cacheEntry);
            }

            try {
              for (final OCacheEntry cacheEntry : group.cacheEntries) {
                if (cacheEntry != null && cacheEntry.usageCounter > 0) {
                  canBeFlushed = false;
                  break;
                }
              }

              if (canBeFlushed) {
                for (final OCacheEntry cacheEntry : group.cacheEntries)
                  flushEntry(cacheEntry);

                for (final OCacheEntry cacheEntry : group.cacheEntries) {
                  cacheEntry.inWriteCache = false;
                  cacheSize.decrementAndGet();

                  if (!cacheEntry.inReadCache)
                    directMemory.free(cacheEntry.dataPointer);
                }

                lastGroupKey = entry.getKey();
                entriesIterator.remove();

                flushedWriteGroups++;
              } else
                group.recencyBit = false;
            } finally {
              for (OCacheEntry cacheEntry : lockedEntries)
                cacheEntry.releaseExclusiveLock();
            }
          }
        } finally {
          lockManager.releaseLock(Thread.currentThread(), entry.getKey(), OLockManager.LOCK.EXCLUSIVE);
        }

      }

      return flushedWriteGroups;
    }
  }

  private final class FileFlushTask implements Callable<Void> {
    private final long fileId;

    private FileFlushTask(long fileId) {
      this.fileId = fileId;
    }

    @Override
    public Void call() throws Exception {
      final GroupKey firstKey = new GroupKey(fileId, 0);
      final GroupKey lastKey = new GroupKey(fileId, Long.MAX_VALUE);

      NavigableMap<GroupKey, OWriteGroup> subMap = writeGroups.subMap(firstKey, true, lastKey, true);
      Iterator<Map.Entry<GroupKey, OWriteGroup>> entryIterator = subMap.entrySet().iterator();

      while (entryIterator.hasNext()) {
        Map.Entry<GroupKey, OWriteGroup> entry = entryIterator.next();
        lockManager.acquireLock(Thread.currentThread(), entry.getKey(), OLockManager.LOCK.EXCLUSIVE);
        try {
          OWriteGroup writeGroup = entry.getValue();
          List<OCacheEntry> lockedEntries = new ArrayList<OCacheEntry>();

          for (OCacheEntry cacheEntry : writeGroup.cacheEntries) {
            cacheEntry.acquireExclusiveLock();
            lockedEntries.add(cacheEntry);
          }

          try {
            for (OCacheEntry cacheEntry : writeGroup.cacheEntries)
              flushEntry(cacheEntry);

            for (OCacheEntry cacheEntry : writeGroup.cacheEntries) {
              cacheEntry.inWriteCache = false;
              cacheSize.decrementAndGet();

              if (!cacheEntry.inReadCache)
                directMemory.free(cacheEntry.dataPointer);
            }
          } finally {
            for (OCacheEntry cacheEntry : lockedEntries)
              cacheEntry.releaseExclusiveLock();
          }

          entryIterator.remove();
        } finally {
          lockManager.releaseLock(Thread.currentThread(), entry.getKey(), OLockManager.LOCK.EXCLUSIVE);
        }

      }

      files.get(fileId).synch();
      return null;
    }
  }

  private final class FileClearTask implements Callable<Void> {
    private final long fileId;

    private FileClearTask(long fileId) {
      this.fileId = fileId;
    }

    @Override
    public Void call() throws Exception {
      final GroupKey firstKey = new GroupKey(fileId, 0);
      final GroupKey lastKey = new GroupKey(fileId, Long.MAX_VALUE);

      NavigableMap<GroupKey, OWriteGroup> subMap = writeGroups.subMap(firstKey, true, lastKey, true);
      Iterator<Map.Entry<GroupKey, OWriteGroup>> entryIterator = subMap.entrySet().iterator();

      while (entryIterator.hasNext()) {
        Map.Entry<GroupKey, OWriteGroup> entry = entryIterator.next();
        lockManager.acquireLock(Thread.currentThread(), entry.getKey(), OLockManager.LOCK.EXCLUSIVE);
        try {
          OWriteGroup writeGroup = entry.getValue();
          for (OCacheEntry cacheEntry : writeGroup.cacheEntries) {
            if (cacheEntry.usageCounter > 0)
              throw new OException("Cache entry is used and can not be deleted. (page index : " + cacheEntry.pageIndex
                  + " file id : " + fileId + ")");

            cacheEntry.inWriteCache = false;
            cacheSize.decrementAndGet();

            if (!cacheEntry.inReadCache)
              directMemory.free(cacheEntry.dataPointer);
          }

          entryIterator.remove();
        } finally {
          lockManager.releaseLock(Thread.currentThread(), entry.getKey(), OLockManager.LOCK.EXCLUSIVE);
        }
      }

      return null;
    }
  }
}
