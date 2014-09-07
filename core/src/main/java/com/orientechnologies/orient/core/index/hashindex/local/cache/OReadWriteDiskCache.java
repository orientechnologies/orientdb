package com.orientechnologies.orient.core.index.hashindex.local.cache;

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
import com.orientechnologies.orient.core.storage.impl.local.OStorageLocalAbstract;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.ODirtyPage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWriteAheadLog;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Future;

/**
 * @author Andrey Lomakin
 * @since 7/24/13
 */
public class OReadWriteDiskCache implements ODiskCache {
  public static final int                             MIN_CACHE_SIZE = 256;

  private int                                         maxSize;
  private int                                         K_IN;
  private int                                         K_OUT;
  private LRUList                                     am;
  private LRUList                                     a1out;
  private LRUList                                     a1in;

  private final OWOWCache                             writeCache;
  private final int                                   pageSize;

  /**
   * Contains all pages in cache for given file.
   */
  private final Map<Long, Set<Long>>                  filePages;

  private final Object                                syncObject;

  private final NavigableMap<PinnedPage, OCacheEntry> pinnedPages    = new TreeMap<PinnedPage, OCacheEntry>();

  private final String                                storageName;

  private static String                               METRIC_HITS;
  private static String                               METRIC_HITS_METADATA;
  private static String                               METRIC_MISSED;
  private static String                               METRIC_MISSED_METADATA;

  public OReadWriteDiskCache(final long readCacheMaxMemory, final long writeCacheMaxMemory, final int pageSize,
      final long writeGroupTTL, final int pageFlushInterval, final OStorageLocalAbstract storageLocal,
      final OWriteAheadLog writeAheadLog, final boolean syncOnPageFlush, final boolean checkMinSize) {
    this(null, readCacheMaxMemory, writeCacheMaxMemory, pageSize, writeGroupTTL, pageFlushInterval, storageLocal, writeAheadLog,
        syncOnPageFlush, checkMinSize);
  }

  public OReadWriteDiskCache(final String storageName, final long readCacheMaxMemory, final long writeCacheMaxMemory,
      final int pageSize, final long writeGroupTTL, final int pageFlushInterval, final OStorageLocalAbstract storageLocal,
      final OWriteAheadLog writeAheadLog, final boolean syncOnPageFlush, final boolean checkMinSize) {
    this.storageName = storageName;
    this.pageSize = pageSize;

    initProfiler();

    this.filePages = new HashMap<Long, Set<Long>>();

    maxSize = normalizeMemory(readCacheMaxMemory, pageSize);
    if (checkMinSize && maxSize < MIN_CACHE_SIZE)
      maxSize = MIN_CACHE_SIZE;

    this.writeCache = new OWOWCache(syncOnPageFlush, pageSize, writeGroupTTL, writeAheadLog, pageFlushInterval, normalizeMemory(
        writeCacheMaxMemory, pageSize), storageLocal, checkMinSize);

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

  @Override
  public long openFile(final String fileName) throws IOException {
    synchronized (syncObject) {
      long fileId = writeCache.isOpen(fileName);
      if (fileId >= 0)
        return fileId;

      fileId = writeCache.openFile(fileName);
      filePages.put(fileId, new HashSet<Long>());

      return fileId;
    }
  }

  @Override
  public void openFile(final long fileId) throws IOException {
    synchronized (syncObject) {
      if (writeCache.isOpen(fileId))
        return;

      writeCache.openFile(fileId);
      filePages.put(fileId, new HashSet<Long>());
    }
  }

  @Override
  public void openFile(String fileName, long fileId) throws IOException {
    synchronized (syncObject) {
      long existingFileId = writeCache.isOpen(fileName);

      if (fileId == existingFileId)
        return;
      else if (existingFileId >= 0)
        throw new OStorageException("File with given name already exists but has different id " + existingFileId + " vs. proposed "
            + fileId);

      writeCache.openFile(fileName, fileId);
      filePages.put(fileId, new HashSet<Long>());
    }
  }

  @Override
  public boolean exists(final String fileName) {
    synchronized (syncObject) {
      return writeCache.exists(fileName);
    }
  }

  @Override
  public boolean exists(long fileId) {
    synchronized (syncObject) {
      return writeCache.exists(fileId);
    }
  }

  @Override
  public String fileNameById(long fileId) {
    synchronized (syncObject) {
      return writeCache.fileNameById(fileId);
    }
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
    synchronized (syncObject) {
      remove(cacheEntry.fileId, cacheEntry.pageIndex);
      pinnedPages.put(new PinnedPage(cacheEntry.fileId, cacheEntry.pageIndex), cacheEntry);
    }
  }

  @Override
  public void loadPinnedPage(final OCacheEntry cacheEntry) throws IOException {
    synchronized (syncObject) {
      cacheEntry.usagesCount++;
    }
  }

  @Override
  public OCacheEntry load(final long fileId, final long pageIndex, final boolean checkPinnedPages) throws IOException {
    synchronized (syncObject) {
      OCacheEntry cacheEntry = null;
      if (checkPinnedPages)
        cacheEntry = pinnedPages.get(new PinnedPage(fileId, pageIndex));

      if (cacheEntry == null)
        cacheEntry = updateCache(fileId, pageIndex);

      cacheEntry.usagesCount++;
      return cacheEntry;
    }
  }

  @Override
  public OCacheEntry allocateNewPage(final long fileId) throws IOException {
    synchronized (syncObject) {
      final long filledUpTo = getFilledUpTo(fileId);
      return load(fileId, filledUpTo, false);
    }
  }

  @Override
  public void release(OCacheEntry cacheEntry) {
    Future<?> flushFuture = null;
    synchronized (syncObject) {
      if (cacheEntry != null)
        cacheEntry.usagesCount--;
      else
        throw new IllegalStateException("record should be released is already free!");

      if (cacheEntry.usagesCount == 0 && cacheEntry.isDirty) {
        flushFuture = writeCache.store(cacheEntry.fileId, cacheEntry.pageIndex, cacheEntry.dataPointer);
        cacheEntry.isDirty = false;
      }
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
    synchronized (syncObject) {
      return writeCache.getFilledUpTo(fileId);
    }
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
    synchronized (syncObject) {
      writeCache.close(fileId, flush);

      final Set<Long> pageIndexes = filePages.get(fileId);

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

      pageIndexes.clear();
    }
  }

  @Override
  public void deleteFile(long fileId) throws IOException {
    synchronized (syncObject) {
      if (isOpen(fileId))
        truncateFile(fileId);

      writeCache.deleteFile(fileId);
      filePages.remove(fileId);
    }
  }

  @Override
  public void truncateFile(long fileId) throws IOException {
    synchronized (syncObject) {
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
    }
  }

  @Override
  public void renameFile(long fileId, String oldFileName, String newFileName) throws IOException {
    synchronized (syncObject) {
      writeCache.renameFile(fileId, oldFileName, newFileName);
    }
  }

  @Override
  public void flushBuffer() throws IOException {
    writeCache.flush();
  }

  @Override
  public void clear() throws IOException {
    writeCache.flush();

    synchronized (syncObject) {
      clearCacheContent();
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
    synchronized (syncObject) {
      clear();
      writeCache.close();
    }
  }

  @Override
  public boolean wasSoftlyClosed(long fileId) throws IOException {
    synchronized (syncObject) {
      return writeCache.wasSoftlyClosed(fileId);
    }
  }

  @Override
  public void setSoftlyClosed(long fileId, boolean softlyClosed) throws IOException {
    synchronized (syncObject) {
      writeCache.setSoftlyClosed(fileId, softlyClosed);
    }
  }

  @Override
  public void setSoftlyClosed(boolean softlyClosed) throws IOException {
    synchronized (syncObject) {
      writeCache.setSoftlyClosed(softlyClosed);
    }
  }

  @Override
  public boolean isOpen(long fileId) {
    synchronized (syncObject) {
      return writeCache.isOpen(fileId);
    }
  }

  private OCacheEntry updateCache(final long fileId, final long pageIndex) throws IOException {
    final OProfilerMBean profiler = storageName != null ? Orient.instance().getProfiler() : null;
    final long startTime = storageName != null ? System.currentTimeMillis() : 0;

    OCacheEntry cacheEntry = am.get(fileId, pageIndex);

    if (cacheEntry != null) {
      am.putToMRU(cacheEntry);

      if (profiler != null && profiler.isRecording())
        profiler.stopChrono(METRIC_HITS, "Requested item was found in Disk Cache", startTime, METRIC_HITS_METADATA);

      return cacheEntry;
    }

    if (profiler != null && profiler.isRecording())
      profiler.stopChrono(METRIC_MISSED, "Requested item was not found in Disk Cache", startTime, METRIC_MISSED_METADATA);

    cacheEntry = a1out.remove(fileId, pageIndex);
    if (cacheEntry != null) {
      removeColdestPageIfNeeded();

      OCachePointer dataPointer = writeCache.load(fileId, pageIndex);
      assert cacheEntry.dataPointer == null;
      assert !cacheEntry.isDirty;

      cacheEntry.dataPointer = dataPointer;

      am.putToMRU(cacheEntry);

      return cacheEntry;
    }

    cacheEntry = a1in.get(fileId, pageIndex);
    if (cacheEntry != null)
      return cacheEntry;

    removeColdestPageIfNeeded();

    OCachePointer dataPointer = writeCache.load(fileId, pageIndex);

    cacheEntry = new OCacheEntry(fileId, pageIndex, dataPointer, false);
    a1in.putToMRU(cacheEntry);

    Set<Long> pages = filePages.get(fileId);
    if (pages == null) {
      pages = new HashSet<Long>();
      filePages.put(fileId, pages);
    }

    pages.add(pageIndex);
    return cacheEntry;
  }

  private void removeColdestPageIfNeeded() throws IOException {
    if (am.size() + a1in.size() >= maxSize) {
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

        if (a1out.size() > K_OUT) {
          OCacheEntry removedEntry = a1out.removeLRU();
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
    synchronized (syncObject) {
      return writeCache.checkStoredPages(commandOutputListener);
    }
  }

  @Override
  public Set<ODirtyPage> logDirtyPagesTable() throws IOException {
    synchronized (syncObject) {
      return writeCache.logDirtyPagesTable();
    }
  }

  @Override
  public void delete() throws IOException {
    synchronized (syncObject) {
      writeCache.delete();

      clearCacheContent();
    }
  }

  int getMaxSize() {
    return maxSize;
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
    if (cacheEntry != null) {
      return cacheEntry;
    }
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

  public void initProfiler() {
    if (storageName != null) {
      final OProfilerMBean profiler = Orient.instance().getProfiler();

      METRIC_HITS = profiler.getDatabaseMetric(storageName, "diskCache.hits");
      METRIC_HITS_METADATA = profiler.getDatabaseMetric(null, "diskCache.hits");
      METRIC_MISSED = profiler.getDatabaseMetric(storageName, "diskCache.missed");
      METRIC_MISSED_METADATA = profiler.getDatabaseMetric(null, "diskCache.missed");

      profiler.registerHookValue(profiler.getDatabaseMetric(storageName, "diskCache.totalMemory"),
          "Total memory used by Disk Cache", METRIC_TYPE.SIZE, new OProfilerHookValue() {
            @Override
            public Object getValue() {
              return (am.size() + a1in.size()) * pageSize;
            }
          }, profiler.getDatabaseMetric(null, "diskCache.totalMemory"));

      profiler.registerHookValue(profiler.getDatabaseMetric(storageName, "diskCache.maxMemory"),
          "Maximum memory used by Disk Cache", METRIC_TYPE.SIZE, new OProfilerHookValue() {
            @Override
            public Object getValue() {
              return maxSize * pageSize;
            }
          }, profiler.getDatabaseMetric(null, "diskCache.maxMemory"));
    }
  }
}
