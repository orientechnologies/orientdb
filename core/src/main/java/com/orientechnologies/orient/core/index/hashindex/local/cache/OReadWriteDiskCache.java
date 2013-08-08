package com.orientechnologies.orient.core.index.hashindex.local.cache;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.exception.OAllCacheEntriesAreUsedException;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.storage.impl.local.OStorageLocalAbstract;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.ODirtyPage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWriteAheadLog;

/**
 * @author Andrey Lomakin
 * @since 7/24/13
 */
public class OReadWriteDiskCache implements ODiskCache {
  public static final int            MIN_CACHE_SIZE = 256;

  private int                        maxSize;
  private int                        K_IN;
  private int                        K_OUT;
  private LRUList                    am;
  private LRUList                    a1out;
  private LRUList                    a1in;

  private final OWOWCache            writeCache;

  /**
   * Contains all pages in cache for given file.
   */
  private final Map<Long, Set<Long>> filePages;

  private final Object               syncObject;

  public OReadWriteDiskCache(long readCacheMaxMemory, long writeCacheMaxMemory, int pageSize, long writeGroupTTL,
      int pageFlushInterval, OStorageLocalAbstract storageLocal, OWriteAheadLog writeAheadLog, boolean syncOnPageFlush,
      boolean checkMinSize) {
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
  public long openFile(String fileName) throws IOException {
    synchronized (syncObject) {
      final long fileId = writeCache.openFile(fileName);
      filePages.put(fileId, new HashSet<Long>());

      return fileId;
    }
  }

  @Override
  public void markDirty(long fileId, long pageIndex) {
    synchronized (syncObject) {
      OReadCacheEntry cacheEntry = a1in.get(fileId, pageIndex);

      if (cacheEntry != null) {
        cacheEntry.isDirty = true;
        return;
      }

      cacheEntry = am.get(fileId, pageIndex);
      if (cacheEntry != null) {
        cacheEntry.isDirty = true;
      } else
        throw new IllegalStateException("Requested page number " + pageIndex + " is not in cache");
    }
  }

  @Override
  public OCachePointer load(long fileId, long pageIndex) throws IOException {
    synchronized (syncObject) {
      final OReadCacheEntry cacheEntry = updateCache(fileId, pageIndex);
      cacheEntry.usagesCount++;
      return cacheEntry.dataPointer;
    }
  }

  @Override
  public void release(long fileId, long pageIndex) {
    synchronized (syncObject) {
      OReadCacheEntry cacheEntry = get(fileId, pageIndex, false);
      if (cacheEntry != null)
        cacheEntry.usagesCount--;
      else
        throw new IllegalStateException("record should be released is already free!");

      if (cacheEntry.usagesCount == 0 && cacheEntry.isDirty) {
        writeCache.store(fileId, pageIndex, cacheEntry.dataPointer);
        cacheEntry.isDirty = false;
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
    synchronized (syncObject) {
      writeCache.flush(fileId);
    }
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
        OReadCacheEntry cacheEntry = get(fileId, pageIndex, true);
        if (cacheEntry != null) {
          if (cacheEntry.dataPointer != null) {
            if (cacheEntry.usagesCount == 0)
              cacheEntry = remove(fileId, pageIndex);
            else
              throw new OStorageException("Page with index " + pageIndex + " for file with id " + fileId
                  + "can not be freed because it is used.");

            cacheEntry.dataPointer.decrementReferrer();
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
        OReadCacheEntry cacheEntry = get(fileId, pageIndex, true);
        if (cacheEntry != null) {
          if (cacheEntry.usagesCount == 0) {
            cacheEntry = remove(fileId, pageIndex);
            if (cacheEntry.dataPointer != null)
              cacheEntry.dataPointer.decrementReferrer();
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
    synchronized (syncObject) {
      writeCache.flush();
    }
  }

  @Override
  public void clear() throws IOException {
    synchronized (syncObject) {
      writeCache.flush();

      for (OReadCacheEntry cacheEntry : am)
        if (cacheEntry.usagesCount == 0)
          cacheEntry.dataPointer.decrementReferrer();
        else
          throw new OStorageException("Page with index " + cacheEntry.pageIndex + " for file id " + cacheEntry.fileId
              + " is used and can not be removed");

      for (OReadCacheEntry cacheEntry : a1in)
        if (cacheEntry.usagesCount == 0)
          cacheEntry.dataPointer.decrementReferrer();
        else
          throw new OStorageException("Page with index " + cacheEntry.pageIndex + " for file id " + cacheEntry.fileId
              + " is used and can not be removed");

      a1out.clear();
      am.clear();
      a1in.clear();
    }
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
  public boolean isOpen(long fileId) {
    synchronized (syncObject) {
      return writeCache.isOpen(fileId);
    }
  }

  private OReadCacheEntry updateCache(long fileId, long pageIndex) throws IOException {
    OReadCacheEntry cacheEntry = am.get(fileId, pageIndex);

    if (cacheEntry != null) {
      am.putToMRU(cacheEntry);

      return cacheEntry;
    }

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

    cacheEntry = new OReadCacheEntry(fileId, pageIndex, dataPointer, false);
    a1in.putToMRU(cacheEntry);

    filePages.get(fileId).add(pageIndex);
    return cacheEntry;
  }

  private void removeColdestPageIfNeeded() throws IOException {
    if (am.size() + a1in.size() >= maxSize) {
      if (a1in.size() > K_IN) {
        OReadCacheEntry removedFromAInEntry = a1in.removeLRU();

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
          OReadCacheEntry removedEntry = a1out.removeLRU();
          assert removedEntry.dataPointer == null;
          assert !removedEntry.isDirty;

          Set<Long> pageEntries = filePages.get(removedEntry.fileId);
          pageEntries.remove(removedEntry.pageIndex);
        }
      } else {
        OReadCacheEntry removedEntry = am.removeLRU();

        if (removedEntry == null) {
          increaseCacheSize();
        } else {
          assert removedEntry.usagesCount == 0;
          assert !removedEntry.isDirty;

          removedEntry.dataPointer.decrementReferrer();

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
  public void forceSyncStoredChanges() throws IOException {
    synchronized (syncObject) {
      writeCache.forceSyncStoredChanges();
    }
  }

  int getMaxSize() {
    return maxSize;
  }

  private OReadCacheEntry get(long fileId, long pageIndex, boolean useOutQueue) {
    OReadCacheEntry cacheEntry = am.get(fileId, pageIndex);

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

  private OReadCacheEntry remove(long fileId, long pageIndex) {
    OReadCacheEntry cacheEntry = am.remove(fileId, pageIndex);
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
    long tmpMaxSize = maxSize / pageSize;
    if (tmpMaxSize >= Integer.MAX_VALUE) {
      return Integer.MAX_VALUE;
    } else {
      return (int) tmpMaxSize;
    }
  }
}
