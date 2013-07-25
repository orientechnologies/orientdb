package com.orientechnologies.orient.core.index.hashindex.local.cache;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.orientechnologies.common.directmemory.ODirectMemory;
import com.orientechnologies.common.directmemory.ODirectMemoryFactory;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.exception.OAllCacheEntriesAreUsedException;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.storage.impl.local.OStorageLocalAbstract;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OLocalPage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.ODirtyPage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWriteAheadLog;

/**
 * @author Andrey Lomakin
 * @since 7/24/13
 */
public class OReadWriteDiskCache implements ODiskCache {
  private int                        maxSize;
  private int                        K_IN;
  private int                        K_OUT;
  private LRUList                    am;
  private LRUList                    a1out;
  private LRUList                    a1in;

  private final ODirectMemory        directMemory = ODirectMemoryFactory.INSTANCE.directMemory();

  private final OWOWCache            writeCache;

  /**
   * Contains all pages in cache for given file.
   */
  private final Map<Long, Set<Long>> filePages;

  private final Object               syncObject;

  public OReadWriteDiskCache(long readCacheMaxMemory, long writeCacheMaxMemory, int pageSize, int writeGroupTTL,
      int pageFlushInterval, OStorageLocalAbstract storageLocal, OWriteAheadLog writeAheadLog, boolean syncOnPageFlush) {
    this.filePages = new HashMap<Long, Set<Long>>();

    maxSize = normalizeMemory(readCacheMaxMemory, pageSize);

    this.writeCache = new OWOWCache(syncOnPageFlush, pageSize, writeGroupTTL, writeAheadLog, pageFlushInterval, normalizeMemory(
        writeCacheMaxMemory, pageSize), storageLocal);

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
      OCacheEntry cacheEntry = a1in.get(fileId, pageIndex);

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
  public long load(long fileId, long pageIndex) throws IOException {
    synchronized (syncObject) {
      final OCacheEntry cacheEntry = updateCache(fileId, pageIndex);
      cacheEntry.usageCounter++;
      return cacheEntry.dataPointer;
    }
  }

  @Override
  public void release(long fileId, long pageIndex) {
    synchronized (syncObject) {
      OCacheEntry cacheEntry = get(fileId, pageIndex, false);
      if (cacheEntry != null)
        cacheEntry.usageCounter--;
      else
        throw new IllegalStateException("record should be released is already free!");
      if (cacheEntry.usageCounter == 0 && cacheEntry.isDirty) {
        writeCache.put(fileId, pageIndex, cacheEntry.dataPointer);
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
        OCacheEntry cacheEntry = get(fileId, pageIndex, true);
        if (cacheEntry != null) {
          if (cacheEntry.usageCounter == 0) {
            cacheEntry = remove(fileId, pageIndex);

            if (cacheEntry.dataPointer != ODirectMemory.NULL_POINTER)
              directMemory.free(cacheEntry.dataPointer);

          } else
            throw new OStorageException("Page with index " + pageIndex + " for file with id " + fileId
                + "can not be freed because it is used.");

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
        if (cacheEntry != null) {
          if (cacheEntry.usageCounter == 0) {
            cacheEntry = remove(fileId, pageIndex);
            if (cacheEntry.dataPointer != ODirectMemory.NULL_POINTER)
              directMemory.free(cacheEntry.dataPointer);
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

      for (OCacheEntry cacheEntry : am)
        if (cacheEntry.usageCounter == 0)
          directMemory.free(cacheEntry.dataPointer);
        else
          throw new OStorageException("Page with index " + cacheEntry.pageIndex + " for file id " + cacheEntry.fileId
              + " is used and can not be removed");

      for (OCacheEntry cacheEntry : a1in)
        if (cacheEntry.usageCounter == 0)
          directMemory.free(cacheEntry.dataPointer);
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

  private OCacheEntry updateCache(long fileId, long pageIndex) throws IOException {
    OCacheEntry cacheEntry = am.get(fileId, pageIndex);

    if (cacheEntry != null) {
      am.putToMRU(cacheEntry);

      return cacheEntry;
    }

    cacheEntry = a1out.remove(fileId, pageIndex);
    if (cacheEntry != null) {
      removeColdestPageIfNeeded();

      long dataPointer = writeCache.get(fileId, pageIndex);

      assert cacheEntry.usageCounter == 0;
      assert !cacheEntry.isDirty;

      cacheEntry.dataPointer = dataPointer;
      cacheEntry.loadedLSN = OLocalPage.getLogSequenceNumberFromPage(directMemory, dataPointer);

      am.putToMRU(cacheEntry);

      return cacheEntry;
    }

    cacheEntry = a1in.get(fileId, pageIndex);
    if (cacheEntry != null)
      return cacheEntry;

    removeColdestPageIfNeeded();

    long dataPointer = writeCache.get(fileId, pageIndex);
    OLogSequenceNumber lsn = OLocalPage.getLogSequenceNumberFromPage(directMemory, dataPointer);

    cacheEntry = new OCacheEntry(fileId, pageIndex, dataPointer, false, lsn);
    a1in.putToMRU(cacheEntry);

    filePages.get(fileId).add(pageIndex);
    return cacheEntry;
  }

  private void removeColdestPageIfNeeded() throws IOException {
    if (am.size() + a1in.size() >= maxSize) {
      if (a1in.size() > K_IN) {
        OCacheEntry removedFromAInEntry = a1in.removeLRU();

        if (removedFromAInEntry == null) {
          increaseCacheSize();
        } else {
          assert removedFromAInEntry.usageCounter == 0;
          assert !removedFromAInEntry.isDirty;

          directMemory.free(removedFromAInEntry.dataPointer);

          removedFromAInEntry.dataPointer = ODirectMemory.NULL_POINTER;
          removedFromAInEntry.loadedLSN = null;

          a1out.putToMRU(removedFromAInEntry);
        }

        if (a1out.size() > K_OUT) {
          OCacheEntry removedEntry = a1out.removeLRU();
          assert removedEntry.usageCounter == 0;
          assert !removedEntry.isDirty;

          Set<Long> pageEntries = filePages.get(removedEntry.fileId);
          pageEntries.remove(removedEntry.pageIndex);
        }
      } else {
        OCacheEntry removedEntry = am.removeLRU();

        if (removedEntry == null) {
          increaseCacheSize();
        } else {
          assert removedEntry.usageCounter == 0;
          assert !removedEntry.isDirty;

          directMemory.free(removedEntry.dataPointer);

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
      return new HashSet<ODirtyPage>(); // TODO
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
      if (cacheEntry.usageCounter > 1)
        throw new IllegalStateException("Record cannot be removed because it is used!");
      return cacheEntry;
    }

    cacheEntry = a1out.remove(fileId, pageIndex);
    if (cacheEntry != null) {
      return cacheEntry;
    }
    cacheEntry = a1in.remove(fileId, pageIndex);
    if (cacheEntry != null && cacheEntry.usageCounter > 1)
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
