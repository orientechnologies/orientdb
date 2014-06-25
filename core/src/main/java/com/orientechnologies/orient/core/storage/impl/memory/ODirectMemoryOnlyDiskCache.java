package com.orientechnologies.orient.core.storage.impl.memory;

import com.orientechnologies.common.directmemory.ODirectMemoryPointer;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.index.hashindex.local.cache.OCacheEntry;
import com.orientechnologies.orient.core.index.hashindex.local.cache.OCachePointer;
import com.orientechnologies.orient.core.index.hashindex.local.cache.ODiskCache;
import com.orientechnologies.orient.core.index.hashindex.local.cache.OPageDataVerificationError;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.ODirtyPage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author Andrey Lomakin <a href="mailto:lomakin.andrey@gmail.com">Andrey Lomakin</a>
 * @since 6/24/14
 */
public class ODirectMemoryOnlyDiskCache implements ODiskCache {
  private final Lock                            metadataLock  = new ReentrantLock();

  private final Map<String, Long>               fileNameIdMap = new HashMap<String, Long>();
  private final Map<Long, String>               fileIdNameMap = new HashMap<Long, String>();

  private final ConcurrentMap<Long, MemoryFile> files         = new ConcurrentHashMap<Long, MemoryFile>();

  private final AtomicLong                      counter       = new AtomicLong();

  private final int                             pageSize;

  public ODirectMemoryOnlyDiskCache(int pageSize) {
    this.pageSize = pageSize;
  }

  @Override
  public long openFile(String fileName) throws IOException {
    metadataLock.lock();
    try {
      Long fileId = fileNameIdMap.get(fileName);

      if (fileId == null) {
        final long id = counter.incrementAndGet();
        files.put(id, new MemoryFile(id, pageSize));
        fileNameIdMap.put(fileName, id);
        fileId = id;
      }

      fileIdNameMap.put(fileId, fileName);
      return fileId;
    } finally {
      metadataLock.unlock();
    }
  }

  @Override
  public void openFile(long fileId) throws IOException {
    final MemoryFile memoryFile = files.get(fileId);
    if (memoryFile == null)
      throw new OStorageException("File with id " + fileId + " does not exist");
  }

  @Override
  public void openFile(String fileName, long fileId) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public OCacheEntry load(long fileId, long pageIndex, boolean checkPinnedPages) throws IOException {
    final MemoryFile memoryFile = getFile(fileId);

    return new OCacheEntry(fileId, pageIndex, memoryFile.loadPage(pageIndex), false);
  }

  @Override
  public void pinPage(OCacheEntry cacheEntry) throws IOException {
  }

  @Override
  public void loadPinnedPage(OCacheEntry cacheEntry) throws IOException {
  }

  @Override
  public OCacheEntry allocateNewPage(long fileId) throws IOException {
    final MemoryFile memoryFile = getFile(fileId);
    final long pageIndex = memoryFile.addNewPage();

    return new OCacheEntry(fileId, pageIndex, memoryFile.loadPage(pageIndex), false);
  }

  private MemoryFile getFile(long fileId) {
    final MemoryFile memoryFile = files.get(fileId);

    if (memoryFile == null)
      throw new OStorageException("File with id " + fileId + " does not exist");

    return memoryFile;
  }

  @Override
  public void release(OCacheEntry cacheEntry) {
  }

  @Override
  public long getFilledUpTo(long fileId) throws IOException {
    final MemoryFile memoryFile = getFile(fileId);
    return memoryFile.size();
  }

  @Override
  public void flushFile(long fileId) throws IOException {
  }

  @Override
  public void closeFile(long fileId) throws IOException {
    closeFile(fileId, false);
  }

  @Override
  public void closeFile(long fileId, boolean flush) throws IOException {
    metadataLock.lock();
    try {
      final String fileName = fileIdNameMap.remove(fileId);
      if (fileName == null)
        return;

      fileNameIdMap.remove(fileName);
      MemoryFile file = files.remove(fileId);
      if (file != null)
        file.clear();
    } finally {
      metadataLock.unlock();
    }
  }

  @Override
  public void deleteFile(long fileId) throws IOException {
    closeFile(fileId, false);
  }

  @Override
  public void renameFile(long fileId, String oldFileName, String newFileName) throws IOException {

  }

  @Override
  public void truncateFile(long fileId) throws IOException {
    final MemoryFile file = getFile(fileId);
    file.clear();
  }

  @Override
  public boolean wasSoftlyClosed(long fileId) throws IOException {
    return true;
  }

  @Override
  public void setSoftlyClosed(long fileId, boolean softlyClosed) throws IOException {
  }

  @Override
  public void setSoftlyClosed(boolean softlyClosed) throws IOException {
  }

  @Override
  public void flushBuffer() throws IOException {
  }

  @Override
  public void close() throws IOException {
    metadataLock.lock();
    try {
      for (MemoryFile file : files.values())
        file.clear();

      files.clear();
      fileIdNameMap.clear();
      fileNameIdMap.clear();
    } finally {
      metadataLock.unlock();
    }
  }

  @Override
  public void delete() throws IOException {
    close();
  }

  @Override
  public OPageDataVerificationError[] checkStoredPages(OCommandOutputListener commandOutputListener) {
    return new OPageDataVerificationError[0];
  }

  @Override
  public Set<ODirtyPage> logDirtyPagesTable() throws IOException {
    return Collections.emptySet();
  }

  @Override
  public void forceSyncStoredChanges() throws IOException {
  }

  @Override
  public boolean isOpen(long fileId) {
    return files.get(fileId) != null;
  }

  @Override
  public boolean exists(String name) {
    metadataLock.lock();
    try {
      final Long fileId = fileNameIdMap.get(name);
      if (fileId == null)
        return false;

      final MemoryFile memoryFile = files.get(fileId);
      return memoryFile != null;
    } finally {
      metadataLock.unlock();
    }
  }

  @Override
  public String fileNameById(long fileId) {
    metadataLock.lock();
    try {
      return fileIdNameMap.get(fileId);
    } finally {
      metadataLock.unlock();
    }
  }

  @Override
  public void lock() throws IOException {
  }

  @Override
  public void unlock() throws IOException {
  }

  private static final class MemoryFile {
    private final long                                       id;

    private final int                                        pageSize;
    private final ReadWriteLock                              clearLock = new ReentrantReadWriteLock();

    private final ConcurrentSkipListMap<Long, OCachePointer> content   = new ConcurrentSkipListMap<Long, OCachePointer>();

    private MemoryFile(long id, int pageSize) {
      this.id = id;
      this.pageSize = pageSize;
    }

    private OCachePointer loadPage(long index) {
      clearLock.readLock().lock();
      try {
        OCachePointer cachePointer = content.get(index);
        if (cachePointer != null)
          return cachePointer;

        ODirectMemoryPointer directMemoryPointer = new ODirectMemoryPointer(pageSize);
        cachePointer = new OCachePointer(directMemoryPointer, new OLogSequenceNumber(0, 0));
        cachePointer.incrementReferrer();

        OCachePointer oldCachePointer = content.putIfAbsent(index, cachePointer);
        if (oldCachePointer != null)
          cachePointer.decrementReferrer();
        {
          cachePointer = oldCachePointer;
        }

        return cachePointer;
      } finally {
        clearLock.readLock().unlock();
      }
    }

    private long addNewPage() {
      clearLock.readLock().lock();
      try {
        long index = -1;
        do {
          Long lastIndex = content.lastKey();
          if (lastIndex == null)
            index = 0;
          else
            index = lastIndex + 1;

          final ODirectMemoryPointer directMemoryPointer = new ODirectMemoryPointer(pageSize);
          final OCachePointer cachePointer = new OCachePointer(directMemoryPointer, new OLogSequenceNumber(0, 0));
          cachePointer.incrementReferrer();

          OCachePointer oldCachePointer = content.putIfAbsent(index, cachePointer);
          if (oldCachePointer != null) {
            cachePointer.decrementReferrer();
            index = -1;
          }
        } while (index < 0);

        return index;
      } finally {
        clearLock.readLock().unlock();
      }
    }

    private long size() {
      clearLock.readLock().lock();
      try {
        Long lastKey = content.lastKey();
        if (lastKey == null)
          return 0;

        return content.lastKey() + 1;
      } finally {
        clearLock.readLock().unlock();
      }
    }

    private void clear() {
      clearLock.writeLock().lock();
      try {
        for (OCachePointer pointer : content.values())
          pointer.decrementReferrer();

        content.clear();
      } finally {
        clearLock.writeLock().unlock();
      }

    }
  }
}