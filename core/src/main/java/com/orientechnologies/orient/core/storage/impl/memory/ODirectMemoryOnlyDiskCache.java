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

package com.orientechnologies.orient.core.storage.impl.memory;

import com.orientechnologies.common.directmemory.ODirectMemoryPointer;
import com.orientechnologies.common.util.OCommonConst;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.storage.cache.*;
import com.orientechnologies.orient.core.storage.cache.local.OWOWCache;
import com.orientechnologies.orient.core.storage.impl.local.OLowDiskSpaceListener;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Future;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientechnologies.com)
 * @since 6/24/14
 */
public class ODirectMemoryOnlyDiskCache extends OAbstractWriteCache implements OReadCache, OWriteCache {
  private final Lock                               metadataLock  = new ReentrantLock();

  private final Map<String, Integer>               fileNameIdMap = new HashMap<String, Integer>();
  private final Map<Integer, String>               fileIdNameMap = new HashMap<Integer, String>();

  private final ConcurrentMap<Integer, MemoryFile> files         = new ConcurrentHashMap<Integer, MemoryFile>();

  private int                                      counter       = 0;

  private final int                                pageSize;
  private final int                                id;

  public ODirectMemoryOnlyDiskCache(int pageSize, int id) {
    this.pageSize = pageSize;
    this.id = id;
  }

  @Override
  public long addFile(String fileName, OWriteCache writeCache) {
    metadataLock.lock();
    try {
      Integer fileId = fileNameIdMap.get(fileName);

      if (fileId == null) {
        counter++;
        final int id = counter;

        files.put(id, new MemoryFile(this.id, id, pageSize));
        fileNameIdMap.put(fileName, id);

        fileId = id;

        fileIdNameMap.put(fileId, fileName);
      } else {
        throw new OStorageException(fileName + " already exists.");
      }

      return composeFileId(id, fileId);
    } finally {
      metadataLock.unlock();
    }
  }

  @Override
  public long bookFileId(String fileName) {
    metadataLock.lock();
    try {
      counter++;
      return composeFileId(id, counter);
    } finally {
      metadataLock.unlock();
    }
  }

  @Override
  public long openFile(String fileName, OWriteCache writeCache) {
    metadataLock.lock();
    try {
      Integer fileId = fileNameIdMap.get(fileName);

      if (fileId == null) {
        throw new OStorageException("File " + fileName + " does not exist.");
      }

      return composeFileId(id, fileId);
    } finally {
      metadataLock.unlock();
    }
  }

  @Override
  public void openFile(long fileId, OWriteCache writeCache) {
    int intId = extractFileId(fileId);
    final MemoryFile memoryFile = files.get(intId);
    if (memoryFile == null)
      throw new OStorageException("File with id " + intId + " does not exist");
  }

  @Override
  public void openFile(String fileName, long fileId, OWriteCache writeCache) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void addFile(String fileName, long fileId, OWriteCache writeCache) {
    int intId = extractFileId(fileId);

    metadataLock.lock();
    try {
      if (files.containsKey(intId))
        throw new OStorageException("File with id " + intId + " already exists.");

      if (fileNameIdMap.containsKey(fileName))
        throw new OStorageException(fileName + " already exists.");

      files.put(intId, new MemoryFile(id, intId, pageSize));
      fileNameIdMap.put(fileName, intId);
      fileIdNameMap.put(intId, fileName);
    } finally {
      metadataLock.unlock();
    }
  }

  @Override
  public OCacheEntry load(long fileId, long pageIndex, boolean checkPinnedPages, OWriteCache writeCache) {
    int intId = extractFileId(fileId);

    final MemoryFile memoryFile = getFile(intId);
    final OCacheEntry cacheEntry = memoryFile.loadPage(pageIndex);
    if (cacheEntry == null)
      return null;

    synchronized (cacheEntry) {
      cacheEntry.incrementUsages();
    }

    return cacheEntry;
  }

  @Override
  public void pinPage(OCacheEntry cacheEntry) {
  }

  @Override
  public OCacheEntry allocateNewPage(long fileId, OWriteCache writeCache) {
    int intId = extractFileId(fileId);

    final MemoryFile memoryFile = getFile(intId);
    final OCacheEntry cacheEntry = memoryFile.addNewPage();

    synchronized (cacheEntry) {
      cacheEntry.incrementUsages();
    }

    return cacheEntry;
  }

  private MemoryFile getFile(int fileId) {
    final MemoryFile memoryFile = files.get(fileId);

    if (memoryFile == null)
      throw new OStorageException("File with id " + fileId + " does not exist");

    return memoryFile;
  }

  @Override
  public void release(OCacheEntry cacheEntry, OWriteCache writeCache) {
    synchronized (cacheEntry) {
      cacheEntry.decrementUsages();
    }
  }

  @Override
  public long getFilledUpTo(long fileId) {
    int intId = extractFileId(fileId);

    final MemoryFile memoryFile = getFile(intId);
    return memoryFile.size();
  }

  @Override
  public void flush(long fileId) {
  }

  @Override
  public void close(long fileId, boolean flush) {
  }

  @Override
  public void deleteFile(long fileId) {
    int intId = extractFileId(fileId);
    metadataLock.lock();
    try {
      final String fileName = fileIdNameMap.remove(intId);
      if (fileName == null)
        return;

      fileNameIdMap.remove(fileName);
      MemoryFile file = files.remove(intId);
      if (file != null)
        file.clear();
    } finally {
      metadataLock.unlock();
    }
  }

  @Override
  public void renameFile(long fileId, String oldFileName, String newFileName) {
    int intId = extractFileId(fileId);

    metadataLock.lock();
    try {
      String fileName = fileIdNameMap.get(intId);
      if (fileName == null)
        return;

      fileNameIdMap.remove(fileName);

      fileName = newFileName + fileName.substring(fileName.lastIndexOf(oldFileName) + fileName.length());

      fileIdNameMap.put(intId, fileName);
      fileNameIdMap.put(fileName, intId);
    } finally {
      metadataLock.unlock();
    }
  }

  @Override
  public void truncateFile(long fileId) {
    int intId = extractFileId(fileId);

    final MemoryFile file = getFile(intId);
    file.clear();
  }

  @Override
  public void flush() {
  }

  @Override
  public long[] close() {
    return null;
  }

  @Override
  public void clear() {
    delete();
  }

  @Override
  public long[] delete() {
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

    return null;
  }

  @Override
  public void deleteStorage(OWriteCache writeCache) throws IOException {
    delete();
  }

  @Override
  public void closeStorage(OWriteCache writeCache) throws IOException {
    close();
  }

  @Override
  public OPageDataVerificationError[] checkStoredPages(OCommandOutputListener commandOutputListener) {
    return OCommonConst.EMPTY_PAGE_DATA_VERIFICATION_ARRAY;
  }

  @Override
  public boolean isOpen(long fileId) {
    return files.get(extractFileId(fileId)) != null;
  }

  @Override
  public boolean exists(String name) {
    metadataLock.lock();
    try {
      final Integer fileId = fileNameIdMap.get(name);
      if (fileId == null)
        return false;

      final MemoryFile memoryFile = files.get(fileId);
      return memoryFile != null;
    } finally {
      metadataLock.unlock();
    }
  }

  @Override
  public boolean exists(long fileId) {
    int intId = extractFileId(fileId);

    metadataLock.lock();
    try {
      final MemoryFile memoryFile = files.get(intId);
      return memoryFile != null;
    } finally {
      metadataLock.unlock();
    }
  }

  @Override
  public String fileNameById(long fileId) {
    int intId = extractFileId(fileId);

    metadataLock.lock();
    try {
      return fileIdNameMap.get(intId);
    } finally {
      metadataLock.unlock();
    }
  }

  @Override
  public void lock() {
  }

  @Override
  public void unlock() {
  }

  private static final class MemoryFile {
    private final int                                      id;
    private final int                                      storageId;

    private final int                                      pageSize;
    private final ReadWriteLock                            clearLock = new ReentrantReadWriteLock();

    private final ConcurrentSkipListMap<Long, OCacheEntry> content   = new ConcurrentSkipListMap<Long, OCacheEntry>();

    private MemoryFile(int storageId, int id, int pageSize) {
      this.storageId = storageId;
      this.id = id;
      this.pageSize = pageSize;
    }

    private OCacheEntry loadPage(long index) {
      clearLock.readLock().lock();
      try {
        return content.get(index);
      } finally {
        clearLock.readLock().unlock();
      }
    }

    private OCacheEntry addNewPage() {
      clearLock.readLock().lock();
      try {
        OCacheEntry cacheEntry;

        long index = -1;
        do {
          if (content.isEmpty())
            index = 0;
          else {
            long lastIndex = content.lastKey();
            index = lastIndex + 1;
          }

          final ODirectMemoryPointer directMemoryPointer = new ODirectMemoryPointer(new byte[pageSize + 2
              * ODurablePage.PAGE_PADDING]);
          final OCachePointer cachePointer = new OCachePointer(directMemoryPointer, new OLogSequenceNumber(-1, -1), id, index);
          cachePointer.incrementReferrer();

          cacheEntry = new OCacheEntry(composeFileId(storageId, id), index, cachePointer, false);

          OCacheEntry oldCacheEntry = content.putIfAbsent(index, cacheEntry);

          if (oldCacheEntry != null) {
            cacheEntry.getCachePointer().decrementReferrer();
            index = -1;
          }
        } while (index < 0);

        return cacheEntry;
      } finally {
        clearLock.readLock().unlock();
      }
    }

    private long size() {
      clearLock.readLock().lock();
      try {
        if (content.isEmpty())
          return 0;

        try {
          return content.lastKey() + 1;
        } catch (NoSuchElementException e) {
          return 0;
        }

      } finally {
        clearLock.readLock().unlock();
      }
    }

    private long getUsedMemory() {
      return content.size();
    }

    private void clear() {
      boolean thereAreNotReleased = false;

      clearLock.writeLock().lock();
      try {
        for (OCacheEntry entry : content.values()) {
          synchronized (entry) {
            thereAreNotReleased |= entry.getUsagesCount() > 0;
            entry.getCachePointer().decrementReferrer();
          }
        }

        content.clear();
      } finally {
        clearLock.writeLock().unlock();
      }

      if (thereAreNotReleased)
        throw new IllegalStateException("Some cache entries were not released. Storage may be in invalid state.");
    }
  }

  @Override
  public long getUsedMemory() {
    long totalPages = 0;
    for (MemoryFile file : files.values())
      totalPages += file.getUsedMemory();

    return totalPages * (pageSize + 2 * OWOWCache.PAGE_PADDING);
  }

  @Override
  public void startFuzzyCheckpoints() {
  }

  @Override
  public boolean checkLowDiskSpace() {
    return true;
  }

  @Override
  public void makeFuzzyCheckpoint() {
  }

  @Override
  public void addLowDiskSpaceListener(OLowDiskSpaceListener listener) {
  }

  @Override
  public void removeLowDiskSpaceListener(OLowDiskSpaceListener listener) {
  }

  @Override
  public long openFile(String fileName) {
    return openFile(fileName, null);
  }

  @Override
  public long addFile(String fileName) {
    return addFile(fileName, null);
  }

  @Override
  public void openFile(String fileName, long fileId) {
    openFile(fileName, fileId, null);
  }

  @Override
  public void addFile(String fileName, long fileId) {
    addFile(fileName, fileId, null);
  }

  @Override
  public void openFile(long fileId) {
    openFile(fileId, null);
  }

  @Override
  public Future store(long fileId, long pageIndex, OCachePointer dataPointer) {
    throw new UnsupportedOperationException();
  }

  @Override
  public OCachePointer load(long fileId, long pageIndex, boolean addNewPages) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getExclusiveWriteCachePagesSize() {
    return 0;
  }

  @Override
  public Long isOpen(String fileName) {
    metadataLock.lock();
    try {
      Integer result = fileNameIdMap.get(fileName);

      if (result == null)
        return null;

      return composeFileId(id, result);
    } finally {
      metadataLock.unlock();
    }
  }

  @Override
  public void truncateFile(long fileId, OWriteCache writeCache) throws IOException {
    truncateFile(fileId);
  }

  @Override
  public int getId() {
    return id;
  }

  @Override
  public void closeFile(long fileId, boolean flush, OWriteCache writeCache) throws IOException {
    close(fileId, flush);
  }

  @Override
  public void deleteFile(long fileId, OWriteCache writeCache) throws IOException {
    deleteFile(fileId);
  }
}
