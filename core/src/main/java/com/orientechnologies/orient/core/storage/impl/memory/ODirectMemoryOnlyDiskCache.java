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
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.index.hashindex.local.cache.*;
import com.orientechnologies.orient.core.storage.impl.local.OLowDiskSpaceListener;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientechnologies.com)
 * @since 6/24/14
 */
public class ODirectMemoryOnlyDiskCache implements ODiskCache {
  private final Lock                            metadataLock  = new ReentrantLock();

  private final Map<String, Long>               fileNameIdMap = new HashMap<String, Long>();
  private final Map<Long, String>               fileIdNameMap = new HashMap<Long, String>();

  private final ConcurrentMap<Long, MemoryFile> files         = new ConcurrentHashMap<Long, MemoryFile>();

  private long                                  counter       = 0;

  private final int                             pageSize;

  public ODirectMemoryOnlyDiskCache(int pageSize) {
    this.pageSize = pageSize;
  }

  @Override
  public long addFile(String fileName) throws IOException {
    metadataLock.lock();
    try {
      Long fileId = fileNameIdMap.get(fileName);

      if (fileId == null) {
        counter++;
        final long id = counter;

        files.put(id, new MemoryFile(id, pageSize));
        fileNameIdMap.put(fileName, id);

        fileId = id;

        fileIdNameMap.put(fileId, fileName);
      } else {
        throw new OStorageException(fileName + " already exists.");
      }

      return fileId;
    } finally {
      metadataLock.unlock();
    }
  }

  @Override
  public long bookFileId(String fileName) {
    metadataLock.lock();
    try {
      counter++;
      return counter;
    } finally {
      metadataLock.unlock();
    }
  }

  @Override
  public long openFile(String fileName) throws IOException {
    metadataLock.lock();
    try {
      Long fileId = fileNameIdMap.get(fileName);

      if (fileId == null) {
        throw new OStorageException("File " + fileName + " does not exist.");
      }

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
  public void addFile(String fileName, long fileId) throws IOException {
    metadataLock.lock();
    try {
      if (files.containsKey(fileId))
        throw new OStorageException("File with id " + fileId + " already exists.");

      if (fileNameIdMap.containsKey(fileName))
        throw new OStorageException(fileName + " already exists.");

      files.put(fileId, new MemoryFile(fileId, pageSize));
      fileNameIdMap.put(fileName, fileId);
      fileIdNameMap.put(fileId, fileName);
    } finally {
      metadataLock.unlock();
    }
  }

  @Override
  public OCacheEntry load(long fileId, long pageIndex, boolean checkPinnedPages) throws IOException {
    final MemoryFile memoryFile = getFile(fileId);
    final OCacheEntry cacheEntry = memoryFile.loadPage(pageIndex);
    if (cacheEntry == null)
      return null;

    synchronized (cacheEntry) {
      cacheEntry.incrementUsages();
    }

    return cacheEntry;
  }

  @Override
  public void pinPage(OCacheEntry cacheEntry) throws IOException {
  }

  @Override
  public OCacheEntry allocateNewPage(long fileId) throws IOException {
    final MemoryFile memoryFile = getFile(fileId);
    final OCacheEntry cacheEntry = memoryFile.addNewPage();

    synchronized (cacheEntry) {
      cacheEntry.incrementUsages();
    }

    return cacheEntry;
  }

  private MemoryFile getFile(long fileId) {
    final MemoryFile memoryFile = files.get(fileId);

    if (memoryFile == null)
      throw new OStorageException("File with id " + fileId + " does not exist");

    return memoryFile;
  }

  @Override
  public void release(OCacheEntry cacheEntry) {
    synchronized (cacheEntry) {
      cacheEntry.decrementUsages();
    }
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
  }

  @Override
  public void closeFile(long fileId, boolean flush) throws IOException {
  }

  @Override
  public void deleteFile(long fileId) throws IOException {
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
  public void renameFile(long fileId, String oldFileName, String newFileName) throws IOException {
    metadataLock.lock();
    try {
      String fileName = fileIdNameMap.get(fileId);
      if (fileName == null)
        return;

      fileNameIdMap.remove(fileName);

      fileName = newFileName + fileName.substring(fileName.lastIndexOf(oldFileName) + fileName.length());

      fileIdNameMap.put(fileId, fileName);
      fileNameIdMap.put(fileName, fileId);
    } finally {
      metadataLock.unlock();
    }
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
  }

  @Override
  public void delete() throws IOException {
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
  public OPageDataVerificationError[] checkStoredPages(OCommandOutputListener commandOutputListener) {
    return new OPageDataVerificationError[0];
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
  public boolean exists(long fileId) {
    metadataLock.lock();
    try {
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
    private final long                                     id;

    private final int                                      pageSize;
    private final ReadWriteLock                            clearLock = new ReentrantReadWriteLock();

    private final ConcurrentSkipListMap<Long, OCacheEntry> content   = new ConcurrentSkipListMap<Long, OCacheEntry>();

    private MemoryFile(long id, int pageSize) {
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
          final OCachePointer cachePointer = new OCachePointer(directMemoryPointer, new OLogSequenceNumber(-1, -1));
          cachePointer.incrementReferrer();

          cacheEntry = new OCacheEntry(id, index, cachePointer, false);

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
  public void addLowDiskSpaceListener(OLowDiskSpaceListener listener) {
  }

  @Override
  public void removeLowDiskSpaceListener(OLowDiskSpaceListener listener) {
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
}
