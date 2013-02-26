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
import java.util.Map;

import com.orientechnologies.common.concur.lock.OLockManager;
import com.orientechnologies.common.directmemory.ODirectMemory;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.config.OStorageSegmentConfiguration;
import com.orientechnologies.orient.core.storage.fs.OFile;
import com.orientechnologies.orient.core.storage.impl.local.OMultiFileSegment;
import com.orientechnologies.orient.core.storage.impl.local.OStorageLocal;

/**
 * @author Andrey Lomakin
 * @since 25.02.13
 */
public class ARCBuffer {
  private final int                                 maxSize;
  private final int                                 pageSize;

  private long                                      p = 0;

  private final LRUList                             fetchedOnce;
  private final LRUList                             fetchedOnceEvicted;

  private final LRUList                             fetchedSeveralTimes;
  private final LRUList                             fetchedSeveralTimesEvicted;

  private final ODirectMemory                       directMemory;

  private final Map<String, OMultiFileSegment>      openFiles;

  private final OLockManager<FileLockKey, Runnable> lockManager;
  private final Object                              syncObject;
  private final OStorageLocal                       storageLocal;

  public ARCBuffer(long maxMemory, ODirectMemory directMemory, int pageSize, OStorageLocal storageLocal) {
    this.directMemory = directMemory;
    this.pageSize = pageSize;
    this.storageLocal = storageLocal;
    this.openFiles = new HashMap<String, OMultiFileSegment>();

    this.lockManager = new OLockManager<FileLockKey, Runnable>(OGlobalConfiguration.ENVIRONMENT_CONCURRENT.getValueAsBoolean(),
        1000);

    long tmpMaxSize = maxMemory / pageSize;
    if (tmpMaxSize >= Integer.MAX_VALUE)
      maxSize = Integer.MAX_VALUE;
    else
      maxSize = (int) tmpMaxSize;

    fetchedOnce = new LRUList();
    fetchedOnceEvicted = new LRUList();

    fetchedSeveralTimes = new LRUList();
    fetchedSeveralTimesEvicted = new LRUList();

    syncObject = new Object();
  }

  public void openFile(OStorageSegmentConfiguration fileConfiguration, String fileExtension) throws IOException {
    synchronized (syncObject) {
      if (!openFiles.containsKey(fileConfiguration.name)) {
        final OMultiFileSegment multiFileSegment = new OMultiFileSegment(storageLocal, fileConfiguration, fileExtension, pageSize);
        if (multiFileSegment.getFile(0).exists())
          multiFileSegment.open();
        else
          multiFileSegment.create(pageSize);

        final String fullName = fileConfiguration.name + fileExtension;
        openFiles.put(fullName, multiFileSegment);
      }
    }
  }

  public long loadAndLockForWrite(String fileName, String fileExtension, long filePosition) throws IOException {
    synchronized (syncObject) {
      final String fullName = fileName + fileExtension;
      final LRUEntry lruEntry = load(fullName, filePosition);

      lockManager.acquireLock(Thread.currentThread(), new FileLockKey(fullName, filePosition), OLockManager.LOCK.EXCLUSIVE);
      lruEntry.isDirty = true;

      return lruEntry.dataPointer;
    }
  }

  public long loadAndLockForRead(String fileName, String fileExtension, long filePosition) throws IOException {
    synchronized (syncObject) {
      final String fullName = fileName + fileExtension;
      final LRUEntry lruEntry = load(fullName, filePosition);

      lockManager.acquireLock(Thread.currentThread(), new FileLockKey(fullName, filePosition), OLockManager.LOCK.SHARED);
      return lruEntry.dataPointer;
    }
  }

  public void releaseReadLock(String fileName, String fileExtension, long filePosition) {
    final String fullName = fileName + fileExtension;
    lockManager.releaseLock(Thread.currentThread(), new FileLockKey(fullName, filePosition), OLockManager.LOCK.SHARED);
  }

  public void releaseWriteLock(String fileName, String fileExtension, long filePosition) {
    final String fullName = fileName + fileExtension;
    lockManager.releaseLock(Thread.currentThread(), new FileLockKey(fullName, filePosition), OLockManager.LOCK.EXCLUSIVE);
  }

  public long getFilledUpTo(String fileName, String fileExtension) {
    synchronized (syncObject) {
      final String fullName = fileName + fileExtension;
      return openFiles.get(fullName).getFilledUpTo();
    }
  }

  private LRUEntry load(final String fileName, long filePosition) throws IOException {
    LRUEntry lruEntry = fetchedOnce.get(fileName, filePosition);
    if (lruEntry != null) {
      fetchedOnce.remove(fileName, filePosition);

      fetchedSeveralTimes.putToMRU(fileName, filePosition, lruEntry.dataPointer, lruEntry.isDirty);

      return lruEntry;
    }

    lruEntry = fetchedSeveralTimes.get(fileName, filePosition);
    if (lruEntry != null) {
      fetchedSeveralTimes.putToMRU(fileName, filePosition, lruEntry.dataPointer, lruEntry.isDirty);
      return lruEntry;
    }

    if (fetchedOnceEvicted.contains(fileName, filePosition)) {
      int onceSize = fetchedOnceEvicted.size();
      int severalTimesSize = fetchedSeveralTimesEvicted.size();

      long delta = onceSize >= severalTimesSize ? 1 : severalTimesSize / onceSize;
      p = Math.min(p + delta, maxSize);

      long dataPointer = cacheFileContent(fileName, filePosition);

      replace(false);

      fetchedOnceEvicted.remove(fileName, filePosition);

      return fetchedSeveralTimes.putToMRU(fileName, filePosition, dataPointer, false);
    }

    if (fetchedSeveralTimesEvicted.contains(fileName, filePosition)) {
      int onceSize = fetchedOnceEvicted.size();
      int severalTimesSize = fetchedSeveralTimesEvicted.size();

      long delta = severalTimesSize >= onceSize ? 1 : onceSize / severalTimesSize;
      p = Math.max(p - delta, 0);

      replace(true);

      fetchedSeveralTimesEvicted.remove(fileName, filePosition);

      long dataPointer = cacheFileContent(fileName, filePosition);

      return fetchedSeveralTimes.putToMRU(fileName, filePosition, dataPointer, false);
    }

    if (fetchedOnce.size() + fetchedOnceEvicted.size() == maxSize) {
      if (fetchedOnce.size() < maxSize) {
        fetchedOnceEvicted.removeLRU();
        replace(false);
      } else {
        LRUEntry removedEntry = fetchedOnceEvicted.removeLRU();
        evictFileContent(fileName, filePosition, removedEntry.dataPointer, false);
      }
    } else {
      int totalSize = fetchedOnce.size() + fetchedSeveralTimes.size() + fetchedOnceEvicted.size()
          + fetchedSeveralTimesEvicted.size();
      if (totalSize >= maxSize) {
        if (totalSize == 2 * maxSize)
          fetchedSeveralTimesEvicted.removeLRU();

        replace(false);
      }
    }

    long dataPointer = cacheFileContent(fileName, filePosition);

    return fetchedOnce.putToMRU(fileName, filePosition, dataPointer, false);
  }

  private long cacheFileContent(String fileName, long filePosition) throws IOException {
    final OMultiFileSegment multiFileSegment = openFiles.get(fileName);
    final long endPosition = filePosition + pageSize;

    byte[] content = new byte[pageSize];
    if (multiFileSegment.getFilledUpTo() >= endPosition) {
      final long[] pos = multiFileSegment.getRelativePosition(filePosition);

      final OFile file = multiFileSegment.getFile((int) pos[0]);
      file.read(pos[1], content, content.length);
    } else if (multiFileSegment.getFilledUpTo() > filePosition) {
      int dif = (int) (filePosition - multiFileSegment.getFilledUpTo());

      final long[] pos = multiFileSegment.getRelativePosition(filePosition);

      final OFile file = multiFileSegment.getFile((int) pos[0]);
      file.read(pos[1], content, dif);

      multiFileSegment.allocateSpace((int) (endPosition - dif));
    } else {
      multiFileSegment.allocateSpace((int) (endPosition - multiFileSegment.getFilledUpTo()));
    }

    long dataPointer;

    dataPointer = directMemory.allocate(content);
    return dataPointer;
  }

  private void evictFileContent(String fileName, long filePosition, long dataPointer, boolean isDirty) throws IOException {
    if (isDirty) {
      final OMultiFileSegment multiFileSegment = openFiles.get(fileName);
      final byte[] content = directMemory.get(dataPointer, pageSize);

      final long[] pos = multiFileSegment.getRelativePosition(filePosition);
      final OFile file = multiFileSegment.getFile((int) pos[0]);
      file.write(pos[1], content);
    }

    directMemory.free(dataPointer);
  }

  private void replace(boolean dataEvictedSeveralTimes) throws IOException {
    if (fetchedOnce.size() > 0 && fetchedOnce.size() > p || (dataEvictedSeveralTimes && fetchedOnce.size() == p)) {
      LRUEntry lruEntry = fetchedOnce.removeLRU();
      evictFileContent(lruEntry.fileName, lruEntry.filePosition, lruEntry.dataPointer, lruEntry.isDirty);
      fetchedOnceEvicted.putToMRU(lruEntry.fileName, lruEntry.filePosition, lruEntry.dataPointer, false);
    } else {
      LRUEntry lruEntry = fetchedSeveralTimes.removeLRU();
      evictFileContent(lruEntry.fileName, lruEntry.filePosition, lruEntry.dataPointer, lruEntry.isDirty);
      fetchedSeveralTimesEvicted.putToMRU(lruEntry.fileName, lruEntry.filePosition, lruEntry.dataPointer, false);
    }
  }

  private final class FileLockKey {
    private final String fileName;
    private final long   filePosition;

    private FileLockKey(String fileName, long filePosition) {
      this.fileName = fileName;
      this.filePosition = filePosition;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o)
        return true;
      if (o == null || getClass() != o.getClass())
        return false;

      FileLockKey fileKey = (FileLockKey) o;

      if (filePosition != fileKey.filePosition)
        return false;
      if (!fileName.equals(fileKey.fileName))
        return false;

      return true;
    }

    @Override
    public int hashCode() {
      int result = fileName.hashCode();
      result = 31 * result + (int) (filePosition ^ (filePosition >>> 32));
      return result;
    }
  }
}
