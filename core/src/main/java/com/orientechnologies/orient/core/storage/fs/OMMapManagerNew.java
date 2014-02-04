/*
 * Copyright 1999-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
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
package com.orientechnologies.orient.core.storage.fs;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

import com.orientechnologies.common.concur.lock.OLockManager;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.profiler.OAbstractProfiler.OProfilerHookValue;
import com.orientechnologies.common.profiler.OProfilerMBean.METRIC_TYPE;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;

/**
 * @author Lev Sivashov <a href="mailto:lsivashov@gmail.com">lsivashov@gmail.com</a>
 * @since 06.05.12
 *        <p/>
 *        This class in new realization of mmap manager that uses OS swap mechanism to mmap files.
 */
public class OMMapManagerNew extends OMMapManagerAbstract implements OMMapManager {
  private static final int                                            BINARY_SEARCH_THRESHOLD = 10;

  private static final OMMapBufferEntry[]                             EMPTY_BUFFER_ENTRIES    = new OMMapBufferEntry[0];

  private final ConcurrentHashMap<OFileMMap, OMMapBufferEntry[]>      bufferPoolPerFile       = new ConcurrentHashMap<OFileMMap, OMMapBufferEntry[]>();
  private final ConcurrentHashMap<OFileMMap, LastMMapEntrySearchInfo> mapEntrySearchInfo      = new ConcurrentHashMap<OFileMMap, LastMMapEntrySearchInfo>();

  private final OLockManager<OFileMMap, Runnable>                     lockManager             = new OLockManager<OFileMMap, Runnable>(
                                                                                                  OGlobalConfiguration.ENVIRONMENT_CONCURRENT
                                                                                                      .getValueAsBoolean(),
                                                                                                  OGlobalConfiguration.STORAGE_RECORD_LOCK_TIMEOUT
                                                                                                      .getValueAsInteger());
  private long                                                        metricMappedPages       = 0;
  private long                                                        metricReusedPages       = 0;

  private TimerTask                                                   autoFlushTask;

  private int                                                         autoFlushUnusedTime;

  public void init() {
    Orient
        .instance()
        .getProfiler()
        .registerHookValue("system.file.mmap.mappedPages", "Number of memory mapped pages used", METRIC_TYPE.COUNTER,
            new OProfilerHookValue() {
              public Object getValue() {
                return metricMappedPages;
              }
            });
    Orient
        .instance()
        .getProfiler()
        .registerHookValue("system.file.mmap.reusedPages", "Number of times memory mapped pages have been reused",
            METRIC_TYPE.COUNTER, new OProfilerHookValue() {
              public Object getValue() {
                return metricReusedPages;
              }
            });

    int autoFlushTimer = OGlobalConfiguration.FILE_MMAP_AUTOFLUSH_TIMER.getValueAsInteger();
    if (autoFlushTimer > 0) {
      autoFlushTimer *= 1000;
      autoFlushUnusedTime = OGlobalConfiguration.FILE_MMAP_AUTOFLUSH_UNUSED_TIME.getValueAsInteger() * 1000;

      autoFlushTask = new TimerTask() {
        @Override
        public void run() {
          flush();
        }
      };

      Orient.instance().getTimer().schedule(autoFlushTask, autoFlushTimer, autoFlushTimer);
    }
  }

  /**
   * {@inheritDoc}
   * <p/>
   * If file is not mapped new file record will be created inside mmap manager. First method search if necessary record is already
   * mapped. And returns buffer on success. If additional mapping is required it will be performed to map all files content. After
   * that mapped array will be returned.
   */
  public OMMapBufferEntry[] acquire(final OFileMMap iFile, final long iBeginOffset, final int iSize,
      final OMMapManager.OPERATION_TYPE iOperationType, final OMMapManager.ALLOC_STRATEGY iStrategy) {
    if (iStrategy == OMMapManager.ALLOC_STRATEGY.MMAP_NEVER) {
      return null;
    }

    // lockManager.acquireLock(iFile, this, OLockManager.LOCK.EXCLUSIVE);
    // if entry for this file does not exist then we create it
    OMMapBufferEntry[] fileEntries = bufferPoolPerFile.get(iFile);
    if (fileEntries == null) {
      fileEntries = EMPTY_BUFFER_ENTRIES;
      bufferPoolPerFile.putIfAbsent(iFile, fileEntries);
    }

    // search for entries among existing
    OMMapBufferEntry[] foundEntries = searchAmongExisting(iFile, fileEntries, iBeginOffset, iSize);

    // if something is found we look whether the range is complete
    if (foundEntries.length > 0) {
      // look if requested range's right boundary is less than last entry's right boundary
      final OMMapBufferEntry lastEntry = foundEntries[foundEntries.length - 1];
      if (lastEntry.beginOffset + lastEntry.size >= iBeginOffset + iSize) {
        acquireLocksOnEntries(foundEntries, iOperationType);
        metricReusedPages++;
        return foundEntries;
      }
    }

    // map new entry
    final OMMapBufferEntry newMappedEntry;

    lockManager.acquireLock(Thread.currentThread(), iFile, OLockManager.LOCK.EXCLUSIVE);
    try {

      // second search after locking file
      fileEntries = bufferPoolPerFile.get(iFile);
      foundEntries = searchAmongExisting(iFile, fileEntries, iBeginOffset, iSize);

      // total size that is mapped
      long totalMappedSize = 0;
      if (fileEntries.length > 0) {
        final OMMapBufferEntry lastEntry = fileEntries[fileEntries.length - 1];
        totalMappedSize = lastEntry.beginOffset + lastEntry.size;
      }

      try {
        newMappedEntry = mapNew(iFile, totalMappedSize);
      } catch (IOException ex) {
        return null;
      }
      final OMMapBufferEntry[] newEntries = addEntry(fileEntries, newMappedEntry);
      bufferPoolPerFile.put(iFile, newEntries);

      // add new entry to previously found
      final OMMapBufferEntry[] resultEntries = addEntry(foundEntries, newMappedEntry);
      acquireLocksOnEntries(resultEntries, iOperationType);

      return resultEntries;
    } finally {
      lockManager.releaseLock(Thread.currentThread(), iFile, OLockManager.LOCK.EXCLUSIVE);
    }
  }

  /**
   * {@inheritDoc}
   * <p/>
   * Flushes all closed files on disk. If some mapped entries not flushed successfully file will be associated with not flushed
   * entries. When flush will be performed again not flushed records will be flushed again, File information for files which have
   * all records flushed will be removed from mmap manager.
   */
  public void flush() {
    OLogManager.instance().debug(this, "[OMMapManagerNew] flushing pages in memory...");
    int flushedBlocks = 0;
    int totalBlocks = 0;

    final long now = System.currentTimeMillis();

    for (Iterator<Map.Entry<OFileMMap, OMMapBufferEntry[]>> it = bufferPoolPerFile.entrySet().iterator(); it.hasNext();) {
      OFileMMap file;
      final Map.Entry<OFileMMap, OMMapBufferEntry[]> mapEntry = it.next();
      file = mapEntry.getKey();

      // FLUSHES ALL THE BLOCK OF THE FILE
      lockManager.acquireLock(Thread.currentThread(), file, OLockManager.LOCK.EXCLUSIVE);
      try {
        if (file.isClosed()) {
          OMMapBufferEntry[] notFlushed = EMPTY_BUFFER_ENTRIES;
          for (OMMapBufferEntry entry : mapEntry.getValue()) {
            totalBlocks++;

            if (removeEntry(entry))
              // OK: FLUSHED
              flushedBlocks++;
            else
              // CANNOT FLUSH AWAY
              notFlushed = addEntry(notFlushed, entry);
          }

          if (notFlushed.length == 0) {
            // NO REMAINING BUFFERS TO FLUSH, REMOVE THE ENTIRE FILE ENTRY
            it.remove();
          } else
            // SOME BUFFER CANNOT BE FLUSHED, KEEP ONLY THEM
            mapEntry.setValue(notFlushed);
        } else if (autoFlushUnusedTime > 0) {
          // JUST FLUSH BUFFERS TO DISK
          for (OMMapBufferEntry entry : mapEntry.getValue()) {
            totalBlocks++;
            if (entry.isDirty() && (autoFlushUnusedTime == 0 || now - entry.getLastUsed() > autoFlushUnusedTime)) {
              flushedBlocks++;
              entry.flush();
            }
          }
        }

      } finally {
        lockManager.releaseLock(Thread.currentThread(), file, OLockManager.LOCK.EXCLUSIVE);
      }

    }
    OLogManager.instance().debug(this, "[OMMapManagerNew] flushed %d/%d blocks", flushedBlocks, totalBlocks);
  }

  /**
   * {@inheritDoc}
   * <p/>
   * Removes mapped entries for all existing files.
   */
  public void shutdown() {
    for (Map.Entry<OFileMMap, OMMapBufferEntry[]> entries : bufferPoolPerFile.entrySet()) {
      OFileMMap file;
      file = entries.getKey();
      lockManager.acquireLock(Thread.currentThread(), file, OLockManager.LOCK.EXCLUSIVE);
      try {
        removeFileEntries(entries.getValue());
      } finally {
        lockManager.releaseLock(Thread.currentThread(), file, OLockManager.LOCK.EXCLUSIVE);
      }
    }
    bufferPoolPerFile.clear();
    mapEntrySearchInfo.clear();
  }

  /**
   * This method search in already mapped entries to find if necessary entry is already mapped and can be returned.
   * 
   * @param file
   *          to search mapped entry for it.
   * @param fileEntries
   *          already mapped entries.
   * @param beginOffset
   *          position in file that should be mmaped.
   * @param size
   *          size that should be mmaped.
   * @return {@code EMPTY_BUFFER_ENTRIES} if nothing found or found entries otherwise.
   */
  private OMMapBufferEntry[] searchAmongExisting(OFileMMap file, final OMMapBufferEntry[] fileEntries, final long beginOffset,
      final int size) {
    if (fileEntries.length == 0) {
      return EMPTY_BUFFER_ENTRIES;
    }

    final OMMapBufferEntry lastEntry = fileEntries[fileEntries.length - 1];
    if (lastEntry.beginOffset + lastEntry.size <= beginOffset) {
      return EMPTY_BUFFER_ENTRIES;
    }

    final LastMMapEntrySearchInfo entrySearchInfo = mapEntrySearchInfo.get(file);

    final int beginSearchPosition;
    final int endSearchPosition;

    if (entrySearchInfo == null) {
      beginSearchPosition = 0;
      endSearchPosition = fileEntries.length - 1;
    } else {
      if (entrySearchInfo.requestedPosition <= beginOffset) {
        beginSearchPosition = entrySearchInfo.foundMmapIndex;
        endSearchPosition = fileEntries.length - 1;
      } else {
        beginSearchPosition = 0;
        endSearchPosition = entrySearchInfo.foundMmapIndex;
      }
    }

    final int resultFirstPosition;
    if (endSearchPosition - beginSearchPosition > BINARY_SEARCH_THRESHOLD)
      resultFirstPosition = binarySearch(fileEntries, beginOffset, beginSearchPosition, endSearchPosition);
    else
      resultFirstPosition = linearSearch(fileEntries, beginOffset, beginSearchPosition, endSearchPosition);

    if (beginSearchPosition < 0)
      return EMPTY_BUFFER_ENTRIES;

    int resultLastPosition = fileEntries.length - 1;

    for (int i = resultFirstPosition; i <= resultLastPosition; i++) {
      final OMMapBufferEntry entry = fileEntries[i];
      if (entry.beginOffset + entry.size >= beginOffset + size) {
        resultLastPosition = i;
        break;
      }
    }

    final int length = resultLastPosition - resultFirstPosition + 1;
    final OMMapBufferEntry[] foundEntries = new OMMapBufferEntry[length];

    if (length > 0) {
      System.arraycopy(fileEntries, resultFirstPosition, foundEntries, 0, length);
      mapEntrySearchInfo.put(file, new LastMMapEntrySearchInfo(resultFirstPosition, beginOffset));
    }

    return foundEntries;
  }

  /**
   * This method is used in
   * com.orientechnologies.orient.core.storage.fs.OMMapManagerNew#searchAmongExisting(com.orientechnologies.orient
   * .core.storage.fs.OFileMMap, com.orientechnologies.orient.core.storage.fs.OMMapBufferEntry[], long, int) to search necessary
   * entry.
   * 
   * @param fileEntries
   *          already mapped entries.
   * @param beginOffset
   *          position in file that should be mmaped.
   * @param beginPosition
   *          first position in fileEntries.
   * @param endPosition
   *          last position in fileEntries.
   * @return -1 if entries is not found. Otherwise returns middle position of file entries to perform new binary search step.
   */
  private int binarySearch(OMMapBufferEntry[] fileEntries, long beginOffset, int beginPosition, int endPosition) {
    int midPosition;

    while (beginPosition <= endPosition) {
      midPosition = (beginPosition + endPosition) >>> 1;

      final OMMapBufferEntry entry = fileEntries[midPosition];

      if (entry.beginOffset + entry.size > beginOffset && entry.beginOffset <= beginOffset) {
        // FOUND: USE IT
        return midPosition;
      }

      if (beginPosition == endPosition) {
        // NOT FOUND
        return -1;
      }

      if (beginOffset > entry.beginOffset)
        beginPosition = midPosition + 1;
      else
        endPosition = midPosition;
    }

    // NOT FOUND
    return -1;
  }

  /**
   * This method is used in
   * com.orientechnologies.orient.core.storage.fs.OMMapManagerNew#searchAmongExisting(com.orientechnologies.orient
   * .core.storage.fs.OFileMMap, com.orientechnologies.orient.core.storage.fs.OMMapBufferEntry[], long, int) to search necessary
   * entry.
   * 
   * @param fileEntries
   *          already mapped entries.
   * @param beginOffset
   *          position in file that should be mmaped.
   * @param beginPosition
   *          first position in fileEntries.
   * @param endPosition
   *          last position in fileEntries.
   * @return -1 if entries is not found. Otherwise returns position of necessary file entry.
   */
  private int linearSearch(OMMapBufferEntry[] fileEntries, long beginOffset, int beginPosition, int endPosition) {
    for (int i = beginPosition; i <= endPosition; i++) {
      final OMMapBufferEntry entry = fileEntries[i];
      if (entry.beginOffset + entry.size > beginOffset && entry.beginOffset <= beginOffset) {
        return i;
      }
    }
    return -1;
  }

  /**
   * This method maps new part of file if not all file content is mapped.
   * 
   * @param file
   *          that will be mapped.
   * @param beginOffset
   *          position in file from what mapping should be applied.
   * @return mapped entry.
   * @throws IOException
   *           is thrown if mapping is unsuccessfully.
   */
  private OMMapBufferEntry mapNew(final OFileMMap file, final long beginOffset) throws IOException {
    metricMappedPages++;
    return new OMMapBufferEntry(file, file.map(beginOffset, (int) (file.getFileSize() - beginOffset)), beginOffset,
        (int) (file.getFileSize() - beginOffset));
  }

  private OMMapBufferEntry[] addEntry(final OMMapBufferEntry[] sourceEntries, final OMMapBufferEntry newEntry) {
    final OMMapBufferEntry[] newEntries = new OMMapBufferEntry[sourceEntries.length + 1];
    System.arraycopy(sourceEntries, 0, newEntries, 0, sourceEntries.length);
    newEntries[sourceEntries.length] = newEntry;
    return newEntries;
  }

  /**
   * Locks all entries.
   * 
   * @param entries
   *          that will be locked.
   * @param operationType
   *          determine read or write lock will be performed.
   */
  private void acquireLocksOnEntries(final OMMapBufferEntry[] entries, OPERATION_TYPE operationType) {
    if (operationType == OPERATION_TYPE.WRITE)
      for (OMMapBufferEntry entry : entries) {
        entry.acquireLock();
        entry.setDirty();
      }
    else
      for (OMMapBufferEntry entry : entries)
        entry.acquireLock();
  }

  /**
   * Removes the file.
   */
  public void removeFile(final OFileMMap iFile) {
    lockManager.acquireLock(Thread.currentThread(), iFile, OLockManager.LOCK.EXCLUSIVE);
    try {
      mapEntrySearchInfo.remove(iFile);
      final OMMapBufferEntry[] entries = bufferPoolPerFile.remove(iFile);
      removeFileEntries(entries);
    } finally {
      lockManager.releaseLock(Thread.currentThread(), iFile, OLockManager.LOCK.EXCLUSIVE);
    }
  }

  /**
   * Close one file mapped entry.
   * 
   * @param entry
   *          that will be closed.
   */
  private void closeEntry(OMMapBufferEntry entry) {
    entry.acquireLock();
    try {
      entry.close();
    } finally {
      entry.releaseLock();
    }
  }

  /**
   * Removes all files entries. Flush will be performed before removing.
   * 
   * @param fileEntries
   *          that will be removed.
   */
  private void removeFileEntries(OMMapBufferEntry[] fileEntries) {
    if (fileEntries != null) {
      for (OMMapBufferEntry entry : fileEntries) {
        removeEntry(entry);
      }
    }
  }

  /**
   * Flush and remove one mapped file entry.
   * 
   * @param entry
   *          that will be removed.
   * @return true on success and false otherwise.
   */
  private boolean removeEntry(final OMMapBufferEntry entry) {
    if (!entry.flush()) {
      return false;
    }
    closeEntry(entry);
    return true;
  }

  /**
   * Flushes buffers to disk for a file.
   */
  public boolean flushFile(final OFileMMap iFile) {
    lockManager.acquireLock(Thread.currentThread(), iFile, OLockManager.LOCK.SHARED);
    try {
      boolean allFlushed = true;
      final OMMapBufferEntry[] fileEntries = bufferPoolPerFile.get(iFile);
      if (fileEntries != null) {
        for (OMMapBufferEntry entry : fileEntries) {
          if (!entry.flush())
            allFlushed = false;
        }
      }

      return allFlushed;

    } finally {
      lockManager.releaseLock(Thread.currentThread(), iFile, OLockManager.LOCK.SHARED);
    }
  }

  private static final class LastMMapEntrySearchInfo {
    private final int  foundMmapIndex;
    private final long requestedPosition;

    private LastMMapEntrySearchInfo(int foundMmapIndex, long requestedPosition) {
      this.foundMmapIndex = foundMmapIndex;
      this.requestedPosition = requestedPosition;
    }
  }
}
