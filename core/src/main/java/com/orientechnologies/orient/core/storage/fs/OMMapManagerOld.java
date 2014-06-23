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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.io.OIOException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.profiler.OAbstractProfiler.OProfilerHookValue;
import com.orientechnologies.common.profiler.OProfilerMBean.METRIC_TYPE;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;

public class OMMapManagerOld extends OMMapManagerAbstract implements OMMapManager {

  private static final long                             MIN_MEMORY                       = 50000000;
  private static OMMapManagerOld.OVERLAP_STRATEGY       overlapStrategy;
  private static OMMapManager.ALLOC_STRATEGY            lastStrategy;
  private static int                                    blockSize;
  private static long                                   maxMemory;
  private static long                                   totalMemory;
  private static final ReadWriteLock                    lock                             = new ReentrantReadWriteLock();

  private static long                                   metricUsedChannel                = 0;
  private static long                                   metricReusedPagesBetweenLast     = 0;
  private static long                                   metricReusedPages                = 0;
  private static long                                   metricOverlappedPageUsingChannel = 0;

  private static List<OMMapBufferEntry>                 bufferPoolLRU                    = new ArrayList<OMMapBufferEntry>();
  private static Map<OFileMMap, List<OMMapBufferEntry>> bufferPoolPerFile                = new HashMap<OFileMMap, List<OMMapBufferEntry>>();

  /**
   * Strategy that determine what should manager do if mmapped files overlaps.
   */
  public enum OVERLAP_STRATEGY {
    NO_OVERLAP_USE_CHANNEL, NO_OVERLAP_FLUSH_AND_USE_CHANNEL, OVERLAP
  }

  OMMapManagerOld() {
  }

  public void init() {
    blockSize = OGlobalConfiguration.FILE_MMAP_BLOCK_SIZE.getValueAsInteger();
    maxMemory = OGlobalConfiguration.FILE_MMAP_MAX_MEMORY.getValueAsLong();
    setOverlapStrategy(OGlobalConfiguration.FILE_MMAP_OVERLAP_STRATEGY.getValueAsInteger());

    Orient
        .instance()
        .getProfiler()
        .registerHookValue("system.file.mmap.totalMemory", "Total memory used by memory mapping", METRIC_TYPE.SIZE,
            new OProfilerHookValue() {
              public Object getValue() {
                return totalMemory;
              }
            });

    Orient
        .instance()
        .getProfiler()
        .registerHookValue("system.file.mmap.maxMemory", "Maximum memory usable by memory mapping", METRIC_TYPE.SIZE,
            new OProfilerHookValue() {
              public Object getValue() {
                return maxMemory;
              }
            });

    Orient
        .instance()
        .getProfiler()
        .registerHookValue("system.file.mmap.blockSize", "Total block size used for memory mapping", METRIC_TYPE.SIZE,
            new OProfilerHookValue() {
              public Object getValue() {
                return blockSize;
              }
            });

    Orient
        .instance()
        .getProfiler()
        .registerHookValue("system.file.mmap.blocks", "Total memory used by memory mapping", METRIC_TYPE.COUNTER,
            new OProfilerHookValue() {
              public Object getValue() {
                lock.readLock().lock();
                try {
                  return bufferPoolLRU.size();
                } finally {
                  lock.readLock().unlock();
                }
              }
            });

    Orient
        .instance()
        .getProfiler()
        .registerHookValue("system.file.mmap.alloc.strategy", "Memory mapping allocation strategy", METRIC_TYPE.TEXT,
            new OProfilerHookValue() {
              public Object getValue() {
                return lastStrategy;
              }
            });

    Orient
        .instance()
        .getProfiler()
        .registerHookValue("system.file.mmap.overlap.strategy", "Memory mapping overlapping strategy", METRIC_TYPE.TEXT,
            new OProfilerHookValue() {
              public Object getValue() {
                return overlapStrategy;
              }
            });

    Orient
        .instance()
        .getProfiler()
        .registerHookValue("system.file.mmap.usedChannel",
            "Number of times the memory mapping has been bypassed to use direct file channel", METRIC_TYPE.COUNTER,
            new OProfilerHookValue() {
              public Object getValue() {
                return metricUsedChannel;
              }
            });

    Orient
        .instance()
        .getProfiler()
        .registerHookValue("system.file.mmap.reusedPagesBetweenLast",
            "Number of times a memory mapped page has been reused in short time", METRIC_TYPE.COUNTER, new OProfilerHookValue() {
              public Object getValue() {
                return metricReusedPagesBetweenLast;
              }
            });
    Orient
        .instance()
        .getProfiler()
        .registerHookValue("system.file.mmap.reusedPages", "Number of times a memory mapped page has been reused",
            METRIC_TYPE.COUNTER, new OProfilerHookValue() {
              public Object getValue() {
                return metricReusedPages;
              }
            });
    Orient
        .instance()
        .getProfiler()
        .registerHookValue("system.file.mmap.overlappedPageUsingChannel",
            "Number of times a direct file channel access has been used because overlapping", METRIC_TYPE.COUNTER,
            new OProfilerHookValue() {
              public Object getValue() {
                return metricOverlappedPageUsingChannel;
              }
            });
  }

  public OMMapBufferEntry[] acquire(final OFileMMap iFile, final long iBeginOffset, final int iSize,
      final OMMapManager.OPERATION_TYPE iOperationType, final OMMapManager.ALLOC_STRATEGY iStrategy) {
    return acquire(iFile, iBeginOffset, iSize, false, iOperationType, iStrategy);
  }

  /**
   * Requests a mmap buffer to use.
   * 
   * @param iFile
   *          MMap file
   * @param iBeginOffset
   *          Begin offset
   * @param iSize
   *          Portion size requested
   * @param iForce
   *          Tells if the size is mandatory or can be rounded to the next segment
   * @param iOperationType
   *          READ or WRITE
   * @param iStrategy
   *          used to determine how to use mmap. List of sttrategies is available in {@code OGlobalConfiguration} class.
   * @return The mmap buffer entry if found, or null if the operation is READ and the buffer pool is full.
   */
  private OMMapBufferEntry[] acquire(final OFileMMap iFile, final long iBeginOffset, final int iSize, final boolean iForce,
      final OMMapManager.OPERATION_TYPE iOperationType, final OMMapManager.ALLOC_STRATEGY iStrategy) {

    if (iStrategy == OMMapManager.ALLOC_STRATEGY.MMAP_NEVER)
      return null;

    lock.writeLock().lock();
    try {
      lastStrategy = iStrategy;

      OMMapBufferEntry entry = searchBetweenLastBlocks(iFile, iBeginOffset, iSize);
      try {
        if (entry != null && entry.buffer != null)
          return new OMMapBufferEntry[] { entry };

        // SEARCH THE REQUESTED RANGE IN THE CACHED BUFFERS
        List<OMMapBufferEntry> fileEntries = bufferPoolPerFile.get(iFile);
        if (fileEntries == null) {
          fileEntries = new ArrayList<OMMapBufferEntry>();
          bufferPoolPerFile.put(iFile, fileEntries);
        }

        int position = searchEntry(fileEntries, iBeginOffset, iSize);
        if (position > -1) {
          // FOUND !!!
          entry = fileEntries.get(position);
          if (entry != null && entry.buffer != null)
            return new OMMapBufferEntry[] { entry };
        }

        int p = (position + 2) * -1;

        // CHECK IF THERE IS A BUFFER THAT OVERLAPS
        if (!allocIfOverlaps(iBeginOffset, iSize, fileEntries, p)) {
          metricUsedChannel++;
          return null;
        }

        int bufferSize = computeBestEntrySize(iFile, iBeginOffset, iSize, iForce, fileEntries, p);

        if (totalMemory + bufferSize > maxMemory
            && (iStrategy == OMMapManager.ALLOC_STRATEGY.MMAP_ONLY_AVAIL_POOL || iOperationType == OMMapManager.OPERATION_TYPE.READ
                && iStrategy == OMMapManager.ALLOC_STRATEGY.MMAP_WRITE_ALWAYS_READ_IF_AVAIL_POOL)) {
          metricUsedChannel++;
          return null;
        }

        entry = null;
        // FREE LESS-USED BUFFERS UNTIL THE FREE-MEMORY IS DOWN THE CONFIGURED MAX LIMIT
        do {
          if (totalMemory + bufferSize > maxMemory)
            freeResources();

          // RECOMPUTE THE POSITION AFTER REMOVING
          fileEntries = bufferPoolPerFile.get(iFile);
          position = searchEntry(fileEntries, iBeginOffset, iSize);
          if (position > -1) {
            // FOUND: THIS IS PRETTY STRANGE SINCE IT WASN'T FOUND!
            entry = fileEntries.get(position);
            if (entry != null && entry.buffer != null)
              return new OMMapBufferEntry[] { entry };
          }

          // LOAD THE PAGE
          try {
            entry = mapBuffer(iFile, iBeginOffset, bufferSize);
          } catch (IllegalArgumentException e) {
            throw e;
          } catch (Exception e) {
            // REDUCE MAX MEMORY TO FORCE EMPTY BUFFERS
            maxMemory = maxMemory * 90 / 100;
            OLogManager.instance().warn(OMMapManagerOld.class, "Memory mapping error, try to reduce max memory to %d and retry...",
                e, maxMemory);
          }
        } while (entry == null && maxMemory > MIN_MEMORY);

        if (entry == null || !entry.isValid())
          throw new OIOException("You cannot access to the file portion " + iBeginOffset + "-" + iBeginOffset + iSize + " bytes");

        totalMemory += bufferSize;
        bufferPoolLRU.add(entry);

        p = (position + 2) * -1;
        if (p < 0)
          p = 0;

        if (fileEntries == null) {
          // IN CASE THE CLEAN HAS REMOVED THE LIST
          fileEntries = new ArrayList<OMMapBufferEntry>();
          bufferPoolPerFile.put(iFile, fileEntries);
        }

        fileEntries.add(p, entry);

        if (entry != null && entry.buffer != null)
          return new OMMapBufferEntry[] { entry };

      } finally {
        if (entry != null) {
          entry.acquireLock();

          if (iOperationType == OMMapManager.OPERATION_TYPE.WRITE)
            entry.setDirty();
        }
      }

      return null;
    } finally {
      lock.writeLock().unlock();
    }
  }

  private static void freeResources() {
    final long memoryThreshold = (long) (maxMemory * 0.75);
    final long startingMemory = totalMemory;

    if (OLogManager.instance().isDebugEnabled())
      OLogManager.instance().debug(null, "Freeing off-heap memory as mmmap blocks, target is %s...",
          OFileUtils.getSizeAsString(startingMemory - memoryThreshold));

    // SORT AS LRU, FIRST = MOST USED
    Collections.sort(bufferPoolLRU, new Comparator<OMMapBufferEntry>() {
      public int compare(final OMMapBufferEntry o1, final OMMapBufferEntry o2) {
        return (int) (o1.getLastUsed() - o2.getLastUsed());
      }
    });

    // REMOVE THE LESS USED ENTRY AND UPDATE THE TOTAL MEMORY
    for (Iterator<OMMapBufferEntry> it = bufferPoolLRU.iterator(); it.hasNext();) {
      final OMMapBufferEntry entry = it.next();

      // REMOVE FROM COLLECTIONS
      if (removeEntry(entry))
        it.remove();

      if (totalMemory < memoryThreshold)
        break;
    }

    if (OLogManager.instance().isDebugEnabled())
      OLogManager.instance().debug(null, "Freed off-heap memory as mmmap blocks for %s...",
          OFileUtils.getSizeAsString(startingMemory - totalMemory));
  }

  private static OMMapBufferEntry searchBetweenLastBlocks(final OFileMMap iFile, final long iBeginOffset, final int iSize) {
    if (!bufferPoolLRU.isEmpty()) {
      // SEARCH IF IT'S BETWEEN THE LAST 5 BLOCK USED: THIS IS THE COMMON CASE ON MASSIVE INSERTION
      final int min = Math.max(bufferPoolLRU.size() - 5, -1);
      for (int i = bufferPoolLRU.size() - 1; i > min; --i) {
        final OMMapBufferEntry e = bufferPoolLRU.get(i);

        if (e.isValid() && e.file == iFile && iBeginOffset >= e.beginOffset && iBeginOffset + iSize <= e.beginOffset + e.size) {
          // FOUND: USE IT
          metricReusedPagesBetweenLast++;
          e.updateLastUsedTime();
          return e;
        }
      }
    }
    return null;
  }

  /**
   * Flushes away all the buffers of closed files. This frees the memory.
   */
  public void flush() {
    lock.writeLock().lock();
    try {
      OMMapBufferEntry entry;
      for (Iterator<OMMapBufferEntry> it = bufferPoolLRU.iterator(); it.hasNext();) {
        entry = it.next();
        if (entry.file != null && entry.file.isClosed()) {
          if (removeEntry(entry))
            it.remove();
        }
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  protected static boolean removeEntry(final OMMapBufferEntry entry) {
    if (!entry.flush())
      return false;

    entry.acquireLock();
    try {
      // COMMITTED: REMOVE IT
      final List<OMMapBufferEntry> file = bufferPoolPerFile.get(entry.file);
      if (file != null) {
        file.remove(entry);
        if (file.isEmpty())
          bufferPoolPerFile.remove(entry.file);
      }
      entry.close();

      totalMemory -= entry.size;
      return true;

    } finally {
      entry.releaseLock();
    }
  }

  /**
   * Removes the file.
   */
  public void removeFile(final OFileMMap iFile) {
    lock.writeLock().lock();

    try {
      final List<OMMapBufferEntry> entries = bufferPoolPerFile.remove(iFile);
      if (entries != null) {
        for (OMMapBufferEntry entry : entries) {
          bufferPoolLRU.remove(entry);
          removeEntry(entry);
        }
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Flushes all the buffers of the passed file.
   * 
   * @param iFile
   *          file to flush on disk.
   */
  public boolean flushFile(final OFileMMap iFile) {
    lock.readLock().lock();
    try {
      final List<OMMapBufferEntry> entries = bufferPoolPerFile.get(iFile);
      boolean allFlushed = true;
      if (entries != null)
        for (OMMapBufferEntry entry : entries)
          if (!entry.flush())
            allFlushed = false;

      return allFlushed;
    } finally {
      lock.readLock().unlock();
    }
  }

  public void shutdown() {
    lock.writeLock().lock();
    try {
      for (OMMapBufferEntry entry : new ArrayList<OMMapBufferEntry>(bufferPoolLRU))
        removeEntry(entry);

      bufferPoolLRU.clear();
      bufferPoolPerFile.clear();
      totalMemory = 0;
    } finally {
      lock.writeLock().unlock();
    }
  }

  public long getMaxMemory() {
    return maxMemory;
  }

  public static void setMaxMemory(final long iMaxMemory) {
    maxMemory = iMaxMemory;
  }

  public long getTotalMemory() {
    return totalMemory;
  }

  public int getBlockSize() {
    return blockSize;
  }

  public static void setBlockSize(final int blockSize) {
    OMMapManagerOld.blockSize = blockSize;
  }

  public OVERLAP_STRATEGY getOverlapStrategy() {
    return overlapStrategy;
  }

  public static void setOverlapStrategy(int overlapStrategy) {
    OMMapManagerOld.overlapStrategy = OVERLAP_STRATEGY.values()[overlapStrategy];
  }

  public void setOverlapStrategy(OVERLAP_STRATEGY overlapStrategy) {
    OMMapManagerOld.overlapStrategy = overlapStrategy;
  }

  public int getOverlappedBlocks() {
    lock.readLock().lock();
    try {
      int count = 0;
      for (OFileMMap f : bufferPoolPerFile.keySet()) {
        count += getOverlappedBlocks(f);
      }
      return count;
    } finally {
      lock.readLock().unlock();
    }
  }

  private int getOverlappedBlocks(final OFileMMap iFile) {
    lock.readLock().lock();
    try {
      int count = 0;

      final List<OMMapBufferEntry> blocks = bufferPoolPerFile.get(iFile);
      long lastPos = -1;
      for (OMMapBufferEntry block : blocks) {
        if (lastPos > -1 && lastPos > block.beginOffset) {
          OLogManager.instance().warn(null, "Found overlapped block for file %s at position %d. Previous offset+size was %d",
              iFile, block.beginOffset, lastPos);
          count++;
        }

        lastPos = block.beginOffset + block.size;
      }
      return count;
    } finally {
      lock.readLock().unlock();
    }
  }

  private static OMMapBufferEntry mapBuffer(final OFileMMap iFile, final long iBeginOffset, final int iSize) throws IOException {
    long timer = Orient.instance().getProfiler().startChrono();
    try {
      return new OMMapBufferEntry(iFile, iFile.map(iBeginOffset, iSize), iBeginOffset, iSize);
    } finally {
      Orient.instance().getProfiler().stopChrono("OMMapManager.loadPage", "Load a memory mapped page in memory", timer);
    }
  }

  /**
   * Search for a buffer in the ordered list.
   * 
   * @param fileEntries
   *          to search necessary record.
   * @param iBeginOffset
   *          file offset to start search from it.
   * @param iSize
   *          that will be contained in founded entries.
   * @return negative number means not found. The position to insert is the (return value +1)*-1. Zero or positive number is the
   *         found position.
   */
  private static int searchEntry(final List<OMMapBufferEntry> fileEntries, final long iBeginOffset, final int iSize) {
    if (fileEntries == null || fileEntries.size() == 0)
      return -1;

    int high = fileEntries.size() - 1;
    if (high < 0)
      // NOT FOUND
      return -1;

    int low = 0;
    int mid = -1;

    // BINARY SEARCH
    OMMapBufferEntry e;

    while (low <= high) {
      mid = (low + high) >>> 1;
      e = fileEntries.get(mid);

      if (iBeginOffset >= e.beginOffset && iBeginOffset + iSize <= e.beginOffset + e.size) {
        // FOUND: USE IT
        metricReusedPages++;
        e.updateLastUsedTime();
        return mid;
      }

      if (low == high) {
        if (iBeginOffset > e.beginOffset)
          // NEXT POSITION
          low++;

        // NOT FOUND
        return (low + 2) * -1;
      }

      if (iBeginOffset >= e.beginOffset)
        low = mid + 1;
      else
        high = mid;
    }

    // NOT FOUND
    return mid;
  }

  private static boolean allocIfOverlaps(final long iBeginOffset, final int iSize, final List<OMMapBufferEntry> fileEntries,
      final int p) {
    if (overlapStrategy == OVERLAP_STRATEGY.OVERLAP)
      return true;

    boolean overlaps = false;
    OMMapBufferEntry entry = null;
    if (p > 0) {
      // CHECK LOWER OFFSET
      entry = fileEntries.get(p - 1);
      overlaps = entry.beginOffset <= iBeginOffset && entry.beginOffset + entry.size >= iBeginOffset;
    }

    if (!overlaps && p < fileEntries.size() - 1) {
      // CHECK HIGHER OFFSET
      entry = fileEntries.get(p);
      overlaps = iBeginOffset + iSize >= entry.beginOffset;
    }

    if (overlaps) {
      // READ NOT IN BUFFER POOL: RETURN NULL TO LET TO THE CALLER TO EXECUTE A DIRECT READ WITHOUT MMAP
      metricOverlappedPageUsingChannel++;
      if (overlapStrategy == OVERLAP_STRATEGY.NO_OVERLAP_FLUSH_AND_USE_CHANNEL)
        entry.flush();
      return false;
    }

    return true;
  }

  private static int computeBestEntrySize(final OFileMMap iFile, final long iBeginOffset, final int iSize, final boolean iForce,
      List<OMMapBufferEntry> fileEntries, int p) {
    int bufferSize;
    if (p > -1 && p < fileEntries.size()) {
      // GET NEXT ENTRY AS SIZE LIMIT
      bufferSize = (int) (fileEntries.get(p).beginOffset - iBeginOffset);
      if (bufferSize < iSize)
        // ROUND TO THE BUFFER SIZE
        bufferSize = iSize;

      if (bufferSize < blockSize)
        bufferSize = blockSize;
    } else {
      // ROUND TO THE BUFFER SIZE
      bufferSize = iForce ? iSize : iSize < blockSize ? blockSize : iSize;

      if (iBeginOffset + bufferSize > iFile.getFileSize())
        // REQUESTED BUFFER IS TOO LARGE: GET AS MAXIMUM AS POSSIBLE
        bufferSize = (int) (iFile.getFileSize() - iBeginOffset);
    }

    if (bufferSize <= 0)
      throw new IllegalArgumentException("Invalid range requested for file " + iFile + ". Requested " + iSize
          + " bytes from the address " + iBeginOffset + " while the total file size is " + iFile.getFileSize());

    return bufferSize;
  }
}
