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

package com.orientechnologies.orient.core.storage.impl.local.paginated;

import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.hashindex.local.cache.OCacheEntry;
import com.orientechnologies.orient.core.index.hashindex.local.cache.ODiskCache;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperationsManager;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurableComponent;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;

import java.io.IOException;
import java.util.Arrays;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientechnologies.com)
 * @since 10/7/13
 */
public class OClusterPositionMap extends ODurableComponent {
  public static final String DEF_EXTENSION = ".cpm";

  private long               fileId;
  private boolean            useWal;
  private ODiskCache         diskCache;

  public OClusterPositionMap(OAbstractPaginatedStorage storage, String name, boolean useWal) {
    super(storage, name, DEF_EXTENSION);

    acquireExclusiveLock();
    try {
      this.useWal = useWal;
      this.diskCache = storage.getDiskCache();
    } finally {
      releaseExclusiveLock();
    }
  }

  public void setUseWal(boolean useWal) {
    acquireExclusiveLock();
    try {
      this.useWal = useWal;
    } finally {
      releaseExclusiveLock();
    }
  }

  public void open() throws IOException {
    acquireExclusiveLock();
    try {
      OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();
      fileId = openFile(atomicOperation, getFullName(), diskCache);
    } finally {
      releaseExclusiveLock();
    }
  }

  public void create() throws IOException {
    final OAtomicOperation atomicOperation = startAtomicOperation();

    acquireExclusiveLock();
    try {
      fileId = addFile(atomicOperation, getFullName(), diskCache);
      endAtomicOperation(false);
    } catch (IOException ioe) {
      endAtomicOperation(true);
      throw ioe;
    } catch (Exception e) {
      endAtomicOperation(true);
      throw new OStorageException("Error during cluster position - physical position map.", e);
    } finally {
      releaseExclusiveLock();
    }
  }

  public void flush() throws IOException {
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        diskCache.flushFile(fileId);
      } finally {
        releaseSharedLock();
      }
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  public void close(boolean flush) throws IOException {
    acquireExclusiveLock();
    try {
      diskCache.closeFile(fileId, flush);
    } finally {
      releaseExclusiveLock();
    }
  }

  public void truncate() throws IOException {
    final OAtomicOperation atomicOperation = startAtomicOperation();
    acquireExclusiveLock();
    try {
      truncateFile(atomicOperation, fileId, diskCache);
      endAtomicOperation(false);
    } catch (IOException ioe) {
      endAtomicOperation(true);
      throw ioe;
    } catch (Exception e) {
      endAtomicOperation(true);
      throw new OStorageException("Error during truncation of cluster position - physical position map", e);
    } finally {
      releaseExclusiveLock();
    }
  }

  public void delete() throws IOException {
    final OAtomicOperation atomicOperation = startAtomicOperation();

    acquireExclusiveLock();
    try {
      deleteFile(atomicOperation, fileId, diskCache);
      endAtomicOperation(false);
    } catch (IOException ioe) {
      endAtomicOperation(true);
      throw ioe;
    } catch (Exception e) {
      endAtomicOperation(true);
      throw new OStorageException("Error during deletion of cluster position - physical position map.", e);
    } finally {
      releaseExclusiveLock();
    }
  }

  public void rename(String newName) throws IOException {
    startAtomicOperation();
    acquireExclusiveLock();
    try {
      diskCache.renameFile(fileId, getFullName(), newName + getExtension());
      setName(newName);
      endAtomicOperation(false);
    } catch (IOException ioe) {
      endAtomicOperation(true);
      throw ioe;
    } catch (Exception e) {
      endAtomicOperation(true);
      throw new OStorageException("Error during rename of cluster position - physical position map.");
    } finally {
      releaseExclusiveLock();
    }
  }

  public long add(long pageIndex, int recordPosition) throws IOException {
    OAtomicOperation atomicOperation = startAtomicOperation();

    acquireExclusiveLock();
    try {
      long lastPage = getFilledUpTo(atomicOperation, diskCache, fileId) - 1;
      OCacheEntry cacheEntry;
      if (lastPage < 0)
        cacheEntry = addPage(atomicOperation, fileId, diskCache);
      else
        cacheEntry = loadPage(atomicOperation, fileId, lastPage, false, diskCache);

      cacheEntry.acquireExclusiveLock();
      try {

        OClusterPositionMapBucket bucket = new OClusterPositionMapBucket(cacheEntry, getChangesTree(atomicOperation, cacheEntry));
        if (bucket.isFull()) {
          cacheEntry.releaseExclusiveLock();
          releasePage(atomicOperation, cacheEntry, diskCache);

          cacheEntry = addPage(atomicOperation, fileId, diskCache);

          cacheEntry.acquireExclusiveLock();
          bucket = new OClusterPositionMapBucket(cacheEntry, getChangesTree(atomicOperation, cacheEntry));
        }

        final long index = bucket.add(pageIndex, recordPosition);
        final long result = index + cacheEntry.getPageIndex() * OClusterPositionMapBucket.MAX_ENTRIES;

        endAtomicOperation(false);
        return result;
      } catch (Throwable e) {
        endAtomicOperation(true);
        throw new OStorageException("Error during creation of mapping between logical adn physical record position.", e);
      } finally {
        cacheEntry.releaseExclusiveLock();
        releasePage(atomicOperation, cacheEntry, diskCache);
      }
    } finally {
      releaseExclusiveLock();
    }
  }

  public OClusterPositionMapBucket.PositionEntry get(final long clusterPosition) throws IOException {
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        long pageIndex = clusterPosition / OClusterPositionMapBucket.MAX_ENTRIES;
        int index = (int) (clusterPosition % OClusterPositionMapBucket.MAX_ENTRIES);

        final OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();

        if (pageIndex >= getFilledUpTo(atomicOperation, diskCache, fileId))
          return null;

        final OCacheEntry cacheEntry = loadPage(atomicOperation, fileId, pageIndex, false, diskCache);
        try {
          final OClusterPositionMapBucket bucket = new OClusterPositionMapBucket(cacheEntry, getChangesTree(atomicOperation,
              cacheEntry));
          return bucket.get(index);
        } finally {
          releasePage(atomicOperation, cacheEntry, diskCache);
        }
      } finally {
        releaseSharedLock();
      }
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  public OClusterPositionMapBucket.PositionEntry remove(final long clusterPosition) throws IOException {
    OAtomicOperation atomicOperation = startAtomicOperation();

    acquireExclusiveLock();
    try {
      long pageIndex = clusterPosition / OClusterPositionMapBucket.MAX_ENTRIES;
      int index = (int) (clusterPosition % OClusterPositionMapBucket.MAX_ENTRIES);

      final OCacheEntry cacheEntry = loadPage(atomicOperation, fileId, pageIndex, false, diskCache);
      cacheEntry.acquireExclusiveLock();
      try {
        final OClusterPositionMapBucket bucket = new OClusterPositionMapBucket(cacheEntry, getChangesTree(atomicOperation,
            cacheEntry));

        OClusterPositionMapBucket.PositionEntry positionEntry = bucket.remove(index);
        if (positionEntry == null) {
          endAtomicOperation(false);
          return null;
        }

        endAtomicOperation(false);
        return positionEntry;
      } catch (Throwable e) {
        endAtomicOperation(true);

        throw new OStorageException("Error during removal of mapping between logical and physical record position.", e);
      } finally {
        cacheEntry.releaseExclusiveLock();
        releasePage(atomicOperation, cacheEntry, diskCache);
      }
    } finally {
      releaseExclusiveLock();
    }
  }

  public long[] higherPositions(final long clusterPosition) throws IOException {
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        if (clusterPosition == Long.MAX_VALUE)
          return new long[0];

        return ceilingPositions(clusterPosition + 1);
      } finally {
        releaseSharedLock();
      }
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  public long[] ceilingPositions(long clusterPosition) throws IOException {
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        if (clusterPosition < 0)
          clusterPosition = 0;

        long pageIndex = clusterPosition / OClusterPositionMapBucket.MAX_ENTRIES;
        int index = (int) (clusterPosition % OClusterPositionMapBucket.MAX_ENTRIES);

        OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();
        final long filledUpTo = getFilledUpTo(atomicOperation, diskCache, fileId);

        if (pageIndex >= filledUpTo)
          return new long[0];

        long[] result = null;
        do {
          OCacheEntry cacheEntry = loadPage(atomicOperation, fileId, pageIndex, false, diskCache);
          OClusterPositionMapBucket bucket = new OClusterPositionMapBucket(cacheEntry, getChangesTree(atomicOperation, cacheEntry));
          int resultSize = bucket.getSize() - index;

          if (resultSize <= 0) {
            releasePage(atomicOperation, cacheEntry, diskCache);
            pageIndex++;
            index = 0;
          } else {
            int entriesCount = 0;
            long startIndex = cacheEntry.getPageIndex() * OClusterPositionMapBucket.MAX_ENTRIES + index;

            result = new long[resultSize];
            for (int i = 0; i < resultSize; i++) {
              if (bucket.exists(i + index)) {
                result[entriesCount] = startIndex + i;
                entriesCount++;
              }
            }

            if (entriesCount == 0) {
              result = null;
              pageIndex++;
              index = 0;
            } else
              result = Arrays.copyOf(result, entriesCount);

            releasePage(atomicOperation, cacheEntry, diskCache);
          }
        } while (result == null && pageIndex < filledUpTo);

        if (result == null)
          result = new long[0];

        return result;
      } finally {
        releaseSharedLock();
      }
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  public long[] lowerPositions(final long clusterPosition) throws IOException {

    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        if (clusterPosition == 0)
          return new long[0];

        return floorPositions(clusterPosition - 1);
      } finally {
        releaseSharedLock();
      }
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  public long[] floorPositions(final long clusterPosition) throws IOException {
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        if (clusterPosition < 0)
          return new long[0];

        long pageIndex = clusterPosition / OClusterPositionMapBucket.MAX_ENTRIES;
        int index = (int) (clusterPosition % OClusterPositionMapBucket.MAX_ENTRIES);

        OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();

        final long filledUpTo = getFilledUpTo(atomicOperation, diskCache, fileId);
        long[] result;

        if (pageIndex >= filledUpTo) {
          pageIndex = filledUpTo - 1;
          index = Integer.MIN_VALUE;
        }

        if (pageIndex < 0) {
          return new long[0];
        }

        do {
          OCacheEntry cacheEntry = loadPage(atomicOperation, fileId, pageIndex, false, diskCache);
          OClusterPositionMapBucket bucket = new OClusterPositionMapBucket(cacheEntry, getChangesTree(atomicOperation, cacheEntry));
          if (index == Integer.MIN_VALUE)
            index = bucket.getSize() - 1;

          int resultSize = index + 1;
          int entriesCount = 0;

          long startPosition = cacheEntry.getPageIndex() * OClusterPositionMapBucket.MAX_ENTRIES;
          result = new long[resultSize];

          for (int i = 0; i < resultSize; i++) {
            if (bucket.exists(i)) {
              result[entriesCount] = startPosition + i;
              entriesCount++;
            }
          }

          if (entriesCount == 0) {
            result = null;
            pageIndex--;
            index = Integer.MIN_VALUE;
          } else
            result = Arrays.copyOf(result, entriesCount);

          releasePage(atomicOperation, cacheEntry, diskCache);
        } while (result == null && pageIndex >= 0);

        if (result == null)
          result = new long[0];

        return result;
      } finally {
        releaseSharedLock();
      }
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  public long getFirstPosition() throws IOException {
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();

        final long filledUpTo = getFilledUpTo(atomicOperation, diskCache, fileId);
        for (long pageIndex = 0; pageIndex < filledUpTo; pageIndex++) {
          OCacheEntry cacheEntry = loadPage(atomicOperation, fileId, pageIndex, false, diskCache);
          try {
            OClusterPositionMapBucket bucket = new OClusterPositionMapBucket(cacheEntry,
                getChangesTree(atomicOperation, cacheEntry));
            int bucketSize = bucket.getSize();

            for (int index = 0; index < bucketSize; index++) {
              if (bucket.exists(index))
                return pageIndex * OClusterPositionMapBucket.MAX_ENTRIES + index;
            }
          } finally {
            releasePage(atomicOperation, cacheEntry, diskCache);
          }
        }

        return ORID.CLUSTER_POS_INVALID;
      } finally {
        releaseSharedLock();
      }
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  public long getLastPosition() throws IOException {
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();
        final long filledUpTo = getFilledUpTo(atomicOperation, diskCache, fileId);

        for (long pageIndex = filledUpTo - 1; pageIndex >= 0; pageIndex--) {
          OCacheEntry cacheEntry = loadPage(atomicOperation, fileId, pageIndex, false, diskCache);
          try {
            OClusterPositionMapBucket bucket = new OClusterPositionMapBucket(cacheEntry,
                getChangesTree(atomicOperation, cacheEntry));
            final int bucketSize = bucket.getSize();

            for (int index = bucketSize - 1; index >= 0; index--) {
              if (bucket.exists(index))
                return pageIndex * OClusterPositionMapBucket.MAX_ENTRIES + index;
            }
          } finally {
            releasePage(atomicOperation, cacheEntry, diskCache);
          }
        }

        return ORID.CLUSTER_POS_INVALID;
      } finally {
        releaseSharedLock();
      }
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  public boolean wasSoftlyClosed() throws IOException {
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        return diskCache.wasSoftlyClosed(fileId);
      } finally {
        releaseSharedLock();
      }
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }


  @Override
  protected OAtomicOperation startAtomicOperation() throws IOException {
    return atomicOperationsManager.startAtomicOperation(this, !useWal);
  }
}
