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

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.util.OCommonConst;
import com.orientechnologies.orient.core.exception.OClusterPositionMapException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurableComponent;

import java.io.IOException;
import java.util.Arrays;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientechnologies.com)
 * @since 10/7/13
 */
public class OClusterPositionMap extends ODurableComponent {
  public static final String DEF_EXTENSION = ".cpm";
  private long               fileId;

  public OClusterPositionMap(OAbstractPaginatedStorage storage, String name, String lockName) {
    super(storage, name, DEF_EXTENSION, lockName);
  }

  public void open() throws IOException {
    startOperation();
    try {
      acquireExclusiveLock();
      try {
        OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();
        fileId = openFile(atomicOperation, getFullName());
      } finally {
        releaseExclusiveLock();
      }
    } finally {
      completeOperation();
    }
  }

  public void create() throws IOException {
    startOperation();
    try {
      final OAtomicOperation atomicOperation = startAtomicOperation(false);

      acquireExclusiveLock();
      try {
        fileId = addFile(atomicOperation, getFullName());
        endAtomicOperation(false, null);
      } catch (IOException ioe) {
        endAtomicOperation(true, ioe);
        throw ioe;
      } catch (Exception e) {
        endAtomicOperation(true, e);
        throw OException
            .wrapException(new OClusterPositionMapException("Error during cluster position - physical position map", this), e);
      } finally {
        releaseExclusiveLock();
      }
    } finally {
      completeOperation();
    }
  }

  public void flush() throws IOException {
    startOperation();
    try {
      atomicOperationsManager.acquireReadLock(this);
      try {
        acquireSharedLock();
        try {
          writeCache.flush(fileId);
        } finally {
          releaseSharedLock();
        }
      } finally {
        atomicOperationsManager.releaseReadLock(this);
      }
    } finally {
      completeOperation();
    }
  }

  public void close(boolean flush) throws IOException {
    startOperation();
    try {
      acquireExclusiveLock();
      try {
        readCache.closeFile(fileId, flush, writeCache);
      } finally {
        releaseExclusiveLock();
      }
    } finally {
      completeOperation();
    }
  }

  public void truncate() throws IOException {
    startOperation();
    try {
      final OAtomicOperation atomicOperation = startAtomicOperation(true);
      acquireExclusiveLock();
      try {
        truncateFile(atomicOperation, fileId);
        endAtomicOperation(false, null);
      } catch (IOException ioe) {
        endAtomicOperation(true, ioe);
        throw ioe;
      } catch (Exception e) {
        endAtomicOperation(true, e);
        throw OException.wrapException(
            new OClusterPositionMapException("Error during truncation of cluster position - physical position map", this), e);
      } finally {
        releaseExclusiveLock();
      }
    } finally {
      completeOperation();
    }
  }

  public void delete() throws IOException {
    startOperation();
    try {
      final OAtomicOperation atomicOperation = startAtomicOperation(false);

      acquireExclusiveLock();
      try {
        deleteFile(atomicOperation, fileId);
        endAtomicOperation(false, null);
      } catch (IOException ioe) {
        endAtomicOperation(true, ioe);
        throw ioe;
      } catch (Exception e) {
        endAtomicOperation(true, e);
        throw OException.wrapException(
            new OClusterPositionMapException("Error during deletion of cluster position - physical position map", this), e);
      } finally {
        releaseExclusiveLock();
      }
    } finally {
      completeOperation();
    }
  }

  public void rename(String newName) throws IOException {
    startOperation();
    try {
      startAtomicOperation(true);
      acquireExclusiveLock();
      try {
        writeCache.renameFile(fileId, getFullName(), newName + getExtension());
        setName(newName);
        endAtomicOperation(false, null);
      } catch (IOException ioe) {
        endAtomicOperation(true, ioe);
        throw ioe;
      } catch (Exception e) {
        endAtomicOperation(true, e);
        throw OException.wrapException(
            new OClusterPositionMapException("Error during rename of cluster position - physical position map", this), e);
      } finally {
        releaseExclusiveLock();
      }
    } finally {
      completeOperation();
    }
  }

  public long add(long pageIndex, int recordPosition) throws IOException {
    startOperation();
    try {
      OAtomicOperation atomicOperation = startAtomicOperation(true);

      acquireExclusiveLock();
      try {
        long lastPage = getFilledUpTo(atomicOperation, fileId) - 1;
        OCacheEntry cacheEntry;
        if (lastPage < 0)
          cacheEntry = addPage(atomicOperation, fileId);
        else
          cacheEntry = loadPage(atomicOperation, fileId, lastPage, false, 1);

        cacheEntry.acquireExclusiveLock();
        try {

          OClusterPositionMapBucket bucket = new OClusterPositionMapBucket(cacheEntry, getChanges(atomicOperation, cacheEntry));
          if (bucket.isFull()) {
            cacheEntry.releaseExclusiveLock();
            releasePage(atomicOperation, cacheEntry);

            cacheEntry = addPage(atomicOperation, fileId);

            cacheEntry.acquireExclusiveLock();
            bucket = new OClusterPositionMapBucket(cacheEntry, getChanges(atomicOperation, cacheEntry));
          }

          final long index = bucket.add(pageIndex, recordPosition);
          final long result = index + cacheEntry.getPageIndex() * OClusterPositionMapBucket.MAX_ENTRIES;

          endAtomicOperation(false, null);
          return result;
        } catch (Exception e) {
          endAtomicOperation(true, e);
          throw OException.wrapException(new OClusterPositionMapException(
              "Error during creation of mapping between logical and physical record position", this), e);
        } finally {
          cacheEntry.releaseExclusiveLock();
          releasePage(atomicOperation, cacheEntry);
        }
      } finally {
        releaseExclusiveLock();
      }
    } finally {
      completeOperation();
    }
  }

  public long allocate() throws IOException {
    startOperation();
    try {
      OAtomicOperation atomicOperation = startAtomicOperation(true);

      acquireExclusiveLock();
      try {
        long lastPage = getFilledUpTo(atomicOperation, fileId) - 1;
        OCacheEntry cacheEntry;
        if (lastPage < 0)
          cacheEntry = addPage(atomicOperation, fileId);
        else
          cacheEntry = loadPage(atomicOperation, fileId, lastPage, false, 1);

        cacheEntry.acquireExclusiveLock();
        try {

          OClusterPositionMapBucket bucket = new OClusterPositionMapBucket(cacheEntry, getChanges(atomicOperation, cacheEntry));
          if (bucket.isFull()) {
            cacheEntry.releaseExclusiveLock();
            releasePage(atomicOperation, cacheEntry);

            cacheEntry = addPage(atomicOperation, fileId);

            cacheEntry.acquireExclusiveLock();
            bucket = new OClusterPositionMapBucket(cacheEntry, getChanges(atomicOperation, cacheEntry));
          }

          final long index = bucket.allocate();
          final long result = index + cacheEntry.getPageIndex() * OClusterPositionMapBucket.MAX_ENTRIES;

          endAtomicOperation(false, null);
          return result;
        } catch (Exception e) {
          endAtomicOperation(true, e);
          throw OException.wrapException(new OClusterPositionMapException(
              "Error during creation of mapping between logical and physical record position", this), e);
        } finally {
          cacheEntry.releaseExclusiveLock();
          releasePage(atomicOperation, cacheEntry);
        }
      } finally {
        releaseExclusiveLock();
      }
    } finally {
      completeOperation();
    }
  }

  public void update(final long clusterPosition, final OClusterPositionMapBucket.PositionEntry entry) throws IOException {
    startOperation();
    try {
      OAtomicOperation atomicOperation = startAtomicOperation(true);

      acquireExclusiveLock();
      try {
        final long pageIndex = clusterPosition / OClusterPositionMapBucket.MAX_ENTRIES;
        final int index = (int) (clusterPosition % OClusterPositionMapBucket.MAX_ENTRIES);

        if (pageIndex >= getFilledUpTo(atomicOperation, fileId))
          throw new OClusterPositionMapException(
              "Passed in cluster position " + clusterPosition + " is outside of range of cluster-position map", this);

        final OCacheEntry cacheEntry = loadPage(atomicOperation, fileId, pageIndex, false, 1);
        cacheEntry.acquireExclusiveLock();
        try {
          final OClusterPositionMapBucket bucket = new OClusterPositionMapBucket(cacheEntry,
              getChanges(atomicOperation, cacheEntry));
          bucket.set(index, entry);
        } finally {
          cacheEntry.releaseExclusiveLock();
          releasePage(atomicOperation, cacheEntry);
        }

        endAtomicOperation(false, null);
      } catch (IOException e) {
        endAtomicOperation(true, e);
        throw OException.wrapException(
            new OClusterPositionMapException("Error of update of mapping between logical and physical record position", this), e);
      } catch (RuntimeException e) {
        endAtomicOperation(true, e);
        throw OException.wrapException(
            new OClusterPositionMapException("Error of update of mapping between logical and physical record position", this), e);
      } finally {
        releaseExclusiveLock();
      }
    } finally {
      completeOperation();
    }
  }

  public void resurrect(final long clusterPosition, final OClusterPositionMapBucket.PositionEntry entry) throws IOException {
    startOperation();
    try {
      OAtomicOperation atomicOperation = startAtomicOperation(true);

      acquireExclusiveLock();
      try {
        final long pageIndex = clusterPosition / OClusterPositionMapBucket.MAX_ENTRIES;
        final int index = (int) (clusterPosition % OClusterPositionMapBucket.MAX_ENTRIES);

        if (pageIndex >= getFilledUpTo(atomicOperation, fileId))
          throw new OClusterPositionMapException(
              "Passed in cluster position " + clusterPosition + " is outside of range of cluster-position map", this);

        final OCacheEntry cacheEntry = loadPage(atomicOperation, fileId, pageIndex, false, 1);
        cacheEntry.acquireExclusiveLock();
        try {
          final OClusterPositionMapBucket bucket = new OClusterPositionMapBucket(cacheEntry,
              getChanges(atomicOperation, cacheEntry));
          bucket.resurrect(index, entry);
        } finally {
          cacheEntry.releaseExclusiveLock();
          releasePage(atomicOperation, cacheEntry);
        }

        endAtomicOperation(false, null);
      } catch (IOException e) {
        endAtomicOperation(true, e);
        throw OException.wrapException(
            new OClusterPositionMapException("Error of resurrecting mapping between logical and physical record position", this),
            e);
      } catch (RuntimeException e) {
        endAtomicOperation(true, e);
        throw OException.wrapException(
            new OClusterPositionMapException("Error of resurrecting mapping between logical and physical record position", this),
            e);
      } finally {
        releaseExclusiveLock();
      }
    } finally {
      completeOperation();
    }
  }

  public OClusterPositionMapBucket.PositionEntry get(final long clusterPosition, final int pageCount) throws IOException {
    startOperation();
    try {
      atomicOperationsManager.acquireReadLock(this);
      try {
        acquireSharedLock();
        try {
          long pageIndex = clusterPosition / OClusterPositionMapBucket.MAX_ENTRIES;
          int index = (int) (clusterPosition % OClusterPositionMapBucket.MAX_ENTRIES);

          final OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();

          if (pageIndex >= getFilledUpTo(atomicOperation, fileId))
            return null;

          final OCacheEntry cacheEntry = loadPage(atomicOperation, fileId, pageIndex, false, pageCount);
          cacheEntry.acquireSharedLock();
          try {
            final OClusterPositionMapBucket bucket = new OClusterPositionMapBucket(cacheEntry,
                getChanges(atomicOperation, cacheEntry));
            return bucket.get(index);
          } finally {
            cacheEntry.releaseSharedLock();
            releasePage(atomicOperation, cacheEntry);
          }
        } finally {
          releaseSharedLock();
        }
      } finally {
        atomicOperationsManager.releaseReadLock(this);
      }
    } finally {
      completeOperation();
    }
  }

  public OClusterPositionMapBucket.PositionEntry remove(final long clusterPosition) throws IOException {
    startOperation();
    try {
      OAtomicOperation atomicOperation = startAtomicOperation(true);

      acquireExclusiveLock();
      try {
        long pageIndex = clusterPosition / OClusterPositionMapBucket.MAX_ENTRIES;
        int index = (int) (clusterPosition % OClusterPositionMapBucket.MAX_ENTRIES);

        final OCacheEntry cacheEntry = loadPage(atomicOperation, fileId, pageIndex, false, 1);
        cacheEntry.acquireExclusiveLock();
        try {
          final OClusterPositionMapBucket bucket = new OClusterPositionMapBucket(cacheEntry,
              getChanges(atomicOperation, cacheEntry));

          OClusterPositionMapBucket.PositionEntry positionEntry = bucket.remove(index);
          if (positionEntry == null) {
            endAtomicOperation(false, null);
            return null;
          }

          endAtomicOperation(false, null);
          return positionEntry;
        } catch (Exception e) {
          endAtomicOperation(true, e);

          throw OException.wrapException(new OClusterPositionMapException(
              "Error during removal of mapping between logical and physical record position", this), e);
        } finally {
          cacheEntry.releaseExclusiveLock();
          releasePage(atomicOperation, cacheEntry);
        }
      } finally {
        releaseExclusiveLock();
      }
    } finally {
      completeOperation();
    }
  }

  public long[] higherPositions(final long clusterPosition) throws IOException {
    startOperation();
    try {
      atomicOperationsManager.acquireReadLock(this);
      try {
        acquireSharedLock();
        try {
          if (clusterPosition == Long.MAX_VALUE)
            return OCommonConst.EMPTY_LONG_ARRAY;

          return ceilingPositions(clusterPosition + 1);
        } finally {
          releaseSharedLock();
        }
      } finally {
        atomicOperationsManager.releaseReadLock(this);
      }
    } finally {
      completeOperation();
    }
  }

  public long[] ceilingPositions(long clusterPosition) throws IOException {
    startOperation();
    try {
      atomicOperationsManager.acquireReadLock(this);
      try {
        acquireSharedLock();
        try {
          if (clusterPosition < 0)
            clusterPosition = 0;

          long pageIndex = clusterPosition / OClusterPositionMapBucket.MAX_ENTRIES;
          int index = (int) (clusterPosition % OClusterPositionMapBucket.MAX_ENTRIES);

          OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();
          final long filledUpTo = getFilledUpTo(atomicOperation, fileId);

          if (pageIndex >= filledUpTo)
            return OCommonConst.EMPTY_LONG_ARRAY;

          long[] result = null;
          do {
            OCacheEntry cacheEntry = loadPage(atomicOperation, fileId, pageIndex, false, 1);
            cacheEntry.acquireSharedLock();

            OClusterPositionMapBucket bucket = new OClusterPositionMapBucket(cacheEntry, getChanges(atomicOperation, cacheEntry));
            int resultSize = bucket.getSize() - index;

            if (resultSize <= 0) {
              cacheEntry.releaseSharedLock();
              releasePage(atomicOperation, cacheEntry);
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

              cacheEntry.releaseSharedLock();
              releasePage(atomicOperation, cacheEntry);
            }
          } while (result == null && pageIndex < filledUpTo);

          if (result == null)
            result = OCommonConst.EMPTY_LONG_ARRAY;

          return result;
        } finally {
          releaseSharedLock();
        }
      } finally {
        atomicOperationsManager.releaseReadLock(this);
      }
    } finally {
      completeOperation();
    }
  }

  public long[] lowerPositions(final long clusterPosition) throws IOException {
    startOperation();
    try {
      atomicOperationsManager.acquireReadLock(this);
      try {
        acquireSharedLock();
        try {
          if (clusterPosition == 0)
            return OCommonConst.EMPTY_LONG_ARRAY;

          return floorPositions(clusterPosition - 1);
        } finally {
          releaseSharedLock();
        }
      } finally {
        atomicOperationsManager.releaseReadLock(this);
      }
    } finally {
      completeOperation();
    }
  }

  public long[] floorPositions(final long clusterPosition) throws IOException {
    startOperation();
    try {
      atomicOperationsManager.acquireReadLock(this);
      try {
        acquireSharedLock();
        try {
          if (clusterPosition < 0)
            return OCommonConst.EMPTY_LONG_ARRAY;

          long pageIndex = clusterPosition / OClusterPositionMapBucket.MAX_ENTRIES;
          int index = (int) (clusterPosition % OClusterPositionMapBucket.MAX_ENTRIES);

          OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();

          final long filledUpTo = getFilledUpTo(atomicOperation, fileId);
          long[] result;

          if (pageIndex >= filledUpTo) {
            pageIndex = filledUpTo - 1;
            index = Integer.MIN_VALUE;
          }

          if (pageIndex < 0) {
            return OCommonConst.EMPTY_LONG_ARRAY;
          }

          do {
            OCacheEntry cacheEntry = loadPage(atomicOperation, fileId, pageIndex, false, 1);
            cacheEntry.acquireSharedLock();

            OClusterPositionMapBucket bucket = new OClusterPositionMapBucket(cacheEntry, getChanges(atomicOperation, cacheEntry));
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

            cacheEntry.releaseSharedLock();
            releasePage(atomicOperation, cacheEntry);
          } while (result == null && pageIndex >= 0);

          if (result == null)
            result = OCommonConst.EMPTY_LONG_ARRAY;

          return result;
        } finally {
          releaseSharedLock();
        }
      } finally {
        atomicOperationsManager.releaseReadLock(this);
      }
    } finally {
      completeOperation();
    }
  }

  public long getFirstPosition() throws IOException {
    startOperation();
    try {
      atomicOperationsManager.acquireReadLock(this);
      try {
        acquireSharedLock();
        try {
          OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();

          final long filledUpTo = getFilledUpTo(atomicOperation, fileId);
          for (long pageIndex = 0; pageIndex < filledUpTo; pageIndex++) {
            OCacheEntry cacheEntry = loadPage(atomicOperation, fileId, pageIndex, false, 1);
            cacheEntry.acquireSharedLock();
            try {
              OClusterPositionMapBucket bucket = new OClusterPositionMapBucket(cacheEntry, getChanges(atomicOperation, cacheEntry));
              int bucketSize = bucket.getSize();

              for (int index = 0; index < bucketSize; index++) {
                if (bucket.exists(index))
                  return pageIndex * OClusterPositionMapBucket.MAX_ENTRIES + index;
              }
            } finally {
              cacheEntry.releaseSharedLock();
              releasePage(atomicOperation, cacheEntry);
            }
          }

          return ORID.CLUSTER_POS_INVALID;
        } finally {
          releaseSharedLock();
        }
      } finally {
        atomicOperationsManager.releaseReadLock(this);
      }
    } finally {
      completeOperation();
    }
  }

  public byte getStatus(final long clusterPosition) throws IOException {
    startOperation();
    try {
      atomicOperationsManager.acquireReadLock(this);
      try {
        acquireSharedLock();
        try {
          final long pageIndex = clusterPosition / OClusterPositionMapBucket.MAX_ENTRIES;
          final int index = (int) (clusterPosition % OClusterPositionMapBucket.MAX_ENTRIES);

          final OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();

          if (pageIndex >= getFilledUpTo(atomicOperation, fileId))
            return OClusterPositionMapBucket.NOT_EXISTENT;

          final OCacheEntry cacheEntry = loadPage(atomicOperation, fileId, pageIndex, false, 1);
          cacheEntry.acquireSharedLock();
          try {
            final OClusterPositionMapBucket bucket = new OClusterPositionMapBucket(cacheEntry,
                getChanges(atomicOperation, cacheEntry));

            return bucket.getStatus(index);

          } finally {
            cacheEntry.releaseSharedLock();
            releasePage(atomicOperation, cacheEntry);
          }
        } finally {
          releaseSharedLock();
        }
      } finally {
        atomicOperationsManager.releaseReadLock(this);
      }
    } finally {
      completeOperation();
    }
  }

  public long getLastPosition() throws IOException {
    startOperation();
    try {
      atomicOperationsManager.acquireReadLock(this);
      try {
        acquireSharedLock();
        try {
          OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();
          final long filledUpTo = getFilledUpTo(atomicOperation, fileId);

          for (long pageIndex = filledUpTo - 1; pageIndex >= 0; pageIndex--) {
            OCacheEntry cacheEntry = loadPage(atomicOperation, fileId, pageIndex, false, 1);
            cacheEntry.acquireSharedLock();
            try {
              OClusterPositionMapBucket bucket = new OClusterPositionMapBucket(cacheEntry, getChanges(atomicOperation, cacheEntry));
              final int bucketSize = bucket.getSize();

              for (int index = bucketSize - 1; index >= 0; index--) {
                if (bucket.exists(index))
                  return pageIndex * OClusterPositionMapBucket.MAX_ENTRIES + index;
              }
            } finally {
              cacheEntry.releaseSharedLock();
              releasePage(atomicOperation, cacheEntry);
            }
          }

          return ORID.CLUSTER_POS_INVALID;
        } finally {
          releaseSharedLock();
        }
      } finally {
        atomicOperationsManager.releaseReadLock(this);
      }
    } finally {
      completeOperation();
    }
  }

  /**
   * Returns the next position available.
   */
  public long getNextPosition() throws IOException {
    startOperation();
    try {
      atomicOperationsManager.acquireReadLock(this);
      try {
        acquireSharedLock();
        try {
          OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();
          final long filledUpTo = getFilledUpTo(atomicOperation, fileId);

          for (long pageIndex = filledUpTo - 1; pageIndex >= 0; pageIndex--) {
            OCacheEntry cacheEntry = loadPage(atomicOperation, fileId, pageIndex, false, 1);
            cacheEntry.acquireSharedLock();
            try {
              OClusterPositionMapBucket bucket = new OClusterPositionMapBucket(cacheEntry, getChanges(atomicOperation, cacheEntry));
              final int bucketSize = bucket.getSize();
              return pageIndex * OClusterPositionMapBucket.MAX_ENTRIES + bucketSize;
            } finally {
              cacheEntry.releaseSharedLock();
              releasePage(atomicOperation, cacheEntry);
            }
          }

          return ORID.CLUSTER_POS_INVALID;
        } finally {
          releaseSharedLock();
        }
      } finally {
        atomicOperationsManager.releaseReadLock(this);
      }
    } finally {
      completeOperation();
    }
  }
}
