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

package com.orientechnologies.orient.core.storage.impl.local.paginated;

import java.io.IOException;
import java.util.Arrays;

import com.orientechnologies.common.concur.resource.OSharedResourceAdaptive;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.id.OClusterPosition;
import com.orientechnologies.orient.core.id.OClusterPositionFactory;
import com.orientechnologies.orient.core.index.hashindex.local.cache.OCacheEntry;
import com.orientechnologies.orient.core.index.hashindex.local.cache.OCachePointer;
import com.orientechnologies.orient.core.index.hashindex.local.cache.ODiskCache;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperationsManager;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurableComponent;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.*;

/**
 * @author Andrey Lomakin <a href="mailto:lomakin.andrey@gmail.com">Andrey Lomakin</a>
 * @since 10/7/13
 */
public class OClusterPositionMap extends ODurableComponent {
  public static final String DEF_EXTENSION = ".cpm";

  private final ODiskCache   diskCache;
  private String             name;
  private long               fileId;
  private boolean            useWal;

  public OClusterPositionMap(ODiskCache diskCache, String name, OWriteAheadLog writeAheadLog,
      OAtomicOperationsManager atomicOperationsManager, boolean useWal) {
    acquireExclusiveLock();
    try {
      this.diskCache = diskCache;
      this.name = name;
      this.useWal = useWal;

      init(atomicOperationsManager, writeAheadLog);
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
      fileId = diskCache.openFile(name + DEF_EXTENSION);
    } finally {
      releaseExclusiveLock();
    }
  }

  public void create() throws IOException {
    acquireExclusiveLock();
    try {
      fileId = diskCache.openFile(name + DEF_EXTENSION);
    } finally {
      releaseExclusiveLock();
    }
  }

  public void flush() throws IOException {
    acquireSharedLock();
    try {
      diskCache.flushFile(fileId);
    } finally {
      releaseSharedLock();
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
    acquireExclusiveLock();
    try {
      diskCache.truncateFile(fileId);
    } finally {
      releaseExclusiveLock();
    }
  }

  public void delete() throws IOException {
    acquireExclusiveLock();
    try {
      diskCache.deleteFile(fileId);
    } finally {
      releaseExclusiveLock();
    }
  }

  public void rename(String newName) throws IOException {
    acquireExclusiveLock();
    try {
      diskCache.renameFile(fileId, this.name, newName);
      name = newName;
    } finally {
      releaseExclusiveLock();
    }
  }

  public OClusterPosition add(long pageIndex, int recordPosition) throws IOException {
    acquireExclusiveLock();
    try {
      long lastPage = diskCache.getFilledUpTo(fileId) - 1;

      boolean isNewPage = false;
      if (lastPage < 0) {
        lastPage = 0;
        isNewPage = true;
      }

      OCacheEntry cacheEntry = diskCache.load(fileId, lastPage, false);
      OCachePointer cachePointer = cacheEntry.getCachePointer();
      cachePointer.acquireExclusiveLock();
      try {
        startAtomicOperation();

        final ODurablePage.TrackMode trackMode = getTrackMode();

        OClusterPositionMapBucket bucket = new OClusterPositionMapBucket(cachePointer.getDataPointer(), trackMode);
        if (bucket.isFull()) {
          cachePointer.releaseExclusiveLock();
          diskCache.release(cacheEntry);

          isNewPage = true;
          cacheEntry = diskCache.allocateNewPage(fileId);
          cachePointer = cacheEntry.getCachePointer();

          cachePointer.acquireExclusiveLock();
          bucket = new OClusterPositionMapBucket(cachePointer.getDataPointer(), trackMode);
        }

        final long index = bucket.add(pageIndex, recordPosition);
        final OClusterPosition result = OClusterPositionFactory.INSTANCE.valueOf(index + cacheEntry.getPageIndex()
            * OClusterPositionMapBucket.MAX_ENTRIES);

        logPageChanges(bucket, fileId, cacheEntry.getPageIndex(), isNewPage);
        cacheEntry.markDirty();

        endAtomicOperation(false);
        return result;
      } catch (Exception e) {
        endAtomicOperation(true);
        throw new OStorageException("Error during creation of mapping between logical adn physical record position.", e);
      } finally {
        cachePointer.releaseExclusiveLock();
        diskCache.release(cacheEntry);
      }
    } finally {
      releaseExclusiveLock();
    }
  }

  public OClusterPositionMapBucket.PositionEntry get(OClusterPosition clusterPosition) throws IOException {
    acquireSharedLock();
    try {
      final long position = clusterPosition.longValue();

      long pageIndex = position / OClusterPositionMapBucket.MAX_ENTRIES;
      int index = (int) (position % OClusterPositionMapBucket.MAX_ENTRIES);

      final OCacheEntry cacheEntry = diskCache.load(fileId, pageIndex, false);
      final OCachePointer cachePointer = cacheEntry.getCachePointer();

      try {
        final OClusterPositionMapBucket bucket = new OClusterPositionMapBucket(cachePointer.getDataPointer(),
            ODurablePage.TrackMode.NONE);
        return bucket.get(index);
      } finally {
        diskCache.release(cacheEntry);
      }
    } finally {
      releaseSharedLock();
    }
  }

  public OClusterPositionMapBucket.PositionEntry remove(OClusterPosition clusterPosition) throws IOException {
    acquireExclusiveLock();
    try {
      final long position = clusterPosition.longValue();

      long pageIndex = position / OClusterPositionMapBucket.MAX_ENTRIES;
      int index = (int) (position % OClusterPositionMapBucket.MAX_ENTRIES);

      final OCacheEntry cacheEntry = diskCache.load(fileId, pageIndex, false);
      final OCachePointer cachePointer = cacheEntry.getCachePointer();
      cachePointer.acquireExclusiveLock();
      try {
        startAtomicOperation();
        final ODurablePage.TrackMode trackMode = getTrackMode();
        final OClusterPositionMapBucket bucket = new OClusterPositionMapBucket(cachePointer.getDataPointer(), trackMode);

        OClusterPositionMapBucket.PositionEntry positionEntry = bucket.remove(index);
        if (positionEntry == null)
          return null;

        cacheEntry.markDirty();

        logPageChanges(bucket, fileId, pageIndex, false);

        endAtomicOperation(false);
        return positionEntry;
      } catch (Exception e) {
        endAtomicOperation(true);

        throw new OStorageException("Error during removal of mapping between logical and physical record position.", e);
      } finally {
        cachePointer.releaseExclusiveLock();
        diskCache.release(cacheEntry);
      }
    } finally {
      releaseExclusiveLock();
    }
  }

  public OClusterPosition[] higherPositions(OClusterPosition clusterPosition) throws IOException {
    acquireSharedLock();
    try {
      final long position = clusterPosition.longValue();
      if (position == Long.MAX_VALUE)
        return new OClusterPosition[0];

      return ceilingPositions(OClusterPositionFactory.INSTANCE.valueOf(position + 1));
    } finally {
      releaseSharedLock();
    }
  }

  public OClusterPosition[] ceilingPositions(OClusterPosition clusterPosition) throws IOException {
    acquireSharedLock();
    try {
      long position = clusterPosition.longValue();
      if (position < 0)
        position = 0;

      long pageIndex = position / OClusterPositionMapBucket.MAX_ENTRIES;
      int index = (int) (position % OClusterPositionMapBucket.MAX_ENTRIES);

      final long filledUpTo = diskCache.getFilledUpTo(fileId);

      if (pageIndex >= filledUpTo)
        return new OClusterPosition[0];

      OClusterPosition[] result = null;
      do {
        OCacheEntry cacheEntry = diskCache.load(fileId, pageIndex, false);
        OCachePointer cachePointer = cacheEntry.getCachePointer();

        OClusterPositionMapBucket bucket = new OClusterPositionMapBucket(cachePointer.getDataPointer(), ODurablePage.TrackMode.NONE);
        int resultSize = bucket.getSize() - index;

        if (resultSize <= 0) {
          diskCache.release(cacheEntry);
          pageIndex++;
          index = 0;
        } else {
          int entriesCount = 0;
          long startIndex = cacheEntry.getPageIndex() * OClusterPositionMapBucket.MAX_ENTRIES + index;

          result = new OClusterPosition[resultSize];
          for (int i = 0; i < resultSize; i++) {
            if (bucket.exists(i + index)) {
              result[entriesCount] = OClusterPositionFactory.INSTANCE.valueOf(startIndex + i);
              entriesCount++;
            }
          }

          if (entriesCount == 0) {
            result = null;
            pageIndex++;
            index = 0;
          } else
            result = Arrays.copyOf(result, entriesCount);

          diskCache.release(cacheEntry);
        }
      } while (result == null && pageIndex < filledUpTo);

      if (result == null)
        result = new OClusterPosition[0];

      return result;
    } finally {
      releaseSharedLock();
    }
  }

  public OClusterPosition[] lowerPositions(OClusterPosition clusterPosition) throws IOException {
    acquireSharedLock();
    try {
      final long position = clusterPosition.longValue();
      if (position == 0)
        return new OClusterPosition[0];

      return floorPositions(OClusterPositionFactory.INSTANCE.valueOf(position - 1));
    } finally {
      releaseSharedLock();
    }
  }

  public OClusterPosition[] floorPositions(OClusterPosition clusterPosition) throws IOException {
    acquireSharedLock();
    try {
      final long position = clusterPosition.longValue();
      if (position < 0)
        return new OClusterPosition[0];

      long pageIndex = position / OClusterPositionMapBucket.MAX_ENTRIES;
      int index = (int) (position % OClusterPositionMapBucket.MAX_ENTRIES);

      final long filledUpTo = diskCache.getFilledUpTo(fileId);
      OClusterPosition[] result;

      if (pageIndex >= filledUpTo) {
        pageIndex = filledUpTo - 1;
        index = Integer.MIN_VALUE;
      }

      do {
        OCacheEntry cacheEntry = diskCache.load(fileId, pageIndex, false);
        OCachePointer cachePointer = cacheEntry.getCachePointer();

        OClusterPositionMapBucket bucket = new OClusterPositionMapBucket(cachePointer.getDataPointer(), ODurablePage.TrackMode.NONE);
        if (index == Integer.MIN_VALUE)
          index = bucket.getSize() - 1;

        int resultSize = index + 1;
        int entriesCount = 0;

        long startPosition = cacheEntry.getPageIndex() * OClusterPositionMapBucket.MAX_ENTRIES;
        result = new OClusterPosition[resultSize];

        for (int i = 0; i < resultSize; i++) {
          if (bucket.exists(i)) {
            result[entriesCount] = OClusterPositionFactory.INSTANCE.valueOf(startPosition + i);
            entriesCount++;
          }
        }

        if (entriesCount == 0) {
          result = null;
          pageIndex--;
          index = Integer.MIN_VALUE;
        } else
          result = Arrays.copyOf(result, entriesCount);

        diskCache.release(cacheEntry);
      } while (result == null && pageIndex >= 0);

      if (result == null)
        result = new OClusterPosition[0];

      return result;
    } finally {
      releaseSharedLock();
    }
  }

  public OClusterPosition getFirstPosition() throws IOException {
    acquireSharedLock();
    try {
      final long filledUpTo = diskCache.getFilledUpTo(fileId);
      for (long pageIndex = 0; pageIndex < filledUpTo; pageIndex++) {
        OCacheEntry cacheEntry = diskCache.load(fileId, pageIndex, false);
        OCachePointer cachePointer = cacheEntry.getCachePointer();
        try {
          OClusterPositionMapBucket bucket = new OClusterPositionMapBucket(cachePointer.getDataPointer(),
              ODurablePage.TrackMode.NONE);
          int bucketSize = bucket.getSize();

          for (int index = 0; index < bucketSize; index++) {
            if (bucket.exists(index))
              return OClusterPositionFactory.INSTANCE.valueOf(pageIndex * OClusterPositionMapBucket.MAX_ENTRIES + index);
          }
        } finally {
          diskCache.release(cacheEntry);
        }
      }

      return OClusterPosition.INVALID_POSITION;
    } finally {
      releaseSharedLock();
    }
  }

  public OClusterPosition getLastPosition() throws IOException {
    acquireSharedLock();
    try {
      final long filledUpTo = diskCache.getFilledUpTo(fileId);
      for (long pageIndex = filledUpTo - 1; pageIndex >= 0; pageIndex--) {
        OCacheEntry cacheEntry = diskCache.load(fileId, pageIndex, false);
        OCachePointer cachePointer = cacheEntry.getCachePointer();
        try {
          OClusterPositionMapBucket bucket = new OClusterPositionMapBucket(cachePointer.getDataPointer(),
              ODurablePage.TrackMode.NONE);
          final int bucketSize = bucket.getSize();

          for (int index = bucketSize - 1; index >= 0; index--) {
            if (bucket.exists(index))
              return OClusterPositionFactory.INSTANCE.valueOf(pageIndex * OClusterPositionMapBucket.MAX_ENTRIES + index);
          }
        } finally {
          diskCache.release(cacheEntry);
        }
      }

      return OClusterPosition.INVALID_POSITION;
    } finally {
      releaseSharedLock();
    }
  }

  public boolean wasSoftlyClosed() throws IOException {
    acquireSharedLock();
    try {
      return diskCache.wasSoftlyClosed(fileId);
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  protected ODurablePage.TrackMode getTrackMode() {
    if (!useWal)
      return ODurablePage.TrackMode.NONE;

    return super.getTrackMode();
  }

  @Override
  protected void endAtomicOperation(boolean rollback) throws IOException {
    if (useWal)
      super.endAtomicOperation(rollback);
  }

  @Override
  protected void startAtomicOperation() throws IOException {
    if (useWal)
      super.startAtomicOperation();
  }
}
