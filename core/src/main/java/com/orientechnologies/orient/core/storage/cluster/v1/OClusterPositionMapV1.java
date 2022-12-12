/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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
 *  * For more information: http://orientdb.com
 *
 */

package com.orientechnologies.orient.core.storage.cluster.v1;

import com.orientechnologies.common.util.OCommonConst;
import com.orientechnologies.orient.core.exception.OClusterPositionMapException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cluster.OClusterPositionMap;
import com.orientechnologies.orient.core.storage.cluster.OClusterPositionMapBucket;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;
import java.io.IOException;
import java.util.Arrays;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 10/7/13
 */
public final class OClusterPositionMapV1 extends OClusterPositionMap {
  private long fileId;

  OClusterPositionMapV1(
      final OAbstractPaginatedStorage storage,
      final String name,
      final String lockName,
      final String extension) {
    super(storage, name, extension, lockName);
  }

  public void open(final OAtomicOperation atomicOperation) throws IOException {
    fileId = openFile(atomicOperation, getFullName());
  }

  public void create(final OAtomicOperation atomicOperation) throws IOException {
    fileId = addFile(atomicOperation, getFullName());

    if (getFilledUpTo(atomicOperation, fileId) == 0) {

      try (final OCacheEntry cacheEntry = addPage(atomicOperation, fileId)) {
        final MapEntryPoint mapEntryPoint = new MapEntryPoint(cacheEntry);
        mapEntryPoint.setFileSize(0);
      }
    } else {
      try (final OCacheEntry cacheEntry = loadPageForWrite(atomicOperation, fileId, 0, false)) {
        final MapEntryPoint mapEntryPoint = new MapEntryPoint(cacheEntry);
        mapEntryPoint.setFileSize(0);
      }
    }
  }

  public void flush() {
    writeCache.flush(fileId);
  }

  public void close(final boolean flush) {
    readCache.closeFile(fileId, flush, writeCache);
  }

  public void truncate(final OAtomicOperation atomicOperation) throws IOException {
    try (final OCacheEntry cacheEntry = loadPageForWrite(atomicOperation, fileId, 0, true)) {
      final MapEntryPoint mapEntryPoint = new MapEntryPoint(cacheEntry);
      mapEntryPoint.setFileSize(0);
    }
  }

  public void delete(final OAtomicOperation atomicOperation) throws IOException {
    deleteFile(atomicOperation, fileId);
  }

  void rename(final String newName) throws IOException {
    writeCache.renameFile(fileId, newName + getExtension());
    setName(newName);
  }

  public long add(
      final long pageIndex, final int recordPosition, final OAtomicOperation atomicOperation)
      throws IOException {
    OCacheEntry cacheEntry;
    boolean clear = false;

    try (final OCacheEntry entryPointEntry = loadPageForWrite(atomicOperation, fileId, 0, true)) {
      final MapEntryPoint mapEntryPoint = new MapEntryPoint(entryPointEntry);
      final int lastPage = mapEntryPoint.getFileSize();
      long filledUpTo = getFilledUpTo(atomicOperation, fileId);

      assert lastPage <= filledUpTo - 1;

      if (lastPage == 0) {
        if (lastPage == filledUpTo - 1) {
          cacheEntry = addPage(atomicOperation, fileId);
          filledUpTo++;
        } else {
          cacheEntry = loadPageForWrite(atomicOperation, fileId, lastPage + 1, false);
        }

        mapEntryPoint.setFileSize(lastPage + 1);
        clear = true;
      } else {
        cacheEntry = loadPageForWrite(atomicOperation, fileId, lastPage, true);
      }

      try {
        OClusterPositionMapBucket bucket = new OClusterPositionMapBucket(cacheEntry);
        if (clear) {
          bucket.init();
        }

        if (bucket.isFull()) {
          cacheEntry.close();

          assert lastPage <= filledUpTo - 1;

          if (lastPage == filledUpTo - 1) {
            cacheEntry = addPage(atomicOperation, fileId);
          } else {
            cacheEntry = loadPageForWrite(atomicOperation, fileId, lastPage + 1, false);
          }

          mapEntryPoint.setFileSize(lastPage + 1);

          bucket = new OClusterPositionMapBucket(cacheEntry);
          bucket.init();
        }

        final long index = bucket.add(pageIndex, recordPosition);
        return index + (cacheEntry.getPageIndex() - 1) * OClusterPositionMapBucket.MAX_ENTRIES;
      } finally {
        cacheEntry.close();
      }
    }
  }

  private long getLastPage(final OAtomicOperation atomicOperation) throws IOException {
    long lastPage;
    try (final OCacheEntry entryPointEntry = loadPageForRead(atomicOperation, fileId, 0)) {
      final MapEntryPoint mapEntryPoint = new MapEntryPoint(entryPointEntry);
      lastPage = mapEntryPoint.getFileSize();
    }
    return lastPage;
  }

  public long allocate(final OAtomicOperation atomicOperation) throws IOException {
    OCacheEntry cacheEntry;
    boolean clear = false;

    try (final OCacheEntry entryPointEntry = loadPageForWrite(atomicOperation, fileId, 0, true)) {
      final MapEntryPoint mapEntryPoint = new MapEntryPoint(entryPointEntry);
      final int lastPage = mapEntryPoint.getFileSize();

      long filledUpTo = getFilledUpTo(atomicOperation, fileId);

      assert lastPage <= filledUpTo - 1;

      if (lastPage == 0) {
        if (lastPage == filledUpTo - 1) {
          cacheEntry = addPage(atomicOperation, fileId);
          filledUpTo++;
        } else {
          cacheEntry = loadPageForWrite(atomicOperation, fileId, lastPage + 1, false);
        }
        mapEntryPoint.setFileSize(lastPage + 1);

        clear = true;
      } else {
        cacheEntry = loadPageForWrite(atomicOperation, fileId, lastPage, true);
      }

      try {
        OClusterPositionMapBucket bucket = new OClusterPositionMapBucket(cacheEntry);
        if (clear) {
          bucket.init();
        }
        if (bucket.isFull()) {
          cacheEntry.close();

          assert lastPage <= filledUpTo - 1;

          if (lastPage == filledUpTo - 1) {
            cacheEntry = addPage(atomicOperation, fileId);
          } else {
            cacheEntry = loadPageForWrite(atomicOperation, fileId, lastPage + 1, false);
          }

          mapEntryPoint.setFileSize(lastPage + 1);

          bucket = new OClusterPositionMapBucket(cacheEntry);
          bucket.init();
        }

        final long index = bucket.allocate();
        return index + (cacheEntry.getPageIndex() - 1) * OClusterPositionMapBucket.MAX_ENTRIES;
      } finally {
        cacheEntry.close();
      }
    }
  }

  public void update(
      final long clusterPosition,
      final OClusterPositionMapBucket.PositionEntry entry,
      final OAtomicOperation atomicOperation)
      throws IOException {

    final long pageIndex = clusterPosition / OClusterPositionMapBucket.MAX_ENTRIES + 1;
    final int index = (int) (clusterPosition % OClusterPositionMapBucket.MAX_ENTRIES);

    final long lastPage = getLastPage(atomicOperation);
    if (pageIndex > lastPage) {
      throw new OClusterPositionMapException(
          "Passed in cluster position "
              + clusterPosition
              + " is outside of range of cluster-position map",
          this);
    }

    ;
    try (final OCacheEntry cacheEntry =
        loadPageForWrite(atomicOperation, fileId, pageIndex, true)) {
      final OClusterPositionMapBucket bucket = new OClusterPositionMapBucket(cacheEntry);
      bucket.set(index, entry);
    }
  }

  public OClusterPositionMapBucket.PositionEntry get(
      final long clusterPosition, final OAtomicOperation atomicOperation) throws IOException {
    final long pageIndex = clusterPosition / OClusterPositionMapBucket.MAX_ENTRIES + 1;
    final int index = (int) (clusterPosition % OClusterPositionMapBucket.MAX_ENTRIES);

    final long lastPage = getLastPage(atomicOperation);
    if (pageIndex > lastPage) {
      return null;
    }

    try (final OCacheEntry cacheEntry = loadPageForRead(atomicOperation, fileId, pageIndex)) {
      final OClusterPositionMapBucket bucket = new OClusterPositionMapBucket(cacheEntry);
      return bucket.get(index);
    }
  }

  public void remove(final long clusterPosition, final OAtomicOperation atomicOperation)
      throws IOException {
    final long pageIndex = clusterPosition / OClusterPositionMapBucket.MAX_ENTRIES + 1;
    final int index = (int) (clusterPosition % OClusterPositionMapBucket.MAX_ENTRIES);

    try (final OCacheEntry cacheEntry =
        loadPageForWrite(atomicOperation, fileId, pageIndex, true)) {
      final OClusterPositionMapBucket bucket = new OClusterPositionMapBucket(cacheEntry);
      bucket.remove(index);
    }
  }

  long[] higherPositions(final long clusterPosition, final OAtomicOperation atomicOperation)
      throws IOException {
    if (clusterPosition == Long.MAX_VALUE) {
      return OCommonConst.EMPTY_LONG_ARRAY;
    }

    return ceilingPositions(clusterPosition + 1, atomicOperation);
  }

  OClusterPositionEntry[] higherPositionsEntries(
      final long clusterPosition, final OAtomicOperation atomicOperation) throws IOException {
    if (clusterPosition == Long.MAX_VALUE) {
      return new OClusterPositionEntry[] {};
    }

    final long realPosition;
    if (clusterPosition < 0) {
      realPosition = 0;
    } else {
      realPosition = clusterPosition + 1;
    }

    long pageIndex = realPosition / OClusterPositionMapBucket.MAX_ENTRIES + 1;
    int index = (int) (realPosition % OClusterPositionMapBucket.MAX_ENTRIES);

    final long lastPage = getLastPage(atomicOperation);

    if (pageIndex > lastPage) {
      return new OClusterPositionEntry[] {};
    }

    OClusterPositionEntry[] result = null;
    do {
      try (final OCacheEntry cacheEntry = loadPageForRead(atomicOperation, fileId, pageIndex)) {

        final OClusterPositionMapBucket bucket = new OClusterPositionMapBucket(cacheEntry);
        final int resultSize = bucket.getSize() - index;

        if (resultSize <= 0) {
          pageIndex++;
          index = 0;
        } else {
          int entriesCount = 0;
          final long startIndex =
              cacheEntry.getPageIndex() * OClusterPositionMapBucket.MAX_ENTRIES + index;

          result = new OClusterPositionEntry[resultSize];
          for (int i = 0; i < resultSize; i++) {
            if (bucket.exists(i + index)) {
              final OClusterPositionMapBucket.PositionEntry val = bucket.get(i + index);
              assert val != null;
              result[entriesCount] =
                  new OClusterPositionEntry(
                      startIndex + i, val.getPageIndex(), val.getRecordPosition());
              entriesCount++;
            }
          }

          if (entriesCount == 0) {
            result = null;
            pageIndex++;
            index = 0;
          } else {
            result = Arrays.copyOf(result, entriesCount);
          }
        }
      }
    } while (result == null && pageIndex <= lastPage);

    if (result == null) {
      result = new OClusterPositionEntry[] {};
    }

    return result;
  }

  long[] ceilingPositions(long clusterPosition, final OAtomicOperation atomicOperation)
      throws IOException {
    if (clusterPosition < 0) {
      clusterPosition = 0;
    }

    long pageIndex = clusterPosition / OClusterPositionMapBucket.MAX_ENTRIES + 1;
    int index = (int) (clusterPosition % OClusterPositionMapBucket.MAX_ENTRIES);

    final long lastPage = getLastPage(atomicOperation);

    if (pageIndex > lastPage) {
      return OCommonConst.EMPTY_LONG_ARRAY;
    }

    long[] result = null;
    do {
      try (final OCacheEntry cacheEntry = loadPageForRead(atomicOperation, fileId, pageIndex)) {

        final OClusterPositionMapBucket bucket = new OClusterPositionMapBucket(cacheEntry);
        final int resultSize = bucket.getSize() - index;

        if (resultSize <= 0) {
          pageIndex++;
          index = 0;
        } else {
          int entriesCount = 0;
          final long startIndex =
              cacheEntry.getPageIndex() * OClusterPositionMapBucket.MAX_ENTRIES + index;

          result = new long[resultSize];
          for (int i = 0; i < resultSize; i++) {
            if (bucket.exists(i + index)) {
              result[entriesCount] = startIndex + i - OClusterPositionMapBucket.MAX_ENTRIES;
              entriesCount++;
            }
          }

          if (entriesCount == 0) {
            result = null;
            pageIndex++;
            index = 0;
          } else {
            result = Arrays.copyOf(result, entriesCount);
          }
        }
      }
    } while (result == null && pageIndex <= lastPage);

    if (result == null) {
      result = OCommonConst.EMPTY_LONG_ARRAY;
    }

    return result;
  }

  long[] lowerPositions(final long clusterPosition, final OAtomicOperation atomicOperation)
      throws IOException {
    if (clusterPosition == 0) {
      return OCommonConst.EMPTY_LONG_ARRAY;
    }

    return floorPositions(clusterPosition - 1, atomicOperation);
  }

  long[] floorPositions(final long clusterPosition, final OAtomicOperation atomicOperation)
      throws IOException {
    if (clusterPosition < 0) {
      return OCommonConst.EMPTY_LONG_ARRAY;
    }

    long pageIndex = clusterPosition / OClusterPositionMapBucket.MAX_ENTRIES + 1;
    int index = (int) (clusterPosition % OClusterPositionMapBucket.MAX_ENTRIES);

    final long lastPage = getLastPage(atomicOperation);
    long[] result;

    if (pageIndex > lastPage) {
      pageIndex = lastPage;
      index = Integer.MIN_VALUE;
    }

    if (pageIndex < 0) {
      return OCommonConst.EMPTY_LONG_ARRAY;
    }

    do {
      try (final OCacheEntry cacheEntry = loadPageForRead(atomicOperation, fileId, pageIndex)) {

        final OClusterPositionMapBucket bucket = new OClusterPositionMapBucket(cacheEntry);
        if (index == Integer.MIN_VALUE) {
          index = bucket.getSize() - 1;
        }

        final int resultSize = index + 1;
        int entriesCount = 0;

        final long startPosition =
            cacheEntry.getPageIndex() * OClusterPositionMapBucket.MAX_ENTRIES;
        result = new long[resultSize];

        for (int i = 0; i < resultSize; i++) {
          if (bucket.exists(i)) {
            result[entriesCount] = startPosition + i - OClusterPositionMapBucket.MAX_ENTRIES;
            entriesCount++;
          }
        }

        if (entriesCount == 0) {
          result = null;
          pageIndex--;
          index = Integer.MIN_VALUE;
        } else {
          result = Arrays.copyOf(result, entriesCount);
        }
      }
    } while (result == null && pageIndex >= 0);

    if (result == null) {
      result = OCommonConst.EMPTY_LONG_ARRAY;
    }

    return result;
  }

  long getFirstPosition(final OAtomicOperation atomicOperation) throws IOException {
    final long lastPage = getLastPage(atomicOperation);

    for (long pageIndex = 1; pageIndex <= lastPage; pageIndex++) {
      try (final OCacheEntry cacheEntry = loadPageForRead(atomicOperation, fileId, pageIndex)) {
        final OClusterPositionMapBucket bucket = new OClusterPositionMapBucket(cacheEntry);
        final int bucketSize = bucket.getSize();

        for (int index = 0; index < bucketSize; index++) {
          if (bucket.exists(index)) {
            return pageIndex * OClusterPositionMapBucket.MAX_ENTRIES
                + index
                - OClusterPositionMapBucket.MAX_ENTRIES;
          }
        }
      }
    }

    return ORID.CLUSTER_POS_INVALID;
  }

  public byte getStatus(final long clusterPosition, final OAtomicOperation atomicOperation)
      throws IOException {
    final long pageIndex =
        (clusterPosition + OClusterPositionMapBucket.MAX_ENTRIES)
            / OClusterPositionMapBucket.MAX_ENTRIES;
    final int index = (int) (clusterPosition % OClusterPositionMapBucket.MAX_ENTRIES);

    final long lastPage = getLastPage(atomicOperation);
    if (pageIndex > lastPage) {
      return OClusterPositionMapBucket.NOT_EXISTENT;
    }

    try (final OCacheEntry cacheEntry = loadPageForRead(atomicOperation, fileId, pageIndex)) {
      final OClusterPositionMapBucket bucket = new OClusterPositionMapBucket(cacheEntry);

      return bucket.getStatus(index);
    }
  }

  public long getLastPosition(final OAtomicOperation atomicOperation) throws IOException {
    final long lastPage = getLastPage(atomicOperation);

    for (long pageIndex = lastPage; pageIndex >= 1; pageIndex--) {

      try (final OCacheEntry cacheEntry = loadPageForRead(atomicOperation, fileId, pageIndex); ) {
        final OClusterPositionMapBucket bucket = new OClusterPositionMapBucket(cacheEntry);
        final int bucketSize = bucket.getSize();

        for (int index = bucketSize - 1; index >= 0; index--) {
          if (bucket.exists(index)) {
            return pageIndex * OClusterPositionMapBucket.MAX_ENTRIES
                + index
                - OClusterPositionMapBucket.MAX_ENTRIES;
          }
        }
      }
    }

    return ORID.CLUSTER_POS_INVALID;
  }

  /** Returns the next position available. */
  long getNextPosition(final OAtomicOperation atomicOperation) throws IOException {
    final long pageIndex = getLastPage(atomicOperation);
    try (final OCacheEntry cacheEntry = loadPageForRead(atomicOperation, fileId, pageIndex)) {
      final OClusterPositionMapBucket bucket = new OClusterPositionMapBucket(cacheEntry);
      final int bucketSize = bucket.getSize();
      return pageIndex * OClusterPositionMapBucket.MAX_ENTRIES + bucketSize;
    }
  }

  public long getFileId() {
    return fileId;
  }

  public static final class OClusterPositionEntry {
    private final long position;
    private final long page;
    private final int offset;

    OClusterPositionEntry(final long position, final long page, final int offset) {
      this.position = position;
      this.page = page;
      this.offset = offset;
    }

    public long getPosition() {
      return position;
    }

    public long getPage() {
      return page;
    }

    public int getOffset() {
      return offset;
    }
  }
}
