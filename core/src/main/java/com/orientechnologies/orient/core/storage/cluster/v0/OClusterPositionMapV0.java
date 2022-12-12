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

package com.orientechnologies.orient.core.storage.cluster.v0;

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
public class OClusterPositionMapV0 extends OClusterPositionMap {
  private long fileId;

  OClusterPositionMapV0(
      final OAbstractPaginatedStorage storage, final String name, final String lockName) {
    super(storage, name, DEF_EXTENSION, lockName);
  }

  public void open(final OAtomicOperation atomicOperation) throws IOException {
    fileId = openFile(atomicOperation, getFullName());
  }

  public void create(final OAtomicOperation atomicOperation) throws IOException {
    fileId = addFile(atomicOperation, getFullName());
  }

  public void flush() {
    writeCache.flush(fileId);
  }

  public void close(final boolean flush) throws IOException {
    readCache.closeFile(fileId, flush, writeCache);
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
    final long lastPage = getFilledUpTo(atomicOperation, fileId) - 1;
    OCacheEntry cacheEntry;
    boolean clear = false;

    if (lastPage < 0) {
      cacheEntry = addPage(atomicOperation, fileId);
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

        cacheEntry = addPage(atomicOperation, fileId);

        bucket = new OClusterPositionMapBucket(cacheEntry);
        bucket.init();
      }

      final long index = bucket.add(pageIndex, recordPosition);
      return index + cacheEntry.getPageIndex() * OClusterPositionMapBucket.MAX_ENTRIES;
    } finally {
      cacheEntry.close();
    }
  }

  public long allocate(final OAtomicOperation atomicOperation) throws IOException {
    boolean clear = false;
    final long lastPage = getFilledUpTo(atomicOperation, fileId) - 1;
    OCacheEntry cacheEntry;
    if (lastPage < 0) {
      cacheEntry = addPage(atomicOperation, fileId);
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

        cacheEntry = addPage(atomicOperation, fileId);

        bucket = new OClusterPositionMapBucket(cacheEntry);
        if (clear) {
          bucket.init();
        }
      }

      final long index = bucket.allocate();
      return index + cacheEntry.getPageIndex() * OClusterPositionMapBucket.MAX_ENTRIES;
    } finally {
      cacheEntry.close();
    }
  }

  public void update(
      final long clusterPosition,
      final OClusterPositionMapBucket.PositionEntry entry,
      final OAtomicOperation atomicOperation)
      throws IOException {

    final long pageIndex = clusterPosition / OClusterPositionMapBucket.MAX_ENTRIES;
    final int index = (int) (clusterPosition % OClusterPositionMapBucket.MAX_ENTRIES);

    if (pageIndex >= getFilledUpTo(atomicOperation, fileId)) {
      throw new OClusterPositionMapException(
          "Passed in cluster position "
              + clusterPosition
              + " is outside of range of cluster-position map",
          this);
    }

    try (final OCacheEntry cacheEntry =
        loadPageForWrite(atomicOperation, fileId, pageIndex, true)) {
      final OClusterPositionMapBucket bucket = new OClusterPositionMapBucket(cacheEntry);
      bucket.set(index, entry);
    }
  }

  public OClusterPositionMapBucket.PositionEntry get(
      final long clusterPosition, final OAtomicOperation atomicOperation) throws IOException {
    final long pageIndex = clusterPosition / OClusterPositionMapBucket.MAX_ENTRIES;
    final int index = (int) (clusterPosition % OClusterPositionMapBucket.MAX_ENTRIES);

    if (pageIndex >= getFilledUpTo(atomicOperation, fileId)) {
      return null;
    }

    try (final OCacheEntry cacheEntry = loadPageForRead(atomicOperation, fileId, pageIndex)) {
      final OClusterPositionMapBucket bucket = new OClusterPositionMapBucket(cacheEntry);
      return bucket.get(index);
    }
  }

  public void remove(final long clusterPosition, final OAtomicOperation atomicOperation)
      throws IOException {
    final long pageIndex = clusterPosition / OClusterPositionMapBucket.MAX_ENTRIES;
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
    long realPosition = clusterPosition + 1;
    if (clusterPosition == Long.MAX_VALUE) {
      return new OClusterPositionEntry[] {};
    }

    if (realPosition < 0) {
      realPosition = 0;
    }

    long pageIndex = realPosition / OClusterPositionMapBucket.MAX_ENTRIES;
    int index = (int) (realPosition % OClusterPositionMapBucket.MAX_ENTRIES);

    final long filledUpTo = getFilledUpTo(atomicOperation, fileId);

    if (pageIndex >= filledUpTo) {
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
    } while (result == null && pageIndex < filledUpTo);

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

    long pageIndex = clusterPosition / OClusterPositionMapBucket.MAX_ENTRIES;
    int index = (int) (clusterPosition % OClusterPositionMapBucket.MAX_ENTRIES);

    final long filledUpTo = getFilledUpTo(atomicOperation, fileId);

    if (pageIndex >= filledUpTo) {
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
              result[entriesCount] = startIndex + i;
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
    } while (result == null && pageIndex < filledUpTo);

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

    long pageIndex = clusterPosition / OClusterPositionMapBucket.MAX_ENTRIES;
    int index = (int) (clusterPosition % OClusterPositionMapBucket.MAX_ENTRIES);

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
            result[entriesCount] = startPosition + i;
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
    final long filledUpTo = getFilledUpTo(atomicOperation, fileId);
    for (long pageIndex = 0; pageIndex < filledUpTo; pageIndex++) {
      try (final OCacheEntry cacheEntry = loadPageForRead(atomicOperation, fileId, pageIndex)) {
        final OClusterPositionMapBucket bucket = new OClusterPositionMapBucket(cacheEntry);
        final int bucketSize = bucket.getSize();

        for (int index = 0; index < bucketSize; index++) {
          if (bucket.exists(index)) {
            return pageIndex * OClusterPositionMapBucket.MAX_ENTRIES + index;
          }
        }
      }
    }

    return ORID.CLUSTER_POS_INVALID;
  }

  public byte getStatus(final long clusterPosition, final OAtomicOperation atomicOperation)
      throws IOException {
    final long pageIndex = clusterPosition / OClusterPositionMapBucket.MAX_ENTRIES;
    final int index = (int) (clusterPosition % OClusterPositionMapBucket.MAX_ENTRIES);

    if (pageIndex >= getFilledUpTo(atomicOperation, fileId)) {
      return OClusterPositionMapBucket.NOT_EXISTENT;
    }

    try (final OCacheEntry cacheEntry = loadPageForRead(atomicOperation, fileId, pageIndex)) {
      final OClusterPositionMapBucket bucket = new OClusterPositionMapBucket(cacheEntry);

      return bucket.getStatus(index);
    }
  }

  public long getLastPosition(final OAtomicOperation atomicOperation) throws IOException {
    final long filledUpTo = getFilledUpTo(atomicOperation, fileId);

    for (long pageIndex = filledUpTo - 1; pageIndex >= 0; pageIndex--) {
      try (final OCacheEntry cacheEntry = loadPageForRead(atomicOperation, fileId, pageIndex)) {
        final OClusterPositionMapBucket bucket = new OClusterPositionMapBucket(cacheEntry);
        final int bucketSize = bucket.getSize();

        for (int index = bucketSize - 1; index >= 0; index--) {
          if (bucket.exists(index)) {
            return pageIndex * OClusterPositionMapBucket.MAX_ENTRIES + index;
          }
        }
      }
    }

    return ORID.CLUSTER_POS_INVALID;
  }

  /** Returns the next position available. */
  long getNextPosition(final OAtomicOperation atomicOperation) throws IOException {
    final long filledUpTo = getFilledUpTo(atomicOperation, fileId);

    final long pageIndex = filledUpTo - 1;

    try (final OCacheEntry cacheEntry = loadPageForRead(atomicOperation, fileId, pageIndex)) {
      final OClusterPositionMapBucket bucket = new OClusterPositionMapBucket(cacheEntry);
      final int bucketSize = bucket.getSize();
      return pageIndex * OClusterPositionMapBucket.MAX_ENTRIES + bucketSize;
    }
  }

  public long getFileId() {
    return fileId;
  }

  public static class OClusterPositionEntry {
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
