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

package com.orientechnologies.orient.core.storage.index.versionmap;

import com.orientechnologies.common.util.OCommonConst;
import com.orientechnologies.orient.core.exception.OVersionPositionMapException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperationsManager;
import java.io.IOException;
import java.util.Arrays;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 10/7/13
 */
public final class OVersionPositionMapV0 extends OVersionPositionMap {
  private long fileId;

  public OVersionPositionMapV0(
      final OAbstractPaginatedStorage storage,
      final String name,
      final String lockName,
      final String extension) {
    super(storage, name, extension, lockName);
  }

  @Override
  public void create(final OAtomicOperation atomicOperation) {
    executeInsideComponentOperation(
        atomicOperation,
        operation -> {
          acquireExclusiveLock();
          try {
            // fileId = addFile(atomicOperation, getFullName());
            // TODO: [DR] initCusterState(atomicOperation);
            this.createVPM(atomicOperation);
          } finally {
            releaseExclusiveLock();
          }
        });
  }

  @Override
  public void delete(OAtomicOperation atomicOperation) {
    executeInsideComponentOperation(
        atomicOperation,
        operation -> {
          acquireExclusiveLock();
          try {
            // TODO: [DR] final long entries = getEntries();
            // if (entries > 0) {
            //  throw new NotEmptyComponentCanNotBeRemovedException(
            //      getName()
            //          + " : Not empty cluster can not be deleted. Cluster has "
            //          + entries
            //          + " records");
            // }
            this.deleteVPM(atomicOperation);
          } finally {
            releaseExclusiveLock();
          }
        });
  }

  @Override
  public void open() throws IOException {
    acquireExclusiveLock();
    try {
      final OAtomicOperation atomicOperation = OAtomicOperationsManager.getCurrentOperation();
      this.openVPM(atomicOperation);
    } finally {
      releaseExclusiveLock();
    }
  }

  /*@Override
  public void close() {
    close(true);
  }

  @Override
  public void close(final boolean flush) {
    acquireExclusiveLock();
    try {
      if (flush) {
        synch();
      }
      readCache.closeFile(fileId, flush, writeCache);
      this.closeVPM(flush);
    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  public void synch() {
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        writeCache.flush(fileId);
        this.flushVPM();
      } finally {
        releaseSharedLock();
      }
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }*/

  @Override
  public void updateVersion(final int hash, final int version) {
    // TODO: [DR]
  }

  @Override
  public int getVersion(final int hash) {
    // TODO: [DR]
    return 0;
  }

  public void openVPM(final OAtomicOperation atomicOperation) throws IOException {
    fileId = openFile(atomicOperation, getFullName());
  }

  public void createVPM(final OAtomicOperation atomicOperation) throws IOException {
    fileId = addFile(atomicOperation, getFullName());
    if (getFilledUpTo(atomicOperation, fileId) == 0) {
      final OCacheEntry cacheEntry = addPage(atomicOperation, fileId);
      try {
        final MapEntryPoint mapEntryPoint = new MapEntryPoint(cacheEntry);
        mapEntryPoint.setFileSize(0);
      } finally {
        releasePageFromWrite(atomicOperation, cacheEntry);
      }
    } else {
      final OCacheEntry cacheEntry = loadPageForWrite(atomicOperation, fileId, 0, false, false);
      try {
        final MapEntryPoint mapEntryPoint = new MapEntryPoint(cacheEntry);
        mapEntryPoint.setFileSize(0);
      } finally {
        releasePageFromWrite(atomicOperation, cacheEntry);
      }
    }
  }

  public void flushVPM() {
    writeCache.flush(fileId);
  }

  public void closeVPM(final boolean flush) {
    readCache.closeFile(fileId, flush, writeCache);
  }

  public void truncate(final OAtomicOperation atomicOperation) throws IOException {
    final OCacheEntry cacheEntry = loadPageForWrite(atomicOperation, fileId, 0, false, true);
    try {
      final MapEntryPoint mapEntryPoint = new MapEntryPoint(cacheEntry);
      mapEntryPoint.setFileSize(0);
    } finally {
      releasePageFromWrite(atomicOperation, cacheEntry);
    }
  }

  public void deleteVPM(final OAtomicOperation atomicOperation) throws IOException {
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

    final OCacheEntry entryPointEntry = loadPageForWrite(atomicOperation, fileId, 0, false, true);
    try {
      final MapEntryPoint mapEntryPoint = new MapEntryPoint(entryPointEntry);
      final int lastPage = mapEntryPoint.getFileSize();
      long filledUpTo = getFilledUpTo(atomicOperation, fileId);

      assert lastPage <= filledUpTo - 1;

      if (lastPage == 0) {
        if (lastPage == filledUpTo - 1) {
          cacheEntry = addPage(atomicOperation, fileId);
          filledUpTo++;
        } else {
          cacheEntry = loadPageForWrite(atomicOperation, fileId, lastPage + 1, false, false);
        }
        mapEntryPoint.setFileSize(lastPage + 1);
        clear = true;
      } else {
        cacheEntry = loadPageForWrite(atomicOperation, fileId, lastPage, false, true);
      }

      try {
        // TODO: structure plain array
        OVersionPositionMapBucket bucket = new OVersionPositionMapBucket(cacheEntry);
        if (clear) {
          bucket.init();
        }
        if (bucket.isFull()) {
          releasePageFromWrite(atomicOperation, cacheEntry);

          assert lastPage <= filledUpTo - 1;

          if (lastPage == filledUpTo - 1) {
            cacheEntry = addPage(atomicOperation, fileId);
          } else {
            cacheEntry = loadPageForWrite(atomicOperation, fileId, lastPage + 1, false, false);
          }
          mapEntryPoint.setFileSize(lastPage + 1);

          bucket = new OVersionPositionMapBucket(cacheEntry);
          bucket.init();
        }
        final long index = bucket.add(pageIndex, recordPosition);
        return index + (cacheEntry.getPageIndex() - 1) * OVersionPositionMapBucket.MAX_ENTRIES;
      } finally {
        releasePageFromWrite(atomicOperation, cacheEntry);
      }
    } finally {
      releasePageFromWrite(atomicOperation, entryPointEntry);
    }
  }

  private long getLastPage(final OAtomicOperation atomicOperation) throws IOException {
    long lastPage;
    final OCacheEntry entryPointEntry = loadPageForRead(atomicOperation, fileId, 0, false);
    try {
      final MapEntryPoint mapEntryPoint = new MapEntryPoint(entryPointEntry);
      lastPage = mapEntryPoint.getFileSize();
    } finally {
      releasePageFromRead(atomicOperation, entryPointEntry);
    }
    return lastPage;
  }

  public long allocate(final OAtomicOperation atomicOperation) throws IOException {
    OCacheEntry cacheEntry;
    boolean clear = false;

    final OCacheEntry entryPointEntry = loadPageForWrite(atomicOperation, fileId, 0, false, true);
    try {
      final MapEntryPoint mapEntryPoint = new MapEntryPoint(entryPointEntry);
      final int lastPage = mapEntryPoint.getFileSize();
      long filledUpTo = getFilledUpTo(atomicOperation, fileId);
      assert lastPage <= filledUpTo - 1;

      if (lastPage == 0) {
        if (lastPage == filledUpTo - 1) {
          cacheEntry = addPage(atomicOperation, fileId);
          filledUpTo++;
        } else {
          cacheEntry = loadPageForWrite(atomicOperation, fileId, lastPage + 1, false, false);
        }
        mapEntryPoint.setFileSize(lastPage + 1);
        clear = true;
      } else {
        cacheEntry = loadPageForWrite(atomicOperation, fileId, lastPage, false, true);
      }

      try {
        OVersionPositionMapBucket bucket = new OVersionPositionMapBucket(cacheEntry);
        if (clear) {
          bucket.init();
        }

        if (bucket.isFull()) {
          releasePageFromWrite(atomicOperation, cacheEntry);
          assert lastPage <= filledUpTo - 1;

          if (lastPage == filledUpTo - 1) {
            cacheEntry = addPage(atomicOperation, fileId);
          } else {
            cacheEntry = loadPageForWrite(atomicOperation, fileId, lastPage + 1, false, false);
          }
          mapEntryPoint.setFileSize(lastPage + 1);

          bucket = new OVersionPositionMapBucket(cacheEntry);
          bucket.init();
        }
        final long index = bucket.allocate();
        return index + (cacheEntry.getPageIndex() - 1) * OVersionPositionMapBucket.MAX_ENTRIES;
      } finally {
        releasePageFromWrite(atomicOperation, cacheEntry);
      }
    } finally {
      releasePageFromWrite(atomicOperation, entryPointEntry);
    }
  }

  public void update(
      final long clusterPosition,
      final OVersionPositionMapBucket.PositionEntry entry,
      final OAtomicOperation atomicOperation)
      throws IOException {

    // TODO: [DR] calculate version
    final long pageIndex = clusterPosition / OVersionPositionMapBucket.MAX_ENTRIES + 1;
    final int index = (int) (clusterPosition % OVersionPositionMapBucket.MAX_ENTRIES);

    final long lastPage = getLastPage(atomicOperation);
    if (pageIndex > lastPage) {
      throw new OVersionPositionMapException(
          "Passed in cluster position "
              + clusterPosition
              + " is outside of range of cluster-position map",
          this);
    }
    final OCacheEntry cacheEntry =
        loadPageForWrite(atomicOperation, fileId, pageIndex, false, true);
    try {
      final OVersionPositionMapBucket bucket = new OVersionPositionMapBucket(cacheEntry);
      bucket.set(index, entry);
    } finally {
      releasePageFromWrite(atomicOperation, cacheEntry);
    }
  }

  public OVersionPositionMapBucket.PositionEntry get(
      final long clusterPosition, int pageCount, final OAtomicOperation atomicOperation)
      throws IOException {
    final long pageIndex = clusterPosition / OVersionPositionMapBucket.MAX_ENTRIES + 1;
    final int index = (int) (clusterPosition % OVersionPositionMapBucket.MAX_ENTRIES);
    final long lastPage = getLastPage(atomicOperation);

    if (pageIndex > lastPage) {
      return null;
    }
    pageCount = (int) Math.min(lastPage - pageIndex + 1, pageCount);

    final OCacheEntry cacheEntry =
        loadPageForRead(atomicOperation, fileId, pageIndex, false, pageCount);
    try {
      final OVersionPositionMapBucket bucket = new OVersionPositionMapBucket(cacheEntry);
      return bucket.get(index);
    } finally {
      releasePageFromRead(atomicOperation, cacheEntry);
    }
  }

  public void remove(final long clusterPosition, final OAtomicOperation atomicOperation)
      throws IOException {
    final long pageIndex = clusterPosition / OVersionPositionMapBucket.MAX_ENTRIES + 1;
    final int index = (int) (clusterPosition % OVersionPositionMapBucket.MAX_ENTRIES);

    final OCacheEntry cacheEntry =
        loadPageForWrite(atomicOperation, fileId, pageIndex, false, true);
    try {
      final OVersionPositionMapBucket bucket = new OVersionPositionMapBucket(cacheEntry);
      bucket.remove(index);
    } finally {
      releasePageFromWrite(atomicOperation, cacheEntry);
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
    long pageIndex = realPosition / OVersionPositionMapBucket.MAX_ENTRIES + 1;
    int index = (int) (realPosition % OVersionPositionMapBucket.MAX_ENTRIES);

    final long lastPage = getLastPage(atomicOperation);

    if (pageIndex > lastPage) {
      return new OClusterPositionEntry[] {};
    }

    OClusterPositionEntry[] result = null;
    do {
      final OCacheEntry cacheEntry = loadPageForRead(atomicOperation, fileId, pageIndex, false, 1);

      final OVersionPositionMapBucket bucket = new OVersionPositionMapBucket(cacheEntry);
      final int resultSize = bucket.getSize() - index;

      if (resultSize <= 0) {
        releasePageFromRead(atomicOperation, cacheEntry);
        pageIndex++;
        index = 0;
      } else {
        int entriesCount = 0;
        // final long startIndex =
        //     cacheEntry.getPageIndex() * OVersionPositionMapBucket.MAX_ENTRIES + index;

        result = new OClusterPositionEntry[resultSize];
        for (int i = 0; i < resultSize; i++) {
          if (bucket.exists(i + index)) {
            final OVersionPositionMapBucket.PositionEntry val = bucket.get(i + index);
            assert val != null;
            result[entriesCount] =
                new OClusterPositionEntry(index + i, val.getPageIndex(), val.getRecordPosition());
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
        releasePageFromRead(atomicOperation, cacheEntry);
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
    long pageIndex = clusterPosition / OVersionPositionMapBucket.MAX_ENTRIES + 1;
    int index = (int) (clusterPosition % OVersionPositionMapBucket.MAX_ENTRIES);

    final long lastPage = getLastPage(atomicOperation);

    if (pageIndex > lastPage) {
      return OCommonConst.EMPTY_LONG_ARRAY;
    }
    long[] result = null;
    do {
      final OCacheEntry cacheEntry = loadPageForRead(atomicOperation, fileId, pageIndex, false, 1);

      final OVersionPositionMapBucket bucket = new OVersionPositionMapBucket(cacheEntry);
      final int resultSize = bucket.getSize() - index;

      if (resultSize <= 0) {
        releasePageFromRead(atomicOperation, cacheEntry);
        pageIndex++;
        index = 0;
      } else {
        int entriesCount = 0;
        final long startIndex =
            cacheEntry.getPageIndex() * OVersionPositionMapBucket.MAX_ENTRIES + index;

        result = new long[resultSize];
        for (int i = 0; i < resultSize; i++) {
          if (bucket.exists(i + index)) {
            result[entriesCount] = startIndex + i - OVersionPositionMapBucket.MAX_ENTRIES;
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
        releasePageFromRead(atomicOperation, cacheEntry);
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
    long pageIndex = clusterPosition / OVersionPositionMapBucket.MAX_ENTRIES + 1;
    int index = (int) (clusterPosition % OVersionPositionMapBucket.MAX_ENTRIES);

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
      final OCacheEntry cacheEntry = loadPageForRead(atomicOperation, fileId, pageIndex, false, 1);

      final OVersionPositionMapBucket bucket = new OVersionPositionMapBucket(cacheEntry);
      if (index == Integer.MIN_VALUE) {
        index = bucket.getSize() - 1;
      }

      final int resultSize = index + 1;
      int entriesCount = 0;

      final long startPosition = cacheEntry.getPageIndex() * OVersionPositionMapBucket.MAX_ENTRIES;
      result = new long[resultSize];

      for (int i = 0; i < resultSize; i++) {
        if (bucket.exists(i)) {
          result[entriesCount] = startPosition + i - OVersionPositionMapBucket.MAX_ENTRIES;
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

      releasePageFromRead(atomicOperation, cacheEntry);
    } while (result == null && pageIndex >= 0);

    if (result == null) {
      result = OCommonConst.EMPTY_LONG_ARRAY;
    }
    return result;
  }

  long getFirstPosition(final OAtomicOperation atomicOperation) throws IOException {
    final long lastPage = getLastPage(atomicOperation);

    for (long pageIndex = 1; pageIndex <= lastPage; pageIndex++) {
      final OCacheEntry cacheEntry = loadPageForRead(atomicOperation, fileId, pageIndex, false, 1);
      try {
        final OVersionPositionMapBucket bucket = new OVersionPositionMapBucket(cacheEntry);
        final int bucketSize = bucket.getSize();

        for (int index = 0; index < bucketSize; index++) {
          if (bucket.exists(index)) {
            return pageIndex * OVersionPositionMapBucket.MAX_ENTRIES
                + index
                - OVersionPositionMapBucket.MAX_ENTRIES;
          }
        }
      } finally {
        releasePageFromRead(atomicOperation, cacheEntry);
      }
    }
    return ORID.CLUSTER_POS_INVALID;
  }

  public byte getStatus(final long clusterPosition, final OAtomicOperation atomicOperation)
      throws IOException {
    final long pageIndex =
        (clusterPosition + OVersionPositionMapBucket.MAX_ENTRIES)
            / OVersionPositionMapBucket.MAX_ENTRIES;
    final int index = (int) (clusterPosition % OVersionPositionMapBucket.MAX_ENTRIES);

    final long lastPage = getLastPage(atomicOperation);
    if (pageIndex > lastPage) {
      return OVersionPositionMapBucket.NOT_EXISTENT;
    }

    final OCacheEntry cacheEntry = loadPageForRead(atomicOperation, fileId, pageIndex, false, 1);
    try {
      final OVersionPositionMapBucket bucket = new OVersionPositionMapBucket(cacheEntry);
      return bucket.getStatus(index);
    } finally {
      releasePageFromRead(atomicOperation, cacheEntry);
    }
  }

  public long getLastPosition(final OAtomicOperation atomicOperation) throws IOException {
    final long lastPage = getLastPage(atomicOperation);

    for (long pageIndex = lastPage; pageIndex >= 1; pageIndex--) {
      final OCacheEntry cacheEntry = loadPageForRead(atomicOperation, fileId, pageIndex, false, 1);
      try {
        final OVersionPositionMapBucket bucket = new OVersionPositionMapBucket(cacheEntry);
        final int bucketSize = bucket.getSize();
        for (int index = bucketSize - 1; index >= 0; index--) {
          if (bucket.exists(index)) {
            return pageIndex * OVersionPositionMapBucket.MAX_ENTRIES
                + index
                - OVersionPositionMapBucket.MAX_ENTRIES;
          }
        }
      } finally {
        releasePageFromRead(atomicOperation, cacheEntry);
      }
    }
    return ORID.CLUSTER_POS_INVALID;
  }

  /** Returns the next position available. */
  long getNextPosition(final OAtomicOperation atomicOperation) throws IOException {
    final long pageIndex = getLastPage(atomicOperation);
    final OCacheEntry cacheEntry = loadPageForRead(atomicOperation, fileId, pageIndex, false, 1);
    try {
      final OVersionPositionMapBucket bucket = new OVersionPositionMapBucket(cacheEntry);
      final int bucketSize = bucket.getSize();
      return pageIndex * OVersionPositionMapBucket.MAX_ENTRIES + bucketSize;
    } finally {
      releasePageFromRead(atomicOperation, cacheEntry);
    }
  }

  public long getFileId() {
    return fileId;
  }

  /* void replaceFileId(final long newFileId) {
    this.fileId = newFileId;
  }*/

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
