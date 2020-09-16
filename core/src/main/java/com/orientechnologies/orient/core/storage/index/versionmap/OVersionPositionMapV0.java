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
import com.orientechnologies.orient.core.storage.version.OVersionPage;

import java.io.IOException;
import java.util.Arrays;

import static com.orientechnologies.orient.core.config.OGlobalConfiguration.DISK_CACHE_PAGE_SIZE;
import static com.orientechnologies.orient.core.config.OGlobalConfiguration.PAGINATED_STORAGE_LOWEST_FREELIST_BOUNDARY;

public final class OVersionPositionMapV0 extends OVersionPositionMap {
  private static final int STATE_ENTRY_INDEX = 0;
  private static final int DISK_PAGE_SIZE = DISK_CACHE_PAGE_SIZE.getValueAsInteger();
  private static final int LOWEST_FREELIST_BOUNDARY =
      PAGINATED_STORAGE_LOWEST_FREELIST_BOUNDARY.getValueAsInteger();
  private static final int FREE_LIST_SIZE = DISK_PAGE_SIZE - LOWEST_FREELIST_BOUNDARY;

  private long fileId;

  // TODO: move to VPM
  private final int[] keyVersions;
  private static final int DEFAULT_VERSION = 0;
  private static final int CONCURRENT_DISTRIBUTED_TRANSACTIONS = 1000;
  private static final int SAFETY_FILL_FACTOR = 10;
  private static final int DEFAULT_VERSION_ARRAY_SIZE =
      CONCURRENT_DISTRIBUTED_TRANSACTIONS * SAFETY_FILL_FACTOR;

  public OVersionPositionMapV0(
      final OAbstractPaginatedStorage storage,
      final String name,
      final String lockName,
      final String extension) {
    super(storage, name, extension, lockName);

    // TODO: [DR] merge in[] into versionPositionMap
    keyVersions = new int[DEFAULT_VERSION_ARRAY_SIZE];
    for (int i = 0; i < DEFAULT_VERSION_ARRAY_SIZE; i++) {
      keyVersions[i] = DEFAULT_VERSION;
    }
  }

  @Override
  public void create(final OAtomicOperation atomicOperation) {
    executeInsideComponentOperation(
        atomicOperation,
        operation -> {
          acquireExclusiveLock();
          try {
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
            // TODO: [DR]
            // final long entries = getEntries();
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
  public void updateVersion(final int hash) {
    // TODO: [DR] better use the existing update method?
    final int version = ++keyVersions[hash];
    keyVersions[hash] = version;
    System.out.println("update: " + hash + ", " + version);
  }

  @Override
  public int getVersion(final int hash) {
    final int version = keyVersions[hash];
    System.out.println("get: " + hash + ", " + version);
    return version;
  }

  public void openVPM(final OAtomicOperation atomicOperation) throws IOException {
    fileId = openFile(atomicOperation, getFullName());
  }

  public void createVPM(final OAtomicOperation atomicOperation) throws IOException {
    fileId = addFile(atomicOperation, getFullName());
    if (getFilledUpTo(atomicOperation, fileId) == 0) {
      // first one page is added for the meta data
      addInitializedPage(atomicOperation);

      // then let us add several empty data pages
      int numberOfPages = (int) Math.ceil((1000*10*4) / OVersionPage.PAGE_SIZE);
      numberOfPages = (numberOfPages == 0) ? 1 : numberOfPages;
      for (int i = 0; i < numberOfPages; i++) {
        addInitializedPage(atomicOperation);
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

  private void addInitializedPage(final OAtomicOperation atomicOperation) throws IOException {
    final OCacheEntry cacheEntry = addPage(atomicOperation, fileId);
    try {
      final MapEntryPoint mapEntryPoint = new MapEntryPoint(cacheEntry);
      mapEntryPoint.setFileSize(0);
    } finally {
      releasePageFromWrite(atomicOperation, cacheEntry);
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
      final long versionPosition,
      final OVersionPositionMapBucket.PositionEntry entry,
      final OAtomicOperation atomicOperation)
      throws IOException {

    // TODO: [DR] calculate version
    final long pageIndex = versionPosition / OVersionPositionMapBucket.MAX_ENTRIES + 1;
    final int index = (int) (versionPosition % OVersionPositionMapBucket.MAX_ENTRIES);

    final long lastPage = getLastPage(atomicOperation);
    if (pageIndex > lastPage) {
      throw new OVersionPositionMapException(
          "Passed in version position "
              + versionPosition
              + " is outside of range of version-position map",
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
      final long versionPosition, int pageCount, final OAtomicOperation atomicOperation)
      throws IOException {
    final long pageIndex = versionPosition / OVersionPositionMapBucket.MAX_ENTRIES + 1;
    final int index = (int) (versionPosition % OVersionPositionMapBucket.MAX_ENTRIES);
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

  public void remove(final long versionPosition, final OAtomicOperation atomicOperation)
      throws IOException {
    final long pageIndex = versionPosition / OVersionPositionMapBucket.MAX_ENTRIES + 1;
    final int index = (int) (versionPosition % OVersionPositionMapBucket.MAX_ENTRIES);

    final OCacheEntry cacheEntry =
        loadPageForWrite(atomicOperation, fileId, pageIndex, false, true);
    try {
      final OVersionPositionMapBucket bucket = new OVersionPositionMapBucket(cacheEntry);
      bucket.remove(index);
    } finally {
      releasePageFromWrite(atomicOperation, cacheEntry);
    }
  }

  long[] higherPositions(final long versionPosition, final OAtomicOperation atomicOperation)
      throws IOException {
    if (versionPosition == Long.MAX_VALUE) {
      return OCommonConst.EMPTY_LONG_ARRAY;
    }
    return ceilingPositions(versionPosition + 1, atomicOperation);
  }

  OVersionPositionEntry[] higherPositionsEntries(
      final long versionPosition, final OAtomicOperation atomicOperation) throws IOException {
    if (versionPosition == Long.MAX_VALUE) {
      return new OVersionPositionEntry[] {};
    }

    final long realPosition;
    if (versionPosition < 0) {
      realPosition = 0;
    } else {
      realPosition = versionPosition + 1;
    }
    long pageIndex = realPosition / OVersionPositionMapBucket.MAX_ENTRIES + 1;
    int index = (int) (realPosition % OVersionPositionMapBucket.MAX_ENTRIES);

    final long lastPage = getLastPage(atomicOperation);

    if (pageIndex > lastPage) {
      return new OVersionPositionEntry[] {};
    }

    OVersionPositionEntry[] result = null;
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

        result = new OVersionPositionEntry[resultSize];
        for (int i = 0; i < resultSize; i++) {
          if (bucket.exists(i + index)) {
            final OVersionPositionMapBucket.PositionEntry val = bucket.get(i + index);
            assert val != null;
            result[entriesCount] =
                new OVersionPositionEntry(index + i, val.getPageIndex(), val.getRecordPosition());
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
      result = new OVersionPositionEntry[] {};
    }
    return result;
  }

  long[] ceilingPositions(long versionPosition, final OAtomicOperation atomicOperation)
      throws IOException {
    if (versionPosition < 0) {
      versionPosition = 0;
    }
    long pageIndex = versionPosition / OVersionPositionMapBucket.MAX_ENTRIES + 1;
    int index = (int) (versionPosition % OVersionPositionMapBucket.MAX_ENTRIES);

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

  long[] lowerPositions(final long versionPosition, final OAtomicOperation atomicOperation)
      throws IOException {
    if (versionPosition == 0) {
      return OCommonConst.EMPTY_LONG_ARRAY;
    }
    return floorPositions(versionPosition - 1, atomicOperation);
  }

  long[] floorPositions(final long versionPosition, final OAtomicOperation atomicOperation)
      throws IOException {
    if (versionPosition < 0) {
      return OCommonConst.EMPTY_LONG_ARRAY;
    }
    long pageIndex = versionPosition / OVersionPositionMapBucket.MAX_ENTRIES + 1;
    int index = (int) (versionPosition % OVersionPositionMapBucket.MAX_ENTRIES);

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
    return ORID.VERSION_POS_INVALID;
  }

  public byte getStatus(final long versionPosition, final OAtomicOperation atomicOperation)
      throws IOException {
    final long pageIndex =
        (versionPosition + OVersionPositionMapBucket.MAX_ENTRIES)
            / OVersionPositionMapBucket.MAX_ENTRIES;
    final int index = (int) (versionPosition % OVersionPositionMapBucket.MAX_ENTRIES);

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
    return ORID.VERSION_POS_INVALID;
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

  public static final class OVersionPositionEntry {
    private final long position;
    private final long page;
    private final int offset;

    OVersionPositionEntry(final long position, final long page, final int offset) {
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
