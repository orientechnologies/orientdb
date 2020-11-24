/*
 * Copyright 2010-2013 OrientDB LTD (info--at--orientdb.com)
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
package com.orientechnologies.orient.core.storage.cluster.v2;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.config.OStorageClusterConfiguration;
import com.orientechnologies.orient.core.config.OStoragePaginatedClusterConfiguration;
import com.orientechnologies.orient.core.conflict.ORecordConflictStrategy;
import com.orientechnologies.orient.core.exception.NotEmptyComponentCanNotBeRemovedException;
import com.orientechnologies.orient.core.exception.OPaginatedClusterException;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.OMetadataInternal;
import com.orientechnologies.orient.core.storage.OPhysicalPosition;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cluster.OClusterPage;
import com.orientechnologies.orient.core.storage.cluster.OClusterPageDebug;
import com.orientechnologies.orient.core.storage.cluster.OClusterPositionMap;
import com.orientechnologies.orient.core.storage.cluster.OClusterPositionMapBucket;
import com.orientechnologies.orient.core.storage.cluster.OPaginatedCluster;
import com.orientechnologies.orient.core.storage.cluster.OPaginatedClusterDebug;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.OClusterBrowseEntry;
import com.orientechnologies.orient.core.storage.impl.local.OClusterBrowsePage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 10/7/13
 */
public final class OPaginatedClusterV2 extends OPaginatedCluster {

  private static final int STATE_ENTRY_INDEX = 0;
  private static final int BINARY_VERSION = 2;

  private static final int PAGE_INDEX_OFFSET = 16;
  private static final int RECORD_POSITION_MASK = 0xFFFF;

  private final boolean systemCluster;
  private final OClusterPositionMapV2 clusterPositionMap;
  private final FreeSpaceMap freeSpaceMap;
  private final String storageName;

  private volatile int id;
  private long fileId;
  private ORecordConflictStrategy recordConflictStrategy;

  private static final class AddEntryResult {

    private final long pageIndex;
    private final int pagePosition;

    private final int recordVersion;
    private final int recordsSizeDiff;

    private AddEntryResult(
        final long pageIndex,
        final int pagePosition,
        final int recordVersion,
        final int recordsSizeDiff) {
      this.pageIndex = pageIndex;
      this.pagePosition = pagePosition;
      this.recordVersion = recordVersion;
      this.recordsSizeDiff = recordsSizeDiff;
    }
  }

  public OPaginatedClusterV2(final String name, final OAbstractPaginatedStorage storage) {
    this(
        name,
        OPaginatedCluster.DEF_EXTENSION,
        OClusterPositionMap.DEF_EXTENSION,
        FreeSpaceMap.DEF_EXTENSION,
        storage);
  }

  public OPaginatedClusterV2(
      final String name,
      final String dataExtension,
      final String cpmExtension,
      final String fsmExtension,
      final OAbstractPaginatedStorage storage) {
    super(storage, name, dataExtension, name + dataExtension);

    systemCluster = OMetadataInternal.SYSTEM_CLUSTER.contains(name);
    clusterPositionMap = new OClusterPositionMapV2(storage, getName(), getFullName(), cpmExtension);
    freeSpaceMap = new FreeSpaceMap(storage, name, fsmExtension, getFullName());
    storageName = storage.getName();
  }

  @Override
  public void configure(final int id, final String clusterName) throws IOException {
    acquireExclusiveLock();
    try {
      init(id, clusterName, null);
    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  public boolean exists() {
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        final OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();
        return isFileExists(atomicOperation, getFullName());
      } finally {
        releaseSharedLock();
      }
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  @Override
  public int getBinaryVersion() {
    return BINARY_VERSION;
  }

  @Override
  public OStoragePaginatedClusterConfiguration generateClusterConfig() {
    acquireSharedLock();
    try {
      return new OStoragePaginatedClusterConfiguration(
          id,
          getName(),
          null,
          true,
          OStoragePaginatedClusterConfiguration.DEFAULT_GROW_FACTOR,
          OStoragePaginatedClusterConfiguration.DEFAULT_GROW_FACTOR,
          null,
          null,
          null,
          Optional.ofNullable(recordConflictStrategy)
              .map(ORecordConflictStrategy::getName)
              .orElse(null),
          OStorageClusterConfiguration.STATUS.ONLINE,
          BINARY_VERSION);

    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public void configure(final OStorage storage, final OStorageClusterConfiguration config)
      throws IOException {
    acquireExclusiveLock();
    try {
      init(
          config.getId(),
          config.getName(),
          ((OStoragePaginatedClusterConfiguration) config).conflictStrategy);
    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  public void create(OAtomicOperation atomicOperation) {
    executeInsideComponentOperation(
        atomicOperation,
        operation -> {
          acquireExclusiveLock();
          try {
            fileId = addFile(atomicOperation, getFullName());
            initCusterState(atomicOperation);
            clusterPositionMap.create(atomicOperation);
            freeSpaceMap.create(atomicOperation);
          } finally {
            releaseExclusiveLock();
          }
        });
  }

  @Override
  public void open(OAtomicOperation atomicOperation) {
    executeInsideComponentOperation(
        atomicOperation,
        operation -> {
          acquireExclusiveLock();
          try {
            fileId = openFile(atomicOperation, getFullName());
            clusterPositionMap.open(atomicOperation);
            if (freeSpaceMap.exists(atomicOperation)) {
              freeSpaceMap.open(atomicOperation);
            } else {
              OLogManager.instance()
                  .infoNoDb(
                      this,
                      "Free space map is absent inside of %s cluster"
                          + " of storage %s . Information about free space present inside of each page will "
                          + "be recovered.",
                      getName(),
                      storageName);
              OLogManager.instance()
                  .infoNoDb(
                      this,
                      "Scanning of free space for cluster %s in storage %s started ...",
                      getName(),
                      storageName);

              freeSpaceMap.create(atomicOperation);
              final long filledUpTo = getFilledUpTo(atomicOperation, fileId);
              for (int pageIndex = 0; pageIndex < filledUpTo; pageIndex++) {
                final OCacheEntry cacheEntry =
                    loadPageForRead(atomicOperation, fileId, pageIndex, true);
                try {
                  final OClusterPage clusterPage = new OClusterPage(cacheEntry);
                  freeSpaceMap.updatePageFreeSpace(
                      atomicOperation, pageIndex, clusterPage.getMaxRecordSize());
                } finally {
                  releasePageFromRead(atomicOperation, cacheEntry);
                }

                if (pageIndex > 0 && pageIndex % 1_000 == 0) {
                  OLogManager.instance()
                      .infoNoDb(
                          this,
                          "%d pages out of %d (%d %) were processed in cluster %s ...",
                          pageIndex + 1,
                          filledUpTo,
                          100 * (pageIndex + 1) / filledUpTo,
                          getName());
                }
              }

              OLogManager.instance()
                  .infoNoDb(this, "Page scan for cluster %s " + "is completed.", getName());
            }
          } finally {
            releaseExclusiveLock();
          }
        });
  }

  @Override
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
      clusterPositionMap.close(flush);
    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  public void delete(OAtomicOperation atomicOperation) {
    executeInsideComponentOperation(
        atomicOperation,
        operation -> {
          acquireExclusiveLock();
          try {
            final long entries = getEntries();
            if (entries > 0) {
              throw new NotEmptyComponentCanNotBeRemovedException(
                  getName()
                      + " : Not empty cluster can not be deleted. Cluster has "
                      + entries
                      + " records");
            }
            deleteFile(atomicOperation, fileId);
            clusterPositionMap.delete(atomicOperation);
            freeSpaceMap.delete(atomicOperation);
          } finally {
            releaseExclusiveLock();
          }
        });
  }

  @Override
  public boolean isSystemCluster() {
    return systemCluster;
  }

  @Override
  public String compression() {
    acquireSharedLock();
    try {
      return null;
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public String encryption() {
    acquireSharedLock();
    try {
      return null;
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public OPhysicalPosition allocatePosition(
      final byte recordType, OAtomicOperation atomicOperation) {
    return calculateInsideComponentOperation(
        atomicOperation,
        operation -> {
          acquireExclusiveLock();
          try {
            return createPhysicalPosition(
                recordType, clusterPositionMap.allocate(atomicOperation), -1);
          } finally {
            releaseExclusiveLock();
          }
        });
  }

  @Override
  public OPhysicalPosition createRecord(
      final byte[] content,
      final int recordVersion,
      final byte recordType,
      final OPhysicalPosition allocatedPosition,
      final OAtomicOperation atomicOperation) {
    return calculateInsideComponentOperation(
        atomicOperation,
        operation -> {
          acquireExclusiveLock();
          try {
            final int entryContentLength = getEntryContentLength(content.length);

            if (entryContentLength < OClusterPage.MAX_RECORD_SIZE) {
              final byte[] entryContent = new byte[entryContentLength];

              int entryPosition = 0;
              entryContent[entryPosition] = recordType;
              entryPosition++;

              OIntegerSerializer.INSTANCE.serializeNative(
                  content.length, entryContent, entryPosition);
              entryPosition += OIntegerSerializer.INT_SIZE;

              System.arraycopy(content, 0, entryContent, entryPosition, content.length);
              entryPosition += content.length;

              entryContent[entryPosition] = 1;
              entryPosition++;

              OLongSerializer.INSTANCE.serializeNative(-1L, entryContent, entryPosition);

              final AddEntryResult addEntryResult =
                  addEntry(recordVersion, entryContent, atomicOperation);

              updateClusterState(1, addEntryResult.recordsSizeDiff, atomicOperation);

              final long clusterPosition;
              if (allocatedPosition != null) {
                clusterPositionMap.update(
                    allocatedPosition.clusterPosition,
                    new OClusterPositionMapBucket.PositionEntry(
                        addEntryResult.pageIndex, addEntryResult.pagePosition),
                    atomicOperation);
                clusterPosition = allocatedPosition.clusterPosition;
              } else {
                clusterPosition =
                    clusterPositionMap.add(
                        addEntryResult.pageIndex, addEntryResult.pagePosition, atomicOperation);
              }

              return createPhysicalPosition(
                  recordType, clusterPosition, addEntryResult.recordVersion);
            } else {
              final int entrySize =
                  content.length + OIntegerSerializer.INT_SIZE + OByteSerializer.BYTE_SIZE;

              int fullEntryPosition = 0;
              final byte[] fullEntry = new byte[entrySize];

              fullEntry[fullEntryPosition] = recordType;
              fullEntryPosition++;

              OIntegerSerializer.INSTANCE.serializeNative(
                  content.length, fullEntry, fullEntryPosition);
              fullEntryPosition += OIntegerSerializer.INT_SIZE;

              System.arraycopy(content, 0, fullEntry, fullEntryPosition, content.length);

              long prevPageRecordPointer = -1;
              long firstPageIndex = -1;
              int firstPagePosition = -1;

              int version = 0;

              int from = 0;
              int to =
                  from
                      + (OClusterPage.MAX_RECORD_SIZE
                          - OByteSerializer.BYTE_SIZE
                          - OLongSerializer.LONG_SIZE);

              int recordsSizeDiff = 0;

              do {
                final byte[] entryContent =
                    new byte[to - from + OByteSerializer.BYTE_SIZE + OLongSerializer.LONG_SIZE];
                System.arraycopy(fullEntry, from, entryContent, 0, to - from);

                if (from > 0) {
                  entryContent[
                          entryContent.length
                              - OLongSerializer.LONG_SIZE
                              - OByteSerializer.BYTE_SIZE] =
                      0;
                } else {
                  entryContent[
                          entryContent.length
                              - OLongSerializer.LONG_SIZE
                              - OByteSerializer.BYTE_SIZE] =
                      1;
                }

                OLongSerializer.INSTANCE.serializeNative(
                    -1L, entryContent, entryContent.length - OLongSerializer.LONG_SIZE);

                final AddEntryResult addEntryResult =
                    addEntry(recordVersion, entryContent, atomicOperation);
                recordsSizeDiff += addEntryResult.recordsSizeDiff;

                if (firstPageIndex == -1) {
                  firstPageIndex = addEntryResult.pageIndex;
                  firstPagePosition = addEntryResult.pagePosition;
                  version = addEntryResult.recordVersion;
                }

                final long addedPagePointer =
                    createPagePointer(addEntryResult.pageIndex, addEntryResult.pagePosition);
                if (prevPageRecordPointer >= 0) {
                  final long prevPageIndex = getPageIndex(prevPageRecordPointer);
                  final int prevPageRecordPosition = getRecordPosition(prevPageRecordPointer);

                  final OCacheEntry prevPageCacheEntry =
                      loadPageForWrite(atomicOperation, fileId, prevPageIndex, false, true);
                  try {
                    final OClusterPage prevPage = new OClusterPage(prevPageCacheEntry);
                    prevPage.setRecordLongValue(
                        prevPageRecordPosition, -OLongSerializer.LONG_SIZE, addedPagePointer);
                  } finally {
                    releasePageFromWrite(atomicOperation, prevPageCacheEntry);
                  }
                }

                prevPageRecordPointer = addedPagePointer;
                from = to;
                to =
                    to
                        + (OClusterPage.MAX_RECORD_SIZE
                            - OLongSerializer.LONG_SIZE
                            - OByteSerializer.BYTE_SIZE);
                if (to > fullEntry.length) {
                  to = fullEntry.length;
                }

              } while (from < to);

              updateClusterState(1, recordsSizeDiff, atomicOperation);
              final long clusterPosition;
              if (allocatedPosition != null) {
                clusterPositionMap.update(
                    allocatedPosition.clusterPosition,
                    new OClusterPositionMapBucket.PositionEntry(firstPageIndex, firstPagePosition),
                    atomicOperation);
                clusterPosition = allocatedPosition.clusterPosition;
              } else {
                clusterPosition =
                    clusterPositionMap.add(firstPageIndex, firstPagePosition, atomicOperation);
              }

              return createPhysicalPosition(recordType, clusterPosition, version);
            }
          } finally {
            releaseExclusiveLock();
          }
        });
  }

  private static int getEntryContentLength(final int grownContentSize) {

    return grownContentSize
        + 2 * OByteSerializer.BYTE_SIZE
        + OIntegerSerializer.INT_SIZE
        + OLongSerializer.LONG_SIZE;
  }

  @Override
  public ORawBuffer readRecord(final long clusterPosition, final boolean prefetchRecords)
      throws IOException {
    int pagesToPrefetch = 1;

    if (prefetchRecords) {
      pagesToPrefetch = OGlobalConfiguration.QUERY_SCAN_PREFETCH_PAGES.getValueAsInteger();
    }
    return readRecord(clusterPosition, pagesToPrefetch);
  }

  private ORawBuffer readRecord(final long clusterPosition, final int pageCount)
      throws IOException {
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        final OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();

        final OClusterPositionMapBucket.PositionEntry positionEntry =
            clusterPositionMap.get(clusterPosition, pageCount, atomicOperation);
        if (positionEntry == null) {
          return null;
        }
        return internalReadRecord(
            clusterPosition,
            positionEntry.getPageIndex(),
            positionEntry.getRecordPosition(),
            pageCount,
            atomicOperation);
      } finally {
        releaseSharedLock();
      }
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  private ORawBuffer internalReadRecord(
      final long clusterPosition,
      final long pageIndex,
      final int recordPosition,
      int pageCount,
      final OAtomicOperation atomicOperation)
      throws IOException {
    if (pageCount > 1) {
      final OCacheEntry stateCacheEntry =
          loadPageForRead(atomicOperation, fileId, STATE_ENTRY_INDEX, false);
      try {
        final OPaginatedClusterStateV2 state = new OPaginatedClusterStateV2(stateCacheEntry);
        pageCount = (int) Math.min(state.getFileSize() + 1 - pageIndex, pageCount);
      } finally {
        releasePageFromRead(atomicOperation, stateCacheEntry);
      }
    }

    int recordVersion;
    final OCacheEntry cacheEntry =
        loadPageForRead(atomicOperation, fileId, pageIndex, false, pageCount);
    try {
      final OClusterPage localPage = new OClusterPage(cacheEntry);
      recordVersion = localPage.getRecordVersion(recordPosition);
    } finally {
      releasePageFromRead(atomicOperation, cacheEntry);
    }

    final byte[] fullContent =
        readFullEntry(clusterPosition, pageIndex, recordPosition, atomicOperation, pageCount);
    if (fullContent == null) {
      return null;
    }

    int fullContentPosition = 0;

    final byte recordType = fullContent[fullContentPosition];
    fullContentPosition++;

    final int readContentSize =
        OIntegerSerializer.INSTANCE.deserializeNative(fullContent, fullContentPosition);
    fullContentPosition += OIntegerSerializer.INT_SIZE;

    byte[] recordContent =
        Arrays.copyOfRange(fullContent, fullContentPosition, fullContentPosition + readContentSize);

    return new ORawBuffer(recordContent, recordVersion, recordType);
  }

  @Override
  public ORawBuffer readRecordIfVersionIsNotLatest(
      final long clusterPosition, final int recordVersion)
      throws IOException, ORecordNotFoundException {
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        final OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();
        final OClusterPositionMapBucket.PositionEntry positionEntry =
            clusterPositionMap.get(clusterPosition, 1, atomicOperation);

        if (positionEntry == null) {
          throw new ORecordNotFoundException(
              new ORecordId(id, clusterPosition),
              "Record for cluster with id "
                  + id
                  + " and position "
                  + clusterPosition
                  + " is absent.");
        }

        final int recordPosition = positionEntry.getRecordPosition();
        final long pageIndex = positionEntry.getPageIndex();

        int loadedRecordVersion;
        final OCacheEntry cacheEntry = loadPageForRead(atomicOperation, fileId, pageIndex, false);
        try {
          final OClusterPage localPage = new OClusterPage(cacheEntry);
          if (localPage.isDeleted(recordPosition)) {
            throw new ORecordNotFoundException(
                new ORecordId(id, clusterPosition),
                "Record for cluster with id "
                    + id
                    + " and position "
                    + clusterPosition
                    + " is absent.");
          }

          loadedRecordVersion = localPage.getRecordVersion(recordPosition);
        } finally {
          releasePageFromRead(atomicOperation, cacheEntry);
        }

        if (loadedRecordVersion > recordVersion) {
          return readRecord(clusterPosition, false);
        }

        return null;
      } finally {
        releaseSharedLock();
      }
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  @Override
  public boolean deleteRecord(OAtomicOperation atomicOperation, final long clusterPosition) {
    return calculateInsideComponentOperation(
        atomicOperation,
        operation -> {
          acquireExclusiveLock();
          try {
            final OClusterPositionMapBucket.PositionEntry positionEntry =
                clusterPositionMap.get(clusterPosition, 1, atomicOperation);
            if (positionEntry == null) {
              return false;
            }

            long pageIndex = positionEntry.getPageIndex();
            int recordPosition = positionEntry.getRecordPosition();

            long nextPagePointer;
            int removedContentSize = 0;

            do {
              boolean cacheEntryReleased = false;
              final int maxRecordSize;
              OCacheEntry cacheEntry =
                  loadPageForWrite(atomicOperation, fileId, pageIndex, false, true);
              try {
                OClusterPage localPage = new OClusterPage(cacheEntry);

                if (localPage.isDeleted(recordPosition)) {
                  if (removedContentSize == 0) {
                    cacheEntryReleased = true;
                    releasePageFromWrite(atomicOperation, cacheEntry);
                    return false;
                  } else {
                    throw new OPaginatedClusterException(
                        "Content of record " + new ORecordId(id, clusterPosition) + " was broken",
                        this);
                  }
                } else if (removedContentSize == 0) {
                  releasePageFromWrite(atomicOperation, cacheEntry);

                  cacheEntry = loadPageForWrite(atomicOperation, fileId, pageIndex, false, true);

                  localPage = new OClusterPage(cacheEntry);
                }

                final int initialFreeSpace = localPage.getFreeSpace();
                final byte[] content = localPage.deleteRecord(recordPosition, true);
                atomicOperation.addDeletedRecordPosition(
                    id, cacheEntry.getPageIndex(), recordPosition);
                assert content != null;

                maxRecordSize = localPage.getMaxRecordSize();
                removedContentSize += localPage.getFreeSpace() - initialFreeSpace;
                nextPagePointer =
                    OLongSerializer.INSTANCE.deserializeNative(
                        content, content.length - OLongSerializer.LONG_SIZE);
              } finally {
                if (!cacheEntryReleased) {
                  releasePageFromWrite(atomicOperation, cacheEntry);
                }
              }

              freeSpaceMap.updatePageFreeSpace(atomicOperation, (int) pageIndex, maxRecordSize);

              pageIndex = getPageIndex(nextPagePointer);
              recordPosition = getRecordPosition(nextPagePointer);
            } while (nextPagePointer >= 0);

            updateClusterState(-1, -removedContentSize, atomicOperation);

            clusterPositionMap.remove(clusterPosition, atomicOperation);
            return true;
          } finally {
            releaseExclusiveLock();
          }
        });
  }

  @Override
  public void updateRecord(
      final long clusterPosition,
      byte[] content,
      final int recordVersion,
      final byte recordType,
      OAtomicOperation atomicOperation) {
    executeInsideComponentOperation(
        atomicOperation,
        operation -> {
          acquireExclusiveLock();
          try {
            final OClusterPositionMapBucket.PositionEntry positionEntry =
                clusterPositionMap.get(clusterPosition, 1, atomicOperation);
            if (positionEntry == null) {
              return;
            }
            int nextRecordPosition = positionEntry.getRecordPosition();
            long nextPageIndex = positionEntry.getPageIndex();

            int newRecordPosition = -1;
            long newPageIndex = -1;

            long prevPageIndex = -1;
            int prevRecordPosition = -1;

            long nextEntryPointer = -1;
            int from = 0;
            int to;

            long sizeDiff = 0;
            byte[] updateEntry = null;

            do {
              final int entrySize;
              final int updatedEntryPosition;

              if (updateEntry == null) {
                if (from == 0) {
                  entrySize =
                      Math.min(getEntryContentLength(content.length), OClusterPage.MAX_RECORD_SIZE);
                  to =
                      entrySize
                          - (2 * OByteSerializer.BYTE_SIZE
                              + OIntegerSerializer.INT_SIZE
                              + OLongSerializer.LONG_SIZE);
                } else {
                  entrySize =
                      Math.min(
                          content.length
                              - from
                              + OByteSerializer.BYTE_SIZE
                              + OLongSerializer.LONG_SIZE,
                          OClusterPage.MAX_RECORD_SIZE);
                  to = from + entrySize - (OByteSerializer.BYTE_SIZE + OLongSerializer.LONG_SIZE);
                }

                updateEntry = new byte[entrySize];
                int entryPosition = 0;

                if (from == 0) {
                  updateEntry[entryPosition] = recordType;
                  entryPosition++;

                  OIntegerSerializer.INSTANCE.serializeNative(
                      content.length, updateEntry, entryPosition);
                  entryPosition += OIntegerSerializer.INT_SIZE;
                }

                System.arraycopy(content, from, updateEntry, entryPosition, to - from);
                entryPosition += to - from;

                if (nextPageIndex == positionEntry.getPageIndex()) {
                  updateEntry[entryPosition] = 1;
                }

                entryPosition++;

                OLongSerializer.INSTANCE.serializeNative(-1, updateEntry, entryPosition);

                assert to >= content.length || entrySize == OClusterPage.MAX_RECORD_SIZE;
              } else {
                entrySize = updateEntry.length;

                if (from == 0) {
                  to =
                      entrySize
                          - (2 * OByteSerializer.BYTE_SIZE
                              + OIntegerSerializer.INT_SIZE
                              + OLongSerializer.LONG_SIZE);
                } else {
                  to = from + entrySize - (OByteSerializer.BYTE_SIZE + OLongSerializer.LONG_SIZE);
                }
              }
              final boolean isNew;
              final boolean newFreePage;
              if (nextPageIndex < 0) {
                nextPageIndex = freeSpaceMap.findFreePage(entrySize);
                isNew = nextPageIndex < 0;
                newFreePage = true;
              } else {
                isNew = false;
                newFreePage = false;
              }

              final OCacheEntry cacheEntry;
              if (isNew) {
                final OCacheEntry stateCacheEntry =
                    loadPageForWrite(atomicOperation, fileId, STATE_ENTRY_INDEX, false, true);
                try {
                  final OPaginatedClusterStateV2 clusterState =
                      new OPaginatedClusterStateV2(stateCacheEntry);

                  final int fileSize = clusterState.getFileSize();
                  final long filledUpTo = getFilledUpTo(atomicOperation, fileId);

                  if (fileSize == filledUpTo - 1) {
                    cacheEntry = addPage(atomicOperation, fileId);
                  } else {
                    assert fileSize < filledUpTo - 1;

                    cacheEntry =
                        loadPageForWrite(atomicOperation, fileId, fileSize + 1, false, false);
                  }

                  clusterState.setFileSize(fileSize + 1);
                } finally {
                  releasePageFromWrite(atomicOperation, stateCacheEntry);
                }
              } else {
                cacheEntry = loadPageForWrite(atomicOperation, fileId, nextPageIndex, false, true);
              }

              final int maxRecordSize;
              try {
                final OClusterPage localPage = new OClusterPage(cacheEntry);
                if (isNew) {
                  localPage.init();
                } else {
                  assert !newFreePage || localPage.getMaxRecordSize() >= entrySize;
                }
                final int pageFreeSpace = localPage.getFreeSpace();

                if (nextRecordPosition >= 0) {
                  if (localPage.isDeleted(nextRecordPosition)) {
                    throw new OPaginatedClusterException(
                        "Record with rid " + new ORecordId(id, clusterPosition) + " was deleted",
                        this);
                  }

                  final int currentEntrySize = localPage.getRecordSize(nextRecordPosition);
                  nextEntryPointer =
                      localPage.getRecordLongValue(
                          nextRecordPosition, currentEntrySize - OLongSerializer.LONG_SIZE);

                  if (currentEntrySize == entrySize) {
                    localPage.replaceRecord(nextRecordPosition, updateEntry, recordVersion);
                    updatedEntryPosition = nextRecordPosition;
                  } else {
                    localPage.deleteRecord(nextRecordPosition, true);
                    atomicOperation.addDeletedRecordPosition(
                        id, cacheEntry.getPageIndex(), nextRecordPosition);

                    if (localPage.getMaxRecordSize() >= entrySize) {
                      updatedEntryPosition =
                          localPage.appendRecord(
                              recordVersion,
                              updateEntry,
                              -1,
                              atomicOperation.getBookedRecordPositions(
                                  id, cacheEntry.getPageIndex()));
                    } else {
                      updatedEntryPosition = -1;
                    }
                  }

                  if (nextEntryPointer >= 0) {
                    nextRecordPosition = getRecordPosition(nextEntryPointer);
                    nextPageIndex = getPageIndex(nextEntryPointer);
                  } else {
                    nextPageIndex = -1;
                    nextRecordPosition = -1;
                  }

                } else {
                  assert localPage.getFreeSpace() >= entrySize;
                  updatedEntryPosition =
                      localPage.appendRecord(
                          recordVersion,
                          updateEntry,
                          -1,
                          atomicOperation.getBookedRecordPositions(id, cacheEntry.getPageIndex()));

                  nextPageIndex = -1;
                  nextRecordPosition = -1;
                }

                final int updatedPageFreeSpace = localPage.getFreeSpace();
                maxRecordSize = localPage.getMaxRecordSize();
                sizeDiff += pageFreeSpace - updatedPageFreeSpace;

              } finally {
                releasePageFromWrite(atomicOperation, cacheEntry);
              }

              freeSpaceMap.updatePageFreeSpace(
                  atomicOperation, cacheEntry.getPageIndex(), maxRecordSize);

              if (updatedEntryPosition >= 0) {
                if (from == 0) {
                  newPageIndex = cacheEntry.getPageIndex();
                  newRecordPosition = updatedEntryPosition;
                }

                from = to;

                if (prevPageIndex >= 0) {
                  final OCacheEntry prevCacheEntry =
                      loadPageForWrite(atomicOperation, fileId, prevPageIndex, false, true);
                  try {
                    final OClusterPage prevPage = new OClusterPage(prevCacheEntry);
                    prevPage.setRecordLongValue(
                        prevRecordPosition,
                        -OLongSerializer.LONG_SIZE,
                        createPagePointer(cacheEntry.getPageIndex(), updatedEntryPosition));
                  } finally {
                    releasePageFromWrite(atomicOperation, prevCacheEntry);
                  }
                }

                prevPageIndex = cacheEntry.getPageIndex();
                prevRecordPosition = updatedEntryPosition;

                updateEntry = null;
              }
            } while (to < content.length || updateEntry != null);

            // clear unneeded pages
            while (nextEntryPointer >= 0) {
              nextPageIndex = getPageIndex(nextEntryPointer);
              nextRecordPosition = getRecordPosition(nextEntryPointer);

              final int maxRecodSize;
              final OCacheEntry cacheEntry =
                  loadPageForWrite(atomicOperation, fileId, nextPageIndex, false, true);
              try {
                final OClusterPage localPage = new OClusterPage(cacheEntry);
                final int freeSpace = localPage.getFreeSpace();

                nextEntryPointer =
                    localPage.getRecordLongValue(nextRecordPosition, -OLongSerializer.LONG_SIZE);
                localPage.deleteRecord(nextRecordPosition, true);
                atomicOperation.addDeletedRecordPosition(
                    id, cacheEntry.getPageIndex(), nextRecordPosition);

                final int pageFreeSpace = localPage.getFreeSpace();
                maxRecodSize = localPage.getMaxRecordSize();
                sizeDiff += freeSpace - pageFreeSpace;
              } finally {
                releasePageFromWrite(atomicOperation, cacheEntry);
              }

              freeSpaceMap.updatePageFreeSpace(atomicOperation, (int) nextPageIndex, maxRecodSize);
            }

            assert newPageIndex >= 0;
            assert newRecordPosition >= 0;

            if (newPageIndex != positionEntry.getPageIndex()
                || newRecordPosition != positionEntry.getRecordPosition()) {
              clusterPositionMap.update(
                  clusterPosition,
                  new OClusterPositionMapBucket.PositionEntry(newPageIndex, newRecordPosition),
                  atomicOperation);
            }

            updateClusterState(0, sizeDiff, atomicOperation);

          } finally {
            releaseExclusiveLock();
          }
        });
  }

  @Override
  public long getTombstonesCount() {
    return 0;
  }

  @Override
  public OPhysicalPosition getPhysicalPosition(final OPhysicalPosition position)
      throws IOException {
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        final OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();
        final long clusterPosition = position.clusterPosition;
        final OClusterPositionMapBucket.PositionEntry positionEntry =
            clusterPositionMap.get(clusterPosition, 1, atomicOperation);

        if (positionEntry == null) {
          return null;
        }

        final long pageIndex = positionEntry.getPageIndex();
        final int recordPosition = positionEntry.getRecordPosition();

        final OCacheEntry cacheEntry = loadPageForRead(atomicOperation, fileId, pageIndex, false);
        try {
          final OClusterPage localPage = new OClusterPage(cacheEntry);
          if (localPage.isDeleted(recordPosition)) {
            return null;
          }

          if (localPage.getRecordByteValue(
                  recordPosition, -OLongSerializer.LONG_SIZE - OByteSerializer.BYTE_SIZE)
              == 0) {
            return null;
          }

          final OPhysicalPosition physicalPosition = new OPhysicalPosition();
          physicalPosition.recordSize = -1;

          physicalPosition.recordType = localPage.getRecordByteValue(recordPosition, 0);
          physicalPosition.recordVersion = localPage.getRecordVersion(recordPosition);
          physicalPosition.clusterPosition = position.clusterPosition;

          return physicalPosition;
        } finally {
          releasePageFromRead(atomicOperation, cacheEntry);
        }
      } finally {
        releaseSharedLock();
      }
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  @Override
  public boolean isDeleted(final OPhysicalPosition position) throws IOException {
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        final OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();
        final long clusterPosition = position.clusterPosition;
        final OClusterPositionMapBucket.PositionEntry positionEntry =
            clusterPositionMap.get(clusterPosition, 1, atomicOperation);

        if (positionEntry == null) {
          return false;
        }

        final long pageIndex = positionEntry.getPageIndex();
        final int recordPosition = positionEntry.getRecordPosition();

        final OCacheEntry cacheEntry = loadPageForRead(atomicOperation, fileId, pageIndex, false);
        try {
          final OClusterPage localPage = new OClusterPage(cacheEntry);
          return localPage.isDeleted(recordPosition);
        } finally {
          releasePageFromRead(atomicOperation, cacheEntry);
        }
      } finally {
        releaseSharedLock();
      }
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  @Override
  public long getEntries() {
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        final OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();
        final OCacheEntry pinnedStateEntry =
            loadPageForRead(atomicOperation, fileId, STATE_ENTRY_INDEX, true);
        try {
          return new OPaginatedClusterStateV2(pinnedStateEntry).getSize();
        } finally {
          releasePageFromRead(atomicOperation, pinnedStateEntry);
        }
      } finally {
        releaseSharedLock();
      }
    } catch (final IOException ioe) {
      throw OException.wrapException(
          new OPaginatedClusterException(
              "Error during retrieval of size of '" + getName() + "' cluster", this),
          ioe);
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  @Override
  public long getFirstPosition() throws IOException {
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        final OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();
        return clusterPositionMap.getFirstPosition(atomicOperation);
      } finally {
        releaseSharedLock();
      }
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  @Override
  public long getLastPosition() throws IOException {
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        final OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();
        return clusterPositionMap.getLastPosition(atomicOperation);
      } finally {
        releaseSharedLock();
      }
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  @Override
  public long getNextPosition() throws IOException {
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        final OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();
        return clusterPositionMap.getNextPosition(atomicOperation);
      } finally {
        releaseSharedLock();
      }
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  @Override
  public String getFileName() {
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        return writeCache.fileNameById(fileId);
      } finally {
        releaseSharedLock();
      }
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  @Override
  public int getId() {
    return id;
  }

  /** Returns the fileId used in disk cache. */
  public long getFileId() {
    return fileId;
  }

  @Override
  public void synch() {
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        writeCache.flush(fileId);
        clusterPositionMap.flush();
      } finally {
        releaseSharedLock();
      }
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  @Override
  public long getRecordsSize() throws IOException {
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        final OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();

        final OCacheEntry pinnedStateEntry =
            loadPageForRead(atomicOperation, fileId, STATE_ENTRY_INDEX, true);
        try {
          return new OPaginatedClusterStateV2(pinnedStateEntry).getRecordsSize();
        } finally {
          releasePageFromRead(atomicOperation, pinnedStateEntry);
        }
      } finally {
        releaseSharedLock();
      }
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  @Override
  public OPhysicalPosition[] higherPositions(final OPhysicalPosition position) throws IOException {
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        final OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();
        final long[] clusterPositions =
            clusterPositionMap.higherPositions(position.clusterPosition, atomicOperation);
        return convertToPhysicalPositions(clusterPositions);
      } finally {
        releaseSharedLock();
      }
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  @Override
  public OPhysicalPosition[] ceilingPositions(final OPhysicalPosition position) throws IOException {
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        final OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();
        final long[] clusterPositions =
            clusterPositionMap.ceilingPositions(position.clusterPosition, atomicOperation);
        return convertToPhysicalPositions(clusterPositions);
      } finally {
        releaseSharedLock();
      }
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  @Override
  public OPhysicalPosition[] lowerPositions(final OPhysicalPosition position) throws IOException {
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        final OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();
        final long[] clusterPositions =
            clusterPositionMap.lowerPositions(position.clusterPosition, atomicOperation);
        return convertToPhysicalPositions(clusterPositions);
      } finally {
        releaseSharedLock();
      }
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  @Override
  public OPhysicalPosition[] floorPositions(final OPhysicalPosition position) throws IOException {
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        final OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();
        final long[] clusterPositions =
            clusterPositionMap.floorPositions(position.clusterPosition, atomicOperation);
        return convertToPhysicalPositions(clusterPositions);
      } finally {
        releaseSharedLock();
      }
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  @Override
  public ORecordConflictStrategy getRecordConflictStrategy() {
    return recordConflictStrategy;
  }

  @Override
  public void setRecordConflictStrategy(final String stringValue) {
    acquireExclusiveLock();
    try {
      recordConflictStrategy =
          Orient.instance().getRecordConflictStrategy().getStrategy(stringValue);
    } finally {
      releaseExclusiveLock();
    }
  }

  private void updateClusterState(
      final long sizeDiff, final long recordsSizeDiff, final OAtomicOperation atomicOperation)
      throws IOException {
    final OCacheEntry pinnedStateEntry =
        loadPageForWrite(atomicOperation, fileId, STATE_ENTRY_INDEX, false, true);
    try {
      final OPaginatedClusterStateV2 paginatedClusterState =
          new OPaginatedClusterStateV2(pinnedStateEntry);
      paginatedClusterState.setSize((int) (paginatedClusterState.getSize() + sizeDiff));
      paginatedClusterState.setRecordsSize(
          (int) (paginatedClusterState.getRecordsSize() + recordsSizeDiff));
    } finally {
      releasePageFromWrite(atomicOperation, pinnedStateEntry);
    }
  }

  private void init(final int id, final String name, final String conflictStrategy)
      throws IOException {
    OFileUtils.checkValidName(name);

    if (conflictStrategy != null) {
      this.recordConflictStrategy =
          Orient.instance().getRecordConflictStrategy().getStrategy(conflictStrategy);
    }

    this.id = id;
  }

  @Override
  public void setClusterName(final String newName) {
    acquireExclusiveLock();
    try {
      writeCache.renameFile(fileId, newName + getExtension());
      clusterPositionMap.rename(newName);
      freeSpaceMap.rename(newName);

      setName(newName);
    } catch (IOException e) {
      throw OException.wrapException(
          new OPaginatedClusterException("Error during renaming of cluster", this), e);
    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  public void setEncryption(final String method, final String key) {
    throw new UnsupportedOperationException("Encryption should be configured on storage level.");
  }

  private static OPhysicalPosition createPhysicalPosition(
      final byte recordType, final long clusterPosition, final int version) {
    final OPhysicalPosition physicalPosition = new OPhysicalPosition();
    physicalPosition.recordType = recordType;
    physicalPosition.recordSize = -1;
    physicalPosition.clusterPosition = clusterPosition;
    physicalPosition.recordVersion = version;
    return physicalPosition;
  }

  private byte[] readFullEntry(
      final long clusterPosition,
      long pageIndex,
      int recordPosition,
      final OAtomicOperation atomicOperation,
      int pageCount)
      throws IOException {
    final List<byte[]> recordChunks = new ArrayList<>(2);
    int contentSize = 0;

    if (pageCount > 1) {
      final OCacheEntry stateCacheEntry =
          loadPageForRead(atomicOperation, fileId, STATE_ENTRY_INDEX, false);
      try {
        final OPaginatedClusterStateV2 state = new OPaginatedClusterStateV2(stateCacheEntry);
        pageCount = (int) Math.min(state.getFileSize() + 1 - pageIndex, pageCount);
      } finally {
        releasePageFromRead(atomicOperation, stateCacheEntry);
      }
    }

    long nextPagePointer;
    boolean firstEntry = true;
    do {
      final OCacheEntry cacheEntry =
          loadPageForRead(atomicOperation, fileId, pageIndex, false, pageCount);
      try {
        final OClusterPage localPage = new OClusterPage(cacheEntry);

        if (localPage.isDeleted(recordPosition)) {
          if (recordChunks.isEmpty()) {
            return null;
          } else {
            throw new OPaginatedClusterException(
                "Content of record " + new ORecordId(id, clusterPosition) + " was broken", this);
          }
        }

        final byte[] content =
            localPage.getRecordBinaryValue(
                recordPosition, 0, localPage.getRecordSize(recordPosition));
        assert content != null;

        if (firstEntry
            && content[content.length - OLongSerializer.LONG_SIZE - OByteSerializer.BYTE_SIZE]
                == 0) {
          return null;
        }

        recordChunks.add(content);
        nextPagePointer =
            OLongSerializer.INSTANCE.deserializeNative(
                content, content.length - OLongSerializer.LONG_SIZE);
        contentSize += content.length - OLongSerializer.LONG_SIZE - OByteSerializer.BYTE_SIZE;

        firstEntry = false;
      } finally {
        releasePageFromRead(atomicOperation, cacheEntry);
      }

      pageIndex = getPageIndex(nextPagePointer);
      recordPosition = getRecordPosition(nextPagePointer);
    } while (nextPagePointer >= 0);

    return convertRecordChunksToSingleChunk(recordChunks, contentSize);
  }

  private static byte[] convertRecordChunksToSingleChunk(
      final List<byte[]> recordChunks, final int contentSize) {
    final byte[] fullContent;
    if (recordChunks.size() == 1) {
      fullContent = recordChunks.get(0);
    } else {
      fullContent = new byte[contentSize + OLongSerializer.LONG_SIZE + OByteSerializer.BYTE_SIZE];
      int fullContentPosition = 0;
      for (final byte[] recordChuck : recordChunks) {
        System.arraycopy(
            recordChuck,
            0,
            fullContent,
            fullContentPosition,
            recordChuck.length - OLongSerializer.LONG_SIZE - OByteSerializer.BYTE_SIZE);
        fullContentPosition +=
            recordChuck.length - OLongSerializer.LONG_SIZE - OByteSerializer.BYTE_SIZE;
      }
    }
    return fullContent;
  }

  private static long createPagePointer(final long pageIndex, final int pagePosition) {
    return pageIndex << PAGE_INDEX_OFFSET | pagePosition;
  }

  private static int getRecordPosition(final long nextPagePointer) {
    return (int) (nextPagePointer & RECORD_POSITION_MASK);
  }

  private static long getPageIndex(final long nextPagePointer) {
    return nextPagePointer >>> PAGE_INDEX_OFFSET;
  }

  private AddEntryResult addEntry(
      final int recordVersion, final byte[] entryContent, final OAtomicOperation atomicOperation)
      throws IOException {

    int recordSizesDiff;
    int position;
    int finalVersion = 0;
    long pageIndex;

    do {
      pageIndex = freeSpaceMap.findFreePage(entryContent.length);
      final boolean newPage = pageIndex < 0;

      final OCacheEntry cacheEntry;
      if (newPage) {
        final OCacheEntry stateCacheEntry =
            loadPageForWrite(atomicOperation, fileId, STATE_ENTRY_INDEX, false, true);
        try {
          final OPaginatedClusterStateV2 clusterState =
              new OPaginatedClusterStateV2(stateCacheEntry);
          final int fileSize = clusterState.getFileSize();
          final long filledUpTo = getFilledUpTo(atomicOperation, fileId);

          if (fileSize == filledUpTo - 1) {
            cacheEntry = addPage(atomicOperation, fileId);
          } else {
            assert fileSize < filledUpTo - 1;

            cacheEntry = loadPageForWrite(atomicOperation, fileId, fileSize + 1, false, false);
          }

          clusterState.setFileSize(fileSize + 1);
        } finally {
          releasePageFromWrite(atomicOperation, stateCacheEntry);
        }
      } else {
        cacheEntry = loadPageForWrite(atomicOperation, fileId, pageIndex, false, true);
      }

      pageIndex = cacheEntry.getPageIndex();
      final int maxRecordSize;
      try {
        final OClusterPage localPage = new OClusterPage(cacheEntry);
        if (newPage) {
          localPage.init();
        } else {
          assert entryContent.length <= localPage.getMaxRecordSize();
        }

        final int initialFreeSpace = localPage.getFreeSpace();

        position =
            localPage.appendRecord(
                recordVersion,
                entryContent,
                -1,
                atomicOperation.getBookedRecordPositions(id, cacheEntry.getPageIndex()));

        final int freeSpace = localPage.getFreeSpace();
        recordSizesDiff = initialFreeSpace - freeSpace;
        maxRecordSize = localPage.getMaxRecordSize();

        if (position >= 0) {
          finalVersion = localPage.getRecordVersion(position);
        }
      } finally {
        releasePageFromWrite(atomicOperation, cacheEntry);
      }

      freeSpaceMap.updatePageFreeSpace(atomicOperation, (int) pageIndex, maxRecordSize);
    } while (position < 0);

    return new AddEntryResult(pageIndex, position, finalVersion, recordSizesDiff);
  }

  private void initCusterState(final OAtomicOperation atomicOperation) throws IOException {
    final OCacheEntry stateEntry;
    if (getFilledUpTo(atomicOperation, fileId) == 0) {
      stateEntry = addPage(atomicOperation, fileId);
    } else {
      stateEntry = loadPageForWrite(atomicOperation, fileId, STATE_ENTRY_INDEX, false, false);
    }

    assert stateEntry.getPageIndex() == 0;
    try {
      final OPaginatedClusterStateV2 paginatedClusterState =
          new OPaginatedClusterStateV2(stateEntry);
      paginatedClusterState.setSize(0);
      paginatedClusterState.setRecordsSize(0);
      paginatedClusterState.setFileSize(0);
    } finally {
      releasePageFromWrite(atomicOperation, stateEntry);
    }
  }

  private static OPhysicalPosition[] convertToPhysicalPositions(final long[] clusterPositions) {
    final OPhysicalPosition[] positions = new OPhysicalPosition[clusterPositions.length];
    for (int i = 0; i < positions.length; i++) {
      final OPhysicalPosition physicalPosition = new OPhysicalPosition();
      physicalPosition.clusterPosition = clusterPositions[i];
      positions[i] = physicalPosition;
    }
    return positions;
  }

  public OPaginatedClusterDebug readDebug(final long clusterPosition) throws IOException {
    final OPaginatedClusterDebug debug = new OPaginatedClusterDebug();
    debug.clusterPosition = clusterPosition;
    debug.fileId = fileId;
    final OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();

    final OClusterPositionMapBucket.PositionEntry positionEntry =
        clusterPositionMap.get(clusterPosition, 1, atomicOperation);
    if (positionEntry == null) {
      debug.empty = true;
      return debug;
    }

    long pageIndex = positionEntry.getPageIndex();
    int recordPosition = positionEntry.getRecordPosition();

    debug.pages = new ArrayList<>(2);
    int contentSize = 0;

    long nextPagePointer;
    boolean firstEntry = true;
    do {
      final OClusterPageDebug debugPage = new OClusterPageDebug();
      debugPage.pageIndex = pageIndex;
      final OCacheEntry cacheEntry = loadPageForRead(atomicOperation, fileId, pageIndex, false);
      try {
        final OClusterPage localPage = new OClusterPage(cacheEntry);

        if (localPage.isDeleted(recordPosition)) {
          if (debug.pages.isEmpty()) {
            debug.empty = true;
            return debug;
          } else {
            throw new OPaginatedClusterException(
                "Content of record " + new ORecordId(id, clusterPosition) + " was broken", this);
          }
        }
        debugPage.inPagePosition = recordPosition;
        debugPage.inPageSize = localPage.getRecordSize(recordPosition);
        final byte[] content =
            localPage.getRecordBinaryValue(recordPosition, 0, debugPage.inPageSize);
        assert content != null;

        debugPage.content = content;
        if (firstEntry
            && content[content.length - OLongSerializer.LONG_SIZE - OByteSerializer.BYTE_SIZE]
                == 0) {
          debug.empty = true;
          return debug;
        }

        debug.pages.add(debugPage);
        nextPagePointer =
            OLongSerializer.INSTANCE.deserializeNative(
                content, content.length - OLongSerializer.LONG_SIZE);
        contentSize += content.length - OLongSerializer.LONG_SIZE - OByteSerializer.BYTE_SIZE;

        firstEntry = false;
      } finally {
        releasePageFromRead(atomicOperation, cacheEntry);
      }

      pageIndex = getPageIndex(nextPagePointer);
      recordPosition = getRecordPosition(nextPagePointer);
    } while (nextPagePointer >= 0);
    debug.contentSize = contentSize;
    return debug;
  }

  public RECORD_STATUS getRecordStatus(final long clusterPosition) throws IOException {
    final OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();
    acquireSharedLock();
    try {
      final byte status = clusterPositionMap.getStatus(clusterPosition, atomicOperation);

      switch (status) {
        case OClusterPositionMapBucket.NOT_EXISTENT:
          return RECORD_STATUS.NOT_EXISTENT;
        case OClusterPositionMapBucket.ALLOCATED:
          return RECORD_STATUS.ALLOCATED;
        case OClusterPositionMapBucket.FILLED:
          return RECORD_STATUS.PRESENT;
        case OClusterPositionMapBucket.REMOVED:
          return RECORD_STATUS.REMOVED;
      }

      // UNREACHABLE
      return null;
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public void acquireAtomicExclusiveLock() {
    atomicOperationsManager.acquireExclusiveLockTillOperationComplete(this);
  }

  @Override
  public String toString() {
    return "plocal cluster: " + getName();
  }

  @Override
  public OClusterBrowsePage nextPage(final long lastPosition) throws IOException {
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        final OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();

        final OClusterPositionMapV2.OClusterPositionEntry[] nextPositions =
            clusterPositionMap.higherPositionsEntries(lastPosition, atomicOperation);
        if (nextPositions.length > 0) {
          final long newLastPosition = nextPositions[nextPositions.length - 1].getPosition();
          final List<OClusterBrowseEntry> nexv = new ArrayList<>(nextPositions.length);
          for (final OClusterPositionMapV2.OClusterPositionEntry pos : nextPositions) {
            final ORawBuffer buff =
                internalReadRecord(
                    pos.getPosition(), pos.getPage(), pos.getOffset(), 1, atomicOperation);
            nexv.add(new OClusterBrowseEntry(pos.getPosition(), buff));
          }
          return new OClusterBrowsePage(nexv, newLastPosition);
        } else {
          return null;
        }
      } finally {
        releaseSharedLock();
      }
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }
}
