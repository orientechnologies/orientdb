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
import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.orient.core.Orient;
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
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;
import java.io.IOException;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.ListIterator;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 10/7/13
 */
public final class OPaginatedClusterV2 extends OPaginatedCluster {
  // max chunk size - nex page pointer - first record flag
  private static final int MAX_ENTRY_SIZE =
      OClusterPage.MAX_RECORD_SIZE - OByteSerializer.BYTE_SIZE - OLongSerializer.LONG_SIZE;

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

                try (final OCacheEntry cacheEntry =
                    loadPageForRead(atomicOperation, fileId, pageIndex)) {
                  final OClusterPage clusterPage = new OClusterPage(cacheEntry);
                  freeSpaceMap.updatePageFreeSpace(
                      atomicOperation, pageIndex, clusterPage.getMaxRecordSize());
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
            final int[] result =
                serializeRecord(
                    content,
                    calculateClusterEntrySize(content.length),
                    recordType,
                    recordVersion,
                    -1,
                    atomicOperation,
                    entrySize -> findNewPageToWrite(atomicOperation, entrySize),
                    page -> {
                      final OCacheEntry cacheEntry = page.getCacheEntry();
                      try {
                        cacheEntry.close();
                      } catch (final IOException e) {
                        throw OException.wrapException(
                            new OPaginatedClusterException("Can not store the record", this), e);
                      }
                    });

            final int nextPageIndex = result[0];
            final int nextPageOffset = result[1];
            assert result[2] == 0;

            updateClusterState(1, content.length, atomicOperation);

            final long clusterPosition;
            if (allocatedPosition != null) {
              clusterPositionMap.update(
                  allocatedPosition.clusterPosition,
                  new OClusterPositionMapBucket.PositionEntry(nextPageIndex, nextPageOffset),
                  atomicOperation);
              clusterPosition = allocatedPosition.clusterPosition;
            } else {
              clusterPosition =
                  clusterPositionMap.add(nextPageIndex, nextPageOffset, atomicOperation);
            }
            return createPhysicalPosition(recordType, clusterPosition, recordVersion);
          } finally {
            releaseExclusiveLock();
          }
        });
  }

  private OClusterPage findNewPageToWrite(
      final OAtomicOperation atomicOperation, final int entrySize) {
    final OClusterPage page;
    try {
      final int nextPageToWrite = findNextFreePageIndexToWrite(entrySize);

      final OCacheEntry cacheEntry;
      boolean isNew;
      if (nextPageToWrite >= 0) {
        cacheEntry = loadPageForWrite(atomicOperation, fileId, nextPageToWrite, true);
        isNew = false;
      } else {
        cacheEntry = allocateNewPage(atomicOperation);
        isNew = true;
      }

      page = new OClusterPage(cacheEntry);
      if (isNew) {
        page.init();
      }
    } catch (final IOException e) {
      throw OException.wrapException(
          new OPaginatedClusterException("Can not store the record", this), e);
    }

    return page;
  }

  private int[] serializeRecord(
      final byte[] content,
      final int len,
      final byte recordType,
      final int recordVersion,
      final long nextRecordPointer,
      final OAtomicOperation atomicOperation,
      final Function<Integer, OClusterPage> pageSupplier,
      final Consumer<OClusterPage> pagePostProcessor)
      throws IOException {

    int bytesToWrite = len;
    int chunkSize = calculateChunkSize(bytesToWrite);

    long nextRecordPointers = nextRecordPointer;
    int nextPageIndex = -1;
    int nextPageOffset = -1;

    while (bytesToWrite > 0) {
      final OClusterPage page = pageSupplier.apply(bytesToWrite);
      if (page == null) {
        return new int[] {nextPageIndex, nextPageOffset, bytesToWrite};
      }

      int maxRecordSize;
      try {
        final int pageChunkSize = Math.min(page.getMaxRecordSize(), chunkSize);

        final ORawPair<byte[], Integer> pair =
            serializeEntryChunk(
                content, pageChunkSize, bytesToWrite, nextRecordPointers, recordType);
        final byte[] chunk = pair.first;

        final OCacheEntry cacheEntry = page.getCacheEntry();
        nextPageOffset =
            page.appendRecord(
                recordVersion,
                chunk,
                -1,
                atomicOperation.getBookedRecordPositions(id, cacheEntry.getPageIndex()));
        assert nextPageOffset >= 0;

        maxRecordSize = page.getMaxRecordSize();

        bytesToWrite -= pair.second;
        assert bytesToWrite >= 0;

        nextPageIndex = cacheEntry.getPageIndex();

        if (bytesToWrite > 0) {
          chunkSize = calculateChunkSize(bytesToWrite);

          nextRecordPointers = createPagePointer(nextPageIndex, nextPageOffset);
        }
      } finally {
        pagePostProcessor.accept(page);
      }

      freeSpaceMap.updatePageFreeSpace(atomicOperation, nextPageIndex, maxRecordSize);
    }

    return new int[] {nextPageIndex, nextPageOffset, 0};
  }

  private ORawPair<byte[], Integer> serializeEntryChunk(
      final byte[] recordContent,
      final int chunkSize,
      final int bytesToWrite,
      final long nextPagePointer,
      final byte recordType) {
    final byte[] chunk = new byte[chunkSize];
    int offset = chunkSize - OLongSerializer.LONG_SIZE;

    OLongSerializer.INSTANCE.serializeNative(nextPagePointer, chunk, offset);

    int written = 0;
    // entry - entry size - record type
    final int contentSize = bytesToWrite - OIntegerSerializer.INT_SIZE - OByteSerializer.BYTE_SIZE;
    // skip first record flag
    final int firstRecordOffset = --offset;

    // there are records data to write
    if (contentSize > 0) {
      final int contentToWrite = Math.min(contentSize, offset);
      System.arraycopy(
          recordContent,
          contentSize - contentToWrite,
          chunk,
          offset - contentToWrite,
          contentToWrite);
      written = contentToWrite;
    }

    int spaceLeft = chunkSize - written - OLongSerializer.LONG_SIZE - OByteSerializer.BYTE_SIZE;

    if (spaceLeft > 0) {
      final int spaceToWrite = bytesToWrite - written;
      assert spaceToWrite <= OIntegerSerializer.INT_SIZE + OByteSerializer.BYTE_SIZE;

      // we need to write only record type
      if (spaceToWrite == 1) {
        chunk[0] = recordType;
        chunk[firstRecordOffset] = 1;
        written++;
      } else {
        // at least part of record size and record type has to be written
        // record size and record type can be written at once
        if (spaceLeft == OIntegerSerializer.INT_SIZE + OByteSerializer.BYTE_SIZE) {
          chunk[0] = recordType;
          OIntegerSerializer.INSTANCE.serializeNative(
              recordContent.length, chunk, OByteSerializer.BYTE_SIZE);
          chunk[firstRecordOffset] = 1;

          written += OIntegerSerializer.INT_SIZE + OByteSerializer.BYTE_SIZE;
        } else {
          final int recordSizePart = spaceToWrite - OByteSerializer.BYTE_SIZE;
          assert recordSizePart <= OIntegerSerializer.INT_SIZE;

          if (recordSizePart == OIntegerSerializer.INT_SIZE
              && spaceLeft == OIntegerSerializer.INT_SIZE) {
            OIntegerSerializer.INSTANCE.serializeNative(recordContent.length, chunk, 0);
            written += OIntegerSerializer.INT_SIZE;
          } else {
            final ByteOrder byteOrder = ByteOrder.nativeOrder();
            if (byteOrder == ByteOrder.LITTLE_ENDIAN) {
              for (int sizeOffset = (recordSizePart - 1) << 3;
                  sizeOffset >= 0 && spaceLeft > 0;
                  sizeOffset -= 8, spaceLeft--, written++) {
                final byte sizeByte = (byte) (0xFF & (recordContent.length >> sizeOffset));
                chunk[spaceLeft - 1] = sizeByte;
              }
            } else {
              for (int sizeOffset = (OIntegerSerializer.INT_SIZE - recordSizePart) << 3;
                  sizeOffset < OIntegerSerializer.INT_SIZE & spaceLeft > 0;
                  sizeOffset += 8, spaceLeft--, written++) {
                final byte sizeByte = (byte) (0xFF & (recordContent.length >> sizeOffset));
                chunk[spaceLeft - 1] = sizeByte;
              }
            }

            if (spaceLeft > 0) {
              chunk[0] = recordType;
              chunk[firstRecordOffset] = 1;
              written++;
            }
          }
        }
      }
    }

    return new ORawPair<>(chunk, written);
  }

  private int findNextFreePageIndexToWrite(int bytesToWrite) throws IOException {
    if (bytesToWrite > MAX_ENTRY_SIZE) {
      bytesToWrite = MAX_ENTRY_SIZE;
    }

    int pageIndex;

    // if page is empty we will not find it inside of free mpa because of the policy
    // that always requests to find page which is bigger than current record
    // so we find page with at least half of the space at the worst case
    // we will split record by two anyway.
    if (bytesToWrite >= ODurablePage.MAX_PAGE_SIZE_BYTES - FreeSpaceMap.NORMALIZATION_INTERVAL) {
      final int halfChunkSize = calculateChunkSize(bytesToWrite / 2);
      pageIndex = freeSpaceMap.findFreePage(halfChunkSize / 2);

      return pageIndex;
    }

    int chunkSize = calculateChunkSize(bytesToWrite);
    pageIndex = freeSpaceMap.findFreePage(chunkSize);

    if (pageIndex < 0 && bytesToWrite > MAX_ENTRY_SIZE / 2) {
      final int halfChunkSize = calculateChunkSize(bytesToWrite / 2);
      if (halfChunkSize > 0) {
        pageIndex = freeSpaceMap.findFreePage(halfChunkSize / 2);
      }
    }

    return pageIndex;
  }

  private static int calculateClusterEntrySize(final int contentSize) {
    // content + record type + content size
    return contentSize + OByteSerializer.BYTE_SIZE + OIntegerSerializer.INT_SIZE;
  }

  private static int calculateContentSizeFromClusterEntrySize(final int contentSize) {
    // content + record type + content size
    return contentSize - OByteSerializer.BYTE_SIZE - OIntegerSerializer.INT_SIZE;
  }

  private static int calculateChunkSize(final int entrySize) {
    // entry content + first entry flag + next entry pointer
    return entrySize + OByteSerializer.BYTE_SIZE + OLongSerializer.LONG_SIZE;
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
    return readRecord(clusterPosition);
  }

  private ORawBuffer readRecord(final long clusterPosition) throws IOException {
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        final OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();

        final OClusterPositionMapBucket.PositionEntry positionEntry =
            clusterPositionMap.get(clusterPosition, atomicOperation);
        if (positionEntry == null) {
          return null;
        }
        return internalReadRecord(
            clusterPosition,
            positionEntry.getPageIndex(),
            positionEntry.getRecordPosition(),
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
      long pageIndex,
      int recordPosition,
      final OAtomicOperation atomicOperation)
      throws IOException {

    int recordVersion = 0;

    final List<byte[]> recordChunks = new ArrayList<>(2);
    int contentSize = 0;

    long nextPagePointer;
    boolean firstEntry = true;
    do {
      try (final OCacheEntry cacheEntry = loadPageForRead(atomicOperation, fileId, pageIndex)) {
        final OClusterPage localPage = new OClusterPage(cacheEntry);
        if (firstEntry) {
          recordVersion = localPage.getRecordVersion(recordPosition);
        }

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
      }

      pageIndex = getPageIndex(nextPagePointer);
      recordPosition = getRecordPosition(nextPagePointer);
    } while (nextPagePointer >= 0);

    byte[] fullContent = convertRecordChunksToSingleChunk(recordChunks, contentSize);

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
            clusterPositionMap.get(clusterPosition, atomicOperation);

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

        try (final OCacheEntry cacheEntry = loadPageForRead(atomicOperation, fileId, pageIndex)) {
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
                clusterPositionMap.get(clusterPosition, atomicOperation);
            if (positionEntry == null) {
              return false;
            }

            long pageIndex = positionEntry.getPageIndex();
            int recordPosition = positionEntry.getRecordPosition();

            long nextPagePointer;
            int removedContentSize = 0;
            int removeRecordSize = 0;

            do {
              boolean cacheEntryReleased = false;
              final int maxRecordSize;
              OCacheEntry cacheEntry = loadPageForWrite(atomicOperation, fileId, pageIndex, true);
              try {
                OClusterPage localPage = new OClusterPage(cacheEntry);

                if (localPage.isDeleted(recordPosition)) {
                  if (removedContentSize == 0) {
                    cacheEntryReleased = true;
                    cacheEntry.close();
                    ;
                    return false;
                  } else {
                    throw new OPaginatedClusterException(
                        "Content of record " + new ORecordId(id, clusterPosition) + " was broken",
                        this);
                  }
                } else if (removedContentSize == 0) {
                  cacheEntry.close();

                  cacheEntry = loadPageForWrite(atomicOperation, fileId, pageIndex, true);

                  localPage = new OClusterPage(cacheEntry);
                }

                final int initialFreeSpace = localPage.getFreeSpace();
                final byte[] content = localPage.deleteRecord(recordPosition, true);
                atomicOperation.addDeletedRecordPosition(
                    id, cacheEntry.getPageIndex(), recordPosition);
                assert content != null;
                removeRecordSize = calculateContentSizeFromClusterEntrySize(content.length);

                maxRecordSize = localPage.getMaxRecordSize();
                removedContentSize += localPage.getFreeSpace() - initialFreeSpace;
                nextPagePointer =
                    OLongSerializer.INSTANCE.deserializeNative(
                        content, content.length - OLongSerializer.LONG_SIZE);
              } finally {
                if (!cacheEntryReleased) {
                  cacheEntry.close();
                }
              }

              freeSpaceMap.updatePageFreeSpace(atomicOperation, (int) pageIndex, maxRecordSize);

              pageIndex = getPageIndex(nextPagePointer);
              recordPosition = getRecordPosition(nextPagePointer);
            } while (nextPagePointer >= 0);

            updateClusterState(-1, -removeRecordSize, atomicOperation);

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
      final byte[] content,
      final int recordVersion,
      final byte recordType,
      final OAtomicOperation atomicOperation) {
    executeInsideComponentOperation(
        atomicOperation,
        operation -> {
          acquireExclusiveLock();
          try {
            final OClusterPositionMapBucket.PositionEntry positionEntry =
                clusterPositionMap.get(clusterPosition, atomicOperation);
            if (positionEntry == null) {
              return;
            }

            int oldContentSize = 0;
            int nextPageIndex = (int) positionEntry.getPageIndex();
            int nextRecordPosition = positionEntry.getRecordPosition();

            long nextPagePointer = createPagePointer(nextPageIndex, nextRecordPosition);

            ArrayList<OClusterPage> storedPages = new ArrayList<>();

            while (nextPagePointer >= 0) {
              final OCacheEntry cacheEntry =
                  loadPageForWrite(atomicOperation, fileId, nextPageIndex, true);
              final OClusterPage page = new OClusterPage(cacheEntry);
              final byte[] deletedRecord = page.deleteRecord(nextRecordPosition, true);
              assert deletedRecord != null;
              oldContentSize = calculateContentSizeFromClusterEntrySize(deletedRecord.length);
              nextPagePointer =
                  OLongSerializer.INSTANCE.deserializeNative(
                      deletedRecord, deletedRecord.length - OLongSerializer.LONG_SIZE);

              nextPageIndex = (int) getPageIndex(nextPagePointer);
              nextRecordPosition = getRecordPosition(nextPagePointer);

              storedPages.add(page);
            }

            final ListIterator<OClusterPage> reverseIterator =
                storedPages.listIterator(storedPages.size());
            int[] result =
                serializeRecord(
                    content,
                    calculateClusterEntrySize(content.length),
                    recordType,
                    recordVersion,
                    -1,
                    atomicOperation,
                    entrySize -> {
                      if (reverseIterator.hasPrevious()) {
                        return reverseIterator.previous();
                      }
                      return null;
                    },
                    page -> {
                      final OCacheEntry cacheEntry = page.getCacheEntry();
                      try {
                        cacheEntry.close();
                      } catch (final IOException e) {
                        throw OException.wrapException(
                            new OPaginatedClusterException(
                                "Can not update record with rid "
                                    + new ORecordId(id, clusterPosition),
                                this),
                            e);
                      }
                    });

            nextPageIndex = result[0];
            nextRecordPosition = result[1];

            while (reverseIterator.hasPrevious()) {
              final OClusterPage page = reverseIterator.previous();
              page.getCacheEntry().close();
            }

            if (result[2] != 0) {
              result =
                  serializeRecord(
                      content,
                      result[2],
                      recordType,
                      recordVersion,
                      createPagePointer(nextPageIndex, nextRecordPosition),
                      atomicOperation,
                      entrySize -> findNewPageToWrite(atomicOperation, entrySize),
                      page -> {
                        final OCacheEntry cacheEntry = page.getCacheEntry();
                        try {
                          cacheEntry.close();
                        } catch (final IOException e) {
                          throw OException.wrapException(
                              new OPaginatedClusterException(
                                  "Can not update record with rid "
                                      + new ORecordId(id, clusterPosition),
                                  this),
                              e);
                        }
                      });

              nextPageIndex = result[0];
              nextRecordPosition = result[1];
            }

            assert result[2] == 0;
            updateClusterState(0, content.length - oldContentSize, atomicOperation);

            if (nextPageIndex != positionEntry.getPageIndex()
                || nextRecordPosition != positionEntry.getRecordPosition()) {
              clusterPositionMap.update(
                  clusterPosition,
                  new OClusterPositionMapBucket.PositionEntry(nextPageIndex, nextRecordPosition),
                  atomicOperation);
            }
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
            clusterPositionMap.get(clusterPosition, atomicOperation);

        if (positionEntry == null) {
          return null;
        }

        final long pageIndex = positionEntry.getPageIndex();
        final int recordPosition = positionEntry.getRecordPosition();

        try (final OCacheEntry cacheEntry = loadPageForRead(atomicOperation, fileId, pageIndex)) {
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
            clusterPositionMap.get(clusterPosition, atomicOperation);

        if (positionEntry == null) {
          return false;
        }

        final long pageIndex = positionEntry.getPageIndex();
        final int recordPosition = positionEntry.getRecordPosition();

        try (final OCacheEntry cacheEntry = loadPageForRead(atomicOperation, fileId, pageIndex)) {
          final OClusterPage localPage = new OClusterPage(cacheEntry);
          return localPage.isDeleted(recordPosition);
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
        try (final OCacheEntry pinnedStateEntry =
            loadPageForRead(atomicOperation, fileId, STATE_ENTRY_INDEX)) {
          return new OPaginatedClusterStateV2(pinnedStateEntry).getSize();
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

        try (final OCacheEntry pinnedStateEntry =
            loadPageForRead(atomicOperation, fileId, STATE_ENTRY_INDEX)) {
          return new OPaginatedClusterStateV2(pinnedStateEntry).getRecordsSize();
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
      final long sizeDiff, long recordSizeDiff, final OAtomicOperation atomicOperation)
      throws IOException {
    try (final OCacheEntry pinnedStateEntry =
        loadPageForWrite(atomicOperation, fileId, STATE_ENTRY_INDEX, true)) {
      final OPaginatedClusterStateV2 paginatedClusterState =
          new OPaginatedClusterStateV2(pinnedStateEntry);
      if (sizeDiff != 0) {
        paginatedClusterState.setSize((int) (paginatedClusterState.getSize() + sizeDiff));
      }
      if (recordSizeDiff != 0) {
        paginatedClusterState.setRecordsSize(
            (int) (paginatedClusterState.getRecordsSize() + recordSizeDiff));
      }
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

  private OCacheEntry allocateNewPage(OAtomicOperation atomicOperation) throws IOException {
    OCacheEntry cacheEntry;
    try (final OCacheEntry stateCacheEntry =
        loadPageForWrite(atomicOperation, fileId, STATE_ENTRY_INDEX, true)) {
      final OPaginatedClusterStateV2 clusterState = new OPaginatedClusterStateV2(stateCacheEntry);
      final int fileSize = clusterState.getFileSize();
      final long filledUpTo = getFilledUpTo(atomicOperation, fileId);

      if (fileSize == filledUpTo - 1) {
        cacheEntry = addPage(atomicOperation, fileId);
      } else {
        assert fileSize < filledUpTo - 1;

        cacheEntry = loadPageForWrite(atomicOperation, fileId, fileSize + 1, false);
      }

      clusterState.setFileSize(fileSize + 1);
    }
    return cacheEntry;
  }

  private void initCusterState(final OAtomicOperation atomicOperation) throws IOException {
    final OCacheEntry stateEntry;
    if (getFilledUpTo(atomicOperation, fileId) == 0) {
      stateEntry = addPage(atomicOperation, fileId);
    } else {
      stateEntry = loadPageForWrite(atomicOperation, fileId, STATE_ENTRY_INDEX, false);
    }

    assert stateEntry.getPageIndex() == 0;
    try {
      final OPaginatedClusterStateV2 paginatedClusterState =
          new OPaginatedClusterStateV2(stateEntry);
      paginatedClusterState.setSize(0);
      paginatedClusterState.setRecordsSize(0);
      paginatedClusterState.setFileSize(0);
    } finally {
      stateEntry.close();
      ;
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
        clusterPositionMap.get(clusterPosition, atomicOperation);
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

      try (final OCacheEntry cacheEntry = loadPageForRead(atomicOperation, fileId, pageIndex)) {
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
                    pos.getPosition(), pos.getPage(), pos.getOffset(), atomicOperation);
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
