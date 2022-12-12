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
package com.orientechnologies.orient.core.storage.cluster.v0;

import static com.orientechnologies.orient.core.config.OGlobalConfiguration.DISK_CACHE_PAGE_SIZE;
import static com.orientechnologies.orient.core.config.OGlobalConfiguration.PAGINATED_STORAGE_LOWEST_FREELIST_BOUNDARY;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.compression.OCompression;
import com.orientechnologies.orient.core.compression.OCompressionFactory;
import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.config.OStorageClusterConfiguration;
import com.orientechnologies.orient.core.config.OStoragePaginatedClusterConfiguration;
import com.orientechnologies.orient.core.conflict.ORecordConflictStrategy;
import com.orientechnologies.orient.core.encryption.OEncryption;
import com.orientechnologies.orient.core.encryption.OEncryptionFactory;
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
public final class OPaginatedClusterV0 extends OPaginatedCluster {
  private static final int BINARY_VERSION = 0;
  private static final int DISK_PAGE_SIZE = DISK_CACHE_PAGE_SIZE.getValueAsInteger();
  private static final int LOWEST_FREELIST_BOUNDARY =
      PAGINATED_STORAGE_LOWEST_FREELIST_BOUNDARY.getValueAsInteger();
  private static final int FREE_LIST_SIZE = DISK_PAGE_SIZE - LOWEST_FREELIST_BOUNDARY;
  private static final int PAGE_INDEX_OFFSET = 16;
  private static final int RECORD_POSITION_MASK = 0xFFFF;
  private static final int ONE_KB = 1024;

  private volatile OCompression compression;
  private volatile OEncryption encryption;
  private final boolean systemCluster;
  private final OClusterPositionMapV0 clusterPositionMap;
  private volatile int id;
  private long fileId;
  private long stateEntryIndex;
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

  private static final class FindFreePageResult {
    private final long pageIndex;
    private final int freePageIndex;

    private FindFreePageResult(final long pageIndex, final int freePageIndex) {
      this.pageIndex = pageIndex;
      this.freePageIndex = freePageIndex;
    }
  }

  public OPaginatedClusterV0(final String name, final OAbstractPaginatedStorage storage) {
    super(storage, name, ".pcl", name + ".pcl");

    systemCluster = OMetadataInternal.SYSTEM_CLUSTER.contains(name);
    clusterPositionMap = new OClusterPositionMapV0(storage, getName(), getFullName());
  }

  @Override
  public void configure(final int id, final String clusterName) throws IOException {
    acquireExclusiveLock();
    try {
      final OContextConfiguration ctxCfg = storage.getConfiguration().getContextConfiguration();
      final String cfgCompression =
          ctxCfg.getValueAsString(OGlobalConfiguration.STORAGE_COMPRESSION_METHOD);
      @SuppressWarnings("deprecation")
      final String cfgEncryption =
          ctxCfg.getValueAsString(OGlobalConfiguration.STORAGE_ENCRYPTION_METHOD);
      final String cfgEncryptionKey =
          ctxCfg.getValueAsString(OGlobalConfiguration.STORAGE_ENCRYPTION_KEY);

      init(id, clusterName, cfgCompression, cfgEncryption, cfgEncryptionKey, null);
    } finally {
      releaseExclusiveLock();
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
          compression.name(),
          encryption.name(),
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
      final OContextConfiguration ctxCfg = storage.getConfiguration().getContextConfiguration();
      final String cfgCompression =
          ctxCfg.getValueAsString(OGlobalConfiguration.STORAGE_COMPRESSION_METHOD);
      @SuppressWarnings("deprecation")
      final String cfgEncryption =
          ctxCfg.getValueAsString(OGlobalConfiguration.STORAGE_ENCRYPTION_METHOD);
      final String cfgEncryptionKey =
          ctxCfg.getValueAsString(OGlobalConfiguration.STORAGE_ENCRYPTION_KEY);

      init(
          config.getId(),
          config.getName(),
          cfgCompression,
          cfgEncryption,
          cfgEncryptionKey,
          ((OStoragePaginatedClusterConfiguration) config).conflictStrategy);
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
  public void create(OAtomicOperation atomicOperation) {
    executeInsideComponentOperation(
        atomicOperation,
        operation -> {
          acquireExclusiveLock();
          try {
            fileId = addFile(atomicOperation, getFullName());

            initCusterState(atomicOperation);

            clusterPositionMap.create(atomicOperation);
          } finally {
            releaseExclusiveLock();
          }
        });
  }

  @Override
  public void open(OAtomicOperation atomicOperation) throws IOException {
    acquireExclusiveLock();
    try {
      fileId = openFile(atomicOperation, getFullName());

      try (final OCacheEntry pinnedStateEntry = loadPageForRead(atomicOperation, fileId, 0)) {
        stateEntryIndex = pinnedStateEntry.getPageIndex();
      }

      clusterPositionMap.open(atomicOperation);
    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  public void close() throws IOException {
    close(true);
  }

  @Override
  public void close(final boolean flush) throws IOException {
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
      return compression.name();
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public String encryption() {
    acquireSharedLock();
    try {
      return encryption.name();
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
      byte[] content,
      final int recordVersion,
      final byte recordType,
      final OPhysicalPosition allocatedPosition,
      OAtomicOperation atomicOperation) {
    content = compression.compress(content);
    content = encryption.encrypt(content);

    final byte[] encryptedContent = content;
    return calculateInsideComponentOperation(
        atomicOperation,
        operation -> {
          acquireExclusiveLock();
          try {
            final int entryContentLength = getEntryContentLength(encryptedContent.length);

            if (entryContentLength < OClusterPage.MAX_RECORD_SIZE) {
              final byte[] entryContent = new byte[entryContentLength];

              int entryPosition = 0;
              entryContent[entryPosition] = recordType;
              entryPosition++;

              OIntegerSerializer.INSTANCE.serializeNative(
                  encryptedContent.length, entryContent, entryPosition);
              entryPosition += OIntegerSerializer.INT_SIZE;

              System.arraycopy(
                  encryptedContent, 0, entryContent, entryPosition, encryptedContent.length);
              entryPosition += encryptedContent.length;

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
                  encryptedContent.length + OIntegerSerializer.INT_SIZE + OByteSerializer.BYTE_SIZE;

              int fullEntryPosition = 0;
              final byte[] fullEntry = new byte[entrySize];

              fullEntry[fullEntryPosition] = recordType;
              fullEntryPosition++;

              OIntegerSerializer.INSTANCE.serializeNative(
                  encryptedContent.length, fullEntry, fullEntryPosition);
              fullEntryPosition += OIntegerSerializer.INT_SIZE;

              System.arraycopy(
                  encryptedContent, 0, fullEntry, fullEntryPosition, encryptedContent.length);

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

                  try (final OCacheEntry prevPageCacheEntry =
                      loadPageForWrite(atomicOperation, fileId, prevPageIndex, true)) {
                    final OClusterPage prevPage = new OClusterPage(prevPageCacheEntry);
                    prevPage.setRecordLongValue(
                        prevPageRecordPosition, -OLongSerializer.LONG_SIZE, addedPagePointer);
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
      final long pageIndex,
      final int recordPosition,
      final OAtomicOperation atomicOperation)
      throws IOException {

    if (getFilledUpTo(atomicOperation, fileId) <= pageIndex) {
      return null;
    }

    int recordVersion;
    try (final OCacheEntry cacheEntry = loadPageForRead(atomicOperation, fileId, pageIndex)) {
      final OClusterPage localPage = new OClusterPage(cacheEntry);
      if (localPage.isDeleted(recordPosition)) {
        return null;
      }

      recordVersion = localPage.getRecordVersion(recordPosition);
    }

    final byte[] fullContent =
        readFullEntry(clusterPosition, pageIndex, recordPosition, atomicOperation);
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

    recordContent = encryption.decrypt(recordContent);
    recordContent = compression.uncompress(recordContent);

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

        if (getFilledUpTo(atomicOperation, fileId) <= pageIndex) {
          throw new ORecordNotFoundException(
              new ORecordId(id, clusterPosition),
              "Record for cluster with id "
                  + id
                  + " and position "
                  + clusterPosition
                  + " is absent.");
        }

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

            if (getFilledUpTo(atomicOperation, fileId) <= pageIndex) {
              return false;
            }

            long nextPagePointer;
            int removedContentSize = 0;

            do {
              boolean cacheEntryReleased = false;
              OCacheEntry cacheEntry = loadPageForWrite(atomicOperation, fileId, pageIndex, true);
              int initialFreePageIndex;
              try {
                OClusterPage localPage = new OClusterPage(cacheEntry);
                initialFreePageIndex = calculateFreePageIndex(localPage);

                if (localPage.isDeleted(recordPosition)) {
                  if (removedContentSize == 0) {
                    cacheEntryReleased = true;
                    cacheEntry.close();
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

                final byte[] content = localPage.deleteRecord(recordPosition, true);
                atomicOperation.addDeletedRecordPosition(
                    id, cacheEntry.getPageIndex(), recordPosition);
                assert content != null;

                final int initialFreeSpace = localPage.getFreeSpace();
                removedContentSize += localPage.getFreeSpace() - initialFreeSpace;
                nextPagePointer =
                    OLongSerializer.INSTANCE.deserializeNative(
                        content, content.length - OLongSerializer.LONG_SIZE);
              } finally {
                if (!cacheEntryReleased) {
                  cacheEntry.close();
                }
              }

              updateFreePagesIndex(initialFreePageIndex, pageIndex, atomicOperation);

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
    content = compression.compress(content);
    content = encryption.encrypt(content);

    final byte[] encryptedContent = content;

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
                      Math.min(
                          getEntryContentLength(encryptedContent.length),
                          OClusterPage.MAX_RECORD_SIZE);
                  to =
                      entrySize
                          - (2 * OByteSerializer.BYTE_SIZE
                              + OIntegerSerializer.INT_SIZE
                              + OLongSerializer.LONG_SIZE);
                } else {
                  entrySize =
                      Math.min(
                          encryptedContent.length
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
                      encryptedContent.length, updateEntry, entryPosition);
                  entryPosition += OIntegerSerializer.INT_SIZE;
                }

                System.arraycopy(encryptedContent, from, updateEntry, entryPosition, to - from);
                entryPosition += to - from;

                if (nextPageIndex == positionEntry.getPageIndex()) {
                  updateEntry[entryPosition] = 1;
                }

                entryPosition++;

                OLongSerializer.INSTANCE.serializeNative(-1, updateEntry, entryPosition);

                assert to >= encryptedContent.length || entrySize == OClusterPage.MAX_RECORD_SIZE;
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

              int freePageIndex = -1;

              if (nextPageIndex < 0) {
                final FindFreePageResult findFreePageResult =
                    findFreePage(entrySize, atomicOperation);
                nextPageIndex = findFreePageResult.pageIndex;
                freePageIndex = findFreePageResult.freePageIndex;
              }

              boolean isNew = false;
              OCacheEntry cacheEntry =
                  loadPageForWrite(atomicOperation, fileId, nextPageIndex, true);
              if (cacheEntry == null) {
                cacheEntry = addPage(atomicOperation, fileId);
                isNew = true;
              }

              try {
                final OClusterPage localPage = new OClusterPage(cacheEntry);
                if (isNew) {
                  localPage.init();
                }
                final int pageFreeSpace = localPage.getFreeSpace();

                if (freePageIndex < 0) {
                  freePageIndex = calculateFreePageIndex(localPage);
                } else {
                  assert isNew || freePageIndex == calculateFreePageIndex(localPage);
                }

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
                    final byte[] oldRecord = localPage.deleteRecord(nextRecordPosition, true);
                    atomicOperation.addDeletedRecordPosition(
                        id, cacheEntry.getPageIndex(), nextRecordPosition);
                    assert oldRecord != null;

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

                sizeDiff += pageFreeSpace - localPage.getFreeSpace();

              } finally {
                cacheEntry.close();
              }

              updateFreePagesIndex(freePageIndex, cacheEntry.getPageIndex(), atomicOperation);

              if (updatedEntryPosition >= 0) {
                if (from == 0) {
                  newPageIndex = cacheEntry.getPageIndex();
                  newRecordPosition = updatedEntryPosition;
                }

                from = to;

                if (prevPageIndex >= 0) {
                  try (final OCacheEntry prevCacheEntry =
                      loadPageForWrite(atomicOperation, fileId, prevPageIndex, true)) {
                    final OClusterPage prevPage = new OClusterPage(prevCacheEntry);
                    prevPage.setRecordLongValue(
                        prevRecordPosition,
                        -OLongSerializer.LONG_SIZE,
                        createPagePointer(cacheEntry.getPageIndex(), updatedEntryPosition));
                  }
                }

                prevPageIndex = cacheEntry.getPageIndex();
                prevRecordPosition = updatedEntryPosition;

                updateEntry = null;
              }
            } while (to < encryptedContent.length || updateEntry != null);

            // clear unneeded pages
            while (nextEntryPointer >= 0) {
              nextPageIndex = getPageIndex(nextEntryPointer);
              nextRecordPosition = getRecordPosition(nextEntryPointer);

              final int freePagesIndex;
              final int freeSpace;

              try (final OCacheEntry cacheEntry =
                  loadPageForWrite(atomicOperation, fileId, nextPageIndex, true)) {
                final OClusterPage localPage = new OClusterPage(cacheEntry);
                freeSpace = localPage.getFreeSpace();
                freePagesIndex = calculateFreePageIndex(localPage);

                nextEntryPointer =
                    localPage.getRecordLongValue(nextRecordPosition, -OLongSerializer.LONG_SIZE);

                localPage.deleteRecord(nextRecordPosition, true);
                atomicOperation.addDeletedRecordPosition(
                    id, cacheEntry.getPageIndex(), nextRecordPosition);

                sizeDiff += freeSpace - localPage.getFreeSpace();
              }

              updateFreePagesIndex(freePagesIndex, nextPageIndex, atomicOperation);
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
            clusterPositionMap.get(clusterPosition, atomicOperation);

        if (positionEntry == null) {
          return null;
        }

        final long pageIndex = positionEntry.getPageIndex();
        final int recordPosition = positionEntry.getRecordPosition();

        final long pagesCount = getFilledUpTo(atomicOperation, fileId);
        if (pageIndex >= pagesCount) {
          return null;
        }

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

        final long pagesCount = getFilledUpTo(atomicOperation, fileId);
        if (pageIndex >= pagesCount) {
          return false;
        }

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
            loadPageForRead(atomicOperation, fileId, stateEntryIndex)) {
          return new OPaginatedClusterStateV0(pinnedStateEntry).getSize();
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
            loadPageForRead(atomicOperation, fileId, stateEntryIndex)) {
          return new OPaginatedClusterStateV0(pinnedStateEntry).getRecordsSize();
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
    try (final OCacheEntry pinnedStateEntry =
        loadPageForWrite(atomicOperation, fileId, stateEntryIndex, true)) {
      final OPaginatedClusterStateV0 paginatedClusterState =
          new OPaginatedClusterStateV0(pinnedStateEntry);
      paginatedClusterState.setSize(paginatedClusterState.getSize() + sizeDiff);
      paginatedClusterState.setRecordsSize(
          paginatedClusterState.getRecordsSize() + recordsSizeDiff);
    }
  }

  private void init(
      final int id,
      final String name,
      final String compression,
      final String encryption,
      final String encryptionKey,
      final String conflictStrategy)
      throws IOException {
    OFileUtils.checkValidName(name);

    this.compression = OCompressionFactory.INSTANCE.getCompression(compression, null);
    this.encryption = OEncryptionFactory.INSTANCE.getEncryption(encryption, encryptionKey);

    if (conflictStrategy != null) {
      this.recordConflictStrategy =
          Orient.instance().getRecordConflictStrategy().getStrategy(conflictStrategy);
    }

    this.id = id;
  }

  @Override
  public void setEncryption(final String method, final String key) {
    acquireExclusiveLock();
    try {
      encryption = OEncryptionFactory.INSTANCE.getEncryption(method, key);
    } catch (final IllegalArgumentException e) {
      //noinspection deprecation
      throw OException.wrapException(
          new OPaginatedClusterException(
              "Invalid value for " + ATTRIBUTES.ENCRYPTION + " attribute", this),
          e);
    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  public void setClusterName(final String newName) {
    acquireExclusiveLock();
    try {
      writeCache.renameFile(fileId, newName + getExtension());
      clusterPositionMap.rename(newName);

      setName(newName);
    } catch (IOException e) {
      throw OException.wrapException(
          new OPaginatedClusterException("Error during renaming of cluster", this), e);
    } finally {
      releaseExclusiveLock();
    }
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
      final OAtomicOperation atomicOperation)
      throws IOException {
    if (getFilledUpTo(atomicOperation, fileId) <= pageIndex) {
      return null;
    }

    final List<byte[]> recordChunks = new ArrayList<>(2);
    int contentSize = 0;

    long nextPagePointer;
    boolean firstEntry = true;
    do {
      try (final OCacheEntry cacheEntry = loadPageForRead(atomicOperation, fileId, pageIndex)) {
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
      final FindFreePageResult findFreePageResult =
          findFreePage(entryContent.length, atomicOperation);

      final int freePageIndex = findFreePageResult.freePageIndex;
      pageIndex = findFreePageResult.pageIndex;

      final boolean newRecord = freePageIndex >= FREE_LIST_SIZE;

      try (OCacheEntry cacheEntry =
          loadOrAddPageForWrite(atomicOperation, fileId, pageIndex, true)) {
        final OClusterPage localPage = new OClusterPage(cacheEntry);
        if (newRecord) {
          localPage.init();
        }

        assert newRecord || freePageIndex == calculateFreePageIndex(localPage);

        final int initialFreeSpace = localPage.getFreeSpace();

        position =
            localPage.appendRecord(
                recordVersion,
                entryContent,
                -1,
                atomicOperation.getBookedRecordPositions(id, cacheEntry.getPageIndex()));

        final int freeSpace = localPage.getFreeSpace();
        recordSizesDiff = initialFreeSpace - freeSpace;

        if (position >= 0) {
          finalVersion = localPage.getRecordVersion(position);
        }
      }

      updateFreePagesIndex(freePageIndex, pageIndex, atomicOperation);
    } while (position < 0);

    return new AddEntryResult(pageIndex, position, finalVersion, recordSizesDiff);
  }

  private FindFreePageResult findFreePage(
      final int contentSize, final OAtomicOperation atomicOperation) throws IOException {
    while (true) {
      int freePageIndex = contentSize / ONE_KB;
      freePageIndex -= PAGINATED_STORAGE_LOWEST_FREELIST_BOUNDARY.getValueAsInteger();
      if (freePageIndex < 0) {
        freePageIndex = 0;
      }

      long pageIndex;

      try (final OCacheEntry pinnedStateEntry =
          loadPageForRead(atomicOperation, fileId, stateEntryIndex)) {
        final OPaginatedClusterStateV0 freePageLists =
            new OPaginatedClusterStateV0(pinnedStateEntry);
        do {
          pageIndex = freePageLists.getFreeListPage(freePageIndex);
          freePageIndex++;
        } while (pageIndex < 0 && freePageIndex < FREE_LIST_SIZE);
      }

      if (pageIndex < 0) {
        pageIndex = getFilledUpTo(atomicOperation, fileId);
      } else {
        freePageIndex--;
      }

      if (freePageIndex < FREE_LIST_SIZE) {
        final OCacheEntry cacheEntry = loadPageForWrite(atomicOperation, fileId, pageIndex, true);

        // free list is broken automatically fix it
        if (cacheEntry == null) {
          updateFreePagesList(freePageIndex, -1, atomicOperation);

          continue;
        } else {
          int realFreePageIndex;
          try {
            final OClusterPage localPage = new OClusterPage(cacheEntry);
            realFreePageIndex = calculateFreePageIndex(localPage);
          } finally {
            cacheEntry.close();
          }

          if (realFreePageIndex != freePageIndex) {
            OLogManager.instance()
                .warn(
                    this,
                    "Page in file %s with index %d was placed in wrong free list, this error will be fixed automatically",
                    getFullName(),
                    pageIndex);

            updateFreePagesIndex(freePageIndex, pageIndex, atomicOperation);
            continue;
          }
        }
      }

      return new FindFreePageResult(pageIndex, freePageIndex);
    }
  }

  private void updateFreePagesIndex(
      final int prevFreePageIndex, final long pageIndex, final OAtomicOperation atomicOperation)
      throws IOException {

    try (final OCacheEntry cacheEntry =
        loadPageForWrite(atomicOperation, fileId, pageIndex, true)) {
      final OClusterPage localPage = new OClusterPage(cacheEntry);
      final int newFreePageIndex = calculateFreePageIndex(localPage);

      if (prevFreePageIndex == newFreePageIndex) {
        return;
      }

      final long nextPageIndex = localPage.getNextPage();
      final long prevPageIndex = localPage.getPrevPage();

      if (prevPageIndex >= 0) {
        try (final OCacheEntry prevPageCacheEntry =
            loadPageForWrite(atomicOperation, fileId, prevPageIndex, true)) {
          final OClusterPage prevPage = new OClusterPage(prevPageCacheEntry);
          assert calculateFreePageIndex(prevPage) == prevFreePageIndex;
          prevPage.setNextPage(nextPageIndex);
        }
      }

      if (nextPageIndex >= 0) {
        try (final OCacheEntry nextPageCacheEntry =
            loadPageForWrite(atomicOperation, fileId, nextPageIndex, true)) {
          final OClusterPage nextPage = new OClusterPage(nextPageCacheEntry);
          if (calculateFreePageIndex(nextPage) != prevFreePageIndex) {
            calculateFreePageIndex(nextPage);
          }

          assert calculateFreePageIndex(nextPage) == prevFreePageIndex;
          nextPage.setPrevPage(prevPageIndex);
        }
      }

      localPage.setNextPage(-1);
      localPage.setPrevPage(-1);

      if (prevFreePageIndex < 0 && newFreePageIndex < 0) {
        return;
      }

      if (prevFreePageIndex >= 0 && prevFreePageIndex < FREE_LIST_SIZE) {
        if (prevPageIndex < 0) {
          updateFreePagesList(prevFreePageIndex, nextPageIndex, atomicOperation);
        }
      }

      if (newFreePageIndex >= 0) {
        long oldFreePage;
        try (final OCacheEntry pinnedStateEntry =
            loadPageForRead(atomicOperation, fileId, stateEntryIndex)) {
          final OPaginatedClusterStateV0 clusterFreeList =
              new OPaginatedClusterStateV0(pinnedStateEntry);
          oldFreePage = clusterFreeList.getFreeListPage(newFreePageIndex);
        }

        if (oldFreePage >= 0) {
          try (final OCacheEntry oldFreePageCacheEntry =
              loadPageForWrite(atomicOperation, fileId, oldFreePage, true)) {
            final OClusterPage oldFreeLocalPage = new OClusterPage(oldFreePageCacheEntry);
            assert calculateFreePageIndex(oldFreeLocalPage) == newFreePageIndex;

            oldFreeLocalPage.setPrevPage(pageIndex);
          }

          localPage.setNextPage(oldFreePage);
          localPage.setPrevPage(-1);
        }

        updateFreePagesList(newFreePageIndex, pageIndex, atomicOperation);
      }
    }
  }

  private void updateFreePagesList(
      final int freeListIndex, final long pageIndex, final OAtomicOperation atomicOperation)
      throws IOException {
    try (final OCacheEntry pinnedStateEntry =
        loadPageForWrite(atomicOperation, fileId, stateEntryIndex, true)) {
      final OPaginatedClusterStateV0 paginatedClusterState =
          new OPaginatedClusterStateV0(pinnedStateEntry);
      paginatedClusterState.setFreeListPage(freeListIndex, pageIndex);
    }
  }

  private static int calculateFreePageIndex(final OClusterPage localPage) {
    int newFreePageIndex;
    if (localPage.isEmpty()) {
      newFreePageIndex = FREE_LIST_SIZE - 1;
    } else {
      newFreePageIndex = (localPage.getMaxRecordSize() - (ONE_KB - 1)) / ONE_KB;

      newFreePageIndex -= LOWEST_FREELIST_BOUNDARY;
    }
    return newFreePageIndex;
  }

  private void initCusterState(final OAtomicOperation atomicOperation) throws IOException {
    try (final OCacheEntry pinnedStateEntry = addPage(atomicOperation, fileId)) {
      final OPaginatedClusterStateV0 paginatedClusterState =
          new OPaginatedClusterStateV0(pinnedStateEntry);

      paginatedClusterState.setSize(0);
      paginatedClusterState.setRecordsSize(0);

      for (int i = 0; i < FREE_LIST_SIZE; i++) {
        paginatedClusterState.setFreeListPage(i, -1);
      }

      stateEntryIndex = pinnedStateEntry.getPageIndex();
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
    if (getFilledUpTo(atomicOperation, fileId) <= pageIndex) {
      debug.empty = true;
      return debug;
    }

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

        final OClusterPositionMapV0.OClusterPositionEntry[] nextPositions =
            clusterPositionMap.higherPositionsEntries(lastPosition, atomicOperation);
        if (nextPositions.length > 0) {
          final long newLastPosition = nextPositions[nextPositions.length - 1].getPosition();
          final List<OClusterBrowseEntry> nexv = new ArrayList<>(nextPositions.length);
          for (final OClusterPositionMapV0.OClusterPositionEntry pos : nextPositions) {
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
