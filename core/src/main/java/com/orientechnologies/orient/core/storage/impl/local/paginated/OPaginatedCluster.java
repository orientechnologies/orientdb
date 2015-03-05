/*
 * Copyright 2010-2013 Orient Technologies LTD (info--at--orientechnologies.com)
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

import com.orientechnologies.common.concur.lock.OModificationLock;
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
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.hashindex.local.cache.OCacheEntry;
import com.orientechnologies.orient.core.index.hashindex.local.cache.ODiskCache;
import com.orientechnologies.orient.core.storage.OCluster;
import com.orientechnologies.orient.core.storage.OClusterEntryIterator;
import com.orientechnologies.orient.core.storage.OPhysicalPosition;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurableComponent;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;
import com.orientechnologies.orient.core.version.ORecordVersion;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32;

import static com.orientechnologies.orient.core.config.OGlobalConfiguration.DISK_CACHE_PAGE_SIZE;
import static com.orientechnologies.orient.core.config.OGlobalConfiguration.PAGINATED_STORAGE_LOWEST_FREELIST_BOUNDARY;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientechnologies.com)
 * @since 10/7/13
 */
public class OPaginatedCluster extends ODurableComponent implements OCluster {
  public static final  String            DEF_EXTENSION            = ".pcl";
  private static final int               DISK_PAGE_SIZE           = DISK_CACHE_PAGE_SIZE.getValueAsInteger();
  private static final int               LOWEST_FREELIST_BOUNDARY = PAGINATED_STORAGE_LOWEST_FREELIST_BOUNDARY.getValueAsInteger();
  private final static int               FREE_LIST_SIZE           = DISK_PAGE_SIZE - LOWEST_FREELIST_BOUNDARY;
  private static final int               PAGE_INDEX_OFFSET        = 16;
  private static final int               RECORD_POSITION_MASK     = 0xFFFF;
  private static final int               ONE_KB                   = 1024;
  private final        OModificationLock externalModificationLock = new OModificationLock();
  private volatile OCompression                          compression;
  private          ODiskCache                            diskCache;
  private          OClusterPositionMap                   clusterPositionMap;
  private volatile String                                name;
  private          OAbstractPaginatedStorage             storageLocal;
  private volatile int                                   id;
  private          long                                  fileId;
  private          OStoragePaginatedClusterConfiguration config;
  private          OCacheEntry                           pinnedStateEntry;
  private          boolean                               useCRC32;
  private          ORecordConflictStrategy               recordConflictStrategy;

  private static final class AddEntryResult {
    private final long pageIndex;
    private final int  pagePosition;

    private final ORecordVersion recordVersion;
    private final int            recordsSizeDiff;

    public AddEntryResult(long pageIndex, int pagePosition, ORecordVersion recordVersion, int recordsSizeDiff) {
      this.pageIndex = pageIndex;
      this.pagePosition = pagePosition;
      this.recordVersion = recordVersion;
      this.recordsSizeDiff = recordsSizeDiff;
    }
  }

  private static final class FindFreePageResult {
    private final long pageIndex;
    private final int  freePageIndex;

    private FindFreePageResult(long pageIndex, int freePageIndex) {
      this.pageIndex = pageIndex;
      this.freePageIndex = freePageIndex;
    }
  }

  public OPaginatedCluster() {
    super(OGlobalConfiguration.ENVIRONMENT_CONCURRENT.getValueAsBoolean());
    useCRC32 = OGlobalConfiguration.STORAGE_USE_CRC32_FOR_EACH_RECORD.getValueAsBoolean();
  }

  @Override
  public void configure(final OStorage storage, final int id, final String clusterName, final Object... parameters) throws IOException {
    externalModificationLock.requestModificationLock();
    try {
      acquireExclusiveLock();
      try {
        final OContextConfiguration ctxCfg = storage.getConfiguration().getContextConfiguration();
        final String cfgCompression = ctxCfg.getValueAsString(OGlobalConfiguration.STORAGE_COMPRESSION_METHOD);

        config = new OStoragePaginatedClusterConfiguration(storage.getConfiguration(), id, clusterName, null, true, OStoragePaginatedClusterConfiguration.DEFAULT_GROW_FACTOR, OStoragePaginatedClusterConfiguration.DEFAULT_GROW_FACTOR, cfgCompression, null, OStorageClusterConfiguration.STATUS.ONLINE);
        config.name = clusterName;

        init((OAbstractPaginatedStorage) storage, config);
      } finally {
        releaseExclusiveLock();
      }
    } finally {
      externalModificationLock.releaseModificationLock();
    }
  }

  @Override
  public void configure(OStorage storage, OStorageClusterConfiguration config) throws IOException {
    externalModificationLock.requestModificationLock();
    try {
      acquireExclusiveLock();
      try {
        init((OAbstractPaginatedStorage) storage, config);
      } finally {
        releaseExclusiveLock();
      }
    } finally {
      externalModificationLock.releaseModificationLock();
    }
  }

  public boolean exists() {
    return diskCache.exists(name + DEF_EXTENSION);
  }

  @Override
  public void create(int startSize) throws IOException {
    externalModificationLock.requestModificationLock();
    try {
      acquireExclusiveLock();
      try {
        fileId = diskCache.openFile(name + DEF_EXTENSION);

        startAtomicOperation();

        initCusterState();

        endAtomicOperation(false);

        if (config.root.clusters.size()<=config.id) {
            config.root.clusters.add(config);
        } else {
            config.root.clusters.set(config.id, config);
        }

        clusterPositionMap.create();
      } catch (Throwable e) {
        endAtomicOperation(true);
        throw new OStorageException(null, e);
      } finally {
        releaseExclusiveLock();
      }
    } finally {
      externalModificationLock.releaseModificationLock();
    }
  }

  @Override
  public void open() throws IOException {
    externalModificationLock.requestModificationLock();
    try {
      acquireExclusiveLock();
      try {
        fileId = diskCache.openFile(name + DEF_EXTENSION);
        pinnedStateEntry = diskCache.load(fileId, 0, false);

        try {
          diskCache.pinPage(pinnedStateEntry);
        } finally {
          diskCache.release(pinnedStateEntry);
        }

        clusterPositionMap.open();
      } finally {
        releaseExclusiveLock();
      }
    } finally {
      externalModificationLock.releaseModificationLock();
    }
  }

  @Override
  public void close() throws IOException {
    close(true);
  }

  public void close(boolean flush) throws IOException {
    externalModificationLock.requestModificationLock();
    try {
      acquireExclusiveLock();
      try {
        if (flush) {
            synch();
        }

        diskCache.closeFile(fileId, flush);
        clusterPositionMap.close(flush);
      } finally {
        releaseExclusiveLock();
      }
    } finally {
      externalModificationLock.releaseModificationLock();
    }
  }

  @Override
  public void delete() throws IOException {
    externalModificationLock.requestModificationLock();
    try {
      acquireExclusiveLock();
      try {
        diskCache.deleteFile(fileId);
        clusterPositionMap.delete();
      } finally {
        releaseExclusiveLock();
      }
    } finally {
      externalModificationLock.releaseModificationLock();
    }
  }

  @Override
  public Object set(OCluster.ATTRIBUTES attribute, Object value) throws IOException {
    if (attribute == null) {
        throw new IllegalArgumentException("attribute is null");
    }

    final String stringValue = value != null ? value.toString() : null;

    externalModificationLock.requestModificationLock();
    try {
      acquireExclusiveLock();
      try {

        switch (attribute) {
        case NAME:
          setNameInternal(stringValue);
          break;
        case USE_WAL:
          setUseWalInternal(stringValue);
          break;
        case RECORD_GROW_FACTOR:
          setRecordGrowFactorInternal(stringValue);
          break;
        case RECORD_OVERFLOW_GROW_FACTOR:
          setRecordOverflowGrowFactorInternal(stringValue);
          break;
        case COMPRESSION:
          if (getEntries() > 0) {
              throw new IllegalArgumentException("Cannot change compression setting on cluster '" + getName()
                      + "' because it is not empty");
        }
          setCompressionInternal(stringValue);
          break;
        case CONFLICTSTRATEGY:
          setRecordConflictStrategy(stringValue);
          break;
        case STATUS: {
          return storageLocal.setClusterStatus(id, OStorageClusterConfiguration.STATUS.valueOf(stringValue.toUpperCase()));
        }
        default:
          throw new IllegalArgumentException("Runtime change of attribute '" + attribute + " is not supported");
        }

      } finally {
        releaseExclusiveLock();
      }
    } finally {
      externalModificationLock.releaseModificationLock();
    }

    return null;
  }

  @Override
  public boolean useWal() {
    acquireSharedLock();
    try {
      return config.useWal;
    } finally {
      releaseSharedLock();
    }

  }

  @Override
  public float recordGrowFactor() {
    acquireSharedLock();
    try {
      return config.recordGrowFactor;
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public float recordOverflowGrowFactor() {
    acquireSharedLock();
    try {
      return config.recordOverflowGrowFactor;
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public String compression() {
    acquireSharedLock();
    try {
      return config.compression;
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public void convertToTombstone(long iPosition) throws IOException {
    throw new UnsupportedOperationException("convertToTombstone");
  }

  public OPhysicalPosition createRecord(byte[] content, final ORecordVersion recordVersion, final byte recordType)
      throws IOException {
    content = compression.compress(content);

    externalModificationLock.requestModificationLock();
    try {
      acquireExclusiveLock();
      try {

        int grownContentSize = (int) (config.recordGrowFactor * content.length);
        int entryContentLength = grownContentSize + 2 * OByteSerializer.BYTE_SIZE + OIntegerSerializer.INT_SIZE
            + OLongSerializer.LONG_SIZE;

        if (useCRC32) {
            entryContentLength += OIntegerSerializer.INT_SIZE;
        }

        if (entryContentLength < OClusterPage.MAX_RECORD_SIZE) {
          startAtomicOperation();
          try {
            lockTillAtomicOperationCompletes();

            byte[] entryContent = new byte[entryContentLength];

            int entryPosition = 0;
            entryContent[entryPosition] = recordType;
            entryPosition++;

            OIntegerSerializer.INSTANCE.serializeNative(content.length, entryContent, entryPosition);
            entryPosition += OIntegerSerializer.INT_SIZE;

            System.arraycopy(content, 0, entryContent, entryPosition, content.length);
            entryPosition += grownContentSize;

            if (useCRC32) {
              CRC32 crc32 = new CRC32();
              crc32.update(entryContent, 0, entryPosition);
              OIntegerSerializer.INSTANCE.serializeNative((int) crc32.getValue(), entryContent, entryPosition);
              entryPosition += OIntegerSerializer.INT_SIZE;
            }

            entryContent[entryPosition] = 1;
            entryPosition++;

            OLongSerializer.INSTANCE.serializeNative(-1L, entryContent, entryPosition);

            ODurablePage.TrackMode trackMode = getTrackMode();

            final AddEntryResult addEntryResult = addEntry(recordVersion, entryContent, trackMode);

            updateClusterState(trackMode, 1, addEntryResult.recordsSizeDiff);

            final long clusterPosition = clusterPositionMap.add(addEntryResult.pageIndex, addEntryResult.pagePosition);

            endAtomicOperation(false);

            return createPhysicalPosition(recordType, clusterPosition, addEntryResult.recordVersion);
          } catch (Throwable e) {
            endAtomicOperation(true);
            throw new OStorageException(null, e);
          }
        } else {
          startAtomicOperation();
          try {
            lockTillAtomicOperationCompletes();

            final OClusterPage.TrackMode trackMode = getTrackMode();

            int entrySize = grownContentSize + OIntegerSerializer.INT_SIZE + OByteSerializer.BYTE_SIZE;

            if (useCRC32) {
                entrySize += OIntegerSerializer.INT_SIZE;
            }

            int fullEntryPosition = 0;
            byte[] fullEntry = new byte[entrySize];

            fullEntry[fullEntryPosition] = recordType;
            fullEntryPosition++;

            OIntegerSerializer.INSTANCE.serializeNative(content.length, fullEntry, fullEntryPosition);
            fullEntryPosition += OIntegerSerializer.INT_SIZE;

            System.arraycopy(content, 0, fullEntry, fullEntryPosition, content.length);
            fullEntryPosition += grownContentSize;

            if (useCRC32) {
              CRC32 crc32 = new CRC32();
              crc32.update(fullEntry, 0, fullEntryPosition);
              OIntegerSerializer.INSTANCE.serializeNative((int) crc32.getValue(), fullEntry, fullEntryPosition);
            }

            long prevPageRecordPointer = -1;
            long firstPageIndex = -1;
            int firstPagePosition = -1;

            ORecordVersion version = null;

            int from = 0;
            int to = from + (OClusterPage.MAX_RECORD_SIZE - OByteSerializer.BYTE_SIZE - OLongSerializer.LONG_SIZE);

            int recordsSizeDiff = 0;

            do {
              byte[] entryContent = new byte[to - from + OByteSerializer.BYTE_SIZE + OLongSerializer.LONG_SIZE];
              System.arraycopy(fullEntry, from, entryContent, 0, to - from);

              if (from > 0) {
                  entryContent[entryContent.length - OLongSerializer.LONG_SIZE - OByteSerializer.BYTE_SIZE] = 0;
              } else {
                  entryContent[entryContent.length - OLongSerializer.LONG_SIZE - OByteSerializer.BYTE_SIZE] = 1;
              }

              OLongSerializer.INSTANCE.serializeNative(-1L, entryContent, entryContent.length - OLongSerializer.LONG_SIZE);

              final AddEntryResult addEntryResult = addEntry(recordVersion, entryContent, trackMode);
              recordsSizeDiff += addEntryResult.recordsSizeDiff;

              if (firstPageIndex == -1) {
                firstPageIndex = addEntryResult.pageIndex;
                firstPagePosition = addEntryResult.pagePosition;
                version = addEntryResult.recordVersion;
              }

              long addedPagePointer = createPagePointer(addEntryResult.pageIndex, addEntryResult.pagePosition);
              if (prevPageRecordPointer >= 0) {
                long prevPageIndex = prevPageRecordPointer >>> PAGE_INDEX_OFFSET;
                int prevPageRecordPosition = (int) (prevPageRecordPointer & RECORD_POSITION_MASK);

                final OCacheEntry prevPageCacheEntry = diskCache.load(fileId, prevPageIndex, false);
                prevPageCacheEntry.acquireExclusiveLock();
                try {
                  final OClusterPage prevPage = new OClusterPage(prevPageCacheEntry, false, ODurablePage.TrackMode.FULL);
                  prevPage.setRecordLongValue(prevPageRecordPosition, -OLongSerializer.LONG_SIZE, addedPagePointer);

                  logPageChanges(prevPage, fileId, prevPageIndex, false);

                  prevPageCacheEntry.markDirty();
                } finally {
                  prevPageCacheEntry.releaseExclusiveLock();
                  diskCache.release(prevPageCacheEntry);
                }
              }

              prevPageRecordPointer = addedPagePointer;
              from = to;
              to = to + (OClusterPage.MAX_RECORD_SIZE - OLongSerializer.LONG_SIZE - OByteSerializer.BYTE_SIZE);
              if (to > fullEntry.length) {
                  to = fullEntry.length;
              }

            } while (from < to);

            updateClusterState(trackMode, 1, recordsSizeDiff);

            long clusterPosition = clusterPositionMap.add(firstPageIndex, firstPagePosition);

            endAtomicOperation(false);

            return createPhysicalPosition(recordType, clusterPosition, version);
          } catch (Throwable e) {
            endAtomicOperation(true);
            throw new OStorageException(null, e);
          }
        }
      } finally {
        releaseExclusiveLock();
      }
    } finally {
      externalModificationLock.releaseModificationLock();
    }
  }

  public ORawBuffer readRecord(long clusterPosition) throws IOException {
    acquireSharedLock();
    try {
      OClusterPositionMapBucket.PositionEntry positionEntry = clusterPositionMap.get(clusterPosition);
      if (positionEntry == null) {
          return null;
      }

      int recordPosition = positionEntry.getRecordPosition();
      long pageIndex = positionEntry.getPageIndex();

      if (diskCache.getFilledUpTo(fileId) <= pageIndex) {
          return null;
      }

      ORecordVersion recordVersion = null;
      OCacheEntry cacheEntry = diskCache.load(fileId, pageIndex, false);
      try {
        final OClusterPage localPage = new OClusterPage(cacheEntry, false, ODurablePage.TrackMode.NONE);
        if (localPage.isDeleted(recordPosition)) {
            return null;
        }

        recordVersion = localPage.getRecordVersion(recordPosition);
      } finally {
        diskCache.release(cacheEntry);
      }

      byte[] fullContent = readFullEntry(clusterPosition, pageIndex, recordPosition);
      if (fullContent == null) {
          return null;
      }

      if (useCRC32) {
        CRC32 crc32 = new CRC32();
        final int crcPosition = fullContent.length - OLongSerializer.LONG_SIZE - OByteSerializer.BYTE_SIZE
            - OIntegerSerializer.INT_SIZE;
        crc32.update(fullContent, 0, crcPosition);

        final int crc = OIntegerSerializer.INSTANCE.deserializeNative(fullContent, crcPosition);
        if (crc != (int) crc32.getValue()) {
            throw new OStorageException("Content of record for cluster with id " + id + " and position " + clusterPosition
                    + " is broken.");
        }
      }

      int fullContentPosition = 0;

      byte recordType = fullContent[fullContentPosition];
      fullContentPosition++;

      int readContentSize = OIntegerSerializer.INSTANCE.deserializeNative(fullContent, fullContentPosition);
      fullContentPosition += OIntegerSerializer.INT_SIZE;

      byte[] recordContent = compression.uncompress(fullContent, fullContentPosition, readContentSize);
      return new ORawBuffer(recordContent, recordVersion, recordType);
    } finally {
      releaseSharedLock();
    }
  }

  public boolean deleteRecord(long clusterPosition) throws IOException {
    externalModificationLock.requestModificationLock();
    boolean operationStarted = false;
    try {
      acquireExclusiveLock();
      try {
        OClusterPositionMapBucket.PositionEntry positionEntry = clusterPositionMap.get(clusterPosition);
        if (positionEntry == null) {
            return false;
        }

        long pageIndex = positionEntry.getPageIndex();
        int recordPosition = positionEntry.getRecordPosition();

        if (diskCache.getFilledUpTo(fileId) <= pageIndex) {
            return false;
        }

        final OClusterPage.TrackMode trackMode = getTrackMode();

        long nextPagePointer = -1;
        int removedContentSize = 0;
        do {
          final OCacheEntry cacheEntry = diskCache.load(fileId, pageIndex, false);
          cacheEntry.acquireExclusiveLock();
          int initialFreePageIndex;
          try {
            final OClusterPage localPage = new OClusterPage(cacheEntry, false, trackMode);
            initialFreePageIndex = calculateFreePageIndex(localPage);

            if (localPage.isDeleted(recordPosition)) {
              if (removedContentSize == 0) {
                  return false;
              } else {
                  throw new OStorageException("Content of record " + new ORecordId(id, clusterPosition) + " was broken.");
              }
            } else if (removedContentSize == 0) {
              startAtomicOperation();
              operationStarted = true;
              lockTillAtomicOperationCompletes();
            }

            byte[] content = localPage.getRecordBinaryValue(recordPosition, 0, localPage.getRecordSize(recordPosition));

            int initialFreeSpace = localPage.getFreeSpace();
            localPage.deleteRecord(recordPosition);

            removedContentSize += localPage.getFreeSpace() - initialFreeSpace;
            nextPagePointer = OLongSerializer.INSTANCE.deserializeNative(content, content.length - OLongSerializer.LONG_SIZE);

            logPageChanges(localPage, fileId, pageIndex, false);
          } finally {
            cacheEntry.releaseExclusiveLock();
            diskCache.release(cacheEntry);
          }

          updateFreePagesIndex(initialFreePageIndex, pageIndex, trackMode);

          pageIndex = nextPagePointer >>> PAGE_INDEX_OFFSET;
          recordPosition = (int) (nextPagePointer & RECORD_POSITION_MASK);
        } while (nextPagePointer >= 0);

        updateClusterState(trackMode, -1, -removedContentSize);

        clusterPositionMap.remove(clusterPosition);
        endAtomicOperation(false);

        return true;
      } catch (Throwable e) {
        if (operationStarted) {
            endAtomicOperation(true);
        }
        throw new OStorageException(null, e);
      } finally {
        releaseExclusiveLock();
      }
    } finally {
      externalModificationLock.releaseModificationLock();
    }
  }

  @Override
  public boolean hideRecord(long position) throws IOException {
    externalModificationLock.requestModificationLock();
    try {
      acquireExclusiveLock();
      try {
        OClusterPositionMapBucket.PositionEntry positionEntry = clusterPositionMap.get(position);
        if (positionEntry == null) {
            return false;
        }

        long pageIndex = positionEntry.getPageIndex();

        if (diskCache.getFilledUpTo(fileId) <= pageIndex) {
            return false;
        }

        startAtomicOperation();
        try {
          lockTillAtomicOperationCompletes();

          final OClusterPage.TrackMode trackMode = getTrackMode();
          updateClusterState(trackMode, -1, 0);
          clusterPositionMap.remove(position);
          endAtomicOperation(false);

          return true;
        } catch (Throwable e) {
          endAtomicOperation(true);
          throw new OStorageException(null, e);
        }
      } finally {
        releaseExclusiveLock();
      }
    } finally {
      externalModificationLock.releaseModificationLock();
    }

  }

  public void updateRecord(long clusterPosition, byte[] content, final ORecordVersion recordVersion,
      final byte recordType) throws IOException {
    content = compression.compress(content);

    externalModificationLock.requestModificationLock();
    try {
      acquireExclusiveLock();
      try {
        OClusterPositionMapBucket.PositionEntry positionEntry = clusterPositionMap.get(clusterPosition);
        if (positionEntry == null) {
            return;
        }

        int recordPosition = positionEntry.getRecordPosition();
        long pageIndex = positionEntry.getPageIndex();
        long pagePointer = createPagePointer(pageIndex, recordPosition);

        byte[] fullEntryContent = readFullEntry(clusterPosition, pageIndex, recordPosition);
        if (fullEntryContent == null) {
            return;
        }

        if (useCRC32) {
          CRC32 crc32 = new CRC32();
          final int crcPosition = fullEntryContent.length - OLongSerializer.LONG_SIZE - OByteSerializer.BYTE_SIZE
              - OIntegerSerializer.INT_SIZE;
          crc32.update(fullEntryContent, 0, crcPosition);

          final int crc = OIntegerSerializer.INSTANCE.deserializeNative(fullEntryContent, crcPosition);
          if (crc != (int) crc32.getValue()) {
              throw new OStorageException("Content of record for cluster with id " + id + " and position " + clusterPosition
                      + " is broken.");
          }
        }

        int updatedContentLength = content.length + 2 * OByteSerializer.BYTE_SIZE + OIntegerSerializer.INT_SIZE
            + OLongSerializer.LONG_SIZE;

        if (useCRC32) {
            updatedContentLength += OIntegerSerializer.INT_SIZE;
        }

        byte[] recordEntry;
        if (updatedContentLength <= fullEntryContent.length) {
            recordEntry = new byte[fullEntryContent.length - OLongSerializer.LONG_SIZE - OByteSerializer.BYTE_SIZE];
        } else {
          final int grownContent = (int) (content.length * config.recordOverflowGrowFactor);
          if (!useCRC32) {
              recordEntry = new byte[grownContent + OByteSerializer.BYTE_SIZE + OIntegerSerializer.INT_SIZE];
            } else {
              recordEntry = new byte[grownContent + OByteSerializer.BYTE_SIZE + 2 * OIntegerSerializer.INT_SIZE];
          }
        }

        final OClusterPage.TrackMode trackMode = getTrackMode();

        startAtomicOperation();
        try {
          lockTillAtomicOperationCompletes();

          int entryPosition = 0;
          recordEntry[entryPosition] = recordType;
          entryPosition++;

          OIntegerSerializer.INSTANCE.serializeNative(content.length, recordEntry, entryPosition);
          entryPosition += OIntegerSerializer.INT_SIZE;

          System.arraycopy(content, 0, recordEntry, entryPosition, content.length);

          if (useCRC32) {
            CRC32 crc32 = new CRC32();
            final int crcPosition = recordEntry.length - OIntegerSerializer.INT_SIZE;
            crc32.update(recordEntry, 0, crcPosition);

            OIntegerSerializer.INSTANCE.serializeNative((int) crc32.getValue(), recordEntry, crcPosition);
          }

          int recordsSizeDiff = 0;
          long prevPageRecordPointer = -1;

          int currentPos = 0;
          while (pagePointer >= 0 && currentPos < recordEntry.length) {
            recordPosition = (int) (pagePointer & RECORD_POSITION_MASK);
            pageIndex = pagePointer >>> PAGE_INDEX_OFFSET;

            int freePageIndex;
            final OCacheEntry cacheEntry = diskCache.load(fileId, pageIndex, false);
            cacheEntry.acquireExclusiveLock();
            try {
              final OClusterPage localPage = new OClusterPage(cacheEntry, false, trackMode);
              int freeSpace = localPage.getFreeSpace();
              freePageIndex = calculateFreePageIndex(localPage);

              final int chunkSize = localPage.getRecordSize(recordPosition);
              final long nextPagePointer = localPage.getRecordLongValue(recordPosition, -OLongSerializer.LONG_SIZE);

              int newChunkLen = Math.min(recordEntry.length - currentPos + OLongSerializer.LONG_SIZE + OByteSerializer.BYTE_SIZE,
                  chunkSize);
              int dataLen = newChunkLen - OLongSerializer.LONG_SIZE - OByteSerializer.BYTE_SIZE;

              byte[] newRecordChunk = new byte[newChunkLen];
              System.arraycopy(recordEntry, currentPos, newRecordChunk, 0, dataLen);

              if (currentPos > 0) {
                  newRecordChunk[newRecordChunk.length - OLongSerializer.LONG_SIZE - OByteSerializer.BYTE_SIZE] = 0;
              } else {
                  newRecordChunk[newRecordChunk.length - OLongSerializer.LONG_SIZE - OByteSerializer.BYTE_SIZE] = 1;
              }

              OLongSerializer.INSTANCE.serializeNative(-1L, newRecordChunk, newRecordChunk.length - OLongSerializer.LONG_SIZE);

              if (prevPageRecordPointer >= 0) {
                long prevPageIndex = prevPageRecordPointer >>> PAGE_INDEX_OFFSET;
                int prevPageRecordPosition = (int) (prevPageRecordPointer & RECORD_POSITION_MASK);

                final OCacheEntry prevPageCacheEntry = diskCache.load(fileId, prevPageIndex, false);
                prevPageCacheEntry.acquireExclusiveLock();
                try {
                  final OClusterPage prevPage = new OClusterPage(prevPageCacheEntry, false, trackMode);
                  prevPage.setRecordLongValue(prevPageRecordPosition, -OLongSerializer.LONG_SIZE, pagePointer);

                  logPageChanges(prevPage, fileId, prevPageIndex, false);

                  prevPageCacheEntry.markDirty();
                } finally {
                  prevPageCacheEntry.releaseExclusiveLock();
                  diskCache.release(prevPageCacheEntry);
                }
              }

              localPage.replaceRecord(recordPosition, newRecordChunk, recordVersion.getCounter() != -2 ? recordVersion : null);

              currentPos += dataLen;

              recordsSizeDiff += freeSpace - localPage.getFreeSpace();
              prevPageRecordPointer = pagePointer;
              pagePointer = nextPagePointer;

              logPageChanges(localPage, fileId, pageIndex, false);
            } finally {
              cacheEntry.releaseExclusiveLock();
              diskCache.release(cacheEntry);
            }

            updateFreePagesIndex(freePageIndex, pageIndex, trackMode);
          }

          int from = currentPos;
          int to = from + (OClusterPage.MAX_RECORD_SIZE - OByteSerializer.BYTE_SIZE - OLongSerializer.LONG_SIZE);
          if (to > recordEntry.length) {
              to = recordEntry.length;
          }

          while (from < to) {
            byte[] entryContent = new byte[to - from + OByteSerializer.BYTE_SIZE + OLongSerializer.LONG_SIZE];
            System.arraycopy(recordEntry, from, entryContent, 0, to - from);

            if (from > 0) {
                entryContent[entryContent.length - OLongSerializer.LONG_SIZE - OByteSerializer.BYTE_SIZE] = 0;
            } else {
                entryContent[entryContent.length - OLongSerializer.LONG_SIZE - OByteSerializer.BYTE_SIZE] = 1;
            }

            OLongSerializer.INSTANCE.serializeNative(-1L, entryContent, entryContent.length - OLongSerializer.LONG_SIZE);

            final AddEntryResult addEntryResult = addEntry(recordVersion, entryContent, trackMode);
            recordsSizeDiff += addEntryResult.recordsSizeDiff;

            long addedPagePointer = createPagePointer(addEntryResult.pageIndex, addEntryResult.pagePosition);
            if (prevPageRecordPointer >= 0) {

              long prevPageIndex = prevPageRecordPointer >>> PAGE_INDEX_OFFSET;
              int prevPageRecordPosition = (int) (prevPageRecordPointer & RECORD_POSITION_MASK);

              final OCacheEntry prevPageCacheEntry = diskCache.load(fileId, prevPageIndex, false);
              prevPageCacheEntry.acquireExclusiveLock();
              try {
                final OClusterPage prevPage = new OClusterPage(prevPageCacheEntry, false, trackMode);

                prevPage.setRecordLongValue(prevPageRecordPosition, -OLongSerializer.LONG_SIZE, addedPagePointer);

                logPageChanges(prevPage, fileId, prevPageIndex, false);

                prevPageCacheEntry.markDirty();
              } finally {
                prevPageCacheEntry.releaseExclusiveLock();
                diskCache.release(prevPageCacheEntry);
              }
            }

            prevPageRecordPointer = addedPagePointer;
            from = to;
            to = to + (OClusterPage.MAX_RECORD_SIZE - OLongSerializer.LONG_SIZE - OByteSerializer.BYTE_SIZE);
            if (to > recordEntry.length) {
                to = recordEntry.length;
            }
          }

          updateClusterState(trackMode, 0, recordsSizeDiff);

          endAtomicOperation(false);
        } catch (Throwable e) {
          endAtomicOperation(true);
          throw new OStorageException(null, e);
        }
      } finally {
        releaseExclusiveLock();
      }
    } finally {
      externalModificationLock.releaseModificationLock();
    }
  }

  @Override
  public long getTombstonesCount() {
    return 0;
  }

  @Override
  public boolean hasTombstonesSupport() {
    return false;
  }

  @Override
  public void truncate() throws IOException {
    storageLocal.checkForClusterPermissions(getName());

    externalModificationLock.requestModificationLock();
    try {
      acquireExclusiveLock();
      try {
        if (config.useWal) {
            startAtomicOperation();
        }

        diskCache.truncateFile(fileId);
        clusterPositionMap.truncate();

        initCusterState();

        if (config.useWal) {
            endAtomicOperation(false);
        }

      } catch (Throwable e) {
        if (config.useWal) {
            endAtomicOperation(true);
        }
        throw new OStorageException(null, e);
      } finally {
        releaseExclusiveLock();
      }
    } finally {
      externalModificationLock.releaseModificationLock();
    }
  }

  @Override
  public OPhysicalPosition getPhysicalPosition(OPhysicalPosition position) throws IOException {
    acquireSharedLock();
    try {
      long clusterPosition = position.clusterPosition;
      OClusterPositionMapBucket.PositionEntry positionEntry = clusterPositionMap.get(clusterPosition);

      if (positionEntry == null) {
          return null;
      }

      long pageIndex = positionEntry.getPageIndex();
      int recordPosition = positionEntry.getRecordPosition();

      long pagesCount = diskCache.getFilledUpTo(fileId);
      if (pageIndex >= pagesCount) {
          return null;
      }

      OCacheEntry cacheEntry = diskCache.load(fileId, pageIndex, false);
      try {
        final OClusterPage localPage = new OClusterPage(cacheEntry, false, ODurablePage.TrackMode.NONE);
        if (localPage.isDeleted(recordPosition)) {
            return null;
        }

        if (localPage.getRecordByteValue(recordPosition, -OLongSerializer.LONG_SIZE - OByteSerializer.BYTE_SIZE) == 0) {
            return null;
        }

        final OPhysicalPosition physicalPosition = new OPhysicalPosition();
        physicalPosition.recordSize = -1;

        physicalPosition.recordType = localPage.getRecordByteValue(recordPosition, 0);
        physicalPosition.recordVersion = localPage.getRecordVersion(recordPosition);
        physicalPosition.clusterPosition = position.clusterPosition;

        return physicalPosition;
      } finally {
        diskCache.release(cacheEntry);
      }

    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public long getEntries() {
    acquireSharedLock();
    try {
      diskCache.loadPinnedPage(pinnedStateEntry);
      try {
        return new OPaginatedClusterState(pinnedStateEntry, ODurablePage.TrackMode.NONE).getSize();
      } finally {
        diskCache.release(pinnedStateEntry);
      }
    } catch (IOException ioe) {
      throw new OStorageException("Error during retrieval of size of " + name + " cluster.");
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public long getFirstPosition() throws IOException {
    acquireSharedLock();
    try {
      return clusterPositionMap.getFirstPosition();
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public long getLastPosition() throws IOException {
    acquireSharedLock();
    try {
      return clusterPositionMap.getLastPosition();
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public int getId() {
    return id;
  }

  @Override
  public void synch() throws IOException {
    acquireSharedLock();
    try {
      diskCache.flushFile(fileId);
      clusterPositionMap.flush();
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public void setSoftlyClosed(boolean softlyClosed) throws IOException {
    acquireExclusiveLock();
    try {
      diskCache.setSoftlyClosed(fileId, softlyClosed);
    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  public boolean wasSoftlyClosed() throws IOException {
    acquireSharedLock();
    try {
      return diskCache.wasSoftlyClosed(fileId) || clusterPositionMap.wasSoftlyClosed();
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public long getRecordsSize() throws IOException {
    acquireSharedLock();
    try {
      diskCache.loadPinnedPage(pinnedStateEntry);
      try {
        return new OPaginatedClusterState(pinnedStateEntry, ODurablePage.TrackMode.NONE).getRecordsSize();
      } finally {
        diskCache.release(pinnedStateEntry);
      }
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public boolean isHashBased() {
    return false;
  }

  @Override
  public OClusterEntryIterator absoluteIterator() {
    acquireSharedLock();
    try {
      return new OClusterEntryIterator(this);
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public OPhysicalPosition[] higherPositions(OPhysicalPosition position) throws IOException {
    acquireSharedLock();
    try {
      final long[] clusterPositions = clusterPositionMap.higherPositions(position.clusterPosition);
      return convertToPhysicalPositions(clusterPositions);
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public OPhysicalPosition[] ceilingPositions(OPhysicalPosition position) throws IOException {
    acquireSharedLock();
    try {
      final long[] clusterPositions = clusterPositionMap.ceilingPositions(position.clusterPosition);
      return convertToPhysicalPositions(clusterPositions);
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public OPhysicalPosition[] lowerPositions(OPhysicalPosition position) throws IOException {
    acquireSharedLock();
    try {
      final long[] clusterPositions = clusterPositionMap.lowerPositions(position.clusterPosition);
      return convertToPhysicalPositions(clusterPositions);
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public OPhysicalPosition[] floorPositions(OPhysicalPosition position) throws IOException {
    acquireSharedLock();
    try {
      final long[] clusterPositions = clusterPositionMap.floorPositions(position.clusterPosition);
      return convertToPhysicalPositions(clusterPositions);
    } finally {
      releaseSharedLock();
    }
  }

  public OModificationLock getExternalModificationLock() {
    return externalModificationLock;
  }

  public ORecordConflictStrategy getRecordConflictStrategy() {
    return recordConflictStrategy;
  }

  private void setRecordConflictStrategy(final String stringValue) {
    recordConflictStrategy = Orient.instance().getRecordConflictStrategy().getStrategy(stringValue);
    config.conflictStrategy = stringValue;
    storageLocal.getConfiguration().update();
  }

  @Override
  protected void endAtomicOperation(boolean rollback) throws IOException {
    if (!config.useWal) {
        return;
    }

    super.endAtomicOperation(rollback);
  }

  @Override
  protected void startAtomicOperation() throws IOException {
    if (!config.useWal) {
        return;
    }

    super.startAtomicOperation();
  }

  @Override
  protected void logPageChanges(ODurablePage localPage, long fileId, long pageIndex, boolean isNewPage) throws IOException {
    if (!config.useWal) {
        return;
    }

    super.logPageChanges(localPage, fileId, pageIndex, isNewPage);
  }

  @Override
  protected ODurablePage.TrackMode getTrackMode() {
    if (!config.useWal) {
        return ODurablePage.TrackMode.NONE;
    }

    return super.getTrackMode();
  }

  private long createPagePointer(long pageIndex, int pagePosition) {
    return pageIndex << PAGE_INDEX_OFFSET | pagePosition;
  }

  private void updateClusterState(ODurablePage.TrackMode trackMode, long sizeDiff, long recordsSizeDiff) throws IOException {
    diskCache.loadPinnedPage(pinnedStateEntry);
    pinnedStateEntry.acquireExclusiveLock();
    try {
      OPaginatedClusterState paginatedClusterState = new OPaginatedClusterState(pinnedStateEntry, trackMode);
      paginatedClusterState.setSize(paginatedClusterState.getSize() + sizeDiff);
      paginatedClusterState.setRecordsSize(paginatedClusterState.getRecordsSize() + recordsSizeDiff);

      logPageChanges(paginatedClusterState, fileId, pinnedStateEntry.getPageIndex(), false);
      pinnedStateEntry.markDirty();
    } finally {
      pinnedStateEntry.releaseExclusiveLock();
      diskCache.release(pinnedStateEntry);
    }
  }

  private void init(OAbstractPaginatedStorage storage, OStorageClusterConfiguration config) throws IOException {
    OFileUtils.checkValidName(config.getName());

    this.config = (OStoragePaginatedClusterConfiguration) config;
    this.compression = OCompressionFactory.INSTANCE.getCompression(this.config.compression);
    if (((OStoragePaginatedClusterConfiguration) config).conflictStrategy != null) {
        this.recordConflictStrategy = Orient.instance().getRecordConflictStrategy()
                .getStrategy(((OStoragePaginatedClusterConfiguration) config).conflictStrategy);
    }

    storageLocal = storage;

    init(storage);

    diskCache = storageLocal.getDiskCache();
    name = config.getName();
    this.id = config.getId();

    clusterPositionMap = new OClusterPositionMap(storage, diskCache, name, this.config.useWal);
  }

  private void setCompressionInternal(String stringValue) {
    try {
      compression = OCompressionFactory.INSTANCE.getCompression(stringValue);
      config.compression = stringValue;
      storageLocal.getConfiguration().update();
    } catch (IllegalArgumentException e) {
      throw new OStorageException("Invalid value for " + OCluster.ATTRIBUTES.COMPRESSION + " attribute. ", e);
    }

  }

  private void setRecordOverflowGrowFactorInternal(String stringValue) {
    try {
      float growFactor = Float.parseFloat(stringValue);
      if (growFactor < 1) {
          throw new OStorageException(OCluster.ATTRIBUTES.RECORD_OVERFLOW_GROW_FACTOR + " can not be less than 1");
      }

      config.recordOverflowGrowFactor = growFactor;
      storageLocal.getConfiguration().update();
    } catch (NumberFormatException nfe) {
      throw new OStorageException("Invalid value for cluster attribute " + OCluster.ATTRIBUTES.RECORD_OVERFLOW_GROW_FACTOR
          + " was passed [" + stringValue + "].", nfe);
    }
  }

  private void setRecordGrowFactorInternal(String stringValue) {
    try {
      float growFactor = Float.parseFloat(stringValue);
      if (growFactor < 1) {
          throw new OStorageException(OCluster.ATTRIBUTES.RECORD_GROW_FACTOR + " can not be less than 1");
      }

      config.recordGrowFactor = growFactor;
      storageLocal.getConfiguration().update();
    } catch (NumberFormatException nfe) {
      throw new OStorageException("Invalid value for cluster attribute " + OCluster.ATTRIBUTES.RECORD_GROW_FACTOR + " was passed ["
          + stringValue + "].", nfe);
    }
  }

  private void setUseWalInternal(String stringValue) {
    if (!(stringValue.equals("true") || stringValue.equals("false"))) {
        throw new OStorageException("Invalid value for cluster attribute " + OCluster.ATTRIBUTES.USE_WAL + " was passed ["
                + stringValue + "].");
    }

    config.useWal = Boolean.valueOf(stringValue);
    clusterPositionMap.setUseWal(config.useWal);
    storageLocal.getConfiguration().update();
  }

  private void setNameInternal(String newName) throws IOException {
    diskCache.renameFile(fileId, this.name + DEF_EXTENSION, newName + DEF_EXTENSION);
    clusterPositionMap.rename(newName);

    config.name = newName;
    storageLocal.renameCluster(name, newName);
    name = newName;
    storageLocal.getConfiguration().update();
  }

  private OPhysicalPosition createPhysicalPosition(byte recordType, long clusterPosition, ORecordVersion version) {
    final OPhysicalPosition physicalPosition = new OPhysicalPosition();
    physicalPosition.recordType = recordType;
    physicalPosition.recordSize = -1;
    physicalPosition.clusterPosition = clusterPosition;
    physicalPosition.recordVersion = version;
    return physicalPosition;
  }

  private byte[] readFullEntry(long clusterPosition, long pageIndex, int recordPosition) throws IOException {
    if (diskCache.getFilledUpTo(fileId) <= pageIndex) {
        return null;
    }

    final List<byte[]> recordChunks = new ArrayList<byte[]>();
    int contentSize = 0;

    long nextPagePointer = -1;
    boolean firstEntry = true;
    do {
      OCacheEntry cacheEntry = diskCache.load(fileId, pageIndex, false);
      try {
        final OClusterPage localPage = new OClusterPage(cacheEntry, false, ODurablePage.TrackMode.NONE);

        if (localPage.isDeleted(recordPosition)) {
          if (recordChunks.isEmpty()) {
              return null;
          } else {
              throw new OStorageException("Content of record " + new ORecordId(id, clusterPosition) + " was broken.");
          }
        }

        byte[] content = localPage.getRecordBinaryValue(recordPosition, 0, localPage.getRecordSize(recordPosition));

        if (firstEntry && content[content.length - OLongSerializer.LONG_SIZE - OByteSerializer.BYTE_SIZE] == 0) {
            return null;
        }

        recordChunks.add(content);
        nextPagePointer = OLongSerializer.INSTANCE.deserializeNative(content, content.length - OLongSerializer.LONG_SIZE);
        contentSize += content.length - OLongSerializer.LONG_SIZE - OByteSerializer.BYTE_SIZE;

        firstEntry = false;
      } finally {
        diskCache.release(cacheEntry);
      }

      pageIndex = nextPagePointer >>> PAGE_INDEX_OFFSET;
      recordPosition = (int) (nextPagePointer & RECORD_POSITION_MASK);
    } while (nextPagePointer >= 0);

    byte[] fullContent;
    if (recordChunks.size() == 1) {
        fullContent = recordChunks.get(0);
    } else {
      fullContent = new byte[contentSize + OLongSerializer.LONG_SIZE + OByteSerializer.BYTE_SIZE];
      int fullContentPosition = 0;
      for (byte[] recordChuck : recordChunks) {
        System.arraycopy(recordChuck, 0, fullContent, fullContentPosition, recordChuck.length - OLongSerializer.LONG_SIZE
            - OByteSerializer.BYTE_SIZE);
        fullContentPosition += recordChuck.length - OLongSerializer.LONG_SIZE - OByteSerializer.BYTE_SIZE;
      }
    }

    return fullContent;
  }

  private AddEntryResult addEntry(ORecordVersion recordVersion, byte[] entryContent, OClusterPage.TrackMode trackMode)
      throws IOException {
    final FindFreePageResult findFreePageResult = findFreePage(entryContent.length, trackMode);

    int freePageIndex = findFreePageResult.freePageIndex;
    long pageIndex = findFreePageResult.pageIndex;

    boolean newRecord = freePageIndex >= FREE_LIST_SIZE;

    final OCacheEntry cacheEntry = diskCache.load(fileId, pageIndex, false);

    cacheEntry.acquireExclusiveLock();
    int recordSizesDiff;
    int position;
    final ORecordVersion finalVersion;
    try {
      final OClusterPage localPage = new OClusterPage(cacheEntry, newRecord, trackMode);
      assert newRecord || freePageIndex == calculateFreePageIndex(localPage);

      int initialFreeSpace = localPage.getFreeSpace();

      position = localPage.appendRecord(recordVersion, entryContent, false);
      assert position >= 0;

      finalVersion = localPage.getRecordVersion(position);

      int freeSpace = localPage.getFreeSpace();
      recordSizesDiff = initialFreeSpace - freeSpace;

      logPageChanges(localPage, fileId, pageIndex, newRecord);
    } finally {
      cacheEntry.releaseExclusiveLock();
      diskCache.release(cacheEntry);
    }

    updateFreePagesIndex(freePageIndex, pageIndex, trackMode);

    return new AddEntryResult(pageIndex, position, finalVersion, recordSizesDiff);
  }

  private FindFreePageResult findFreePage(int contentSize, OClusterPage.TrackMode trackMode) throws IOException {
    diskCache.loadPinnedPage(pinnedStateEntry);
    try {
      while (true) {
        int freePageIndex = contentSize / ONE_KB;
        freePageIndex -= PAGINATED_STORAGE_LOWEST_FREELIST_BOUNDARY.getValueAsInteger();
        if (freePageIndex < 0) {
            freePageIndex = 0;
        }

        OPaginatedClusterState freePageLists = new OPaginatedClusterState(pinnedStateEntry, ODurablePage.TrackMode.NONE);
        long pageIndex;
        do {
          pageIndex = freePageLists.getFreeListPage(freePageIndex);
          freePageIndex++;
        } while (pageIndex < 0 && freePageIndex < FREE_LIST_SIZE);

        if (pageIndex < 0) {
            pageIndex = diskCache.getFilledUpTo(fileId);
        } else {
            freePageIndex--;
        }

        if (freePageIndex < FREE_LIST_SIZE) {
          OCacheEntry cacheEntry = diskCache.load(fileId, pageIndex, false);
          int realFreePageIndex;
          try {
            OClusterPage localPage = new OClusterPage(cacheEntry, false, ODurablePage.TrackMode.NONE);
            realFreePageIndex = calculateFreePageIndex(localPage);
          } finally {
            diskCache.release(cacheEntry);
          }

          if (realFreePageIndex != freePageIndex) {
            OLogManager.instance().warn(this,
                "Page in file %s with index %d was placed in wrong free list, this error will be fixed automatically.",
                name + DEF_EXTENSION, pageIndex);

            updateFreePagesIndex(freePageIndex, pageIndex, trackMode);
            continue;
          }
        }

        return new FindFreePageResult(pageIndex, freePageIndex);
      }
    } finally {
      diskCache.release(pinnedStateEntry);
    }
  }

  private void updateFreePagesIndex(int prevFreePageIndex, long pageIndex, OClusterPage.TrackMode trackMode) throws IOException {
    final OCacheEntry cacheEntry = diskCache.load(fileId, pageIndex, false);
    cacheEntry.acquireExclusiveLock();
    try {
      final OClusterPage localPage = new OClusterPage(cacheEntry, false, trackMode);
      int newFreePageIndex = calculateFreePageIndex(localPage);

      if (prevFreePageIndex == newFreePageIndex) {
          return;
      }

      long nextPageIndex = localPage.getNextPage();
      long prevPageIndex = localPage.getPrevPage();

      if (prevPageIndex >= 0) {
        final OCacheEntry prevPageCacheEntry = diskCache.load(fileId, prevPageIndex, false);
        prevPageCacheEntry.acquireExclusiveLock();
        try {
          final OClusterPage prevPage = new OClusterPage(prevPageCacheEntry, false, trackMode);
          assert calculateFreePageIndex(prevPage) == prevFreePageIndex;
          prevPage.setNextPage(nextPageIndex);

          logPageChanges(prevPage, fileId, prevPageIndex, false);

          prevPageCacheEntry.markDirty();
        } finally {
          prevPageCacheEntry.releaseExclusiveLock();
          diskCache.release(prevPageCacheEntry);
        }
      }

      if (nextPageIndex >= 0) {
        final OCacheEntry nextPageCacheEntry = diskCache.load(fileId, nextPageIndex, false);
        nextPageCacheEntry.acquireExclusiveLock();
        try {
          final OClusterPage nextPage = new OClusterPage(nextPageCacheEntry, false, trackMode);
          if (calculateFreePageIndex(nextPage) != prevFreePageIndex) {
              calculateFreePageIndex(nextPage);
          }

          assert calculateFreePageIndex(nextPage) == prevFreePageIndex;
          nextPage.setPrevPage(prevPageIndex);

          logPageChanges(nextPage, fileId, nextPageIndex, false);
        } finally {
          nextPageCacheEntry.releaseExclusiveLock();
          diskCache.release(nextPageCacheEntry);
        }
      }

      localPage.setNextPage(-1);
      localPage.setPrevPage(-1);

      if (prevFreePageIndex < 0 && newFreePageIndex < 0) {
          return;
      }

      if (prevFreePageIndex >= 0 && prevFreePageIndex < FREE_LIST_SIZE) {
        if (prevPageIndex < 0) {
            updateFreePagesList(prevFreePageIndex, nextPageIndex);
        }
      }

      if (newFreePageIndex >= 0) {
        long oldFreePage;
        diskCache.loadPinnedPage(pinnedStateEntry);
        try {
          OPaginatedClusterState clusterFreeList = new OPaginatedClusterState(pinnedStateEntry, ODurablePage.TrackMode.NONE);
          oldFreePage = clusterFreeList.getFreeListPage(newFreePageIndex);
        } finally {
          diskCache.release(pinnedStateEntry);
        }

        if (oldFreePage >= 0) {
          final OCacheEntry oldFreePageCacheEntry = diskCache.load(fileId, oldFreePage, false);
          oldFreePageCacheEntry.acquireExclusiveLock();
          try {
            final OClusterPage oldFreeLocalPage = new OClusterPage(oldFreePageCacheEntry, false, trackMode);
            assert calculateFreePageIndex(oldFreeLocalPage) == newFreePageIndex;

            oldFreeLocalPage.setPrevPage(pageIndex);

            logPageChanges(oldFreeLocalPage, fileId, oldFreePage, false);

            oldFreePageCacheEntry.markDirty();
          } finally {
            oldFreePageCacheEntry.releaseExclusiveLock();
            diskCache.release(oldFreePageCacheEntry);
          }

          localPage.setNextPage(oldFreePage);
          localPage.setPrevPage(-1);
        }

        updateFreePagesList(newFreePageIndex, pageIndex);
      }

      logPageChanges(localPage, fileId, pageIndex, false);
    } finally {
      cacheEntry.releaseExclusiveLock();
      diskCache.release(cacheEntry);
    }
  }

  private void updateFreePagesList(int freeListIndex, long pageIndex) throws IOException {
    ODurablePage.TrackMode trackMode = getTrackMode();

    diskCache.loadPinnedPage(pinnedStateEntry);
    pinnedStateEntry.acquireExclusiveLock();
    try {
      OPaginatedClusterState paginatedClusterState = new OPaginatedClusterState(pinnedStateEntry, trackMode);
      paginatedClusterState.setFreeListPage(freeListIndex, pageIndex);

      logPageChanges(paginatedClusterState, fileId, pinnedStateEntry.getPageIndex(), false);
      pinnedStateEntry.markDirty();
    } finally {
      pinnedStateEntry.releaseExclusiveLock();
      diskCache.release(pinnedStateEntry);
    }
  }

  private int calculateFreePageIndex(OClusterPage localPage) {
    int newFreePageIndex;
    if (localPage.isEmpty()) {
        newFreePageIndex = FREE_LIST_SIZE - 1;
    } else {
      newFreePageIndex = (localPage.getMaxRecordSize() - (ONE_KB - 1)) / ONE_KB;

      newFreePageIndex -= LOWEST_FREELIST_BOUNDARY;
    }
    return newFreePageIndex;
  }

  private void initCusterState() throws IOException {
    ODurablePage.TrackMode trackMode = getTrackMode();

    pinnedStateEntry = diskCache.allocateNewPage(fileId);
    pinnedStateEntry.acquireExclusiveLock();
    try {
      OPaginatedClusterState paginatedClusterState = new OPaginatedClusterState(pinnedStateEntry, trackMode);

      diskCache.pinPage(pinnedStateEntry);
      paginatedClusterState.setSize(0);
      paginatedClusterState.setRecordsSize(0);

      for (int i = 0; i < FREE_LIST_SIZE; i++) {
          paginatedClusterState.setFreeListPage(i, -1);
      }

      logPageChanges(paginatedClusterState, fileId, pinnedStateEntry.getPageIndex(), true);
      pinnedStateEntry.markDirty();
    } finally {
      pinnedStateEntry.releaseExclusiveLock();
      diskCache.release(pinnedStateEntry);
    }

  }

  private OPhysicalPosition[] convertToPhysicalPositions(long[] clusterPositions) {
    OPhysicalPosition[] positions = new OPhysicalPosition[clusterPositions.length];
    for (int i = 0; i < positions.length; i++) {
      OPhysicalPosition physicalPosition = new OPhysicalPosition();
      physicalPosition.clusterPosition = clusterPositions[i];
      positions[i] = physicalPosition;
    }
    return positions;
  }
}
