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
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.OMetadata;
import com.orientechnologies.orient.core.storage.OCluster;
import com.orientechnologies.orient.core.storage.OClusterEntryIterator;
import com.orientechnologies.orient.core.storage.OPhysicalPosition;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurableComponent;
import com.orientechnologies.orient.core.version.ORecordVersion;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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
  private final    boolean                               systemCluster;
  private volatile OCompression                          compression;
  private          OClusterPositionMap                   clusterPositionMap;
  private          OAbstractPaginatedStorage             storageLocal;
  private volatile int                                   id;
  private          long                                  fileId;
  private          OStoragePaginatedClusterConfiguration config;
  private          long                                  pinnedStateEntryIndex;
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

  public OPaginatedCluster(final String name, final OAbstractPaginatedStorage storage) {
    super(storage, name, ".pcl");

    systemCluster = OMetadata.SYSTEM_CLUSTER.contains(name);
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
  public void create(int startSize) throws IOException {
    externalModificationLock.requestModificationLock();
    try {
      OAtomicOperation atomicOperation = startAtomicOperation();
      acquireExclusiveLock();
      try {
        fileId = addFile(atomicOperation, getFullName());

        initCusterState(atomicOperation);

        if (config.root.clusters.size() <= config.id)
          config.root.clusters.add(config);
        else
          config.root.clusters.set(config.id, config);

        clusterPositionMap.create();

        endAtomicOperation(false, null);
      } catch (Exception e) {
        endAtomicOperation(true, e);
        throw new OStorageException("Error during creation of cluster with name " + getName(), e);
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
        final OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();
        fileId = openFile(atomicOperation, getFullName());

        OCacheEntry pinnedStateEntry = loadPage(atomicOperation, fileId, 0, false);
        try {
          pinPage(atomicOperation, pinnedStateEntry);
          pinnedStateEntryIndex = pinnedStateEntry.getPageIndex();
        } finally {
          releasePage(atomicOperation, pinnedStateEntry);
        }

        clusterPositionMap.open();
      } finally {
        releaseExclusiveLock();
      }
    } finally {
      externalModificationLock.releaseModificationLock();
    }
  }

  public void replaceFile(File file) throws IOException {
    externalModificationLock.requestModificationLock();
    try {
      acquireExclusiveLock();
      try {
        final String newFileName = file.getName() + "$temp";

        final File rootDir = new File(storageLocal.getConfiguration().getDirectory());
        final File newFile = new File(rootDir, newFileName);

        OFileUtils.copyFile(file, newFile);

        final long newFileId = readCache.openFile(newFileName, writeCache);

        readCache.deleteFile(fileId, writeCache);
        fileId = newFileId;
        writeCache.renameFile(fileId, newFileName, getFullName());
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
        if (flush)
          synch();

        readCache.closeFile(fileId, flush, writeCache);
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
      final OAtomicOperation atomicOperation = startAtomicOperation();
      acquireExclusiveLock();
      try {
        deleteFile(atomicOperation, fileId);

        clusterPositionMap.delete();

        endAtomicOperation(false, null);
      } catch (IOException ioe) {
        endAtomicOperation(true, ioe);

        throw ioe;
      } catch (Exception e) {
        endAtomicOperation(true, e);

        throw new OStorageException("Error during deletion of cluset " + getName(), e);
      } finally {
        releaseExclusiveLock();
      }
    } finally {
      externalModificationLock.releaseModificationLock();
    }
  }

  @Override
  public Object set(OCluster.ATTRIBUTES attribute, Object value) throws IOException {
    if (attribute == null)
      throw new IllegalArgumentException("attribute is null");

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
          if (getEntries() > 0)
            throw new IllegalArgumentException("Cannot change compression setting on cluster '" + getName()
                + "' because it is not empty");
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
  public boolean isSystemCluster() {
    return systemCluster;
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
      OAtomicOperation atomicOperation = startAtomicOperation();
      acquireExclusiveLock();
      try {
        int entryContentLength = getEntryContentLength(content.length);

        if (entryContentLength < OClusterPage.MAX_RECORD_SIZE) {
          try {
            byte[] entryContent = new byte[entryContentLength];

            int entryPosition = 0;
            entryContent[entryPosition] = recordType;
            entryPosition++;

            OIntegerSerializer.INSTANCE.serializeNative(content.length, entryContent, entryPosition);
            entryPosition += OIntegerSerializer.INT_SIZE;

            System.arraycopy(content, 0, entryContent, entryPosition, content.length);
            entryPosition += content.length;

            entryContent[entryPosition] = 1;
            entryPosition++;

            OLongSerializer.INSTANCE.serializeNative(-1L, entryContent, entryPosition);

            final AddEntryResult addEntryResult = addEntry(recordVersion, entryContent, atomicOperation);

            updateClusterState(1, addEntryResult.recordsSizeDiff, atomicOperation);

            final long clusterPosition = clusterPositionMap.add(addEntryResult.pageIndex, addEntryResult.pagePosition);

            endAtomicOperation(false, null);

            return createPhysicalPosition(recordType, clusterPosition, addEntryResult.recordVersion);
          } catch (Exception e) {
            endAtomicOperation(true, e);
            throw new OStorageException(null, e);
          }
        } else {
          try {
            int entrySize = content.length + OIntegerSerializer.INT_SIZE + OByteSerializer.BYTE_SIZE;

            int fullEntryPosition = 0;
            byte[] fullEntry = new byte[entrySize];

            fullEntry[fullEntryPosition] = recordType;
            fullEntryPosition++;

            OIntegerSerializer.INSTANCE.serializeNative(content.length, fullEntry, fullEntryPosition);
            fullEntryPosition += OIntegerSerializer.INT_SIZE;

            System.arraycopy(content, 0, fullEntry, fullEntryPosition, content.length);

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

              if (from > 0)
                entryContent[entryContent.length - OLongSerializer.LONG_SIZE - OByteSerializer.BYTE_SIZE] = 0;
              else
                entryContent[entryContent.length - OLongSerializer.LONG_SIZE - OByteSerializer.BYTE_SIZE] = 1;

              OLongSerializer.INSTANCE.serializeNative(-1L, entryContent, entryContent.length - OLongSerializer.LONG_SIZE);

              final AddEntryResult addEntryResult = addEntry(recordVersion, entryContent, atomicOperation);
              recordsSizeDiff += addEntryResult.recordsSizeDiff;

              if (firstPageIndex == -1) {
                firstPageIndex = addEntryResult.pageIndex;
                firstPagePosition = addEntryResult.pagePosition;
                version = addEntryResult.recordVersion;
              }

              long addedPagePointer = createPagePointer(addEntryResult.pageIndex, addEntryResult.pagePosition);
              if (prevPageRecordPointer >= 0) {
                long prevPageIndex = getPageIndex(prevPageRecordPointer);
                int prevPageRecordPosition = getRecordPosition(prevPageRecordPointer);

                final OCacheEntry prevPageCacheEntry = loadPage(atomicOperation, fileId, prevPageIndex, false);
                prevPageCacheEntry.acquireExclusiveLock();
                try {
                  final OClusterPage prevPage = new OClusterPage(prevPageCacheEntry, false, getChangesTree(atomicOperation,
                      prevPageCacheEntry));
                  prevPage.setRecordLongValue(prevPageRecordPosition, -OLongSerializer.LONG_SIZE, addedPagePointer);
                } finally {
                  prevPageCacheEntry.releaseExclusiveLock();
                  releasePage(atomicOperation, prevPageCacheEntry);
                }
              }

              prevPageRecordPointer = addedPagePointer;
              from = to;
              to = to + (OClusterPage.MAX_RECORD_SIZE - OLongSerializer.LONG_SIZE - OByteSerializer.BYTE_SIZE);
              if (to > fullEntry.length)
                to = fullEntry.length;

            } while (from < to);

            updateClusterState(1, recordsSizeDiff, atomicOperation);

            long clusterPosition = clusterPositionMap.add(firstPageIndex, firstPagePosition);

            endAtomicOperation(false, null);

            return createPhysicalPosition(recordType, clusterPosition, version);
          } catch (Exception e) {
            endAtomicOperation(true, e);
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

  private static int getEntryContentLength(int grownContentSize) {
    int entryContentLength = grownContentSize + 2 * OByteSerializer.BYTE_SIZE + OIntegerSerializer.INT_SIZE
        + OLongSerializer.LONG_SIZE;

    return entryContentLength;
  }

  public ORawBuffer readRecord(long clusterPosition) throws IOException {
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();
        OClusterPositionMapBucket.PositionEntry positionEntry = clusterPositionMap.get(clusterPosition);
        if (positionEntry == null)
          return null;

        int recordPosition = positionEntry.getRecordPosition();
        long pageIndex = positionEntry.getPageIndex();

        if (getFilledUpTo(atomicOperation, fileId) <= pageIndex)
          return null;

        ORecordVersion recordVersion = null;
        OCacheEntry cacheEntry = loadPage(atomicOperation, fileId, pageIndex, false);
        try {
          final OClusterPage localPage = new OClusterPage(cacheEntry, false, getChangesTree(atomicOperation, cacheEntry));
          if (localPage.isDeleted(recordPosition))
            return null;

          recordVersion = localPage.getRecordVersion(recordPosition);
        } finally {
          releasePage(atomicOperation, cacheEntry);
        }

        byte[] fullContent = readFullEntry(clusterPosition, pageIndex, recordPosition, atomicOperation);
        if (fullContent == null)
          return null;

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
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  @Override
  public ORawBuffer readRecordIfVersionIsNotLatest(long clusterPosition, ORecordVersion recordVersion) throws IOException,
      ORecordNotFoundException {
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();
        OClusterPositionMapBucket.PositionEntry positionEntry = clusterPositionMap.get(clusterPosition);

        if (positionEntry == null)
          throw new ORecordNotFoundException("Record for cluster with id " + id + " and position " + clusterPosition
              + " is absent.");

        int recordPosition = positionEntry.getRecordPosition();
        long pageIndex = positionEntry.getPageIndex();

        if (getFilledUpTo(atomicOperation, fileId) <= pageIndex)
          throw new ORecordNotFoundException("Record for cluster with id " + id + " and position " + clusterPosition
              + " is absent.");

        ORecordVersion loadedRecordVersion = null;

        OCacheEntry cacheEntry = loadPage(atomicOperation, fileId, pageIndex, false);
        try {
          final OClusterPage localPage = new OClusterPage(cacheEntry, false, getChangesTree(atomicOperation, cacheEntry));
          if (localPage.isDeleted(recordPosition))
            throw new ORecordNotFoundException("Record for cluster with id " + id + " and position " + clusterPosition
                + " is absent.");

          loadedRecordVersion = localPage.getRecordVersion(recordPosition);
        } finally {
          releasePage(atomicOperation, cacheEntry);
        }

        if (loadedRecordVersion.compareTo(recordVersion) > 0)
          return readRecord(clusterPosition);

        return null;
      } finally {
        releaseSharedLock();
      }
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  public boolean deleteRecord(long clusterPosition) throws IOException {
    externalModificationLock.requestModificationLock();
    try {
      OAtomicOperation atomicOperation = startAtomicOperation();
      acquireExclusiveLock();
      try {

        OClusterPositionMapBucket.PositionEntry positionEntry = clusterPositionMap.get(clusterPosition);
        if (positionEntry == null) {
          endAtomicOperation(false, null);
          return false;
        }

        long pageIndex = positionEntry.getPageIndex();
        int recordPosition = positionEntry.getRecordPosition();

        if (getFilledUpTo(atomicOperation, fileId) <= pageIndex) {
          endAtomicOperation(false, null);
          return false;
        }

        long nextPagePointer = -1;
        int removedContentSize = 0;

        do {
          OCacheEntry cacheEntry = loadPage(atomicOperation, fileId, pageIndex, false);
          cacheEntry.acquireExclusiveLock();
          int initialFreePageIndex;
          try {
            OClusterPage localPage = new OClusterPage(cacheEntry, false, getChangesTree(atomicOperation, cacheEntry));
            initialFreePageIndex = calculateFreePageIndex(localPage);

            if (localPage.isDeleted(recordPosition)) {
              if (removedContentSize == 0) {
                endAtomicOperation(false, null);
                return false;
              } else
                throw new OStorageException("Content of record " + new ORecordId(id, clusterPosition) + " was broken.");
            } else if (removedContentSize == 0) {
              cacheEntry.releaseExclusiveLock();
              releasePage(atomicOperation, cacheEntry);

              cacheEntry = loadPage(atomicOperation, fileId, pageIndex, false);
              cacheEntry.acquireExclusiveLock();

              localPage = new OClusterPage(cacheEntry, false, getChangesTree(atomicOperation, cacheEntry));
            }

            byte[] content = localPage.getRecordBinaryValue(recordPosition, 0, localPage.getRecordSize(recordPosition));

            int initialFreeSpace = localPage.getFreeSpace();
            localPage.deleteRecord(recordPosition);

            removedContentSize += localPage.getFreeSpace() - initialFreeSpace;
            nextPagePointer = OLongSerializer.INSTANCE.deserializeNative(content, content.length - OLongSerializer.LONG_SIZE);
          } finally {
            cacheEntry.releaseExclusiveLock();
            releasePage(atomicOperation, cacheEntry);
          }

          updateFreePagesIndex(initialFreePageIndex, pageIndex, atomicOperation);

          pageIndex = getPageIndex(nextPagePointer);
          recordPosition = getRecordPosition(nextPagePointer);
        } while (nextPagePointer >= 0);

        updateClusterState(-1, -removedContentSize, atomicOperation);

        clusterPositionMap.remove(clusterPosition);
        endAtomicOperation(false, null);

        return true;
      } catch (Exception e) {
        endAtomicOperation(true, e);
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
      OAtomicOperation atomicOperation = startAtomicOperation();
      acquireExclusiveLock();
      try {
        OClusterPositionMapBucket.PositionEntry positionEntry = clusterPositionMap.get(position);

        if (positionEntry == null) {
          endAtomicOperation(false, null);
          return false;
        }

        long pageIndex = positionEntry.getPageIndex();
        if (getFilledUpTo(atomicOperation, fileId) <= pageIndex) {
          endAtomicOperation(false, null);
          return false;
        }

        try {
          updateClusterState(-1, 0, atomicOperation);
          clusterPositionMap.remove(position);
          endAtomicOperation(false, null);

          return true;
        } catch (Exception e) {
          endAtomicOperation(true, e);
          throw new OStorageException(null, e);
        }
      } finally {
        releaseExclusiveLock();
      }
    } finally {
      externalModificationLock.releaseModificationLock();
    }

  }

  public void updateRecord(final long clusterPosition, byte[] content, final ORecordVersion recordVersion, final byte recordType)
      throws IOException {
    content = compression.compress(content);

    externalModificationLock.requestModificationLock();
    try {
      OAtomicOperation atomicOperation = startAtomicOperation();

      acquireExclusiveLock();
      try {
        final OClusterPositionMapBucket.PositionEntry positionEntry = clusterPositionMap.get(clusterPosition);

        if (positionEntry == null) {
          endAtomicOperation(false, null);
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
              entrySize = Math.min(getEntryContentLength(content.length), OClusterPage.MAX_RECORD_SIZE);
              to = entrySize - (2 * OByteSerializer.BYTE_SIZE + OIntegerSerializer.INT_SIZE + OLongSerializer.LONG_SIZE);
            } else {
              entrySize = Math.min(content.length - from + OByteSerializer.BYTE_SIZE + OLongSerializer.LONG_SIZE,
                  OClusterPage.MAX_RECORD_SIZE);
              to = from + entrySize - (OByteSerializer.BYTE_SIZE + OLongSerializer.LONG_SIZE);
            }

            updateEntry = new byte[entrySize];
            int entryPosition = 0;

            if (from == 0) {
              updateEntry[entryPosition] = recordType;
              entryPosition++;

              OIntegerSerializer.INSTANCE.serializeNative(content.length, updateEntry, entryPosition);
              entryPosition += OIntegerSerializer.INT_SIZE;
            }

            System.arraycopy(content, from, updateEntry, entryPosition, to - from);
            entryPosition += to - from;

            if (nextPageIndex == positionEntry.getPageIndex())
              updateEntry[entryPosition] = 1;

            entryPosition++;

            OLongSerializer.INSTANCE.serializeNative(-1, updateEntry, entryPosition);

            if (to < content.length) {
              assert entrySize == OClusterPage.MAX_RECORD_SIZE;
            }
          } else {
            entrySize = updateEntry.length;

            if (from == 0) {
              to = entrySize - (2 * OByteSerializer.BYTE_SIZE + OIntegerSerializer.INT_SIZE + OLongSerializer.LONG_SIZE);
            } else {
              to = from + entrySize - (OByteSerializer.BYTE_SIZE + OLongSerializer.LONG_SIZE);
            }
          }

          int freePageIndex = -1;

          if (nextPageIndex < 0) {
            FindFreePageResult findFreePageResult = findFreePage(entrySize, atomicOperation);
            nextPageIndex = findFreePageResult.pageIndex;
            freePageIndex = findFreePageResult.freePageIndex;
          }

          boolean isNew = false;
          OCacheEntry cacheEntry = loadPage(atomicOperation, fileId, nextPageIndex, false);
          if (cacheEntry == null) {
            cacheEntry = addPage(atomicOperation, fileId);
            nextPageIndex = cacheEntry.getPageIndex();
            isNew = true;
          }

          cacheEntry.acquireExclusiveLock();
          try {
            final OClusterPage localPage = new OClusterPage(cacheEntry, isNew, getChangesTree(atomicOperation, cacheEntry));
            final int pageFreeSpace = localPage.getFreeSpace();

            if (freePageIndex < 0)
              freePageIndex = calculateFreePageIndex(localPage);
            else
              assert isNew || freePageIndex == calculateFreePageIndex(localPage);

            if (nextRecordPosition >= 0) {
              if (localPage.isDeleted(nextRecordPosition))
                throw new OStorageException("Record with rid : " + new ORecordId(id, clusterPosition) + " was deleted.");

              int currentEntrySize = localPage.getRecordSize(nextRecordPosition);
              nextEntryPointer = localPage.getRecordLongValue(nextRecordPosition, currentEntrySize - OLongSerializer.LONG_SIZE);

              if (currentEntrySize == entrySize) {
                localPage.replaceRecord(nextRecordPosition, updateEntry, recordVersion);
                updatedEntryPosition = nextRecordPosition;
              } else {
                localPage.deleteRecord(nextRecordPosition);

                if (localPage.getFreeSpace() >= entrySize) {
                  updatedEntryPosition = localPage.appendRecord(recordVersion, updateEntry);
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
              updatedEntryPosition = localPage.appendRecord(recordVersion, updateEntry);

              assert updatedEntryPosition >= 0;

              nextPageIndex = -1;
              nextRecordPosition = -1;
            }

            sizeDiff += pageFreeSpace - localPage.getFreeSpace();

          } finally {
            cacheEntry.releaseExclusiveLock();
            releasePage(atomicOperation, cacheEntry);
          }

          updateFreePagesIndex(freePageIndex, cacheEntry.getPageIndex(), atomicOperation);

          if (updatedEntryPosition >= 0) {
            if (from == 0) {
              newPageIndex = cacheEntry.getPageIndex();
              newRecordPosition = updatedEntryPosition;
            }

            from = to;

            if (prevPageIndex >= 0) {
              OCacheEntry prevCacheEntry = loadPage(atomicOperation, fileId, prevPageIndex, false);
              prevCacheEntry.acquireExclusiveLock();
              try {
                OClusterPage prevPage = new OClusterPage(prevCacheEntry, false, getChangesTree(atomicOperation, prevCacheEntry));
                prevPage.setRecordLongValue(prevRecordPosition, -OLongSerializer.LONG_SIZE,
                    createPagePointer(cacheEntry.getPageIndex(), updatedEntryPosition));
              } finally {
                prevCacheEntry.releaseExclusiveLock();
                releasePage(atomicOperation, prevCacheEntry);
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

          final int freePagesIndex;
          final int freeSpace;

          OCacheEntry cacheEntry = loadPage(atomicOperation, fileId, nextPageIndex, false);
          cacheEntry.acquireExclusiveLock();
          try {
            final OClusterPage localPage = new OClusterPage(cacheEntry, false, getChangesTree(atomicOperation, cacheEntry));
            freeSpace = localPage.getFreeSpace();
            freePagesIndex = calculateFreePageIndex(localPage);

            nextEntryPointer = localPage.getRecordLongValue(nextRecordPosition, -OLongSerializer.LONG_SIZE);
            localPage.deleteRecord(nextRecordPosition);

            sizeDiff += freeSpace - localPage.getFreeSpace();
          } finally {
            cacheEntry.releaseExclusiveLock();
            releasePage(atomicOperation, cacheEntry);
          }

          updateFreePagesIndex(freePagesIndex, nextPageIndex, atomicOperation);
        }

        assert newPageIndex >= 0;
        assert newRecordPosition >= 0;

        if (newPageIndex != positionEntry.getPageIndex() || newRecordPosition != positionEntry.getRecordPosition()) {
          clusterPositionMap.update(clusterPosition, new OClusterPositionMapBucket.PositionEntry(newPageIndex, newRecordPosition));
        }

        updateClusterState(0, sizeDiff, atomicOperation);

        endAtomicOperation(false, null);
      } catch (Exception e) {
        endAtomicOperation(true, e);
        throw new OStorageException("Error during record update.", e);
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
      OAtomicOperation atomicOperation = startAtomicOperation();

      acquireExclusiveLock();
      try {
        truncateFile(atomicOperation, fileId);
        clusterPositionMap.truncate();

        initCusterState(atomicOperation);

        endAtomicOperation(false, null);

      } catch (Exception e) {
        endAtomicOperation(true, e);
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
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        long clusterPosition = position.clusterPosition;
        OClusterPositionMapBucket.PositionEntry positionEntry = clusterPositionMap.get(clusterPosition);

        if (positionEntry == null)
          return null;

        OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();
        long pageIndex = positionEntry.getPageIndex();
        int recordPosition = positionEntry.getRecordPosition();

        long pagesCount = getFilledUpTo(atomicOperation, fileId);
        if (pageIndex >= pagesCount)
          return null;

        OCacheEntry cacheEntry = loadPage(atomicOperation, fileId, pageIndex, false);
        try {
          final OClusterPage localPage = new OClusterPage(cacheEntry, false, getChangesTree(atomicOperation, cacheEntry));
          if (localPage.isDeleted(recordPosition))
            return null;

          if (localPage.getRecordByteValue(recordPosition, -OLongSerializer.LONG_SIZE - OByteSerializer.BYTE_SIZE) == 0)
            return null;

          final OPhysicalPosition physicalPosition = new OPhysicalPosition();
          physicalPosition.recordSize = -1;

          physicalPosition.recordType = localPage.getRecordByteValue(recordPosition, 0);
          physicalPosition.recordVersion = localPage.getRecordVersion(recordPosition);
          physicalPosition.clusterPosition = position.clusterPosition;

          return physicalPosition;
        } finally {
          releasePage(atomicOperation, cacheEntry);
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
        final OCacheEntry pinnedStateEntry = loadPage(atomicOperation, fileId, pinnedStateEntryIndex, true);
        try {
          return new OPaginatedClusterState(pinnedStateEntry, getChangesTree(atomicOperation, pinnedStateEntry)).getSize();
        } finally {
          releasePage(atomicOperation, pinnedStateEntry);
        }
      } finally {
        releaseSharedLock();
      }
    } catch (IOException ioe) {
      throw new OStorageException("Error during retrieval of size of " + getName() + " cluster.", ioe);
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
        return clusterPositionMap.getFirstPosition();
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
        return clusterPositionMap.getLastPosition();
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

  /**
   * Returns the fileId used in WriteCache.
   */
  public long getFileId() {
    return fileId;
  }

  @Override
  public void synch() throws IOException {
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

        final OCacheEntry pinnedStateEntry = loadPage(atomicOperation, fileId, pinnedStateEntryIndex, true);
        try {
          return new OPaginatedClusterState(pinnedStateEntry, getChangesTree(atomicOperation, pinnedStateEntry)).getRecordsSize();
        } finally {
          releasePage(atomicOperation, pinnedStateEntry);
        }
      } finally {
        releaseSharedLock();
      }
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  @Override
  public boolean isHashBased() {
    return false;
  }

  @Override
  public OClusterEntryIterator absoluteIterator() {
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        return new OClusterEntryIterator(this);
      } finally {
        releaseSharedLock();
      }
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  @Override
  public OPhysicalPosition[] higherPositions(OPhysicalPosition position) throws IOException {
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        final long[] clusterPositions = clusterPositionMap.higherPositions(position.clusterPosition);
        return convertToPhysicalPositions(clusterPositions);
      } finally {
        releaseSharedLock();
      }
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  @Override
  public OPhysicalPosition[] ceilingPositions(OPhysicalPosition position) throws IOException {
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        final long[] clusterPositions = clusterPositionMap.ceilingPositions(position.clusterPosition);
        return convertToPhysicalPositions(clusterPositions);
      } finally {
        releaseSharedLock();
      }
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  @Override
  public OPhysicalPosition[] lowerPositions(OPhysicalPosition position) throws IOException {
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        final long[] clusterPositions = clusterPositionMap.lowerPositions(position.clusterPosition);
        return convertToPhysicalPositions(clusterPositions);
      } finally {
        releaseSharedLock();
      }
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  @Override
  public OPhysicalPosition[] floorPositions(OPhysicalPosition position) throws IOException {
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        final long[] clusterPositions = clusterPositionMap.floorPositions(position.clusterPosition);
        return convertToPhysicalPositions(clusterPositions);
      } finally {
        releaseSharedLock();
      }
    } finally {
      atomicOperationsManager.releaseReadLock(this);
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
  protected OAtomicOperation startAtomicOperation() throws IOException {
    return atomicOperationsManager.startAtomicOperation(this, !config.useWal);
  }

  private void updateClusterState(long sizeDiff, long recordsSizeDiff, OAtomicOperation atomicOperation) throws IOException {
    final OCacheEntry pinnedStateEntry = loadPage(atomicOperation, fileId, pinnedStateEntryIndex, true);
    pinnedStateEntry.acquireExclusiveLock();
    try {
      OPaginatedClusterState paginatedClusterState = new OPaginatedClusterState(pinnedStateEntry, getChangesTree(atomicOperation,
          pinnedStateEntry));
      paginatedClusterState.setSize(paginatedClusterState.getSize() + sizeDiff);
      paginatedClusterState.setRecordsSize(paginatedClusterState.getRecordsSize() + recordsSizeDiff);
    } finally {
      pinnedStateEntry.releaseExclusiveLock();
      releasePage(atomicOperation, pinnedStateEntry);
    }
  }

  private void init(OAbstractPaginatedStorage storage, OStorageClusterConfiguration config) throws IOException {
    OFileUtils.checkValidName(config.getName());

    this.config = (OStoragePaginatedClusterConfiguration) config;
    this.compression = OCompressionFactory.INSTANCE.getCompression(this.config.compression);
    if (((OStoragePaginatedClusterConfiguration) config).conflictStrategy != null)
      this.recordConflictStrategy = Orient.instance().getRecordConflictStrategy()
          .getStrategy(((OStoragePaginatedClusterConfiguration) config).conflictStrategy);

    storageLocal = storage;

    this.id = config.getId();

    clusterPositionMap = new OClusterPositionMap(storage, getName(), this.config.useWal);
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
      if (growFactor < 1)
        throw new OStorageException(OCluster.ATTRIBUTES.RECORD_OVERFLOW_GROW_FACTOR + " cannot be less than 1");

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
      if (growFactor < 1)
        throw new OStorageException(OCluster.ATTRIBUTES.RECORD_GROW_FACTOR + " cannot be less than 1");

      config.recordGrowFactor = growFactor;
      storageLocal.getConfiguration().update();
    } catch (NumberFormatException nfe) {
      throw new OStorageException("Invalid value for cluster attribute " + OCluster.ATTRIBUTES.RECORD_GROW_FACTOR + " was passed ["
          + stringValue + "].", nfe);
    }
  }

  private void setUseWalInternal(String stringValue) {
    if (!(stringValue.equals("true") || stringValue.equals("false")))
      throw new OStorageException("Invalid value for cluster attribute " + OCluster.ATTRIBUTES.USE_WAL + " was passed ["
          + stringValue + "].");

    config.useWal = Boolean.valueOf(stringValue);
    clusterPositionMap.setUseWal(config.useWal);
    storageLocal.getConfiguration().update();
  }

  private void setNameInternal(String newName) throws IOException {

    writeCache.renameFile(fileId, getFullName(), newName + getExtension());
    clusterPositionMap.rename(newName);

    config.name = newName;
    storageLocal.renameCluster(getName(), newName);
    setName(newName);

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

  private byte[] readFullEntry(long clusterPosition, long pageIndex, int recordPosition, OAtomicOperation atomicOperation)
      throws IOException {
    if (getFilledUpTo(atomicOperation, fileId) <= pageIndex)
      return null;

    final List<byte[]> recordChunks = new ArrayList<byte[]>();
    int contentSize = 0;

    long nextPagePointer = -1;
    boolean firstEntry = true;
    do {
      OCacheEntry cacheEntry = loadPage(atomicOperation, fileId, pageIndex, false);
      try {
        final OClusterPage localPage = new OClusterPage(cacheEntry, false, getChangesTree(atomicOperation, cacheEntry));

        if (localPage.isDeleted(recordPosition)) {
          if (recordChunks.isEmpty())
            return null;
          else
            throw new OStorageException("Content of record " + new ORecordId(id, clusterPosition) + " was broken.");
        }

        byte[] content = localPage.getRecordBinaryValue(recordPosition, 0, localPage.getRecordSize(recordPosition));

        if (firstEntry && content[content.length - OLongSerializer.LONG_SIZE - OByteSerializer.BYTE_SIZE] == 0)
          return null;

        recordChunks.add(content);
        nextPagePointer = OLongSerializer.INSTANCE.deserializeNative(content, content.length - OLongSerializer.LONG_SIZE);
        contentSize += content.length - OLongSerializer.LONG_SIZE - OByteSerializer.BYTE_SIZE;

        firstEntry = false;
      } finally {
        releasePage(atomicOperation, cacheEntry);
      }

      pageIndex = getPageIndex(nextPagePointer);
      recordPosition = getRecordPosition(nextPagePointer);
    } while (nextPagePointer >= 0);

    byte[] fullContent;
    if (recordChunks.size() == 1)
      fullContent = recordChunks.get(0);
    else {
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

  private static long createPagePointer(long pageIndex, int pagePosition) {
    return pageIndex << PAGE_INDEX_OFFSET | pagePosition;
  }

  private static int getRecordPosition(long nextPagePointer) {
    return (int) (nextPagePointer & RECORD_POSITION_MASK);
  }

  private static long getPageIndex(long nextPagePointer) {
    return nextPagePointer >>> PAGE_INDEX_OFFSET;
  }

  private AddEntryResult addEntry(ORecordVersion recordVersion, byte[] entryContent, OAtomicOperation atomicOperation)
      throws IOException {
    final FindFreePageResult findFreePageResult = findFreePage(entryContent.length, atomicOperation);

    int freePageIndex = findFreePageResult.freePageIndex;
    long pageIndex = findFreePageResult.pageIndex;

    boolean newRecord = freePageIndex >= FREE_LIST_SIZE;

    OCacheEntry cacheEntry = loadPage(atomicOperation, fileId, pageIndex, false);
    if (cacheEntry == null)
      cacheEntry = addPage(atomicOperation, fileId);

    cacheEntry.acquireExclusiveLock();
    int recordSizesDiff;
    int position;
    final ORecordVersion finalVersion;

    try {
      final OClusterPage localPage = new OClusterPage(cacheEntry, newRecord, getChangesTree(atomicOperation, cacheEntry));
      assert newRecord || freePageIndex == calculateFreePageIndex(localPage);

      int initialFreeSpace = localPage.getFreeSpace();

      position = localPage.appendRecord(recordVersion, entryContent);
      assert position >= 0;

      finalVersion = localPage.getRecordVersion(position);

      int freeSpace = localPage.getFreeSpace();
      recordSizesDiff = initialFreeSpace - freeSpace;
    } finally {
      cacheEntry.releaseExclusiveLock();
      releasePage(atomicOperation, cacheEntry);
    }

    updateFreePagesIndex(freePageIndex, pageIndex, atomicOperation);

    return new AddEntryResult(pageIndex, position, finalVersion, recordSizesDiff);
  }

  private FindFreePageResult findFreePage(int contentSize, OAtomicOperation atomicOperation) throws IOException {
    final OCacheEntry pinnedStateEntry = loadPage(atomicOperation, fileId, pinnedStateEntryIndex, true);
    try {
      while (true) {
        int freePageIndex = contentSize / ONE_KB;
        freePageIndex -= PAGINATED_STORAGE_LOWEST_FREELIST_BOUNDARY.getValueAsInteger();
        if (freePageIndex < 0)
          freePageIndex = 0;

        OPaginatedClusterState freePageLists = new OPaginatedClusterState(pinnedStateEntry, getChangesTree(atomicOperation,
            pinnedStateEntry));
        long pageIndex;
        do {
          pageIndex = freePageLists.getFreeListPage(freePageIndex);
          freePageIndex++;
        } while (pageIndex < 0 && freePageIndex < FREE_LIST_SIZE);

        if (pageIndex < 0)
          pageIndex = getFilledUpTo(atomicOperation, fileId);
        else
          freePageIndex--;

        if (freePageIndex < FREE_LIST_SIZE) {
          OCacheEntry cacheEntry = loadPage(atomicOperation, fileId, pageIndex, false);
          int realFreePageIndex;
          try {
            OClusterPage localPage = new OClusterPage(cacheEntry, false, getChangesTree(atomicOperation, cacheEntry));
            realFreePageIndex = calculateFreePageIndex(localPage);
          } finally {
            releasePage(atomicOperation, cacheEntry);
          }

          if (realFreePageIndex != freePageIndex) {
            OLogManager.instance().warn(this,
                "Page in file %s with index %d was placed in wrong free list, this error will be fixed automatically.",
                getFullName(), pageIndex);

            updateFreePagesIndex(freePageIndex, pageIndex, atomicOperation);
            continue;
          }
        }

        return new FindFreePageResult(pageIndex, freePageIndex);
      }
    } finally {
      releasePage(atomicOperation, pinnedStateEntry);
    }
  }

  private void updateFreePagesIndex(int prevFreePageIndex, long pageIndex, OAtomicOperation atomicOperation) throws IOException {
    final OCacheEntry cacheEntry = loadPage(atomicOperation, fileId, pageIndex, false);

    cacheEntry.acquireExclusiveLock();
    try {
      final OClusterPage localPage = new OClusterPage(cacheEntry, false, getChangesTree(atomicOperation, cacheEntry));
      int newFreePageIndex = calculateFreePageIndex(localPage);

      if (prevFreePageIndex == newFreePageIndex)
        return;

      long nextPageIndex = localPage.getNextPage();
      long prevPageIndex = localPage.getPrevPage();

      if (prevPageIndex >= 0) {
        final OCacheEntry prevPageCacheEntry = loadPage(atomicOperation, fileId, prevPageIndex, false);
        prevPageCacheEntry.acquireExclusiveLock();
        try {
          final OClusterPage prevPage = new OClusterPage(prevPageCacheEntry, false, getChangesTree(atomicOperation,
              prevPageCacheEntry));
          assert calculateFreePageIndex(prevPage) == prevFreePageIndex;
          prevPage.setNextPage(nextPageIndex);
        } finally {
          prevPageCacheEntry.releaseExclusiveLock();
          releasePage(atomicOperation, prevPageCacheEntry);
        }
      }

      if (nextPageIndex >= 0) {
        final OCacheEntry nextPageCacheEntry = loadPage(atomicOperation, fileId, nextPageIndex, false);
        nextPageCacheEntry.acquireExclusiveLock();
        try {
          final OClusterPage nextPage = new OClusterPage(nextPageCacheEntry, false, getChangesTree(atomicOperation,
              nextPageCacheEntry));
          if (calculateFreePageIndex(nextPage) != prevFreePageIndex)
            calculateFreePageIndex(nextPage);

          assert calculateFreePageIndex(nextPage) == prevFreePageIndex;
          nextPage.setPrevPage(prevPageIndex);

        } finally {
          nextPageCacheEntry.releaseExclusiveLock();
          releasePage(atomicOperation, nextPageCacheEntry);
        }
      }

      localPage.setNextPage(-1);
      localPage.setPrevPage(-1);

      if (prevFreePageIndex < 0 && newFreePageIndex < 0)
        return;

      if (prevFreePageIndex >= 0 && prevFreePageIndex < FREE_LIST_SIZE) {
        if (prevPageIndex < 0)
          updateFreePagesList(prevFreePageIndex, nextPageIndex, atomicOperation);
      }

      if (newFreePageIndex >= 0) {
        long oldFreePage;
        OCacheEntry pinnedStateEntry = loadPage(atomicOperation, fileId, pinnedStateEntryIndex, true);
        try {
          OPaginatedClusterState clusterFreeList = new OPaginatedClusterState(pinnedStateEntry, getChangesTree(atomicOperation,
              pinnedStateEntry));
          oldFreePage = clusterFreeList.getFreeListPage(newFreePageIndex);
        } finally {
          releasePage(atomicOperation, pinnedStateEntry);
        }

        if (oldFreePage >= 0) {
          final OCacheEntry oldFreePageCacheEntry = loadPage(atomicOperation, fileId, oldFreePage, false);
          oldFreePageCacheEntry.acquireExclusiveLock();
          try {
            final OClusterPage oldFreeLocalPage = new OClusterPage(oldFreePageCacheEntry, false, getChangesTree(atomicOperation,
                oldFreePageCacheEntry));
            assert calculateFreePageIndex(oldFreeLocalPage) == newFreePageIndex;

            oldFreeLocalPage.setPrevPage(pageIndex);
          } finally {
            oldFreePageCacheEntry.releaseExclusiveLock();
            releasePage(atomicOperation, oldFreePageCacheEntry);
          }

          localPage.setNextPage(oldFreePage);
          localPage.setPrevPage(-1);
        }

        updateFreePagesList(newFreePageIndex, pageIndex, atomicOperation);
      }
    } finally {
      cacheEntry.releaseExclusiveLock();
      releasePage(atomicOperation, cacheEntry);
    }
  }

  private void updateFreePagesList(int freeListIndex, long pageIndex, OAtomicOperation atomicOperation) throws IOException {
    final OCacheEntry pinnedStateEntry = loadPage(atomicOperation, fileId, pinnedStateEntryIndex, true);
    pinnedStateEntry.acquireExclusiveLock();
    try {
      OPaginatedClusterState paginatedClusterState = new OPaginatedClusterState(pinnedStateEntry, getChangesTree(atomicOperation,
          pinnedStateEntry));
      paginatedClusterState.setFreeListPage(freeListIndex, pageIndex);
    } finally {
      pinnedStateEntry.releaseExclusiveLock();
      releasePage(atomicOperation, pinnedStateEntry);
    }
  }

  private int calculateFreePageIndex(OClusterPage localPage) {
    int newFreePageIndex;
    if (localPage.isEmpty())
      newFreePageIndex = FREE_LIST_SIZE - 1;
    else {
      newFreePageIndex = (localPage.getMaxRecordSize() - (ONE_KB - 1)) / ONE_KB;

      newFreePageIndex -= LOWEST_FREELIST_BOUNDARY;
    }
    return newFreePageIndex;
  }

  private void initCusterState(OAtomicOperation atomicOperation) throws IOException {
    OCacheEntry pinnedStateEntry = addPage(atomicOperation, fileId);
    pinnedStateEntry.acquireExclusiveLock();
    try {
      OPaginatedClusterState paginatedClusterState = new OPaginatedClusterState(pinnedStateEntry, getChangesTree(atomicOperation,
          pinnedStateEntry));

      pinPage(atomicOperation, pinnedStateEntry);
      paginatedClusterState.setSize(0);
      paginatedClusterState.setRecordsSize(0);

      for (int i = 0; i < FREE_LIST_SIZE; i++)
        paginatedClusterState.setFreeListPage(i, -1);

      pinnedStateEntryIndex = pinnedStateEntry.getPageIndex();
    } finally {
      pinnedStateEntry.releaseExclusiveLock();
      releasePage(atomicOperation, pinnedStateEntry);
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

  public OPaginatedClusterDebug readDebug(long clusterPosition) throws IOException {

    OPaginatedClusterDebug debug = new OPaginatedClusterDebug();
    debug.clusterPosition = clusterPosition;
    debug.fileId = fileId;
    OAtomicOperation atomicOperation = null;
    OClusterPositionMapBucket.PositionEntry positionEntry = clusterPositionMap.get(clusterPosition);
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

    debug.pages = new ArrayList<OClusterPageDebug>();
    int contentSize = 0;

    long nextPagePointer = -1;
    boolean firstEntry = true;
    do {
      OClusterPageDebug debugPage = new OClusterPageDebug();
      debugPage.pageIndex = pageIndex;
      OCacheEntry cacheEntry = loadPage(atomicOperation, fileId, pageIndex, false);
      try {
        final OClusterPage localPage = new OClusterPage(cacheEntry, false, getChangesTree(atomicOperation, cacheEntry));

        if (localPage.isDeleted(recordPosition)) {
          if (debug.pages.isEmpty()) {
            debug.empty = true;
            return debug;
          } else
            throw new OStorageException("Content of record " + new ORecordId(id, clusterPosition) + " was broken.");
        }
        debugPage.inPagePosition = recordPosition;
        debugPage.inPageSize = localPage.getRecordSize(recordPosition);
        byte[] content = localPage.getRecordBinaryValue(recordPosition, 0, debugPage.inPageSize);
        debugPage.content = content;
        if (firstEntry && content[content.length - OLongSerializer.LONG_SIZE - OByteSerializer.BYTE_SIZE] == 0) {
          debug.empty = true;
          return debug;
        }

        debug.pages.add(debugPage);
        nextPagePointer = OLongSerializer.INSTANCE.deserializeNative(content, content.length - OLongSerializer.LONG_SIZE);
        contentSize += content.length - OLongSerializer.LONG_SIZE - OByteSerializer.BYTE_SIZE;

        firstEntry = false;
      } finally {
        releasePage(atomicOperation, cacheEntry);
      }

      pageIndex = getPageIndex(nextPagePointer);
      recordPosition = getRecordPosition(nextPagePointer);
    } while (nextPagePointer >= 0);
    debug.contentSize = contentSize;
    return debug;
  }

  @Override
  public String toString() {
    return "plocal cluster: " + getName();
  }
}
