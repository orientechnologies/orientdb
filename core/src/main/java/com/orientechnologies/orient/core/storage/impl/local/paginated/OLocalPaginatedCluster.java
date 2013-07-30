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

import static com.orientechnologies.orient.core.config.OGlobalConfiguration.DISK_CACHE_PAGE_SIZE;
import static com.orientechnologies.orient.core.config.OGlobalConfiguration.PAGINATED_STORAGE_LOWEST_FREELIST_BOUNDARY;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32;

import com.orientechnologies.common.concur.lock.OModificationLock;
import com.orientechnologies.common.concur.resource.OSharedResourceAdaptive;
import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.config.OStorageClusterConfiguration;
import com.orientechnologies.orient.core.config.OStorageFileConfiguration;
import com.orientechnologies.orient.core.config.OStoragePaginatedClusterConfiguration;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.id.OClusterPosition;
import com.orientechnologies.orient.core.id.OClusterPositionFactory;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.hashindex.local.cache.ODiskCache;
import com.orientechnologies.orient.core.serialization.compression.OCompression;
import com.orientechnologies.orient.core.serialization.compression.OCompressionFactory;
import com.orientechnologies.orient.core.storage.*;
import com.orientechnologies.orient.core.storage.fs.OFile;
import com.orientechnologies.orient.core.storage.fs.OFileFactory;
import com.orientechnologies.orient.core.storage.impl.local.OSingleFileSegment;
import com.orientechnologies.orient.core.storage.impl.local.OStorageVariableParser;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.*;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.updatePageRecord.OFullPageDiff;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.updatePageRecord.OPageDiff;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.updatePageRecord.OUpdatePageRecord;
import com.orientechnologies.orient.core.version.ORecordVersion;

/**
 * @author Andrey Lomakin
 * @since 22.03.13
 */
public class OLocalPaginatedCluster extends OSharedResourceAdaptive implements OCluster {
  public static final String                    DEF_EXTENSION                = ".pcl";
  private static final String                   CLUSTER_STATE_FILE_EXTENSION = ".pls";

  private final static int                      FREE_LIST_SIZE               = DISK_CACHE_PAGE_SIZE.getValueAsInteger()
                                                                                 - PAGINATED_STORAGE_LOWEST_FREELIST_BOUNDARY
                                                                                     .getValueAsInteger();

  private final static int                      STATE_SIZE                   = OIntegerSerializer.INT_SIZE + 2
                                                                                 * OLongSerializer.LONG_SIZE + FREE_LIST_SIZE
                                                                                 * OLongSerializer.LONG_SIZE + 3
                                                                                 * OIntegerSerializer.INT_SIZE
                                                                                 + OLongSerializer.LONG_SIZE;

  private OCompression                          compression;

  public static final String                    TYPE                         = "PHYSICAL";

  private static final int                      PAGE_INDEX_OFFSET            = 16;
  private static final int                      RECORD_POSITION_MASK         = 0xFFFF;
  private static final int                      ONE_KB                       = 1024;

  private ODiskCache                            diskCache;

  private String                                name;
  private OLocalPaginatedStorage                storageLocal;
  private volatile int                          id;
  private long                                  fileId;

  private long                                  size;
  private long                                  recordsSize;

  private OStoragePaginatedClusterConfiguration config;

  private OSingleFileSegment                    clusterStateHolder;

  private long[]                                freePageLists                = new long[FREE_LIST_SIZE];

  private final OModificationLock               externalModificationLock     = new OModificationLock();

  private OWriteAheadLog                        writeAheadLog;

  private ThreadLocal<OOperationUnitId>         currentUnitId                = new ThreadLocal<OOperationUnitId>();
  private ThreadLocal<OLogSequenceNumber>       startLSN                     = new ThreadLocal<OLogSequenceNumber>();

  private boolean                               useFirstStateHolder          = true;

  public OLocalPaginatedCluster() {
    super(OGlobalConfiguration.ENVIRONMENT_CONCURRENT.getValueAsBoolean());
    for (int i = 0; i < freePageLists.length; i++)
      freePageLists[i] = -1;
  }

  @Override
  public void configure(OStorage storage, int id, String clusterName, String location, int dataSegmentId, Object... parameters)
      throws IOException {
    externalModificationLock.requestModificationLock();
    try {
      acquireExclusiveLock();
      try {
        config = new OStoragePaginatedClusterConfiguration(storage.getConfiguration(), id, clusterName, null, true,
            OStoragePaginatedClusterConfiguration.DEFAULT_GROW_FACTOR, OStoragePaginatedClusterConfiguration.DEFAULT_GROW_FACTOR,
            OGlobalConfiguration.STORAGE_COMPRESSION_METHOD.getValueAsString());

        config.name = clusterName;

        init((OLocalPaginatedStorage) storage, config);
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
        init((OLocalPaginatedStorage) storage, config);
      } finally {
        releaseExclusiveLock();
      }
    } finally {
      externalModificationLock.releaseModificationLock();
    }
  }

  private void init(OLocalPaginatedStorage storage, OStorageClusterConfiguration config) throws IOException {
    OFileUtils.checkValidName(config.getName());

    this.config = (OStoragePaginatedClusterConfiguration) config;
    this.compression = OCompressionFactory.INSTANCE.getCompression(this.config.compression);

    storageLocal = storage;
    writeAheadLog = storage.getWALInstance();

    diskCache = storageLocal.getDiskCache();
    name = config.getName();
    this.id = config.getId();

    OStorageFileConfiguration clusterStateConfiguration = new OStorageFileConfiguration(null,
        OStorageVariableParser.DB_PATH_VARIABLE + "/" + config.getName() + CLUSTER_STATE_FILE_EXTENSION, OFileFactory.CLASSIC,
        "1024", "50%");
    clusterStateHolder = new OSingleFileSegment(storage, clusterStateConfiguration);
  }

  public boolean exists() {
    return clusterStateHolder.exists();
  }

  @Override
  public void create(int startSize) throws IOException {
    externalModificationLock.requestModificationLock();
    try {
      acquireExclusiveLock();
      try {
        fileId = diskCache.openFile(name + DEF_EXTENSION);

        final int statesSize = 2 * STATE_SIZE;

        clusterStateHolder.create(statesSize);
        OFile file = clusterStateHolder.getFile();
        file.allocateSpace(statesSize);
        file.write(0, new byte[statesSize]);

        if (config.root.clusters.size() <= config.id)
          config.root.clusters.add(config);
        else
          config.root.clusters.set(config.id, config);

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
        clusterStateHolder.open();

        loadClusterState();
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

        diskCache.closeFile(fileId, flush);
        clusterStateHolder.close();

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
        clusterStateHolder.delete();
      } finally {
        releaseExclusiveLock();
      }
    } finally {
      externalModificationLock.releaseModificationLock();
    }
  }

  @Override
  public void set(ATTRIBUTES attribute, Object value) throws IOException {
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
          setCompressionInternal(stringValue);
          break;
        }

      } finally {
        releaseExclusiveLock();
      }
    } finally {
      externalModificationLock.releaseModificationLock();
    }
  }

  private void setCompressionInternal(String stringValue) {
    try {
      compression = OCompressionFactory.INSTANCE.getCompression(stringValue);
      config.compression = stringValue;
      storageLocal.getConfiguration().update();
    } catch (IllegalArgumentException e) {
      throw new OStorageException("Invalid value for " + ATTRIBUTES.COMPRESSION + " attribute. ", e);
    }

  }

  private void setRecordOverflowGrowFactorInternal(String stringValue) {
    try {
      float growFactor = Float.parseFloat(stringValue);
      if (growFactor < 1)
        throw new OStorageException(ATTRIBUTES.RECORD_OVERFLOW_GROW_FACTOR + " can not be less than 1");

      config.recordOverflowGrowFactor = growFactor;
      storageLocal.getConfiguration().update();
    } catch (NumberFormatException nfe) {
      throw new OStorageException("Invalid value for cluster attribute " + ATTRIBUTES.RECORD_OVERFLOW_GROW_FACTOR + " was passed ["
          + stringValue + "].", nfe);
    }
  }

  private void setRecordGrowFactorInternal(String stringValue) {
    try {
      float growFactor = Float.parseFloat(stringValue);
      if (growFactor < 1)
        throw new OStorageException(ATTRIBUTES.RECORD_GROW_FACTOR + " can not be less than 1");

      config.recordGrowFactor = growFactor;
      storageLocal.getConfiguration().update();
    } catch (NumberFormatException nfe) {
      throw new OStorageException("Invalid value for cluster attribute " + ATTRIBUTES.RECORD_GROW_FACTOR + " was passed ["
          + stringValue + "].", nfe);
    }
  }

  private void setUseWalInternal(String stringValue) {
    if (!(stringValue.equals("true") || stringValue.equals("false")))
      throw new OStorageException("Invalid value for cluster attribute " + ATTRIBUTES.USE_WAL + " was passed [" + stringValue
          + "].");

    config.useWal = Boolean.valueOf(stringValue);
    storageLocal.getConfiguration().update();
  }

  private void setNameInternal(String newName) throws IOException {
    diskCache.renameFile(fileId, this.name, newName);
    clusterStateHolder.rename(name, newName);

    config.name = newName;
    storageLocal.renameCluster(name, newName);
    name = newName;
    storageLocal.getConfiguration().update();
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
  public void convertToTombstone(OClusterPosition iPosition) throws IOException {
    throw new UnsupportedOperationException("convertToTombstone");
  }

  public OPhysicalPosition createRecord(byte[] content, final ORecordVersion recordVersion, final byte recordType,
      OStorageTransaction transaction) throws IOException {
    externalModificationLock.requestModificationLock();
    try {
      acquireExclusiveLock();
      try {
        final long prevSize = size;
        final long prevRecordsSize = recordsSize;

        content = compression.compress(content);

        int grownContentSize = (int) (config.recordGrowFactor * content.length);
        int entryContentLength = grownContentSize + 2 * OByteSerializer.BYTE_SIZE + OIntegerSerializer.INT_SIZE
            + OLongSerializer.LONG_SIZE;

        if (entryContentLength < OLocalPage.MAX_RECORD_SIZE) {
          startRecordOperation(transaction, false);

          byte[] entryContent = new byte[entryContentLength];

          int entryPosition = 0;
          entryContent[entryPosition] = recordType;
          entryPosition++;

          OIntegerSerializer.INSTANCE.serializeNative(content.length, entryContent, entryPosition);
          entryPosition += OIntegerSerializer.INT_SIZE;

          System.arraycopy(content, 0, entryContent, entryPosition, content.length);
          entryPosition += grownContentSize;

          entryContent[entryPosition] = 1;
          entryPosition++;

          OLongSerializer.INSTANCE.serializeNative(-1L, entryContent, entryPosition);
          OLocalPage.TrackMode trackMode;
          if (!config.useWal || writeAheadLog == null)
            trackMode = OLocalPage.TrackMode.NONE;
          else if (transaction != null)
            trackMode = OLocalPage.TrackMode.BOTH;
          else
            trackMode = OLocalPage.TrackMode.FORWARD;

          final AddEntryResult addEntryResult = addEntry(recordVersion, entryContent, trackMode);

          size++;
          recordsSize += addEntryResult.recordsSizeDiff;

          logClusterState(prevSize, prevRecordsSize);

          endRecordOperation(transaction);

          return createPhysicalPosition(recordType, addEntryResult.pagePointer, addEntryResult.recordVersion);
        } else {
          startRecordOperation(transaction, true);

          OLocalPage.TrackMode trackMode = (!config.useWal || writeAheadLog == null) ? OLocalPage.TrackMode.NONE
              : OLocalPage.TrackMode.BOTH;
          int entrySize = grownContentSize + OIntegerSerializer.INT_SIZE + OByteSerializer.BYTE_SIZE;

          int fullEntryPosition = 0;
          byte[] fullEntry = new byte[entrySize];

          fullEntry[fullEntryPosition] = recordType;
          fullEntryPosition++;

          OIntegerSerializer.INSTANCE.serializeNative(content.length, fullEntry, fullEntryPosition);
          fullEntryPosition += OIntegerSerializer.INT_SIZE;

          System.arraycopy(content, 0, fullEntry, fullEntryPosition, content.length);

          long prevPageRecordPointer = -1;
          long firstPagePointer = -1;
          ORecordVersion version = null;

          int from = 0;
          int to = from + (OLocalPage.MAX_RECORD_SIZE - OByteSerializer.BYTE_SIZE - OLongSerializer.LONG_SIZE);

          int recordsSizeDiff = 0;

          do {
            byte[] entryContent = new byte[to - from + OByteSerializer.BYTE_SIZE + OLongSerializer.LONG_SIZE];
            System.arraycopy(fullEntry, from, entryContent, 0, to - from);

            if (from > 0)
              entryContent[entryContent.length - OLongSerializer.LONG_SIZE - OByteSerializer.BYTE_SIZE] = 0;
            else
              entryContent[entryContent.length - OLongSerializer.LONG_SIZE - OByteSerializer.BYTE_SIZE] = 1;

            OLongSerializer.INSTANCE.serializeNative(-1L, entryContent, entryContent.length - OLongSerializer.LONG_SIZE);

            final AddEntryResult addEntryResult = addEntry(recordVersion, entryContent, trackMode);
            recordsSizeDiff += addEntryResult.recordsSizeDiff;

            if (firstPagePointer == -1) {
              firstPagePointer = addEntryResult.pagePointer;
              version = addEntryResult.recordVersion;
            }

            long addedPagePointer = addEntryResult.pagePointer;
            if (prevPageRecordPointer >= 0) {

              long prevPageIndex = prevPageRecordPointer >>> PAGE_INDEX_OFFSET;
              int prevPageRecordPosition = (int) (prevPageRecordPointer & RECORD_POSITION_MASK);

              long prevPageMemoryPointer = diskCache.load(fileId, prevPageIndex);
              try {
                final OLocalPage prevPage = new OLocalPage(prevPageMemoryPointer, false, OLocalPage.TrackMode.BOTH);

                int prevRecordPageOffset = prevPage.getRecordPageOffset(prevPageRecordPosition);
                int prevPageRecordSize = prevPage.getRecordSize(prevPageRecordPosition);

                prevPage.setLongValue(prevRecordPageOffset + prevPageRecordSize - OLongSerializer.LONG_SIZE, addedPagePointer);

                logPageChanges(prevPage, prevPageIndex, false);

                diskCache.markDirty(fileId, prevPageIndex);
              } finally {
                diskCache.release(fileId, prevPageIndex);
              }
            }

            prevPageRecordPointer = addedPagePointer;
            from = to;
            to = to + (OLocalPage.MAX_RECORD_SIZE - OLongSerializer.LONG_SIZE - OByteSerializer.BYTE_SIZE);
            if (to > fullEntry.length)
              to = fullEntry.length;

          } while (from < to);

          size++;
          recordsSize += recordsSizeDiff;

          logClusterState(prevSize, prevRecordsSize);

          endRecordOperation(transaction);

          return createPhysicalPosition(recordType, firstPagePointer, version);
        }
      } finally {
        releaseExclusiveLock();
      }
    } finally {
      externalModificationLock.releaseModificationLock();
    }
  }

  private void endRecordOperation(OStorageTransaction transaction) throws IOException {
    if (transaction == null && config.useWal && writeAheadLog != null) {
      writeAheadLog.log(new OAtomicUnitEndRecord(currentUnitId.get(), false));
    }

    currentUnitId.set(null);
    startLSN.set(null);
  }

  private void startRecordOperation(OStorageTransaction transaction, boolean rollbackMode) throws IOException {
    if (transaction == null) {
      if (config.useWal && writeAheadLog != null) {
        OOperationUnitId unitId = OOperationUnitId.generateId();

        OLogSequenceNumber lsn = writeAheadLog.log(new OAtomicUnitStartRecord(rollbackMode, unitId));
        startLSN.set(lsn);
        currentUnitId.set(unitId);
      }
    } else {
      startLSN.set(transaction.getStartLSN());
      currentUnitId.set(transaction.getOperationUnitId());
    }
  }

  private OPhysicalPosition createPhysicalPosition(byte recordType, long firstPagePointer, ORecordVersion version) {
    final OPhysicalPosition physicalPosition = new OPhysicalPosition();
    physicalPosition.recordType = recordType;
    physicalPosition.recordSize = -1;
    physicalPosition.dataSegmentId = -1;
    physicalPosition.dataSegmentPos = -1;
    physicalPosition.clusterPosition = OClusterPositionFactory.INSTANCE.valueOf(firstPagePointer);
    physicalPosition.recordVersion = version;
    return physicalPosition;
  }

  public ORawBuffer readRecord(OClusterPosition clusterPosition) throws IOException {
    acquireSharedLock();
    try {
      long pagePointer = clusterPosition.longValue();
      int recordPosition = (int) (pagePointer & RECORD_POSITION_MASK);

      long pageIndex = pagePointer >>> PAGE_INDEX_OFFSET;

      if (diskCache.getFilledUpTo(fileId) <= pageIndex)
        return null;

      ORecordVersion recordVersion = null;
      long pointer = diskCache.load(fileId, pageIndex);
      try {
        final OLocalPage localPage = new OLocalPage(pointer, false, OLocalPage.TrackMode.NONE);

        int recordPageOffset = localPage.getRecordPageOffset(recordPosition);

        if (recordPageOffset < 0)
          return null;

        recordVersion = localPage.getRecordVersion(recordPosition);
      } finally {
        diskCache.release(fileId, pageIndex);
      }

      byte[] fullContent = readFullEntry(clusterPosition);
      if (fullContent == null)
        return null;

      int fullContentPosition = 0;

      byte recordType = fullContent[fullContentPosition];
      fullContentPosition++;

      int readContentSize = OIntegerSerializer.INSTANCE.deserializeNative(fullContent, fullContentPosition);
      fullContentPosition += OIntegerSerializer.INT_SIZE;

      byte[] recordContent = new byte[readContentSize];
      System.arraycopy(fullContent, fullContentPosition, recordContent, 0, recordContent.length);

      recordContent = compression.uncompress(recordContent);
      return new ORawBuffer(recordContent, recordVersion, recordType);
    } finally {
      releaseSharedLock();
    }
  }

  private byte[] readFullEntry(OClusterPosition clusterPosition) throws IOException {
    long pagePointer = clusterPosition.longValue();
    int recordPosition = (int) (pagePointer & RECORD_POSITION_MASK);

    long pageIndex = pagePointer >>> PAGE_INDEX_OFFSET;

    if (diskCache.getFilledUpTo(fileId) <= pageIndex)
      return null;

    final List<byte[]> recordChunks = new ArrayList<byte[]>();
    int contentSize = 0;

    long nextPagePointer = -1;
    boolean firstEntry = true;
    do {
      long pointer = diskCache.load(fileId, pageIndex);
      try {
        final OLocalPage localPage = new OLocalPage(pointer, false, OLocalPage.TrackMode.NONE);

        int recordPageOffset = localPage.getRecordPageOffset(recordPosition);

        if (recordPageOffset < 0) {
          if (recordChunks.isEmpty())
            return null;
          else
            throw new OStorageException("Content of record " + new ORecordId(id, clusterPosition) + " was broken.");
        }

        byte[] content = localPage.getBinaryValue(recordPageOffset, localPage.getRecordSize(recordPosition));

        if (firstEntry && content[content.length - OLongSerializer.LONG_SIZE - OByteSerializer.BYTE_SIZE] == 0)
          return null;

        recordChunks.add(content);
        nextPagePointer = OLongSerializer.INSTANCE.deserializeNative(content, content.length - OLongSerializer.LONG_SIZE);
        contentSize += content.length - OLongSerializer.LONG_SIZE - OByteSerializer.BYTE_SIZE;

        firstEntry = false;
      } finally {
        diskCache.release(fileId, pageIndex);
      }

      pageIndex = nextPagePointer >>> PAGE_INDEX_OFFSET;
      recordPosition = (int) (nextPagePointer & RECORD_POSITION_MASK);
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

  public boolean deleteRecord(OClusterPosition clusterPosition, OStorageTransaction transaction) throws IOException {
    externalModificationLock.requestModificationLock();
    try {
      acquireExclusiveLock();
      try {
        long pagePointer = clusterPosition.longValue();
        int recordPosition = (int) (pagePointer & RECORD_POSITION_MASK);

        long pageIndex = pagePointer >>> PAGE_INDEX_OFFSET;

        if (diskCache.getFilledUpTo(fileId) <= pageIndex)
          return false;

        final long prevSize = size;
        final long prevRecordsSize = recordsSize;

        boolean isRecordSpreadAcrossSeveralPages = isRecordSpreadAcrossSeveralPages(pageIndex, recordPosition);

        final OLocalPage.TrackMode trackMode;
        if (!config.useWal || writeAheadLog == null)
          trackMode = OLocalPage.TrackMode.NONE;
        else if (transaction != null || isRecordSpreadAcrossSeveralPages)
          trackMode = OLocalPage.TrackMode.BOTH;
        else
          trackMode = OLocalPage.TrackMode.FORWARD;

        long nextPagePointer = -1;
        int removedContentSize = 0;
        do {
          long pointer = diskCache.load(fileId, pageIndex);
          int initialFreePageIndex;
          try {
            final OLocalPage localPage = new OLocalPage(pointer, false, trackMode);
            initialFreePageIndex = calculateFreePageIndex(localPage);

            int recordPageOffset = localPage.getRecordPageOffset(recordPosition);
            if (recordPageOffset < 0) {
              if (removedContentSize == 0)
                return false;
              else
                throw new OStorageException("Content of record " + new ORecordId(id, clusterPosition) + " was broken.");
            } else if (removedContentSize == 0) {
              startRecordOperation(transaction, isRecordSpreadAcrossSeveralPages);
            }

            byte[] content = localPage.getBinaryValue(recordPageOffset, localPage.getRecordSize(recordPosition));

            int initialFreeSpace = localPage.getFreeSpace();
            localPage.deleteRecord(recordPosition);

            removedContentSize += localPage.getFreeSpace() - initialFreeSpace;
            nextPagePointer = OLongSerializer.INSTANCE.deserializeNative(content, content.length - OLongSerializer.LONG_SIZE);

            logPageChanges(localPage, pageIndex, false);

            diskCache.markDirty(fileId, pageIndex);
          } finally {
            diskCache.release(fileId, pageIndex);
          }

          updateFreePagesIndex(initialFreePageIndex, pageIndex, trackMode);

          pageIndex = nextPagePointer >>> PAGE_INDEX_OFFSET;
          recordPosition = (int) (nextPagePointer & RECORD_POSITION_MASK);
        } while (nextPagePointer >= 0);

        size--;
        recordsSize -= removedContentSize;

        logClusterState(prevSize, prevRecordsSize);

        endRecordOperation(transaction);

        return true;
      } finally {
        releaseExclusiveLock();
      }
    } finally {
      externalModificationLock.releaseModificationLock();
    }
  }

  public void updateRecord(OClusterPosition clusterPosition, byte[] content, final ORecordVersion recordVersion,
      final byte recordType, OStorageTransaction transaction) throws IOException {
    externalModificationLock.requestModificationLock();
    try {
      acquireExclusiveLock();
      try {
        final long prevSize = size;
        final long prevRecordsSize = recordsSize;

        byte[] fullEntryContent = readFullEntry(clusterPosition);
        if (fullEntryContent == null)
          return;

        content = compression.compress(content);

        int updatedContentLength = content.length + 2 * OByteSerializer.BYTE_SIZE + OIntegerSerializer.INT_SIZE
            + OLongSerializer.LONG_SIZE;

        long pagePointer = clusterPosition.longValue();
        int recordPosition = (int) (pagePointer & RECORD_POSITION_MASK);
        long pageIndex = pagePointer >>> PAGE_INDEX_OFFSET;

        boolean isRecordSpreadAcrossSeveralPages = isRecordSpreadAcrossSeveralPages(pageIndex, recordPosition);

        byte[] recordEntry;
        if (updatedContentLength <= fullEntryContent.length)
          recordEntry = new byte[fullEntryContent.length - OLongSerializer.LONG_SIZE - OByteSerializer.BYTE_SIZE];
        else {
          int grownContent = (int) (content.length * config.recordOverflowGrowFactor);
          recordEntry = new byte[grownContent + OByteSerializer.BYTE_SIZE + OIntegerSerializer.INT_SIZE];

          isRecordSpreadAcrossSeveralPages = true;
        }

        final OLocalPage.TrackMode trackMode;
        if (!config.useWal || writeAheadLog == null)
          trackMode = OLocalPage.TrackMode.NONE;
        else if (transaction != null || isRecordSpreadAcrossSeveralPages)
          trackMode = OLocalPage.TrackMode.BOTH;
        else
          trackMode = OLocalPage.TrackMode.FORWARD;

        startRecordOperation(transaction, isRecordSpreadAcrossSeveralPages);

        int entryPosition = 0;
        recordEntry[entryPosition] = recordType;
        entryPosition++;

        OIntegerSerializer.INSTANCE.serializeNative(content.length, recordEntry, entryPosition);
        entryPosition += OIntegerSerializer.INT_SIZE;

        System.arraycopy(content, 0, recordEntry, entryPosition, content.length);

        int recordsSizeDiff = 0;
        long prevPageRecordPointer = -1;

        int currentPos = 0;
        while (pagePointer >= 0 && currentPos < recordEntry.length) {
          recordPosition = (int) (pagePointer & RECORD_POSITION_MASK);
          pageIndex = pagePointer >>> PAGE_INDEX_OFFSET;

          int freePageIndex;
          long dataPointer = diskCache.load(fileId, pageIndex);
          try {
            final OLocalPage localPage = new OLocalPage(dataPointer, false, trackMode);
            int freeSpace = localPage.getFreeSpace();
            freePageIndex = calculateFreePageIndex(localPage);

            int recordPageOffset = localPage.getRecordPageOffset(recordPosition);
            int chunkSize = localPage.getRecordSize(recordPosition);

            long nextPagePointer = localPage.getLongValue(recordPageOffset + +chunkSize - OLongSerializer.LONG_SIZE);

            int newChunkLen = Math.min(recordEntry.length - currentPos + OLongSerializer.LONG_SIZE + OByteSerializer.BYTE_SIZE,
                chunkSize);
            int dataLen = newChunkLen - OLongSerializer.LONG_SIZE - OByteSerializer.BYTE_SIZE;

            byte[] newRecordChunk = new byte[newChunkLen];
            System.arraycopy(recordEntry, currentPos, newRecordChunk, 0, dataLen);

            if (currentPos > 0)
              newRecordChunk[newRecordChunk.length - OLongSerializer.LONG_SIZE - OByteSerializer.BYTE_SIZE] = 0;
            else
              newRecordChunk[newRecordChunk.length - OLongSerializer.LONG_SIZE - OByteSerializer.BYTE_SIZE] = 1;

            OLongSerializer.INSTANCE.serializeNative(-1L, newRecordChunk, newRecordChunk.length - OLongSerializer.LONG_SIZE);

            if (prevPageRecordPointer >= 0) {
              long prevPageIndex = prevPageRecordPointer >>> PAGE_INDEX_OFFSET;
              int prevPageRecordPosition = (int) (prevPageRecordPointer & RECORD_POSITION_MASK);

              long prevPageMemoryPointer = diskCache.load(fileId, prevPageIndex);
              try {
                final OLocalPage prevPage = new OLocalPage(prevPageMemoryPointer, false, trackMode);

                int prevRecordPageOffset = prevPage.getRecordPageOffset(prevPageRecordPosition);
                int prevPageRecordSize = prevPage.getRecordSize(prevPageRecordPosition);

                prevPage.setLongValue(prevRecordPageOffset + prevPageRecordSize - OLongSerializer.LONG_SIZE, pagePointer);

                logPageChanges(prevPage, prevPageIndex, false);

                diskCache.markDirty(fileId, prevPageIndex);
              } finally {
                diskCache.release(fileId, prevPageIndex);
              }
            }

            localPage.replaceRecord(recordPosition, newRecordChunk, recordVersion.getCounter() != -2 ? recordVersion : null);

            currentPos += dataLen;

            recordsSizeDiff += freeSpace - localPage.getFreeSpace();
            prevPageRecordPointer = pagePointer;
            pagePointer = nextPagePointer;

            logPageChanges(localPage, pageIndex, false);

            diskCache.markDirty(fileId, pageIndex);
          } finally {
            diskCache.release(fileId, pageIndex);
          }

          updateFreePagesIndex(freePageIndex, pageIndex, trackMode);
        }

        int from = currentPos;
        int to = from + (OLocalPage.MAX_RECORD_SIZE - OByteSerializer.BYTE_SIZE - OLongSerializer.LONG_SIZE);
        if (to > recordEntry.length)
          to = recordEntry.length;

        while (from < to) {
          byte[] entryContent = new byte[to - from + OByteSerializer.BYTE_SIZE + OLongSerializer.LONG_SIZE];
          System.arraycopy(recordEntry, from, entryContent, 0, to - from);

          if (from > 0)
            entryContent[entryContent.length - OLongSerializer.LONG_SIZE - OByteSerializer.BYTE_SIZE] = 0;
          else
            entryContent[entryContent.length - OLongSerializer.LONG_SIZE - OByteSerializer.BYTE_SIZE] = 1;

          OLongSerializer.INSTANCE.serializeNative(-1L, entryContent, entryContent.length - OLongSerializer.LONG_SIZE);

          final AddEntryResult addEntryResult = addEntry(recordVersion, entryContent, trackMode);
          recordsSizeDiff += addEntryResult.recordsSizeDiff;

          long addedPagePointer = addEntryResult.pagePointer;
          if (prevPageRecordPointer >= 0) {

            long prevPageIndex = prevPageRecordPointer >>> PAGE_INDEX_OFFSET;
            int prevPageRecordPosition = (int) (prevPageRecordPointer & RECORD_POSITION_MASK);

            long prevPageMemoryPointer = diskCache.load(fileId, prevPageIndex);
            try {
              final OLocalPage prevPage = new OLocalPage(prevPageMemoryPointer, false, trackMode);

              int recordPageOffset = prevPage.getRecordPageOffset(prevPageRecordPosition);
              int prevPageRecordSize = prevPage.getRecordSize(prevPageRecordPosition);

              prevPage.setLongValue(recordPageOffset + prevPageRecordSize - OLongSerializer.LONG_SIZE, addedPagePointer);

              logPageChanges(prevPage, prevPageIndex, false);

              diskCache.markDirty(fileId, prevPageIndex);
            } finally {
              diskCache.release(fileId, prevPageIndex);
            }
          }

          prevPageRecordPointer = addedPagePointer;
          from = to;
          to = to + (OLocalPage.MAX_RECORD_SIZE - OLongSerializer.LONG_SIZE - OByteSerializer.BYTE_SIZE);
          if (to > recordEntry.length)
            to = recordEntry.length;
        }

        recordsSize += recordsSizeDiff;

        logClusterState(prevSize, prevRecordsSize);

        endRecordOperation(transaction);

      } finally {
        releaseExclusiveLock();
      }
    } finally {
      externalModificationLock.releaseModificationLock();
    }
  }

  private boolean isRecordSpreadAcrossSeveralPages(long pageIndex, int recordPosition) throws IOException {
    long pagePointer = diskCache.load(fileId, pageIndex);
    try {
      OLocalPage localPage = new OLocalPage(pagePointer, false, OLocalPage.TrackMode.NONE);
      int recordPageOffset = localPage.getRecordPageOffset(recordPosition);

      if (recordPageOffset < 0)
        return false;

      int recordSize = localPage.getRecordSize(recordPosition);
      long nextPagePointer = localPage.getLongValue(recordPageOffset + recordSize - OLongSerializer.LONG_SIZE);
      return nextPagePointer >= 0;
    } finally {
      diskCache.release(fileId, pageIndex);
    }
  }

  private void restorePage(OAbstractPageWALRecord walRecord) throws IOException {
    acquireExclusiveLock();
    try {
      if (walRecord instanceof OAddNewPageRecord) {
        // skip it
        return;
      }

      if (!(walRecord instanceof OUpdatePageRecord)) {
        assert false;
        OLogManager.instance().error(this, "Unknown WAL record type -  %s", walRecord.getClass().getName());
      }

      restorePageData((OUpdatePageRecord) walRecord);
    } finally {
      releaseExclusiveLock();
    }
  }

  private void revertPage(OAbstractPageWALRecord walRecord) throws IOException {
    acquireExclusiveLock();
    try {
      if (walRecord instanceof OAddNewPageRecord) {
        // skip it
        return;
      }

      if (!(walRecord instanceof OUpdatePageRecord)) {
        assert false;
        OLogManager.instance().error(this, "Unknown WAL record type -  %s", walRecord.getClass().getName());
      }

      revertPageData((OUpdatePageRecord) walRecord);
    } finally {
      releaseExclusiveLock();
    }
  }

  private void revertPageData(OUpdatePageRecord updatePageRecord) throws IOException {
    OLogSequenceNumber prevLSN = updatePageRecord.getPrevLsn();

    if (prevLSN == null) {
      OLogManager.instance().error(this, "Current record %s does not have previous LSN reference, rollback is impossible",
          updatePageRecord);
      assert false;
    }

    long pageIndex = updatePageRecord.getPageIndex();
    long pagePointer = diskCache.load(fileId, pageIndex);
    try {
      final OLocalPage page = new OLocalPage(pagePointer, false, OLocalPage.TrackMode.NONE);
      List<OPageDiff<?>> pageDiffs = updatePageRecord.getChanges();

      List<OFullPageDiff<?>> fullPageDiffs = new ArrayList<OFullPageDiff<?>>(pageDiffs.size());
      for (OPageDiff<?> pageDiff : pageDiffs) {
        if (pageDiff instanceof OFullPageDiff<?>) {
          fullPageDiffs.add((OFullPageDiff<?>) pageDiff);
        } else {
          assert false;
          OLogManager.instance()
              .error(this, "Record operation %s can not be reverted, rollback will be aborted.", updatePageRecord);
          return;
        }
      }

      page.revertChanges(fullPageDiffs);
      page.setLsn(prevLSN);

      diskCache.markDirty(fileId, pageIndex);
    } finally {
      diskCache.release(fileId, pageIndex);
    }
  }

  private void restorePageData(OUpdatePageRecord updatePageRecord) throws IOException {
    long pageIndex = updatePageRecord.getPageIndex();
    long pagePointer = diskCache.load(fileId, pageIndex);
    try {
      final OLocalPage page = new OLocalPage(pagePointer, false, OLocalPage.TrackMode.NONE);
      page.restoreChanges(updatePageRecord.getChanges());

      page.setLsn(updatePageRecord.getLsn());

      diskCache.markDirty(fileId, pageIndex);
    } finally {
      diskCache.release(fileId, pageIndex);
    }
  }

  private AddEntryResult addEntry(ORecordVersion recordVersion, byte[] entryContent, OLocalPage.TrackMode trackMode)
      throws IOException {
    final FindFreePageResult findFreePageResult = findFreePage(entryContent.length, trackMode);

    int freePageIndex = findFreePageResult.freePageIndex;
    long pageIndex = findFreePageResult.pageIndex;
    boolean newRecord = freePageIndex >= freePageLists.length;

    long pagePointer = diskCache.load(fileId, pageIndex);
    int recordSizesDiff;
    int position;
    final ORecordVersion finalVersion;
    try {
      final OLocalPage localPage = new OLocalPage(pagePointer, newRecord, trackMode);
      assert newRecord || freePageIndex == calculateFreePageIndex(localPage);

      int initialFreeSpace = localPage.getFreeSpace();

      position = localPage.appendRecord(recordVersion, entryContent, false);
      assert position >= 0;

      finalVersion = localPage.getRecordVersion(position);

      int freeSpace = localPage.getFreeSpace();
      recordSizesDiff = initialFreeSpace - freeSpace;

      logPageChanges(localPage, pageIndex, newRecord);

      diskCache.markDirty(fileId, pageIndex);
    } finally {
      diskCache.release(fileId, pageIndex);
    }

    updateFreePagesIndex(freePageIndex, pageIndex, trackMode);

    return new AddEntryResult((pageIndex << PAGE_INDEX_OFFSET) | position, finalVersion, recordSizesDiff);
  }

  private FindFreePageResult findFreePage(int contentSize, OLocalPage.TrackMode trackMode) throws IOException {
    while (true) {
      int freePageIndex = contentSize / ONE_KB;
      freePageIndex -= PAGINATED_STORAGE_LOWEST_FREELIST_BOUNDARY.getValueAsInteger();
      if (freePageIndex < 0)
        freePageIndex = 0;

      long pageIndex;
      do {
        pageIndex = freePageLists[freePageIndex];
        freePageIndex++;
      } while (pageIndex < 0 && freePageIndex < freePageLists.length);

      if (pageIndex < 0)
        pageIndex = diskCache.getFilledUpTo(fileId);
      else
        freePageIndex--;

      if (freePageIndex < freePageLists.length) {
        long pointer = diskCache.load(fileId, pageIndex);
        int realFreePageIndex;
        try {
          OLocalPage localPage = new OLocalPage(pointer, false, OLocalPage.TrackMode.NONE);
          realFreePageIndex = calculateFreePageIndex(localPage);
        } finally {
          diskCache.release(fileId, pageIndex);
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
  }

  private void updateFreePagesIndex(int prevFreePageIndex, long pageIndex, OLocalPage.TrackMode trackMode) throws IOException {
    long pointer = diskCache.load(fileId, pageIndex);
    try {
      final OLocalPage localPage = new OLocalPage(pointer, false, trackMode);

      int newFreePageIndex = calculateFreePageIndex(localPage);

      if (prevFreePageIndex == newFreePageIndex)
        return;

      long nextPageIndex = localPage.getNextPage();
      long prevPageIndex = localPage.getPrevPage();

      if (prevPageIndex >= 0) {
        long prevPagePointer = diskCache.load(fileId, prevPageIndex);
        try {
          final OLocalPage prevPage = new OLocalPage(prevPagePointer, false, trackMode);
          assert calculateFreePageIndex(prevPage) == prevFreePageIndex;
          prevPage.setNextPage(nextPageIndex);

          logPageChanges(prevPage, prevPageIndex, false);

          diskCache.markDirty(fileId, prevPageIndex);
        } finally {
          diskCache.release(fileId, prevPageIndex);
        }
      }

      if (nextPageIndex >= 0) {
        long nextPagePointer = diskCache.load(fileId, nextPageIndex);
        try {
          final OLocalPage nextPage = new OLocalPage(nextPagePointer, false, trackMode);
          if (calculateFreePageIndex(nextPage) != prevFreePageIndex)
            calculateFreePageIndex(nextPage);

          assert calculateFreePageIndex(nextPage) == prevFreePageIndex;
          nextPage.setPrevPage(prevPageIndex);

          logPageChanges(nextPage, nextPageIndex, false);

          diskCache.markDirty(fileId, nextPageIndex);
        } finally {
          diskCache.release(fileId, nextPageIndex);
        }
      }

      localPage.setNextPage(-1);
      localPage.setPrevPage(-1);

      if (prevFreePageIndex < 0 && newFreePageIndex < 0)
        return;

      if (prevFreePageIndex >= 0 && prevFreePageIndex < freePageLists.length) {
        if (prevPageIndex < 0)
          updateFreePagesList(prevFreePageIndex, nextPageIndex);
      }

      if (newFreePageIndex >= 0) {
        long oldFreePage = freePageLists[newFreePageIndex];
        if (oldFreePage >= 0) {
          long oldFreePagePointer = diskCache.load(fileId, oldFreePage);
          try {
            final OLocalPage oldFreeLocalPage = new OLocalPage(oldFreePagePointer, false, trackMode);
            assert calculateFreePageIndex(oldFreeLocalPage) == newFreePageIndex;

            oldFreeLocalPage.setPrevPage(pageIndex);

            logPageChanges(oldFreeLocalPage, oldFreePage, false);

            diskCache.markDirty(fileId, oldFreePage);
          } finally {
            diskCache.release(fileId, oldFreePage);
          }

          localPage.setNextPage(oldFreePage);
          localPage.setPrevPage(-1);
        }

        updateFreePagesList(newFreePageIndex, pageIndex);
      }

      logPageChanges(localPage, pageIndex, false);

      diskCache.markDirty(fileId, pageIndex);
    } finally {
      diskCache.release(fileId, pageIndex);
    }
  }

  private void updateFreePagesList(int freePageIndex, long pageIndex) throws IOException {
    if (!config.useWal || writeAheadLog == null)
      freePageLists[freePageIndex] = pageIndex;
    else {
      final long prevPageIndex = freePageLists[freePageIndex];
      freePageLists[freePageIndex] = pageIndex;
      writeAheadLog.log(new OFreePageChangeRecord(currentUnitId.get(), id, freePageIndex, prevPageIndex, pageIndex));
    }
  }

  private void logPageChanges(OLocalPage localPage, long pageIndex, boolean isNewPage) throws IOException {
    if (config.useWal && writeAheadLog != null) {
      List<OPageDiff<?>> pageChanges = localPage.getPageChanges();
      if (pageChanges.isEmpty())
        return;

      OOperationUnitId unitId = currentUnitId.get();
      assert unitId != null;

      OLogSequenceNumber prevLsn;
      if (isNewPage)
        prevLsn = startLSN.get();
      else
        prevLsn = localPage.getLsn();

      OLogSequenceNumber lsn = writeAheadLog.log(new OUpdatePageRecord(pageIndex, id, unitId, pageChanges, prevLsn));

      localPage.setLsn(lsn);
    }
  }

  private int calculateFreePageIndex(OLocalPage localPage) {
    int newFreePageIndex;
    if (localPage.isEmpty())
      newFreePageIndex = freePageLists.length - 1;
    else {
      newFreePageIndex = (localPage.getMaxRecordSize() - (ONE_KB - 1)) / ONE_KB;

      newFreePageIndex -= PAGINATED_STORAGE_LOWEST_FREELIST_BOUNDARY.getValueAsInteger();
    }
    return newFreePageIndex;
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
        final long prevSize = size;
        final long prevRecordsSize = recordsSize;

        if (config.useWal && writeAheadLog != null) {
          OOperationUnitId operationUnitId = OOperationUnitId.generateId();
          OLogSequenceNumber lsn = writeAheadLog.log(new OAtomicUnitStartRecord(false, operationUnitId));
          currentUnitId.set(operationUnitId);

          startLSN.set(lsn);
        }

        diskCache.truncateFile(fileId);
        clusterStateHolder.truncate();

        size = 0;
        recordsSize = 0;

        logClusterState(prevSize, prevRecordsSize);

        if (config.useWal && writeAheadLog != null) {
          writeAheadLog.log(new OAtomicUnitEndRecord(currentUnitId.get(), false));

          currentUnitId.set(null);
          startLSN.set(null);
        }

        for (int i = 0; i < freePageLists.length; i++)
          freePageLists[i] = -1;
      } finally {
        releaseExclusiveLock();
      }
    } finally {
      externalModificationLock.releaseModificationLock();
    }

    storageLocal.scheduleFullCheckpoint();
  }

  @Override
  public String getType() {
    return TYPE;
  }

  @Override
  public int getDataSegmentId() {
    return -1;
  }

  @Override
  public boolean addPhysicalPosition(OPhysicalPosition iPPosition) throws IOException {
    throw new UnsupportedOperationException("addPhysicalPosition");
  }

  @Override
  public OPhysicalPosition getPhysicalPosition(OPhysicalPosition position) throws IOException {
    acquireSharedLock();
    try {
      OClusterPosition clusterPosition = position.clusterPosition;
      long pagePointer = clusterPosition.longValue();

      long pageIndex = pagePointer >>> PAGE_INDEX_OFFSET;
      int recordPosition = (int) (pagePointer & RECORD_POSITION_MASK);

      long pagesCount = diskCache.getFilledUpTo(fileId);
      if (pageIndex >= pagesCount)
        return null;

      long pointer = diskCache.load(fileId, pageIndex);
      try {
        final OLocalPage localPage = new OLocalPage(pointer, false, OLocalPage.TrackMode.NONE);
        int recordPageOffset = localPage.getRecordPageOffset(recordPosition);

        if (recordPageOffset < 0)
          return null;

        int recordSize = localPage.getRecordSize(recordPosition);
        if (localPage.getByteValue(recordPageOffset + recordSize - OLongSerializer.LONG_SIZE - OByteSerializer.BYTE_SIZE) == 0)
          return null;

        final OPhysicalPosition physicalPosition = new OPhysicalPosition();
        physicalPosition.dataSegmentId = -1;
        physicalPosition.dataSegmentPos = -1;
        physicalPosition.recordSize = -1;

        physicalPosition.recordType = localPage.getByteValue(recordPageOffset);
        physicalPosition.recordVersion = localPage.getRecordVersion(recordPosition);
        physicalPosition.clusterPosition = position.clusterPosition;

        return physicalPosition;
      } finally {
        diskCache.release(fileId, pageIndex);
      }

    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public void updateDataSegmentPosition(OClusterPosition iPosition, int iDataSegmentId, long iDataPosition) throws IOException {
    throw new UnsupportedOperationException("updateDataSegmentPosition");
  }

  @Override
  public void removePhysicalPosition(OClusterPosition iPosition) throws IOException {
    throw new UnsupportedOperationException("updateDataSegmentPosition");
  }

  @Override
  public void updateRecordType(OClusterPosition iPosition, byte iRecordType) throws IOException {
    throw new UnsupportedOperationException("updateRecordType");
  }

  @Override
  public void updateVersion(OClusterPosition iPosition, ORecordVersion iVersion) throws IOException {
    throw new UnsupportedOperationException("updateVersion");
  }

  @Override
  public long getEntries() {
    acquireSharedLock();
    try {
      return size;
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public OClusterPosition getFirstPosition() throws IOException {
    acquireSharedLock();
    try {
      final OPhysicalPosition[] physicalPositions = findFirstPhysicalPosition(0, 0);
      if (physicalPositions.length == 0)
        return OClusterPosition.INVALID_POSITION;

      return physicalPositions[0].clusterPosition;
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public OClusterPosition getLastPosition() throws IOException {
    acquireSharedLock();
    try {
      long pagesCount = diskCache.getFilledUpTo(fileId);
      for (long i = pagesCount - 1; i >= 0; i--) {
        long pagePointer = diskCache.load(fileId, i);
        try {
          final OLocalPage localPage = new OLocalPage(pagePointer, false, OLocalPage.TrackMode.NONE);
          final int recordsCount = localPage.getRecordsCount();

          if (recordsCount > 0) {
            int recordPosition = Integer.MAX_VALUE;

            for (int n = 0; n < recordsCount; n++) {
              recordPosition = localPage.findLastRecord(recordPosition);

              int recordPageOffset = localPage.getRecordPageOffset(recordPosition);
              int recordSize = localPage.getRecordSize(recordPosition);

              if (localPage.getByteValue(recordPageOffset + recordSize - OByteSerializer.BYTE_SIZE - OLongSerializer.LONG_SIZE) == 1)
                return OClusterPositionFactory.INSTANCE.valueOf((i << PAGE_INDEX_OFFSET) | recordPosition);

              recordPosition--;
            }
          }
        } finally {
          diskCache.release(fileId, i);
        }
      }

      return OClusterPosition.INVALID_POSITION;
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public void lock() {
    throw new UnsupportedOperationException("lock");
  }

  @Override
  public void unlock() {
    throw new UnsupportedOperationException("unlock");
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

      flushClusterState();
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public void setSoftlyClosed(boolean softlyClosed) throws IOException {
    acquireExclusiveLock();
    try {
      diskCache.setSoftlyClosed(fileId, softlyClosed);
      clusterStateHolder.setSoftlyClosed(softlyClosed);
    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  public boolean wasSoftlyClosed() throws IOException {
    acquireSharedLock();
    try {
      boolean wasSoftlyClosed = diskCache.wasSoftlyClosed(fileId);
      wasSoftlyClosed = wasSoftlyClosed && clusterStateHolder.wasSoftlyClosedAtPreviousTime();
      return wasSoftlyClosed;
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public String getName() {
    acquireSharedLock();
    try {
      return name;
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public long getRecordsSize() throws IOException {
    acquireSharedLock();
    try {
      return recordsSize;
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
      OClusterPosition clusterPosition = position.clusterPosition;
      long pagePointer = clusterPosition.longValue();

      long pageIndex;
      int recordPosition;
      if (pagePointer >= 0) {
        pageIndex = pagePointer >>> PAGE_INDEX_OFFSET;
        recordPosition = (int) (pagePointer & RECORD_POSITION_MASK) + 1;
      } else {
        pageIndex = 0;
        recordPosition = 0;
      }

      return findFirstPhysicalPosition(pageIndex, recordPosition);
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public OPhysicalPosition[] ceilingPositions(OPhysicalPosition position) throws IOException {
    acquireSharedLock();
    try {
      OClusterPosition clusterPosition = position.clusterPosition;
      long pagePointer = clusterPosition.longValue();

      long pageIndex = pagePointer >>> PAGE_INDEX_OFFSET;
      int recordPosition = (int) (pagePointer & RECORD_POSITION_MASK);

      return findFirstPhysicalPosition(pageIndex, recordPosition);
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public OPhysicalPosition[] lowerPositions(OPhysicalPosition position) throws IOException {
    acquireSharedLock();
    try {
      OClusterPosition clusterPosition = position.clusterPosition;
      long pagePointer = clusterPosition.longValue();

      long pageIndex = pagePointer >>> PAGE_INDEX_OFFSET;
      int recordPosition = (int) (pagePointer & RECORD_POSITION_MASK) - 1;

      return findLastPhysicalPosition(pageIndex, recordPosition);
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public OPhysicalPosition[] floorPositions(OPhysicalPosition position) throws IOException {
    acquireSharedLock();
    try {
      OClusterPosition clusterPosition = position.clusterPosition;
      long pagePointer = clusterPosition.longValue();

      long pageIndex = pagePointer >>> PAGE_INDEX_OFFSET;
      int recordPosition = (int) (pagePointer & RECORD_POSITION_MASK);

      return findLastPhysicalPosition(pageIndex, recordPosition);
    } finally {
      releaseSharedLock();
    }
  }

  public OModificationLock getExternalModificationLock() {
    return externalModificationLock;
  }

  private OPhysicalPosition[] findFirstPhysicalPosition(long pageIndex, int recordPosition) throws IOException {
    long pagesCount = diskCache.getFilledUpTo(fileId);
    pageLoop: for (long i = pageIndex; i < pagesCount; i++) {
      long pointer = diskCache.load(fileId, i);

      try {
        final OLocalPage localPage = new OLocalPage(pointer, false, OLocalPage.TrackMode.NONE);

        int recordsCount = localPage.getRecordsCount();

        if (recordsCount > 0) {
          while (true) {
            recordPosition = localPage.findFirstRecord(recordPosition);
            if (recordPosition < 0) {
              recordPosition = 0;
              continue pageLoop;
            } else {
              int recordPageOffset = localPage.getRecordPageOffset(recordPosition);
              int recordSize = localPage.getRecordSize(recordPosition);

              if (localPage.getByteValue(recordPageOffset + recordSize - OLongSerializer.LONG_SIZE - OByteSerializer.BYTE_SIZE) == 1) {
                OPhysicalPosition physicalPosition = new OPhysicalPosition();

                physicalPosition.clusterPosition = OClusterPositionFactory.INSTANCE.valueOf((i << PAGE_INDEX_OFFSET)
                    | recordPosition);
                physicalPosition.recordVersion = localPage.getRecordVersion(recordPosition);
                physicalPosition.recordType = localPage.getByteValue(recordPageOffset);

                physicalPosition.recordSize = -1;
                physicalPosition.dataSegmentId = -1;
                physicalPosition.dataSegmentPos = -1;

                return new OPhysicalPosition[] { physicalPosition };
              }

              recordPosition++;
            }
          }
        }

      } finally {
        diskCache.release(fileId, i);
      }
    }

    return new OPhysicalPosition[0];
  }

  private OPhysicalPosition[] findLastPhysicalPosition(long pageIndex, int recordPosition) throws IOException {
    long pagesCount = diskCache.getFilledUpTo(fileId);
    long endPageIndex;
    if (pagesCount <= pageIndex) {
      recordPosition = Integer.MAX_VALUE;
      endPageIndex = pagesCount - 1;
    } else {
      endPageIndex = pageIndex;
    }

    pageLoop: for (long i = endPageIndex; i >= 0; i--) {
      long pointer = diskCache.load(fileId, i);
      try {
        final OLocalPage localPage = new OLocalPage(pointer, false, OLocalPage.TrackMode.NONE);

        int recordsCount = localPage.getRecordsCount();

        if (recordsCount > 0) {
          while (true) {
            recordPosition = localPage.findLastRecord(recordPosition);
            if (recordPosition < 0) {
              recordPosition = Integer.MAX_VALUE;
              continue pageLoop;
            } else {
              int recordPageOffset = localPage.getRecordPageOffset(recordPosition);
              int recordSize = localPage.getRecordSize(recordPosition);

              if (localPage.getByteValue(recordPageOffset + recordSize - OLongSerializer.LONG_SIZE - OByteSerializer.BYTE_SIZE) == 1) {
                OPhysicalPosition physicalPosition = new OPhysicalPosition();

                physicalPosition.clusterPosition = OClusterPositionFactory.INSTANCE.valueOf((i << PAGE_INDEX_OFFSET)
                    | recordPosition);
                physicalPosition.recordVersion = localPage.getRecordVersion(recordPosition);
                physicalPosition.recordType = localPage.getByteValue(recordPageOffset);

                physicalPosition.recordSize = -1;
                physicalPosition.dataSegmentId = -1;
                physicalPosition.dataSegmentPos = -1;

                return new OPhysicalPosition[] { physicalPosition };
              }

              recordPosition--;
            }
          }
        }

      } finally {
        diskCache.release(fileId, i);
      }
    }

    return new OPhysicalPosition[0];
  }

  public void flushClusterState() throws IOException {

    OFile file = clusterStateHolder.getFile();

    final CRC32 crc32 = new CRC32();
    byte[] clusterState = new byte[STATE_SIZE];

    int offset = OIntegerSerializer.INT_SIZE;

    if (config.useWal && writeAheadLog != null) {
      OLogSequenceNumber end = writeAheadLog.end();

      offset += OIntegerSerializer.INT_SIZE;

      OIntegerSerializer.INSTANCE.serializeNative(end.getSegment(), clusterState, offset);
      offset += OIntegerSerializer.INT_SIZE;

      OLongSerializer.INSTANCE.serializeNative(end.getPosition(), clusterState, offset);
      offset += OLongSerializer.LONG_SIZE;
    } else {
      OIntegerSerializer.INSTANCE.serializeNative(1, clusterState, offset);
      offset += OIntegerSerializer.INT_SIZE;

      OLongSerializer.INSTANCE.serializeNative(System.currentTimeMillis(), clusterState, offset);
      offset += OLongSerializer.LONG_SIZE + OIntegerSerializer.INT_SIZE;
    }

    OLongSerializer.INSTANCE.serializeNative(size, clusterState, offset);
    offset += OLongSerializer.LONG_SIZE;

    OLongSerializer.INSTANCE.serializeNative(recordsSize, clusterState, offset);
    offset += OLongSerializer.LONG_SIZE;

    OIntegerSerializer.INSTANCE.serializeNative(freePageLists.length, clusterState, offset);
    offset += OIntegerSerializer.INT_SIZE;

    for (long freePageIndex : freePageLists) {
      OLongSerializer.INSTANCE.serializeNative(freePageIndex, clusterState, offset);
      offset += OLongSerializer.LONG_SIZE;
    }

    crc32.update(clusterState, OIntegerSerializer.INT_SIZE, STATE_SIZE - OIntegerSerializer.INT_SIZE);
    OIntegerSerializer.INSTANCE.serializeNative((int) crc32.getValue(), clusterState, 0);

    long fileOffset;
    if (useFirstStateHolder)
      fileOffset = 0;
    else
      fileOffset = STATE_SIZE;
    useFirstStateHolder = !useFirstStateHolder;

    file.write(fileOffset, clusterState);
    clusterStateHolder.synch();
  }

  private void loadClusterState() throws IOException {
    OFile file = clusterStateHolder.getFile();
    byte[] clusterStateOne = new byte[STATE_SIZE];
    byte[] clusterStateTwo = new byte[STATE_SIZE];
    byte[] clusterState;

    file.read(0, clusterStateOne, STATE_SIZE);
    file.read(STATE_SIZE, clusterStateTwo, STATE_SIZE);

    CRC32 crc32 = new CRC32();
    crc32.update(clusterStateOne, OIntegerSerializer.INT_SIZE, STATE_SIZE - OIntegerSerializer.INT_SIZE);

    int crcOne = OIntegerSerializer.INSTANCE.deserializeNative(clusterStateOne, 0);
    Comparable tsOne;

    if (crcOne != (int) crc32.getValue()) {
      tsOne = null;
    } else {
      int type = OIntegerSerializer.INSTANCE.deserializeNative(clusterStateOne, OIntegerSerializer.INT_SIZE);

      if (type == 1) {
        tsOne = OLongSerializer.INSTANCE.deserializeNative(clusterStateOne, 2 * OIntegerSerializer.INT_SIZE);
      } else if (type == 0) {
        int segment = OIntegerSerializer.INSTANCE.deserializeNative(clusterStateOne, 2 * OIntegerSerializer.INT_SIZE);
        long position = OLongSerializer.INSTANCE.deserializeNative(clusterStateOne, 3 * OIntegerSerializer.INT_SIZE);

        tsOne = new OLogSequenceNumber(segment, position);
      } else
        throw new OStorageException("Invalid type of cluster state timestamp");
    }

    int crcTwo = OIntegerSerializer.INSTANCE.deserializeNative(clusterStateTwo, 0);

    Comparable tsTwo;
    crc32.reset();
    crc32.update(clusterStateTwo, OIntegerSerializer.INT_SIZE, STATE_SIZE - OIntegerSerializer.INT_SIZE);
    if (crcTwo != (int) crc32.getValue()) {
      tsTwo = null;
    } else {
      int type = OIntegerSerializer.INSTANCE.deserializeNative(clusterStateTwo, OIntegerSerializer.INT_SIZE);

      if (type == 1) {
        tsTwo = OLongSerializer.INSTANCE.deserializeNative(clusterStateTwo, 2 * OIntegerSerializer.INT_SIZE);
      } else if (type == 0) {
        int segment = OIntegerSerializer.INSTANCE.deserializeNative(clusterStateTwo, 2 * OIntegerSerializer.INT_SIZE);
        long position = OLongSerializer.INSTANCE.deserializeNative(clusterStateTwo, 3 * OIntegerSerializer.INT_SIZE);

        tsTwo = new OLogSequenceNumber(segment, position);
      } else
        throw new OStorageException("Invalid type of cluster state timestamp");
    }

    if (tsOne == null && tsTwo == null) {
      OLogManager.instance().error(this, "Cluster state can not be loaded from file.");
      return;
    }

    if (tsOne == null) {
      clusterState = clusterStateTwo;
      useFirstStateHolder = true;
    } else if (tsTwo == null) {
      clusterState = clusterStateOne;
      useFirstStateHolder = false;
    } else {
      int cmp = tsOne.compareTo(tsTwo);
      if (cmp >= 0) {
        clusterState = clusterStateOne;
        useFirstStateHolder = false;
      } else {
        clusterState = clusterStateTwo;
        useFirstStateHolder = true;
      }
    }

    int offset = 3 * OIntegerSerializer.INT_SIZE + OLongSerializer.LONG_SIZE;
    size = OLongSerializer.INSTANCE.deserializeNative(clusterState, offset);
    offset += OLongSerializer.LONG_SIZE;

    recordsSize = OLongSerializer.INSTANCE.deserializeNative(clusterState, offset);
    offset += OLongSerializer.LONG_SIZE;

    int freeListSize = OIntegerSerializer.INSTANCE.deserializeNative(clusterState, offset);
    offset += OIntegerSerializer.INT_SIZE;

    freePageLists = new long[freeListSize];

    for (int i = 0; i < freeListSize; i++) {
      freePageLists[i] = OLongSerializer.INSTANCE.deserializeNative(clusterState, offset);
      offset += OLongSerializer.LONG_SIZE;
    }
  }

  private boolean checkFreePages() throws IOException {
    long filledUpTo = diskCache.getFilledUpTo(fileId);
    for (long i = 0; i < filledUpTo; i++) {
      long pointer = diskCache.load(fileId, i);
      int freePageIndex;
      try {
        final OLocalPage page = new OLocalPage(pointer, false, OLocalPage.TrackMode.NONE);
        freePageIndex = calculateFreePageIndex(page);
        if (freePageIndex < 0)
          continue;
      } finally {
        diskCache.release(fileId, i);
      }

      if (!findInFreeList(freePageIndex, i))
        return false;
    }

    return true;
  }

  private boolean findInFreeList(int freePageIndex, long testPageIndex) throws IOException {
    long pageIndex = freePageLists[freePageIndex];
    long prevPageIndex = -1;
    while (pageIndex != testPageIndex && pageIndex >= 0) {
      long nextPageIndex;
      long pointer = diskCache.load(fileId, pageIndex);
      try {
        final OLocalPage localPage = new OLocalPage(pointer, false, OLocalPage.TrackMode.NONE);
        assert prevPageIndex == localPage.getPrevPage();

        prevPageIndex = pageIndex;
        nextPageIndex = localPage.getNextPage();
      } finally {
        diskCache.release(fileId, pageIndex);
      }

      pageIndex = nextPageIndex;
    }

    return pageIndex == testPageIndex;
  }

  private void logClusterState(long prevSize, long prevRecordsSize) throws IOException {
    if (!config.useWal || writeAheadLog == null)
      return;

    OOperationUnitId operationUnitId = currentUnitId.get();
    assert operationUnitId != null;

    writeAheadLog.log(new OClusterStateRecord(size, recordsSize, id, prevSize, prevRecordsSize, operationUnitId));
  }

  private void restoreClusterState(OClusterStateRecord walRecord) {
    size = walRecord.getSize();
    recordsSize = walRecord.getRecordsSize();
  }

  public void restoreRecord(OWALRecord record) throws IOException {
    if (record instanceof OClusterStateRecord)
      restoreClusterState((OClusterStateRecord) record);
    else if (record instanceof OAbstractPageWALRecord)
      restorePage((OAbstractPageWALRecord) record);
    else if (record instanceof OFreePageChangeRecord) {
      restoreFreePageListState((OFreePageChangeRecord) record);
    } else {
      OLogManager.instance().error(this, "Invalid WAL record type was passed %s. Given record will be skipped.", record.getClass());
      assert false : "Invalid WAL record type was passed " + record.getClass().getName();
    }
  }

  private void restoreFreePageListState(OFreePageChangeRecord record) {
    freePageLists[record.getFreePageIndex()] = record.getPageIndex();
  }

  public void revertRecord(OWALRecord record) throws IOException {
    if (record instanceof OClusterStateRecord) {
      final OClusterStateRecord clusterStateRecord = (OClusterStateRecord) record;

      this.recordsSize = clusterStateRecord.getPrevRecordsSize();
      this.size = clusterStateRecord.getPrevSize();

    } else if (record instanceof OAbstractPageWALRecord)
      revertPage((OAbstractPageWALRecord) record);
    else if (record instanceof OFreePageChangeRecord) {
      revertFreePageListState((OFreePageChangeRecord) record);
    } else {
      OLogManager.instance().error(this, "Invalid WAL record type was passed %s. Given record will be skipped.", record.getClass());
      assert false : "Invalid WAL record type was passed " + record.getClass().getName();
    }
  }

  private void revertFreePageListState(OFreePageChangeRecord record) {
    freePageLists[record.getFreePageIndex()] = record.getPrevPageIndex();
  }

  private static final class AddEntryResult {
    private final long           pagePointer;
    private final ORecordVersion recordVersion;
    private final int            recordsSizeDiff;

    public AddEntryResult(long pagePointer, ORecordVersion recordVersion, int recordsSizeDiff) {
      this.pagePointer = pagePointer;
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
}
