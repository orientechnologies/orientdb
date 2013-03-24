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

import java.io.IOException;

import com.orientechnologies.common.concur.resource.OSharedResourceAdaptive;
import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.config.OStorageClusterConfiguration;
import com.orientechnologies.orient.core.config.OStorageSegmentConfiguration;
import com.orientechnologies.orient.core.id.OClusterPosition;
import com.orientechnologies.orient.core.index.hashindex.local.cache.ODiskCache;
import com.orientechnologies.orient.core.storage.OCluster;
import com.orientechnologies.orient.core.storage.OClusterEntryIterator;
import com.orientechnologies.orient.core.storage.OPhysicalPosition;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.fs.OFileFactory;
import com.orientechnologies.orient.core.storage.impl.local.OStorageLocal;
import com.orientechnologies.orient.core.version.ORecordVersion;
import com.orientechnologies.orient.core.version.OVersionFactory;

/**
 * @author Andrey Lomakin
 * @since 22.03.13
 */
public class OLocalPaginatedCluster extends OSharedResourceAdaptive implements OCluster {
  private static final String DEF_EXTENSION   = ".pcl";

  public static final String  TYPE            = "PHYSICAL";

  private ODiskCache          diskCache;

  private String              name;
  private OStorageLocal       storageLocal;
  private int                 id;
  private long                fileId;

  private long                size;
  private long                recordsSize;

  private long[]              freePageIndexes = new long[48];

  public OLocalPaginatedCluster() {
    super(OGlobalConfiguration.ENVIRONMENT_CONCURRENT.getValueAsBoolean());
    for (int i = 0; i < freePageIndexes.length; i++)
      freePageIndexes[i] = -1;
  }

  @Override
  public void configure(OStorage storage, int id, String clusterName, String location, int dataSegmentId, Object... parameters)
      throws IOException {
    acquireExclusiveLock();
    try {
      storageLocal = (OStorageLocal) storage;
      diskCache = storageLocal.getDiskCache();
      name = clusterName;
      this.id = id;
    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  public void configure(OStorage storage, OStorageClusterConfiguration config) throws IOException {
    acquireExclusiveLock();
    try {
      storageLocal = (OStorageLocal) storage;
      diskCache = storageLocal.getDiskCache();
      name = config.getName();
      this.id = config.getId();
    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  public void create(int startSize) throws IOException {
    acquireExclusiveLock();
    try {
      final OStorageSegmentConfiguration fileConfiguration = new OStorageSegmentConfiguration(storageLocal.getConfiguration(),
          name, id);
      fileConfiguration.fileType = OFileFactory.CLASSIC;
      fileId = diskCache.openFile(fileConfiguration, DEF_EXTENSION);

      long pagePointer = diskCache.allocateAndLockForWrite(fileId, 0);
      try {
        OLocalPage localPage = new OLocalPage(pagePointer, true);
        freePageIndexes[freePageIndexes.length - 1] = 0;
      } finally {
        diskCache.releaseWriteLock(fileId, 0);
      }

    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  public void open() throws IOException {
    acquireExclusiveLock();
    try {
      final OStorageSegmentConfiguration fileConfiguration = new OStorageSegmentConfiguration(storageLocal.getConfiguration(),
          name, id);
      fileConfiguration.fileType = OFileFactory.CLASSIC;
      fileId = diskCache.openFile(fileConfiguration, DEF_EXTENSION);
    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  public void close() throws IOException {
    acquireExclusiveLock();
    try {
      diskCache.closeFile(fileId);
    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  public void delete() throws IOException {
    acquireExclusiveLock();
    try {
      diskCache.deleteFile(fileId);
    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  public void set(ATTRIBUTES iAttribute, Object iValue) throws IOException {
    // TODO
  }

  @Override
  public void convertToTombstone(OClusterPosition iPosition) throws IOException {
    throw new UnsupportedOperationException("convertToTombstone");
  }

  public long createRecord(final byte[] content, final ORecordVersion recordVersion, final byte recordType) throws IOException {
    acquireExclusiveLock();
    try {
      long nextPagePointer = -1;

      int entryContentLength = content.length + OByteSerializer.BYTE_SIZE + OLongSerializer.LONG_SIZE;

      if (entryContentLength < OLocalPage.MAX_RECORD_SIZE) {
        byte[] entryContent = new byte[entryContentLength];

        int entryPosition = 0;
        entryContent[entryPosition] = recordType;
        entryPosition++;

        System.arraycopy(content, 0, entryContent, entryPosition, content.length);
        entryPosition += content.length;

        OLongSerializer.INSTANCE.serializeNative(nextPagePointer, entryContent, entryPosition);

        size++;
        return addEntry(recordVersion, entryContent);
      } else {
        int entrySize = content.length + OVersionFactory.instance().getVersionSize() + OByteSerializer.BYTE_SIZE;

        int fullEntryPosition = 0;
        byte[] fullEntry = new byte[entrySize];

        fullEntry[fullEntryPosition] = recordType;
        fullEntryPosition++;

        System.arraycopy(content, 0, fullEntry, fullEntryPosition, content.length);

        nextPagePointer = -1;
        int to = fullEntry.length;
        int from = to - (OLocalPage.MAX_RECORD_SIZE - OLongSerializer.LONG_SIZE);
        do {
          byte[] entryContent = new byte[from - to + OLongSerializer.LONG_SIZE];
          System.arraycopy(fullEntry, from, entryContent, 0, from - to);

          OLongSerializer.INSTANCE.serializeNative(nextPagePointer, entryContent, entryContent.length - OLongSerializer.LONG_SIZE);
          nextPagePointer = addEntry(recordVersion, entryContent);

          to = from;
          from = from - (OLocalPage.MAX_RECORD_SIZE - OLongSerializer.LONG_SIZE);
          if (from < 0)
            from = 0;
        } while (from > to);

        size++;
        return nextPagePointer;
      }
    } finally {
      releaseExclusiveLock();
    }
  }

  private long addEntry(ORecordVersion recordVersion, byte[] entryContent) throws IOException {
    int freePagePosition = entryContent.length / 1024;
    freePagePosition -= 16;

    long freePageIndex;
    do {
      freePageIndex = freePageIndexes[freePagePosition];
      freePagePosition++;
    } while (freePageIndex < 0 && freePagePosition < 48);

    boolean newRecord = false;
    if (freePageIndex < 0) {
      freePageIndex = diskCache.getFilledUpTo(fileId);
      newRecord = true;
    }

    long pagePointer = diskCache.loadAndLockForWrite(fileId, freePageIndex);
    try {
      final OLocalPage localPage = new OLocalPage(pagePointer, newRecord);
      int initialFreeSpace = localPage.getFreeSpace();

      int position = localPage.appendRecord(recordVersion, entryContent);
      assert position >= 0;

      int freeSpace = localPage.getFreeSpace();
      recordsSize += initialFreeSpace - freeSpace;

      int newPagePosition = (freeSpace - 1023) / 1024;
      newPagePosition -= 16;

      if (newPagePosition != freePagePosition) {
        long nextPage = localPage.getNextPage();
        freePageIndexes[freePagePosition] = nextPage;

        localPage.setNextPage(freePageIndexes[newPagePosition]);
        freePageIndexes[newPagePosition] = freePageIndex;
      }

      return (freePageIndex << 16) | position;
    } finally {
      diskCache.releaseWriteLock(fileId, freePageIndex);
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
    acquireExclusiveLock();
    try {
      diskCache.truncateFile(fileId);
    } finally {
      releaseExclusiveLock();
    }
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
  public OPhysicalPosition getPhysicalPosition(OPhysicalPosition iPPosition) throws IOException {
    return null; // TODO
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
    return size;
  }

  @Override
  public OClusterPosition getFirstPosition() throws IOException {
    return null; // TODO
  }

  @Override
  public OClusterPosition getLastPosition() throws IOException {
    return null; // TODO
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
    diskCache.flushFile(fileId);
  }

  @Override
  public void setSoftlyClosed(boolean softlyClosed) throws IOException {
    diskCache.setSoftlyClosed(fileId, softlyClosed);
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public long getRecordsSize() throws IOException {
    return recordsSize;
  }

  @Override
  public boolean isHashBased() {
    return false;
  }

  @Override
  public OClusterEntryIterator absoluteIterator() {
    return null;// TODO
  }

  @Override
  public OPhysicalPosition[] higherPositions(OPhysicalPosition position) throws IOException {
    return new OPhysicalPosition[0]; // TODO
  }

  @Override
  public OPhysicalPosition[] ceilingPositions(OPhysicalPosition position) throws IOException {
    return new OPhysicalPosition[0]; // TODO
  }

  @Override
  public OPhysicalPosition[] lowerPositions(OPhysicalPosition position) throws IOException {
    return new OPhysicalPosition[0]; // TODO
  }

  @Override
  public OPhysicalPosition[] floorPositions(OPhysicalPosition position) throws IOException {
    return new OPhysicalPosition[0]; // TODO
  }
}
