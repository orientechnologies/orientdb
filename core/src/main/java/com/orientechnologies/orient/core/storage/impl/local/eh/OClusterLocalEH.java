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
package com.orientechnologies.orient.core.storage.impl.local.eh;

import java.io.IOException;

import com.orientechnologies.common.concur.resource.OSharedResourceAdaptive;
import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.config.OStorageClusterConfiguration;
import com.orientechnologies.orient.core.config.OStorageEHClusterConfiguration;
import com.orientechnologies.orient.core.config.OStorageFileConfiguration;
import com.orientechnologies.orient.core.id.OClusterPosition;
import com.orientechnologies.orient.core.id.OClusterPositionFactory;
import com.orientechnologies.orient.core.index.hashindex.local.OHashFunction;
import com.orientechnologies.orient.core.index.hashindex.local.OHashIndexBucket;
import com.orientechnologies.orient.core.index.hashindex.local.OLocalHashTable;
import com.orientechnologies.orient.core.storage.OCluster;
import com.orientechnologies.orient.core.storage.OClusterEntryIterator;
import com.orientechnologies.orient.core.storage.OPhysicalPosition;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.fs.OFileFactory;
import com.orientechnologies.orient.core.storage.impl.local.OSingleFileSegment;
import com.orientechnologies.orient.core.storage.impl.local.OStorageLocal;
import com.orientechnologies.orient.core.storage.impl.local.OStorageVariableParser;
import com.orientechnologies.orient.core.version.ORecordVersion;

/**
 * @author Andrey Lomakin
 * @since 06.02.13
 */
public class OClusterLocalEH extends OSharedResourceAdaptive implements OCluster {
  public static final String                                         TYPE                                  = "PHYSICAL";
  public static final String                                         METADATA_CONFIGURATION_FILE_EXTENSION = ".oem";
  public static final String                                         TREE_STATE_FILE_EXTENSION             = ".oet";
  public static final String                                         BUCKET_FILE_EXTENSION                 = ".oef";
  public static final String                                         CLUSTER_STATE_FILE_EXTENSION          = ".ocs";

  private long                                                       tombstonesCount;

  private OStorageLocal                                              storage;

  private int                                                        id;
  private String                                                     name;

  private OStorageEHClusterConfiguration                             config;
  private OSingleFileSegment                                         clusterStateHolder;

  private final OLocalHashTable<OClusterPosition, OPhysicalPosition> localHashTable;

  public OClusterLocalEH() {
    super(OGlobalConfiguration.ENVIRONMENT_CONCURRENT.getValueAsBoolean());
    localHashTable = new OLocalHashTable<OClusterPosition, OPhysicalPosition>(METADATA_CONFIGURATION_FILE_EXTENSION,
        TREE_STATE_FILE_EXTENSION, BUCKET_FILE_EXTENSION, new OHashFunction<OClusterPosition>() {
          @Override
          public long hashCode(OClusterPosition value) {
            return value.longValueHigh();
          }
        });
  }

  @Override
  public void configure(OStorage iStorage, int iId, String iClusterName, String iLocation, int iDataSegmentId,
      Object... iParameters) throws IOException {
    acquireExclusiveLock();
    try {
      config = new OStorageEHClusterConfiguration(iStorage.getConfiguration(), iId, iClusterName, iLocation, iDataSegmentId);
      init(iStorage, iId, iClusterName, iLocation, iDataSegmentId);
    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  public void configure(OStorage iStorage, OStorageClusterConfiguration iConfig) throws IOException {
    acquireExclusiveLock();
    try {
      config = (OStorageEHClusterConfiguration) iConfig;
      init(iStorage, config.getId(), config.getName(), config.getLocation(), config.getDataSegmentId());
    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  public void create(int iStartSize) throws IOException {
    acquireExclusiveLock();
    try {
      localHashTable.create(name, OClusterPositionSerializer.INSTANCE, OPhysicalPositionSerializer.INSTANCE, storage);
      clusterStateHolder.create(-1);

      if (config.root.clusters.size() <= config.id)
        config.root.clusters.add(config);
      else
        config.root.clusters.set(config.id, config);
    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  public void open() throws IOException {
    acquireExclusiveLock();
    try {
      localHashTable.load(name, storage);
      clusterStateHolder.open();

      tombstonesCount = clusterStateHolder.getFile().readLong(0);
    } finally {
      releaseExclusiveLock();
    }
  }

  protected void init(final OStorage iStorage, final int iId, final String iClusterName, final String iLocation,
      final int iDataSegmentId, final Object... iParameters) throws IOException {
    OFileUtils.checkValidName(iClusterName);

    OStorageFileConfiguration clusterStateConfiguration = new OStorageFileConfiguration(null,
        OStorageVariableParser.DB_PATH_VARIABLE + "/" + config.name + CLUSTER_STATE_FILE_EXTENSION, OFileFactory.CLASSIC, "1024",
        "50%");

    config.dataSegmentId = iDataSegmentId;
    storage = (OStorageLocal) iStorage;
    name = iClusterName;
    id = iId;

    clusterStateHolder = new OSingleFileSegment(storage, clusterStateConfiguration);
  }

  @Override
  public void close() throws IOException {
    acquireExclusiveLock();
    try {
      saveState();

      clusterStateHolder.synch();

      localHashTable.close();
      clusterStateHolder.close();
    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  public void delete() throws IOException {
    acquireExclusiveLock();
    try {
      truncate();

      localHashTable.delete();
      clusterStateHolder.delete();
    } finally {
      releaseExclusiveLock();
    }

  }

  public void set(ATTRIBUTES iAttribute, Object iValue) throws IOException {
    if (iAttribute == null)
      throw new IllegalArgumentException("attribute is null");

    final String stringValue = iValue != null ? iValue.toString() : null;

    acquireExclusiveLock();
    try {

      switch (iAttribute) {
      case NAME:
        setNameInternal(stringValue);
        break;
      case DATASEGMENT:
        setDataSegmentInternal(stringValue);
        break;
      }

    } finally {
      releaseExclusiveLock();
    }
  }

  private void setNameInternal(final String iNewName) {
    if (storage.getClusterIdByName(iNewName) > -1)
      throw new IllegalArgumentException("Cluster with name '" + iNewName + "' already exists");

    localHashTable.rename(iNewName);

    config.name = iNewName;
    storage.renameCluster(name, iNewName);
    name = iNewName;
    storage.getConfiguration().update();
  }

  /**
   * Assigns a different data-segment id.
   * 
   * @param iName
   *          Data-segment's name
   */
  private void setDataSegmentInternal(final String iName) {
    config.dataSegmentId = storage.getDataSegmentIdByName(iName);
    storage.getConfiguration().update();
  }

  @Override
  public void convertToTombstone(OClusterPosition iPosition) throws IOException {
    acquireExclusiveLock();
    try {
      final OPhysicalPosition physicalPosition = localHashTable.get(iPosition);
      if (physicalPosition == null)
        return;

      final ORecordVersion version = physicalPosition.recordVersion;
      version.convertToTombstone();
      tombstonesCount++;
    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  public long getTombstonesCount() {
    acquireSharedLock();
    try {
      return tombstonesCount;
    } finally {
      releaseSharedLock();
    }

  }

  @Override
  public boolean hasTombstonesSupport() {
    return true;
  }

  @Override
  public void truncate() throws IOException {
    storage.checkForClusterPermissions(getName());

    acquireExclusiveLock();
    try {
      OHashIndexBucket.Entry<OClusterPosition, OPhysicalPosition> entry = localHashTable.firstEntry();

      while (entry != null) {
        if (storage.checkForRecordValidity(entry.value)) {
          storage.getDataSegmentById(entry.value.dataSegmentId).deleteRecord(entry.value.dataSegmentPos);
        }

        final OHashIndexBucket.Entry<OClusterPosition, OPhysicalPosition>[] entries = localHashTable.higherEntries(entry.key, 1);
        if (entries.length > 0)
          entry = entries[0];
        else
          entry = null;
      }

      localHashTable.clear();
      tombstonesCount = 0;
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
    acquireSharedLock();
    try {
      return config.dataSegmentId;
    } finally {
      releaseSharedLock();
    }

  }

  @Override
  public boolean addPhysicalPosition(OPhysicalPosition iPPosition) throws IOException {
    acquireExclusiveLock();
    try {
      if (localHashTable.get(iPPosition.clusterPosition) != null)
        return false;

      localHashTable.put(iPPosition.clusterPosition, iPPosition);
      return true;
    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  public OPhysicalPosition getPhysicalPosition(OPhysicalPosition iPPosition) throws IOException {
    acquireSharedLock();
    try {
      return localHashTable.get(iPPosition.clusterPosition);
    } finally {
      releaseSharedLock();
    }

  }

  @Override
  public void updateDataSegmentPosition(OClusterPosition iPosition, int iDataSegmentId, long iDataPosition) throws IOException {
    acquireExclusiveLock();
    try {
      OPhysicalPosition position = localHashTable.get(iPosition);
      position.dataSegmentId = iDataSegmentId;
      position.dataSegmentPos = iDataPosition;
      localHashTable.put(iPosition, position);
    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  public void removePhysicalPosition(OClusterPosition iPosition) throws IOException {
    acquireExclusiveLock();
    try {
      OPhysicalPosition physicalPosition = localHashTable.remove(iPosition);
      if (physicalPosition != null && physicalPosition.recordVersion.isTombstone())
        tombstonesCount--;

    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  public void updateRecordType(OClusterPosition iPosition, byte iRecordType) throws IOException {
    acquireExclusiveLock();
    try {
      OPhysicalPosition position = localHashTable.get(iPosition);
      position.recordType = iRecordType;
      localHashTable.put(iPosition, position);
    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  public void updateVersion(OClusterPosition iPosition, ORecordVersion iVersion) throws IOException {
    acquireExclusiveLock();
    try {
      OPhysicalPosition position = localHashTable.get(iPosition);
      position.recordVersion = iVersion;
      localHashTable.put(iPosition, position);
    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  public long getEntries() {
    acquireSharedLock();
    try {
      return localHashTable.size();
    } finally {
      releaseSharedLock();
    }

  }

  @Override
  public OClusterPosition getFirstPosition() throws IOException {
    acquireSharedLock();
    try {
      final OHashIndexBucket.Entry<OClusterPosition, OPhysicalPosition> entry = localHashTable.firstEntry();
      if (entry == null)
        return OClusterPosition.INVALID_POSITION;

      return entry.key;
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public OClusterPosition getLastPosition() throws IOException {
    acquireSharedLock();
    try {
      final OHashIndexBucket.Entry<OClusterPosition, OPhysicalPosition> entry = localHashTable.lastEntry();
      if (entry == null)
        return OClusterPosition.INVALID_POSITION;

      return entry.key;
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public String toString() {
    return name + " (id=" + id + ")";
  }

  @Override
  public void lock() {
    acquireSharedLock();
  }

  @Override
  public void unlock() {
    releaseSharedLock();
  }

  @Override
  public int getId() {
    acquireSharedLock();
    try {
      return id;
    } finally {
      releaseSharedLock();
    }

  }

  @Override
  public void synch() throws IOException {
    acquireExclusiveLock();
    try {
      saveState();

      localHashTable.flush();
      clusterStateHolder.synch();

    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  public void setSoftlyClosed(boolean softlyClosed) throws IOException {
    acquireExclusiveLock();
    try {
      localHashTable.setSoftlyClosed(softlyClosed);
      clusterStateHolder.setSoftlyClosed(softlyClosed);
    } finally {
      releaseExclusiveLock();
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
      long size = 0;
      OHashIndexBucket.Entry<OClusterPosition, OPhysicalPosition>[] entries = localHashTable
          .ceilingEntries(OClusterPositionFactory.INSTANCE.valueOf(0));
      while (entries.length > 0) {
        for (OHashIndexBucket.Entry<OClusterPosition, OPhysicalPosition> entry : entries) {
          if (entry.value.dataSegmentPos > -1 && !entry.value.recordVersion.isTombstone())
            size += storage.getDataSegmentById(entry.value.dataSegmentId).getRecordSize(entry.value.dataSegmentPos);
        }
        entries = localHashTable.higherEntries(entries[entries.length - 1].key);
      }
      return size;
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public boolean isHashBased() {
    return true;
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
      OHashIndexBucket.Entry<OClusterPosition, OPhysicalPosition>[] entries = localHashTable
          .higherEntries(position.clusterPosition);
      OPhysicalPosition[] positions = new OPhysicalPosition[entries.length];
      for (int i = 0; i < entries.length; i++)
        positions[i] = entries[i].value;

      return positions;
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public OPhysicalPosition[] ceilingPositions(OPhysicalPosition position) throws IOException {
    acquireSharedLock();
    try {
      OHashIndexBucket.Entry<OClusterPosition, OPhysicalPosition>[] entries = localHashTable
          .ceilingEntries(position.clusterPosition);
      OPhysicalPosition[] positions = new OPhysicalPosition[entries.length];
      for (int i = 0; i < entries.length; i++)
        positions[i] = entries[i].value;

      return positions;
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public OPhysicalPosition[] lowerPositions(OPhysicalPosition position) throws IOException {
    acquireSharedLock();
    try {
      OHashIndexBucket.Entry<OClusterPosition, OPhysicalPosition>[] entries = localHashTable.lowerEntries(position.clusterPosition);
      OPhysicalPosition[] positions = new OPhysicalPosition[entries.length];
      for (int i = 0; i < entries.length; i++)
        positions[i] = entries[i].value;

      return positions;
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public OPhysicalPosition[] floorPositions(OPhysicalPosition position) throws IOException {
    acquireSharedLock();
    try {
      OHashIndexBucket.Entry<OClusterPosition, OPhysicalPosition>[] entries = localHashTable.floorEntries(position.clusterPosition);
      OPhysicalPosition[] positions = new OPhysicalPosition[entries.length];
      for (int i = 0; i < entries.length; i++)
        positions[i] = entries[i].value;

      return positions;
    } finally {
      releaseSharedLock();
    }
  }

  private void saveState() throws IOException {
    clusterStateHolder.getFile().writeLong(0, tombstonesCount);
  }
}
