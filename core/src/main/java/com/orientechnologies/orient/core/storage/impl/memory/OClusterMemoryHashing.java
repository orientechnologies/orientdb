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
package com.orientechnologies.orient.core.storage.impl.memory;

import java.io.IOException;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import com.orientechnologies.common.concur.lock.OModificationLock;
import com.orientechnologies.orient.core.id.OClusterPosition;
import com.orientechnologies.orient.core.storage.OCluster;
import com.orientechnologies.orient.core.storage.OClusterEntryIterator;
import com.orientechnologies.orient.core.storage.OPhysicalPosition;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import com.orientechnologies.orient.core.version.ORecordVersion;

/**
 * @author Artem Loginov (logart2007@gmail.com)
 */
public class OClusterMemoryHashing extends OClusterMemory implements OCluster {

  public static final String                                TYPE            = "MEMORY";

  private NavigableMap<OClusterPosition, OPhysicalPosition> content         = new TreeMap<OClusterPosition, OPhysicalPosition>();

  private long                                              tombstonesCount = 0;

  @Override
  public boolean addPhysicalPosition(OPhysicalPosition physicalPosition) {
    acquireExclusiveLock();
    try {
      if (content.containsKey(physicalPosition.clusterPosition))
        return false;

      content.put(physicalPosition.clusterPosition, physicalPosition);
      return true;
    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  public OPhysicalPosition getPhysicalPosition(OPhysicalPosition physicalPosition) {
    acquireSharedLock();
    try {
      if (physicalPosition.clusterPosition.isNew())
        return null;

      return content.get(physicalPosition.clusterPosition);

    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public void updateDataSegmentPosition(OClusterPosition clusterPosition, int dataSegmentId, long dataPosition) {
    acquireExclusiveLock();
    try {

      final OPhysicalPosition physicalPosition = content.get(clusterPosition);
      physicalPosition.dataSegmentId = dataSegmentId;
      physicalPosition.dataSegmentPos = dataPosition;

    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  public void removePhysicalPosition(OClusterPosition clusterPosition) {
    acquireExclusiveLock();
    try {

      final OPhysicalPosition physicalPosition = content.remove(clusterPosition);
      if (physicalPosition != null && physicalPosition.recordVersion.isTombstone())
        tombstonesCount--;

    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  public void updateRecordType(OClusterPosition clusterPosition, byte recordType) {
    acquireExclusiveLock();
    try {

      content.get(clusterPosition).recordType = recordType;

    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  public void updateVersion(OClusterPosition clusterPosition, ORecordVersion version) {
    acquireExclusiveLock();
    try {

      content.get(clusterPosition).recordVersion = version;

    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  public void convertToTombstone(OClusterPosition clusterPosition) throws IOException {
    acquireExclusiveLock();
    try {

      content.get(clusterPosition).recordVersion.convertToTombstone();
      tombstonesCount++;
    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  public long getTombstonesCount() {
    return tombstonesCount;
  }

  @Override
  public boolean hasTombstonesSupport() {
    return true;
  }

  @Override
  public OModificationLock getExternalModificationLock() {
    throw new UnsupportedOperationException("getExternalModificationLock");
  }

  @Override
  public OPhysicalPosition createRecord(byte[] content, ORecordVersion recordVersion, byte recordType) throws IOException {
    throw new UnsupportedOperationException("createRecord");
  }

  @Override
  public boolean deleteRecord(OClusterPosition clusterPosition) throws IOException {
    throw new UnsupportedOperationException("deleteRecord");
  }

  @Override
  public void updateRecord(OClusterPosition clusterPosition, byte[] content, ORecordVersion recordVersion, byte recordType)
      throws IOException {
    throw new UnsupportedOperationException("updateRecord");
  }

  @Override
  public ORawBuffer readRecord(OClusterPosition clusterPosition) throws IOException {
    throw new UnsupportedOperationException("readRecord");
  }

  @Override
  public long getEntries() {
    acquireSharedLock();
    try {

      return content.size();

    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public OClusterPosition getFirstPosition() {
    acquireSharedLock();
    try {
      if (content.isEmpty())
        return OClusterPosition.INVALID_POSITION;

      final OClusterPosition clusterPosition = content.firstKey();
      if (clusterPosition == null)
        return OClusterPosition.INVALID_POSITION;

      return clusterPosition;
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public OClusterPosition getLastPosition() {
    acquireSharedLock();
    try {
      if (content.isEmpty())
        return OClusterPosition.INVALID_POSITION;

      final OClusterPosition clusterPosition = content.lastKey();
      if (clusterPosition == null)
        return OClusterPosition.INVALID_POSITION;

      return clusterPosition;
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public long getRecordsSize() {
    // TODO implement in future
    return 0; // TODO realization missed!
  }

  @Override
  public boolean isHashBased() {
    return true;
  }

  @Override
  public OClusterEntryIterator absoluteIterator() {
    return new OClusterEntryIterator(this);
  }

  @Override
  protected void clear() {
    content.clear();
  }

  @Override
  public OPhysicalPosition[] higherPositions(OPhysicalPosition position) {
    acquireSharedLock();
    try {
      Map.Entry<OClusterPosition, OPhysicalPosition> entry = content.higherEntry(position.clusterPosition);
      if (entry != null) {
        return new OPhysicalPosition[] { entry.getValue() };
      } else {
        return new OPhysicalPosition[0];
      }
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public OPhysicalPosition[] ceilingPositions(OPhysicalPosition position) throws IOException {
    acquireSharedLock();
    try {
      Map.Entry<OClusterPosition, OPhysicalPosition> entry = content.ceilingEntry(position.clusterPosition);
      if (entry != null) {
        return new OPhysicalPosition[] { entry.getValue() };
      } else {
        return new OPhysicalPosition[0];
      }
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public OPhysicalPosition[] lowerPositions(OPhysicalPosition position) {
    acquireSharedLock();
    try {
      Map.Entry<OClusterPosition, OPhysicalPosition> entry = content.lowerEntry(position.clusterPosition);
      if (entry != null) {
        return new OPhysicalPosition[] { entry.getValue() };
      } else {
        return new OPhysicalPosition[0];
      }
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public OPhysicalPosition[] floorPositions(OPhysicalPosition position) throws IOException {
    acquireSharedLock();
    try {
      Map.Entry<OClusterPosition, OPhysicalPosition> entry = content.floorEntry(position.clusterPosition);
      if (entry != null) {
        return new OPhysicalPosition[] { entry.getValue() };
      } else {
        return new OPhysicalPosition[0];
      }
    } finally {
      releaseSharedLock();
    }
  }
}
