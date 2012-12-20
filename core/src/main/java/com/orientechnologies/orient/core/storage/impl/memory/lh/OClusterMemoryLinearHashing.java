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
package com.orientechnologies.orient.core.storage.impl.memory.lh;

import java.io.IOException;

import com.orientechnologies.orient.core.id.OClusterPosition;
import com.orientechnologies.orient.core.id.OClusterPositionFactory;
import com.orientechnologies.orient.core.storage.OCluster;
import com.orientechnologies.orient.core.storage.OClusterEntryIterator;
import com.orientechnologies.orient.core.storage.OPhysicalPosition;
import com.orientechnologies.orient.core.storage.impl.memory.OClusterMemory;
import com.orientechnologies.orient.core.version.ORecordVersion;

/**
 * @author Artem Loginov (logart2007@gmail.com)
 */
public class OClusterMemoryLinearHashing extends OClusterMemory implements OCluster {

  public static final String                                       TYPE            = "MEMORY";

  private OLinearHashingTable<OClusterPosition, OPhysicalPosition> content         = new OLinearHashingTable<OClusterPosition, OPhysicalPosition>();

  private long                                                     tombstonesCount = 0;

  @Override
  public boolean addPhysicalPosition(OPhysicalPosition physicalPosition) {
    acquireExclusiveLock();
    try {
      return content.put(physicalPosition);
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

      final OPhysicalPosition physicalPosition = content.delete(clusterPosition);
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
      OLinearHashingTable.Entry<OClusterPosition, OPhysicalPosition>[] entries = content
          .higherEntries(OClusterPositionFactory.INSTANCE.valueOf(-1));

      if (entries.length == 0)
        return OClusterPosition.INVALID_POSITION;
      else
        return entries[0].key;
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public OClusterPosition getLastPosition() {
    acquireSharedLock();
    try {
      OLinearHashingTable.Entry<OClusterPosition, OPhysicalPosition>[] entries = content
          .floorEntries(OClusterPositionFactory.INSTANCE.getMaxValue());
      if (entries.length == 0)
        return OClusterPosition.INVALID_POSITION;
      else
        return entries[entries.length - 1].key;
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
  public boolean isLHBased() {
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
      OLinearHashingTable.Entry<OClusterPosition, OPhysicalPosition>[] entries = content.higherEntries(position.clusterPosition);
      if (entries.length > 0) {
        final OPhysicalPosition[] result = new OPhysicalPosition[entries.length];
        for (int i = 0; i < result.length; i++)
          result[i] = entries[i].value;

        return result;
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
      OLinearHashingTable.Entry<OClusterPosition, OPhysicalPosition>[] entries = content.ceilingEntries(position.clusterPosition);
      if (entries.length > 0) {
        final OPhysicalPosition[] result = new OPhysicalPosition[entries.length];
        for (int i = 0; i < result.length; i++)
          result[i] = entries[i].value;

        return result;
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
      OLinearHashingTable.Entry<OClusterPosition, OPhysicalPosition>[] entries = content.lowerEntries(position.clusterPosition);

      if (entries.length > 0) {
        final OPhysicalPosition[] result = new OPhysicalPosition[entries.length];
        for (int i = 0; i < result.length; i++)
          result[i] = entries[i].value;

        return result;
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
      OLinearHashingTable.Entry<OClusterPosition, OPhysicalPosition>[] entries = content.floorEntries(position.clusterPosition);

      if (entries.length > 0) {
        final OPhysicalPosition[] result = new OPhysicalPosition[entries.length];
        for (int i = 0; i < result.length; i++)
          result[i] = entries[i].value;

        return result;
      } else {
        return new OPhysicalPosition[0];
      }
    } finally {
      releaseSharedLock();
    }
  }
}
