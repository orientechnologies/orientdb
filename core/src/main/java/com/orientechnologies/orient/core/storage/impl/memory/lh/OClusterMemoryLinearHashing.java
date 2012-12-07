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

import com.orientechnologies.orient.core.id.OClusterPosition;
import com.orientechnologies.orient.core.id.OClusterPositionFactory;
import com.orientechnologies.orient.core.storage.OCluster;
import com.orientechnologies.orient.core.storage.OClusterEntryIterator;
import com.orientechnologies.orient.core.storage.OPhysicalPosition;
import com.orientechnologies.orient.core.storage.impl.memory.OClusterMemory;
import com.orientechnologies.orient.core.version.ORecordVersion;

/**
 * @author Artem Loginov (artem.loginov@exigenservices.com)
 */
public class OClusterMemoryLinearHashing extends OClusterMemory implements OCluster {

  public static final String                                       TYPE    = "MEMORY";

  private OLinearHashingTable<OClusterPosition, OPhysicalPosition> content = new OLinearHashingTable<OClusterPosition, OPhysicalPosition>();

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

      content.delete(clusterPosition);

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
      OClusterPosition clusterPosition = content.nextRecord(OClusterPositionFactory.INSTANCE.valueOf(-1));
      return clusterPosition == null ? OClusterPosition.INVALID_POSITION : clusterPosition;
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public OClusterPosition getLastPosition() {
    acquireSharedLock();
    try {
      // TODO remake this with relation to point that max value can be stored to DB
      assert !content.contains(OClusterPositionFactory.INSTANCE.getMaxValue());
      OClusterPosition clusterPosition = content.prevRecord(OClusterPositionFactory.INSTANCE.getMaxValue());
      return clusterPosition == null ? OClusterPosition.INVALID_POSITION : clusterPosition;
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
  public boolean isRequiresValidPositionBeforeCreation() {
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
  public OClusterPosition nextRecord(OClusterPosition position) {
    acquireSharedLock();
    try {
      OClusterPosition clusterPosition = content.nextRecord(position);
      if (clusterPosition.isPersistent()) {
        return clusterPosition;
      } else {
        return null;
      }
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public OClusterPosition prevRecord(OClusterPosition position) {
    acquireSharedLock();
    try {
      OClusterPosition clusterPosition = content.prevRecord(position);
      if (clusterPosition.isPersistent()) {
        return clusterPosition;
      } else {
        return null;
      }
    } finally {
      releaseSharedLock();
    }
  }

}
