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
import java.util.ArrayList;
import java.util.List;

import com.orientechnologies.orient.core.id.OClusterPosition;
import com.orientechnologies.orient.core.id.OClusterPositionFactory;
import com.orientechnologies.orient.core.storage.OCluster;
import com.orientechnologies.orient.core.storage.OPhysicalPosition;
import com.orientechnologies.orient.core.version.ORecordVersion;

public class OClusterMemoryArrayList extends OClusterMemory implements OCluster {

  private List<OPhysicalPosition> entries           = new ArrayList<OPhysicalPosition>();
  private List<OPhysicalPosition> removed           = new ArrayList<OPhysicalPosition>();

  protected void clear() {
    entries.clear();
    removed.clear();
  }

  public long getEntries() {
    acquireSharedLock();
    try {

      return entries.size() - removed.size();

    } finally {
      releaseSharedLock();
    }
  }

  public boolean generatePositionBeforeCreation() {
    return false;
  }

  public long getRecordsSize() {
    acquireSharedLock();
    try {

      long size = 0;
      for (OPhysicalPosition e : entries)
        if (e != null)
          size += e.recordSize;
      return size;

    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public OClusterPosition getFirstIdentity() {
    acquireSharedLock();
    try {

      return OClusterPositionFactory.INSTANCE.valueOf(entries.size() == 0 ? -1 : 0);

    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public OClusterPosition getLastIdentity() {
    acquireSharedLock();
    try {
      return OClusterPositionFactory.INSTANCE.valueOf(entries.size() - 1);
    } finally {
      releaseSharedLock();
    }
  }

  public boolean addPhysicalPosition(final OPhysicalPosition iPPosition) {
    acquireExclusiveLock();
    try {

      if (!removed.isEmpty()) {
        final OPhysicalPosition recycledPosition = removed.remove(removed.size() - 1);

        // OVERWRITE DATA
        iPPosition.clusterPosition = recycledPosition.clusterPosition;
        iPPosition.recordVersion = recycledPosition.recordVersion.copy();
        iPPosition.recordVersion.increment();

        int positionToStore = recycledPosition.clusterPosition.intValue();

        entries.set(positionToStore, iPPosition);

      } else {
        iPPosition.clusterPosition = allocateRecord(iPPosition);
        iPPosition.recordVersion.reset();
        entries.add(iPPosition);
      }

    } finally {
      releaseExclusiveLock();
    }

    return true;
  }

  protected OClusterPosition allocateRecord(final OPhysicalPosition iPPosition) {
    return OClusterPositionFactory.INSTANCE.valueOf(entries.size());
  }

  public void updateRecordType(final OClusterPosition iPosition, final byte iRecordType) throws IOException {
    acquireExclusiveLock();
    try {

      entries.get(iPosition.intValue()).recordType = iRecordType;

    } finally {
      releaseExclusiveLock();
    }
  }

  public void updateVersion(OClusterPosition iPosition, ORecordVersion iVersion) throws IOException {
    acquireExclusiveLock();
    try {

      entries.get(iPosition.intValue()).recordVersion = iVersion;

    } finally {
      releaseExclusiveLock();
    }
  }

  public OPhysicalPosition getPhysicalPosition(final OPhysicalPosition iPPosition) {
    acquireSharedLock();
    try {
      if (iPPosition.clusterPosition.intValue() < 0 || iPPosition.clusterPosition.compareTo(getLastIdentity()) > 0)
        return null;

      return entries.get((int) iPPosition.clusterPosition.intValue());

    } finally {
      releaseSharedLock();
    }
  }

  public void removePhysicalPosition(final OClusterPosition iPosition) {
    acquireExclusiveLock();
    try {

      int positionToRemove = iPosition.intValue();
      final OPhysicalPosition ppos = entries.get(positionToRemove);

      // ADD AS HOLE
      removed.add(ppos);

      entries.set(positionToRemove, null);

    } finally {
      releaseExclusiveLock();
    }
  }

  public void updateDataSegmentPosition(final OClusterPosition iPosition, final int iDataSegmentId, final long iDataPosition) {
    acquireExclusiveLock();
    try {

      final OPhysicalPosition ppos = entries.get(iPosition.intValue());
      ppos.dataSegmentId = iDataSegmentId;
      ppos.dataSegmentPos = iDataPosition;

    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  public OClusterPosition nextRecord(OClusterPosition position) {
    int positionInEntries = position.intValue() + 1;
    while (positionInEntries < entries.size() && entries.get(positionInEntries) == null) {
      positionInEntries++;
    }

    if (positionInEntries >= 0 && positionInEntries < entries.size()) {
      return entries.get(positionInEntries).clusterPosition;
    } else {
      return OClusterPosition.INVALID_POSITION;
    }
  }

  @Override
  public OClusterPosition prevRecord(OClusterPosition position) {
    int positionInEntries = position.intValue() - 1;
    while (positionInEntries >= 0 && entries.get(positionInEntries) == null) {
      positionInEntries--;
    }
    if (positionInEntries >= 0 && positionInEntries < entries.size()) {
      return entries.get(positionInEntries).clusterPosition;
    } else {
      return OClusterPosition.INVALID_POSITION;
    }
  }

  @Override
  public OClusterPosition nextTombstone(OClusterPosition position) {
    throw new UnsupportedOperationException("memory cluster does not support tombstones");
  }

  @Override
  public OClusterPosition prevTombstone(OClusterPosition position) {
    throw new UnsupportedOperationException("memory cluster does not support tombstones");
  }

  @Override
  public String toString() {
    return "OClusterMemory [name=" + getName() + ", id=" + getId() + ", entries=" + entries.size() + ", removed=" + removed + "]";
  }
}
