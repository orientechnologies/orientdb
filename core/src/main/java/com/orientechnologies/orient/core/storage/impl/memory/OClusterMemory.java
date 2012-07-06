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

import com.orientechnologies.common.concur.resource.OSharedResourceAdaptive;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.config.OStorageClusterConfiguration;
import com.orientechnologies.orient.core.storage.OCluster;
import com.orientechnologies.orient.core.storage.OClusterPositionIterator;
import com.orientechnologies.orient.core.storage.OPhysicalPosition;
import com.orientechnologies.orient.core.storage.OStorage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class OClusterMemory extends OSharedResourceAdaptive implements OCluster {
  public static final String      TYPE    = "MEMORY";

  private OStorage                storage;
  private int                     id;
  private String                  name;
  private int                     dataSegmentId;
  private List<OPhysicalPosition> entries = new ArrayList<OPhysicalPosition>();
  private List<OPhysicalPosition> removed = new ArrayList<OPhysicalPosition>();

  public OClusterMemory() {
    super(OGlobalConfiguration.ENVIRONMENT_CONCURRENT.getValueAsBoolean());
  }

  public void configure(final OStorage iStorage, final OStorageClusterConfiguration iConfig) throws IOException {
    configure(iStorage, iConfig.getId(), iConfig.getName(), iConfig.getLocation(), iConfig.getDataSegmentId());
  }

  public void configure(final OStorage iStorage, final int iId, final String iClusterName, final String iLocation,
      final int iDataSegmentId, final Object... iParameters) {
    this.storage = iStorage;
    this.id = iId;
    this.name = iClusterName;
    this.dataSegmentId = iDataSegmentId;
  }

  public int getDataSegmentId() {
    acquireSharedLock();
    try {

      return dataSegmentId;

    } finally {
      releaseSharedLock();
    }
  }

  public OClusterPositionIterator absoluteIterator() {
    return new OClusterPositionIterator(this);
  }

  public OClusterPositionIterator absoluteIterator(final long iBeginRange, final long iEndRange) throws IOException {
    return new OClusterPositionIterator(this, iBeginRange, iEndRange);
  }

  public void close() {
    acquireExclusiveLock();
    try {

      entries.clear();
      removed.clear();

    } finally {
      releaseExclusiveLock();
    }
  }

  public void open() throws IOException {
  }

  public void create(final int iStartSize) throws IOException {
  }

  public void delete() throws IOException {
    acquireExclusiveLock();
    try {

      close();
      entries.clear();

    } finally {
      releaseExclusiveLock();
    }
  }

  public void truncate() throws IOException {
    acquireExclusiveLock();
    try {

      entries.clear();
      removed.clear();

    } finally {
      releaseExclusiveLock();
    }
  }

  public void set(ATTRIBUTES iAttribute, Object iValue) throws IOException {
    if (iAttribute == null)
      throw new IllegalArgumentException("attribute is null");

    final String stringValue = iValue != null ? iValue.toString() : null;

    switch (iAttribute) {
    case NAME:
      name = stringValue;
      break;

    case DATASEGMENT:
      dataSegmentId = storage.getDataSegmentIdByName(stringValue);
      break;
    }
  }

  public long getEntries() {
    acquireSharedLock();
    try {

      return entries.size() - removed.size();

    } finally {
      releaseSharedLock();
    }
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

  public long getFirstEntryPosition() {
    acquireSharedLock();
    try {

      return entries.size() == 0 ? -1 : 0;

    } finally {
      releaseSharedLock();
    }
  }

  public long getLastEntryPosition() {
    acquireSharedLock();
    try {

      return entries.size() - 1;

    } finally {
      releaseSharedLock();
    }
  }

  public int getId() {
    return id;
  }

  public String getName() {
    return name;
  }

  public long getAvailablePosition() throws IOException {
    acquireSharedLock();
    try {

      return entries.size();

    } finally {
      releaseSharedLock();
    }
  }

  public void addPhysicalPosition(final OPhysicalPosition iPPosition) {
    acquireExclusiveLock();
    try {

      if (!removed.isEmpty()) {
        final OPhysicalPosition recycledPosition = removed.remove(removed.size() - 1);

        // OVERWRITE DATA
        iPPosition.clusterPosition = recycledPosition.clusterPosition;
        iPPosition.recordVersion = recycledPosition.recordVersion + 1;

        entries.set((int) recycledPosition.clusterPosition, iPPosition);

      } else {
        iPPosition.clusterPosition = allocateRecord(iPPosition);
        iPPosition.recordVersion = 0;
        entries.add(iPPosition);
      }

    } finally {
      releaseExclusiveLock();
    }
  }

  protected long allocateRecord(final OPhysicalPosition iPPosition) {
    return entries.size();
  }

  public void updateRecordType(final long iPosition, final byte iRecordType) throws IOException {
    acquireExclusiveLock();
    try {

      entries.get((int) iPosition).recordType = iRecordType;

    } finally {
      releaseExclusiveLock();
    }
  }

  public void updateVersion(long iPosition, int iVersion) throws IOException {
    acquireExclusiveLock();
    try {

      entries.get((int) iPosition).recordVersion = iVersion;

    } finally {
      releaseExclusiveLock();
    }
  }

  public OPhysicalPosition getPhysicalPosition(final OPhysicalPosition iPPosition) {
    acquireSharedLock();
    try {

      return entries.get((int) iPPosition.clusterPosition);

    } finally {
      releaseSharedLock();
    }
  }

  public void removePhysicalPosition(final long iPosition) {
    acquireExclusiveLock();
    try {

      final OPhysicalPosition ppos = entries.get((int) iPosition);

      // ADD AS HOLE
      removed.add(ppos);

      entries.set((int) iPosition, null);

    } finally {
      releaseExclusiveLock();
    }
  }

  public void updateDataSegmentPosition(final long iPosition, final int iDataSegmentId, final long iDataPosition) {
    acquireExclusiveLock();
    try {

      final OPhysicalPosition ppos = entries.get((int) iPosition);
      ppos.dataSegmentId = iDataSegmentId;
      ppos.dataSegmentPos = iDataPosition;

    } finally {
      releaseExclusiveLock();
    }
  }

  public void synch() {
  }

	public void setSoftlyClosed(boolean softlyClosed) throws IOException {
	}

	public void lock() {
    acquireSharedLock();
  }

  public void unlock() {
    releaseSharedLock();
  }

  public String getType() {
    return TYPE;
  }

  @Override
  public String toString() {
    return "OClusterMemory [name=" + name + ", id=" + id + ", entries=" + entries.size() + ", removed=" + removed + "]";
  }
}
