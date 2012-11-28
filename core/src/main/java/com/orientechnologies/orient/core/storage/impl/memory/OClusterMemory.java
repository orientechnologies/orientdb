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

import com.orientechnologies.common.concur.resource.OSharedResourceAdaptive;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.config.OStorageClusterConfiguration;
import com.orientechnologies.orient.core.id.OClusterPosition;
import com.orientechnologies.orient.core.id.OClusterPositionFactory;
import com.orientechnologies.orient.core.storage.OCluster;
import com.orientechnologies.orient.core.storage.OClusterEntryIterator;
import com.orientechnologies.orient.core.storage.OPhysicalPosition;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.version.ORecordVersion;

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

  public OClusterEntryIterator absoluteIterator() {
    return new OClusterEntryIterator(this);
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
    storage.checkForClusterPermissions(getName());

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

  public boolean addPhysicalPosition(final OPhysicalPosition iPPosition) {
    acquireExclusiveLock();
    try {

      if (!removed.isEmpty()) {
        final OPhysicalPosition recycledPosition = removed.remove(removed.size() - 1);

        // OVERWRITE DATA
        iPPosition.clusterPosition = recycledPosition.clusterPosition;
        iPPosition.recordVersion.increment();

        entries.set(recycledPosition.clusterPosition.intValue(), iPPosition);

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
      if (iPPosition.clusterPosition.intValue() < 0 || iPPosition.clusterPosition.intValue() > getLastEntryPosition())
        return null;

      return entries.get((int) iPPosition.clusterPosition.intValue());

    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public OPhysicalPosition[] getPositionsByEntryPos(long entryPosition) throws IOException {
    OPhysicalPosition ppos = getPhysicalPosition(new OPhysicalPosition(OClusterPositionFactory.INSTANCE.valueOf(entryPosition)));
    if (ppos == null)
      return new OPhysicalPosition[0];

    return new OPhysicalPosition[] { ppos };
  }

  public void removePhysicalPosition(final OClusterPosition iPosition) {
    acquireExclusiveLock();
    try {

      final OPhysicalPosition ppos = entries.get(iPosition.intValue());

      // ADD AS HOLE
      removed.add(ppos);

      entries.set(iPosition.intValue(), null);

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
