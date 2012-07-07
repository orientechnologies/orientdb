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
package com.orientechnologies.orient.core.storage.impl.local;

import java.io.File;
import java.io.IOException;

import com.orientechnologies.common.concur.resource.OSharedResourceAdaptive;
import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.io.OIOException;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.config.OStorageClusterConfiguration;
import com.orientechnologies.orient.core.config.OStorageClusterHoleConfiguration;
import com.orientechnologies.orient.core.config.OStorageFileConfiguration;
import com.orientechnologies.orient.core.config.OStoragePhysicalClusterConfiguration;
import com.orientechnologies.orient.core.memory.OMemoryWatchDog;
import com.orientechnologies.orient.core.serialization.OBinaryProtocol;
import com.orientechnologies.orient.core.storage.OCluster;
import com.orientechnologies.orient.core.storage.OClusterPositionIterator;
import com.orientechnologies.orient.core.storage.OPhysicalPosition;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.fs.OFile;

/**
 * Handles the table to resolve logical address to physical address. Deleted records have negative versions. <br/>
 * <br/>
 * Record structure:<br/>
 * <code>
 * +----------------------+----------------------+-------------+----------------------+<br/>
 * | DATA SEGMENT........ | DATA OFFSET......... | RECORD TYPE | VERSION............. |<br/>
 * | 2 bytes = max 2^15-1 | 8 bytes = max 2^63-1 | 1 byte..... | 4 bytes = max 2^31-1 |<br/>
 * +----------------------+----------------------+-------------+----------------------+<br/>
 * = 15 bytes
 * </code><br/>
 */
public class OClusterLocal extends OSharedResourceAdaptive implements OCluster {
  public static final int                      RECORD_SIZE     = 15;
  public static final String                   TYPE            = "PHYSICAL";
  private static final String                  DEF_EXTENSION   = ".ocl";
  private static final int                     DEF_SIZE        = 1000000;
  private OMultiFileSegment                    fileSegment;

  private int                                  id;
  private long                                 beginOffsetData = -1;
  private long                                 endOffsetData   = -1;        // end of data offset. -1 = latest

  protected OClusterLocalHole                  holeSegment;
  private OStoragePhysicalClusterConfiguration config;
  private OStorageLocal                        storage;
  private String                               name;

  public OClusterLocal() {
    super(OGlobalConfiguration.ENVIRONMENT_CONCURRENT.getValueAsBoolean());
  }

  public void configure(final OStorage iStorage, OStorageClusterConfiguration iConfig) throws IOException {
    config = (OStoragePhysicalClusterConfiguration) iConfig;
    init(iStorage, config.getId(), config.getName(), config.getLocation(), config.getDataSegmentId());
  }

  public void configure(final OStorage iStorage, final int iId, final String iClusterName, final String iLocation,
      final int iDataSegmentId, final Object... iParameters) throws IOException {
    config = new OStoragePhysicalClusterConfiguration(iStorage.getConfiguration(), iId, iDataSegmentId);
    config.name = iClusterName;
    init(iStorage, iId, iClusterName, iLocation, iDataSegmentId);
  }

  public void create(int iStartSize) throws IOException {
    acquireExclusiveLock();
    try {

      if (iStartSize == -1)
        iStartSize = DEF_SIZE;

      if (config.root.clusters.size() <= config.id)
        config.root.clusters.add(config);
      else
        config.root.clusters.set(config.id, config);

      fileSegment.create(iStartSize);
      holeSegment.create();

      fileSegment.files[0].writeHeaderLong(0, beginOffsetData);
      fileSegment.files[0].writeHeaderLong(OBinaryProtocol.SIZE_LONG, beginOffsetData);

    } finally {
      releaseExclusiveLock();
    }
  }

  public void open() throws IOException {
    acquireExclusiveLock();
    try {

      fileSegment.open();
      holeSegment.open();

      beginOffsetData = fileSegment.files[0].readHeaderLong(0);
      endOffsetData = fileSegment.files[0].readHeaderLong(OBinaryProtocol.SIZE_LONG);

    } finally {
      releaseExclusiveLock();
    }
  }

  public void close() throws IOException {
    acquireExclusiveLock();
    try {

      fileSegment.close();
      holeSegment.close();

    } finally {
      releaseExclusiveLock();
    }
  }

  public void delete() throws IOException {
    acquireExclusiveLock();
    try {

      truncate();
      for (OFile f : fileSegment.files)
        f.delete();

      fileSegment.files = null;
      holeSegment.delete();

    } finally {
      releaseExclusiveLock();
    }
  }

  public void truncate() throws IOException {
    acquireExclusiveLock();
    try {

      // REMOVE ALL DATA BLOCKS
      final long begin = getFirstEntryPosition();
      if (begin > -1) {
        final long end = getLastEntryPosition();
        final OPhysicalPosition ppos = new OPhysicalPosition();
        for (ppos.clusterPosition = begin; ppos.clusterPosition <= end; ++ppos.clusterPosition) {
          getPhysicalPosition(ppos);

          if (storage.checkForRecordValidity(ppos))
            storage.getDataSegmentById(ppos.dataSegmentId).deleteRecord(ppos.dataSegmentPos);
        }
      }

      fileSegment.truncate();
      holeSegment.truncate();

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

  /**
   * Fills and return the PhysicalPosition object received as parameter with the physical position of logical record iPosition
   * 
   * @throws IOException
   */
  public OPhysicalPosition getPhysicalPosition(final OPhysicalPosition iPPosition) throws IOException {
    final long filePosition = iPPosition.clusterPosition * RECORD_SIZE;

    acquireSharedLock();
    try {

      final long[] pos = fileSegment.getRelativePosition(filePosition);

      final OFile f = fileSegment.files[(int) pos[0]];
      long p = pos[1];

      iPPosition.dataSegmentId = f.readShort(p);
      iPPosition.dataSegmentPos = f.readLong(p += OBinaryProtocol.SIZE_SHORT);
      iPPosition.recordType = f.readByte(p += OBinaryProtocol.SIZE_LONG);
      iPPosition.recordVersion = f.readInt(p += OBinaryProtocol.SIZE_BYTE);
      return iPPosition;

    } finally {
      releaseSharedLock();
    }
  }

  /**
   * Update position in data segment (usually on defrag)
   * 
   * @throws IOException
   */
  public void updateDataSegmentPosition(long iPosition, final int iDataSegmentId, final long iDataSegmentPosition)
      throws IOException {
    iPosition = iPosition * RECORD_SIZE;

    acquireExclusiveLock();
    try {

      final long[] pos = fileSegment.getRelativePosition(iPosition);

      final OFile f = fileSegment.files[(int) pos[0]];
      long p = pos[1];

      f.writeShort(p, (short) iDataSegmentId);
      f.writeLong(p += OBinaryProtocol.SIZE_SHORT, iDataSegmentPosition);

    } finally {
      releaseExclusiveLock();
    }
  }

  public void updateVersion(long iPosition, final int iVersion) throws IOException {
    iPosition = iPosition * RECORD_SIZE;

    acquireExclusiveLock();
    try {

      final long[] pos = fileSegment.getRelativePosition(iPosition);

      fileSegment.files[(int) pos[0]].writeInt(pos[1] + OBinaryProtocol.SIZE_SHORT + OBinaryProtocol.SIZE_LONG
          + OBinaryProtocol.SIZE_BYTE, iVersion);

    } finally {
      releaseExclusiveLock();
    }
  }

  public void updateRecordType(long iPosition, final byte iRecordType) throws IOException {
    iPosition = iPosition * RECORD_SIZE;

    acquireExclusiveLock();
    try {

      final long[] pos = fileSegment.getRelativePosition(iPosition);

      fileSegment.files[(int) pos[0]].writeByte(pos[1] + OBinaryProtocol.SIZE_SHORT + OBinaryProtocol.SIZE_LONG, iRecordType);

    } finally {
      releaseExclusiveLock();
    }
  }

  /**
   * Removes the Logical position entry. Add to the hole segment and add the minus to the version.
   * 
   * @throws IOException
   */
  public void removePhysicalPosition(final long iPosition) throws IOException {
    final long position = iPosition * RECORD_SIZE;

    acquireExclusiveLock();
    try {

      final long[] pos = fileSegment.getRelativePosition(position);
      final OFile file = fileSegment.files[(int) pos[0]];
      final long p = pos[1] + OBinaryProtocol.SIZE_SHORT + OBinaryProtocol.SIZE_LONG + OBinaryProtocol.SIZE_BYTE;

      holeSegment.pushPosition(position);

      // MARK DELETED SETTING VERSION TO NEGATIVE NUMBER
      final int version = file.readInt(p);
      file.writeInt(p, (version + 1) * -1);

      updateBoundsAfterDeletion(iPosition);

    } finally {
      releaseExclusiveLock();
    }
  }

  public boolean removeHole(final long iPosition) throws IOException {
    acquireExclusiveLock();
    try {

      return holeSegment.removeEntryWithPosition(iPosition * RECORD_SIZE);

    } finally {
      releaseExclusiveLock();
    }
  }

  public int getDataSegmentId() {
    acquireSharedLock();
    try {

      return config.getDataSegmentId();

    } finally {
      releaseSharedLock();
    }
  }

  /**
   * Adds a new entry.
   * 
   * @throws IOException
   */
  public void addPhysicalPosition(final OPhysicalPosition iPPosition) throws IOException {
    final long[] pos;
    final boolean recycled;
    long offset;

    acquireExclusiveLock();
    try {

      offset = holeSegment.popLastEntryPosition();
      if (offset > -1) {
        // REUSE THE HOLE
        pos = fileSegment.getRelativePosition(offset);
        recycled = true;
      } else {
        // NO HOLES FOUND: ALLOCATE MORE SPACE
        pos = allocateRecord();
        offset = fileSegment.getAbsolutePosition(pos);
        recycled = false;
      }

      final OFile file = fileSegment.files[(int) pos[0]];
      long p = pos[1];

      file.writeShort(p, (short) iPPosition.dataSegmentId);
      file.writeLong(p += OBinaryProtocol.SIZE_SHORT, iPPosition.dataSegmentPos);
      file.writeByte(p += OBinaryProtocol.SIZE_LONG, iPPosition.recordType);

      if (recycled)
        // GET LAST VERSION
        iPPosition.recordVersion = file.readInt(p + OBinaryProtocol.SIZE_BYTE) * -1;
      else
        iPPosition.recordVersion = 0;

      file.writeInt(p + OBinaryProtocol.SIZE_BYTE, iPPosition.recordVersion);

      iPPosition.clusterPosition = offset / RECORD_SIZE;

      updateBoundsAfterInsertion(iPPosition.clusterPosition);

    } finally {
      releaseExclusiveLock();
    }
  }

  /**
   * Allocates space to store a new record.
   */
  protected long[] allocateRecord() throws IOException {
    return fileSegment.allocateSpace(RECORD_SIZE);
  }

  public long getFirstEntryPosition() {
    acquireSharedLock();
    try {

      return beginOffsetData;

    } finally {
      releaseSharedLock();
    }
  }

  /**
   * Returns the endOffsetData value if it's not equals to the last one, otherwise the total entries.
   */
  public long getLastEntryPosition() {
    acquireSharedLock();
    try {

      return endOffsetData > -1 ? endOffsetData : fileSegment.getFilledUpTo() / RECORD_SIZE - 1;

    } finally {
      releaseSharedLock();
    }
  }

  public long getEntries() {
    acquireSharedLock();
    try {

      return fileSegment.getFilledUpTo() / RECORD_SIZE - holeSegment.getHoles();

    } finally {
      releaseSharedLock();
    }
  }

  public int getId() {
    return id;
  }

  public OClusterPositionIterator absoluteIterator() {
    return new OClusterPositionIterator(this);
  }

  public OClusterPositionIterator absoluteIterator(final long iBeginRange, final long iEndRange) throws IOException {
    return new OClusterPositionIterator(this, iBeginRange, iEndRange);
  }

  public long getSize() {
    acquireSharedLock();
    try {

      return fileSegment.getSize();

    } finally {
      releaseSharedLock();
    }
  }

  public long getFilledUpTo() {
    acquireSharedLock();
    try {

      return fileSegment.getFilledUpTo();

    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public String toString() {
    return name + " (id=" + id + ")";
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

  public long getRecordsSize() {
    acquireSharedLock();
    try {

      long size = fileSegment.getFilledUpTo();
      final OClusterPositionIterator it = absoluteIterator();
      final OPhysicalPosition pos = new OPhysicalPosition();
      while (it.hasNext()) {
        pos.clusterPosition = it.next();
        getPhysicalPosition(pos);
        if (pos.dataSegmentPos > -1 && pos.recordVersion > -1)
          size += storage.getDataSegmentById(pos.dataSegmentId).getRecordSize(pos.dataSegmentPos);
      }

      return size;

    } catch (IOException e) {
      throw new OIOException("Error on calculating cluster size for: " + getName(), e);

    } finally {
      releaseSharedLock();
    }
  }

  public void synch() throws IOException {
    acquireSharedLock();
    try {

      fileSegment.synch();
      holeSegment.synch();

    } finally {
      releaseSharedLock();
    }
  }

  public void setSoftlyClosed(boolean softlyClosed) throws IOException {
    acquireExclusiveLock();
    try {

      fileSegment.setSoftlyClosed(softlyClosed);
      holeSegment.setSoftlyClosed(softlyClosed);

    } finally {
      releaseExclusiveLock();
    }
  }

  public String getName() {
    return name;
  }

  public OStoragePhysicalClusterConfiguration getConfig() {
    return config;
  }

  private void setNameInternal(final String iNewName) {
    if (storage.getClusterIdByName(iNewName) > -1)
      throw new IllegalArgumentException("Cluster with name '" + iNewName + "' already exists");

    for (int i = 0; i < fileSegment.files.length; i++) {
      final String osFileName = fileSegment.files[i].getName();
      if (osFileName.startsWith(name)) {
        final File newFile = new File(storage.getStoragePath() + "/" + iNewName
            + osFileName.substring(osFileName.lastIndexOf(name) + name.length()));
        for (OStorageFileConfiguration conf : config.infoFiles) {
          if (conf.parent.name.equals(name))
            conf.parent.name = iNewName;
          if (conf.path.endsWith(osFileName))
            conf.path = new String(conf.path.replace(osFileName, newFile.getName()));
        }
        boolean renamed = fileSegment.files[i].renameTo(newFile);
        while (!renamed) {
          OMemoryWatchDog.freeMemory(100);
          renamed = fileSegment.files[i].renameTo(newFile);
        }
      }
    }
    config.name = iNewName;
    holeSegment.rename(name, iNewName);
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
    final int dataId = storage.getDataSegmentIdByName(iName);
    config.setDataSegmentId(dataId);
    storage.getConfiguration().update();
  }

  protected void updateBoundsAfterInsertion(final long iPosition) throws IOException {
    if (iPosition < beginOffsetData || beginOffsetData == -1) {
      // UPDATE END OF DATA
      beginOffsetData = iPosition;
      fileSegment.files[0].writeHeaderLong(0, beginOffsetData);
    }

    if (endOffsetData > -1 && iPosition > endOffsetData) {
      // UPDATE END OF DATA
      endOffsetData = iPosition;
      fileSegment.files[0].writeHeaderLong(OBinaryProtocol.SIZE_LONG, endOffsetData);
    }
  }

  protected void updateBoundsAfterDeletion(final long iPosition) throws IOException {
    final long position = iPosition * RECORD_SIZE;

    if (iPosition == beginOffsetData) {
      if (getEntries() == 0)
        beginOffsetData = -1;
      else {
        // DISCOVER THE BEGIN OF DATA
        beginOffsetData++;

        long[] fetchPos;
        for (long currentPos = position + RECORD_SIZE; currentPos < fileSegment.getFilledUpTo(); currentPos += RECORD_SIZE) {
          fetchPos = fileSegment.getRelativePosition(currentPos);

          if (fileSegment.files[(int) fetchPos[0]].readShort(fetchPos[1]) != -1)
            // GOOD RECORD: SET IT AS BEGIN
            break;

          beginOffsetData++;
        }
      }

      fileSegment.files[0].writeHeaderLong(0, beginOffsetData);
    }

    if (iPosition == endOffsetData) {
      if (getEntries() == 0)
        endOffsetData = -1;
      else {
        // DISCOVER THE END OF DATA
        endOffsetData--;

        long[] fetchPos;
        for (long currentPos = position - RECORD_SIZE; currentPos >= beginOffsetData; currentPos -= RECORD_SIZE) {

          fetchPos = fileSegment.getRelativePosition(currentPos);

          if (fileSegment.files[(int) fetchPos[0]].readShort(fetchPos[1]) != -1)
            // GOOD RECORD: SET IT AS BEGIN
            break;
          endOffsetData--;
        }
      }

      fileSegment.files[0].writeHeaderLong(OBinaryProtocol.SIZE_LONG, endOffsetData);
    }
  }

  protected void init(final OStorage iStorage, final int iId, final String iClusterName, final String iLocation,
      final int iDataSegmentId, final Object... iParameters) throws IOException {
    OFileUtils.checkValidName(iClusterName);
    
    storage = (OStorageLocal) iStorage;
    ((OStoragePhysicalClusterConfiguration) config).setDataSegmentId(iDataSegmentId);
    config.id = iId;
    config.name = iClusterName;
    name = iClusterName;
    id = iId;

    if (fileSegment == null) {
      fileSegment = new OMultiFileSegment(storage, config, DEF_EXTENSION, RECORD_SIZE);

      config.setHoleFile(new OStorageClusterHoleConfiguration(config, OStorageVariableParser.DB_PATH_VARIABLE + "/" + config.name,
          config.fileType, config.fileMaxSize));

      holeSegment = new OClusterLocalHole(this, storage, config.getHoleFile());
    }
  }

}
