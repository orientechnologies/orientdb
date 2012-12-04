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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.orientechnologies.common.concur.resource.OSharedResourceAdaptiveExternal;
import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.config.OStorageDataConfiguration;
import com.orientechnologies.orient.core.config.OStorageDataHoleConfiguration;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.id.OClusterPosition;
import com.orientechnologies.orient.core.id.OClusterPositionFactory;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.serialization.OBinaryProtocol;
import com.orientechnologies.orient.core.storage.OCluster;
import com.orientechnologies.orient.core.storage.ODataSegment;
import com.orientechnologies.orient.core.storage.OPhysicalPosition;
import com.orientechnologies.orient.core.storage.fs.OFile;
import com.orientechnologies.orient.core.version.OVersionFactory;

/**
 * Handle the table to resolve logical address to physical address.<br/>
 * <br/>
 * Record structure:<br/>
 * <br/>
 * +--------------+--------------+--------------+----------------------+<br/>
 * | CONTENT SIZE | CLUSTER ID . | CLUSTER POS. | CONTENT ............ |<br/>
 * | 4 bytes .... | 2 bytes .... | 8 or 192 | <RECORD SIZE> bytes. |<br/>
 * +--------------+--------------+--------------+----------------------+<br/>
 * = 14+? bytes<br/>
 */
public class ODataLocal extends OMultiFileSegment implements ODataSegment {
  static final String                           DEF_EXTENSION    = ".oda";
  private static final int                      CLUSTER_POS_SIZE = OClusterPositionFactory.INSTANCE.getSerializedSize();
  public static final int                       RECORD_FIX_SIZE  = 2 + CLUSTER_POS_SIZE
                                                                     + OVersionFactory.instance().getVersionSize();

  protected final int                           id;
  protected final ODataLocalHole                holeSegment;
  protected int                                 defragMaxHoleDistance;
  protected int                                 defragStrategy;
  protected long                                defStartSize;

  private final String                          PROFILER_HOLE_FIND_CLOSER;
  private final String                          PROFILER_UPDATE_REUSED_ALL;
  private final String                          PROFILER_UPDATE_REUSED_PARTIAL;
  private final String                          PROFILER_UPDATE_NOT_REUSED;
  private final String                          PROFILER_MOVE_RECORD;
  private final String                          PROFILER_HOLE_CREATE;
  private final String                          PROFILER_DEFRAG;
  private final OSharedResourceAdaptiveExternal lock             = new OSharedResourceAdaptiveExternal(
                                                                     OGlobalConfiguration.ENVIRONMENT_CONCURRENT
                                                                         .getValueAsBoolean(),
                                                                     0, true);

  public ODataLocal(final OStorageLocal iStorage, final OStorageDataConfiguration iConfig, final int iId) throws IOException {
    super(iStorage, iConfig, DEF_EXTENSION, 0);
    id = iId;

    OFileUtils.checkValidName(iConfig.name);

    iConfig.holeFile = new OStorageDataHoleConfiguration(iConfig, iConfig.getLocation() + "/" + name, iConfig.fileType,
        iConfig.maxSize);
    holeSegment = new ODataLocalHole(iStorage, iConfig.holeFile);

    defStartSize = OFileUtils.getSizeAsNumber(iConfig.fileStartSize);
    defragMaxHoleDistance = OGlobalConfiguration.FILE_DEFRAG_HOLE_MAX_DISTANCE.getValueAsInteger();
    defragStrategy = OGlobalConfiguration.FILE_DEFRAG_STRATEGY.getValueAsInteger();

    PROFILER_HOLE_CREATE = "db." + storage.getName() + ".data.createHole";
    PROFILER_HOLE_FIND_CLOSER = "db." + storage.getName() + ".data.findClosestHole";
    PROFILER_UPDATE_REUSED_ALL = "db." + storage.getName() + ".data.update.reusedAll";
    PROFILER_UPDATE_REUSED_PARTIAL = "db." + storage.getName() + ".data.update.reusedPartial";
    PROFILER_UPDATE_NOT_REUSED = "db." + storage.getName() + ".data.update.notReused";
    PROFILER_DEFRAG = "db." + storage.getName() + ".data.defrag";
    PROFILER_MOVE_RECORD = "db." + storage.getName() + ".data.move";
  }

  @Override
  public void open() throws IOException {
    acquireExclusiveLock();
    try {

      super.open();
      holeSegment.open();

    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  public void create(final int iStartSize) throws IOException {
    acquireExclusiveLock();
    try {

      super.create((int) (iStartSize > -1 ? iStartSize : defStartSize));
      holeSegment.create(-1);

    } finally {
      releaseExclusiveLock();
    }
  }

  public void drop() throws IOException {
    acquireExclusiveLock();
    try {

      close();
      super.delete();
      holeSegment.delete();

    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  public void close() throws IOException {
    acquireExclusiveLock();
    try {

      super.close();
      holeSegment.close();

    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  public void synch() throws IOException {
    acquireSharedLock();
    try {

      holeSegment.synch();
      super.synch();

    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public void setSoftlyClosed(boolean softlyClosed) throws IOException {
    acquireExclusiveLock();
    try {

      holeSegment.setSoftlyClosed(softlyClosed);
      super.setSoftlyClosed(softlyClosed);

    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  public long getSize() {
    acquireSharedLock();
    try {

      return super.getFilledUpTo();

    } finally {
      releaseSharedLock();
    }
  }

  /**
   * Add the record content in file.
   * 
   * @param iContent
   *          The content to write
   * @return The record offset.
   * @throws IOException
   */
  public long addRecord(final ORecordId iRid, final byte[] iContent) throws IOException {

    if (iContent.length == 0)
      // AVOID UNUSEFUL CREATION OF EMPTY RECORD: IT WILL BE CREATED AT FIRST UPDATE
      return -1;

    final int recordSize = iContent.length + RECORD_FIX_SIZE;

    acquireExclusiveLock();
    try {

      final long[] newFilePosition = getFreeSpace(recordSize);
      writeRecord(newFilePosition, iRid.clusterId, iRid.clusterPosition, iContent);
      return getAbsolutePosition(newFilePosition);

    } finally {
      releaseExclusiveLock();
    }
  }

  /**
   * Returns the record content from file.
   * 
   * @throws IOException
   */
  public byte[] getRecord(final long iPosition) throws IOException {
    if (iPosition == -1)
      return null;

    acquireSharedLock();
    try {

      final long[] pos = getRelativePosition(iPosition);
      final OFile file = files[(int) pos[0]];

      final int recordSize = file.readInt(pos[1]);
      if (recordSize <= 0)
        // RECORD DELETED
        return null;

      if (pos[1] + RECORD_FIX_SIZE + recordSize > file.getFilledUpTo())
        throw new OStorageException(
            "Error on reading record from file '"
                + file.getName()
                + "', position "
                + iPosition
                + ", size "
                + OFileUtils.getSizeAsString(recordSize)
                + ": the record size is bigger then the file itself ("
                + OFileUtils.getSizeAsString(getFilledUpTo())
                + "). Probably the record is dirty due to a previous crash. It is strongly suggested to restore the database or export and reimport this one.");

      final byte[] content = new byte[recordSize];
      file.read(pos[1] + RECORD_FIX_SIZE, content, recordSize);
      return content;

    } finally {
      releaseSharedLock();
    }
  }

  /**
   * Returns the record size.
   * 
   * @throws IOException
   */
  public int getRecordSize(final long iPosition) throws IOException {
    acquireSharedLock();
    try {

      final long[] pos = getRelativePosition(iPosition);
      final OFile file = files[(int) pos[0]];

      return file.readInt(pos[1]);

    } finally {
      releaseSharedLock();
    }
  }

  public ORecordId getRecordRid(final long iPosition) throws IOException {
    acquireSharedLock();
    try {
      final long[] pos = getRelativePosition(iPosition);
      final OFile file = files[(int) pos[0]];

      final int clusterId = file.readShort(pos[1] + OBinaryProtocol.SIZE_INT);
      final byte[] clusterContent = new byte[CLUSTER_POS_SIZE];
      file.read(pos[1] + OBinaryProtocol.SIZE_INT + OBinaryProtocol.SIZE_SHORT, clusterContent, CLUSTER_POS_SIZE);

      return new ORecordId(clusterId, OClusterPositionFactory.INSTANCE.fromStream(clusterContent));
    } finally {
      releaseSharedLock();
    }
  }

  public void setRecordRid(final long iPosition, final ORID rid) throws IOException {
    if (iPosition < 0)
      return;

    acquireExclusiveLock();
    try {
      final long[] fpos = getRelativePosition(iPosition);
      final OFile file = files[(int) fpos[0]];
      long pos = fpos[1] + OBinaryProtocol.SIZE_INT;

      file.writeShort(pos, (short) rid.getClusterId());
      pos += OBinaryProtocol.SIZE_SHORT;

      file.write(pos, rid.getClusterPosition().toStream());
    } finally {
      releaseExclusiveLock();
    }
  }

  /**
   * Set the record content in file.
   * 
   * @param iPosition
   *          The previous record's offset
   * @param iContent
   *          The content to write
   * @return The new record offset or the same received as parameter is the old space was reused.
   * @throws IOException
   */
  public long setRecord(final long iPosition, final ORecordId iRid, final byte[] iContent) throws IOException {
    acquireExclusiveLock();
    try {

      long[] pos = getRelativePosition(iPosition);
      final OFile file = files[(int) pos[0]];

      final int recordSize = file.readInt(pos[1]);
      final int contentLength = iContent != null ? iContent.length : 0;

      if (contentLength == recordSize) {
        // USE THE OLD SPACE SINCE SIZE ISN'T CHANGED
        file.write(pos[1] + RECORD_FIX_SIZE, iContent);

        Orient.instance().getProfiler().updateCounter(PROFILER_UPDATE_REUSED_ALL, "", +1);
        return iPosition;
      } else if (recordSize - contentLength > RECORD_FIX_SIZE + 50) {
        // USE THE OLD SPACE BUT UPDATE THE CURRENT SIZE. IT'S PREFEREABLE TO USE THE SAME INSTEAD OF FINDING A BEST SUITED FOR IT
        // TO AVOID CHANGES TO REF FILE AS WELL.
        writeRecord(pos, iRid.clusterId, iRid.clusterPosition, iContent);

        // CREATE A HOLE WITH THE DIFFERENCE OF SPACE
        createHole(iPosition + RECORD_FIX_SIZE + contentLength, recordSize - contentLength - RECORD_FIX_SIZE);

        Orient.instance().getProfiler()
            .updateCounter(PROFILER_UPDATE_REUSED_PARTIAL, "Space reused partially in data segment during record update", +1);
      } else {
        // CREATE A HOLE FOR THE ENTIRE OLD RECORD
        createHole(iPosition, recordSize);

        // USE A NEW SPACE
        pos = getFreeSpace(contentLength + RECORD_FIX_SIZE);
        writeRecord(pos, iRid.clusterId, iRid.clusterPosition, iContent);

        Orient.instance().getProfiler()
            .updateCounter(PROFILER_UPDATE_NOT_REUSED, "Space not reused in data segment during record update", +1);
      }

      return getAbsolutePosition(pos);

    } finally {
      releaseExclusiveLock();
    }
  }

  public int deleteRecord(final long iPosition) throws IOException {
    acquireExclusiveLock();
    try {

      if (iPosition == -1)
        return 0;

      final long[] pos = getRelativePosition(iPosition);
      final OFile file = files[(int) pos[0]];

      final int recordSize = file.readInt(pos[1]);
      createHole(iPosition, recordSize);
      return recordSize;

    } finally {
      releaseExclusiveLock();
    }
  }

  /**
   * Returns the total number of holes.
   * 
   * @throws IOException
   */
  public long getHoles() {
    acquireSharedLock();
    try {

      return holeSegment.getHoles();

    } finally {
      releaseSharedLock();
    }
  }

  /**
   * Returns the list of holes as pair of position & ppos
   * 
   * @throws IOException
   */
  public List<ODataHoleInfo> getHolesList() {
    acquireSharedLock();
    try {

      final List<ODataHoleInfo> holes = new ArrayList<ODataHoleInfo>();

      final int tot = holeSegment.getHoles();
      for (int i = 0; i < tot; ++i) {
        final ODataHoleInfo h = holeSegment.getHole(i);
        if (h != null)
          holes.add(h);
      }

      return holes;

    } finally {
      releaseSharedLock();
    }
  }

  public int getId() {
    return id;
  }

  private void createHole(final long iRecordOffset, final int iRecordSize) throws IOException {
    long holePositionOffset = iRecordOffset;
    int holeSize = iRecordSize + RECORD_FIX_SIZE;

    final long timer = Orient.instance().getProfiler().startChrono();
    try {

      long[] pos = getRelativePosition(iRecordOffset);
      final OFile file = files[(int) pos[0]];

      final ODataHoleInfo closestHole = getCloserHole(iRecordOffset, iRecordSize, file, pos);

      Orient
          .instance()
          .getProfiler()
          .stopChrono(PROFILER_HOLE_FIND_CLOSER, "Time to find the closer hole in data segment", timer, "db.*.data.findClosestHole");

      if (closestHole == null)
        // CREATE A NEW ONE
        holeSegment.createHole(iRecordOffset, holeSize);
      else if (closestHole.dataOffset + closestHole.size == iRecordOffset) {
        // IT'S CONSECUTIVE TO ANOTHER HOLE AT THE LEFT: UPDATE LAST ONE
        holeSize += closestHole.size;
        holeSegment.updateHole(closestHole, closestHole.dataOffset, holeSize);
        holePositionOffset = closestHole.dataOffset;

      } else if (holePositionOffset + holeSize == closestHole.dataOffset) {
        // IT'S CONSECUTIVE TO ANOTHER HOLE AT THE RIGHT: UPDATE LAST ONE
        holeSize += closestHole.size;
        holeSegment.updateHole(closestHole, holePositionOffset, holeSize);

      } else {
        if (defragStrategy == 1)
          // ASYNCHRONOUS DEFRAG: CREATE A NEW HOLE AND DEFRAG LATER
          holeSegment.createHole(iRecordOffset, holeSize);
        else {
          defragHole(file, iRecordOffset, iRecordSize, closestHole);
          return;
        }
      }

      // WRITE NEGATIVE RECORD SIZE TO MARK AS DELETED
      pos = getRelativePosition(holePositionOffset);
      files[(int) pos[0]].writeInt(pos[1], holeSize * -1);

    } finally {
      Orient.instance().getProfiler()
          .stopChrono(PROFILER_HOLE_CREATE, "Time to create the hole in data segment", timer, "db.*.data.createHole");
    }
  }

  private void defragHole(final OFile file, final long iRecordOffset, final int iRecordSize, ODataHoleInfo closestHole)
      throws IOException {
    final long timer = Orient.instance().getProfiler().startChrono();

    int holeSize = iRecordSize + RECORD_FIX_SIZE;

    if (closestHole == null) {
      final long[] pos = getRelativePosition(iRecordOffset);
      closestHole = getCloserHole(iRecordOffset, iRecordSize, file, pos);
    }

    // QUITE CLOSE, AUTO-DEFRAG!
    long closestHoleOffset;
    if (iRecordOffset > closestHole.dataOffset)
      closestHoleOffset = (closestHole.dataOffset + closestHole.size) - iRecordOffset;
    else
      closestHoleOffset = closestHole.dataOffset - (iRecordOffset + iRecordSize);

    long holePositionOffset;

    if (closestHoleOffset < 0) {
      // MOVE THE DATA ON THE RIGHT AND USE ONE HOLE FOR BOTH
      closestHoleOffset *= -1;

      // SEARCH LAST SEGMENT
      long moveFrom = closestHole.dataOffset + closestHole.size;
      int recordSize;

      final long offsetLimit = iRecordOffset;

      final List<long[]> segmentPositions = new ArrayList<long[]>();

      while (moveFrom < offsetLimit) {
        final long[] pos = getRelativePosition(moveFrom);

        if (pos[1] >= file.getFilledUpTo())
          // END OF FILE
          break;

        int recordContentSize = file.readInt(pos[1]);
        if (recordContentSize < 0)
          // FOUND HOLE
          break;

        recordSize = recordContentSize + RECORD_FIX_SIZE;

        // SAVE DATA IN ARRAY
        segmentPositions.add(0, new long[] { moveFrom, recordSize });

        moveFrom += recordSize;
      }

      long gap = offsetLimit + holeSize;

      for (long[] item : segmentPositions) {
        final int sizeMoved = moveRecord(item[0], gap - item[1]);

        if (sizeMoved < 0)
          throw new IllegalStateException("Cannot move record at position " + moveFrom + ": found hole");
        else if (sizeMoved != item[1])
          throw new IllegalStateException("Corrupted hole at position " + item[0] + ": found size " + sizeMoved + " instead of "
              + item[1]);

        gap -= sizeMoved;
      }

      holePositionOffset = closestHole.dataOffset;
      holeSize += closestHole.size;
    } else {
      // MOVE THE DATA ON THE LEFT AND USE ONE HOLE FOR BOTH
      long moveFrom = iRecordOffset + holeSize;
      long moveTo = iRecordOffset;
      final long moveUpTo = closestHole.dataOffset;

      while (moveFrom < moveUpTo) {
        final int sizeMoved = moveRecord(moveFrom, moveTo);

        if (sizeMoved < 0)
          throw new IllegalStateException("Cannot move record at position " + moveFrom + ": found hole");

        moveFrom += sizeMoved;
        moveTo += sizeMoved;
      }

      if (moveFrom != moveUpTo)
        throw new IllegalStateException("Corrupted holes: found offset " + moveFrom + " instead of " + moveUpTo
            + " while creating a new hole on position " + iRecordOffset + ", size " + iRecordSize + ". The closest hole "
            + closestHole.holeOffset + " points to position " + closestHole.dataOffset + ", size " + closestHole.size);

      holePositionOffset = moveTo;
      holeSize += closestHole.size;
    }

    holeSegment.updateHole(closestHole, holePositionOffset, holeSize);

    // WRITE NEGATIVE RECORD SIZE TO MARK AS DELETED
    final long[] pos = getRelativePosition(holePositionOffset);
    files[(int) pos[0]].writeInt(pos[1], holeSize * -1);

    Orient.instance().getProfiler()
        .stopChrono(PROFILER_HOLE_CREATE, "Time to create the hole in data segment", timer, "db.*.data.createHole");
  }

  private ODataHoleInfo getCloserHole(final long iRecordOffset, final int iRecordSize, final OFile file, final long[] pos) {
    if (holeSegment.getHoles() == 0)
      return null;

    // COMPUTE DEFRAG HOLE DISTANCE
    final int defragHoleDistance;
    if (defragMaxHoleDistance > 0)
      // FIXED SIZE
      defragHoleDistance = defragMaxHoleDistance;
    else {
      // DYNAMIC SIZE
      final long size = getSize();
      defragHoleDistance = Math.max(32768 * (int) (size / 10000000), 32768);
    }

    // GET FILE RANGE
    final long[] fileRanges;
    if (pos[0] == 0)
      fileRanges = new long[] { 0, file.getFilledUpTo() };
    else {
      final long size = (files[0].getFileSize() * pos[0]);
      fileRanges = new long[] { size, size + file.getFilledUpTo() };
    }

    // FIND THE CLOSEST HOLE
    return holeSegment.getCloserHole(iRecordOffset, iRecordSize, Math.max(iRecordOffset - defragHoleDistance, fileRanges[0]),
        Math.min(iRecordOffset + iRecordSize + defragHoleDistance, fileRanges[1]));
  }

  private int moveRecord(final long iSourcePosition, final long iDestinationPosition) throws IOException {
    // GET RECORD TO MOVE
    final long[] pos = getRelativePosition(iSourcePosition);
    final OFile file = files[(int) pos[0]];

    final int recordSize = file.readInt(pos[1]);

    if (recordSize < 0)
      // FOUND HOLE
      return -1;

    final long timer = Orient.instance().getProfiler().startChrono();

    final int clusterId = file.readShort(pos[1] + OBinaryProtocol.SIZE_INT);
    final byte[] clusterPositionContent = new byte[CLUSTER_POS_SIZE];
    file.read(pos[1] + OBinaryProtocol.SIZE_INT + OBinaryProtocol.SIZE_SHORT, clusterPositionContent, CLUSTER_POS_SIZE);

    final OClusterPosition clusterPosition = OClusterPositionFactory.INSTANCE.fromStream(clusterPositionContent);
    final byte[] content = new byte[recordSize];
    file.read(pos[1] + RECORD_FIX_SIZE, content, recordSize);

    if (clusterId > -1) {
      // CHANGE THE POINTMENT OF CLUSTER TO THE NEW POSITION. -1 MEANS TEMP RECORD
      final OCluster cluster = storage.getClusterById(clusterId);

      final OPhysicalPosition ppos = cluster.getPhysicalPosition(new OPhysicalPosition(clusterPosition));

      if (ppos.dataSegmentPos != iSourcePosition)
        OLogManager.instance().warn(this,
            "Found corrupted record hole for rid %d:%d: data position is wrong: %d <-> %d. Auto fixed by writing position %d",
            clusterId, clusterPosition, ppos.dataSegmentPos, iSourcePosition, iDestinationPosition);

      cluster.updateDataSegmentPosition(clusterPosition, id, iDestinationPosition);
    }

    writeRecord(getRelativePosition(iDestinationPosition), clusterId, clusterPosition, content);

    Orient.instance().getProfiler()
        .stopChrono(PROFILER_MOVE_RECORD, "Time to move a chunk in data segment", timer, "db.*.data.move");

    return recordSize + RECORD_FIX_SIZE;
  }

  private void writeRecord(final long[] iFilePosition, final int iClusterSegment, final OClusterPosition iClusterPosition,
      final byte[] iContent) throws IOException {
    final OFile file = files[(int) iFilePosition[0]];

    file.writeInt(iFilePosition[1], iContent != null ? iContent.length : 0);
    file.writeShort(iFilePosition[1] + OBinaryProtocol.SIZE_INT, (short) iClusterSegment);
    // TestSimulateError.onDataLocalWriteRecord(this, iFilePosition, iClusterSegment, iClusterPosition, iContent);
    file.write(iFilePosition[1] + OBinaryProtocol.SIZE_INT + OBinaryProtocol.SIZE_SHORT, iClusterPosition.toStream());

    file.write(iFilePosition[1] + RECORD_FIX_SIZE, iContent);
  }

  private long[] getFreeSpace(final int recordSize) throws IOException {
    // GET THE POSITION TO RECYCLE FOLLOWING THE CONFIGURED STRATEGY IF ANY
    final long position = holeSegment.popFirstAvailableHole(recordSize);

    final long[] newFilePosition;
    if (position > -1)
      newFilePosition = getRelativePosition(position);
    else
      // ALLOCATE NEW SPACE FOR IT
      newFilePosition = allocateSpace(recordSize);
    return newFilePosition;
  }

  public void acquireExclusiveLock() {
    lock.acquireExclusiveLock();
  }

  public void releaseExclusiveLock() {
    lock.releaseExclusiveLock();
  }

  public void acquireSharedLock() {
    lock.acquireSharedLock();
  }

  public void releaseSharedLock() {
    lock.releaseSharedLock();
  }
}
