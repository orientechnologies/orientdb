/*
 * Copyright 1999-2010 Luca Garulli (l.garulli--at--orientechnologies.com)
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

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.orient.core.OConstants;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.config.OStorageDataConfiguration;
import com.orientechnologies.orient.core.config.OStorageDataHoleConfiguration;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.storage.OCluster;
import com.orientechnologies.orient.core.storage.OPhysicalPosition;
import com.orientechnologies.orient.core.storage.fs.OFile;

/**
 * Handle the table to resolve logical address to physical address.<br/>
 * <br/>
 * Record structure:<br/>
 * <br/>
 * +--------------+--------------+--------------+----------------------+<br/>
 * | CONTENT SIZE | CLUSTER ID . | CLUSTER POS. | CONTENT ............ |<br/>
 * | 4 bytes .... | 2 bytes .... | 8 bytes .... | <RECORD SIZE> bytes. |<br/>
 * +--------------+--------------+--------------+----------------------+<br/>
 * = 14+? bytes<br/>
 */
public class ODataLocal extends OMultiFileSegment {
	static final String							DEF_EXTENSION		= ".oda";
	public static final int					RECORD_FIX_SIZE	= 14;
	protected final int							id;
	protected final ODataLocalHole	holeSegment;
	protected int										defragMaxHoleDistance;
	protected int										defragStrategy;
	protected long									defStartSize;

	private final String						PROFILER_HOLE_FIND_CLOSER;
	private final String						PROFILER_UPDATE_REUSED_ALL;
	private final String						PROFILER_UPDATE_REUSED_PARTIAL;
	private final String						PROFILER_UPDATE_NOT_REUSED;
	private final String						PROFILER_MOVE_RECORD;
	private final String						PROFILER_HOLE_HANDLE;

	public ODataLocal(final OStorageLocal iStorage, final OStorageDataConfiguration iConfig, final int iId) throws IOException {
		super(iStorage, iConfig, DEF_EXTENSION, 0);
		id = iId;

		iConfig.holeFile = new OStorageDataHoleConfiguration(iConfig, OStorageVariableParser.DB_PATH_VARIABLE + "/" + name,
				iConfig.fileType, iConfig.maxSize);
		holeSegment = new ODataLocalHole(iStorage, iConfig.holeFile);

		defStartSize = OFileUtils.getSizeAsNumber(iConfig.fileStartSize);
		defragMaxHoleDistance = OGlobalConfiguration.FILE_DEFRAG_HOLE_MAX_DISTANCE.getValueAsInteger();
		defragStrategy = OGlobalConfiguration.FILE_DEFRAG_STRATEGY.getValueAsInteger();

		PROFILER_HOLE_HANDLE = "storage." + storage.getName() + ".data.handleHole";
		PROFILER_HOLE_FIND_CLOSER = "storage." + storage.getName() + ".data.findClosestHole";
		PROFILER_UPDATE_REUSED_ALL = "storage." + storage.getName() + ".data.update.reusedAll";
		PROFILER_UPDATE_REUSED_PARTIAL = "storage." + storage.getName() + ".data.update.reusedPartial";
		PROFILER_UPDATE_NOT_REUSED = "storage." + storage.getName() + ".data.update.notReused";
		PROFILER_MOVE_RECORD = "storage." + storage.getName() + ".data.move";
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

		acquireExclusiveLock();
		try {
			final int recordSize = iContent.length + RECORD_FIX_SIZE;

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
								+ file.getOsFile().getName()
								+ "', position "
								+ iPosition
								+ ", size "
								+ OFileUtils.getSizeAsString(recordSize)
								+ ": the record size is bigger then the file itself ("
								+ OFileUtils.getSizeAsString(getFilledUpTo())
								+ "). Probably the record is dirty due to a previous crash. It strongly suggested to restore the database or export and reimport this one.");

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
			// if (recordSize <= 0)
			// OLogManager.instance().error(this, "Error while writing to data file. The record size was invalid", OIOException.class);

			if (iContent.length == recordSize) {
				// USE THE OLD SPACE SINCE SIZE ISN'T CHANGED
				file.write(pos[1] + RECORD_FIX_SIZE, iContent);

				OProfiler.getInstance().updateCounter(PROFILER_UPDATE_REUSED_ALL, +1);
				return iPosition;
			} else if (recordSize - iContent.length > RECORD_FIX_SIZE + 50) {
				// USE THE OLD SPACE BUT UPDATE THE CURRENT SIZE. IT'S PREFEREABLE TO USE THE SAME INSTEAD FINDING A BEST SUITED FOR IT TO
				// AVOID CHANGES TO REF FILE AS WELL.
				writeRecord(pos, iRid.clusterId, iRid.clusterPosition, iContent);

				// CREATE A HOLE WITH THE DIFFERENCE OF SPACE
				handleHole(iPosition + RECORD_FIX_SIZE + iContent.length, recordSize - iContent.length - RECORD_FIX_SIZE);

				OProfiler.getInstance().updateCounter(PROFILER_UPDATE_REUSED_PARTIAL, +1);
			} else {
				// CREATE A HOLE FOR THE ENTIRE OLD RECORD
				handleHole(iPosition, recordSize);

				// USE A NEW SPACE
				pos = getFreeSpace(iContent.length + RECORD_FIX_SIZE);
				writeRecord(pos, iRid.clusterId, iRid.clusterPosition, iContent);

				OProfiler.getInstance().updateCounter(PROFILER_UPDATE_NOT_REUSED, +1);
			}

			return getAbsolutePosition(pos);

		} finally {
			releaseExclusiveLock();
		}
	}

	public int deleteRecord(final long iPosition) throws IOException {
		if (iPosition == -1)
			return 0;

		acquireExclusiveLock();
		try {

			final long[] pos = getRelativePosition(iPosition);
			final OFile file = files[(int) pos[0]];
			file.writeShort(pos[1] + OConstants.SIZE_INT, (short) -1);
			file.writeLong(pos[1] + OConstants.SIZE_INT + OConstants.SIZE_SHORT, -1);

			final int recordSize = file.readInt(pos[1]);
			handleHole(iPosition, recordSize);
			return recordSize;

		} finally {
			releaseExclusiveLock();
		}
	}

	public void updateRid(final long iPosition, final ORecordId iRid) throws IOException {
		long[] pos = getRelativePosition(iPosition);
		final OFile file = files[(int) pos[0]];
		file.writeShort(pos[1] + OConstants.SIZE_INT, (short) iRid.clusterId);
		file.writeLong(pos[1] + OConstants.SIZE_INT + OConstants.SIZE_SHORT, iRid.clusterPosition);
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
		final List<ODataHoleInfo> holes = new ArrayList<ODataHoleInfo>();

		acquireSharedLock();
		try {

			final int tot = holeSegment.getHoles();
			for (int i = 0; i < tot; ++i) {
				final ODataHoleInfo h = holeSegment.getHole(i);
				if (h != null)
					holes.add(h);
			}

		} finally {
			releaseSharedLock();
		}
		return holes;
	}

	public int getId() {
		return id;
	}

	public long loadVersion() throws IOException {
		acquireExclusiveLock();
		try {

			return files[0].readHeaderLong(OConstants.SIZE_LONG);

		} finally {
			releaseExclusiveLock();
		}
	}

	public void saveVersion(final long iVersion) throws IOException {
		acquireExclusiveLock();
		try {

			files[0].writeHeaderLong(OConstants.SIZE_LONG, iVersion);

		} finally {
			releaseExclusiveLock();
		}
	}

	public void handleHole(final long iRecordOffset, final int iRecordSize) throws IOException {
		acquireExclusiveLock();
		try {

			long holePositionOffset = iRecordOffset;
			int holeSize = iRecordSize + RECORD_FIX_SIZE;

			final long timer = OProfiler.getInstance().startChrono();

			long[] pos = getRelativePosition(iRecordOffset);
			final OFile file = files[(int) pos[0]];

			final ODataHoleInfo closestHole = getCloserHole(iRecordOffset, iRecordSize, file, pos);

			OProfiler.getInstance().stopChrono(PROFILER_HOLE_FIND_CLOSER, timer);

			if (closestHole == null)
				// CREATE A NEW ONE
				holeSegment.createHole(iRecordOffset, holeSize);
			else if (closestHole.dataOffset + closestHole.size == iRecordOffset) {
				// IT'S CONSECUTIVE TO ANOTHER HOLE AT THE LEFT: UPDATE LAST ONE
				holeSize += closestHole.size;
				holeSegment.updateHole(closestHole, closestHole.dataOffset, holeSize);

			} else if (holePositionOffset + holeSize == closestHole.dataOffset) {
				// IT'S CONSECUTIVE TO ANOTHER HOLE AT THE RIGHT: UPDATE LAST ONE
				holeSize += closestHole.size;
				holeSegment.updateHole(closestHole, holePositionOffset, holeSize);

			} else {
				// QUITE CLOSE, AUTO-DEFRAG!
				long closestHoleOffset;
				if (iRecordOffset > closestHole.dataOffset)
					closestHoleOffset = (closestHole.dataOffset + closestHole.size) - iRecordOffset;
				else
					closestHoleOffset = closestHole.dataOffset - (iRecordOffset + iRecordSize);

				if (closestHoleOffset < 0) {
					// MOVE THE DATA ON THE RIGHT AND USE ONE HOLE FOR BOTH
					closestHoleOffset *= -1;

					// SEARCH LAST SEGMENT
					long moveFrom = closestHole.dataOffset + closestHole.size;
					int recordSize;

					final long offsetLimit = iRecordOffset;

					final List<long[]> segmentPositions = new ArrayList<long[]>();

					while (moveFrom < offsetLimit) {
						pos = getRelativePosition(moveFrom);

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
							throw new IllegalStateException("Can't move record at position " + moveFrom + ": found hole");
						else if (sizeMoved != item[1])
							throw new IllegalStateException("Corrupted hole at position " + item[0] + ": found size " + sizeMoved
									+ " instead of " + item[1]);

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
							throw new IllegalStateException("Can't move record at position " + moveFrom + ": found hole");

						moveFrom += sizeMoved;
						moveTo += sizeMoved;
					}

					if (moveFrom != moveUpTo)
						throw new IllegalStateException("Corrupted holes: Found offset " + moveFrom + " instead of " + moveUpTo);

					holePositionOffset = moveTo;
					holeSize += closestHole.size;
				}

				holeSegment.updateHole(closestHole, holePositionOffset, holeSize);
			}

			// WRITE NEGATIVE RECORD SIZE TO MARK AS DELETED
			pos = getRelativePosition(holePositionOffset);
			files[(int) pos[0]].writeInt(pos[1], holeSize * -1);

			OProfiler.getInstance().stopChrono(PROFILER_HOLE_HANDLE, timer);

		} finally {
			releaseExclusiveLock();
		}
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

		final long timer = OProfiler.getInstance().startChrono();

		final short clusterId = file.readShort(pos[1] + OConstants.SIZE_INT);
		final long clusterPosition = file.readLong(pos[1] + OConstants.SIZE_INT + OConstants.SIZE_SHORT);

		final byte[] content = new byte[recordSize];
		file.read(pos[1] + RECORD_FIX_SIZE, content, recordSize);

		if (clusterId > -1) {
			// CHANGE THE POINTMENT OF CLUSTER TO THE NEW POSITION. -1 MEANS TEMP RECORD
			final OCluster cluster = storage.getClusterById(clusterId);
			final OPhysicalPosition ppos = cluster.getPhysicalPosition(clusterPosition, new OPhysicalPosition());

			if (ppos.dataPosition != iSourcePosition)
				throw new OStorageException("Found corrupted record hole for rid " + clusterId + ":" + clusterPosition
						+ ": data position is wrong: " + ppos.dataPosition + "<->" + iSourcePosition);

			cluster.setPhysicalPosition(clusterPosition, iDestinationPosition);
		}

		writeRecord(getRelativePosition(iDestinationPosition), clusterId, clusterPosition, content);

		storage.getTxManager().getTxSegment().movedRecord(getId(), iSourcePosition, iDestinationPosition);

		OProfiler.getInstance().stopChrono(PROFILER_MOVE_RECORD, timer);

		return recordSize + RECORD_FIX_SIZE;
	}

	protected void writeRecord(final long[] iFilePosition, final int iClusterSegment, final long iClusterPosition,
			final byte[] iContent) throws IOException {
		final OFile file = files[(int) iFilePosition[0]];

		file.writeInt(iFilePosition[1], iContent.length);
		file.writeShort(iFilePosition[1] + OConstants.SIZE_INT, (short) iClusterSegment);
		file.writeLong(iFilePosition[1] + OConstants.SIZE_INT + OConstants.SIZE_SHORT, iClusterPosition);

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
}
