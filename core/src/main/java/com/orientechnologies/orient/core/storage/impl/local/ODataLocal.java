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

import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.orient.core.OConstants;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.config.OStorageDataConfiguration;
import com.orientechnologies.orient.core.config.OStorageDataHoleConfiguration;
import com.orientechnologies.orient.core.exception.OStorageException;
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
	private static final int				DEF_START_SIZE	= 10000000;
	public static final int					RECORD_FIX_SIZE	= 14;
	protected final int							id;
	protected final ODataLocalHole	holeSegment;
	protected final int							defragMaxHoleDistance;

	public ODataLocal(final OStorageLocal iStorage, final OStorageDataConfiguration iConfig, final int iId) throws IOException {
		super(iStorage, iConfig, DEF_EXTENSION, 0);
		id = iId;

		iConfig.holeFile = new OStorageDataHoleConfiguration(iConfig, OStorageVariableParser.DB_PATH_VARIABLE + "/" + name,
				iConfig.fileType, iConfig.maxSize);
		holeSegment = new ODataLocalHole(iStorage, iConfig.holeFile);

		defragMaxHoleDistance = OGlobalConfiguration.FILE_DEFRAG_HOLE_MAX_DISTANCE.getValueAsInteger();
	}

	@Override
	public void open() throws IOException {
		super.open();
		holeSegment.open();
	}

	@Override
	public void create(final int iStartSize) throws IOException {
		super.create(iStartSize > -1 ? iStartSize : DEF_START_SIZE);
		holeSegment.create(-1);
	}

	@Override
	public void close() throws IOException {
		super.close();
		holeSegment.close();
	}

	/**
	 * Add the record content in file.
	 * 
	 * @param iContent
	 *          The content to write
	 * @return The record offset.
	 * @throws IOException
	 */
	public long addRecord(final int iClusterSegment, final long iClusterPosition, final byte[] iContent) throws IOException {
		if (iContent.length == 0)
			// AVOID UNUSEFUL CREATION OF EMPTY RECORD: IT WILL BE CREATED AT FIRST UPDATE
			return -1;

		acquireExclusiveLock();
		try {
			final int recordSize = iContent.length + RECORD_FIX_SIZE;

			final long[] newFilePosition = getFreeSpace(recordSize);
			writeRecord(newFilePosition, iClusterSegment, iClusterPosition, iContent);
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

			byte[] content = new byte[recordSize];
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
	public long setRecord(long iPosition, final int iClusterSegment, final long iClusterPosition, final byte[] iContent)
			throws IOException {
		acquireExclusiveLock();
		try {
			long[] pos = getRelativePosition(iPosition);
			final OFile file = files[(int) pos[0]];

			final int recordSize = file.readInt(pos[1]);
			// if (recordSize <= 0)
			// OLogManager.instance().error(this, "Error while writing to data file. The record size was invalid", OIOException.class);

			if (iContent.length == recordSize) {
				// USE THE OLD SPACE SINCE SIZE IT ISN'T CHANGED
				file.write(pos[1] + RECORD_FIX_SIZE, iContent);

				OProfiler.getInstance().updateCounter("ODataLocal.setRecord:tot.reused.space", +1);
			} else if (recordSize - iContent.length > RECORD_FIX_SIZE) {
				// USE THE OLD SPACE BUT UPDATE THE CURRENT SIZE. IT'S PREFEREABLE TO USE THE SAME INSTEAD FINDING A BEST SUITED FOR IT TO
				// AVOID CHANGES TO REF FILE AS WELL.
				writeRecord(pos, iClusterSegment, iClusterPosition, iContent);

				// CREATE A HOLE WITH THE DIFFERENCE OF SPACE
				createHole(iPosition + RECORD_FIX_SIZE + iContent.length, recordSize - iContent.length - RECORD_FIX_SIZE);

				OProfiler.getInstance().updateCounter("ODataLocal.setRecord:part.reused.space", +1);
			} else {
				// CREATE A HOLE FOR THE ENTIRE OLD RECORD
				createHole(iPosition, recordSize);

				// USE A NEW SPACE
				pos = getFreeSpace(iContent.length + RECORD_FIX_SIZE);
				writeRecord(pos, iClusterSegment, iClusterPosition, iContent);

				OProfiler.getInstance().updateCounter("ODataLocal.setRecord:new.space", +1);
			}

			return getAbsolutePosition(pos);

		} finally {
			releaseExclusiveLock();
		}
	}

	public int deleteRecord(final long iPosition) throws IOException {
		acquireExclusiveLock();
		try {
			final long[] pos = getRelativePosition(iPosition);
			final OFile file = files[(int) pos[0]];

			final int recordSize = file.readInt(pos[1]);
			createHole(iPosition, recordSize);
			return recordSize;

		} finally {
			releaseExclusiveLock();
		}
	}

	protected void writeRecord(final long[] iFilePosition, final int iClusterSegment, final long iClusterPosition,
			final byte[] iContent) throws IOException {
		final OFile file = files[(int) iFilePosition[0]];

		file.writeInt(iFilePosition[1], iContent.length);
		file.writeShort(iFilePosition[1] + OConstants.SIZE_INT, (short) iClusterSegment);
		file.writeLong(iFilePosition[1] + OConstants.SIZE_INT + OConstants.SIZE_SHORT, iClusterPosition);

		file.write(iFilePosition[1] + RECORD_FIX_SIZE, iContent);
	}

	public void createHole(final long iRecordOffset, final int iRecordSize) throws IOException {
		acquireExclusiveLock();
		try {
			long holePositionOffset = iRecordOffset;
			int holeSize = iRecordSize + RECORD_FIX_SIZE;

			final int holes = holeSegment.getHoles();

			if (holes > 0) {
				final OPhysicalPosition ppos = new OPhysicalPosition();

				// FIND THE CLOSEST HOLE
				int closestHoleIndex = -1;
				long closestHoleOffset = Integer.MAX_VALUE;
				OPhysicalPosition closestPpos = new OPhysicalPosition();
				for (int i = 0; i < holes; ++i) {
					holeSegment.getHole(i, ppos);

					if (ppos.dataPosition == -1)
						// FREE HOLE
						continue;

					boolean closest = false;

					if (iRecordOffset > ppos.dataPosition) {
						if (closestHoleIndex == -1 || iRecordOffset - (ppos.dataPosition + ppos.recordSize) < Math.abs(closestHoleOffset)) {
							closestHoleOffset = (ppos.dataPosition + ppos.recordSize) - iRecordOffset;
							closest = true;
						}
					} else {
						if (closestHoleIndex == -1 || ppos.dataPosition - (iRecordOffset + iRecordSize) < Math.abs(closestHoleOffset)) {
							closestHoleOffset = ppos.dataPosition - (iRecordOffset + iRecordSize);
							closest = true;
						}
					}

					if (closest) {
						closestHoleIndex = i;
						ppos.copyTo(closestPpos);
					}
				}

				if (closestPpos.dataPosition + closestPpos.recordSize == iRecordOffset) {
					// IT'S CONSECUTIVE TO ANOTHER HOLE AT THE LEFT: UPDATE LAST ONE
					holeSize += closestPpos.recordSize;
					holeSegment.updateHole(closestHoleIndex, closestPpos.dataPosition, holeSize);

				} else if (holePositionOffset + holeSize == closestPpos.dataPosition) {
					// IT'S CONSECUTIVE TO ANOTHER HOLE AT THE RIGHT: UPDATE LAST ONE
					holeSize += closestPpos.recordSize;
					holeSegment.updateHole(closestHoleIndex, holePositionOffset, holeSize);

				} else {
					if (Math.abs(closestHoleOffset) < defragMaxHoleDistance) {
						// QUITE CLOSE, AUTO-DEFRAG!

						if (closestHoleOffset < 0) {
							// MOVE THE DATA ON THE RIGHT AND USE ONE HOLE FOR BOTH
							closestHoleOffset *= -1;

							// SEARCH LAST SEGMENT
							long moveFrom = closestPpos.dataPosition + closestPpos.recordSize;
							int recordSize;

							final long offsetLimit = Math.min(iRecordOffset, getFilledUpTo());

							final List<long[]> segmentPositions = new ArrayList<long[]>();
							do {
								final long[] pos = getRelativePosition(moveFrom);
								final OFile file = files[(int) pos[0]];

								recordSize = file.readInt(pos[1]) + RECORD_FIX_SIZE;

								// SAVE DATA IN ARRAY
								segmentPositions.add(0, new long[] { moveFrom, recordSize });

								moveFrom += recordSize;
							} while (moveFrom < offsetLimit);

							long gap = offsetLimit + holeSize;

							for (long[] item : segmentPositions) {
								final int sizeMoved = moveRecord(item[0], gap - item[1]);

								if (sizeMoved != item[1])
									throw new IllegalStateException("Corrupted holes: Found size " + sizeMoved + " instead of " + item[1]);

								gap -= sizeMoved;
							}

							holePositionOffset = closestPpos.dataPosition;
							holeSize += closestPpos.recordSize;

						} else {
							// MOVE THE DATA ON THE LEFT AND USE ONE HOLE FOR BOTH
							long moveFrom = iRecordOffset + iRecordSize + RECORD_FIX_SIZE;
							long moveTo = iRecordOffset;
							long moveUpTo = closestPpos.dataPosition;

							do {
								final int sizeMoved = moveRecord(moveFrom, moveTo);

								moveFrom += sizeMoved;
								moveTo += sizeMoved;

							} while (moveFrom < moveUpTo);

							if (moveFrom != moveUpTo)
								throw new IllegalStateException("Corrupted holes: Found offset " + moveFrom + " instead of " + moveUpTo);

							holePositionOffset = moveTo;
							holeSize += closestPpos.recordSize;
						}

						holeSegment.updateHole(closestHoleIndex, holePositionOffset, holeSize);

					} else {
						// CREATE A NEW ONE
						holeSegment.createHole(iRecordOffset, holeSize);
					}
				}
			} else
				// CREATE A NEW ONE
				holeSegment.createHole(iRecordOffset, holeSize);

			// WRITE NEGATIVE RECORD SIZE TO MARK AS DELETED
			final long[] pos = getRelativePosition(holePositionOffset);
			files[(int) pos[0]].writeInt(pos[1], holeSize * -1);

		} finally {
			releaseExclusiveLock();
		}
	}

	private int moveRecord(long iSourcePosition, long iDestinationPosition) throws IOException {
		final long timer = OProfiler.getInstance().startChrono();

		// GET RECORD TO MOVE
		final long[] pos = getRelativePosition(iSourcePosition);
		final OFile file = files[(int) pos[0]];

		final int recordSize = file.readInt(pos[1]);

		if (recordSize < 0)
			// FOUND HOLE
			return -1;

		final short clusterId = file.readShort(pos[1] + OConstants.SIZE_INT);
		final long clusterPosition = file.readLong(pos[1] + OConstants.SIZE_INT + OConstants.SIZE_SHORT);

		byte[] content = new byte[recordSize];
		file.read(pos[1] + RECORD_FIX_SIZE, content, recordSize);

		if (clusterId > -1) {
			// CHANGE THE POINTMENT OF CLUSTER TO THE NEW POSITION
			final OCluster cluster = storage.getClusterById(clusterId);
			final OPhysicalPosition ppos = cluster.getPhysicalPosition(clusterPosition, new OPhysicalPosition());

			if (ppos.dataPosition != iSourcePosition)
				throw new OStorageException("Found corrupted record hole for rid " + clusterId + ":" + clusterPosition
						+ ": data position is wrong: " + ppos.dataPosition + "<->" + iSourcePosition);

			cluster.setPhysicalPosition(clusterPosition, iDestinationPosition);
		}

		writeRecord(getRelativePosition(iDestinationPosition), clusterId, clusterPosition, content);

		OProfiler.getInstance().stopChrono("Storage.data.move", timer);

		return recordSize + RECORD_FIX_SIZE;
	}

	/**
	 * Returns the list of holes as pair of position & ppos
	 * 
	 * @throws IOException
	 */
	public List<OPhysicalPosition> getHoles() throws IOException {
		final List<OPhysicalPosition> holes = new ArrayList<OPhysicalPosition>();

		acquireExclusiveLock();
		try {
			final int tot = holeSegment.getHoles();
			for (int i = 0; i < tot; ++i) {
				final OPhysicalPosition ppos = holeSegment.getHole(i, new OPhysicalPosition());
				holes.add(ppos);
			}
		} finally {
			releaseExclusiveLock();
		}
		return holes;
	}

	public int getId() {
		return id;
	}

	private long[] getFreeSpace(final int recordSize) throws IOException {
		final long[] newFilePosition;
		final long position = holeSegment.popBestHole(recordSize);
		if (position > -1)
			newFilePosition = getRelativePosition(position);
		else
			// ALLOCATE NEW SPACE FOR IT
			newFilePosition = allocateSpace(recordSize);
		return newFilePosition;
	}
}
