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
import java.util.Comparator;
import java.util.List;
import java.util.TreeMap;

import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.orient.core.config.OStorageFileConfiguration;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.serialization.OBinaryProtocol;

/**
 * Handles the holes inside data segments. Exists only 1 hole segment per data-segment even if multiple data-files are configured.
 * The synchronization is in charge to the ODataSegment instance. The holes are kept in memory for a fast access to it.<br/>
 * <br/>
 * Record structure:<br/>
 * <br/>
 * +----------------------+----------------------+<br/>
 * | DATA OFFSET......... | DATA SIZE........... |<br/>
 * | 8 bytes = max 2^63-1 | 4 bytes = max 2^31-1 |<br/>
 * +----------------------+----------------------+<br/>
 * = 12 bytes<br/>
 */
public class ODataLocalHole extends OSingleFileSegment {
	private static final int														DEF_START_SIZE			= 262144;
	private static final int														RECORD_SIZE					= 12;
	private int																					maxHoleSize					= -1;

	private final List<Integer>													freeHoles						= new ArrayList<Integer>();
	private final ODataHoleInfo													cursor							= new ODataHoleInfo();

	private final List<ODataHoleInfo>										availableHolesList	= new ArrayList<ODataHoleInfo>();
	private final TreeMap<ODataHoleInfo, ODataHoleInfo>	availableHolesBySize;
	private final TreeMap<ODataHoleInfo, ODataHoleInfo>	availableHolesByPosition;

	private final String																PROFILER_DATA_RECYCLED_COMPLETE;
	private final String																PROFILER_DATA_RECYCLED_PARTIAL;
	private final String																PROFILER_DATA_RECYCLED_NOTFOUND;
	private final String																PROFILER_DATA_HOLE_CREATE;
	private final String																PROFILER_DATA_HOLE_UPDATE;

	public ODataLocalHole(final OStorageLocal iStorage, final OStorageFileConfiguration iConfig) throws IOException {
		super(iStorage, iConfig);

		PROFILER_DATA_RECYCLED_COMPLETE = "storage." + storage.getName() + ".data.recycled.complete";
		PROFILER_DATA_RECYCLED_PARTIAL = "storage." + storage.getName() + ".data.recycled.partial";
		PROFILER_DATA_RECYCLED_NOTFOUND = "storage." + storage.getName() + ".data.recycled.notFound";
		PROFILER_DATA_HOLE_CREATE = "storage." + storage.getName() + ".data.createHole";
		PROFILER_DATA_HOLE_UPDATE = "storage." + storage.getName() + ".data.updateHole";

		availableHolesBySize = new TreeMap<ODataHoleInfo, ODataHoleInfo>();
		availableHolesByPosition = new TreeMap<ODataHoleInfo, ODataHoleInfo>(new Comparator<ODataHoleInfo>() {
			public int compare(final ODataHoleInfo o1, final ODataHoleInfo o2) {
				if (o1.dataOffset == o2.dataOffset)
					return 0;
				if (o1.dataOffset > o2.dataOffset)
					return 1;
				return -1;
			}
		});

	}

	@Override
	public boolean open() throws IOException {
		final boolean status = super.open();
		loadHolesInMemory();
		return status;
	}

	@Override
	public void create(final int iStartSize) throws IOException {
		super.create(iStartSize > -1 ? iStartSize : DEF_START_SIZE);
	}

	/**
	 * Appends the hole to the end of the segment.
	 * 
	 * @throws IOException
	 */
	public void createHole(final long iRecordOffset, final int iRecordSize) throws IOException {
		final long timer = OProfiler.getInstance().startChrono();

		// IN MEMORY
		final int recycledPosition;
		final ODataHoleInfo hole;
		if (!freeHoles.isEmpty()) {
			// RECYCLE THE FIRST FREE HOLE
			recycledPosition = freeHoles.remove(0);
			hole = availableHolesList.get(recycledPosition);
			hole.dataOffset = iRecordOffset;
			hole.size = iRecordSize;
		} else {
			// APPEND A NEW ONE
			recycledPosition = getHoles();
			hole = new ODataHoleInfo(iRecordSize, iRecordOffset, recycledPosition);
			availableHolesList.add(hole);
			file.allocateSpace(RECORD_SIZE);
		}

		availableHolesBySize.put(hole, hole);
		availableHolesByPosition.put(hole, hole);

		if (maxHoleSize < iRecordSize)
			maxHoleSize = iRecordSize;

		// TO FILE
		final long p = recycledPosition * RECORD_SIZE;
		file.writeLong(p, iRecordOffset);
		file.writeInt(p + OBinaryProtocol.SIZE_LONG, iRecordSize);

		OProfiler.getInstance().stopChrono(PROFILER_DATA_HOLE_CREATE, timer);
	}

	public ODataHoleInfo getCloserHole(final long iHolePosition, final int iHoleSize, final long iLowerRange, final long iHigherRange) {
		cursor.dataOffset = iHolePosition;
		ODataHoleInfo lowerHole = availableHolesByPosition.lowerKey(cursor);

		cursor.dataOffset = iHolePosition + iHoleSize;
		ODataHoleInfo higherHole = availableHolesByPosition.higherKey(cursor);

		if (lowerHole != null && higherHole != null && lowerHole != higherHole && lowerHole.dataOffset >= higherHole.dataOffset)
			// CHECK ERROR
			throw new OStorageException("Found bad order in hole list: " + lowerHole + " is higher than " + higherHole);

		final ODataHoleInfo closestHole;
		if (lowerHole != null && (lowerHole.dataOffset + lowerHole.size < iLowerRange))
			// OUT OF RANGE: INVALID IT
			lowerHole = null;

		if (higherHole != null && (higherHole.dataOffset > iHigherRange))
			// OUT OF RANGE: INVALID IT
			higherHole = null;

		if (lowerHole == higherHole)
			closestHole = higherHole;
		else if (lowerHole == null && higherHole != null)
			closestHole = higherHole;
		else if (lowerHole != null && higherHole == null)
			closestHole = lowerHole;
		else if (iHolePosition - (lowerHole.dataOffset + lowerHole.size) > higherHole.dataOffset - iHolePosition)
			closestHole = higherHole;
		else
			closestHole = lowerHole;

		return closestHole;
	}

	/**
	 * Returns the first available hole (at least iRecordSize length) to be reused.
	 * 
	 * @return
	 * 
	 * @throws IOException
	 */
	public long popFirstAvailableHole(final int iRecordSize) throws IOException {
		if (maxHoleSize > -1 && iRecordSize + ODataLocal.RECORD_FIX_SIZE + 50 > maxHoleSize)
			// DON'T BROWSE: NO ONE HOLE WITH THIS SIZE IS AVAILABLE
			return -1;

		final long timer = OProfiler.getInstance().startChrono();

		if (!availableHolesBySize.isEmpty()) {
			cursor.size = iRecordSize;

			// SEARCH THE HOLE WITH THE SAME SIZE
			ODataHoleInfo hole = availableHolesBySize.get(cursor);
			if (hole != null && hole.size == iRecordSize) {
				// PERFECT MATCH: DELETE THE HOLE
				OProfiler.getInstance().stopChrono(PROFILER_DATA_RECYCLED_COMPLETE, timer);
				final long pos = hole.dataOffset;
				deleteHole(hole.holeOffset);
				return pos;
			}

			// TRY WITH THE BIGGEST HOLE
			hole = availableHolesBySize.lastKey();
			if (hole.size > iRecordSize + ODataLocal.RECORD_FIX_SIZE + 50) {
				// GOOD MATCH SINCE THE HOLE IS BIG ENOUGH ALSO FOR ANOTHER RECORD: UPDATE THE HOLE WITH THE DIFFERENCE
				final long pos = hole.dataOffset;
				OProfiler.getInstance().stopChrono(PROFILER_DATA_RECYCLED_PARTIAL, timer);
				updateHole(hole, hole.dataOffset + iRecordSize, hole.size - iRecordSize);
				return pos;
			}
		}

		OProfiler.getInstance().stopChrono(PROFILER_DATA_RECYCLED_NOTFOUND, timer);

		return -1;
	}

	/**
	 * Fills the holes information into OPhysicalPosition object given as parameter.
	 * 
	 * @return true, if it's a valid hole, otherwise false
	 * @throws IOException
	 */
	public ODataHoleInfo getHole(final int iPosition) {
		final ODataHoleInfo hole = availableHolesList.get(iPosition);
		if (hole.dataOffset == -1)
			return null;
		return hole;
	}

	/**
	 * Update hole data
	 * 
	 * @param iUpdateFromMemory
	 * 
	 * @throws IOException
	 */
	public void updateHole(final ODataHoleInfo iHole, final long iNewDataOffset, final int iNewRecordSize) throws IOException {
		final long timer = OProfiler.getInstance().startChrono();

		final boolean offsetChanged = iNewDataOffset != iHole.dataOffset;
		final boolean sizeChanged = iNewRecordSize != iHole.size;

		if (maxHoleSize < iNewRecordSize)
			maxHoleSize = iNewRecordSize;

		// IN MEMORY
		if (offsetChanged)
			availableHolesByPosition.remove(iHole);
		if (sizeChanged)
			availableHolesBySize.remove(iHole);

		if (offsetChanged)
			iHole.dataOffset = iNewDataOffset;
		if (sizeChanged)
			iHole.size = iNewRecordSize;

		if (offsetChanged)
			availableHolesByPosition.put(iHole, iHole);
		if (sizeChanged)
			availableHolesBySize.put(iHole, iHole);

		// TO FILE
		final long holePosition = iHole.holeOffset * RECORD_SIZE;
		if (offsetChanged)
			file.writeLong(holePosition, iNewDataOffset);
		if (sizeChanged)
			file.writeInt(holePosition + OBinaryProtocol.SIZE_LONG, iNewRecordSize);

		OProfiler.getInstance().stopChrono(PROFILER_DATA_HOLE_UPDATE, timer);
	}

	/**
	 * Delete the hole
	 * 
	 * @param iRemoveAlsoFromMemory
	 * 
	 * @throws IOException
	 */
	public void deleteHole(int iHolePosition) throws IOException {
		// IN MEMORY
		final ODataHoleInfo hole = availableHolesList.get(iHolePosition);
		availableHolesBySize.remove(hole);
		availableHolesByPosition.remove(hole);

		hole.dataOffset = -1;
		freeHoles.add(iHolePosition);

		// TO FILE
		iHolePosition = iHolePosition * RECORD_SIZE;
		file.writeLong(iHolePosition, -1);
	}

	public int getHoles() {
		return (file.getFilledUpTo() / RECORD_SIZE);
	}

	private void loadHolesInMemory() throws IOException {
		final int holes = getHoles();

		for (int pos = 0; pos < holes; ++pos) {
			final long dataOffset = file.readLong(pos * RECORD_SIZE);
			final int recordSize = file.readInt(pos * RECORD_SIZE + OBinaryProtocol.SIZE_LONG);

			final ODataHoleInfo hole = new ODataHoleInfo(recordSize, dataOffset, pos);

			availableHolesList.add(hole);

			if (dataOffset == -1)
				freeHoles.add(pos);
			else {
				availableHolesBySize.put(hole, hole);
				availableHolesByPosition.put(hole, hole);

				if (maxHoleSize < recordSize)
					maxHoleSize = recordSize;
			}
		}
	}
}
