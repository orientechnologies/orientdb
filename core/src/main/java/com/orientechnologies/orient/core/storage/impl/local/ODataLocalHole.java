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

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.orient.core.OConstants;
import com.orientechnologies.orient.core.config.OStorageFileConfiguration;
import com.orientechnologies.orient.core.storage.OPhysicalPosition;

/**
 * Handles the holes inside data segments. Exists only 1 hole segment per data-segment even if multiple data-files are configured.
 * The synchronization is in charge to the ODataSegment instance.<br/>
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
	private static final int	DEF_START_SIZE	= 262144;
	private static final int	RECORD_SIZE			= 12;
	private int								maxHoleSize			= -1;

	public ODataLocalHole(final OStorageLocal iStorage, final OStorageFileConfiguration iConfig) throws IOException {
		super(iStorage, iConfig);
	}

	@Override
	public void create(final int iStartSize) throws IOException {
		super.create(iStartSize > -1 ? iStartSize : DEF_START_SIZE);
	}

	/**
	 * Appends the hole to the end of segment
	 * 
	 * @throws IOException
	 */
	public void createHole(final long iRecordOffset, final int iRecordSize) throws IOException {
		// SEARCH A FREE HOLE (WITH POSITION = -1)
		long recycledPosition = -1;
		int holes = getHoles();
		for (int pos = 0; pos < holes; ++pos) {
			if (file.readLong(pos * RECORD_SIZE) == -1) {
				recycledPosition = pos * RECORD_SIZE;
				break;
			}
		}

		if (recycledPosition == -1) {
			// APPEND A NEW ONE
			recycledPosition = holes * RECORD_SIZE;
			file.allocateSpace(RECORD_SIZE);
		}

		file.writeLong(recycledPosition, iRecordOffset);
		file.writeInt(recycledPosition + OConstants.SIZE_LONG, iRecordSize);

		if (maxHoleSize < iRecordSize)
			maxHoleSize = iRecordSize;
	}

	/**
	 * Returns the first available hole (at least iRecordSize length) to be reused.
	 * 
	 * @return
	 * 
	 * @throws IOException
	 */
	public long popFirstAvailableHole(final int iRecordSize) throws IOException {
		if (maxHoleSize > -1 && iRecordSize > maxHoleSize)
			// DON'T BROWSE: NO ONE HOLE WITH THIS SIZE IS AVAILABLE
			return -1;

		final long timer = OProfiler.getInstance().startChrono();

		// BROWSE IN ASCENDING ORDER UNTIL A GOOD POSITION IS FOUND (!=-1)
		int tempMaxHoleSize = 0;
		int holes = getHoles();
		for (int pos = 0; pos < holes; ++pos) {
			final long recycledPosition = file.readLong(pos * RECORD_SIZE);

			if (recycledPosition > -1) {
				// VALID HOLE
				final int recordSize = file.readInt(pos * RECORD_SIZE + OConstants.SIZE_LONG);

				if (recordSize > tempMaxHoleSize)
					tempMaxHoleSize = recordSize;

				if (recordSize == iRecordSize) {
					// PERFECT MATCH: DELETE THE HOLE
					OProfiler.getInstance().stopChrono("Storage.data.recycled.complete", timer);
					deleteHole(pos);
					return recycledPosition;

				} else if (recordSize > iRecordSize + ODataLocal.RECORD_FIX_SIZE + 50) {
					// GOOD MATCH SINCE THE HOLE IS BIG ENOUGH ALSO FOR ANOTHER RECORD: UPDATE THE HOLE WITH THE DIFFERENCE
					OProfiler.getInstance().stopChrono("Storage.data.recycled.partial", timer);
					updateHole(pos, recycledPosition + iRecordSize, recordSize - iRecordSize);
					return recycledPosition;
				}
			}
		}

		maxHoleSize = tempMaxHoleSize;

		OProfiler.getInstance().stopChrono("Storage.data.recycled.notfound", timer);

		return -1;
	}

	/**
	 * Returns best hole as size to avoid extreme defragmentation.
	 * 
	 * @return
	 * 
	 * @throws IOException
	 */
	public long popBestHole(final int iRecordSize) throws IOException {
		if (maxHoleSize > -1 && iRecordSize > maxHoleSize)
			// DON'T BROWSE: NO ONE HOLE WITH THIS SIZE IS AVAILABLE
			return -1;

		final long timer = OProfiler.getInstance().startChrono();

		// BROWSE IN ASCENDING ORDER UNTIL A GOOD POSITION IS FOUND (!=-1)
		int tempMaxHoleSize = -1;
		int bestHoleIndex = -1;
		int bestHoleSizeGap = -1;
		long bestHolePosition = -1;

		final int holes = getHoles();
		for (int pos = 0; pos < holes; ++pos) {
			final long recycledPosition = file.readLong(pos * RECORD_SIZE);

			if (recycledPosition > -1) {
				// VALID HOLE
				final int recordSize = file.readInt(pos * RECORD_SIZE + OConstants.SIZE_LONG);

				if (recordSize > tempMaxHoleSize)
					tempMaxHoleSize = recordSize;

				if (recordSize == iRecordSize) {
					bestHoleIndex = pos;
					bestHolePosition = recycledPosition;
					bestHoleSizeGap = 0;
					tempMaxHoleSize = -1;
					break;
				}

				if (recordSize > iRecordSize && (bestHoleSizeGap == -1 || recordSize - iRecordSize > bestHoleSizeGap)) {
					bestHoleIndex = pos;
					bestHoleSizeGap = recordSize - iRecordSize;
					bestHolePosition = recycledPosition;
				}
			}
		}

		maxHoleSize = tempMaxHoleSize;

		if (bestHoleIndex > -1) {
			if (OLogManager.instance().isDebugEnabled())
				OLogManager.instance().debug(this, "Recycling hole data #%d", bestHoleIndex);

			if (bestHoleSizeGap == 0) {
				// PERFECT MATCH: DELETE THE HOLE
				deleteHole(bestHoleIndex);
				OProfiler.getInstance().stopChrono("Storage.data.recycled.complete", timer);
			} else {
				// UPDATE THE HOLE WITH THE DIFFERENCE
				updateHole(bestHoleIndex, bestHolePosition + iRecordSize, bestHoleSizeGap);
				OProfiler.getInstance().stopChrono("Storage.data.recycled.partial", timer);
			}

			return bestHolePosition;
		}

		OProfiler.getInstance().stopChrono("Storage.data.recycled.notfound", timer);

		return -1;
	}

	/**
	 * Return hole data
	 * 
	 * @throws IOException
	 */
	public OPhysicalPosition getHole(int iPosition, final OPhysicalPosition iPPosition) throws IOException {
		iPosition = iPosition * RECORD_SIZE;
		iPPosition.dataPosition = file.readLong(iPosition);
		iPPosition.recordSize = file.readInt(iPosition + OConstants.SIZE_LONG);
		return iPPosition;
	}

	/**
	 * Update hole data
	 * 
	 * @throws IOException
	 */
	public void updateHole(int iPosition, final long iNewDataPosition, final int iNewRecordSize) throws IOException {
		iPosition = iPosition * RECORD_SIZE;
		file.writeLong(iPosition, iNewDataPosition);
		file.writeInt(iPosition + OConstants.SIZE_LONG, iNewRecordSize);
	}

	/**
	 * Delete the hole
	 * 
	 * @throws IOException
	 */
	public void deleteHole(int iPosition) throws IOException {
		iPosition = iPosition * RECORD_SIZE;
		file.writeLong(iPosition, -1);
	}

	public int getHoles() {
		return (file.getFilledUpTo() / RECORD_SIZE);
	}
}
