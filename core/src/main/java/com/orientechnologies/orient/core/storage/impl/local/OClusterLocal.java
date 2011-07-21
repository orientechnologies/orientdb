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

import java.io.File;
import java.io.IOException;

import com.orientechnologies.orient.core.OConstants;
import com.orientechnologies.orient.core.OMemoryWatchDog;
import com.orientechnologies.orient.core.config.OStorageClusterHoleConfiguration;
import com.orientechnologies.orient.core.config.OStorageFileConfiguration;
import com.orientechnologies.orient.core.config.OStoragePhysicalClusterConfiguration;
import com.orientechnologies.orient.core.storage.OCluster;
import com.orientechnologies.orient.core.storage.OClusterPositionIterator;
import com.orientechnologies.orient.core.storage.OPhysicalPosition;
import com.orientechnologies.orient.core.storage.fs.OFile;
import com.orientechnologies.orient.core.storage.fs.OMMapManager;

/**
 * Handles the table to resolve logical address to physical address. Deleted records have version = -1. <br/><br/>
 * Record structure:<br/>
 * <code>
 * +----------------------+----------------------+-------------+----------------------+<br/>
 * | DATA SEGMENT........ | DATA OFFSET......... | RECORD TYPE | VERSION............. |<br/>
 * | 2 bytes = max 2^15-1 | 8 bytes = max 2^63-1 | 1 byte..... | 4 bytes = max 2^31-1 |<br/>
 * +----------------------+----------------------+-------------+----------------------+<br/>
 * = 15 bytes
 * </code><br/>
 */
public class OClusterLocal extends OMultiFileSegment implements OCluster {
	private static final String				DEF_EXTENSION		= ".ocl";
	private static final int					RECORD_SIZE			= 15;
	private static final int					DEF_SIZE				= 1000000;
	public static final String				TYPE						= "PHYSICAL";

	private int												id;
	private long											beginOffsetData	= -1;
	private long											endOffsetData		= -1;				// end of data offset. -1 = latest

	protected final OClusterLocalHole	holeSegment;

	public OClusterLocal(final OStorageLocal iStorage, final OStoragePhysicalClusterConfiguration iConfig) throws IOException {
		super(iStorage, iConfig, DEF_EXTENSION, RECORD_SIZE);
		id = iConfig.getId();

		iConfig.holeFile = new OStorageClusterHoleConfiguration(iConfig, OStorageVariableParser.DB_PATH_VARIABLE + "/" + iConfig.name,
				iConfig.fileType, iConfig.fileMaxSize);

		holeSegment = new OClusterLocalHole(this, iStorage, iConfig.holeFile);
	}

	@Override
	public void create(int iStartSize) throws IOException {
		acquireExclusiveLock();
		try {

			if (iStartSize == -1)
				iStartSize = DEF_SIZE;

			super.create(iStartSize);
			holeSegment.create();

			files[0].writeHeaderLong(0, beginOffsetData);
			files[0].writeHeaderLong(OConstants.SIZE_LONG, beginOffsetData);

		} finally {
			releaseExclusiveLock();
		}
	}

	@Override
	public void open() throws IOException {
		acquireExclusiveLock();
		try {

			super.open();
			holeSegment.open();

			beginOffsetData = files[0].readHeaderLong(0);
			endOffsetData = files[0].readHeaderLong(OConstants.SIZE_LONG);

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
	public void delete() throws IOException {
		acquireExclusiveLock();
		try {

			truncate();
			for (OFile file : files) {
				OMMapManager.removeFile(file);
				file.delete();
			}
			files = null;
			holeSegment.delete();

		} finally {
			releaseExclusiveLock();
		}
	}

	@Override
	public void truncate() throws IOException {
		acquireExclusiveLock();
		try {

			// REMOVE ALL DATA BLOCKS
			final long tot = getEntries();
			final OPhysicalPosition ppos = new OPhysicalPosition();
			for (long i = 0; i < tot; ++i) {
				getPhysicalPosition(i, ppos);

				if (storage.checkForRecordValidity(ppos))
					storage.getDataSegment(ppos.dataSegment).deleteRecord(ppos.dataPosition);
			}

			super.truncate();
			holeSegment.truncate();

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
			setNameInternal(stringValue);
		}

	}

	/**
	 * Fills and return the PhysicalPosition object received as parameter with the physical position of logical record iPosition
	 * 
	 * @throws IOException
	 */
	public OPhysicalPosition getPhysicalPosition(long iPosition, final OPhysicalPosition iPPosition) throws IOException {
		iPosition = iPosition * RECORD_SIZE;

		acquireSharedLock();
		try {

			final long[] pos = getRelativePosition(iPosition);

			final OFile file = files[(int) pos[0]];
			long p = pos[1];

			iPPosition.dataSegment = file.readShort(p);
			iPPosition.dataPosition = file.readLong(p += OConstants.SIZE_SHORT);
			iPPosition.type = file.readByte(p += OConstants.SIZE_LONG);
			iPPosition.version = file.readInt(p += OConstants.SIZE_BYTE);
			return iPPosition;

		} finally {
			releaseSharedLock();
		}
	}

	/**
	 * Changes the PhysicalPosition of the logical record iPosition.
	 * 
	 * @throws IOException
	 */
	public void setPhysicalPosition(long iPosition, final int iDataId, final long iDataPosition, final byte iRecordType)
			throws IOException {
		iPosition = iPosition * RECORD_SIZE;

		acquireExclusiveLock();
		try {

			final long[] pos = getRelativePosition(iPosition);

			final OFile file = files[(int) pos[0]];
			long p = pos[1];

			file.writeShort(p, (short) iDataId);
			file.writeLong(p += OConstants.SIZE_SHORT, iDataPosition);
			file.writeByte(p += OConstants.SIZE_LONG, iRecordType);

		} finally {
			releaseExclusiveLock();
		}
	}

	/**
	 * Update position in data segment (usually on defrag)
	 * 
	 * @throws IOException
	 */
	public void setPhysicalPosition(long iPosition, final long iDataPosition) throws IOException {
		iPosition = iPosition * RECORD_SIZE;

		acquireExclusiveLock();
		try {

			final long[] pos = getRelativePosition(iPosition);

			final OFile file = files[(int) pos[0]];
			long p = pos[1];

			file.writeLong(p += OConstants.SIZE_SHORT, iDataPosition);

		} finally {
			releaseExclusiveLock();
		}
	}

	public void updateVersion(long iPosition, final int iVersion) throws IOException {
		iPosition = iPosition * RECORD_SIZE;

		acquireExclusiveLock();
		try {

			final long[] pos = getRelativePosition(iPosition);

			files[(int) pos[0]].writeInt(pos[1] + OConstants.SIZE_SHORT + OConstants.SIZE_LONG + OConstants.SIZE_BYTE, iVersion);

		} finally {
			releaseExclusiveLock();
		}
	}

	public void updateRecordType(long iPosition, final byte iRecordType) throws IOException {
		iPosition = iPosition * RECORD_SIZE;

		acquireExclusiveLock();
		try {

			final long[] pos = getRelativePosition(iPosition);

			files[(int) pos[0]].writeByte(pos[1] + OConstants.SIZE_SHORT + OConstants.SIZE_LONG, iRecordType);

		} finally {
			releaseExclusiveLock();
		}
	}

	/**
	 * Remove the Logical position entry. Add to the hole segment and change the version to -1.
	 * 
	 * @throws IOException
	 */
	public void removePhysicalPosition(long iPosition, final OPhysicalPosition iPPosition) throws IOException {
		final long position = iPosition * RECORD_SIZE;

		acquireExclusiveLock();
		try {

			final long[] pos = getRelativePosition(position);
			final OFile file = files[(int) pos[0]];
			long p = pos[1];

			// SAVE THE OLD DATA AND RETRIEVE THEM TO THE CALLER
			iPPosition.dataSegment = file.readShort(p);
			iPPosition.dataPosition = file.readLong(p += OConstants.SIZE_SHORT);
			iPPosition.type = file.readByte(p += OConstants.SIZE_LONG);
			iPPosition.version = file.readInt(p += OConstants.SIZE_BYTE);

			holeSegment.pushPosition(position);

			// SET VERSION = -1
			file.writeInt(p, -1);

			if (iPosition == beginOffsetData) {
				if (getEntries() == 0)
					beginOffsetData = -1;
				else {
					// DISCOVER THE BEGIN OF DATA
					beginOffsetData++;

					long[] fetchPos;
					for (long currentPos = position + RECORD_SIZE; currentPos < getFilledUpTo(); currentPos += RECORD_SIZE) {
						fetchPos = getRelativePosition(currentPos);

						if (files[(int) fetchPos[0]].readShort(fetchPos[1]) != -1)
							// GOOD RECORD: SET IT AS BEGIN
							break;

						beginOffsetData++;
					}
				}

				files[0].writeHeaderLong(0, beginOffsetData);
			}

			if (iPosition == endOffsetData) {
				if (getEntries() == 0)
					endOffsetData = -1;
				else {
					// DISCOVER THE END OF DATA
					endOffsetData--;

					long[] fetchPos;
					for (long currentPos = position - RECORD_SIZE; currentPos >= beginOffsetData; currentPos -= RECORD_SIZE) {

						fetchPos = getRelativePosition(currentPos);

						if (files[(int) fetchPos[0]].readShort(fetchPos[1]) != -1)
							// GOOD RECORD: SET IT AS BEGIN
							break;
						endOffsetData--;
					}
				}

				files[0].writeHeaderLong(OConstants.SIZE_LONG, endOffsetData);
			}

		} finally {
			releaseExclusiveLock();
		}
	}

	public boolean removeHole(final long iPosition) throws IOException {
		acquireExclusiveLock();
		try {

			return holeSegment.removeEntryWithPosition(iPosition);

		} finally {
			releaseExclusiveLock();
		}
	}

	/**
	 * Adds a new entry.
	 * 
	 * @throws IOException
	 */
	public long addPhysicalPosition(final int iDataSegmentId, final long iPosition, final byte iRecordType) throws IOException {
		acquireExclusiveLock();
		try {

			long offset = holeSegment.popLastEntryPosition();

			final long[] pos;
			if (offset > -1)
				// REUSE THE HOLE
				pos = getRelativePosition(offset);
			else {
				// NO HOLES FOUND: ALLOCATE MORE SPACE
				pos = allocateSpace(RECORD_SIZE);
				offset = getAbsolutePosition(pos);
			}

			OFile file = files[(int) pos[0]];
			long p = pos[1];

			file.writeShort(p, (short) iDataSegmentId);
			file.writeLong(p += OConstants.SIZE_SHORT, iPosition);
			file.writeByte(p += OConstants.SIZE_LONG, iRecordType);
			file.writeInt(p += OConstants.SIZE_BYTE, 0);

			final long returnedPosition = offset / RECORD_SIZE;

			if (returnedPosition < beginOffsetData || beginOffsetData == -1) {
				// UPDATE END OF DATA
				beginOffsetData = returnedPosition;
				files[0].writeHeaderLong(0, beginOffsetData);
			}

			if (endOffsetData > -1 && returnedPosition > endOffsetData) {
				// UPDATE END OF DATA
				endOffsetData = returnedPosition;
				files[0].writeHeaderLong(OConstants.SIZE_LONG, endOffsetData);
			}

			return returnedPosition;

		} finally {
			releaseExclusiveLock();
		}
	}

	public long getFirstEntryPosition() throws IOException {
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
	public long getLastEntryPosition() throws IOException {
		acquireSharedLock();
		try {

			return endOffsetData > -1 ? endOffsetData : getFilledUpTo() / RECORD_SIZE - 1;

		} finally {
			releaseSharedLock();
		}
	}

	public long getEntries() {
		acquireSharedLock();
		try {

			return getFilledUpTo() / RECORD_SIZE - holeSegment.getHoles();

		} finally {
			releaseSharedLock();
		}
	}

	public int getId() {
		return id;
	}

	public OClusterPositionIterator absoluteIterator() throws IOException {
		return new OClusterPositionIterator(this);
	}

	public OClusterPositionIterator absoluteIterator(long iBeginRange, long iEndRange) throws IOException {
		return new OClusterPositionIterator(this, iBeginRange, iEndRange);
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

	public long getRecordsSize() throws IOException {
		long size = 0l;
		OClusterPositionIterator it = absoluteIterator();
		OPhysicalPosition pos = new OPhysicalPosition();
		while (it.hasNext()) {
			Long position = it.next();
			pos = getPhysicalPosition(position.longValue(), pos);
			if (pos.dataPosition > -1)
				size += storage.getDataSegment(pos.dataSegment).getRecordSize(pos.dataPosition);
		}
		return size;
	}

	private void setNameInternal(String iNewName) {
		if (storage.getClusterIdByName(iNewName) > -1)
			throw new IllegalArgumentException("Cluster with name '" + iNewName + "' already exists");
		acquireExclusiveLock();
		try {
			for (int i = 0; i < files.length; i++) {
				final File osFile = files[i].getOsFile();
				if (osFile.getName().startsWith(name)) {
					final File newFile = new File(storage.getStoragePath() + "/" + iNewName
							+ osFile.getName().substring(osFile.getName().lastIndexOf(name) + name.length()));
					for (OStorageFileConfiguration conf : config.infoFiles) {
						if (conf.parent.name.equals(name))
							conf.parent.name = iNewName;
						if (conf.path.endsWith(osFile.getName()))
							conf.path = new String(conf.path.replace(osFile.getName(), newFile.getName()));
					}
					boolean renamed = osFile.renameTo(newFile);
					while (!renamed) {
						OMemoryWatchDog.freeMemory(100);
						renamed = osFile.renameTo(newFile);
					}
				}
			}
			config.name = iNewName;
			holeSegment.rename(name, iNewName);
			storage.renameCluster(name, iNewName);
			name = iNewName;
			storage.getConfiguration().update();
		} finally {
			releaseExclusiveLock();
		}

	}
}
