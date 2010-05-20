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

import com.orientechnologies.orient.core.OConstants;
import com.orientechnologies.orient.core.config.OStorageClusterHoleConfiguration;
import com.orientechnologies.orient.core.config.OStoragePhysicalClusterConfiguration;
import com.orientechnologies.orient.core.storage.OCluster;
import com.orientechnologies.orient.core.storage.OClusterPositionIterator;
import com.orientechnologies.orient.core.storage.OPhysicalPosition;
import com.orientechnologies.orient.core.storage.fs.OFile;

/**
 * Handle the table to resolve logical address to physical address.<br/>
 * <br/>
 * Record structure:<br/>
 * <br/>
 * +----------------------+----------------------+-------------+----------------------+<br/>
 * | DATA SEGMENT........ | DATA OFFSET......... | RECORD TYPE | VERSION............. |<br/>
 * | 2 bytes = max 2^15-1 | 8 bytes = max 2^63-1 | 1 byte..... | 4 bytes = max 2^31-1 |<br/>
 * +----------------------+----------------------+-------------+----------------------+<br/>
 * = 15 bytes<br/>
 */
public class OClusterPhysical extends OMultiFileSegment implements OCluster {
	private static final String				DEF_EXTENSION	= ".ocl";
	private static final int					RECORD_SIZE		= 15;
	private static final int					DEF_SIZE			= 1000000;
	public static final String				TYPE					= "PHYSICAL";

	private int												id;

	protected final OClusterLocalHole	holeSegment;

	public OClusterPhysical(final OStorageLocal iStorage, final OStoragePhysicalClusterConfiguration iConfig) throws IOException {
		super(iStorage, iConfig, DEF_EXTENSION, RECORD_SIZE);
		id = iConfig.getId();

		iConfig.holeFile = new OStorageClusterHoleConfiguration(iConfig, OStorageVariableParser.DB_PATH_VARIABLE + "/" + iConfig.name,
				iConfig.fileType, iConfig.fileMaxSize);

		holeSegment = new OClusterLocalHole(this, iStorage, iConfig.holeFile);
	}

	@Override
	public void create(int iStartSize) throws IOException {
		if (iStartSize == -1)
			iStartSize = DEF_SIZE;

		super.create(iStartSize);
		holeSegment.create();
	}

	@Override
	public void open() throws IOException {
		try {
			acquireExclusiveLock();

			super.open();
			holeSegment.open();

		} finally {

			releaseExclusiveLock();
		}
	}

	@Override
	public void close() throws IOException {
		super.close();
		holeSegment.close();
	}

	/**
	 * Fill and return the PhysicalPosition object received as parameter with the physical position of logical record iPosition
	 * 
	 * @throws IOException
	 */
	public OPhysicalPosition getPhysicalPosition(long iPosition, final OPhysicalPosition iPPosition) throws IOException {
		iPosition = iPosition * RECORD_SIZE;

		try {
			acquireSharedLock();

			int[] pos = getRelativePosition(iPosition);

			int p = pos[1];

			iPPosition.dataSegment = files[pos[0]].readShort(p);
			iPPosition.dataPosition = files[pos[0]].readLong(p += OConstants.SIZE_SHORT);
			iPPosition.type = files[pos[0]].readByte(p += OConstants.SIZE_LONG);
			iPPosition.version = files[pos[0]].readInt(p += OConstants.SIZE_BYTE);
			return iPPosition;

		} finally {
			releaseSharedLock();
		}
	}

	/**
	 * Change the PhysicalPosition of the logical record iPosition.
	 * 
	 * @throws IOException
	 */
	public void setPhysicalPosition(long iPosition, final int iDataId, final long iDataPosition, final byte iRecordType)
			throws IOException {
		iPosition = iPosition * RECORD_SIZE;

		try {
			acquireExclusiveLock();

			int[] pos = getRelativePosition(iPosition);

			int p = pos[1];

			files[pos[0]].writeShort(p, (short) iDataId);
			files[pos[0]].writeLong(p += OConstants.SIZE_SHORT, iDataPosition);
			files[pos[0]].writeByte(p += OConstants.SIZE_LONG, iRecordType);

		} finally {
			releaseExclusiveLock();
		}
	}

	public void updateVersion(long iPosition, final int iVersion) throws IOException {
		iPosition = iPosition * RECORD_SIZE;

		try {
			acquireExclusiveLock();

			int[] pos = getRelativePosition(iPosition);

			files[pos[0]].writeInt(pos[1] + OConstants.SIZE_SHORT + OConstants.SIZE_LONG + OConstants.SIZE_BYTE, iVersion);

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
		iPosition = iPosition * RECORD_SIZE;

		try {
			acquireExclusiveLock();

			int[] pos = getRelativePosition(iPosition);
			OFile file = files[pos[0]];
			int p = pos[1];

			// SAVE THE OLD DATA AND RETRIEVE THEM TO THE CALLER
			iPPosition.dataSegment = file.readShort(p);
			iPPosition.dataPosition = file.readLong(p += OConstants.SIZE_SHORT);
			iPPosition.type = file.readByte(p += OConstants.SIZE_LONG);
			iPPosition.version = file.readInt(p += OConstants.SIZE_BYTE);

			holeSegment.pushPosition(iPosition);

			file.writeInt(p, -1);
		} finally {
			releaseExclusiveLock();
		}
	}

	public boolean removeHole(long iPosition) throws IOException {
		return holeSegment.removeEntryWithPosition(iPosition);
	}

	/**
	 * Add a new entry.
	 * 
	 * @throws IOException
	 */
	public long addPhysicalPosition(final int iDataSegmentId, final long iPosition, final byte iRecordType) throws IOException {
		try {
			acquireExclusiveLock();

			long offset = holeSegment.popLastEntryPosition();

			final int[] pos;
			if (offset > -1)
				// REUSE THE HOLE
				pos = getRelativePosition(offset);
			else {
				// NO HOLES FOUND: ALLOCATE MORE SPACE
				pos = allocateSpace(RECORD_SIZE);
				offset = getAbsolutePosition(pos);
			}

			OFile file = files[pos[0]];
			int p = pos[1];

			file.writeShort(p, (short) iDataSegmentId);
			file.writeLong(p += OConstants.SIZE_SHORT, iPosition);
			file.writeByte(p += OConstants.SIZE_LONG, iRecordType);
			file.writeInt(p += OConstants.SIZE_BYTE, 0);

			return offset / RECORD_SIZE;

		} finally {
			releaseExclusiveLock();
		}
	}

	public long getElements() {
		try {
			acquireSharedLock();

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
}
