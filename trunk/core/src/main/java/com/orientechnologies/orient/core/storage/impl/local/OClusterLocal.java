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
 * +----------------------+----------------------+----------------------+<br/>
 * | DATA SEGMENT........ | DATA OFFSET......... | VERSION............. |<br/>
 * | 2 bytes = max 2^15-1 | 8 bytes = max 2^63-1 | 4 bytes = max 2^31-1 |<br/>
 * +----------------------+----------------------+----------------------+<br/>
 * = 14 bytes<br/>
 */
public class OClusterLocal extends OMultiFileSegment implements OCluster {
	private static final String				DEF_EXTENSION	= ".ocl";
	private static final int					RECORD_SIZE		= 14;
	private final int									id;
	protected final OClusterLocalHole	holeSegment;

	public OClusterLocal(final OStorageLocal iStorage, final OStoragePhysicalClusterConfiguration iConfig, final int iId,
			final String iClusterName) throws IOException {
		super(iStorage, iConfig, DEF_EXTENSION, RECORD_SIZE);
		id = iId;

		iConfig.holeFile = new OStorageClusterHoleConfiguration(iConfig, OStorageVariableParser.DB_PATH_VARIABLE + "/" + iClusterName,
				iConfig.fileType, iConfig.fileMaxSize);

		holeSegment = new OClusterLocalHole(this, iStorage, iConfig.holeFile);
	}

	@Override
	public void create(int iStartSize) throws IOException {
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

			iPPosition.dataSegment = files[pos[0]].readShort(pos[1]);
			iPPosition.dataPosition = files[pos[0]].readLong(pos[1] + OConstants.SIZE_SHORT);
			iPPosition.version = files[pos[0]].readInt(pos[1] + OConstants.SIZE_SHORT + OConstants.SIZE_LONG);
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
	public void setPhysicalPosition(long iPosition, final int iDataId, final long iDataPosition) throws IOException {
		iPosition = iPosition * RECORD_SIZE;

		try {
			acquireExclusiveLock();

			int[] pos = getRelativePosition(iPosition);

			files[pos[0]].writeShort(pos[1], (short) iDataId);
			files[pos[0]].writeLong(pos[1] + OConstants.SIZE_SHORT, iDataPosition);

		} finally {
			releaseExclusiveLock();
		}
	}

	public void updateVersion(long iPosition, final int iVersion) throws IOException {
		iPosition = iPosition * RECORD_SIZE;

		try {
			acquireExclusiveLock();

			int[] pos = getRelativePosition(iPosition);

			files[pos[0]].writeInt(pos[1] + OConstants.SIZE_SHORT + OConstants.SIZE_LONG, iVersion);

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

			// SAVE THE OLD DATA AND RETRIEVE THEM TO THE CALLER
			iPPosition.dataSegment = file.readShort(pos[1]);
			iPPosition.dataPosition = file.readLong(pos[1] + OConstants.SIZE_SHORT);
			iPPosition.version = file.readInt(pos[1] + OConstants.SIZE_SHORT + OConstants.SIZE_LONG);

			holeSegment.pushPosition(iPosition);

			file.writeInt(pos[1] + OConstants.SIZE_SHORT + OConstants.SIZE_LONG, -1);
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
	public long addPhysicalPosition(final int iDataSegmentId, final long iPosition) throws IOException {
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

			file.writeShort(pos[1], (short) iDataSegmentId);
			file.writeLong(pos[1] + OConstants.SIZE_SHORT, iPosition);
			file.writeInt(pos[1] + OConstants.SIZE_SHORT + OConstants.SIZE_LONG, 0);

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
}
