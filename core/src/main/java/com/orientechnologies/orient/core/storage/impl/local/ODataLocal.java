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

import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.orient.core.OConstants;
import com.orientechnologies.orient.core.config.OStorageDataConfiguration;
import com.orientechnologies.orient.core.config.OStorageDataHoleConfiguration;
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
	private static final int				RECORD_FIX_SIZE	= 14;
	protected final int							id;
	protected final ODataLocalHole	holeSegment;

	public ODataLocal(final OStorageLocal iStorage, final OStorageDataConfiguration iConfig, final int iId) throws IOException {
		super(iStorage, iConfig, DEF_EXTENSION, 0);
		id = iId;

		iConfig.holeFile = new OStorageDataHoleConfiguration(iConfig, OStorageVariableParser.DB_PATH_VARIABLE + "/" + name,
				iConfig.fileType, iConfig.fileMaxSize);
		holeSegment = new ODataLocalHole(iStorage, iConfig.holeFile);
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

	public long getAvailablePosition(int iSize) throws IOException {
		// TODO: REUSE SPACE FROM THE HOLE FILE
		return getAbsolutePosition(allocateSpace(iSize + RECORD_FIX_SIZE));
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
		try {
			acquireExclusiveLock();

			final int[] newFilePosition = allocateSpace(iContent.length + RECORD_FIX_SIZE);
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
		try {
			acquireSharedLock();

			final int[] pos = getRelativePosition(iPosition);
			final OFile file = files[pos[0]];

			final int recordSize = file.readInt(pos[1]);
			if (recordSize <= 0)
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
		try {
			acquireSharedLock();

			final int[] pos = getRelativePosition(iPosition);
			final OFile file = files[pos[0]];

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
		try {
			acquireExclusiveLock();

			int[] pos = getRelativePosition(iPosition);
			final OFile file = files[pos[0]];

			final int recordSize = file.readInt(pos[1]);
//			if (recordSize <= 0)
//				OLogManager.instance().error(this, "Error while writing to data file. The record size was invalid", OIOException.class);

			if (iContent.length == recordSize) {
				// USE THE OLD SPACE SINCE SIZE IT ISN'T CHANGED
				file.write(pos[1] + RECORD_FIX_SIZE, iContent);

				OProfiler.getInstance().updateStatistic("ODataLocal.setRecord:tot.reused.space", +1);
			} else if (iContent.length < recordSize) {
				// USE THE OLD SPACE BUT UPDATE THE CURRENT SIZE. IT'S PREFEREABLE TO USE THE SAME INSTEAD FINDING A BEST SUITED FOR IT TO
				// AVOID CHANGES TO REF FILE AS WELL.
				writeRecord(pos, iClusterSegment, iClusterPosition, iContent);

				// CREATE A HOLE WITH THE DIFFERENCE OF SPACE
				holeSegment.createHole(iPosition + RECORD_FIX_SIZE + iContent.length, iContent.length - recordSize);

				OProfiler.getInstance().updateStatistic("ODataLocal.setRecord:part.reused.space", +1);
			} else {
				// USE A NEW SPACE
				pos = allocateSpace(iContent.length + RECORD_FIX_SIZE);
				writeRecord(pos, iClusterSegment, iClusterPosition, iContent);

				// CREATE A HOLE FOR THE ENTIRE OLD RECORD
				holeSegment.createHole(iPosition, recordSize);

				OProfiler.getInstance().updateStatistic("ODataLocal.setRecord:new.space", +1);
			}

			return getAbsolutePosition(pos);

		} finally {
			releaseExclusiveLock();
		}
	}

	public int deleteRecord(final long iPosition) throws IOException {
		try {
			acquireExclusiveLock();

			final int[] pos = getRelativePosition(iPosition);
			final OFile file = files[pos[0]];

			final int recordSize = file.readInt(pos[1]);
			if (recordSize > 0) {
				// VALID RECORD: CREATE A HOLE FOR IT
				file.writeInt(pos[1], 0);

				holeSegment.createHole(iPosition, recordSize);
			}
			return recordSize;

		} finally {
			releaseExclusiveLock();
		}
	}

	protected void writeRecord(final int[] iFilePosition, final int iClusterSegment, final long iClusterPosition,
			final byte[] iContent) throws IOException {
		final OFile file = files[iFilePosition[0]];

		file.writeInt(iFilePosition[1], iContent.length);
		file.writeShort(iFilePosition[1] + OConstants.SIZE_INT, (short) iClusterSegment);
		file.writeLong(iFilePosition[1] + OConstants.SIZE_INT + OConstants.SIZE_SHORT, iClusterPosition);

		file.write(iFilePosition[1] + RECORD_FIX_SIZE, iContent);
	}

	public void createHole(long iRecordOffset, int iRecordSize) throws IOException {
		holeSegment.createHole(iRecordOffset, iRecordSize);
	}

	public int getId() {
		return id;
	}
}
