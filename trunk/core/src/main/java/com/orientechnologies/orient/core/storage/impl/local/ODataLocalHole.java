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
import com.orientechnologies.orient.core.config.OStorageFileConfiguration;
import com.orientechnologies.orient.core.storage.OPhysicalPosition;

/**
 * Handle the holes inside data segments. Exists only 1 hole segment per data-segment even if multiple data-files are configured.
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

	public ODataLocalHole(final OStorageLocal iStorage, final OStorageFileConfiguration iConfig) throws IOException {
		super(iStorage, iConfig);
	}

	@Override
	public void create(final int iStartSize) throws IOException {
		super.create(iStartSize > -1 ? iStartSize : DEF_START_SIZE);
	}

	/**
	 * Append the hole to the end of segment
	 * 
	 * @throws IOException
	 */
	public void createHole(final long iRecordOffset, final int iRecordSize) throws IOException {
		final int position = getHoles() * RECORD_SIZE;
		file.allocateSpace(RECORD_SIZE);
		file.writeLong(position, iRecordOffset);
		file.writeInt(position + OConstants.SIZE_LONG, iRecordSize);
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
	public void updateHole(int iPosition, final OPhysicalPosition iPPosition) throws IOException {
		iPosition = iPosition * RECORD_SIZE;
		file.writeLong(iPosition, iPPosition.dataPosition);
		file.writeInt(iPosition + OConstants.SIZE_LONG, iPPosition.recordSize);
	}

	/**
	 * Delete the hole TODO: USELESS?
	 * 
	 * @throws IOException
	 */
	public void deleteHole(int iPosition, final OPhysicalPosition iPPosition) throws IOException {
		iPosition = iPosition * RECORD_SIZE;

		iPPosition.dataPosition = file.readLong(iPosition);

		file.writeInt(iPosition, -1);
	}

	public int getHoles() {
		return (file.getFilledUpTo() / RECORD_SIZE);
	}
}
