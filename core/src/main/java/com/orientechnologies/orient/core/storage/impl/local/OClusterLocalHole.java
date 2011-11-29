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

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.config.OStorageFileConfiguration;
import com.orientechnologies.orient.core.memory.OMemoryWatchDog;

/**
 * Handles the holes inside cluster segments. The synchronization is in charge to the OClusterLocal instance.<br/>
 * <br/>
 * Record structure:<br/>
 * <br/>
 * +----------------------+<br/>
 * | DATA OFFSET......... |<br/>
 * | 8 bytes = max 2^63-1 |<br/>
 * +----------------------+<br/>
 * = 8 bytes<br/>
 */
public class OClusterLocalHole extends OSingleFileSegment {
	private static final int	DEF_START_SIZE	= 262144;
	private static final int	RECORD_SIZE			= 8;

	private OClusterLocal			owner;

	public OClusterLocalHole(final OClusterLocal iClusterLocal, final OStorageLocal iStorage, final OStorageFileConfiguration iConfig)
			throws IOException {
		super(iStorage, iConfig);
		owner = iClusterLocal;
	}

	/**
	 * TODO Check values removing dirty entries (equals to -1)
	 */
	public void defrag() throws IOException {
		OLogManager.instance().debug(this, "Starting to defragment the segment %s of size=%d and filled=%d", file, file.getFileSize(),
				file.getFilledUpTo());

		OLogManager.instance().debug(this, "Defragmentation ended for segment %s. Current size=%d and filled=%d", file,
				file.getFileSize(), file.getFilledUpTo());
	}

	public void create() throws IOException {
		file.create(DEF_START_SIZE);
	}

	/**
	 * Append the hole to the end of segment
	 * 
	 * @throws IOException
	 */
	public long pushPosition(final long iPosition) throws IOException {
		final int position = getHoles() * RECORD_SIZE;
		file.allocateSpace(RECORD_SIZE);

		file.writeLong(position, iPosition);

		if (OLogManager.instance().isDebugEnabled())
			OLogManager.instance().debug(this, "Pushed new hole %s/#%d -> #%d:%d", owner.getName(), position / RECORD_SIZE,
					owner.getId(), iPosition);

		return position;
	}

	/**
	 * Return and remove the recycled position if any.
	 * 
	 * @return the recycled position if found, otherwise -1 that usually means to request more space.
	 * @throws IOException
	 */
	public long popLastEntryPosition() throws IOException {
		// BROWSE IN ASCENDING ORDER UNTIL A GOOD POSITION IS FOUND (!=-1)
		for (int pos = getHoles() - 1; pos >= 0; --pos) {
			final long recycledPosition = file.readLong(pos * RECORD_SIZE);

			if (recycledPosition > -1) {
				if (OLogManager.instance().isDebugEnabled())
					OLogManager.instance().debug(this, "Recycled hole %s/#%d -> #%d:%d", owner.getName(), pos, owner.getId(),
							recycledPosition);

				// SHRINK THE FILE
				file.removeTail((getHoles() - pos) * RECORD_SIZE);

				return recycledPosition;
			}
		}

		return -1;
	}

	/**
	 * Return the recycled position if any.
	 * 
	 * @return the recycled position if found, otherwise -1 that usually means to request more space.
	 * @throws IOException
	 */
	public long getEntryPosition(final long iPosition) throws IOException {
		return file.readLong(iPosition * RECORD_SIZE);
	}

	/**
	 * Remove a hole. Called on transaction recover to invalidate a delete for a record. Try to shrink the file if the invalidated
	 * entry is not in the middle of valid entries.
	 * 
	 * @param iPosition
	 *          Record position to find and invalidate
	 * @return
	 * @throws IOException
	 */
	public boolean removeEntryWithPosition(final long iPosition) throws IOException {
		// BROWSE IN ASCENDING ORDER UNTIL THE REQUESTED POSITION IS FOUND
		boolean canShrink = true;
		for (int pos = getHoles() - 1; pos >= 0; --pos) {
			final long recycledPosition = file.readLong(pos * RECORD_SIZE);

			if (recycledPosition == iPosition) {
				if (OLogManager.instance().isDebugEnabled())
					OLogManager.instance().debug(this, "Removing hole #%d containing the position #%d:%d", pos, owner.getId(),
							recycledPosition);

				file.writeLong(pos * RECORD_SIZE, -1);
				if (canShrink)
					// SHRINK THE FILE
					file.removeTail((getHoles() - pos) * RECORD_SIZE);

				return true;

			} else if (iPosition != -1)
				// NO NULL ENTRY: CAN'T SHRINK WITHOUT LOST OF ENTRIES
				canShrink = false;
		}
		return false;
	}

	public void rename(String iOldName, String iNewName) {
		final File osFile = file.getOsFile();
		if (osFile.getName().startsWith(iOldName)) {
			final File newFile = new File(storage.getStoragePath() + "/" + iNewName
					+ osFile.getName().substring(osFile.getName().lastIndexOf(iOldName) + iOldName.length()));
			boolean renamed = osFile.renameTo(newFile);
			while (!renamed) {
				OMemoryWatchDog.freeMemory(100);
				renamed = osFile.renameTo(newFile);
			}
		}
	}

	/**
	 * Compute the number of holes. Note that not all the holes could be valid.
	 * 
	 * @return
	 */
	public int getHoles() {
		return file.getFilledUpTo() / RECORD_SIZE;
	}
}
