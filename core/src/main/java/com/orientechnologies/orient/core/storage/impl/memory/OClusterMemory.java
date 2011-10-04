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
package com.orientechnologies.orient.core.storage.impl.memory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.orientechnologies.common.concur.resource.OSharedResourceAbstract;
import com.orientechnologies.orient.core.storage.OCluster;
import com.orientechnologies.orient.core.storage.OClusterPositionIterator;
import com.orientechnologies.orient.core.storage.OPhysicalPosition;

public class OClusterMemory extends OSharedResourceAbstract implements OCluster {
	public static final String			TYPE		= "MEMORY";

	private int											id;
	private String									name;
	private List<OPhysicalPosition>	entries	= new ArrayList<OPhysicalPosition>();
	private List<Integer>						removed	= new ArrayList<Integer>();

	public OClusterMemory(final int id, final String name) {
		this.id = id;
		this.name = name;
	}

	public OClusterPositionIterator absoluteIterator() throws IOException {
		return new OClusterPositionIterator(this);
	}

	public OClusterPositionIterator absoluteIterator(final long iBeginRange, final long iEndRange) throws IOException {
		return new OClusterPositionIterator(this, iBeginRange, iEndRange);
	}

	public void close() {
		acquireExclusiveLock();
		try {

			entries.clear();
			removed.clear();

		} finally {
			releaseExclusiveLock();
		}
	}

	public void open() throws IOException {
	}

	public void create(final int iStartSize) throws IOException {
	}

	public void delete() throws IOException {
		acquireExclusiveLock();
		try {

			close();
			entries.clear();

		} finally {
			releaseExclusiveLock();
		}
	}

	public void truncate() throws IOException {
		acquireExclusiveLock();
		try {

			entries.clear();
			removed.clear();

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
			name = stringValue;
		}
	}

	public long getEntries() {
		acquireSharedLock();
		try {

			return entries.size() - removed.size();

		} finally {
			releaseSharedLock();
		}
	}

	public long getSize() {
		acquireSharedLock();
		try {

			long size = 0;
			for (OPhysicalPosition e : entries)
				if (e != null)
					size += e.recordSize;
			return size;

		} finally {
			releaseSharedLock();
		}
	}

	public long getRecordsSize() throws IOException {
		return getSize();
	}

	public long getFirstEntryPosition() {
		acquireSharedLock();
		try {

			return entries.size() == 0 ? -1 : 0;

		} finally {
			releaseSharedLock();
		}
	}

	public long getLastEntryPosition() {
		acquireSharedLock();
		try {

			return entries.size() - 1;

		} finally {
			releaseSharedLock();
		}
	}

	public int getId() {
		return id;
	}

	public String getName() {
		return name;
	}

	public long getAvailablePosition() throws IOException {
		acquireSharedLock();
		try {

			return entries.size();

		} finally {
			releaseSharedLock();
		}
	}

	public long addPhysicalPosition(final int iDataSegmentId, final long iRecordPosition, final byte iRecordType) {
		acquireExclusiveLock();
		try {

			if (removed.size() > 0) {
				final int recycledPosition = removed.remove(removed.size() - 1);
				entries.set(recycledPosition, new OPhysicalPosition(iDataSegmentId, iRecordPosition, iRecordType));
				return recycledPosition;
			} else {
				entries.add(new OPhysicalPosition(iDataSegmentId, iRecordPosition, iRecordType));
				return entries.size() - 1;
			}

		} finally {
			releaseExclusiveLock();
		}
	}

	public void updateRecordType(final long iPosition, final byte iRecordType) throws IOException {
		acquireExclusiveLock();
		try {

			entries.get((int) iPosition).type = iRecordType;

		} finally {
			releaseExclusiveLock();
		}
	}

	public void updateVersion(long iPosition, int iVersion) throws IOException {
		acquireExclusiveLock();
		try {

			entries.get((int) iPosition).version = iVersion;

		} finally {
			releaseExclusiveLock();
		}
	}

	public OPhysicalPosition getPhysicalPosition(final long iPosition, final OPhysicalPosition iPPosition) {
		acquireSharedLock();
		try {

			return entries.get((int) iPosition);

		} finally {
			releaseSharedLock();
		}
	}

	public void removePhysicalPosition(final long iPosition, OPhysicalPosition iPPosition) {
		acquireExclusiveLock();
		try {

			if (entries.set((int) iPosition, null) != null)
				// ADD A REMOVED
				removed.add(new Integer((int) iPosition));

		} finally {
			releaseExclusiveLock();
		}
	}

	public void setPhysicalPosition(final long iPosition, final long iDataPosition) {
		acquireExclusiveLock();
		try {

			final OPhysicalPosition ppos = entries.get((int) iPosition);
			ppos.dataPosition = iDataPosition;

		} finally {
			releaseExclusiveLock();
		}
	}

	public void setPhysicalPosition(final long iPosition, final int iDataId, final long iDataPosition, final byte iRecordType,
			int iVersion) {
		acquireExclusiveLock();
		try {

			final OPhysicalPosition ppos = entries.get((int) iPosition);
			ppos.dataSegment = iDataId;
			ppos.dataPosition = iDataPosition;
			ppos.type = iRecordType;

		} finally {
			releaseExclusiveLock();
		}
	}

	public void synch() {
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

	@Override
	public String toString() {
		return "OClusterMemory [name=" + name + ", id=" + id + ", entries=" + entries.size() + ", removed=" + removed + "]";
	}
}
