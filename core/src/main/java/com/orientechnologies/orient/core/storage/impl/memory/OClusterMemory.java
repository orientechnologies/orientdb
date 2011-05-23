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

	public OClusterPositionIterator absoluteIterator(long iBeginRange, long iEndRange) throws IOException {
		return new OClusterPositionIterator(this, iBeginRange, iEndRange);
	}

	public void close() {
		entries.clear();
		removed.clear();
	}

	public void open() throws IOException {
	}

	public void create(final int iStartSize) throws IOException {
	}

	public void delete() throws IOException {
		close();
		entries.clear();
	}

	public void truncate() throws IOException {
		entries.clear();
		removed.clear();
	}

	public long getEntries() {
		return entries.size() - removed.size();
	}

	public long getSize() {
		long size = 0;
		for (OPhysicalPosition e : entries)
			if (e != null)
				size += e.recordSize;
		return size;
	}

	public long getFirstEntryPosition() {
		return entries.size() == 0 ? -1 : 0;
	}

	public long getLastEntryPosition() {
		return entries.size() - 1;
	}

	public int getId() {
		return id;
	}

	public String getName() {
		return name;
	}

	public long getAvailablePosition() throws IOException {
		return entries.size();
	}

	public long addPhysicalPosition(final int iDataSegmentId, final long iRecordPosition, final byte iRecordType) {
		if (removed.size() > 0) {
			final int recycledPosition = removed.remove(removed.size() - 1);
			entries.set(recycledPosition, new OPhysicalPosition(iDataSegmentId, iRecordPosition, iRecordType));
			return recycledPosition;
		} else {
			entries.add(new OPhysicalPosition(iDataSegmentId, iRecordPosition, iRecordType));
			return entries.size() - 1;
		}
	}

	public void updateRecordType(final long iPosition, final byte iRecordType) throws IOException {
		entries.get((int) iPosition).type = iRecordType;
	}

	public void updateVersion(long iPosition, int iVersion) throws IOException {
		entries.get((int) iPosition).version = iVersion;
	}

	public OPhysicalPosition getPhysicalPosition(final long iPosition, final OPhysicalPosition iPPosition) {
		return entries.get((int) iPosition);
	}

	public void removePhysicalPosition(final long iPosition, OPhysicalPosition iPPosition) {
		if (entries.set((int) iPosition, null) != null)
			// ADD A REMOVED
			removed.add(new Integer((int) iPosition));
	}

	public void setPhysicalPosition(final long iPosition, final long iDataPosition) {
		final OPhysicalPosition ppos = entries.get((int) iPosition);
		ppos.dataPosition = iDataPosition;
	}

	public void setPhysicalPosition(final long iPosition, final int iDataId, final long iDataPosition, final byte iRecordType) {
		final OPhysicalPosition ppos = entries.get((int) iPosition);
		ppos.dataSegment = iDataId;
		ppos.dataPosition = iDataPosition;
		ppos.type = iRecordType;
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
