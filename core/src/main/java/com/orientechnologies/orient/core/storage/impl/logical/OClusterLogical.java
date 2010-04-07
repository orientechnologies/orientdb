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
package com.orientechnologies.orient.core.storage.impl.logical;

import java.io.IOException;

import com.orientechnologies.common.concur.resource.OSharedResourceExternal;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OTreeMapPersistent;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializerAnyStreamable;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializerLong;
import com.orientechnologies.orient.core.storage.OCluster;
import com.orientechnologies.orient.core.storage.OClusterPositionIterator;
import com.orientechnologies.orient.core.storage.OPhysicalPosition;
import com.orientechnologies.orient.core.storage.OStorage;

/**
 * Handle a cluster using a logical structure stored into a read physical local cluster.<br/>
 * 
 */
public class OClusterLogical implements OCluster {
	private String																			name;
	private int																					id;
	private OTreeMapPersistent<Long, OPhysicalPosition>	map;

	private OSharedResourceExternal											lock	= new OSharedResourceExternal();

	/**
	 * Constructor called on creation of the object.
	 * 
	 * @param iDatabase
	 * @param iName
	 * @param iId
	 * @param iRecordId
	 * @throws IOException
	 */
	public OClusterLogical(final ODatabaseRecord<?> iDatabase, final String iName) throws IOException {
		this(iName, -1);
		try {
			map = new OTreeMapPersistent<Long, OPhysicalPosition>(iDatabase, OStorage.DEFAULT_SEGMENT, OStreamSerializerLong.INSTANCE,
					OStreamSerializerAnyStreamable.INSTANCE);
		} catch (Exception e) {
			throw new ODatabaseException("Error on creating internal map for logical cluster: " + iName, e);
		}
		map.save();
	}

	/**
	 * Constructor called on loading of the object.
	 * 
	 * @param iDatabase
	 * @param iName
	 * @param iId
	 * @param iRecordId
	 * @throws IOException
	 */
	public OClusterLogical(final ODatabaseRecord<?> iDatabase, final String iName, final int iId, final ORID iRecordId)
			throws IOException {
		this(iName, iId);
		map = new OTreeMapPersistent<Long, OPhysicalPosition>(iDatabase, OStorage.DEFAULT_SEGMENT, iRecordId);
		map.load();
	}

	protected OClusterLogical(final String iName, final int iId) {
		name = iName;
		id = iId;
	}

	public void create(final int iStartSize) throws IOException {
	}

	public void open() throws IOException {
	}

	public void close() {
	}

	/**
	 * Fill and return the PhysicalPosition object received as parameter with the physical position of logical record iPosition
	 */
	public OPhysicalPosition getPhysicalPosition(final long iPosition, final OPhysicalPosition iPPosition) {
		return map.get(iPosition);
	}

	/**
	 * Change the PhysicalPosition of the logical record iPosition.
	 */
	public void setPhysicalPosition(final long iPosition, final int iDataId, final long iDataPosition, final byte iRecordType) {
		Long key = new Long(iPosition);
		final OPhysicalPosition ppos = map.get(key);
		ppos.dataSegment = iDataId;
		ppos.dataPosition = iDataPosition;
		ppos.type = iRecordType;
		map.put(key, ppos);
	}

	public void updateVersion(long iPosition, final int iVersion) throws IOException {
		final Long key = new Long(iPosition);
		final OPhysicalPosition ppos = map.get(key);
		ppos.version = iVersion;
		map.put(key, ppos);
	}

	/**
	 * Remove the Logical Position entry.
	 */
	public void removePhysicalPosition(final long iPosition, final OPhysicalPosition iPPosition) {
		map.remove(iPosition);
	}

	/**
	 * Add a new entry.
	 * 
	 * @throws IOException
	 */
	public long addPhysicalPosition(final int iDataSegmentId, final long iRecordPosition, final byte iRecordType) throws IOException {
		long pos = getAvailablePosition();
		map.put(new Long(pos), new OPhysicalPosition(iDataSegmentId, iRecordPosition, iRecordType));
		return pos;
	}

	public long getAvailablePosition() throws IOException {
		return map.size();
	}

	public long getElements() {
		return map.size();
	}

	public int getId() {
		return id;
	}

	public OClusterPositionIterator absoluteIterator() throws IOException {
		return new OClusterPositionIterator(this);
	}

	public String getName() {
		return name;
	}

	@Override
	public String toString() {
		return name + " (id=" + id + ")";
	}

	public void synch() {
	}

	public void lock() {
		lock.acquireSharedLock();
	}

	public void unlock() {
		lock.releaseSharedLock();
	}

	public ORID getRID() {
		return map.getRecord().getIdentity();
	}

	public void setId(final int id) {
		this.id = id;
	}

	public void setRID(final ORID iRID) {
		this.map.getRecord().setIdentity(iRID.getClusterId(), iRID.getClusterPosition());
	}
}
