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

import com.orientechnologies.common.concur.resource.OSharedResourceExternal;
import com.orientechnologies.orient.core.config.OStorageLogicalClusterConfiguration;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializerAnyStreamable;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializerLong;
import com.orientechnologies.orient.core.storage.OCluster;
import com.orientechnologies.orient.core.storage.OClusterPositionIterator;
import com.orientechnologies.orient.core.storage.OPhysicalPosition;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.type.tree.OMVRBTreeStorage;

/**
 * Handle a cluster using a logical structure stored into a real physical local cluster.<br/>
 * Uses the dummy position -1 to store the total number of records. It's used only by local storage implementation since it relies
 * on OStorageLocal.
 * 
 */
public class OClusterLogical implements OCluster {
	private String																		name;
	private int																				id;
	private int																				localClusterId;

	private OMVRBTreeStorage<Long, OPhysicalPosition>	map;
	private OPhysicalPosition													total;

	private OSharedResourceExternal										lock	= new OSharedResourceExternal();
	public static final String												TYPE	= "LOGICAL";
	private OStorage																	storage;

	/**
	 * Constructor called on creation of the object.
	 * 
	 * @param iStorage
	 * @param iId
	 * @param iName
	 * @param iId
	 * @param iRecordId
	 * @throws IOException
	 * @throws IOException
	 */
	public OClusterLogical(final OStorageLocal iStorage, final int iId, final String iName, final int iPhysicalClusterId)
			throws IOException {
		this(iName, iId, iPhysicalClusterId);

		try {
			map = new OMVRBTreeStorage<Long, OPhysicalPosition>(iStorage, iStorage.getClusterById(iPhysicalClusterId).getName(),
					OStreamSerializerLong.INSTANCE, OStreamSerializerAnyStreamable.INSTANCE);
			map.getRecord().setIdentity(iPhysicalClusterId, ORID.CLUSTER_POS_INVALID);

			total = new OPhysicalPosition(0, -1, (byte) 0);
			map.put(new Long(-1), total);
			storage = iStorage;

		} catch (Exception e) {
			throw new ODatabaseException("Error on creating internal map for logical cluster: " + iName, e);
		}
		map.save();
	}

	/**
	 * Constructor called on importing.
	 * 
	 * @param iStorage
	 * @param iName
	 * @param iId
	 * @param iRecordId
	 * @throws IOException
	 */
	public OClusterLogical(final OStorageLocal iStorage, final String iName, final int iId, final ORID iRecordId) throws IOException {
		this(iName, iId, 0);

		try {
			map = new OMVRBTreeStorage<Long, OPhysicalPosition>(iStorage, iStorage.getClusterById(iRecordId.getClusterId()).getName(),
					iRecordId);
			map.load();

			total = map.get(new Long(-1));
			if (total == null) {
				total = new OPhysicalPosition(0, map.size(), (byte) 0);
				map.put(new Long(-1), total);
			}
		} catch (Exception e) {
			throw new ODatabaseException("Error on load internal map for logical cluster: " + name, e);
		}
	}

	/**
	 * Constructor called on loading of the object.
	 */
	public OClusterLogical(final OStorageLocal iStorage, final OStorageLogicalClusterConfiguration iConfig) throws IOException {
		this(iStorage, iConfig.name, iConfig.id, iConfig.map);
	}

	protected OClusterLogical(final String iName, final int iId, final int iPhysicalClusterId) {
		name = iName;
		id = iId;
		localClusterId = iPhysicalClusterId;
	}

	public void create(final int iStartSize) throws IOException {
	}

	public void open() throws IOException {
	}

	public void close() {
	}

	public void delete() throws IOException {
		close();
		map.delete();
	}

	public void set(ATTRIBUTES iAttribute, Object iValue) throws IOException {
		if (iAttribute == null)
			throw new IllegalArgumentException("attribute is null");

		final String stringValue = iValue != null ? iValue.toString() : null;

		switch (iAttribute) {
		case NAME:
			storage.renameCluster(name, stringValue);
			name = stringValue;
		}

	}

	/**
	 * Fill and return the PhysicalPosition object received as parameter with the physical position of logical record iPosition
	 */
	public OPhysicalPosition getPhysicalPosition(final long iPosition, final OPhysicalPosition iPPosition) {
		return map.get(iPosition);
	}

	/**
	 * Update position in data segment (usually on defrag)
	 */
	public void setPhysicalPosition(final long iPosition, final long iDataPosition) {
		Long key = new Long(iPosition);
		final OPhysicalPosition ppos = map.get(key);
		ppos.dataPosition = iDataPosition;
		map.put(key, ppos);
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

	public void updateRecordType(final long iPosition, final byte iRecordType) throws IOException {
		final Long key = new Long(iPosition);
		final OPhysicalPosition ppos = map.get(key);
		ppos.type = iRecordType;
		map.put(key, ppos);
	}

	public void updateVersion(long iPosition, final int iVersion) throws IOException {
		final Long key = new Long(iPosition);
		final OPhysicalPosition ppos = map.get(key);
		ppos.version = iVersion;
		map.put(key, ppos);
	}

	public void truncate() throws IOException {
		map.clear();
		total = new OPhysicalPosition(0, -1, (byte) 0);
		map.put(new Long(-1), total);
		map.save();
	}

	/**
	 * Remove the Logical Position entry.
	 */
	public void removePhysicalPosition(final long iPosition, final OPhysicalPosition iPPosition) {
		map.remove(iPosition);

		if (total.dataPosition == iPosition) {
			// LAST ONE: SEARCH THE HIGHER POSITION TO DISCOVER TOTAL MAXIMUM TOTAL RECORDS
			// TODO
			total.dataPosition--;
			map.put(new Long(-1), total);
		}
	}

	/**
	 * Add a new entry.
	 * 
	 * @throws IOException
	 */
	public long addPhysicalPosition(final int iDataSegmentId, final long iRecordPosition, final byte iRecordType) throws IOException {
		final long pos = ++total.dataPosition;
		map.put(new Long(pos), new OPhysicalPosition(iDataSegmentId, iRecordPosition, iRecordType));

		map.put(new Long(-1), total);
		return pos;
	}

	public long getEntries() {
		// RETURN THE MAP SIZE LESS THE DUMMY -1 POSITION
		return map.size() - 1;
	}

	public long getFirstEntryPosition() {
		return 0;
	}

	public long getLastEntryPosition() {
		return total.dataPosition;
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

	public String getType() {
		return TYPE;
	}

	public int getLocalClusterId() {
		return localClusterId;
	}

	/**
	 * Returns always 0 because the real size is computed into local and memory clusters.
	 */
	public long getSize() {
		return 0;
	}

	public long getRecordsSize() throws IOException {
		return 0;
	}

}
