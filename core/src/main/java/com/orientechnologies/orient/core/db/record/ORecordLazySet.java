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
package com.orientechnologies.orient.core.db.record;

import java.util.HashSet;
import java.util.Iterator;

import com.orientechnologies.orient.core.db.record.ORecordMultiValueHelper.MULTIVALUE_STATUS;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;

/**
 * Lazy implementation of Set. It's bound to a source ORecord object to keep track of changes. This avoid to call the makeDirty() by
 * hand when the set is changed.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
@SuppressWarnings("unchecked")
public class ORecordLazySet extends ORecordTrackedSet implements ORecordLazyMultiValue {
	private ODatabaseRecord														database;
	private final byte																recordType;
	private ORecordMultiValueHelper.MULTIVALUE_STATUS	status							= MULTIVALUE_STATUS.EMPTY;
	private boolean																		autoConvertToRecord	= true;

	public ORecordLazySet(final ODatabaseRecord iDatabase, final byte iRecordType) {
		super(null);
		this.database = iDatabase;
		this.recordType = iRecordType;
	}

	public ORecordLazySet(final ORecord<?> iSourceRecord, final byte iRecordType) {
		super(iSourceRecord);
		this.database = iSourceRecord.getDatabase();
		this.recordType = iRecordType;
	}

	@Override
	public Iterator<Object> iterator() {
		return new OLazyRecordIterator(sourceRecord, database, recordType, super.iterator(), autoConvertToRecord);
	}

	@Override
	public boolean add(Object e) {
		if (status == MULTIVALUE_STATUS.ALL_RIDS && e instanceof ORecord<?> && !((ORecord<?>) e).getIdentity().isNew())
			// IT'S BETTER TO LEAVE ALL RIDS AND EXTRACT ONLY THIS ONE
			e = ((ORecord<?>) e).getIdentity();
		else
			status = ORecordMultiValueHelper.getStatus(status, e);

		return super.add(e);
	}

	@Override
	public boolean contains(final Object o) {
		// convertLinks2Records();
		return super.contains(o);
	}

	@Override
	public Object[] toArray() {
		convertLinks2Records();
		return super.toArray();
	}

	@Override
	public <T> T[] toArray(final T[] a) {
		convertLinks2Records();
		return super.toArray(a);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.orientechnologies.orient.core.db.record.ORecordLazyMultiValue#convertLinks2Records()
	 */
	public void convertLinks2Records() {
		if (status == MULTIVALUE_STATUS.ALL_RECORDS || !autoConvertToRecord || database == null)
			// PRECONDITIONS
			return;

		final HashSet<Object> copy = new HashSet<Object>();
		for (Object k : map.keySet())
			copy.add(convertLink2Record(k));

		clear();

		addAll(copy);
		copy.clear();

		status = MULTIVALUE_STATUS.ALL_RECORDS;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.orientechnologies.orient.core.db.record.ORecordLazyMultiValue#convertRecords2Links()
	 */
	public void convertRecords2Links() {
		if (status == MULTIVALUE_STATUS.ALL_RIDS || database == null)
			// PRECONDITIONS
			return;

		final HashSet<Object> copy = new HashSet<Object>();
		for (Object k : map.keySet())
			copy.add(convertRecord2Link(k));

		clear();

		addAll(copy);
		copy.clear();

		status = MULTIVALUE_STATUS.ALL_RIDS;
	}

	@Override
	public boolean remove(Object o) {
		final boolean result = super.remove(o);
		if (size() == 0)
			status = MULTIVALUE_STATUS.EMPTY;
		return result;
	}

	@Override
	public void clear() {
		super.clear();
		status = MULTIVALUE_STATUS.EMPTY;
	}

	@Override
	public String toString() {
		return ORecordMultiValueHelper.toString(this);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.orientechnologies.orient.core.db.record.ORecordLazyMultiValue#isAutoConvertToRecord()
	 */
	public boolean isAutoConvertToRecord() {
		return autoConvertToRecord;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.orientechnologies.orient.core.db.record.ORecordLazyMultiValue#setAutoConvertToRecord(boolean)
	 */
	public void setAutoConvertToRecord(boolean convertToRecord) {
		this.autoConvertToRecord = convertToRecord;
	}

	protected Object convertLink2Record(final Object iElement) {
		if (iElement != null && iElement instanceof ORID) {
			final ORID rid = (ORID) iElement;

			try {
				return database.load(rid);
			} catch (ORecordNotFoundException e) {
				// IGNORE THIS
			}
		}
		return iElement;
	}

	protected Object convertRecord2Link(final Object iElement) {
		if (iElement != null && iElement instanceof ORecord<?> && !((ORecord<?>) iElement).getIdentity().isNew()) {
			if (((ORecord<?>) iElement).isDirty() && !database.isClosed())
				database.save((ORecordInternal<?>) iElement);

			return ((ORecord<?>) iElement).getIdentity();
		}
		return iElement;
	}

	public byte getRecordType() {
		return recordType;
	}

	@Override
	public boolean setDatabase(final ODatabaseRecord iDatabase) {
		if (database != iDatabase) {
			database = iDatabase;
			super.setDatabase(iDatabase);
			return true;
		}
		return false;
	}
}
