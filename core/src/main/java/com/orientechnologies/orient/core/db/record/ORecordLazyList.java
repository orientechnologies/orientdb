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

import java.util.Iterator;

import com.orientechnologies.orient.core.db.record.ORecordMultiValueHelper.MULTIVALUE_STATUS;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * Lazy implementation of ArrayList. It's bound to a source ORecord object to keep track of changes. This avoid to call the
 * makeDirty() by hand when the list is changed.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
@SuppressWarnings({ "serial", "unchecked" })
public class ORecordLazyList extends ORecordTrackedList implements ORecordLazyMultiValue {
	final private byte																recordType;
	private ODatabaseRecord														database;
	private ORecordMultiValueHelper.MULTIVALUE_STATUS	status							= MULTIVALUE_STATUS.EMPTY;
	private boolean																		autoConvertToRecord	= true;

	public ORecordLazyList(final ODatabaseRecord iDatabase) {
		super(null);
		this.database = iDatabase;
		this.recordType = ODocument.RECORD_TYPE;
	}

	public ORecordLazyList(final ODatabaseRecord iDatabase, final byte iRecordType) {
		super(null);
		this.database = iDatabase;
		this.recordType = iRecordType;
	}

	public ORecordLazyList(final ORecordInternal<?> iSourceRecord) {
		super(iSourceRecord);
		this.database = iSourceRecord.getDatabase();
		this.recordType = iSourceRecord.getRecordType();
	}

	@Override
	public Iterator<OIdentifiable> iterator() {
		return new OLazyRecordIterator(sourceRecord, database, recordType, super.iterator(), autoConvertToRecord);
	}

	@Override
	public boolean contains(final Object o) {
		// convertLinks2Records();
		return super.contains(o);
	}

	@Override
	public boolean add(OIdentifiable e) {
		if (status == MULTIVALUE_STATUS.ALL_RIDS && e instanceof ORecord<?> && !((ORecord<?>) e).getIdentity().isNew())
			// IT'S BETTER TO LEAVE ALL RIDS AND EXTRACT ONLY THIS ONE
			e = ((ORecord<?>) e).getIdentity();
		else
			status = ORecordMultiValueHelper.getStatus(status, e);

		return super.add(e);
	}

	@Override
	public void add(int index, OIdentifiable e) {
		if (status == MULTIVALUE_STATUS.ALL_RIDS && e instanceof ORecord<?>)
			// IT'S BETTER TO LEAVE ALL RIDS AND EXTRACT ONLY THIS ONE
			e = ((ORecord<?>) e).getIdentity();
		else
			status = ORecordMultiValueHelper.getStatus(status, e);

		super.add(index, e);
	}

	@Override
	public OIdentifiable set(int index, OIdentifiable e) {
		status = ORecordMultiValueHelper.getStatus(status, e);
		return super.set(index, e);
	}

	@Override
	public OIdentifiable get(final int index) {
		convertLink2Record(index);
		return super.get(index);
	}

	@Override
	public int indexOf(final Object o) {
		// convertLinks2Records();
		return super.indexOf(o);
	}

	@Override
	public int lastIndexOf(final Object o) {
		// convertLinks2Records();
		return super.lastIndexOf(o);
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
	public Object[] toArray() {
		convertLinks2Records();
		return super.toArray();
	}

	@Override
	public <T> T[] toArray(final T[] a) {
		convertLinks2Records();
		return super.toArray(a);
	}

	public void convertLinks2Records() {
		if (status == MULTIVALUE_STATUS.ALL_RECORDS || !autoConvertToRecord || database == null)
			// PRECONDITIONS
			return;

		for (int i = 0; i < size(); ++i) {
			try {
				convertLink2Record(i);
			} catch (ORecordNotFoundException e) {
				// LEAVE THE RID DIRTY
			}
		}

		status = MULTIVALUE_STATUS.ALL_RECORDS;
	}

	public void convertRecords2Links() {
		if (status == MULTIVALUE_STATUS.ALL_RIDS || sourceRecord == null || database == null)
			// PRECONDITIONS
			return;

		for (int i = 0; i < size(); ++i) {
			try {
				convertRecord2Link(i);
			} catch (ORecordNotFoundException e) {
				// LEAVE THE RID DIRTY
			}
		}

		status = MULTIVALUE_STATUS.ALL_RIDS;
	}

	/**
	 * Convert the item requested from link to record.
	 * 
	 * @param iIndex
	 *          Position of the item to convert
	 */
	private void convertLink2Record(final int iIndex) {
		if (status == MULTIVALUE_STATUS.ALL_RECORDS || !autoConvertToRecord || database == null)
			// PRECONDITIONS
			return;

		final OIdentifiable o = super.get(iIndex);

		if (o != null && o instanceof ORecordId) {
			final ORecordId rid = (ORecordId) o;

			try {
				final ORecordInternal<?> record = database.load(rid);
				set(iIndex, (OIdentifiable) record);
			} catch (ORecordNotFoundException e) {
				// IGNORE THIS
			}
		}
	}

	/**
	 * Convert the item requested from record to link.
	 * 
	 * @param iIndex
	 *          Position of the item to convert
	 */
	private void convertRecord2Link(final int iIndex) {
		if (status == MULTIVALUE_STATUS.ALL_RIDS || database == null)
			// PRECONDITIONS
			return;

		final Object o = super.get(iIndex);

		if (o != null && o instanceof ORecord<?> && !((ORecord<?>) o).getIdentity().isNew())
			try {
				if (((ORecord<?>) o).isDirty())
					database.save((ORecordInternal<?>) o);

				super.set(iIndex, ((ORecord<?>) o).getIdentity());
			} catch (ORecordNotFoundException e) {
				// IGNORE THIS
			}
	}

	public boolean isAutoConvertToRecord() {
		return autoConvertToRecord;
	}

	public void setAutoConvertToRecord(boolean convertToDocument) {
		this.autoConvertToRecord = convertToDocument;
	}

	@Override
	public String toString() {
		return ORecordMultiValueHelper.toString(this);
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
