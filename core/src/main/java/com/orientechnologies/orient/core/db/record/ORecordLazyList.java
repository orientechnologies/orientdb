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

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordFactory;
import com.orientechnologies.orient.core.record.ORecordInternal;

/**
 * Lazy implementation of ArrayList. It's bound to a source ORecord object to keep track of changes. This avoid to call the
 * makeDirty() by hand when the list is changed.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
@SuppressWarnings({ "serial" })
public class ORecordLazyList extends ORecordTrackedList {
	final private byte								recordType;
	final private ODatabaseRecord<?>	database;
	private boolean										converted				= false;
	private boolean										convertToRecord	= true;

	public ORecordLazyList(ODatabaseRecord<?> iDatabase, final byte iRecordType) {
		super(null);
		this.database = iDatabase;
		this.recordType = iRecordType;
	}

	public ORecordLazyList(final ORecord<?> iSourceRecord, final byte iRecordType) {
		super(iSourceRecord);
		this.database = iSourceRecord.getDatabase();
		this.recordType = iRecordType;
	}

	@Override
	public Iterator<Object> iterator() {
		return new OLazyRecordIterator(sourceRecord, recordType, super.iterator(), convertToRecord);
	}

	@Override
	public boolean contains(final Object o) {
		convertAll();
		return super.contains(o);
	}

	@Override
	public boolean add(Object element) {
		if (converted && element instanceof ORID)
			converted = false;
		return super.add(element);
	}

	@Override
	public void add(int index, Object element) {
		if (converted && element instanceof ORID)
			converted = false;
		super.add(index, element);
	}

	@Override
	public Object get(final int index) {
		convert(index);
		return super.get(index);
	}

	@Override
	public int indexOf(final Object o) {
		convertAll();
		return super.indexOf(o);
	}

	@Override
	public int lastIndexOf(final Object o) {
		convertAll();
		return super.lastIndexOf(o);
	}

	@Override
	public Object[] toArray() {
		convertAll();
		return super.toArray();
	}

	@Override
	public <T> T[] toArray(final T[] a) {
		convertAll();
		return super.toArray(a);
	}

	public void convertAll() {
		if (converted || !convertToRecord)
			return;
		
		if (sourceRecord == null || sourceRecord.getDatabase() == null)
			return;

		for (int i = 0; i < size(); ++i) {
			try {
				convert(i);
			} catch (ORecordNotFoundException e) {
				// LEAVE THE RID DIRTY
			}
		}

		converted = true;
	}

	public boolean isConvertToRecord() {
		return convertToRecord;
	}

	public void setConvertToRecord(boolean convertToDocument) {
		this.convertToRecord = convertToDocument;
	}

	/**
	 * Convert the item requested.
	 * 
	 * @param iIndex
	 *          Position of the item to convert
	 */
	private void convert(final int iIndex) {
		if (converted || !convertToRecord)
			return;

		if (sourceRecord == null || sourceRecord.getDatabase() == null)
			return;

		final Object o = super.get(iIndex);

		if (o != null && o instanceof ORecordId) {
			final ORecordInternal<?> record = ORecordFactory.newInstance(recordType);
			final ORecordId rid = (ORecordId) o;

			record.setDatabase(database);
			record.setIdentity(rid);

			try {
				record.load();
				super.set(iIndex, record);
			} catch (ORecordNotFoundException e) {
				// IGNORE THIS
			}
		}
	}

	@Override
	public String toString() {
		return OMultiValue.toString(this);
	}
}
