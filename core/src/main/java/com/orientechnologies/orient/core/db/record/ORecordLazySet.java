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

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordFactory;
import com.orientechnologies.orient.core.record.ORecordInternal;

/**
 * Lazy implementation of Set. It's bound to a source ORecord object to keep track of changes. This avoid to call the makeDirty() by
 * hand when the set is changed.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
@SuppressWarnings("serial")
public class ORecordLazySet extends ORecordTrackedSet {
	private final ODatabaseRecord<?>	database;
	private final byte								recordType;
	private boolean										converted	= false;

	public ORecordLazySet(final ODatabaseRecord<?> iDatabase, final byte iRecordType) {
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
		return new OLazyRecordIterator(sourceRecord, recordType, super.iterator());
	}

	@Override
	public boolean add(final Object e) {
		if (converted && e instanceof ORID)
			converted = false;
		setDirty();
		return super.add(e);
	}

	@Override
	public boolean contains(final Object o) {
		convertAll();
		return super.contains(o);
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

	/**
	 * Browse all the set to convert all the items.
	 */
	public void convertAll() {
		if (converted)
			return;

		HashSet<Object> copy = new HashSet<Object>();
		for (Iterator<Object> it = iterator(); it.hasNext();)
			copy.add(convert(it.next()));

		clear();

		addAll(copy);
		copy.clear();

		converted = true;
	}

	protected Object convert(final Object iElement) {
		if (iElement != null && iElement instanceof ORecordId) {
			final ORecordInternal<?> record = ORecordFactory.newInstance(recordType);
			final ORecordId rid = (ORecordId) iElement;

			record.setDatabase(database);
			record.setIdentity(rid);
			record.load();
			return record;
		}
		return iElement;
	}
}
