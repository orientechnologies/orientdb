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
package com.orientechnologies.orient.core.db.document;

import java.util.ArrayList;
import java.util.Iterator;

import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecordFactory;
import com.orientechnologies.orient.core.record.ORecordInternal;

@SuppressWarnings({ "serial" })
public class OLazyRecordList extends ArrayList<Object> {
	private ODatabaseRecord<?>	database;
	private byte								recordType;
	private boolean							converted	= false;

	public OLazyRecordList(final ODatabaseRecord<?> database, final byte iRecordType) {
		this.database = database;
		this.recordType = iRecordType;
	}

	@Override
	public Iterator<Object> iterator() {
		return new OLazyRecordIterator(database, recordType, super.iterator());
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
		if (converted)
			return;

		for (int i = 0; i < size(); ++i)
			convert(i);

		converted = true;
	}

	/**
	 * Convert the item requested.
	 * 
	 * @param iIndex
	 *          Position of the item to convert
	 */
	private void convert(final int iIndex) {
		final Object o = super.get(iIndex);

		if (o != null && o instanceof ORecordId) {
			final ORecordInternal<?> record = ORecordFactory.newInstance(recordType);
			final ORecordId rid = (ORecordId) o;

			record.setDatabase(database);
			record.setIdentity(rid);
			record.load();

			super.set(iIndex, record);
		}
	}
}
