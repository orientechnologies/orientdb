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

import java.util.ArrayList;
import java.util.Iterator;

import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecord;

/**
 * Implementation of ArrayList bound to a source ORecord object to keep track of changes. This avoid to call the makeDirty() by hand
 * when the list is changed.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
@SuppressWarnings({ "serial" })
public class ORecordTrackedList extends ArrayList<Object> implements ORecordElement {
	protected final ORecord<?>	sourceRecord;

	public ORecordTrackedList(final ORecord<?> iSourceRecord) {
		this.sourceRecord = iSourceRecord;
		if (iSourceRecord != null)
			iSourceRecord.setDirty();
	}

	@Override
	public Iterator<Object> iterator() {
		return new ORecordTrackedIterator(sourceRecord, super.iterator());
	}

	@Override
	public boolean contains(final Object o) {
		return super.contains(o);
	}

	@Override
	public boolean add(Object element) {
		setDirty();
		return super.add(element);
	}

	@Override
	public void add(int index, Object element) {
		setDirty();
		super.add(index, element);
	}

	@Override
	public Object get(final int index) {
		return super.get(index);
	}

	@Override
	public int indexOf(final Object o) {
		return super.indexOf(o);
	}

	@Override
	public int lastIndexOf(final Object o) {
		return super.lastIndexOf(o);
	}

	@Override
	public Object[] toArray() {
		return super.toArray();
	}

	@Override
	public <T> T[] toArray(final T[] a) {
		return super.toArray(a);
	}

	@Override
	public Object set(int index, Object element) {
		setDirty();
		return super.set(index, element);
	}

	@Override
	public Object remove(int index) {
		setDirty();
		return super.remove(index);
	}

	@Override
	public boolean remove(Object o) {
		setDirty();
		return super.remove(o);
	}

	@Override
	public void clear() {
		setDirty();
		super.clear();
	}

	@SuppressWarnings("unchecked")
	public ORecordTrackedList setDirty() {
		if (sourceRecord != null)
			sourceRecord.setDirty();
		return this;
	}

	/**
	 * The item's identity doesn't affect nothing.
	 */
	public void onIdentityChanged(final ORecord<?> iRecord, final int iOldHashCode) {
	}

	public void setDatabase(final ODatabaseRecord iDatabase) {
	}
}
