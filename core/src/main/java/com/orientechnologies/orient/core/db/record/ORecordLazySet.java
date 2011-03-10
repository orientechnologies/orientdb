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

import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * Lazy implementation of Set. It's bound to a source ORecord object to keep track of changes. This avoid to call the makeDirty() by
 * hand when the set is changed.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class ORecordLazySet implements Set<OIdentifiable>, ORecordLazyMultiValue, ORecordElement {
	public static final ORecordLazySet	EMPTY_SET	= new ORecordLazySet(null, ODocument.RECORD_TYPE);
	protected ORecordLazyList						delegate;

	public ORecordLazySet(final ODatabaseRecord iDatabase, final byte iRecordType) {
		delegate = new ORecordLazyList(iDatabase, iRecordType);
	}

	public ORecordLazySet(final ORecordInternal<?> iSourceRecord) {
		delegate = new ORecordLazyList(iSourceRecord);
	}

	public void onBeforeIdentityChanged(final ORID iRID) {
		delegate.onBeforeIdentityChanged(iRID);
	}

	public void onAfterIdentityChanged(final ORecord<?> iRecord) {
		delegate.onAfterIdentityChanged(iRecord);
	}

	public <RET> RET setDirty() {
		return (RET) delegate.setDirty();
	}

	public boolean setDatabase(final ODatabaseRecord iDatabase) {
		return delegate.setDatabase(iDatabase);
	}

	public Iterator<OIdentifiable> iterator() {
		return delegate.iterator();
	}

	public Iterator<OIdentifiable> rawIterator() {
		return delegate.rawIterator();
	}

	public void convertLinks2Records() {
		delegate.convertLinks2Records();
	}

	public void convertRecords2Links() {
		delegate.convertRecords2Links();
	}

	public boolean isAutoConvertToRecord() {
		return delegate.isAutoConvertToRecord();
	}

	public void setAutoConvertToRecord(final boolean convertToRecord) {
		delegate.setAutoConvertToRecord(convertToRecord);
	}

	public int size() {
		return delegate.size();
	}

	public boolean isEmpty() {
		return delegate.isEmpty();
	}

	public boolean contains(final Object o) {
		return delegate.contains(o);
	}

	public Object[] toArray() {
		return delegate.toArray();
	}

	public <T> T[] toArray(final T[] a) {
		return toArray(a);
	}

	public boolean add(final OIdentifiable e) {
		if (!contains(e)) {
			delegate.add(e);
			return true;
		}
		return false;
	}

	public boolean remove(final Object o) {
		return delegate.remove(o);
	}

	public boolean containsAll(final Collection<?> c) {
		return delegate.containsAll(c);
	}

	public boolean addAll(final Collection<? extends OIdentifiable> c) {
		return delegate.addAll(c);
	}

	public boolean retainAll(final Collection<?> c) {
		return delegate.retainAll(c);
	}

	public boolean removeAll(final Collection<?> c) {
		return delegate.removeAll(c);
	}

	public void clear() {
		delegate.clear();
	}

	public byte getRecordType() {
		return delegate.getRecordType();
	}
}
