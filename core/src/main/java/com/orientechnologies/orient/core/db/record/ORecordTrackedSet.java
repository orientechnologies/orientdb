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

import java.util.AbstractCollection;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * Implementation of Set bound to a source ORecord object to keep track of changes. This avoid to call the makeDirty() by hand when
 * the set is changed.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class ORecordTrackedSet extends AbstractCollection<Object> implements Set<Object>, ORecordElement {
	protected final ORecord<?>		sourceRecord;
	protected Map<Object, Object>	map	= new HashMap<Object, Object>();

	public ORecordTrackedSet(final ORecord<?> iSourceRecord) {
		this.sourceRecord = iSourceRecord;
		if (iSourceRecord != null)
			iSourceRecord.setDirty();
	}

	public Iterator<Object> iterator() {
		return new ORecordTrackedIterator(sourceRecord, map.values().iterator());
	}

	public boolean add(final Object e) {
		final int h = e.hashCode();
		final Object previous = map.get(h);
		if (previous != null && previous != e)
			map.put(System.currentTimeMillis(), e);
		else
			map.put(h, e);
		setDirty();

		if (e instanceof ODocument)
			((ODocument) e).addOwner(this);
		return true;
	}

	/**
	 * Update an entry with hash changed.
	 * 
	 * @param iRecord
	 * @param o
	 * @return
	 */
	public boolean update(final ORecord<?> iRecord, final int iOldHashCode) {
		Object entry;
		for (Iterator<Object> it = map.keySet().iterator(); it.hasNext();) {
			entry = it.next();
			if ((Integer) entry == iOldHashCode) {
				it.remove();
				add(iRecord);
				setDirty();
				return true;
			}
		}
		return false;
	}

	@Override
	public boolean contains(Object o) {
		return map.containsKey(o.hashCode());
	}

	public boolean remove(Object o) {
		final Object old = map.remove(o.hashCode());
		if (old != null) {

			if (o instanceof ODocument)
				((ODocument) o).removeOwner(this);

			setDirty();
			return true;
		}
		return false;
	}

	public void clear() {
		setDirty();
		map.clear();
	}

	public boolean removeAll(final Collection<?> c) {
		boolean changed = false;
		for (Object item : c) {
			if (map.remove(item) != null)
				changed = true;
		}

		if (changed)
			setDirty();

		return changed;
	}

	public boolean addAll(final Collection<? extends Object> c) {
		if (c.size() == 0)
			return false;

		for (Object o : c) {
			add(o);
		}

		setDirty();
		return true;
	}

	public boolean retainAll(final Collection<?> c) {
		if (super.removeAll(c)) {
			setDirty();
			return true;
		}
		return false;
	}

	@Override
	public int size() {
		return map.size();
	}

	@SuppressWarnings("unchecked")
	public ORecordTrackedSet setDirty() {
		if (sourceRecord != null)
			sourceRecord.setDirty();
		return this;
	}

	public void onIdentityChanged(final ORecord<?> iRecord, final int iOldHashCode) {
		update(iRecord, iOldHashCode);
	}

	public void setDatabase(final ODatabaseRecord iDatabase) {
	}
}
