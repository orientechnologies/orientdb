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

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * Implementation of Set bound to a source ORecord object to keep track of changes. This avoid to call the makeDirty() by hand when
 * the set is changed.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class ORecordTrackedSet extends AbstractCollection<OIdentifiable> implements Set<OIdentifiable>, ORecordElement {
	protected final ORecord<?>		sourceRecord;
	protected Map<Object, Object>	map						= new HashMap<Object, Object>();
	private STATUS								status				= STATUS.NOT_LOADED;
	protected final static Object	ENTRY_REMOVAL	= new Object();

	public ORecordTrackedSet(final ORecord<?> iSourceRecord) {
		this.sourceRecord = iSourceRecord;
		if (iSourceRecord != null)
			iSourceRecord.setDirty();
	}

	public Iterator<OIdentifiable> iterator() {
		return new ORecordTrackedIterator(sourceRecord, map.keySet().iterator());
	}

	public boolean add(final OIdentifiable e) {
		map.put(e, ENTRY_REMOVAL);
		setDirty();

		if (e instanceof ODocument)
			((ODocument) e).addOwner(this);
		return true;
	}

	@Override
	public boolean contains(Object o) {
		return map.containsKey(o);
	}

	public boolean remove(Object o) {
		final Object old = map.remove(o);
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

	public boolean addAll(final Collection<? extends OIdentifiable> c) {
		if (c == null || c.size() == 0)
			return false;

		for (OIdentifiable o : c)
			add(o);

		setDirty();
		return true;
	}

	public boolean retainAll(final Collection<?> c) {
		if (c == null || c.size() == 0)
			return false;

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
		if (status != STATUS.UNMARSHALLING && sourceRecord != null && !sourceRecord.isDirty())
			sourceRecord.setDirty();
		return this;
	}

	public void onBeforeIdentityChanged(final ORID iRID) {
		map.remove(iRID);
		setDirty();
	}

	public void onAfterIdentityChanged(final ORecord<?> iRecord) {
		map.put(iRecord, ENTRY_REMOVAL);
	}

	public boolean setDatabase(final ODatabaseRecord iDatabase) {
		boolean changed = false;

		for (Object o : map.keySet()) {
			if (o instanceof ORecordElement)
				if (((ORecordElement) o).setDatabase(iDatabase))
					changed = true;
		}

		return changed;
	}

	public STATUS getInternalStatus() {
		return status;
	}

	public void setInternalStatus(final STATUS iStatus) {
		status = iStatus;
	}
}
