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
package com.orientechnologies.orient.core.db.object;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import com.orientechnologies.orient.core.db.ODatabasePojoAbstract;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
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
@SuppressWarnings("unchecked")
public class OLazyObjectSet<TYPE> implements Set<Object>, Serializable {
	private static final long					serialVersionUID	= -2497274705163041241L;

	private final ORecord<?>					sourceRecord;
	private final Collection<Object>	underlying;
	private String										fetchPlan;
	private boolean										converted					= false;
	private boolean										convertToRecord		= true;

	public OLazyObjectSet(final ORecord<?> iSourceRecord, final Collection<Object> iSource) {
		this.sourceRecord = iSourceRecord;
		this.underlying = iSource;
	}

	public Iterator<Object> iterator() {
		return (Iterator<Object>) new OLazyObjectIterator<TYPE>(getDatabase(), sourceRecord, underlying.iterator(), convertToRecord);
	}

	public int size() {
		return underlying.size();
	}

	public boolean isEmpty() {
		return underlying.isEmpty();
	}

	public boolean contains(final Object o) {
		return underlying.contains(getDatabase().getRecordByUserObject(o, false));
	}

	public Object[] toArray() {
		return toArray(new Object[size()]);
	}

	public <T> T[] toArray(final T[] a) {
		underlying.toArray(a);
		final ODatabasePojoAbstract<TYPE> database = getDatabase();
		for (int i = 0; i < a.length; ++i)
			a[i] = (T) database.getUserObjectByRecord((ORecordInternal<?>) a[i], fetchPlan);
		return a;
	}

	public boolean add(final Object e) {
		if (converted && e instanceof ORID)
			converted = false;
		setDirty();
		return underlying.add(getDatabase().getRecordByUserObject(e, false));
	}

	public boolean remove(final Object o) {
		setDirty();
		return underlying.remove(getDatabase().getRecordByUserObject(o, false));
	}

	public boolean containsAll(final Collection<?> c) {
		final ODatabasePojoAbstract<TYPE> database = getDatabase();
		for (Object o : c)
			if (!underlying.contains(database.getRecordByUserObject(o, false)))
				return false;

		return true;
	}

	public boolean addAll(final Collection<? extends Object> c) {
		boolean modified = false;
		setDirty();
		final ODatabasePojoAbstract<TYPE> database = getDatabase();
		for (Object o : c)
			if (!underlying.add(database.getRecordByUserObject(o, false)))
				modified = true;
		return modified;
	}

	public boolean retainAll(final Collection<?> c) {
		setDirty();
		return underlying.retainAll(c);
	}

	public void clear() {
		setDirty();
		underlying.clear();
	}

	public boolean removeAll(final Collection<?> c) {
		setDirty();
		boolean modified = false;
		final ODatabasePojoAbstract<TYPE> database = getDatabase();
		for (Object o : c)
			if (!underlying.remove(database.getRecordByUserObject(o, false)))
				modified = true;
		return modified;
	}

	public String getFetchPlan() {
		return fetchPlan;
	}

	public OLazyObjectSet<TYPE> setFetchPlan(String fetchPlan) {
		this.fetchPlan = fetchPlan;
		return this;
	}

	public boolean isConvertToRecord() {
		return convertToRecord;
	}

	public void setConvertToRecord(boolean convertToRecord) {
		this.convertToRecord = convertToRecord;
	}

	@Override
	public String toString() {
		return underlying.toString();
	}

	public void setDirty() {
		if (sourceRecord != null)
			sourceRecord.setDirty();
	}

	public void detach() {
		convertAll();
	}

	protected void convertAll() {
		if (converted || !convertToRecord)
			return;

		final Set<Object> copy = new HashSet<Object>(this);
		underlying.clear();
		final ODatabasePojoAbstract<TYPE> database = getDatabase();
		for (Object e : copy) {
			if (e != null) {
				if (e instanceof ORID)
					add(database.getUserObjectByRecord(
							(ORecordInternal<?>) ((ODatabaseRecord) getDatabase().getUnderlying()).load((ORID) e, fetchPlan), fetchPlan));
				else if (e instanceof ODocument)
					add(database.getUserObjectByRecord((ORecordInternal<?>) e, fetchPlan));
				else
					add(e);
			}
		}

		converted = true;
	}

	protected ODatabasePojoAbstract<TYPE> getDatabase() {
		return (ODatabasePojoAbstract<TYPE>) ODatabaseRecordThreadLocal.INSTANCE.get().getDatabaseOwner();
	}
}
