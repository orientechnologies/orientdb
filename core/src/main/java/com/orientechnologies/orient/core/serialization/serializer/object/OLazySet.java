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
package com.orientechnologies.orient.core.serialization.serializer.object;

import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

import com.orientechnologies.orient.core.db.object.ODatabaseObjectTx;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;

@SuppressWarnings("unchecked")
public class OLazySet<TYPE> implements Set<TYPE> {
	private final ODatabaseObjectTx			database;
	private final Collection<ODocument>	underlying;
	private String											fetchPlan;

	public OLazySet(final ODatabaseObjectTx database, final Collection<ODocument> iSource) {
		this.database = database;
		this.underlying = iSource;
	}

	public Iterator<TYPE> iterator() {
		return new OLazyIterator<TYPE>(database, underlying.iterator());
	}

	public int size() {
		return underlying.size();
	}

	public boolean isEmpty() {
		return underlying.isEmpty();
	}

	public boolean contains(final Object o) {
		convertAll();
		return underlying.contains(underlying.contains(database.getRecordByUserObject(o, false)));
	}

	public Object[] toArray() {
		convertAll();
		return toArray(new Object[size()]);
	}

	public <T> T[] toArray(final T[] a) {
		convertAll();
		underlying.toArray(a);
		for (int i = 0; i < a.length; ++i)
			a[i] = (T) database.getUserObjectByRecord((ORecordInternal<?>) a[i], fetchPlan);
		return a;
	}

	public boolean add(final TYPE e) {
		return underlying.add(database.getRecordByUserObject(e, false));
	}

	public boolean remove(final Object o) {
		return underlying.remove(underlying.contains(database.getRecordByUserObject(o, false)));
	}

	public boolean containsAll(final Collection<?> c) {
		for (Object o : c)
			if (!underlying.contains(database.getRecordByUserObject(o, false)))
				return false;

		return true;
	}

	public boolean addAll(final Collection<? extends TYPE> c) {
		boolean modified = false;
		for (Object o : c)
			if (!underlying.add(database.getRecordByUserObject(o, false)))
				modified = true;
		return modified;
	}

	public boolean retainAll(final Collection<?> c) {
		convertAll();
		return underlying.retainAll(c);
	}

	public void clear() {
		underlying.clear();
	}

	public boolean removeAll(final Collection<?> c) {
		convertAll();
		boolean modified = false;
		for (Object o : c)
			if (!underlying.remove(database.getRecordByUserObject(o, false)))
				modified = true;
		return modified;
	}

	public String getFetchPlan() {
		return fetchPlan;
	}

	public OLazySet<TYPE> setFetchPlan(String fetchPlan) {
		this.fetchPlan = fetchPlan;
		return this;
	}

	private void convertAll() {
		Iterator<TYPE> e = iterator();
		while (e.hasNext()) {
			convert(e.next());
		}
	}

	private void convert(final TYPE o) {
		database.getRecordByUserObject(o, false);
	}
}
