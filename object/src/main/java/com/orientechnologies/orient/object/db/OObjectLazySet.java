/*
 * Copyright 2010-2012 Luca Molino (molino.luca--at--gmail.com)
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
package com.orientechnologies.orient.object.db;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import javassist.util.proxy.ProxyObject;

import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.object.OLazyObjectMultivalueElement;
import com.orientechnologies.orient.core.db.object.OLazyObjectSetInterface;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.object.enhancement.OObjectProxyMethodHandler;

/**
 * Lazy implementation of Set. It's bound to a source ORecord object to keep track of changes. This avoid to call the makeDirty() by
 * hand when the set is changed.
 * 
 * @author Luca Molino (molino.luca--at--gmail.com)
 * 
 */
@SuppressWarnings("unchecked")
public class OObjectLazySet<TYPE> extends HashSet<TYPE> implements OLazyObjectSetInterface<TYPE>, OLazyObjectMultivalueElement,
		Serializable {
	private static final long					serialVersionUID	= 1793910544017627989L;

	private final ProxyObject					sourceRecord;
	private final Set<OIdentifiable>	underlying;
	private String										fetchPlan;
	private boolean										converted					= false;
	private boolean										convertToRecord		= true;

	public OObjectLazySet(final Object iSourceRecord, final Set<OIdentifiable> iRecordSource) {
		this.sourceRecord = iSourceRecord instanceof ProxyObject ? (ProxyObject) iSourceRecord : null;
		this.underlying = iRecordSource;
	}

	public OObjectLazySet(final Object iSourceRecord, final Set<OIdentifiable> iRecordSource,
			final Set<? extends TYPE> iSourceCollection) {
		this.sourceRecord = iSourceRecord instanceof ProxyObject ? (ProxyObject) iSourceRecord : null;
		this.underlying = iRecordSource;
		addAll(iSourceCollection);
	}

	public Iterator<TYPE> iterator() {
		return (Iterator<TYPE>) new OObjectLazyIterator<TYPE>(getDatabase(), sourceRecord, (!converted ? underlying.iterator()
				: super.iterator()), convertToRecord);
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
			a[i] = (T) database.getUserObjectByRecord((OIdentifiable) a[i], fetchPlan);
		return a;
	}

	public boolean add(final TYPE e) {
		if (converted && e instanceof ORID)
			converted = false;
		setDirty();
		return super.add(e) && underlying.add(getDatabase().getRecordByUserObject(e, true));
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

	public boolean addAll(final Collection<? extends TYPE> c) {
		setDirty();
		final ODatabasePojoAbstract<TYPE> database = getDatabase();
		boolean modified = super.addAll(c);
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
		final ODatabasePojoAbstract<TYPE> database = getDatabase();
		boolean modified = super.removeAll(c);
		for (Object o : c)
			if (!underlying.remove(database.getRecordByUserObject(o, false)))
				modified = true;
		return modified;
	}

	public String getFetchPlan() {
		return fetchPlan;
	}

	public boolean isConverted() {
		return converted;
	}

	public OObjectLazySet<TYPE> setFetchPlan(String fetchPlan) {
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
			((OObjectProxyMethodHandler) sourceRecord.getHandler()).setDirty();
	}

	public void detach() {
		convertAll();
	}

	public void detachAll(boolean nonProxiedInstance) {
		convertAndDetachAll(nonProxiedInstance);
	}

	protected void convertAll() {
		if (converted || !convertToRecord)
			return;

		final Set<Object> copy = new HashSet<Object>(underlying);
		this.clear();
		final ODatabasePojoAbstract<TYPE> database = getDatabase();
		for (Object e : copy) {
			if (e != null) {
				if (e instanceof ORID)
					add(database.getUserObjectByRecord(
							(ORecordInternal<?>) ((ODatabaseRecord) getDatabase().getUnderlying()).load((ORID) e, fetchPlan), fetchPlan));
				else if (e instanceof ODocument)
					add(database.getUserObjectByRecord((ORecordInternal<?>) e, fetchPlan));
				else
					add((TYPE) e);
			}
		}

		converted = true;
	}

	protected void convertAndDetachAll(boolean nonProxiedInstance) {
		if (converted || !convertToRecord)
			return;

		final Set<Object> copy = new HashSet<Object>(underlying);
		this.clear();
		final ODatabasePojoAbstract<TYPE> database = getDatabase();
		for (Object e : copy) {
			if (e != null) {
				if (e instanceof ORID) {
					e = database.getUserObjectByRecord(
							(ORecordInternal<?>) ((ODatabaseRecord) getDatabase().getUnderlying()).load((ORID) e, fetchPlan), fetchPlan);
					e = ((OObjectDatabaseTx) getDatabase()).detachAll(e, nonProxiedInstance);
				} else if (e instanceof ODocument) {
					e = database.getUserObjectByRecord((ORecordInternal<?>) e, fetchPlan);
					e = ((OObjectDatabaseTx) getDatabase()).detachAll(e, nonProxiedInstance);
				} else
					add((TYPE) e);
			}
		}

		converted = true;
	}

	protected ODatabasePojoAbstract<TYPE> getDatabase() {
		return (ODatabasePojoAbstract<TYPE>) ODatabaseRecordThreadLocal.INSTANCE.get().getDatabaseOwner();
	}
}
