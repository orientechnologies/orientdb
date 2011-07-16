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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import com.orientechnologies.orient.core.db.ODatabasePojoAbstract;
import com.orientechnologies.orient.core.db.graph.ODatabaseGraphTx;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;

@SuppressWarnings({ "unchecked" })
public class OLazyObjectList<TYPE> implements List<TYPE>, Serializable {
	private static final long											serialVersionUID	= 289711963195698937L;
	private ORecord<?>														sourceRecord;
	private final ArrayList<Object>								list							= new ArrayList<Object>();
	private transient ODatabasePojoAbstract<TYPE>	database;
	private String																fetchPlan;
	private boolean																converted					= false;
	private boolean																convertToRecord		= true;

	public OLazyObjectList(final ODatabaseGraphTx iDatabase, final ORecord<?> iSourceRecord, final Collection<?> iSourceList) {
		this((ODatabasePojoAbstract<TYPE>) iDatabase, iSourceRecord, iSourceList);
		this.sourceRecord = iSourceRecord;
	}

	public OLazyObjectList(final ODatabasePojoAbstract<TYPE> iDatabase, final ORecord<?> iSourceRecord,
			final Collection<?> iSourceList) {
		this(iDatabase);
		this.sourceRecord = iSourceRecord;
		if (iSourceList != null)
			list.addAll(iSourceList);
	}

	public OLazyObjectList(final ODatabasePojoAbstract<TYPE> iDatabase) {
		this.database = iDatabase;
	}

	public Iterator<TYPE> iterator() {
		return new OLazyObjectIterator<TYPE>(database, sourceRecord, list.iterator(), convertToRecord);
	}

	public boolean contains(final Object o) {
		convertAll();
		return list.contains(o);
	}

	public boolean add(TYPE element) {
		if (converted && element instanceof ORID)
			converted = false;
		setDirty();
		return list.add(element);
	}

	public void add(int index, TYPE element) {
		if (converted && element instanceof ORID)
			converted = false;
		setDirty();
		list.add(index, element);
	}

	public TYPE get(final int index) {
		convert(index);
		return (TYPE) list.get(index);
	}

	public int indexOf(final Object o) {
		convertAll();
		return list.indexOf(o);
	}

	public int lastIndexOf(final Object o) {
		convertAll();
		return list.lastIndexOf(o);
	}

	public Object[] toArray() {
		convertAll();
		return list.toArray();
	}

	public <T> T[] toArray(final T[] a) {
		convertAll();
		return list.toArray(a);
	}

	public int size() {
		return list.size();
	}

	public boolean isEmpty() {
		return list.isEmpty();
	}

	public boolean remove(Object o) {
		convertAll();
		setDirty();
		return list.remove(o);
	}

	public boolean containsAll(Collection<?> c) {
		convertAll();
		return list.containsAll(c);
	}

	public boolean addAll(Collection<? extends TYPE> c) {
		setDirty();
		return list.addAll(c);
	}

	public boolean addAll(int index, Collection<? extends TYPE> c) {
		setDirty();
		return list.addAll(index, c);
	}

	public boolean removeAll(Collection<?> c) {
		convertAll();
		setDirty();
		return list.removeAll(c);
	}

	public boolean retainAll(Collection<?> c) {
		convertAll();
		setDirty();
		return list.retainAll(c);
	}

	public void clear() {
		setDirty();
		list.clear();
	}

	public TYPE set(int index, TYPE element) {
		convert(index);
		setDirty();
		return (TYPE) list.set(index, element);
	}

	public TYPE remove(int index) {
		convert(index);
		setDirty();
		return (TYPE) list.remove(index);
	}

	public ListIterator<TYPE> listIterator() {
		return (ListIterator<TYPE>) list.listIterator();
	}

	public ListIterator<TYPE> listIterator(int index) {
		return (ListIterator<TYPE>) list.listIterator(index);
	}

	public List<TYPE> subList(int fromIndex, int toIndex) {
		return (List<TYPE>) list.subList(fromIndex, toIndex);
	}

	public String getFetchPlan() {
		return fetchPlan;
	}

	public OLazyObjectList<TYPE> setFetchPlan(String fetchPlan) {
		this.fetchPlan = fetchPlan;
		return this;
	}

	public boolean isConvertToRecord() {
		return convertToRecord;
	}

	public void setConvertToRecord(boolean convertToRecord) {
		this.convertToRecord = convertToRecord;
	}

	public void convertAll() {
		if (converted || !convertToRecord)
			return;

		for (int i = 0; i < size(); ++i)
			convert(i);

		converted = true;
	}

	public void setDirty() {
		if (sourceRecord != null)
			sourceRecord.setDirty();
	}

	public void assignDatabase(final ODatabasePojoAbstract<TYPE> iDatabase) {
		if (database == null || database.isClosed()) {
			database = iDatabase;
		}
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

		final Object o = list.get(iIndex);

		if (o != null) {
			if (o instanceof ORID)
				list.set(
						iIndex,
						database.getUserObjectByRecord(
								(ORecordInternal<?>) ((ODatabaseRecord) database.getUnderlying()).load((ORID) o, fetchPlan), fetchPlan));
			else if (o instanceof ODocument)
				list.set(iIndex, database.getUserObjectByRecord((ORecordInternal<?>) o, fetchPlan));
		}
	}

	@Override
	public String toString() {
		return list.toString();
	}
}
