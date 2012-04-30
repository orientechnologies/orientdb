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
package com.orientechnologies.orient.object.db;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import javassist.util.proxy.Proxy;

import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.object.ODatabaseObject;
import com.orientechnologies.orient.core.db.object.OLazyObjectListInterface;
import com.orientechnologies.orient.core.db.object.OLazyObjectMultivalueElement;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.object.enhancement.OObjectEntitySerializer;
import com.orientechnologies.orient.object.enhancement.OObjectEntityEnhancer;

@SuppressWarnings({ "unchecked" })
public class OObjectLazyList<TYPE> implements OLazyObjectListInterface<TYPE>, OLazyObjectMultivalueElement, Serializable {
	private static final long					serialVersionUID	= -1665952780303555865L;
	private ORecord<?>								sourceRecord;
	private final List<OIdentifiable>	recordList;
	private final ArrayList<Object>		list							= new ArrayList<Object>();
	private String										fetchPlan;
	private boolean										converted					= false;
	private boolean										convertToRecord		= true;

	public OObjectLazyList(final ORecord<?> iSourceRecord, final List<OIdentifiable> iRecordList) {
		this.sourceRecord = iSourceRecord;
		this.recordList = iRecordList;
		for (int i = 0; i < iRecordList.size(); i++) {
			list.add(i, null);
		}
	}

	public OObjectLazyList(final ORecord<?> iSourceRecord, final List<OIdentifiable> iRecordList,
			final Collection<? extends TYPE> iSourceList) {
		this.sourceRecord = iSourceRecord;
		this.recordList = iRecordList;
		addAll(iSourceList);
		for (int i = iSourceList.size(); i < iRecordList.size(); i++) {
			list.add(i, null);
		}
	}

	public Iterator<TYPE> iterator() {
		return new OObjectLazyListIterator<TYPE>(this, sourceRecord);
	}

	public boolean contains(final Object o) {
		if (o instanceof Proxy)
			return recordList.contains(OObjectEntitySerializer.getDocument((Proxy) o));
		else if (o instanceof OIdentifiable)
			return recordList.contains(o);
		convertAll();
		return list.contains(o);
	}

	public boolean add(TYPE element) {
		boolean dirty = false;
		if (element instanceof OIdentifiable) {
			if (converted)
				converted = false;
			if (recordList.add((OIdentifiable) element)) {
				setDirty();
				return true;
			}
		} else if (element instanceof Proxy)
			dirty = recordList.add((OIdentifiable) OObjectEntitySerializer.getDocument((Proxy) element));
		else {
			element = (TYPE) OObjectEntitySerializer.serializeObject(element, getDatabase());
			dirty = recordList.add((OIdentifiable) OObjectEntitySerializer.getDocument((Proxy) element));
		}
		if (dirty)
			setDirty();
		return list.add(element);
	}

	public void add(int index, TYPE element) {
		setDirty();
		if (element instanceof OIdentifiable) {
			if (converted)
				converted = false;
			recordList.add(index, (OIdentifiable) element);
			return;
		} else if (element instanceof Proxy)
			recordList.add(OObjectEntitySerializer.getDocument((Proxy) element));
		else {
			element = (TYPE) OObjectEntitySerializer.serializeObject(element, getDatabase());
			recordList.add(index, OObjectEntitySerializer.getDocument((Proxy) element));
		}
		list.add(index, element);
	}

	public TYPE get(final int index) {
		TYPE o = (TYPE) list.get(index);
		if (o == null) {
			OIdentifiable record = (OIdentifiable) recordList.get(index);
			o = OObjectEntityEnhancer.getInstance().getProxiedInstance(((ODocument) record.getRecord()).getClassName(),
					getDatabase().getEntityManager(), (ODocument) record.getRecord());
			list.set(index, o);
		}
		return o;
	}

	public int indexOf(final Object o) {
		if (o instanceof Proxy)
			return recordList.indexOf(OObjectEntitySerializer.getDocument((Proxy) o));
		else if (o instanceof OIdentifiable)
			return recordList.indexOf(o);
		convertAll();
		return list.indexOf(o);
	}

	public int lastIndexOf(final Object o) {
		if (o instanceof Proxy)
			return recordList.lastIndexOf(OObjectEntitySerializer.getDocument((Proxy) o));
		else if (o instanceof OIdentifiable)
			return recordList.lastIndexOf(o);
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
		return recordList.size();
	}

	public boolean isEmpty() {
		return recordList.isEmpty();
	}

	public boolean remove(Object o) {
		setDirty();
		if (o instanceof OIdentifiable) {
			int elementIndex = recordList.indexOf(o);
			if (indexLoaded(elementIndex))
				list.remove(elementIndex);
			return recordList.remove(o);
		} else if (o instanceof Proxy)
			recordList.remove((OIdentifiable) OObjectEntitySerializer.getDocument((Proxy) o));
		return list.remove(o);
	}

	public boolean containsAll(Collection<?> c) {
		for (Object o : c) {
			if (!contains(o))
				return false;
		}
		return true;
	}

	public boolean addAll(Collection<? extends TYPE> c) {
		boolean dirty = false;
		for (TYPE element : c) {
			dirty = dirty || add(element);
		}
		if (dirty)
			setDirty();
		return dirty;
	}

	public boolean addAll(int index, Collection<? extends TYPE> c) {
		for (TYPE element : c) {
			add(index, element);
			index++;
		}
		if (c.size() > 0)
			setDirty();
		return c.size() > 0;
	}

	public boolean removeAll(Collection<?> c) {
		boolean dirty = true;
		for (Object o : c) {
			dirty = dirty || remove(o);
		}
		if (dirty)
			setDirty();
		return dirty;
	}

	public boolean retainAll(Collection<?> c) {
		boolean modified = false;
		Iterator<TYPE> e = iterator();
		while (e.hasNext()) {
			if (!c.contains(e.next())) {
				remove(e);
				modified = true;
			}
		}
		return modified;
	}

	public void clear() {
		setDirty();
		recordList.clear();
		list.clear();
	}

	public TYPE set(int index, TYPE element) {
		if (element instanceof OIdentifiable) {
			if (converted)
				converted = false;
			recordList.set(index, (OIdentifiable) element);
		} else if (element instanceof Proxy)
			recordList.set(index, OObjectEntitySerializer.getDocument((Proxy) element));
		else {
			element = (TYPE) OObjectEntitySerializer.serializeObject(element, getDatabase());
			recordList.add(index, OObjectEntitySerializer.getDocument((Proxy) element));
		}
		setDirty();
		return (TYPE) list.set(index, element);
	}

	public TYPE remove(int index) {
		TYPE element;
		OIdentifiable record = recordList.remove(index);
		if (indexLoaded(index)) {
			element = (TYPE) list.remove(index);
		} else {
			element = OObjectEntityEnhancer.getInstance().getProxiedInstance(((ODocument) record.getRecord()).getClassName(),
					getDatabase().getEntityManager(), (ODocument) record.getRecord());
		}
		setDirty();
		return element;
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

	public OObjectLazyList<TYPE> setFetchPlan(final String fetchPlan) {
		this.fetchPlan = fetchPlan;
		return this;
	}

	public boolean isConvertToRecord() {
		return convertToRecord;
	}

	public void setConvertToRecord(boolean convertToRecord) {
		this.convertToRecord = convertToRecord;
	}

	public boolean isConverted() {
		return converted;
	}

	public void detach() {
		convertAll();
	}

	protected void convertAll() {
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

	/**
	 * Convert the item requested.
	 * 
	 * @param iIndex
	 *          Position of the item to convert
	 */
	private void convert(final int iIndex) {
		if (converted || !convertToRecord)
			return;

		Object o = list.get(iIndex);
		if (o == null) {

			final ODatabaseRecord database = getDatabase().getUnderlying();

			o = recordList.get(iIndex);
			ODocument doc;
			if (o instanceof ORID) {
				doc = database.load((ORID) o, fetchPlan);
			} else {
				doc = (ODocument) o;
			}
			list.set(iIndex,
					OObjectEntityEnhancer.getInstance().getProxiedInstance(doc.getClassName(), getDatabase().getEntityManager(), doc));
		}
	}

	protected ODatabaseObject getDatabase() {
		return ((ODatabaseObject) ODatabaseRecordThreadLocal.INSTANCE.get().getDatabaseOwner());
	}

	protected boolean indexLoaded(int iIndex) {
		return list.get(iIndex) != null;
	}

	@Override
	public String toString() {
		return list.toString();
	}
}
