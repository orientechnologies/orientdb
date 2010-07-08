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

import java.util.ArrayList;
import java.util.Iterator;

import com.orientechnologies.orient.core.db.object.ODatabaseObjectTx;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;

@SuppressWarnings({ "unchecked", "serial" })
public class OLazyList<TYPE> extends ArrayList<TYPE> {
	private ODatabaseObjectTx	database;
	private String						fetchPlan;

	public OLazyList(final ODatabaseObjectTx database) {
		this.database = database;
	}

	public Iterator<TYPE> iterator() {
		return new OLazyIterator<TYPE>(database, (Iterator<ODocument>) super.iterator());
	}

	public void convertAll() {
		for (int i = 0; i < size(); ++i)
			convert(i);
	}

	@Override
	public boolean contains(final Object o) {
		convertAll();
		return super.contains(o);
	}

	@Override
	public TYPE get(final int index) {
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

	public String getFetchPlan() {
		return fetchPlan;
	}

	public OLazyList<TYPE> setFetchPlan(String fetchPlan) {
		this.fetchPlan = fetchPlan;
		return this;
	}

	/**
	 * Convert the item requested.
	 * 
	 * @param iIndex
	 *          Position of the item to convert
	 */
	private void convert(final int iIndex) {
		Object o = super.get(iIndex);

		if (o != null && o instanceof ODocument)
			super.set(iIndex, (TYPE) database.getUserObjectByRecord((ORecordInternal<?>) o, fetchPlan));
	}
}
