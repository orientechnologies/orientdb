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
package com.orientechnologies.orient.core.iterator;

import java.util.Iterator;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.graph.ODatabaseGraphTx;
import com.orientechnologies.orient.core.db.graph.OGraphElement;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.ODatabaseRecordAbstract;
import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * Iterator to browse all the vertexes.
 * 
 * @author Luca Garulli
 * 
 */
@SuppressWarnings("unchecked")
public abstract class OGraphElementIterator<T extends OGraphElement> implements Iterator<T>, Iterable<T> {
	protected ODatabaseGraphTx								database;
	protected ORecordIteratorClass<ODocument>	underlying;
	private String														fetchPlan;
	private T																	reusedObject;
	private String														className;

	public OGraphElementIterator(final ODatabaseGraphTx iDatabase, final String iClassName, final boolean iPolymorphic) {
		database = iDatabase;
		className = iClassName;
		underlying = new ORecordIteratorClass<ODocument>((ODatabaseRecord) iDatabase.getUnderlying(),
				(ODatabaseRecordAbstract) ((ODatabaseDocumentTx) iDatabase.getUnderlying()).getUnderlying(), iClassName, iPolymorphic);

		setReuseSameObject(false);
		underlying.setReuseSameRecord(false);
	}

	public OGraphElementIterator(final ODatabaseGraphTx iDatabase, final ORecordIteratorClass<ODocument> iUnderlying) {
		database = iDatabase;
		underlying = iUnderlying;
	}

	public abstract T next(final String iFetchPlan);

	public boolean hasNext() {
		return underlying.hasNext();
	}

	public T next() {
		return next(fetchPlan);
	}

	public void remove() {
		underlying.remove();
	}

	public Iterator<T> iterator() {
		return this;
	}

	public String getFetchPlan() {
		return fetchPlan;
	}

	public OGraphElementIterator<T> setFetchPlan(String fetchPlan) {
		this.fetchPlan = fetchPlan;
		return this;
	}

	/**
	 * Tells if the iterator is using the same object for browsing.
	 * 
	 * @see #setReuseSameObject(boolean)
	 */
	public boolean isReuseSameObject() {
		return reusedObject != null;
	}

	/**
	 * Tells to the iterator to use the same object for browsing. The object will be reset before every use. This improve the
	 * performance and reduce memory utilization since it does not create a new one for each operation, but pay attention to copy the
	 * data of the object once read otherwise they will be reset to the next operation.
	 * 
	 * @param iReuse
	 * @return @see #isReuseSameObject()
	 */
	public OGraphElementIterator<T> setReuseSameObject(boolean iReuse) {
		reusedObject = (T) database.newInstance(className);
		return this;
	}

	/**
	 * Returns the object to use for the operation.
	 * 
	 * @return
	 */
	protected T getObject() {
		final T object;
		if (reusedObject != null) {
			// REUSE THE SAME RECORD AFTER HAVING RESETTED IT
			object = reusedObject;
			object.reset();
		} else
			// CREATE A NEW ONE
			object = (T) database.newInstance(className);
		return object;
	}
}
