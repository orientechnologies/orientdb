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

import java.util.Iterator;

import com.orientechnologies.orient.core.db.ODatabasePojoAbstract;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * Lazy implementation of Iterator that load the records only when accessed. It keep also track of changes to the source record
 * avoiding to call setDirty() by hand.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
@SuppressWarnings({ "unchecked" })
public class OLazyObjectIterator<TYPE> implements Iterator<TYPE> {
	private final ORecord<?>											sourceRecord;
	private final ODatabasePojoAbstract<?, TYPE>	database;
	private final Iterator<Object>								underlying;
	private String																fetchPlan;

	public OLazyObjectIterator(final ODatabasePojoAbstract<?, TYPE> database, final ORecord<?> iSourceRecord,
			final Iterator<Object> iIterator) {
		this.database = database;
		this.sourceRecord = iSourceRecord;
		this.underlying = iIterator;
	}

	public TYPE next() {
		return next(fetchPlan);
	}

	public TYPE next(final String iFetchPlan) {
		final Object value = underlying.next();

		if (value == null)
			return null;

		if (value instanceof ODocument)
			return (TYPE) database.getUserObjectByRecord((ODocument) value, iFetchPlan);

		return (TYPE) value;
	}

	public boolean hasNext() {
		return underlying.hasNext();
	}

	public void remove() {
		underlying.remove();
		if (sourceRecord != null)
			sourceRecord.setDirty();
	}

	public String getFetchPlan() {
		return fetchPlan;
	}

	public void setFetchPlan(String fetchPlan) {
		this.fetchPlan = fetchPlan;
	}
}
