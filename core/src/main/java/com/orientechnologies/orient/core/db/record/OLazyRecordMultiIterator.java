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

import java.util.Iterator;
import java.util.NoSuchElementException;

import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecord;

/**
 * Lazy implementation of Iterator that load the records only when accessed. It keep also track of changes to the source record
 * avoiding to call setDirty() by hand. The main difference with OLazyRecordIterator is that this iterator handles multiple
 * iterators of collections as they are just one.
 * 
 * @see OLazyRecordIterator
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OLazyRecordMultiIterator implements Iterator<OIdentifiable> {
	final private ORecord<?>	sourceRecord;
	final private Object[]		underlyingIterators;
	final private boolean			convertToRecord;
	private int								iteratorIndex	= 0;

	public OLazyRecordMultiIterator(final ORecord<?> iSourceRecord, final Object[] iIterators, final boolean iConvertToRecord) {
		this.sourceRecord = iSourceRecord;
		this.underlyingIterators = iIterators;
		this.convertToRecord = iConvertToRecord;
	}

	public OIdentifiable next() {
		if (!hasNext())
			throw new NoSuchElementException();

		final Iterator<OIdentifiable> underlying = getCurrentIterator();
		final OIdentifiable value = underlying.next();

		if (value == null)
			return null;

		if (value instanceof ORecordId && convertToRecord)
			return ((ORecordId) value).getRecord();

		return value;
	}

	public boolean hasNext() {
		final Iterator<OIdentifiable> underlying = getCurrentIterator();
		boolean again = underlying.hasNext();

		while (!again && iteratorIndex < underlyingIterators.length - 1) {
			iteratorIndex++;
			again = getCurrentIterator().hasNext();
		}

		return again;
	}

	public void remove() {
		final Iterator<OIdentifiable> underlying = getCurrentIterator();
		underlying.remove();
		if (sourceRecord != null)
			sourceRecord.setDirty();
	}

	@SuppressWarnings("unchecked")
	private Iterator<OIdentifiable> getCurrentIterator() {
		if (iteratorIndex > underlyingIterators.length)
			throw new NoSuchElementException();

		return (Iterator<OIdentifiable>) underlyingIterators[iteratorIndex];
	}
}
