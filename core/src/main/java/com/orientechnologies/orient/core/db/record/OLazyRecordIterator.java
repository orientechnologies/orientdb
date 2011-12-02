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

import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecord;

/**
 * Lazy implementation of Iterator that load the records only when accessed. It keep also track of changes to the source record
 * avoiding to call setDirty() by hand.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OLazyRecordIterator implements Iterator<OIdentifiable> {
	final private ORecord<?>												sourceRecord;
	final private Iterator<? extends OIdentifiable>	underlying;
	final private boolean														autoConvert2Record;

	public OLazyRecordIterator(final Iterator<? extends OIdentifiable> iIterator, final boolean iConvertToRecord) {
		this.sourceRecord = null;
		this.underlying = iIterator;
		this.autoConvert2Record = iConvertToRecord;
	}

	public OLazyRecordIterator(final ORecord<?> iSourceRecord, final Iterator<? extends OIdentifiable> iIterator,
			final boolean iConvertToRecord) {
		this.sourceRecord = iSourceRecord;
		this.underlying = iIterator;
		this.autoConvert2Record = iConvertToRecord;
	}

	public OIdentifiable next() {
		final OIdentifiable value = underlying.next();

		if (value == null)
			return null;

		if (value instanceof ORecordId && autoConvert2Record)
			return ((ORecordId) value).getRecord();

		return value;
	}

	public boolean hasNext() {
		return underlying.hasNext();
	}

	public void remove() {
		underlying.remove();
		if (sourceRecord != null)
			sourceRecord.setDirty();
	}
}
