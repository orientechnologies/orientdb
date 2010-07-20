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
package com.orientechnologies.orient.core.db.document;

import java.util.Iterator;

import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordFactory;
import com.orientechnologies.orient.core.record.ORecordInternal;

@SuppressWarnings({ "unchecked" })
public class OLazyRecordIterator<REC extends ORecord<?>> implements Iterator<REC> {
	final private ODatabaseRecord<?>	database;
	final private Iterator<REC>				underlying;
	private byte											recordType;

	public OLazyRecordIterator(final ODatabaseRecord<?> database, final byte iRecordType, final Iterator<REC> iIterator) {
		this.database = database;
		this.underlying = iIterator;
		this.recordType = iRecordType;
	}

	public REC next() {
		final Object value = underlying.next();

		if (value == null)
			return null;

		if (value instanceof ORecordId) {
			ORecordInternal<?> record = ORecordFactory.newInstance(recordType);
			record.setDatabase(database);
			record.setIdentity((ORecordId) value);
			record.load();
			return (REC) record;
		}
		
		return (REC) value;
	}

	public boolean hasNext() {
		return underlying.hasNext();
	}

	public void remove() {
		underlying.remove();
	}
}
