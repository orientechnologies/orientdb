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

import java.util.HashSet;
import java.util.Iterator;

import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.id.ORID;

@SuppressWarnings({ "serial" })
public class OLazyRecordSet extends HashSet<Object> {
	private ODatabaseRecord<?>	database;
	private byte								recordType;
	private boolean							converted	= false;

	public OLazyRecordSet(final ODatabaseRecord<?> database, final byte iRecordType) {
		this.database = database;
		this.recordType = iRecordType;
	}

	@Override
	public Iterator<Object> iterator() {
		return new OLazyRecordIterator(database, recordType, super.iterator());
	}

	@Override
	public boolean add(final Object e) {
		if (converted && e instanceof ORID)
			converted = false;
		return super.add(e);
	}

	@Override
	public boolean contains(final Object o) {
		convertAll();
		return super.contains(o);
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

	/**
	 * Browse all the set to convert all the items.
	 */
	public void convertAll() {
		if (converted)
			return;

		for (Iterator<Object> it = iterator(); it.hasNext(); it.next())
			;

		converted = true;
	}
}
