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

import java.util.Collection;

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordFactory;
import com.orientechnologies.orient.core.record.ORecordInternal;

/**
 * Lazy implementation of LinkedHashMap. It's bound to a source ORecord object to keep track of changes. This avoid to call the
 * makeDirty() by hand when the map is changed.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
@SuppressWarnings("serial")
public class ORecordLazyMap extends ORecordTrackedMap {
	private ODatabaseRecord	database;
	final private byte			recordType;
	private boolean					converted				= false;
	private boolean					convertToRecord	= true;

	public ORecordLazyMap(final ORecord<?> iSourceRecord, final byte iRecordType) {
		super(iSourceRecord);
		this.database = iSourceRecord.getDatabase();
		this.recordType = iRecordType;
	}

	@Override
	public boolean containsValue(final Object o) {
		convertAll();
		return super.containsValue(o);
	}

	@Override
	public Object get(final Object iKey) {
		if (iKey == null)
			return null;

		final String key = iKey.toString();

		convert(key);
		return super.get(key);
	}

	@Override
	public Object put(final Object iKey, final Object iValue) {
		if (converted && iValue instanceof ORID)
			converted = false;
		return super.put(iKey, iValue);
	}

	@Override
	public Collection<Object> values() {
		convertAll();
		return super.values();
	}

	@Override
	public String toString() {
		return OMultiValue.toString(this);
	}

	public boolean isConvertToRecord() {
		return convertToRecord;
	}

	public void setConvertToRecord(boolean convertToRecord) {
		this.convertToRecord = convertToRecord;
	}

	private void convertAll() {
		if (converted || !convertToRecord)
			return;

		if (sourceRecord.getDatabase() == null)
			return;

		for (Object key : super.keySet())
			convert(key);
		converted = true;
	}

	/**
	 * Convert the item requested.
	 * 
	 * @param iIndex
	 *          Position of the item to convert
	 */
	private void convert(final Object iKey) {
		if (converted || !convertToRecord)
			return;

		if (database == null)
			return;

		final Object o = super.get(iKey);

		if (o != null && o instanceof ORecordId) {
			final ORecordInternal<?> record = ORecordFactory.newInstance(recordType);
			final ORecordId rid = (ORecordId) o;

			record.setDatabase(database);
			record.setIdentity(rid);

			try {
				record.load();
				super.put(iKey, record);
			} catch (ORecordNotFoundException e) {
				// IGNORE THIS
			}
		}
	}

	public byte getRecordType() {
		return recordType;
	}

	@Override
	public void setDatabase(final ODatabaseRecord iDatabase) {
		database = iDatabase;
	}
}
