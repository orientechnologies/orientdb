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
import java.util.Iterator;

import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.ORecordMultiValueHelper.MULTIVALUE_CONTENT_TYPE;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * Lazy implementation of LinkedHashMap. It's bound to a source ORecord object to keep track of changes. This avoid to call the
 * makeDirty() by hand when the map is changed.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
@SuppressWarnings({ "serial", "unchecked" })
public class ORecordLazyMap extends OTrackedMap<OIdentifiable> implements ORecordLazyMultiValue {
	final private byte																			recordType;
	private ORecordMultiValueHelper.MULTIVALUE_CONTENT_TYPE	status							= MULTIVALUE_CONTENT_TYPE.EMPTY;
	private boolean																					autoConvertToRecord	= true;

	public ORecordLazyMap(final ORecord<?> iSourceRecord) {
		super(iSourceRecord);
		this.recordType = ODocument.RECORD_TYPE;
	}

	public ORecordLazyMap(final ORecord<?> iSourceRecord, final byte iRecordType) {
		super(iSourceRecord);
		this.recordType = iRecordType;
	}

	@Override
	public boolean containsValue(final Object o) {
		// convertLinks2Records();
		return super.containsValue(o);
	}

	@Override
	public OIdentifiable get(final Object iKey) {
		if (iKey == null)
			return null;

		final String key = iKey.toString();

		convertLink2Record(key);
		return super.get(key);
	}

	@Override
	public OIdentifiable put(final Object iKey, OIdentifiable iValue) {
		if (status == MULTIVALUE_CONTENT_TYPE.ALL_RIDS && iValue instanceof ORecord<?> && !((ORecord<?>) iValue).getIdentity().isNew())
			// IT'S BETTER TO LEAVE ALL RIDS AND EXTRACT ONLY THIS ONE
			iValue = ((ORecord<?>) iValue).getIdentity();
		else
			status = ORecordMultiValueHelper.updateContentType(status, iValue);

		return super.put(iKey, iValue);
	}

	@Override
	public Collection<OIdentifiable> values() {
		convertLinks2Records();
		return super.values();
	}

	@Override
	public OIdentifiable remove(Object o) {
		final OIdentifiable result = super.remove(o);
		if (size() == 0)
			status = MULTIVALUE_CONTENT_TYPE.EMPTY;
		return result;
	}

	@Override
	public void clear() {
		super.clear();
		status = MULTIVALUE_CONTENT_TYPE.EMPTY;
	}

	@Override
	public String toString() {
		return ORecordMultiValueHelper.toString(this);
	}

	public boolean isAutoConvertToRecord() {
		return autoConvertToRecord;
	}

	public void setAutoConvertToRecord(boolean convertToRecord) {
		this.autoConvertToRecord = convertToRecord;
	}

	public void convertLinks2Records() {
		if (status == MULTIVALUE_CONTENT_TYPE.ALL_RECORDS || !autoConvertToRecord)
			// PRECONDITIONS
			return;

		for (Object k : keySet())
			convertLink2Record(k);

		status = MULTIVALUE_CONTENT_TYPE.ALL_RECORDS;
	}

	public boolean convertRecords2Links() {
		if (status == MULTIVALUE_CONTENT_TYPE.ALL_RIDS)
			// PRECONDITIONS
			return true;

		boolean allConverted = true;
		for (Object k : keySet())
			if (!convertRecord2Link(k))
				allConverted = false;

		if (allConverted)
			status = MULTIVALUE_CONTENT_TYPE.ALL_RIDS;

		return allConverted;
	}

	private boolean convertRecord2Link(final Object iKey) {
		if (status == MULTIVALUE_CONTENT_TYPE.ALL_RIDS)
			return true;

		final Object value = super.get(iKey);
		if (value != null)
			if (value instanceof ORecord<?> && !((ORecord<?>) value).getIdentity().isNew()) {
				if (((ORecord<?>) value).isDirty())
					ODatabaseRecordThreadLocal.INSTANCE.get().save((ORecordInternal<?>) value);

				// OVERWRITE
				super.put(iKey, ((ORecord<?>) value).getIdentity());

				// CONVERTED
				return true;
			} else if (value instanceof ORID)
				// ALREADY CONVERTED
				return true;

		return false;
	}

	/**
	 * Convert the item with the received key to a record.
	 * 
	 * @param iIndex
	 *          Position of the item to convert
	 */
	private void convertLink2Record(final Object iKey) {
		if (status == MULTIVALUE_CONTENT_TYPE.ALL_RECORDS)
			return;

		final Object value;

		if (iKey instanceof ORID)
			value = iKey;
		else
			value = super.get(iKey);

		if (value != null && value instanceof ORID) {
			final ORID rid = (ORID) value;

			try {
				// OVERWRITE IT
				super.put(iKey, rid.getRecord());
			} catch (ORecordNotFoundException e) {
				// IGNORE THIS
			}
		}
	}

	public byte getRecordType() {
		return recordType;
	}

	public Iterator<OIdentifiable> rawIterator() {
		return new OLazyRecordIterator(sourceRecord, super.values().iterator(), false);
	}

	public boolean detach() {
		return convertRecords2Links();
	}
}
