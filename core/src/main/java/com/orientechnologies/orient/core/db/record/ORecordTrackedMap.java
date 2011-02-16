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

import java.util.LinkedHashMap;
import java.util.Map;

import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecord;

/**
 * Implementation of LinkedHashMap bound to a source ORecord object to keep track of changes. This avoid to call the makeDirty() by
 * hand when the map is changed.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
@SuppressWarnings("serial")
public class ORecordTrackedMap extends LinkedHashMap<Object, Object> implements ORecordElement {
	final protected ORecord<?>	sourceRecord;

	public ORecordTrackedMap(final ORecord<?> iSourceRecord) {
		this.sourceRecord = iSourceRecord;
		if (iSourceRecord != null)
			iSourceRecord.setDirty();
	}

	@Override
	public Object put(final Object iKey, final Object iValue) {
		setDirty();
		return super.put(iKey, iValue);
	}

	@Override
	public Object remove(final Object iKey) {
		setDirty();
		return super.remove(iKey);
	}

	@Override
	public void clear() {
		setDirty();
		super.clear();
	}

	@Override
	public void putAll(Map<? extends Object, ? extends Object> m) {
		setDirty();
		super.putAll(m);
	}

	@SuppressWarnings("unchecked")
	public ORecordTrackedMap setDirty() {
		if (sourceRecord != null)
			sourceRecord.setDirty();
		return this;
	}

	public void onIdentityChanged(final ORecord<?> iRecord, final int iOldHashCode) {
		final Object old = remove(iRecord);
		if (old != null)
			put(iRecord.getIdentity(), iRecord);
	}

	public void setDatabase(final ODatabaseRecord iDatabase) {
	}
}
