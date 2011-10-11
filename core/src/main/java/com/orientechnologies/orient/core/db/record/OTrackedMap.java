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

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.ORecord;

/**
 * Implementation of LinkedHashMap bound to a source ORecord object to keep track of changes. This avoid to call the makeDirty() by
 * hand when the map is changed.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
@SuppressWarnings("serial")
public class OTrackedMap<T> extends LinkedHashMap<Object, T> implements ORecordElement, Serializable {
	final protected ORecord<?>		sourceRecord;
	private STATUS								status				= STATUS.NOT_LOADED;
	protected final static Object	ENTRY_REMOVAL	= new Object();

	public OTrackedMap(final ORecord<?> iSourceRecord) {
		this.sourceRecord = iSourceRecord;
		if (iSourceRecord != null)
			iSourceRecord.setDirty();
	}

	@SuppressWarnings("unchecked")
	@Override
	public T put(final Object iKey, final T iValue) {
		Object oldValue = super.get(iKey);
		if (oldValue != null && oldValue == iValue)
			return (T) oldValue;

		setDirty();
		return super.put(iKey, iValue);
	}

	@Override
	public T remove(final Object iKey) {
		setDirty();
		return super.remove(iKey);
	}

	@Override
	public void clear() {
		setDirty();
		super.clear();
	}

	@Override
	public void putAll(Map<? extends Object, ? extends T> m) {
		super.putAll(m);
	}

	@SuppressWarnings({ "unchecked" })
	public OTrackedMap<T> setDirty() {
		if (status != STATUS.UNMARSHALLING && sourceRecord != null && !sourceRecord.isDirty())
			sourceRecord.setDirty();
		return this;
	}

	public void onBeforeIdentityChanged(final ORID iRID) {
		remove(iRID);
	}

	@SuppressWarnings("unchecked")
	public void onAfterIdentityChanged(final ORecord<?> iRecord) {
		super.put(iRecord.getIdentity(), (T) iRecord);
	}

	public boolean setDatabase(final ODatabaseRecord iDatabase) {
		boolean changed = false;

		for (Map.Entry<Object, T> e : entrySet()) {
			if (e.getKey() instanceof ORecordElement)
				if (((ORecordElement) e.getKey()).setDatabase(iDatabase))
					changed = true;

			if (e.getValue() instanceof ORecordElement)
				if (((ORecordElement) e.getValue()).setDatabase(iDatabase))
					changed = true;
		}

		return changed;
	}

	public STATUS getInternalStatus() {
		return status;
	}

	public void setInternalStatus(final STATUS iStatus) {
		status = iStatus;
	}
}
