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

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.orientechnologies.orient.core.db.ODatabasePojoAbstract;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;

public class OLazyObjectMap<TYPE> extends HashMap<Object, Object> implements Serializable {
	private static final long					serialVersionUID	= 4146521893082733694L;

	private final ORecord<?>					sourceRecord;
	private final Map<Object, Object>	underlying;
	private String										fetchPlan;
	private boolean										converted					= false;
	private boolean										convertToRecord		= true;

	public OLazyObjectMap(final ORecord<?> iSourceRecord, final Map<Object, Object> iSource) {
		this.sourceRecord = iSourceRecord;
		this.underlying = iSource;

		converted = iSource.isEmpty();
	}

	@Override
	public int size() {
		return underlying.size();
	}

	@Override
	public boolean isEmpty() {
		return underlying.isEmpty();
	}

	@Override
	public boolean containsKey(final Object k) {
		return underlying.containsKey(k);
	}

	@Override
	public boolean containsValue(final Object o) {
		convertAll();
		return underlying.containsValue(o);
	}

	@Override
	@SuppressWarnings("unchecked")
	public Object put(final Object iKey, final Object e) {
		if (e instanceof ODocument) {
			converted = false;
			underlying.put(iKey, e);
		} else {
			if (getDatabase().getRecordByUserObject(e, false) == null) {
				underlying.put(iKey, getDatabase().pojo2Stream((TYPE) e, new ODocument((ODatabaseRecord) getDatabase().getUnderlying())));
			} else {
				underlying.put(iKey, getDatabase().getRecordByUserObject(e, false));
			}
		}
		setDirty();
		return super.put(iKey, e);
	}

	@Override
	public Object remove(final Object iKey) {
		underlying.remove((String) iKey);
		setDirty();
		return super.remove(iKey);
	}

	@Override
	public void clear() {
		converted = true;
		underlying.clear();
		super.clear();
		setDirty();
	}

	public String getFetchPlan() {
		return fetchPlan;
	}

	public boolean isConvertToRecord() {
		return convertToRecord;
	}

	public void setConvertToRecord(boolean convertToRecord) {
		this.convertToRecord = convertToRecord;
	}

	public OLazyObjectMap<TYPE> setFetchPlan(String fetchPlan) {
		this.fetchPlan = fetchPlan;
		return this;
	}

	@Override
	public String toString() {
		return underlying.toString();
	}

	@Override
	public Set<java.util.Map.Entry<Object, Object>> entrySet() {
		convertAll();
		return super.entrySet();
	}

	@Override
	public Object get(final Object iKey) {
		convert((String) iKey);
		return super.get(iKey);
	}

	@Override
	public Set<Object> keySet() {
		convertAll();
		return underlying.keySet();
	}

	@Override
	public void putAll(final Map<? extends Object, ? extends Object> iMap) {
		for (java.util.Map.Entry<? extends Object, ? extends Object> e : iMap.entrySet()) {
			put(e.getKey(), e.getValue());
		}
	}

	@Override
	public Collection<Object> values() {
		convertAll();
		return super.values();
	}

	public Map<Object, Object> getUnderlying() {
		return underlying;
	}

	public void setDirty() {
		if (sourceRecord != null)
			sourceRecord.setDirty();
	}

	/**
	 * Assure that the requested key is converted.
	 */
	private void convert(final String iKey) {
		if (converted || !convertToRecord)
			return;

		super.put(iKey, getDatabase().getUserObjectByRecord((ORecordInternal<?>) underlying.get(iKey), null));
	}

	public void detach() {
		convertAll();
	}

	/**
	 * Converts all the items
	 */
	protected void convertAll() {
		if (converted || !convertToRecord)
			return;

		for (java.util.Map.Entry<Object, Object> e : underlying.entrySet())
			super.put(e.getKey(), getDatabase().getUserObjectByRecord((ORecordInternal<?>) e.getValue(), null));

		converted = true;
	}

	@SuppressWarnings("unchecked")
	protected ODatabasePojoAbstract<TYPE> getDatabase() {
		return (ODatabasePojoAbstract<TYPE>) ODatabaseRecordThreadLocal.INSTANCE.get().getDatabaseOwner();
	}
}
