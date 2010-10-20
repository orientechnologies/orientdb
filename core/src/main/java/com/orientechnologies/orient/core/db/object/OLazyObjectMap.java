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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.orientechnologies.orient.core.db.ODatabasePojoAbstract;
import com.orientechnologies.orient.core.record.ORecordInternal;

@SuppressWarnings("serial")
public class OLazyObjectMap<TYPE> extends HashMap<String, Object> {
	private final ODatabasePojoAbstract<?, TYPE>	database;
	private final Map<String, Object>							underlying;
	private String																fetchPlan;
	private boolean																converted	= false;

	public OLazyObjectMap(final ODatabasePojoAbstract<?, TYPE> database, final Map<String, Object> iSource) {
		this.database = database;
		this.underlying = iSource;

		converted = iSource.isEmpty();
	}

	public int size() {
		return underlying.size();
	}

	public boolean isEmpty() {
		return underlying.isEmpty();
	}

	public boolean containsKey(final Object k) {
		return underlying.containsKey(k);
	}

	public boolean containsValue(final Object o) {
		convertAll();
		return containsValue(o);
	}

	public Object put(final String iKey, final Object e) {
		underlying.put(iKey, database.getRecordByUserObject(e, false));
		return super.put(iKey, e);
	}

	public Object remove(final Object o) {
		underlying.remove(database.getRecordByUserObject(o, false));
		return super.remove(o);
	}

	public void clear() {
		converted = true;
		underlying.clear();
		super.clear();
	}

	public String getFetchPlan() {
		return fetchPlan;
	}

	public OLazyObjectMap<TYPE> setFetchPlan(String fetchPlan) {
		this.fetchPlan = fetchPlan;
		return this;
	}

	@Override
	public String toString() {
		return underlying.toString();
	}

	public Set<java.util.Map.Entry<String, Object>> entrySet() {
		throw new UnsupportedOperationException("entrySet");
	}

	public Object get(final Object iKey) {
		convert((String) iKey);
		return super.get(iKey);
	}

	public Set<String> keySet() {
		convertAll();
		return underlying.keySet();
	}

	public void putAll(Map<? extends String, ? extends Object> arg0) {
		// TODO Auto-generated method stub

	}

	public Collection<Object> values() {
		convertAll();
		return super.values();
	}

	/**
	 * Assure that the requested key is converted.
	 */
	private void convert(final String iKey) {
		if (converted)
			return;

		super.put(iKey, database.getUserObjectByRecord((ORecordInternal<?>) underlying.get(iKey), null));
	}

	/**
	 * Convert all the items
	 */
	private void convertAll() {
		if (converted)
			return;

		for (java.util.Map.Entry<String, Object> e : underlying.entrySet())
			super.put(e.getKey(), database.getUserObjectByRecord((ORecordInternal<?>) e.getValue(), null));

		converted = true;
	}
}
