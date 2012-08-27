/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
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
package com.orientechnologies.orient.object.db;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import javax.print.attribute.standard.MediaSize.ISO;

import javassist.util.proxy.Proxy;
import javassist.util.proxy.ProxyObject;

import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.object.OLazyObjectMapInterface;
import com.orientechnologies.orient.core.db.object.OLazyObjectMultivalueElement;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.object.enhancement.OObjectEntitySerializer;
import com.orientechnologies.orient.object.enhancement.OObjectProxyMethodHandler;

public class OObjectLazyMap<TYPE> extends HashMap<Object, Object> implements Serializable, OLazyObjectMultivalueElement,
		OLazyObjectMapInterface<TYPE> {
	private static final long									serialVersionUID	= -7071023580831419958L;

	private final ProxyObject									sourceRecord;
	private final Map<Object, OIdentifiable>	underlying;
	private String														fetchPlan;
	private boolean														converted					= false;
	private boolean														convertToRecord		= true;

	public OObjectLazyMap(final Object iSourceRecord, final Map<Object, OIdentifiable> iRecordMap) {
		super();
		this.sourceRecord = iSourceRecord instanceof ProxyObject ? (ProxyObject) iSourceRecord : null;
		this.underlying = iRecordMap;
		converted = iRecordMap.isEmpty();
	}

	public OObjectLazyMap(final Object iSourceRecord, final Map<Object, OIdentifiable> iRecordMap,
			final Map<Object, Object> iSourceMap) {
		this(iSourceRecord, iRecordMap);
		putAll(iSourceMap);
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
		if (o instanceof OIdentifiable)
			return underlying.containsValue((OIdentifiable) o);
		else if (o instanceof Proxy)
			return underlying.containsValue(OObjectEntitySerializer.getDocument((Proxy) o));
		return super.containsValue(o);
	}

	@Override
	public Object put(final Object iKey, final Object e) {
		setDirty();
		if (e instanceof OIdentifiable) {
			converted = false;
			return underlying.put(iKey, (OIdentifiable) e);
		} else {
			underlying.put(iKey, getDatabase().getRecordByUserObject(e, true));
			return super.put(iKey, e);
		}
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

	public boolean isConverted() {
		return converted;
	}

	public OObjectLazyMap<TYPE> setFetchPlan(String fetchPlan) {
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

	public void setDirty() {
		if (sourceRecord != null)
			((OObjectProxyMethodHandler) sourceRecord.getHandler()).setDirty();
	}

	public Map<Object, OIdentifiable> getUnderlying() {
		return underlying;
	}

	/**
	 * Assure that the requested key is converted.
	 */
	private void convert(final String iKey) {
		if (converted || !convertToRecord)
			return;

		if (super.containsKey(iKey))
			return;
		TYPE o = getDatabase().getUserObjectByRecord((ORecordInternal<?>) underlying.get(iKey), null);
		((OObjectProxyMethodHandler) (((ProxyObject) o)).getHandler()).setParentObject(sourceRecord);
		super.put(iKey, o);
	}

	public void detach() {
		convertAll();
	}

	public void detachAll(boolean nonProxiedInstance) {
		convertAndDetachAll(nonProxiedInstance);

	}

	/**
	 * Converts all the items
	 */
	protected void convertAll() {
		if (converted || !convertToRecord)
			return;

		for (java.util.Map.Entry<Object, OIdentifiable> e : underlying.entrySet())
			super.put(e.getKey(),
					getDatabase().getUserObjectByRecord((ORecordInternal<?>) ((OIdentifiable) e.getValue()).getRecord(), null));

		converted = true;
	}

	protected void convertAndDetachAll(boolean nonProxiedInstance) {
		if (converted || !convertToRecord)
			return;

		for (java.util.Map.Entry<Object, OIdentifiable> e : underlying.entrySet()) {
			Object o = getDatabase().getUserObjectByRecord((ORecordInternal<?>) ((OIdentifiable) e.getValue()).getRecord(), null);
			o = ((OObjectDatabaseTx) getDatabase()).detachAll(o, nonProxiedInstance);
			super.put(e.getKey(), o);
		}

		converted = true;
	}

	@SuppressWarnings("unchecked")
	protected ODatabasePojoAbstract<TYPE> getDatabase() {
		return (ODatabasePojoAbstract<TYPE>) ODatabaseRecordThreadLocal.INSTANCE.get().getDatabaseOwner();
	}
}
