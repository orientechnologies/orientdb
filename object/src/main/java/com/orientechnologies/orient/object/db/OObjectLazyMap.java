/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://orientdb.com
 *
 */
package com.orientechnologies.orient.object.db;

import com.orientechnologies.orient.core.db.object.OLazyObjectMapInterface;
import com.orientechnologies.orient.core.db.object.OObjectLazyMultivalueElement;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.object.enhancement.OObjectEntitySerializer;
import com.orientechnologies.orient.object.enhancement.OObjectProxyMethodHandler;
import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import javassist.util.proxy.Proxy;
import javassist.util.proxy.ProxyObject;

public class OObjectLazyMap<TYPE> extends HashMap<Object, Object>
    implements Serializable,
        OObjectLazyMultivalueElement<Map<Object, TYPE>>,
        OLazyObjectMapInterface<TYPE> {
  private static final long serialVersionUID = -7071023580831419958L;

  private final ProxyObject sourceRecord;
  private final Map<Object, OIdentifiable> underlying;
  private String fetchPlan;
  private boolean converted = false;
  private boolean convertToRecord = true;
  private final boolean orphanRemoval;

  public OObjectLazyMap(
      final Object iSourceRecord,
      final Map<Object, OIdentifiable> iRecordMap,
      final boolean orphanRemoval) {
    super();
    this.sourceRecord = iSourceRecord instanceof ProxyObject ? (ProxyObject) iSourceRecord : null;
    this.underlying = iRecordMap;
    converted = iRecordMap.isEmpty();
    this.orphanRemoval = orphanRemoval;
  }

  public OObjectLazyMap(
      final Object iSourceRecord,
      final Map<Object, OIdentifiable> iRecordMap,
      final Map<Object, Object> iSourceMap,
      final boolean orphanRemoval) {
    this(iSourceRecord, iRecordMap, orphanRemoval);
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
    if (o instanceof OIdentifiable) return underlying.containsValue((OIdentifiable) o);
    else if (o instanceof Proxy)
      return underlying.containsValue(OObjectEntitySerializer.getDocument((Proxy) o));
    return super.containsValue(o);
  }

  @Override
  public Object put(final Object iKey, final Object e) {
    try {
      OIdentifiable record;
      if (e instanceof OIdentifiable) {
        record = (OIdentifiable) e;
        converted = false;
        OIdentifiable o = underlying.put(iKey, record);
        if (orphanRemoval && sourceRecord != null) {
          if (record != null)
            ((OObjectProxyMethodHandler) sourceRecord.getHandler())
                .getOrphans()
                .remove(record.getIdentity());
          if (o != null && !o.getIdentity().equals(((OIdentifiable) e).getIdentity()))
            ((OObjectProxyMethodHandler) sourceRecord.getHandler())
                .getOrphans()
                .add(o.getIdentity());
        }
        return o;
      } else {
        record = e != null ? getDatabase().getRecordByUserObject(e, true) : null;
        // OIdentifiable oldValue = get(iKey) != null ?
        // getDatabase().getRecordByUserObject(get(iKey), true) : null;
        OIdentifiable oldValue = underlying.get(iKey);
        underlying.put(iKey, record);
        if (orphanRemoval && sourceRecord != null) {
          if (record != null)
            ((OObjectProxyMethodHandler) sourceRecord.getHandler())
                .getOrphans()
                .remove(record.getIdentity());
          if (((record == null && oldValue != null)
              || (oldValue != null && !oldValue.getIdentity().equals(record.getIdentity()))))
            ((OObjectProxyMethodHandler) sourceRecord.getHandler())
                .getOrphans()
                .add(oldValue.getIdentity());
        }
        return super.put(iKey, e);
      }
    } finally {
      setDirty();
    }
  }

  @Override
  public Object remove(final Object iKey) {
    OIdentifiable record = underlying.remove((String) iKey);
    if (orphanRemoval && record != null && sourceRecord != null)
      ((OObjectProxyMethodHandler) sourceRecord.getHandler())
          .getOrphans()
          .add(record.getIdentity());
    setDirty();
    return super.remove(iKey);
  }

  @Override
  public void clear() {
    converted = true;
    if (orphanRemoval && sourceRecord != null)
      for (OIdentifiable value : underlying.values())
        ((OObjectProxyMethodHandler) sourceRecord.getHandler())
            .getOrphans()
            .add(value.getIdentity());
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

  public Object getOrDefault(Object key, Object defaultValue) {
    String keyAsString = String.valueOf(key);
    Object valueToReturn;
    return (((valueToReturn = this.get(keyAsString)) != null) || this.containsKey(keyAsString))
        ? valueToReturn
        : defaultValue;
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
    if (sourceRecord != null) ((OObjectProxyMethodHandler) sourceRecord.getHandler()).setDirty();
  }

  public Map<Object, OIdentifiable> getUnderlying() {
    return underlying;
  }

  /** Assure that the requested key is converted. */
  private void convert(final String iKey) {
    if (converted || !convertToRecord) return;

    if (super.containsKey(iKey)) return;
    final ORecord record = (ORecord) underlying.get(iKey);
    if (record == null) return;
    TYPE o = (TYPE) getDatabase().getUserObjectByRecord(record, null);
    ((OObjectProxyMethodHandler) (((ProxyObject) o)).getHandler()).setParentObject(sourceRecord);
    super.put(iKey, o);
  }

  public void detach() {
    convertAll();
  }

  public void detach(boolean nonProxiedInstance) {
    convertAll();
  }

  public void detachAll(
      boolean nonProxiedInstance,
      Map<Object, Object> alreadyDetached,
      Map<Object, Object> lazyObjects) {
    convertAndDetachAll(nonProxiedInstance, alreadyDetached, lazyObjects);
  }

  @Override
  @SuppressWarnings("unchecked")
  public Map<Object, TYPE> getNonOrientInstance() {
    Map<Object, TYPE> map = new HashMap<Object, TYPE>();
    map.putAll((Map<Object, TYPE>) this);
    return map;
  }

  /** Converts all the items */
  protected void convertAll() {
    if (converted || !convertToRecord) return;

    for (java.util.Map.Entry<Object, OIdentifiable> e : underlying.entrySet())
      super.put(
          e.getKey(),
          getDatabase()
              .getUserObjectByRecord((ORecord) ((OIdentifiable) e.getValue()).getRecord(), null));

    converted = true;
  }

  protected void convertAndDetachAll(
      boolean nonProxiedInstance,
      Map<Object, Object> alreadyDetached,
      Map<Object, Object> lazyObjects) {
    if (converted || !convertToRecord) return;

    for (java.util.Map.Entry<Object, OIdentifiable> e : underlying.entrySet()) {
      Object o =
          getDatabase()
              .getUserObjectByRecord((ORecord) ((OIdentifiable) e.getValue()).getRecord(), null);
      o =
          ((OObjectDatabaseTx) getDatabase())
              .detachAll(o, nonProxiedInstance, alreadyDetached, lazyObjects);
      super.put(e.getKey(), o);
    }

    converted = true;
  }

  @SuppressWarnings("unchecked")
  protected OObjectDatabaseTx getDatabase() {
    return OLazyCollectionUtil.getDatabase();
  }
}
