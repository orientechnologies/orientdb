/*
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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
package com.orientechnologies.orient.object.enumerations;

import com.orientechnologies.orient.core.record.ORecord;
import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class OObjectEnumLazyMap<TYPE extends Enum> extends HashMap<Object, Object>
    implements Serializable, OObjectLazyEnumSerializer<Map<Object, Object>> {
  private static final long serialVersionUID = -8606432090996808181L;

  private final ORecord sourceRecord;
  private final Map<Object, Object> underlying;
  private boolean converted = false;
  private final Class<Enum> enumClass;

  public OObjectEnumLazyMap(
      final Class<Enum> iEnumClass,
      final ORecord iSourceRecord,
      final Map<Object, Object> iRecordMap) {
    super();
    this.sourceRecord = iSourceRecord;
    this.underlying = iRecordMap;
    converted = iRecordMap.isEmpty();
    this.enumClass = iEnumClass;
  }

  public OObjectEnumLazyMap(
      final Class<Enum> iEnumClass,
      final ORecord iSourceRecord,
      final Map<Object, Object> iRecordMap,
      final Map<Object, Object> iSourceMap) {
    this(iEnumClass, iSourceRecord, iRecordMap);
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
    boolean underlyingContains = underlying.containsValue(o.toString());
    return underlyingContains || super.containsValue(o);
  }

  @Override
  public Object put(final Object iKey, final Object e) {
    setDirty();
    underlying.put(iKey, ((TYPE) e).name());
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

  public boolean isConverted() {
    return converted;
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
    convert(iKey);
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
    if (sourceRecord != null) sourceRecord.setDirty();
  }

  /** Assure that the requested key is converted. */
  private void convert(final Object iKey) {
    if (converted) return;

    if (super.containsKey(iKey)) return;

    Object o = underlying.get(String.valueOf(iKey));
    if (o instanceof Number) super.put(iKey, enumClass.getEnumConstants()[((Number) o).intValue()]);
    else super.put(iKey, Enum.valueOf(enumClass, o.toString()));
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
    convertAll();
  }

  @Override
  public Map<Object, Object> getNonOrientInstance() {
    Map<Object, Object> map = new HashMap<Object, Object>();
    map.putAll((Map<Object, Object>) this);
    return map;
  }

  @Override
  public Object getUnderlying() {
    return underlying;
  }

  /** Converts all the items */
  protected void convertAll() {
    if (converted) return;

    for (java.util.Map.Entry<Object, Object> e : underlying.entrySet()) {
      if (e.getValue() instanceof Number)
        super.put(e.getKey(), enumClass.getEnumConstants()[((Number) e.getValue()).intValue()]);
      else super.put(e.getKey(), Enum.valueOf(enumClass, e.getValue().toString()));
    }

    converted = true;
  }
}
