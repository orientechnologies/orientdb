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
package com.orientechnologies.orient.object.serialization;

import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.object.enhancement.OObjectEntitySerializer;
import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class OObjectCustomSerializerMap<TYPE> extends HashMap<Object, Object>
    implements Serializable, OObjectLazyCustomSerializer<Map<Object, TYPE>> {
  private static final long serialVersionUID = -8606432090996808181L;

  private final ORecord sourceRecord;
  private final Map<Object, Object> underlying;
  private boolean converted = false;
  private final Class<?> deserializeClass;

  public OObjectCustomSerializerMap(
      final Class<?> iDeserializeClass,
      final ORecord iSourceRecord,
      final Map<Object, Object> iRecordMap) {
    super();
    this.sourceRecord = iSourceRecord;
    this.underlying = iRecordMap;
    converted = iRecordMap.isEmpty();
    this.deserializeClass = iDeserializeClass;
  }

  public OObjectCustomSerializerMap(
      final Class<?> iDeserializeClass,
      final ORecord iSourceRecord,
      final Map<Object, Object> iRecordMap,
      final Map<Object, Object> iSourceMap) {
    this(iDeserializeClass, iSourceRecord, iRecordMap);
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
    boolean underlyingContains =
        underlying.containsValue(OObjectEntitySerializer.serializeFieldValue(deserializeClass, o));
    return underlyingContains || super.containsValue(o);
  }

  @Override
  public Object put(final Object iKey, final Object e) {
    setDirty();
    underlying.put(iKey, OObjectEntitySerializer.serializeFieldValue(deserializeClass, e));
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
    if (o != null)
      super.put(iKey, OObjectEntitySerializer.deserializeFieldValue(deserializeClass, o));
    else {
      o = underlying.get(iKey);

      if (o != null)
        super.put(iKey, OObjectEntitySerializer.deserializeFieldValue(deserializeClass, o));
    }
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
  @SuppressWarnings("unchecked")
  public Map<Object, TYPE> getNonOrientInstance() {
    Map<Object, TYPE> map = new HashMap<Object, TYPE>();
    map.putAll((Map<Object, TYPE>) this);
    return map;
  }

  @Override
  public Object getUnderlying() {
    return underlying;
  }

  /** Converts all the items */
  protected void convertAll() {
    if (converted) return;

    for (java.util.Map.Entry<Object, Object> e : underlying.entrySet())
      super.put(
          e.getKey(),
          OObjectEntitySerializer.deserializeFieldValue(deserializeClass, e.getValue()));

    converted = true;
  }
}
