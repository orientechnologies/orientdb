/*
 * Copyright 2010-2012 Luca Molino (molino.luca--at--gmail.com)
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

import java.io.Serializable;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.object.enhancement.OObjectEntitySerializer;

/**
 * 
 * @author Luca Molino (molino.luca--at--gmail.com)
 * 
 */
@SuppressWarnings("unchecked")
public class OObjectCustomSerializerSet<TYPE> extends HashSet<TYPE> implements OObjectLazyCustomSerializer<Set<TYPE>>, Serializable {
  private static final long serialVersionUID = -7698875159671927472L;

  private final ORecord  sourceRecord;
  private final Set<Object> underlying;
  private boolean           converted        = false;
  private final Class<?>    deserializeClass;

  public OObjectCustomSerializerSet(final Class<?> iDeserializeClass, final ORecord iSourceRecord,
      final Set<Object> iRecordSource) {
    this.sourceRecord = iSourceRecord;
    this.underlying = iRecordSource;
    this.deserializeClass = iDeserializeClass;
  }

  public OObjectCustomSerializerSet(final Class<?> iDeserializeClass, final ORecord iSourceRecord,
      final Set<Object> iRecordSource, final Set<? extends TYPE> iSourceCollection) {
    this.sourceRecord = iSourceRecord;
    this.underlying = iRecordSource;
    this.deserializeClass = iDeserializeClass;
    convertAll();
    addAll(iSourceCollection);
  }

  public Iterator<TYPE> iterator() {
    return (Iterator<TYPE>) new OObjectCustomSerializerIterator<TYPE>(deserializeClass, sourceRecord, underlying.iterator());
  }

  public int size() {
    return underlying.size();
  }

  public boolean isEmpty() {
    return underlying.isEmpty();
  }

  public boolean contains(final Object o) {
    boolean underlyingContains = underlying.contains(OObjectEntitySerializer.serializeFieldValue(deserializeClass, o));
    return underlyingContains || super.contains(o);
  }

  public Object[] toArray() {
    return toArray(new Object[size()]);
  }

  public <T> T[] toArray(final T[] a) {
    convertAll();
    return super.toArray(a);
  }

  public boolean add(final TYPE e) {
    underlying.add(OObjectEntitySerializer.serializeFieldValue(deserializeClass, e));
    return super.add(e);
  }

  public boolean remove(final Object e) {
    underlying.remove(OObjectEntitySerializer.serializeFieldValue(deserializeClass, e));
    return super.remove(e);
  }

  public boolean containsAll(final Collection<?> c) {
    for (Object o : c)
      if (!super.contains(o) && !underlying.contains(OObjectEntitySerializer.serializeFieldValue(deserializeClass, o)))
        return false;

    return true;
  }

  public boolean addAll(final Collection<? extends TYPE> c) {
    boolean modified = false;
    setDirty();
    for (Object o : c)
      modified = add((TYPE) o) || modified;
    return modified;
  }

  public boolean retainAll(final Collection<?> c) {
    boolean modified = false;
    Iterator<TYPE> e = iterator();
    while (e.hasNext()) {
      if (!c.contains(e.next())) {
        remove(e);
        modified = true;
      }
    }
    return modified;
  }

  public void clear() {
    setDirty();
    underlying.clear();
  }

  public boolean removeAll(final Collection<?> c) {
    setDirty();
    boolean modified = super.removeAll(c);
    for (Object o : c) {
      modified = modified || underlying.remove(OObjectEntitySerializer.serializeFieldValue(deserializeClass, o));
    }
    return modified;
  }

  public boolean isConverted() {
    return converted;
  }

  @Override
  public String toString() {
    return underlying.toString();
  }

  public void setDirty() {
    if (sourceRecord != null)
      sourceRecord.setDirty();
  }

  public void detach() {
    convertAll();
  }

  public void detach(boolean nonProxiedInstance) {
    convertAll();
  }

  public void detachAll(boolean nonProxiedInstance, Map<Object, Object> alreadyDetached, Map<Object, Object> lazyObjects) {
    convertAll();
  }

  @Override
  public Set<TYPE> getNonOrientInstance() {
    Set<TYPE> set = new HashSet<TYPE>();
    set.addAll(this);
    return set;
  }

  @Override
  public Object getUnderlying() {
    return underlying;
  }

  protected void convertAll() {
    if (converted)
      return;

    super.clear();
    for (Object o : underlying) {
      super.add((TYPE) OObjectEntitySerializer.deserializeFieldValue(deserializeClass, o));
    }

    converted = true;
  }

}
