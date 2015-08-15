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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.object.enhancement.OObjectEntitySerializer;

@SuppressWarnings({ "unchecked" })
public class OObjectCustomSerializerList<TYPE> implements List<TYPE>, OObjectLazyCustomSerializer<List<TYPE>>, Serializable {
  private static final long       serialVersionUID = -8541477416577361792L;

  private ORecord              sourceRecord;
  private final List<Object>      serializedList;
  private final ArrayList<Object> list             = new ArrayList<Object>();
  private boolean                 converted        = false;
  private final Class<?>          deserializeClass;

  public OObjectCustomSerializerList(final Class<?> iDeserializeClass, final ORecord iSourceRecord,
      final List<Object> iRecordList) {
    this.sourceRecord = iSourceRecord;
    this.serializedList = iRecordList;
    this.deserializeClass = iDeserializeClass;
    for (int i = 0; i < iRecordList.size(); i++) {
      list.add(i, null);
    }
  }

  public OObjectCustomSerializerList(final Class<?> iDeserializeClass, final ORecord iSourceRecord,
      final List<Object> iRecordList, final Collection<? extends TYPE> iSourceList) {
    this.sourceRecord = iSourceRecord;
    this.serializedList = iRecordList;
    this.deserializeClass = iDeserializeClass;
    addAll(iSourceList);
    for (int i = iSourceList.size(); i < iRecordList.size(); i++) {
      list.add(i, null);
    }
  }

  public Iterator<TYPE> iterator() {
    return new OObjectCustomSerializerIterator<TYPE>(deserializeClass, sourceRecord, serializedList.iterator());
  }

  public boolean contains(final Object o) {
    boolean underlyingContains = serializedList.contains(OObjectEntitySerializer.serializeFieldValue(deserializeClass, o));
    return underlyingContains || list.contains(o);
  }

  public boolean add(TYPE element) {
    serializedList.add(OObjectEntitySerializer.serializeFieldValue(deserializeClass, element));
    return list.add(element);
  }

  public void add(int index, TYPE element) {
    setDirty();
    serializedList.add(index, OObjectEntitySerializer.serializeFieldValue(deserializeClass, element));
    list.add(index, element);
  }

  public TYPE get(final int index) {
    TYPE o = (TYPE) list.get(index);
    if (o == null) {
      Object toDeserialize = serializedList.get(index);
      o = (TYPE) OObjectEntitySerializer.deserializeFieldValue(deserializeClass, toDeserialize);
      list.set(index, o);
    }
    return o;
  }

  public int indexOf(final Object o) {
    return list.indexOf(o);
  }

  public int lastIndexOf(final Object o) {
    return list.lastIndexOf(o);
  }

  public Object[] toArray() {
    convertAll();
    return list.toArray();
  }

  public <T> T[] toArray(final T[] a) {
    convertAll();
    return list.toArray(a);
  }

  public int size() {
    return serializedList.size();
  }

  public boolean isEmpty() {
    return serializedList.isEmpty();
  }

  public boolean remove(Object o) {
    setDirty();
    int indexOfO = list.indexOf(o);
    serializedList.remove(indexOfO);
    return list.remove(o);
  }

  public boolean containsAll(Collection<?> c) {
    for (Object o : c) {
      if (!contains(o))
        return false;
    }
    return true;
  }

  public boolean addAll(Collection<? extends TYPE> c) {
    boolean dirty = false;
    for (TYPE element : c) {
      dirty = add(element) || dirty;
    }
    if (dirty)
      setDirty();
    return dirty;
  }

  public boolean addAll(int index, Collection<? extends TYPE> c) {
    for (TYPE element : c) {
      add(index, element);
      index++;
    }
    if (c.size() > 0)
      setDirty();
    return c.size() > 0;
  }

  public boolean removeAll(Collection<?> c) {
    boolean dirty = true;
    for (Object o : c) {
      dirty = dirty || remove(o);
    }
    if (dirty)
      setDirty();
    return dirty;
  }

  public boolean retainAll(Collection<?> c) {
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
    serializedList.clear();
    list.clear();
  }

  public TYPE set(int index, TYPE element) {
    serializedList.set(index, OObjectEntitySerializer.serializeFieldValue(deserializeClass, element));
    return (TYPE) list.set(index, element);
  }

  public TYPE remove(int index) {
    serializedList.remove(index);
    return (TYPE) list.remove(index);
  }

  public ListIterator<TYPE> listIterator() {
    return (ListIterator<TYPE>) list.listIterator();
  }

  public ListIterator<TYPE> listIterator(int index) {
    return (ListIterator<TYPE>) list.listIterator(index);
  }

  public List<TYPE> subList(int fromIndex, int toIndex) {
    return (List<TYPE>) list.subList(fromIndex, toIndex);
  }

  public boolean isConverted() {
    return converted;
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

  protected void convertAll() {
    if (converted)
      return;

    for (int i = 0; i < size(); ++i)
      convert(i);

    converted = true;
  }

  public void setDirty() {
    if (sourceRecord != null)
      sourceRecord.setDirty();
  }

  @Override
  public List<TYPE> getNonOrientInstance() {
    List<TYPE> list = new ArrayList<TYPE>();
    list.addAll(this);
    return this;
  }

  /**
   * Convert the item requested.
   * 
   * @param iIndex
   *          Position of the item to convert
   */
  private void convert(final int iIndex) {
    if (converted)
      return;

    Object o = list.get(iIndex);
    if (o == null) {
      o = serializedList.get(iIndex);
      list.set(iIndex, OObjectEntitySerializer.deserializeFieldValue(deserializeClass, o));
    }
  }

  protected boolean indexLoaded(int iIndex) {
    return list.get(iIndex) != null;
  }

  @Override
  public String toString() {
    return list.toString();
  }

  @Override
  public Object getUnderlying() {
    return serializedList;
  }
}
