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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import javassist.util.proxy.Proxy;
import javassist.util.proxy.ProxyObject;

import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.object.ODatabaseObject;
import com.orientechnologies.orient.core.db.object.OLazyObjectListInterface;
import com.orientechnologies.orient.core.db.object.OLazyObjectMultivalueElement;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.object.enhancement.OObjectEntityEnhancer;
import com.orientechnologies.orient.object.enhancement.OObjectEntitySerializer;
import com.orientechnologies.orient.object.enhancement.OObjectProxyMethodHandler;

@SuppressWarnings({ "unchecked" })
public class OObjectLazyList<TYPE> extends ArrayList<TYPE> implements OLazyObjectListInterface<TYPE>, OLazyObjectMultivalueElement,
    Serializable {
  private static final long         serialVersionUID = -1665952780303555865L;
  private ProxyObject               sourceRecord;
  private final List<OIdentifiable> recordList;
  private String                    fetchPlan;
  private boolean                   converted        = false;
  private boolean                   convertToRecord  = true;

  public OObjectLazyList(final Object iSourceRecord, final List<OIdentifiable> iRecordList) {
    this.sourceRecord = iSourceRecord instanceof ProxyObject ? (ProxyObject) iSourceRecord : null;
    this.recordList = iRecordList;
    for (int i = 0; i < iRecordList.size(); i++) {
      super.add(i, null);
    }
  }

  public OObjectLazyList(final Object iSourceRecord, final List<OIdentifiable> iRecordList,
      final Collection<? extends TYPE> iSourceList) {
    this.sourceRecord = iSourceRecord instanceof ProxyObject ? (ProxyObject) iSourceRecord : null;
    this.recordList = iRecordList;
    for (int i = 0; i < iRecordList.size(); i++) {
      super.add(i, null);
    }
    addAll(iSourceList);
  }

  public Iterator<TYPE> iterator() {
    return new OObjectLazyListIterator<TYPE>(this, sourceRecord);
  }

  public boolean contains(final Object o) {
    if (o instanceof Proxy)
      return recordList.contains(OObjectEntitySerializer.getDocument((Proxy) o));
    else if (o instanceof OIdentifiable)
      return recordList.contains(o);
    convertAll();
    return super.contains(o);
  }

  public boolean add(TYPE element) {
    boolean dirty = false;
    if (element instanceof OIdentifiable) {
      if (converted)
        converted = false;
      if (recordList.add((OIdentifiable) element)) {
        setDirty();
        return true;
      }
    } else if (element instanceof Proxy)
      dirty = recordList.add((OIdentifiable) OObjectEntitySerializer.getDocument((Proxy) element));
    else {
      element = (TYPE) OObjectEntitySerializer.serializeObject(element, getDatabase());
      dirty = recordList.add((OIdentifiable) OObjectEntitySerializer.getDocument((Proxy) element));
    }
    if (dirty)
      setDirty();
    return super.add(element);
  }

  public void add(int index, TYPE element) {
    setDirty();
    if (element instanceof OIdentifiable) {
      if (converted)
        converted = false;
      recordList.add(index, (OIdentifiable) element);
      return;
    } else if (element instanceof Proxy)
      recordList.add(OObjectEntitySerializer.getDocument((Proxy) element));
    else {
      element = (TYPE) OObjectEntitySerializer.serializeObject(element, getDatabase());
      recordList.add(index, OObjectEntitySerializer.getDocument((Proxy) element));
    }
    super.add(index, element);
  }

  public TYPE get(final int index) {
    TYPE o = (TYPE) super.get(index);
    if (o == null) {
      OIdentifiable record = (OIdentifiable) recordList.get(index);
      o = (TYPE) OObjectEntityEnhancer.getInstance().getProxiedInstance(((ODocument) record.getRecord()).getClassName(),
          getDatabase().getEntityManager(), (ODocument) record.getRecord(), sourceRecord);
      super.set(index, o);
    }
    return o;
  }

  public int indexOf(final Object o) {
    if (o instanceof Proxy)
      return recordList.indexOf(OObjectEntitySerializer.getDocument((Proxy) o));
    else if (o instanceof OIdentifiable)
      return recordList.indexOf(o);
    convertAll();
    return super.indexOf(o);
  }

  public int lastIndexOf(final Object o) {
    if (o instanceof Proxy)
      return recordList.lastIndexOf(OObjectEntitySerializer.getDocument((Proxy) o));
    else if (o instanceof OIdentifiable)
      return recordList.lastIndexOf(o);
    convertAll();
    return super.lastIndexOf(o);
  }

  public Object[] toArray() {
    convertAll();
    return super.toArray();
  }

  public <T> T[] toArray(final T[] a) {
    convertAll();
    return super.toArray(a);
  }

  public int size() {
    return recordList.size();
  }

  public boolean isEmpty() {
    return recordList.isEmpty();
  }

  public boolean remove(Object o) {
    setDirty();
    if (o instanceof OIdentifiable) {
      int elementIndex = recordList.indexOf(o);
      if (indexLoaded(elementIndex))
        super.remove(elementIndex);
      return recordList.remove(o);
    } else if (o instanceof Proxy)
      recordList.remove((OIdentifiable) OObjectEntitySerializer.getDocument((Proxy) o));
    return super.remove(o);
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
      dirty = remove(o) || dirty;
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
    recordList.clear();
    super.clear();
  }

  public TYPE set(int index, TYPE element) {
    if (element instanceof OIdentifiable) {
      if (converted)
        converted = false;
      recordList.set(index, (OIdentifiable) element);
    } else if (element instanceof Proxy)
      recordList.set(index, OObjectEntitySerializer.getDocument((Proxy) element));
    else {
      element = (TYPE) OObjectEntitySerializer.serializeObject(element, getDatabase());
      recordList.add(index, OObjectEntitySerializer.getDocument((Proxy) element));
    }
    setDirty();
    return (TYPE) super.set(index, element);
  }

  public TYPE remove(int index) {
    TYPE element;
    OIdentifiable record = recordList.remove(index);
    if (indexLoaded(index)) {
      element = (TYPE) super.remove(index);
    } else {
      element = (TYPE) OObjectEntityEnhancer.getInstance().getProxiedInstance(((ODocument) record.getRecord()).getClassName(),
          getDatabase().getEntityManager(), (ODocument) record.getRecord(), sourceRecord);
    }
    setDirty();
    return element;
  }

  public ListIterator<TYPE> listIterator() {
    return (ListIterator<TYPE>) super.listIterator();
  }

  public ListIterator<TYPE> listIterator(int index) {
    return (ListIterator<TYPE>) super.listIterator(index);
  }

  public List<TYPE> subList(int fromIndex, int toIndex) {
    return (List<TYPE>) super.subList(fromIndex, toIndex);
  }

  public String getFetchPlan() {
    return fetchPlan;
  }

  public OObjectLazyList<TYPE> setFetchPlan(final String fetchPlan) {
    this.fetchPlan = fetchPlan;
    return this;
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

  public void detach() {
    convertAll();
  }

  protected void convertAll() {
    if (converted || !convertToRecord)
      return;

    for (int i = 0; i < size(); ++i)
      convert(i);

    converted = true;
  }

  public void setDirty() {
    if (sourceRecord != null)
      ((OObjectProxyMethodHandler) sourceRecord.getHandler()).setDirty();
  }

  /**
   * Convert the item requested.
   * 
   * @param iIndex
   *          Position of the item to convert
   */
  private void convert(final int iIndex) {
    if (converted || !convertToRecord)
      return;

    Object o = super.get(iIndex);
    if (o == null) {

      final ODatabaseRecord database = getDatabase().getUnderlying();

      o = recordList.get(iIndex);
      ODocument doc;
      if (o instanceof ORID) {
        doc = database.load((ORID) o, fetchPlan);
      } else {
        doc = (ODocument) o;
      }
      super.set(
          iIndex,
          (TYPE) OObjectEntityEnhancer.getInstance().getProxiedInstance(doc.getClassName(), getDatabase().getEntityManager(), doc,
              sourceRecord));
    }
  }

  public void detachAll(boolean nonProxiedInstance) {
    convertAndDetachAll(nonProxiedInstance);
  }

  protected void convertAndDetachAll(boolean nonProxiedInstance) {
    if (converted || !convertToRecord)
      return;

    for (int i = 0; i < size(); ++i)
      convertAndDetachAll(i, nonProxiedInstance);

    converted = true;
  }

  private void convertAndDetachAll(final int iIndex, boolean nonProxiedInstance) {
    if (converted || !convertToRecord)
      return;

    Object o = super.get(iIndex);
    if (o == null) {

      final ODatabaseRecord database = getDatabase().getUnderlying();

      o = recordList.get(iIndex);
      ODocument doc;
      if (o instanceof ORID) {
        doc = database.load((ORID) o, fetchPlan);
      } else {
        doc = (ODocument) o;
      }
      o = OObjectEntityEnhancer.getInstance().getProxiedInstance(doc.getClassName(), getDatabase().getEntityManager(), doc,
          sourceRecord);
      o = ((OObjectDatabaseTx) getDatabase()).detachAll(o, nonProxiedInstance);
      super.set(iIndex, (TYPE) o);
    }
  }

  protected ODatabaseObject getDatabase() {
    return ((ODatabaseObject) ODatabaseRecordThreadLocal.INSTANCE.get().getDatabaseOwner());
  }

  protected boolean indexLoaded(int iIndex) {
    return super.get(iIndex) != null;
  }

  @Override
  public String toString() {
    return super.toString();
  }
}
