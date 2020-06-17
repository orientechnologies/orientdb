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

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.object.OLazyObjectListInterface;
import com.orientechnologies.orient.core.db.object.OObjectLazyMultivalueElement;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.object.enhancement.OObjectEntityEnhancer;
import com.orientechnologies.orient.object.enhancement.OObjectEntitySerializer;
import com.orientechnologies.orient.object.enhancement.OObjectProxyMethodHandler;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import javassist.util.proxy.Proxy;
import javassist.util.proxy.ProxyObject;

@SuppressWarnings({"unchecked"})
public class OObjectLazyList<TYPE> extends ArrayList<TYPE>
    implements OLazyObjectListInterface<TYPE>,
        OObjectLazyMultivalueElement<List<TYPE>>,
        Serializable {
  private static final long serialVersionUID = -1665952780303555865L;
  private ProxyObject sourceRecord;
  private final List<OIdentifiable> recordList;
  private String fetchPlan;
  private boolean converted = false;
  private boolean convertToRecord = true;
  private final boolean orphanRemoval;

  public OObjectLazyList(
      final Object iSourceRecord,
      final List<OIdentifiable> iRecordList,
      final boolean orphanRemoval) {
    this.sourceRecord = iSourceRecord instanceof ProxyObject ? (ProxyObject) iSourceRecord : null;
    this.recordList = iRecordList;
    this.orphanRemoval = orphanRemoval;
    for (int i = 0; i < iRecordList.size(); i++) {
      super.add(i, null);
    }
  }

  public OObjectLazyList(
      final Object iSourceRecord,
      final List<OIdentifiable> iRecordList,
      final Collection<? extends TYPE> iSourceList,
      final boolean orphanRemoval) {
    this.sourceRecord = iSourceRecord instanceof ProxyObject ? (ProxyObject) iSourceRecord : null;
    this.recordList = iRecordList;
    this.orphanRemoval = orphanRemoval;
    for (int i = 0; i < iRecordList.size(); i++) {
      super.add(i, null);
    }
    addAll(iSourceList);
  }

  public Iterator<TYPE> iterator() {
    return new OObjectLazyListIterator<TYPE>(this, sourceRecord);
  }

  public Spliterator<TYPE> spliterator() {
    return Spliterators.spliterator(this, Spliterator.ORDERED);
  }

  public boolean contains(final Object o) {
    if (o instanceof Proxy)
      return recordList.contains(OObjectEntitySerializer.getDocument((Proxy) o));
    else if (o instanceof OIdentifiable) return recordList.contains(o);
    convertAll();
    return super.contains(o);
  }

  public boolean add(TYPE element) {
    boolean dirty = false;
    OIdentifiable record;
    if (element instanceof OIdentifiable) {
      record = (OIdentifiable) element;
      if (converted) converted = false;
      if (recordList.add(record)) {
        setDirty();
        if (orphanRemoval && record != null && sourceRecord != null)
          ((OObjectProxyMethodHandler) sourceRecord.getHandler())
              .getOrphans()
              .remove(record.getIdentity());
        return true;
      }
    } else if (element instanceof Proxy) {
      record = (OIdentifiable) OObjectEntitySerializer.getDocument((Proxy) element);
      if (orphanRemoval && record != null && sourceRecord != null)
        ((OObjectProxyMethodHandler) sourceRecord.getHandler())
            .getOrphans()
            .remove(record.getIdentity());
      dirty = recordList.add(record);
    } else {
      element = (TYPE) OObjectEntitySerializer.serializeObject(element, getDatabase());
      record = (OIdentifiable) OObjectEntitySerializer.getDocument((Proxy) element);
      if (orphanRemoval && record != null && sourceRecord != null)
        ((OObjectProxyMethodHandler) sourceRecord.getHandler())
            .getOrphans()
            .remove(record.getIdentity());
      dirty = recordList.add(record);
    }
    if (dirty) setDirty();
    return super.add(element);
  }

  public void add(int index, TYPE element) {
    setDirty();
    OIdentifiable record;
    if (element instanceof OIdentifiable) {
      record = (OIdentifiable) element;
      if (converted) converted = false;
      if (orphanRemoval && record != null && sourceRecord != null)
        ((OObjectProxyMethodHandler) sourceRecord.getHandler())
            .getOrphans()
            .remove(record.getIdentity());
      recordList.add(index, record);
      return;
    } else if (element instanceof Proxy) {
      record = (OIdentifiable) OObjectEntitySerializer.getDocument((Proxy) element);
      if (orphanRemoval && record != null && sourceRecord != null)
        ((OObjectProxyMethodHandler) sourceRecord.getHandler())
            .getOrphans()
            .remove(record.getIdentity());
      recordList.add(index, record);
    } else {
      element = (TYPE) OObjectEntitySerializer.serializeObject(element, getDatabase());
      record = (OIdentifiable) OObjectEntitySerializer.getDocument((Proxy) element);
      if (orphanRemoval && record != null && sourceRecord != null)
        ((OObjectProxyMethodHandler) sourceRecord.getHandler())
            .getOrphans()
            .remove(record.getIdentity());
      recordList.add(index, record);
    }
    super.add(index, element);
  }

  public TYPE get(final int index) {
    TYPE o = (TYPE) super.get(index);
    if (o == null) {
      OIdentifiable record = (OIdentifiable) recordList.get(index);
      if (record == null || record.getRecord() == null) {
        OLogManager.instance()
            .warn(
                this,
                "Record "
                    + ((OObjectProxyMethodHandler) sourceRecord.getHandler()).getDoc().getIdentity()
                    + " references a deleted instance");
        return null;
      }
      o =
          (TYPE)
              OObjectEntityEnhancer.getInstance()
                  .getProxiedInstance(
                      ((ODocument) record.getRecord()).getClassName(),
                      getDatabase().getEntityManager(),
                      (ODocument) record.getRecord(),
                      sourceRecord);
      super.set(index, o);
    }
    return o;
  }

  public int indexOf(final Object o) {
    if (o instanceof Proxy)
      return recordList.indexOf(OObjectEntitySerializer.getDocument((Proxy) o));
    else if (o instanceof OIdentifiable) return recordList.indexOf(o);
    convertAll();
    return super.indexOf(o);
  }

  public int lastIndexOf(final Object o) {
    if (o instanceof Proxy)
      return recordList.lastIndexOf(OObjectEntitySerializer.getDocument((Proxy) o));
    else if (o instanceof OIdentifiable) return recordList.lastIndexOf(o);
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
      elementIndex =
          elementIndex > -1 ? elementIndex : recordList.indexOf(((OIdentifiable) o).getRecord());
      if (elementIndex > -1 && indexLoaded(elementIndex)) super.remove(elementIndex);
      if (orphanRemoval && sourceRecord != null)
        ((OObjectProxyMethodHandler) sourceRecord.getHandler())
            .getOrphans()
            .add(((OIdentifiable) o).getIdentity());
      return recordList.remove(o);
    } else if (o instanceof Proxy) {
      OIdentifiable record = (OIdentifiable) OObjectEntitySerializer.getDocument((Proxy) o);
      if (orphanRemoval && record != null && sourceRecord != null)
        ((OObjectProxyMethodHandler) sourceRecord.getHandler())
            .getOrphans()
            .add(record.getIdentity());
      recordList.remove(record);
    } else {
      OIdentifiable record = getDatabase().getRecordByUserObject(o, false);
      if (orphanRemoval && record != null && sourceRecord != null)
        ((OObjectProxyMethodHandler) sourceRecord.getHandler())
            .getOrphans()
            .add(record.getIdentity());
      recordList.remove(record);
    }
    return super.remove(o);
  }

  public boolean containsAll(Collection<?> c) {
    for (Object o : c) {
      if (!contains(o)) return false;
    }
    return true;
  }

  public boolean addAll(Collection<? extends TYPE> c) {
    boolean dirty = false;
    for (TYPE element : c) {
      dirty |= add(element);
    }
    if (dirty) setDirty();
    return dirty;
  }

  public boolean addAll(int index, Collection<? extends TYPE> c) {
    for (TYPE element : c) {
      add(index, element);
      index++;
    }
    if (c.size() > 0) setDirty();
    return c.size() > 0;
  }

  public boolean removeAll(Collection<?> c) {
    boolean dirty = true;
    for (Object o : c) {
      dirty = remove(o) || dirty;
    }
    if (dirty) setDirty();
    return dirty;
  }

  public boolean retainAll(Collection<?> c) {
    boolean modified = false;
    Iterator<TYPE> e = iterator();
    while (e.hasNext()) {
      Object value = e.next();
      if (!c.contains(value)) {
        remove(value);
        modified = true;
      }
    }
    return modified;
  }

  public void clear() {
    setDirty();
    if (orphanRemoval && sourceRecord != null)
      for (OIdentifiable value : recordList)
        ((OObjectProxyMethodHandler) sourceRecord.getHandler())
            .getOrphans()
            .add(value.getIdentity());
    recordList.clear();
    super.clear();
  }

  public TYPE set(int index, TYPE element) {
    OIdentifiable record;
    if (element instanceof OIdentifiable) {
      record = (OIdentifiable) element;
      if (converted) converted = false;
      if (orphanRemoval && record != null && sourceRecord != null)
        ((OObjectProxyMethodHandler) sourceRecord.getHandler())
            .getOrphans()
            .remove(record.getIdentity());
      recordList.set(index, record);
    } else if (element instanceof Proxy) {
      record = (OIdentifiable) OObjectEntitySerializer.getDocument((Proxy) element);
      if (orphanRemoval && record != null && sourceRecord != null)
        ((OObjectProxyMethodHandler) sourceRecord.getHandler())
            .getOrphans()
            .remove(record.getIdentity());
      recordList.set(index, record);
    } else {
      element = (TYPE) OObjectEntitySerializer.serializeObject(element, getDatabase());
      record = getDatabase().getRecordByUserObject(element, false);
      if (orphanRemoval && record != null && sourceRecord != null)
        ((OObjectProxyMethodHandler) sourceRecord.getHandler())
            .getOrphans()
            .remove(record.getIdentity());
      recordList.set(index, record);
    }
    setDirty();
    return (TYPE) super.set(index, element);
  }

  public TYPE remove(int index) {
    TYPE element;
    OIdentifiable record = recordList.remove(index);
    if (indexLoaded(index)) {
      if (orphanRemoval && record != null && sourceRecord != null)
        ((OObjectProxyMethodHandler) sourceRecord.getHandler())
            .getOrphans()
            .add(record.getIdentity());
      element = (TYPE) super.remove(index);
    } else {
      if (orphanRemoval && record != null && sourceRecord != null)
        ((OObjectProxyMethodHandler) sourceRecord.getHandler())
            .getOrphans()
            .add(record.getIdentity());
      element =
          (TYPE)
              OObjectEntityEnhancer.getInstance()
                  .getProxiedInstance(
                      ((ODocument) record.getRecord()).getClassName(),
                      getDatabase().getEntityManager(),
                      (ODocument) record.getRecord(),
                      sourceRecord);
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

  public void detach(boolean nonProxiedInstance) {
    convertAll();
  }

  protected void convertAll() {
    if (converted || !convertToRecord) return;

    for (int i = 0; i < size(); ++i) convert(i);

    converted = true;
  }

  public void setDirty() {
    if (sourceRecord != null) ((OObjectProxyMethodHandler) sourceRecord.getHandler()).setDirty();
  }

  @Override
  public List<TYPE> getNonOrientInstance() {
    List<TYPE> list = new ArrayList<TYPE>();
    list.addAll(this);
    return list;
  }

  @Override
  public Object getUnderlying() {
    return recordList;
  }

  /**
   * Convert the item requested.
   *
   * @param iIndex Position of the item to convert
   */
  private void convert(final int iIndex) {
    if (converted || !convertToRecord) return;

    Object o = super.get(iIndex);
    if (o == null) {

      final ODatabaseDocument database = getDatabase().getUnderlying();

      o = recordList.get(iIndex);
      ODocument doc;
      if (o instanceof ORID) {
        doc = database.load((ORID) o, fetchPlan);
      } else {
        doc = (ODocument) o;
      }
      if (o == null) {
        OLogManager.instance()
            .warn(
                this,
                "Record "
                    + ((OObjectProxyMethodHandler) sourceRecord.getHandler()).getDoc().getIdentity()
                    + " references a deleted instance");
        return;
      }
      super.set(
          iIndex,
          (TYPE)
              OObjectEntityEnhancer.getInstance()
                  .getProxiedInstance(
                      doc.getClassName(), getDatabase().getEntityManager(), doc, sourceRecord));
    }
  }

  public void detachAll(
      boolean nonProxiedInstance,
      Map<Object, Object> alreadyDetached,
      Map<Object, Object> lazyObjects) {
    convertAndDetachAll(nonProxiedInstance, alreadyDetached, lazyObjects);
  }

  protected void convertAndDetachAll(
      boolean nonProxiedInstance,
      Map<Object, Object> alreadyDetached,
      Map<Object, Object> lazyObjects) {
    if (converted || !convertToRecord) return;

    for (int i = 0; i < size(); ++i)
      convertAndDetachAll(i, nonProxiedInstance, alreadyDetached, lazyObjects);

    converted = true;
  }

  private void convertAndDetachAll(
      final int iIndex,
      boolean nonProxiedInstance,
      Map<Object, Object> alreadyDetached,
      Map<Object, Object> lazyObjects) {
    if (converted || !convertToRecord) return;

    Object o = super.get(iIndex);
    if (o == null) {

      final ODatabaseDocument database = getDatabase().getUnderlying();

      o = recordList.get(iIndex);
      ODocument doc;
      if (o instanceof ORID) {
        doc = database.load((ORID) o, fetchPlan);
      } else {
        doc = (ODocument) o;
      }
      if (o == null) {
        OLogManager.instance()
            .warn(
                this,
                "Record "
                    + ((OObjectProxyMethodHandler) sourceRecord.getHandler()).getDoc().getIdentity()
                    + " references a deleted instance");
        return;
      }
      o =
          OObjectEntityEnhancer.getInstance()
              .getProxiedInstance(
                  doc.getClassName(), getDatabase().getEntityManager(), doc, sourceRecord);
      o =
          ((OObjectDatabaseTx) getDatabase())
              .detachAll(o, nonProxiedInstance, alreadyDetached, lazyObjects);
      super.set(iIndex, (TYPE) o);
    }
  }

  protected OObjectDatabaseTx getDatabase() {
    return (OObjectDatabaseTx) OLazyCollectionUtil.getDatabase();
  }

  protected boolean indexLoaded(int iIndex) {
    return super.get(iIndex) != null;
  }

  @Override
  public String toString() {
    return super.toString();
  }
}
