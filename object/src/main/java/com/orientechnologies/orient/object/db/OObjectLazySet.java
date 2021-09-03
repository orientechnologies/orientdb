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
package com.orientechnologies.orient.object.db;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.object.OLazyObjectSetInterface;
import com.orientechnologies.orient.core.db.object.OObjectLazyMultivalueElement;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.object.enhancement.OObjectProxyMethodHandler;
import java.io.Serializable;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import javassist.util.proxy.ProxyObject;

/**
 * Lazy implementation of Set. It's bound to a source ORecord object to keep track of changes. This
 * avoid to call the makeDirty() by hand when the set is changed.
 *
 * @author Luca Molino (molino.luca--at--gmail.com)
 */
@SuppressWarnings("unchecked")
public class OObjectLazySet<TYPE> extends HashSet<TYPE>
    implements OLazyObjectSetInterface<TYPE>,
        OObjectLazyMultivalueElement<Set<TYPE>>,
        Serializable {
  private static final long serialVersionUID = 1793910544017627989L;

  private final ProxyObject sourceRecord;
  private final Set<OIdentifiable> underlying;
  private String fetchPlan;
  private boolean converted = false;
  private boolean convertToRecord = true;
  private final boolean orphanRemoval;

  public OObjectLazySet(
      final Object iSourceRecord,
      final Set<OIdentifiable> iRecordSource,
      final boolean orphanRemoval) {
    this.sourceRecord = iSourceRecord instanceof ProxyObject ? (ProxyObject) iSourceRecord : null;
    this.underlying = iRecordSource;
    this.orphanRemoval = orphanRemoval;
  }

  public OObjectLazySet(
      final Object iSourceRecord,
      final Set<OIdentifiable> iRecordSource,
      final Set<? extends TYPE> iSourceCollection,
      final boolean orphanRemoval) {
    this.sourceRecord = iSourceRecord instanceof ProxyObject ? (ProxyObject) iSourceRecord : null;
    this.underlying = iRecordSource;
    this.orphanRemoval = orphanRemoval;
    addAll(iSourceCollection);
  }

  public Iterator<TYPE> iterator() {
    return new OObjectLazyIterator<TYPE>(
        getDatabase(),
        sourceRecord,
        (!converted ? underlying.iterator() : super.iterator()),
        convertToRecord,
        orphanRemoval);
  }

  public int size() {
    return underlying.size();
  }

  public boolean isEmpty() {
    return super.isEmpty() && underlying.isEmpty();
  }

  public boolean contains(final Object o) {
    return super.contains(o) || underlying.contains(getDatabase().getRecordByUserObject(o, false));
  }

  public Object[] toArray() {
    return toArray(new Object[size()]);
  }

  public <T> T[] toArray(final T[] a) {
    convertAll();
    return super.toArray(a);
  }

  public boolean add(final TYPE e) {
    if (underlying != null && underlying.size() > 0 && !converted) {
      convertAllInternal();
    }
    if (converted && e instanceof ORID) converted = false;
    setDirty();
    boolean thisModified = super.add(e);
    if (thisModified) {
      OIdentifiable record = getDatabase().getRecordByUserObject(e, false);
      if (sourceRecord != null)
        ((OObjectProxyMethodHandler) sourceRecord.getHandler())
            .getOrphans()
            .remove(record.getIdentity());
      underlying.add(record);
      return true;
    }
    return false;
  }

  public boolean remove(final Object o) {
    setDirty();
    OIdentifiable record = getDatabase().getRecordByUserObject(o, false);
    if (orphanRemoval && record != null && sourceRecord != null)
      ((OObjectProxyMethodHandler) sourceRecord.getHandler())
          .getOrphans()
          .add(record.getIdentity());
    boolean thisModified = super.remove(o);
    boolean underlyingModified = underlying.remove(record);
    return thisModified || underlyingModified;
  }

  public boolean containsAll(final Collection<?> c) {
    convertAll();
    for (Object o : c) if (!contains(o)) return false;

    return true;
  }

  public boolean addAll(final Collection<? extends TYPE> c) {
    boolean modified = false;
    for (Object o : c) {
      modified |= add((TYPE) o);
    }
    return modified;
  }

  public boolean retainAll(final Collection<?> c) {
    setDirty();
    final OObjectDatabaseTx database = getDatabase();
    boolean modified = super.retainAll(c);
    Set<Object> toRetain = new HashSet<Object>();
    Set<Object> toRemove = new HashSet<Object>();
    for (Object o : c) {
      OIdentifiable record = database.getRecordByUserObject(o, false);
      toRetain.add(record);
    }
    for (OIdentifiable underlyingRec : underlying) {
      if (toRetain.contains(underlyingRec) && sourceRecord != null) {
        ((OObjectProxyMethodHandler) sourceRecord.getHandler())
            .getOrphans()
            .remove(underlyingRec.getIdentity());
      } else {
        if (sourceRecord != null)
          ((OObjectProxyMethodHandler) sourceRecord.getHandler())
              .getOrphans()
              .add(underlyingRec.getIdentity());
        toRemove.add(underlyingRec);
        modified = true;
      }
    }
    underlying.removeAll(toRemove);
    toRemove.clear();
    toRetain.clear();
    return modified;
  }

  public void clear() {
    setDirty();
    super.clear();
    if (orphanRemoval && sourceRecord != null)
      for (OIdentifiable value : underlying) {
        ((OObjectProxyMethodHandler) sourceRecord.getHandler())
            .getOrphans()
            .add(value.getIdentity());
      }
    underlying.clear();
  }

  public boolean removeAll(final Collection<?> c) {
    setDirty();
    final OObjectDatabaseTx database = getDatabase();
    boolean modified = super.removeAll(c);
    for (Object o : c) {
      OIdentifiable record = database.getRecordByUserObject(o, false);
      if (orphanRemoval && record != null && sourceRecord != null)
        ((OObjectProxyMethodHandler) sourceRecord.getHandler())
            .getOrphans()
            .add(record.getIdentity());
      if (!underlying.remove(database.getRecordByUserObject(o, false))) modified = true;
    }
    return modified;
  }

  public String getFetchPlan() {
    return fetchPlan;
  }

  public boolean isConverted() {
    return converted;
  }

  public OObjectLazySet<TYPE> setFetchPlan(String fetchPlan) {
    this.fetchPlan = fetchPlan;
    return this;
  }

  public boolean isConvertToRecord() {
    return convertToRecord;
  }

  public void setConvertToRecord(boolean convertToRecord) {
    this.convertToRecord = convertToRecord;
  }

  @Override
  public String toString() {
    return super.size() == underlying.size() ? super.toString() : underlying.toString();
  }

  public void setDirty() {
    if (sourceRecord != null) ((OObjectProxyMethodHandler) sourceRecord.getHandler()).setDirty();
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
    if (converted || !convertToRecord) return;

    final Set<Object> copy = new HashSet<Object>(underlying);
    super.clear();
    final OObjectDatabaseTx database = getDatabase();
    for (Object e : copy) {
      if (e != null) {
        if (e instanceof ORID)
          add(
              (TYPE)
                  database.getUserObjectByRecord(
                      ((ODatabaseDocument) getDatabase().getUnderlying()).load((ORID) e, fetchPlan),
                      fetchPlan));
        else if (e instanceof ODocument)
          add((TYPE) database.getUserObjectByRecord((ORecord) e, fetchPlan));
        else add((TYPE) e);
      }
    }

    converted = true;
  }

  protected void convertAllInternal() {
    if (converted || !convertToRecord) return;

    final Set<Object> copy = new HashSet<Object>(underlying);
    super.clear();
    final OObjectDatabaseTx database = getDatabase();
    for (Object e : copy) {
      if (e != null) {
        if (e instanceof ORID)
          super.add(
              (TYPE)
                  database.getUserObjectByRecord(
                      ((ODatabaseDocument) getDatabase().getUnderlying()).load((ORID) e, fetchPlan),
                      fetchPlan));
        else if (e instanceof ODocument)
          super.add((TYPE) database.getUserObjectByRecord((ORecord) e, fetchPlan));
        else super.add((TYPE) e);
      }
    }
    converted = true;
  }

  protected void convertAndDetachAll(
      boolean nonProxiedInstance,
      Map<Object, Object> alreadyDetached,
      Map<Object, Object> lazyObjects) {
    if (converted || !convertToRecord) return;

    final Set<Object> copy = new HashSet<Object>(underlying);
    super.clear();
    final OObjectDatabaseTx database = getDatabase();
    for (Object e : copy) {
      if (e != null) {
        if (e instanceof ORID) {
          e =
              database.getUserObjectByRecord(
                  ((ODatabaseDocument) getDatabase().getUnderlying()).load((ORID) e, fetchPlan),
                  fetchPlan);
          super.add(
              (TYPE)
                  ((OObjectDatabaseTx) getDatabase())
                      .detachAll(e, nonProxiedInstance, alreadyDetached, lazyObjects));
        } else if (e instanceof ODocument) {
          e = database.getUserObjectByRecord((ORecord) e, fetchPlan);
          super.add(
              (TYPE)
                  ((OObjectDatabaseTx) getDatabase())
                      .detachAll(e, nonProxiedInstance, alreadyDetached, lazyObjects));
        } else add((TYPE) e);
      }
    }

    converted = true;
  }

  protected OObjectDatabaseTx getDatabase() {
    return OLazyCollectionUtil.getDatabase();
  }
}
