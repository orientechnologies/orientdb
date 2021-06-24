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
package com.orientechnologies.orient.core.db.record;

import com.orientechnologies.orient.core.record.ORecordAbstract;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import com.orientechnologies.orient.core.record.impl.OSimpleMultiValueTracker;
import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

/**
 * Implementation of LinkedHashMap bound to a source ORecord object to keep track of changes. This
 * avoid to call the makeDirty() by hand when the map is changed.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
@SuppressWarnings("serial")
public class OTrackedMap<T> extends LinkedHashMap<Object, T>
    implements ORecordElement, OTrackedMultiValue<Object, T>, Serializable {
  protected final ORecordElement sourceRecord;
  protected Class<?> genericClass;
  private final boolean embeddedCollection;
  private boolean dirty = false;
  private boolean transactionDirty = false;

  private OSimpleMultiValueTracker<Object, T> tracker = new OSimpleMultiValueTracker<>(this);

  public OTrackedMap(
      final ORecordElement iRecord, final Map<Object, T> iOrigin, final Class<?> cls) {
    this(iRecord);
    genericClass = cls;
    if (iOrigin != null && !iOrigin.isEmpty()) putAll(iOrigin);
  }

  public OTrackedMap(final ORecordElement iSourceRecord) {
    this.sourceRecord = iSourceRecord;
    embeddedCollection = this.getClass().equals(OTrackedMap.class);
  }

  @Override
  public ORecordElement getOwner() {
    return sourceRecord;
  }

  @Override
  public boolean addInternal(T e) {
    throw new UnsupportedOperationException();
  }

  public T putInternal(final Object key, final T value) {
    if (key == null) throw new IllegalArgumentException("null key not supported by embedded map");
    boolean containsKey = containsKey(key);

    T oldValue = super.put(key, value);

    if (containsKey && oldValue == value) return oldValue;

    if (oldValue instanceof ODocument) ODocumentInternal.removeOwner((ODocument) oldValue, this);

    addOwnerToEmbeddedDoc(value);

    return oldValue;
  }

  @Override
  public T put(final Object key, final T value) {
    if (key == null) throw new IllegalArgumentException("null key not supported by embedded map");
    boolean containsKey = containsKey(key);

    T oldValue = super.put(key, value);

    if (containsKey && oldValue == value) return oldValue;
    if (containsKey) {
      updateEvent(key, oldValue, value);
    } else {
      addEvent(key, value);
    }
    return oldValue;
  }

  private void addOwnerToEmbeddedDoc(T e) {
    if (embeddedCollection && e instanceof ODocument && !((ODocument) e).getIdentity().isValid())
      ODocumentInternal.addOwner((ODocument) e, this);
    if (e instanceof ODocument) ORecordInternal.track(sourceRecord, (ODocument) e);
  }

  @Override
  public T remove(final Object iKey) {
    boolean containsKey = containsKey(iKey);
    if (containsKey) {
      final T oldValue = super.remove(iKey);
      removeEvent(iKey, oldValue);
      return oldValue;
    } else {
      return null;
    }
  }

  @Override
  public void clear() {
    for (Map.Entry<Object, T> entry : super.entrySet()) {
      removeEvent(entry.getKey(), entry.getValue());
    }
    super.clear();
  }

  @Override
  public void putAll(Map<?, ? extends T> m) {
    boolean convert = false;
    if (m instanceof OAutoConvertToRecord) {
      convert = ((OAutoConvertToRecord) m).isAutoConvertToRecord();
      ((OAutoConvertToRecord) m).setAutoConvertToRecord(false);
    }
    for (Map.Entry<?, ? extends T> entry : m.entrySet()) {
      put(entry.getKey(), entry.getValue());
    }

    if (m instanceof OAutoConvertToRecord) {
      ((OAutoConvertToRecord) m).setAutoConvertToRecord(convert);
    }
  }

  @SuppressWarnings({"unchecked"})
  public OTrackedMap<T> setDirty() {
    if (sourceRecord != null) {
      if (!(sourceRecord instanceof ORecordAbstract)
          || !((ORecordAbstract) sourceRecord).isDirty()) {
        sourceRecord.setDirty();
      }
    }
    this.dirty = true;
    this.transactionDirty = true;
    return this;
  }

  @Override
  public void setDirtyNoChanged() {
    if (sourceRecord != null) sourceRecord.setDirtyNoChanged();
  }

  public Map<Object, T> returnOriginalState(
      final List<OMultiValueChangeEvent<Object, T>> multiValueChangeEvents) {
    final Map<Object, T> reverted = new HashMap<Object, T>(this);

    final ListIterator<OMultiValueChangeEvent<Object, T>> listIterator =
        multiValueChangeEvents.listIterator(multiValueChangeEvents.size());

    while (listIterator.hasPrevious()) {
      final OMultiValueChangeEvent<Object, T> event = listIterator.previous();
      switch (event.getChangeType()) {
        case ADD:
          reverted.remove(event.getKey());
          break;
        case REMOVE:
          reverted.put(event.getKey(), event.getOldValue());
          break;
        case UPDATE:
          reverted.put(event.getKey(), event.getOldValue());
          break;
        default:
          throw new IllegalArgumentException("Invalid change type : " + event.getChangeType());
      }
    }

    return reverted;
  }

  public Class<?> getGenericClass() {
    return genericClass;
  }

  private Object writeReplace() {
    return new LinkedHashMap<Object, T>(this);
  }

  @Override
  public void replace(OMultiValueChangeEvent<Object, Object> event, Object newValue) {
    super.put(event.getKey(), (T) newValue);
  }

  private void addEvent(Object key, T value) {
    addOwnerToEmbeddedDoc(value);

    if (tracker.isEnabled()) {
      tracker.add(key, value);
    } else {
      setDirty();
    }
  }

  private void updateEvent(Object key, T oldValue, T newValue) {
    if (oldValue instanceof ODocument) ODocumentInternal.removeOwner((ODocument) oldValue, this);

    addOwnerToEmbeddedDoc(newValue);

    if (tracker.isEnabled()) {
      tracker.updated(key, newValue, oldValue);
    } else {
      setDirty();
    }
  }

  private void removeEvent(Object iKey, T removed) {
    if (removed instanceof ODocument) {
      ODocumentInternal.removeOwner((ODocument) removed, this);
    }
    if (tracker.isEnabled()) {
      tracker.remove(iKey, removed);
    } else {
      setDirty();
    }
  }

  public void enableTracking(ORecordElement parent) {
    if (!tracker.isEnabled()) {
      tracker.enable();
      if (this instanceof ORecordLazyMultiValue) {
        OTrackedMultiValue.nestedEnabled(((ORecordLazyMultiValue) this).rawIterator(), this);
      } else {
        OTrackedMultiValue.nestedEnabled(this.values().iterator(), this);
      }
    }
  }

  public void disableTracking(ORecordElement document) {
    if (tracker.isEnabled()) {
      this.tracker.disable();
      if (this instanceof ORecordLazyMultiValue) {
        OTrackedMultiValue.nestedDisable(((ORecordLazyMultiValue) this).rawIterator(), this);
      } else {
        OTrackedMultiValue.nestedDisable(this.values().iterator(), this);
      }
    }
    this.dirty = false;
  }

  @Override
  public void transactionClear() {
    tracker.transactionClear();
    if (this instanceof ORecordLazyMultiValue) {
      OTrackedMultiValue.nestedTransactionClear(((ORecordLazyMultiValue) this).rawIterator());
    } else {
      OTrackedMultiValue.nestedTransactionClear(this.values().iterator());
    }
    this.transactionDirty = false;
  }

  @Override
  public boolean isModified() {
    return dirty;
  }

  @Override
  public boolean isTransactionModified() {
    return transactionDirty;
  }

  @Override
  public OMultiValueChangeTimeLine<Object, Object> getTimeLine() {
    return tracker.getTimeLine();
  }

  public OMultiValueChangeTimeLine<Object, T> getTransactionTimeLine() {
    return tracker.getTransactionTimeLine();
  }
}
