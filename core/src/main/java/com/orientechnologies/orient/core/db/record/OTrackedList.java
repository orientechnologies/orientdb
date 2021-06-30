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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.ListIterator;

/**
 * Implementation of ArrayList bound to a source ORecord object to keep track of changes for literal
 * types. This avoid to call the makeDirty() by hand when the list is changed.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
@SuppressWarnings({"serial"})
public class OTrackedList<T> extends ArrayList<T>
    implements ORecordElement, OTrackedMultiValue<Integer, T>, Serializable {
  protected final ORecordElement sourceRecord;
  protected Class<?> genericClass;
  private final boolean embeddedCollection;
  private boolean dirty = false;
  private boolean transactionDirty = false;
  private OSimpleMultiValueTracker<Integer, T> tracker = new OSimpleMultiValueTracker<>(this);

  public OTrackedList(
      final ORecordElement iRecord,
      final Collection<? extends T> iOrigin,
      final Class<?> iGenericClass) {
    this(iRecord);
    genericClass = iGenericClass;
    if (iOrigin != null && !iOrigin.isEmpty()) addAll(iOrigin);
  }

  public OTrackedList(final ORecordElement iSourceRecord) {
    this.sourceRecord = iSourceRecord;
    embeddedCollection = this.getClass().equals(OTrackedList.class);
  }

  @Override
  public ORecordElement getOwner() {
    return sourceRecord;
  }

  @Override
  public boolean add(T element) {
    final boolean result = super.add(element);

    if (result) {
      addEvent(super.size() - 1, element);
    }

    return result;
  }

  public boolean addInternal(T element) {
    final boolean result = super.add(element);

    if (result) {
      addOwnerToEmbeddedDoc(element);
    }

    return result;
  }

  @Override
  public boolean addAll(final Collection<? extends T> c) {
    boolean convert = false;
    if (c instanceof OAutoConvertToRecord) {
      convert = ((OAutoConvertToRecord) c).isAutoConvertToRecord();
      ((OAutoConvertToRecord) c).setAutoConvertToRecord(false);
    }
    for (T o : c) {
      add(o);
    }

    if (c instanceof OAutoConvertToRecord) {
      ((OAutoConvertToRecord) c).setAutoConvertToRecord(convert);
    }

    return true;
  }

  @Override
  public void add(int index, T element) {
    super.add(index, element);
    addEvent(index, element);
  }

  public T setInternal(int index, T element) {
    final T oldValue = super.set(index, element);

    if (oldValue != null && !oldValue.equals(element)) {
      if (oldValue instanceof ODocument) ODocumentInternal.removeOwner((ODocument) oldValue, this);

      addOwnerToEmbeddedDoc(element);
    }
    return oldValue;
  }

  @Override
  public T set(int index, T element) {
    final T oldValue = super.set(index, element);

    if (oldValue != null && !oldValue.equals(element)) {
      updateEvent(index, oldValue, element);
    }

    return oldValue;
  }

  private void addOwnerToEmbeddedDoc(T e) {
    if (embeddedCollection && e instanceof ODocument && !((ODocument) e).getIdentity().isValid()) {
      ODocumentInternal.addOwner((ODocument) e, this);
    }
    if (e instanceof ODocument) ORecordInternal.track(sourceRecord, (ODocument) e);
  }

  @Override
  public T remove(int index) {
    final T oldValue = super.remove(index);
    removeEvent(index, oldValue);
    return oldValue;
  }

  private void addEvent(int index, T added) {
    addOwnerToEmbeddedDoc(added);

    if (tracker.isEnabled()) {
      tracker.add(index, added);
    } else {
      setDirty();
    }
  }

  private void updateEvent(int index, T oldValue, T newValue) {
    if (oldValue instanceof ODocument) ODocumentInternal.removeOwner((ODocument) oldValue, this);

    addOwnerToEmbeddedDoc(newValue);

    if (tracker.isEnabled()) {
      tracker.updated(index, newValue, oldValue);
    } else {
      setDirty();
    }
  }

  private void removeEvent(int index, T removed) {
    if (removed instanceof ODocument) {
      ODocumentInternal.removeOwner((ODocument) removed, this);
    }
    if (tracker.isEnabled()) {
      tracker.remove(index, removed);
    } else {
      setDirty();
    }
  }

  @Override
  public boolean remove(Object o) {
    final int index = indexOf(o);
    if (index >= 0) {
      remove(index);
      return true;
    }
    return false;
  }

  @Override
  public boolean removeAll(Collection<?> c) {
    boolean removed = false;
    for (Object o : c) removed = removed | remove(o);

    return removed;
  }

  @Override
  public void clear() {
    for (int i = this.size() - 1; i >= 0; i--) {
      final T origValue = this.get(i);
      removeEvent(i, origValue);
    }
    super.clear();
  }

  public void reset() {
    super.clear();
  }

  @SuppressWarnings("unchecked")
  public <RET> RET setDirty() {
    if (sourceRecord != null) {
      if (!(sourceRecord instanceof ORecordAbstract)
          || !((ORecordAbstract) sourceRecord).isDirty()) {
        sourceRecord.setDirty();
      }
    }
    this.dirty = true;
    this.transactionDirty = true;
    return (RET) this;
  }

  @Override
  public void setDirtyNoChanged() {
    if (sourceRecord != null) sourceRecord.setDirtyNoChanged();
  }

  public List<T> returnOriginalState(
      final List<OMultiValueChangeEvent<Integer, T>> multiValueChangeEvents) {
    final List<T> reverted = new ArrayList<T>(this);

    final ListIterator<OMultiValueChangeEvent<Integer, T>> listIterator =
        multiValueChangeEvents.listIterator(multiValueChangeEvents.size());

    while (listIterator.hasPrevious()) {
      final OMultiValueChangeEvent<Integer, T> event = listIterator.previous();
      switch (event.getChangeType()) {
        case ADD:
          reverted.remove(event.getKey().intValue());
          break;
        case REMOVE:
          reverted.add(event.getKey(), event.getOldValue());
          break;
        case UPDATE:
          reverted.set(event.getKey(), event.getOldValue());
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
    return new ArrayList<T>(this);
  }

  @Override
  public void replace(OMultiValueChangeEvent<Object, Object> event, Object newValue) {
    super.set((Integer) event.getKey(), (T) newValue);
  }

  public void enableTracking(ORecordElement parent) {
    if (!tracker.isEnabled()) {
      tracker.enable();
      if (this instanceof ORecordLazyMultiValue) {
        OTrackedMultiValue.nestedEnabled(((ORecordLazyMultiValue) this).rawIterator(), this);
      } else {
        OTrackedMultiValue.nestedEnabled(this.iterator(), this);
      }
    }
  }

  public void disableTracking(ORecordElement parent) {
    if (tracker.isEnabled()) {
      tracker.disable();
      if (this instanceof ORecordLazyMultiValue) {
        OTrackedMultiValue.nestedDisable(((ORecordLazyMultiValue) this).rawIterator(), this);
      } else {
        OTrackedMultiValue.nestedDisable(this.iterator(), this);
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
      OTrackedMultiValue.nestedTransactionClear(this.iterator());
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

  public OMultiValueChangeTimeLine<Integer, T> getTransactionTimeLine() {
    return tracker.getTransactionTimeLine();
  }
}
