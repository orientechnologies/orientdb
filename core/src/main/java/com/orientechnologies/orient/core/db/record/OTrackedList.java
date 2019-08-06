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

import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import com.orientechnologies.orient.core.record.impl.OSimpleMultiValueChangeListener;

import java.io.Serializable;
import java.util.*;

/**
 * Implementation of ArrayList bound to a source ORecord object to keep track of changes for literal types. This avoid to call the
 * makeDirty() by hand when the list is changed.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
@SuppressWarnings({ "serial" })
public class OTrackedList<T> extends ArrayList<T> implements ORecordElement, OTrackedMultiValue<Integer, T>, Serializable {
  protected final ORecord                                     sourceRecord;
  protected       STATUS                                      status          = STATUS.NOT_LOADED;
  protected       List<OMultiValueChangeListener<Integer, T>> changeListeners = null;
  protected       Class<?>                                    genericClass;
  private final   boolean                                     embeddedCollection;
  private         boolean                                     dirty           = false;

  public OTrackedList(final ORecord iRecord, final Collection<? extends T> iOrigin, final Class<?> iGenericClass) {
    this(iRecord);
    genericClass = iGenericClass;
    if (iOrigin != null && !iOrigin.isEmpty())
      addAll(iOrigin);
  }

  public OTrackedList(final ORecord iSourceRecord) {
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
      addOwnerToEmbeddedDoc(element);

      fireCollectionChangedEvent(
          new OMultiValueChangeEvent<Integer, T>(OMultiValueChangeEvent.OChangeType.ADD, super.size() - 1, element));
    }

    addNested(element);
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

    addOwnerToEmbeddedDoc(element);
    addNested(element);
    fireCollectionChangedEvent(new OMultiValueChangeEvent<Integer, T>(OMultiValueChangeEvent.OChangeType.ADD, index, element));
  }

  public T setInternal(int index, T element) {
    final T oldValue = super.set(index, element);

    if (oldValue != null && !oldValue.equals(element)) {
      if (oldValue instanceof ODocument)
        ODocumentInternal.removeOwner((ODocument) oldValue, this);

      addOwnerToEmbeddedDoc(element);
    }
    return oldValue;
  }

  @Override
  public T set(int index, T element) {
    final T oldValue = super.set(index, element);

    if (oldValue != null && !oldValue.equals(element)) {
      if (oldValue instanceof ODocument)
        ODocumentInternal.removeOwner((ODocument) oldValue, this);

      addOwnerToEmbeddedDoc(element);

      fireCollectionChangedEvent(
          new OMultiValueChangeEvent<Integer, T>(OMultiValueChangeEvent.OChangeType.UPDATE, index, element, oldValue));
    }

    addNested(element);

    return oldValue;
  }

  private void addNested(T element) {
    if (element instanceof OTrackedMultiValue) {
      ((OTrackedMultiValue) element)
          .addChangeListener(new ONestedValueChangeListener((ODocument) sourceRecord, this, (OTrackedMultiValue) element));
    }
  }

  private void addOwnerToEmbeddedDoc(T e) {
    if (embeddedCollection && e instanceof ODocument && !((ODocument) e).getIdentity().isValid()) {
      ODocumentInternal.addOwner((ODocument) e, this);
    }
    if (e instanceof ODocument)
      ORecordInternal.track(sourceRecord, (ODocument) e);
  }

  @Override
  public T remove(int index) {
    final T oldValue = super.remove(index);
    if (oldValue instanceof ODocument) {
      ODocumentInternal.removeOwner((ODocument) oldValue, this);
    }

    fireCollectionChangedEvent(
        new OMultiValueChangeEvent<Integer, T>(OMultiValueChangeEvent.OChangeType.REMOVE, index, null, oldValue));
    removeNested(oldValue);

    return oldValue;
  }

  private void removeNested(Object element) {
    if (element instanceof OTrackedMultiValue) {
//      ((OTrackedMultiValue) element).removeRecordChangeListener(null);
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
    for (Object o : c)
      removed = removed | remove(o);

    return removed;
  }

  @Override
  public void clear() {
    if (changeListeners != null && changeListeners.isEmpty()) {
      for (final T item : this) {
        if (item instanceof ODocument)
          ODocumentInternal.removeOwner((ODocument) item, this);
      }
      super.clear();
      setDirty();
    } else {
      final List<T> origValues = new ArrayList<T>(this);
      super.clear();
      if (origValues != null)
        for (int i = origValues.size() - 1; i >= 0; i--) {
          final T origValue = origValues.get(i);

          if (origValue instanceof ODocument)
            ODocumentInternal.removeOwner((ODocument) origValue, this);

          fireCollectionChangedEvent(
              new OMultiValueChangeEvent<Integer, T>(OMultiValueChangeEvent.OChangeType.REMOVE, i, null, origValue));
          removeNested(origValue);
        }
    }

  }

  public void reset() {
    super.clear();
  }

  @SuppressWarnings("unchecked")
  public <RET> RET setDirty() {
    if (sourceRecord != null && !(sourceRecord.isDirty() && ORecordInternal.isContentChanged(sourceRecord))) {
      sourceRecord.setDirty();
    }
    this.dirty = true;
    return (RET) this;
  }

  @Override
  public void setDirtyNoChanged() {
    if (sourceRecord != null)
      sourceRecord.setDirtyNoChanged();
  }

  public void addChangeListener(final OMultiValueChangeListener<Integer, T> changeListener) {
    if (changeListeners == null) {
      changeListeners = new LinkedList<OMultiValueChangeListener<Integer, T>>();
    }
    changeListeners.add(changeListener);
  }

  public void removeRecordChangeListener(final OMultiValueChangeListener<Integer, T> changeListener) {
    if (changeListeners != null) {
      changeListeners.remove(changeListener);
    }
  }

  public List<T> returnOriginalState(final List<OMultiValueChangeEvent<Integer, T>> multiValueChangeEvents) {
    final List<T> reverted = new ArrayList<T>(this);

    final ListIterator<OMultiValueChangeEvent<Integer, T>> listIterator = multiValueChangeEvents
        .listIterator(multiValueChangeEvents.size());

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

  public void fireCollectionChangedEvent(final OMultiValueChangeEvent<Integer, T> event) {
    if (changeListeners != null) {
      for (final OMultiValueChangeListener<Integer, T> changeListener : changeListeners) {
        if (changeListener != null)
          changeListener.onAfterRecordChanged(event);
      }
    }
  }

  public STATUS getInternalStatus() {
    return status;
  }

  public void setInternalStatus(final STATUS iStatus) {
    status = iStatus;
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
    addNested((T) newValue);
  }

  private OSimpleMultiValueChangeListener<Integer, T> changeListener;

  public void enableTracking(ORecordElement parent) {
    if (changeListener == null) {
      final OSimpleMultiValueChangeListener<Integer, T> listener = new OSimpleMultiValueChangeListener<>(this);
      this.addChangeListener(listener);
      changeListener = listener;
      if (this instanceof ORecordLazyMultiValue) {
        OTrackedMultiValue.nestedEnabled(((ORecordLazyMultiValue) this).rawIterator(), this);
      } else {
        OTrackedMultiValue.nestedEnabled(this.iterator(), this);
      }
    }
  }

  public void disableTracking(ORecordElement parent) {
    if (changeListener != null) {
      final OMultiValueChangeListener<Integer, T> changeListener = this.changeListener;
      this.changeListener.timeLine = null;
      this.changeListener = null;
      this.dirty = false;
      removeRecordChangeListener(changeListener);
      if (this instanceof ORecordLazyMultiValue) {
        OTrackedMultiValue.nestedDisable(((ORecordLazyMultiValue) this).rawIterator(), this);
      } else {
        OTrackedMultiValue.nestedDisable(this.iterator(), this);
      }
    }
  }

  @Override
  public boolean isModified() {
    return dirty;
  }

  @Override
  public OMultiValueChangeTimeLine<Object, Object> getTimeLine() {
    if (changeListener == null) {
      return null;
    } else {
      return changeListener.timeLine;
    }
  }

}
