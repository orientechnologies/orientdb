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

import java.io.Serializable;
import java.util.*;

import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import com.orientechnologies.orient.core.record.impl.OSimpleMultiValueChangeListener;

/**
 * Implementation of LinkedHashMap bound to a source ORecord object to keep track of changes. This avoid to call the makeDirty() by
 * hand when the map is changed.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
@SuppressWarnings("serial")
public class OTrackedMap<T> extends LinkedHashMap<Object, T>
    implements ORecordElement, OTrackedMultiValue<Object, T>, Serializable {
  protected final ORecord                                    sourceRecord;
  protected       STATUS                                     status          = STATUS.NOT_LOADED;
  private         List<OMultiValueChangeListener<Object, T>> changeListeners = null;
  protected       Class<?>                                   genericClass;
  private final   boolean                                    embeddedCollection;
  private         boolean                                    dirty           = false;

  public OTrackedMap(final ORecord iRecord, final Map<Object, T> iOrigin, final Class<?> cls) {
    this(iRecord);
    genericClass = cls;
    if (iOrigin != null && !iOrigin.isEmpty())
      putAll(iOrigin);
  }

  public OTrackedMap(final ORecord iSourceRecord) {
    this.sourceRecord = iSourceRecord;
    embeddedCollection = this.getClass().equals(OTrackedMap.class);
  }

  @Override
  public ORecordElement getOwner() {
    return sourceRecord;
  }

  public T putInternal(final Object key, final T value) {
    if (key == null)
      throw new IllegalArgumentException("null key not supported by embedded map");
    boolean containsKey = containsKey(key);

    T oldValue = super.put(key, value);

    if (containsKey && oldValue == value)
      return oldValue;

    if (oldValue instanceof ODocument)
      ODocumentInternal.removeOwner((ODocument) oldValue, this);

    addOwnerToEmbeddedDoc(value);

    return oldValue;
  }

  @Override
  public T put(final Object key, final T value) {
    if (key == null)
      throw new IllegalArgumentException("null key not supported by embedded map");
    boolean containsKey = containsKey(key);

    T oldValue = super.put(key, value);

    if (containsKey && oldValue == value)
      return oldValue;

    if (oldValue instanceof ODocument)
      ODocumentInternal.removeOwner((ODocument) oldValue, this);

    addOwnerToEmbeddedDoc(value);

    if (containsKey)
      fireCollectionChangedEvent(
          new OMultiValueChangeEvent<Object, T>(OMultiValueChangeEvent.OChangeType.UPDATE, key, value, oldValue));
    else
      fireCollectionChangedEvent(new OMultiValueChangeEvent<Object, T>(OMultiValueChangeEvent.OChangeType.ADD, key, value));
    return oldValue;
  }

  private void addOwnerToEmbeddedDoc(T e) {
    if (embeddedCollection && e instanceof ODocument && !((ODocument) e).getIdentity().isValid())
      ODocumentInternal.addOwner((ODocument) e, this);
    if (e instanceof ODocument)
      ORecordInternal.track(sourceRecord, (ODocument) e);
  }

  @Override
  public T remove(final Object iKey) {
    boolean containsKey = containsKey(iKey);
    final T oldValue = super.remove(iKey);

    if (oldValue instanceof ODocument)
      ODocumentInternal.removeOwner((ODocument) oldValue, this);

    if (containsKey) {
      fireCollectionChangedEvent(
          new OMultiValueChangeEvent<Object, T>(OMultiValueChangeEvent.OChangeType.REMOVE, iKey, null, oldValue));
      removeNested(oldValue);
    }

    return oldValue;
  }

  @Override
  public void clear() {
    if (changeListeners == null || changeListeners.isEmpty()) {
      for (T value : super.values()) {
        if (value instanceof ODocument) {
          ODocumentInternal.removeOwner((ODocument) value, this);
        }
      }
      super.clear();
      setDirty();
    } else {
      final Map<Object, T> origValues = new HashMap<Object, T>(this);
      super.clear();
      for (Map.Entry<Object, T> entry : origValues.entrySet()) {
        if (entry.getValue() instanceof ODocument) {
          ODocumentInternal.removeOwner((ODocument) entry.getValue(), this);
        }
        fireCollectionChangedEvent(
            new OMultiValueChangeEvent<Object, T>(OMultiValueChangeEvent.OChangeType.REMOVE, entry.getKey(), null,
                entry.getValue()));
        removeNested(entry.getValue());
      }
    }
  }

  private void removeNested(Object element) {
    if (element instanceof OTrackedMultiValue) {
      //      ((OTrackedMultiValue) element).removeRecordChangeListener(null);
    }
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

  @SuppressWarnings({ "unchecked" })
  public OTrackedMap<T> setDirty() {
    if (sourceRecord != null) {
      sourceRecord.setDirty();
    }
    this.dirty = true;
    return this;
  }

  @Override
  public void setDirtyNoChanged() {
    if (sourceRecord != null)
      sourceRecord.setDirtyNoChanged();
  }

  public STATUS getInternalStatus() {
    return status;
  }

  public void setInternalStatus(final STATUS iStatus) {
    status = iStatus;
  }

  public void addChangeListener(OMultiValueChangeListener<Object, T> changeListener) {
    if (changeListeners == null)
      changeListeners = new LinkedList<OMultiValueChangeListener<Object, T>>();
    changeListeners.add(changeListener);
  }

  public void removeRecordChangeListener(OMultiValueChangeListener<Object, T> changeListener) {
    if (changeListeners != null)
      changeListeners.remove(changeListener);
  }

  public Map<Object, T> returnOriginalState(final List<OMultiValueChangeEvent<Object, T>> multiValueChangeEvents) {
    final Map<Object, T> reverted = new HashMap<Object, T>(this);

    final ListIterator<OMultiValueChangeEvent<Object, T>> listIterator = multiValueChangeEvents
        .listIterator(multiValueChangeEvents.size());

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

  public void fireCollectionChangedEvent(final OMultiValueChangeEvent<Object, T> event) {
    if (changeListeners != null) {
      for (final OMultiValueChangeListener<Object, T> changeListener : changeListeners) {
        if (changeListener != null)
          changeListener.onAfterRecordChanged(event);
      }
    }
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

  private OSimpleMultiValueChangeListener<Object, T> changeListener;

  public void enableTracking(ORecordElement parent) {
    if (changeListener == null) {
      final OSimpleMultiValueChangeListener<Object, T> listener = new OSimpleMultiValueChangeListener<>(this);
      this.addChangeListener(listener);
      changeListener = listener;
      if (this instanceof ORecordLazyMultiValue) {
        OTrackedMultiValue.nestedEnabled(((ORecordLazyMultiValue) this).rawIterator(), this);
      } else {
        OTrackedMultiValue.nestedEnabled(this.values().iterator(), this);
      }
    }
  }

  public void disableTracking(ORecordElement document) {
    if (changeListener != null) {
      final OMultiValueChangeListener<Object, T> changeListener = this.changeListener;
      this.changeListener.timeLine = null;
      this.changeListener = null;
      this.dirty = false;
      removeRecordChangeListener(changeListener);
      if (this instanceof ORecordLazyMultiValue) {
        OTrackedMultiValue.nestedDisable(((ORecordLazyMultiValue) this).rawIterator(), this);
      } else {
        OTrackedMultiValue.nestedDisable(this.values().iterator(), this);
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
