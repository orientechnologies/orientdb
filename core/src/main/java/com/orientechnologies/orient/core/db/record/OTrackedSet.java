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
 * Implementation of Set bound to a source ORecord object to keep track of changes. This avoid to call the makeDirty() by hand when
 * the set is changed.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
@SuppressWarnings("serial")
public class OTrackedSet<T> extends LinkedHashSet<T> implements ORecordElement, OTrackedMultiValue<T, T>, Serializable {
  protected final ORecord  sourceRecord;
  private final   boolean  embeddedCollection;
  protected       Class<?> genericClass;
  private         boolean  dirty = false;

  private OSimpleMultiValueChangeListener<T, T> changeListener = new OSimpleMultiValueChangeListener<>(this);

  public OTrackedSet(final ORecord iRecord, final Collection<? extends T> iOrigin, final Class<?> cls) {
    this(iRecord);
    genericClass = cls;
    if (iOrigin != null && !iOrigin.isEmpty()) {
      addAll(iOrigin);
    }
  }

  public OTrackedSet(final ORecord iSourceRecord) {
    this.sourceRecord = iSourceRecord;
    embeddedCollection = this.getClass().equals(OTrackedSet.class);
  }

  @Override
  public ORecordElement getOwner() {
    return sourceRecord;
  }

  @Override
  public Iterator<T> iterator() {
    return new Iterator<T>() {
      private T current;
      private final Iterator<T> underlying = OTrackedSet.super.iterator();

      @Override
      public boolean hasNext() {
        return underlying.hasNext();
      }

      @Override
      public T next() {
        current = underlying.next();
        return current;
      }

      @Override
      public void remove() {
        underlying.remove();
        removeEvent(current);
      }
    };
  }

  @Override
  public boolean addAll(Collection<? extends T> c) {
    boolean convert = false;
    if (c instanceof OAutoConvertToRecord) {
      convert = ((OAutoConvertToRecord) c).isAutoConvertToRecord();
      ((OAutoConvertToRecord) c).setAutoConvertToRecord(false);
    }
    boolean modified = false;
    for (T o : c) {
      if (add(o)) {
        modified = true;
      }
    }

    if (c instanceof OAutoConvertToRecord) {
      ((OAutoConvertToRecord) c).setAutoConvertToRecord(convert);
    }
    return modified;
  }

  public boolean add(final T e) {
    if (super.add(e)) {
      addEvent(e);
      return true;
    }
    return false;
  }

  public boolean addInternal(final T e) {
    if (super.add(e)) {
      addOwnerToEmbeddedDoc(e);
      return true;
    }
    return false;
  }

  @SuppressWarnings("unchecked")
  @Override
  public boolean remove(final Object o) {
    if (super.remove(o)) {
      removeEvent((T) o);
      return true;
    }
    return false;
  }

  @Override
  public void clear() {
    for (final T item : this) {
      removeEvent(item);
    }
    super.clear();
  }

  private void addEvent(T added) {
    addOwnerToEmbeddedDoc(added);

    if (changeListener.isEnabled()) {
      changeListener.add(added, added);
    } else {
      setDirty();
    }
  }

  private void updateEvent(T oldValue, T newValue) {
    if (oldValue instanceof ODocument) {
      ODocumentInternal.removeOwner((ODocument) oldValue, this);
    }

    addOwnerToEmbeddedDoc(newValue);

    if (changeListener.isEnabled()) {
      changeListener.updated(oldValue, newValue, oldValue);
    } else {
      setDirty();
    }
  }

  private void removeEvent(T removed) {
    if (removed instanceof ODocument) {
      ODocumentInternal.removeOwner((ODocument) removed, this);
    }
    if (changeListener.isEnabled()) {
      changeListener.remove(removed, removed);
    } else {
      setDirty();
    }
  }

  @SuppressWarnings("unchecked")
  public OTrackedSet<T> setDirty() {
    if (sourceRecord != null) {
      sourceRecord.setDirty();
    }
    this.dirty = true;
    return this;
  }

  @Override
  public void setDirtyNoChanged() {
    if (sourceRecord != null) {
      sourceRecord.setDirtyNoChanged();
    }
  }

  public Set<T> returnOriginalState(final List<OMultiValueChangeEvent<T, T>> multiValueChangeEvents) {
    final Set<T> reverted = new HashSet<T>(this);

    final ListIterator<OMultiValueChangeEvent<T, T>> listIterator = multiValueChangeEvents
        .listIterator(multiValueChangeEvents.size());

    while (listIterator.hasPrevious()) {
      final OMultiValueChangeEvent<T, T> event = listIterator.previous();
      switch (event.getChangeType()) {
      case ADD:
        reverted.remove(event.getKey());
        break;
      case REMOVE:
        reverted.add(event.getOldValue());
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

  private void addOwnerToEmbeddedDoc(T e) {
    if (embeddedCollection && e instanceof ODocument && !((ODocument) e).getIdentity().isValid()) {
      ODocumentInternal.addOwner((ODocument) e, this);
      ORecordInternal.track(sourceRecord, (ODocument) e);
    }
  }

  private Object writeReplace() {
    return new HashSet<T>(this);
  }

  @Override
  public void replace(OMultiValueChangeEvent<Object, Object> event, Object newValue) {
    super.remove(event.getKey());
    super.add((T) newValue);
  }

  public void enableTracking(ORecordElement parent) {
    if (!changeListener.isEnabled()) {
      this.changeListener.enable();
      if (this instanceof ORecordLazyMultiValue) {
        OTrackedMultiValue.nestedEnabled(((ORecordLazyMultiValue) this).rawIterator(), this);
      } else {
        OTrackedMultiValue.nestedEnabled(this.iterator(), this);
      }
    }
  }

  public void disableTracking(ORecordElement document) {
    if (changeListener.isEnabled()) {
      this.changeListener.disable();
      if (this instanceof ORecordLazyMultiValue) {
        OTrackedMultiValue.nestedDisable(((ORecordLazyMultiValue) this).rawIterator(), this);
      } else {
        OTrackedMultiValue.nestedDisable(this.iterator(), this);
      }
    }
    this.dirty = false;
  }

  @Override
  public boolean isModified() {
    return dirty;
  }

  @Override
  public OMultiValueChangeTimeLine<Object, Object> getTimeLine() {
    return changeListener.timeLine;
  }
}
