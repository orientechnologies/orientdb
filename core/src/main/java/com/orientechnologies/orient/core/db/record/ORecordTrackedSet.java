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

import java.util.*;

import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import com.orientechnologies.orient.core.record.impl.OSimpleMultiValueChangeListener;

/**
 * Implementation of Set bound to a source ORecord object to keep track of changes. This avoid to call the makeDirty() by hand when
 * the set is changed.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class ORecordTrackedSet extends AbstractCollection<OIdentifiable>
    implements Set<OIdentifiable>, OTrackedMultiValue<OIdentifiable, OIdentifiable>, ORecordElement {
  protected final        ORecord                    sourceRecord;
  protected              Map<OIdentifiable, Object> map           = new HashMap<OIdentifiable, Object>();
  private                STATUS                     status        = STATUS.NOT_LOADED;
  protected static final Object                     ENTRY_REMOVAL = new Object();
  private                boolean                    dirty         = false;

  private List<OMultiValueChangeListener<OIdentifiable, OIdentifiable>> changeListeners;

  public ORecordTrackedSet(final ORecord iSourceRecord) {
    this.sourceRecord = iSourceRecord;
  }

  @Override
  public ORecordElement getOwner() {
    return sourceRecord;
  }

  public Iterator<OIdentifiable> iterator() {
    return new ORecordTrackedIterator(sourceRecord, map.keySet().iterator());
  }

  public boolean add(final OIdentifiable e) {
    if (map.containsKey(e))
      return false;

    map.put(e, ENTRY_REMOVAL);
    addEvent(e);
    return true;
  }

  public boolean addInternal(final OIdentifiable e) {
    if (map.containsKey(e))
      return false;

    map.put(e, ENTRY_REMOVAL);
    addOwnerToEmbeddedDoc(e);
    return true;
  }

  @Override
  public boolean contains(Object o) {
    return map.containsKey(o);
  }

  public boolean remove(Object o) {
    final Object old = map.remove(o);
    if (old != null) {
      removeEvent((OIdentifiable) old);
      return true;
    }
    return false;
  }

  public void clear() {
    setDirty();
    map.clear();
  }

  public boolean removeAll(final Collection<?> c) {
    boolean changed = false;
    for (Object item : c) {
      if (remove(item))
        changed = true;
    }

    if (changed)
      setDirty();

    return changed;
  }

  public boolean addAll(final Collection<? extends OIdentifiable> c) {
    if (c == null || c.size() == 0)
      return false;
    boolean convert = false;
    if (c instanceof OAutoConvertToRecord) {
      convert = ((OAutoConvertToRecord) c).isAutoConvertToRecord();
      ((OAutoConvertToRecord) c).setAutoConvertToRecord(false);
    }
    for (OIdentifiable o : c) {
      add(o);
    }

    if (c instanceof OAutoConvertToRecord) {
      ((OAutoConvertToRecord) c).setAutoConvertToRecord(convert);
    }
    return true;
  }

  public boolean retainAll(final Collection<?> c) {
    if (c == null || c.size() == 0)
      return false;

    if (super.retainAll(c)) {
      return true;
    }
    return false;
  }

  @Override
  public int size() {
    return map.size();
  }

  @SuppressWarnings("unchecked")
  public ORecordTrackedSet setDirty() {
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

  public void addChangeListener(final OMultiValueChangeListener<OIdentifiable, OIdentifiable> changeListener) {
    if (changeListeners == null)
      changeListeners = new LinkedList<OMultiValueChangeListener<OIdentifiable, OIdentifiable>>();
    changeListeners.add(changeListener);
  }

  public void removeRecordChangeListener(final OMultiValueChangeListener<OIdentifiable, OIdentifiable> changeListener) {
    if (changeListeners != null)
      changeListeners.remove(changeListener);
  }

  public Set<OIdentifiable> returnOriginalState(final List<OMultiValueChangeEvent<OIdentifiable, OIdentifiable>> events) {
    final Set<OIdentifiable> reverted = new HashSet<OIdentifiable>(this);

    final ListIterator<OMultiValueChangeEvent<OIdentifiable, OIdentifiable>> listIterator = events.listIterator(events.size());

    while (listIterator.hasPrevious()) {
      final OMultiValueChangeEvent<OIdentifiable, OIdentifiable> event = listIterator.previous();
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

  public void fireCollectionChangedEvent(final OMultiValueChangeEvent<OIdentifiable, OIdentifiable> event) {
    if (changeListeners != null) {
      for (final OMultiValueChangeListener<OIdentifiable, OIdentifiable> changeListener : changeListeners) {
        if (changeListener != null)
          changeListener.onAfterRecordChanged(event);
      }
    }
  }

  protected void addOwnerToEmbeddedDoc(OIdentifiable e) {
    if (e instanceof ODocument && !e.getIdentity().isValid()) {
      ODocumentInternal.addOwner((ODocument) e, this);
    }
    if (sourceRecord != null && e != null) {
      ORecordInternal.track(sourceRecord, e);
    }
  }

  protected void addEvent(OIdentifiable added) {
    addOwnerToEmbeddedDoc(added);

    if (changeListeners != null && !changeListeners.isEmpty()) {
      fireCollectionChangedEvent(
          new OMultiValueChangeEvent<OIdentifiable, OIdentifiable>(OMultiValueChangeEvent.OChangeType.ADD, added, added));
    } else {
      setDirty();
    }
  }

  private void updateEvent(OIdentifiable oldValue, OIdentifiable newValue) {
    if (oldValue instanceof ODocument)
      ODocumentInternal.removeOwner((ODocument) oldValue, this);

    addOwnerToEmbeddedDoc(newValue);

    if (changeListeners != null && !changeListeners.isEmpty()) {
      fireCollectionChangedEvent(
          new OMultiValueChangeEvent<OIdentifiable, OIdentifiable>(OMultiValueChangeEvent.OChangeType.UPDATE, oldValue, newValue,
              oldValue));
    } else {
      setDirty();
    }
  }

  protected void removeEvent(OIdentifiable removed) {
    if (removed instanceof ODocument) {
      ODocumentInternal.removeOwner((ODocument) removed, this);
    }
    if (changeListeners != null && !changeListeners.isEmpty()) {
      fireCollectionChangedEvent(
          new OMultiValueChangeEvent<OIdentifiable, OIdentifiable>(OMultiValueChangeEvent.OChangeType.REMOVE, removed, null,
              removed));
    } else {
      setDirty();
    }
  }

  @Override
  public Class<?> getGenericClass() {
    return OIdentifiable.class;
  }

  @Override
  public void replace(OMultiValueChangeEvent<Object, Object> event, Object newValue) {
    //not needed do nothing
  }

  private OSimpleMultiValueChangeListener<OIdentifiable, OIdentifiable> changeListener;

  public void enableTracking(ORecordElement parent) {
    if (changeListener == null) {
      final OSimpleMultiValueChangeListener<OIdentifiable, OIdentifiable> listener = new OSimpleMultiValueChangeListener<>(this);
      this.addChangeListener(listener);
      changeListener = listener;
      if (this instanceof ORecordLazyMultiValue) {
        OTrackedMultiValue.nestedEnabled(((ORecordLazyMultiValue) this).rawIterator(), this);
      } else {
        OTrackedMultiValue.nestedEnabled(this.iterator(), this);
      }
    }
  }

  public void disableTracking(ORecordElement document) {
    if (changeListener != null) {
      final OMultiValueChangeListener<OIdentifiable, OIdentifiable> changeListener = this.changeListener;
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
