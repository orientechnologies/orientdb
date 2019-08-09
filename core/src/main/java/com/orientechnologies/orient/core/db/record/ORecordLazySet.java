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

import com.orientechnologies.common.collection.OLazyIterator;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.OIdentityChangeListener;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import com.orientechnologies.orient.core.record.impl.OSimpleMultiValueChangeListener;

import java.util.*;
import java.util.Map.Entry;

/**
 * Lazy implementation of Set. Can be bound to a source ORecord object to keep track of changes. This avoid to call the makeDirty()
 * by hand when the set is changed. <p> <b>Internals</b>: <ul> <li>stores new records in a separate IdentityHashMap to keep
 * underlying list (delegate) always ordered and minimizing sort operations</li> <li></li> </ul> <p> </p>
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class ORecordLazySet extends AbstractCollection<OIdentifiable>
    implements Set<OIdentifiable>, OTrackedMultiValue<OIdentifiable, OIdentifiable>, ORecordElement, ORecordLazyMultiValue,
    OIdentityChangeListener {

  protected              boolean                    autoConvertToRecord = true;
  protected final        ORecord                    sourceRecord;
  protected              Map<OIdentifiable, Object> map                 = new HashMap<OIdentifiable, Object>();
  private                STATUS                     status              = STATUS.NOT_LOADED;
  protected static final Object                     ENTRY_REMOVAL       = new Object();
  private                boolean                    dirty               = false;

  private List<OMultiValueChangeListener<OIdentifiable, OIdentifiable>> changeListeners;

  public ORecordLazySet(final ODocument iSourceRecord) {
    this.sourceRecord = iSourceRecord;
  }

  public ORecordLazySet(ODocument iSourceRecord, Collection<OIdentifiable> iOrigin) {
    this(iSourceRecord);
    if (iOrigin != null && !iOrigin.isEmpty())
      addAll(iOrigin);
  }

  @Override
  public ORecordElement getOwner() {
    return sourceRecord;
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
  public ORecordLazySet setDirty() {
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

  @Override
  public boolean detach() {
    return convertRecords2Links();
  }

  @Override
  public Iterator<OIdentifiable> iterator() {
    return new OLazyRecordIterator(new OLazyIterator<OIdentifiable>() {
      {
        iter = ORecordLazySet.this.map.entrySet().iterator();
      }

      private Iterator<Entry<OIdentifiable, Object>> iter;
      private Entry<OIdentifiable, Object> last;

      @Override
      public boolean hasNext() {
        return iter.hasNext();
      }

      @Override
      public OIdentifiable next() {
        Entry<OIdentifiable, Object> entry = iter.next();
        last = entry;
        if (entry.getValue() != ENTRY_REMOVAL)
          return (OIdentifiable) entry.getValue();
        if (entry.getKey() instanceof ORecordId && autoConvertToRecord && ODatabaseRecordThreadLocal.instance().isDefined()) {
          try {
            final ORecord rec = entry.getKey().getRecord();
            if (sourceRecord != null && rec != null)
              ORecordInternal.track(sourceRecord, rec);
            if (iter instanceof OLazyIterator<?>) {
              ((OLazyIterator<Entry<OIdentifiable, Object>>) iter).update(entry);
            }
            last = entry;
          } catch (Exception e) {
            OLogManager.instance().error(this, "Error on iterating record collection", e);
            entry = null;
          }

        }

        return entry == null ? null : entry.getKey();
      }

      @Override
      public void remove() {
        iter.remove();
        if (last.getKey() instanceof ORecord)
          ORecordInternal.removeIdentityChangeListener((ORecord) last.getKey(), ORecordLazySet.this);
      }

      @Override
      public OIdentifiable update(OIdentifiable iValue) {
        if (iValue != null)
          map.put(iValue.getIdentity(), iValue.getRecord());
        return iValue;
      }
    }, autoConvertToRecord);
  }

  @Override
  public Iterator<OIdentifiable> rawIterator() {
    return new OLazyRecordIterator(new ORecordTrackedIterator(sourceRecord, map.keySet().iterator()), false);
  }

  @Override
  public boolean add(OIdentifiable e) {
    if (map.containsKey(e))
      return false;

    if (e == null)
      map.put(null, null);
    else if (e instanceof ORecord && e.getIdentity().isNew()) {
      ORecordInternal.addIdentityChangeListener((ORecord) e, this);
      map.put(e, e);
    } else if (!e.getIdentity().isPersistent()) {
      map.put(e, e);
    } else
      map.put(e, ENTRY_REMOVAL);
    addEvent(e);
    return true;
  }

  public void convertLinks2Records() {
    final Iterator<Entry<OIdentifiable, Object>> all = map.entrySet().iterator();
    while (all.hasNext()) {
      Entry<OIdentifiable, Object> entry = all.next();
      if (!(entry.getValue() instanceof ORecord)) {
        try {
          ORecord record = entry.getKey().getRecord();
          if (record != null) {
            ORecordInternal.unTrack(sourceRecord, entry.getKey());
            ORecordInternal.track(sourceRecord, record);
          }
          entry.setValue(record);
        } catch (ORecordNotFoundException ignore) {
          // IGNORE THIS
        }
      }
    }

  }

  @Override
  public void onAfterIdentityChange(ORecord record) {
    map.put(record, record);
  }

  @Override
  public void onBeforeIdentityChange(ORecord record) {
    map.remove(record);
  }

  @Override
  public boolean convertRecords2Links() {
    return true;
  }

  public boolean clearDeletedRecords() {
    boolean removed = false;
    final Iterator<Entry<OIdentifiable, Object>> all = map.entrySet().iterator();
    while (all.hasNext()) {
      Entry<OIdentifiable, Object> entry = all.next();
      if (entry.getValue() == ENTRY_REMOVAL) {
        try {
          if (entry.getKey().getRecord() == null) {
            all.remove();
            removed = true;
          }
        } catch (ORecordNotFoundException ignore) {
          all.remove();
          removed = true;
        }
      }
    }
    return removed;
  }

  public boolean remove(Object o) {
    if (o == null)
      return clearDeletedRecords();

    final Object old = map.remove(o);
    if (old != null) {
      if (o instanceof ORecord)
        ORecordInternal.removeIdentityChangeListener((ORecord) o, this);
      removeEvent((OIdentifiable) o);
      return true;
    }
    return false;
  }

  @Override
  public boolean isAutoConvertToRecord() {
    return autoConvertToRecord;
  }

  @Override
  public void setAutoConvertToRecord(boolean convertToRecord) {
    this.autoConvertToRecord = convertToRecord;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof Set<?>) {
      Set<Object> coll = ((Set<Object>) obj);
      if (map.size() == coll.size()) {
        for (Object obje : coll) {
          if (!map.containsKey(obje))
            return false;
        }
        return true;
      }
    }
    return false;
  }

  @Override
  public int hashCode() {
    return 0;
  }

}
