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
import com.orientechnologies.orient.core.record.impl.OSimpleMultiValueTracker;
import java.util.AbstractCollection;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * Lazy implementation of Set. Can be bound to a source ORecord object to keep track of changes.
 * This avoid to call the makeDirty() by hand when the set is changed.
 *
 * <p><b>Internals</b>:
 *
 * <ul>
 *   <li>stores new records in a separate IdentityHashMap to keep underlying list (delegate) always
 *       ordered and minimizing sort operations
 *   <li>
 * </ul>
 *
 * <p>
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class ORecordLazySet extends AbstractCollection<OIdentifiable>
    implements Set<OIdentifiable>,
        OTrackedMultiValue<OIdentifiable, OIdentifiable>,
        ORecordElement,
        ORecordLazyMultiValue,
        OIdentityChangeListener {

  protected boolean autoConvertToRecord = true;
  protected final ORecordElement sourceRecord;
  protected Map<OIdentifiable, Object> map = new HashMap<OIdentifiable, Object>();
  protected static final Object ENTRY_REMOVAL = new Object();
  private boolean dirty = false;
  private boolean transactionDirty = false;

  private OSimpleMultiValueTracker<OIdentifiable, OIdentifiable> tracker =
      new OSimpleMultiValueTracker<>(this);

  public ORecordLazySet(final ORecordElement iSourceRecord) {
    this.sourceRecord = iSourceRecord;
  }

  public ORecordLazySet(ORecordElement iSourceRecord, Collection<OIdentifiable> iOrigin) {
    this(iSourceRecord);
    if (iOrigin != null && !iOrigin.isEmpty()) addAll(iOrigin);
  }

  @Override
  public ORecordElement getOwner() {
    return sourceRecord;
  }

  public boolean addInternal(final OIdentifiable e) {
    if (map.containsKey(e)) return false;

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
      if (remove(item)) changed = true;
    }

    if (changed) setDirty();

    return changed;
  }

  public boolean addAll(final Collection<? extends OIdentifiable> c) {
    if (c == null || c.size() == 0) return false;
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
    if (c == null || c.size() == 0) return false;

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
    this.transactionDirty = true;
    return this;
  }

  @Override
  public void setDirtyNoChanged() {
    if (sourceRecord != null) sourceRecord.setDirtyNoChanged();
  }

  public Set<OIdentifiable> returnOriginalState(
      final List<OMultiValueChangeEvent<OIdentifiable, OIdentifiable>> events) {
    final Set<OIdentifiable> reverted = new HashSet<OIdentifiable>(this);

    final ListIterator<OMultiValueChangeEvent<OIdentifiable, OIdentifiable>> listIterator =
        events.listIterator(events.size());

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

  protected void addOwnerToEmbeddedDoc(OIdentifiable e) {
    if (sourceRecord != null && e != null) {
      ORecordInternal.track(sourceRecord, e);
    }
  }

  protected void addEvent(OIdentifiable added) {
    addOwnerToEmbeddedDoc(added);

    if (tracker.isEnabled()) {
      tracker.add(added, added);
    } else {
      setDirty();
    }
  }

  private void updateEvent(OIdentifiable oldValue, OIdentifiable newValue) {
    if (oldValue instanceof ODocument) ODocumentInternal.removeOwner((ODocument) oldValue, this);

    addOwnerToEmbeddedDoc(newValue);

    if (tracker.isEnabled()) {
      tracker.updated(oldValue, newValue, newValue);
    } else {
      setDirty();
    }
  }

  protected void removeEvent(OIdentifiable removed) {
    if (removed instanceof ODocument) {
      ODocumentInternal.removeOwner((ODocument) removed, this);
    }
    if (tracker.isEnabled()) {
      tracker.remove(removed, removed);
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
    // not needed do nothing
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

  public void disableTracking(ORecordElement document) {
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

  @Override
  public boolean detach() {
    return convertRecords2Links();
  }

  @Override
  public Iterator<OIdentifiable> iterator() {
    return new OLazyRecordIterator(
        new OLazyIterator<OIdentifiable>() {
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
            if (entry.getValue() != ENTRY_REMOVAL) return (OIdentifiable) entry.getValue();
            if (entry.getKey() instanceof ORecordId
                && autoConvertToRecord
                && ODatabaseRecordThreadLocal.instance().isDefined()) {
              try {
                final ORecord rec = entry.getKey().getRecord();
                if (sourceRecord != null && rec != null) ORecordInternal.track(sourceRecord, rec);
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
              ORecordInternal.removeIdentityChangeListener(
                  (ORecord) last.getKey(), ORecordLazySet.this);
          }

          @Override
          public OIdentifiable update(OIdentifiable iValue) {
            if (iValue != null) map.put(iValue.getIdentity(), iValue.getRecord());
            return iValue;
          }
        },
        autoConvertToRecord);
  }

  @Override
  public Iterator<OIdentifiable> rawIterator() {
    return new OLazyRecordIterator(
        new ORecordTrackedIterator(sourceRecord, map.keySet().iterator()), false);
  }

  @Override
  public boolean add(OIdentifiable e) {
    if (map.containsKey(e)) return false;

    if (e == null) map.put(null, null);
    else if (e instanceof ORecord && e.getIdentity().isNew()) {
      ORecordInternal.addIdentityChangeListener((ORecord) e, this);
      map.put(e, e);
    } else if (!e.getIdentity().isPersistent()) {
      map.put(e, e);
    } else map.put(e, ENTRY_REMOVAL);
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
    if (o == null) return clearDeletedRecords();

    final Object old = map.remove(o);
    if (old != null) {
      if (o instanceof ORecord) ORecordInternal.removeIdentityChangeListener((ORecord) o, this);
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
          if (!map.containsKey(obje)) return false;
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

  @Override
  public String toString() {
    return ORecordMultiValueHelper.toString(this);
  }

  @Override
  public OMultiValueChangeTimeLine<OIdentifiable, OIdentifiable> getTransactionTimeLine() {
    return tracker.getTransactionTimeLine();
  }
}
