/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
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
 *  * For more information: http://www.orientechnologies.com
 *
 */
package com.orientechnologies.orient.core.db.record;

import java.util.*;

import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;

/**
 * Implementation of Set bound to a source ORecord object to keep track of changes. This avoid to call the makeDirty() by hand when
 * the set is changed.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class ORecordTrackedSet extends AbstractCollection<OIdentifiable> implements Set<OIdentifiable>,
    OTrackedMultiValue<OIdentifiable, OIdentifiable>, ORecordElement {
  protected final ORecord                                               sourceRecord;
  protected Map<OIdentifiable, Object>                                  map             = new HashMap<OIdentifiable, Object>();
  private STATUS                                                        status          = STATUS.NOT_LOADED;
  protected final static Object                                         ENTRY_REMOVAL   = new Object();
  private List<OMultiValueChangeListener<OIdentifiable, OIdentifiable>> changeListeners;

  public ORecordTrackedSet(final ORecord iSourceRecord) {
    this.sourceRecord = iSourceRecord;
    if (iSourceRecord != null)
      iSourceRecord.setDirty();
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
    setDirty();
    
    ORecordInternal.track(sourceRecord, e);
    
    if (e instanceof ODocument)
      ODocumentInternal.addOwner((ODocument) e, this);

    fireCollectionChangedEvent(new OMultiValueChangeEvent<OIdentifiable, OIdentifiable>(OMultiValueChangeEvent.OChangeType.ADD, e,
        e));
    return true;
  }

  @Override
  public boolean contains(Object o) {
    return map.containsKey(o);
  }

  public boolean remove(Object o) {
    final Object old = map.remove(o);
    if (old != null) {
      if (o instanceof ODocument)
        ODocumentInternal.removeOwner((ODocument) o, this);

      setDirty();
      fireCollectionChangedEvent(new OMultiValueChangeEvent<OIdentifiable, OIdentifiable>(
          OMultiValueChangeEvent.OChangeType.REMOVE, (OIdentifiable) o, null, (OIdentifiable) o));
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

    for (OIdentifiable o : c)
      add(o);

    setDirty();
    return true;
  }

  public boolean retainAll(final Collection<?> c) {
    if (c == null || c.size() == 0)
      return false;

    if (super.retainAll(c)) {
      setDirty();
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
    if (status != STATUS.UNMARSHALLING && sourceRecord != null
        && !(sourceRecord.isDirty() && ORecordInternal.isContentChanged(sourceRecord)))
      sourceRecord.setDirty();
    return this;
  }

  @Override
  public void setDirtyNoChanged() {
    if (status != STATUS.UNMARSHALLING && sourceRecord != null)
      sourceRecord.setDirtyNoChanged();
  }

  public STATUS getInternalStatus() {
    return status;
  }

  public void setInternalStatus(final STATUS iStatus) {
    status = iStatus;
  }

  public void addChangeListener(final OMultiValueChangeListener<OIdentifiable, OIdentifiable> changeListener) {
    if(changeListeners == null)
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
    if (getOwner().getInternalStatus() == STATUS.UNMARSHALLING)
      return;

    setDirty();
    if (changeListeners != null) {
      for (final OMultiValueChangeListener<OIdentifiable, OIdentifiable> changeListener : changeListeners) {
        if (changeListener != null)
          changeListener.onAfterRecordChanged(event);
      }
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

}
