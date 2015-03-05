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

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.orientechnologies.common.collection.OLazyIterator;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.OIdentityChangeListener;
import com.orientechnologies.orient.core.record.OIdentityChangeListener;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import com.orientechnologies.orient.core.serialization.serializer.record.OSerializationSetThreadLocal;

/**
 * Lazy implementation of Set. Can be bound to a source ORecord object to keep track of changes. This avoid to call the makeDirty()
 * by hand when the set is changed.
 * <p>
 * <b>Internals</b>:
 * <ul>
 * <li>stores new records in a separate IdentityHashMap to keep underlying list (delegate) always ordered and minimizing sort
 * operations</li>
 * <li></li>
 * </ul>
 * 
 * </p>
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class ORecordLazySet extends ORecordTrackedSet implements Set<OIdentifiable>, ORecordLazyMultiValue, ORecordElement,
    OIdentityChangeListener {
  protected boolean                     autoConvertToRecord = true;
  protected Map<OIdentifiable, ORecord> recordCache;

  public ORecordLazySet(final ODocument iSourceRecord) {
    super(iSourceRecord);
  }

  public ORecordLazySet(ODocument iSourceRecord, Collection<OIdentifiable> iOrigin) {
    this(iSourceRecord);
    if (iOrigin != null && !iOrigin.isEmpty()) {
        addAll(iOrigin);
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
        if (OSerializationSetThreadLocal.check((ODocument) sourceRecord)) {
          iter = new HashSet<Entry<OIdentifiable, Object>>(ORecordLazySet.super.map.entrySet()).iterator();
        } else {
            iter = ORecordLazySet.super.map.entrySet().iterator();
        }
      }
      private Iterator<Entry<OIdentifiable, Object>> iter;
      private Entry<OIdentifiable, Object>           last;

      @Override
      public boolean hasNext() {
        return iter.hasNext();
      }

      @Override
      public OIdentifiable next() {
        Entry<OIdentifiable, Object> entry = iter.next();
        last = entry;
        if (entry.getValue() != ENTRY_REMOVAL) {
            return (OIdentifiable) entry.getValue();
        }
        return entry.getKey();
      }

      @Override
      public void remove() {
        iter.remove();
        if (last.getKey() instanceof ORecord) {
            ORecordInternal.removeIdentityChangeListener((ORecord) last.getKey(), ORecordLazySet.this);
        }
      }

      @Override
      public OIdentifiable update(OIdentifiable iValue) {
        if (iValue != null) {
            map.put(iValue.getIdentity(), iValue.getRecord());
        }
        return iValue;
      }
    }, autoConvertToRecord && getOwner().getInternalStatus() != STATUS.MARSHALLING);
  }

  @Override
  public Iterator<OIdentifiable> rawIterator() {
    return new OLazyRecordIterator(super.iterator(), false);
  }

  @Override
  public boolean add(OIdentifiable e) {
    if (map.containsKey(e)) {
        return false;
    }
    if (e instanceof ORecord && e.getIdentity().isNew()) {
      ORecordInternal.addIdentityChangeListener((ORecord) e, this);
      map.put(e, e);
    } else {
        map.put(e, ENTRY_REMOVAL);
    }
    setDirty();

    fireCollectionChangedEvent(new OMultiValueChangeEvent<OIdentifiable, OIdentifiable>(OMultiValueChangeEvent.OChangeType.ADD, e,
        e));

    return true;
  }

  public void convertLinks2Records() {
    Iterator<Entry<OIdentifiable, Object>> all = map.entrySet().iterator();
    while (all.hasNext()) {
      Entry<OIdentifiable, Object> entry = all.next();
      if (entry.getValue() == ENTRY_REMOVAL) {
        try {
          entry.setValue(entry.getKey().getRecord());
        } catch (ORecordNotFoundException e) {
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
    Iterator<Entry<OIdentifiable, Object>> all = map.entrySet().iterator();
    while (all.hasNext()) {
      Entry<OIdentifiable, Object> entry = all.next();
      if (entry.getValue() == ENTRY_REMOVAL) {
        try {
          if (entry.getKey().getRecord() == null) {
            all.remove();
            removed = true;
          }
        } catch (ORecordNotFoundException e) {
          all.remove();
          removed = true;
        }
      }
    }
    return removed;
  }

  public boolean remove(Object o) {
    if (o == null) {
        return clearDeletedRecords();
    }

    final Object old = map.remove(o);
    if (old != null) {
      if (o instanceof ORecord) {
          ORecordInternal.removeIdentityChangeListener((ORecord) o, this);
      }

      setDirty();
      fireCollectionChangedEvent(new OMultiValueChangeEvent<OIdentifiable, OIdentifiable>(
          OMultiValueChangeEvent.OChangeType.REMOVE, (OIdentifiable) o, null, (OIdentifiable) o));
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

}
