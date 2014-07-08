/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.core.db.record;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;

import com.orientechnologies.common.collection.OLazyIterator;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;

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
public class ORecordLazySet extends ORecordTrackedSet implements Set<OIdentifiable>, ORecordLazyMultiValue, ORecordElement {
  public static final ORecordLazySet EMPTY_SET           = new ORecordLazySet();
  protected boolean                  sorted              = true;
  protected boolean                  autoConvertToRecord = true;

  public ORecordLazySet() {
    super(null);
  }

  public ORecordLazySet(final ODocument iSourceRecord) {
    super(iSourceRecord);
  }

  @Override
  public boolean detach() {
    return convertRecords2Links();
  }

  @Override
  public Iterator<OIdentifiable> iterator() {
    return new OLazyRecordIterator(new OLazyIterator<OIdentifiable>() {
      private Iterator<Entry<OIdentifiable, Object>> iter = ORecordLazySet.super.map.entrySet().iterator();

      @Override
      public boolean hasNext() {
        return iter.hasNext();
      }

      @Override
      public OIdentifiable next() {
        Entry<OIdentifiable, Object> entry = iter.next();
        if (entry.getValue() != ENTRY_REMOVAL)
          return (OIdentifiable) entry.getValue();
        return entry.getKey();
      }

      @Override
      public void remove() {
        iter.remove();
      }

      @Override
      public OIdentifiable update(OIdentifiable iValue) {
        map.put(iValue.getIdentity(), iValue.getRecord());
        return iValue;
      }
    }, true);
  }

  @Override
  public Iterator<OIdentifiable> rawIterator() {
    return new OLazyRecordIterator(super.iterator(), false);
  }

  @Override
  public boolean add(OIdentifiable e) {
    if (map.containsKey(e))
      return false;
    if (e instanceof ORecord)
      map.put(e, e);
    else
      map.put(e, ENTRY_REMOVAL);
    setDirty();

    if (e instanceof ODocument)
      ((ODocument) e).addOwner(this);
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
  public void onAfterIdentityChanged(ORecord<?> iRecord) {
    if (iRecord instanceof ORecord)
      map.put(iRecord, iRecord);
    else
      map.put(iRecord, ENTRY_REMOVAL);
  }

  @Override
  public boolean convertRecords2Links() {
    return true;
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
