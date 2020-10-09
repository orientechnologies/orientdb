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

package com.orientechnologies.orient.core.storage.ridbag;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.OMultiValueChangeEvent;
import com.orientechnologies.orient.core.db.record.OMultiValueChangeTimeLine;
import com.orientechnologies.orient.core.db.record.ORecordElement;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBagDelegate;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.OSimpleMultiValueTracker;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.core.storage.impl.local.paginated.ORecordSerializationContext;
import com.orientechnologies.orient.core.storage.ridbag.sbtree.Change;
import com.orientechnologies.orient.core.storage.ridbag.sbtree.OBonsaiCollectionPointer;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.NavigableMap;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.ConcurrentSkipListMap;

public class ORemoteTreeRidBag implements ORidBagDelegate {
  /** Entries with not valid id. */
  private int size;

  private OSimpleMultiValueTracker<OIdentifiable, OIdentifiable> tracker =
      new OSimpleMultiValueTracker<>(this);

  private boolean autoConvertToRecord = true;

  private transient ORecordElement owner;
  private boolean dirty;
  private boolean transactionDirty = false;
  private ORecordId ownerRecord;
  private String fieldName;
  private OBonsaiCollectionPointer collectionPointer;

  private class RemovableIterator implements Iterator<OIdentifiable> {
    private Iterator<OIdentifiable> iter;
    private OIdentifiable next;
    private OIdentifiable removeNext;

    public RemovableIterator(Iterator<OIdentifiable> iterator) {
      this.iter = iterator;
    }

    @Override
    public boolean hasNext() {
      if (next != null) {
        return true;
      } else {
        if (iter.hasNext()) {
          next = iter.next();
          return true;
        } else {
          return false;
        }
      }
    }

    @Override
    public OIdentifiable next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      OIdentifiable val = next;
      removeNext = next;
      next = null;
      return val;
    }

    @Override
    public void remove() {
      if (removeNext != null) {
        ORemoteTreeRidBag.this.remove(removeNext);
        removeNext = null;
      } else {
        throw new IllegalStateException();
      }
    }
  }

  @Override
  public void setSize(int size) {
    this.size = size;
  }

  public ORemoteTreeRidBag(OBonsaiCollectionPointer pointer) {
    this.size = -1;
    this.collectionPointer = pointer;
  }

  @Override
  public ORecordElement getOwner() {
    return owner;
  }

  @Override
  public void setOwner(ORecordElement owner) {
    if (owner != null && this.owner != null && !this.owner.equals(owner)) {
      throw new IllegalStateException(
          "This data structure is owned by document "
              + owner
              + " if you want to use it in other document create new rid bag instance and copy content of current one.");
    }
    this.owner = owner;
    if (this.owner != null && tracker.getTimeLine() != null) {
      for (OMultiValueChangeEvent event : tracker.getTimeLine().getMultiValueChangeEvents()) {
        switch (event.getChangeType()) {
          case ADD:
            ORecordInternal.track(this.owner, (OIdentifiable) event.getKey());
            break;
        }
      }
    }
  }

  @Override
  public Iterator<OIdentifiable> iterator() {
    List<OIdentifiable> set = loadElements();
    if (this.isAutoConvertToRecord()) {
      return new RemovableIterator(
          set.stream()
              .map(
                  (x) -> {
                    return (OIdentifiable) x.getRecord();
                  })
              .iterator());
    } else {
      return new RemovableIterator(set.iterator());
    }
  }

  @Override
  public Iterator<OIdentifiable> rawIterator() {
    List<OIdentifiable> set = loadElements();
    return new RemovableIterator(set.iterator());
  }

  private List<OIdentifiable> loadElements() {
    ODatabaseDocumentInternal database = ODatabaseRecordThreadLocal.instance().get();
    List<OIdentifiable> set;
    try (OResultSet result =
        database.query("select list(@this.field(?)) as elements from ?", fieldName, ownerRecord)) {
      if (result.hasNext()) {
        set = (result.next().getProperty("elements"));
      } else {
        set = ((List<OIdentifiable>) (List) Collections.emptyList());
      }
    }
    if (tracker.getTimeLine() != null) {
      for (OMultiValueChangeEvent event : tracker.getTimeLine().getMultiValueChangeEvents()) {
        switch (event.getChangeType()) {
          case ADD:
            set.add((OIdentifiable) event.getKey());
            break;
          case REMOVE:
            set.remove(event.getKey());
            break;
        }
      }
    }
    return set;
  }

  @Override
  public void convertLinks2Records() {}

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
    autoConvertToRecord = convertToRecord;
  }

  @Override
  public boolean detach() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void addAll(Collection<OIdentifiable> values) {
    for (OIdentifiable identifiable : values) {
      add(identifiable);
    }
  }

  @Override
  public boolean addInternal(OIdentifiable e) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void add(final OIdentifiable identifiable) {
    if (identifiable == null) {
      throw new IllegalArgumentException("Impossible to add a null identifiable in a ridbag");
    }

    if (size >= 0) {
      size++;
    }

    addEvent(identifiable, identifiable);
  }

  @Override
  public void remove(OIdentifiable identifiable) {
    size--;
    boolean exists;
    removeEvent(identifiable);
  }

  @Override
  public boolean contains(OIdentifiable identifiable) {
    return loadElements().contains(identifiable);
  }

  @Override
  public int size() {
    return updateSize();
  }

  @Override
  public String toString() {
    if (size >= 0) {
      return "[size=" + size + "]";
    }

    return "[...]";
  }

  @Override
  public NavigableMap<OIdentifiable, Change> getChanges() {
    return new ConcurrentSkipListMap<>();
  }

  @Override
  public boolean isEmpty() {
    return size() == 0;
  }

  @Override
  public Class<?> getGenericClass() {
    return OIdentifiable.class;
  }

  @Override
  public Object returnOriginalState(
      List<OMultiValueChangeEvent<OIdentifiable, OIdentifiable>> multiValueChangeEvents) {
    final ORemoteTreeRidBag reverted = new ORemoteTreeRidBag(this.collectionPointer);
    for (OIdentifiable identifiable : this) {
      reverted.add(identifiable);
    }

    final ListIterator<OMultiValueChangeEvent<OIdentifiable, OIdentifiable>> listIterator =
        multiValueChangeEvents.listIterator(multiValueChangeEvents.size());

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

  @Override
  public int getSerializedSize() {
    int result = 2 * OLongSerializer.LONG_SIZE + 3 * OIntegerSerializer.INT_SIZE;
    if (ODatabaseRecordThreadLocal.instance().get().isRemote()
        || ORecordSerializationContext.getContext() == null) {
      result += getChangesSerializedSize();
    }
    return result;
  }

  @Override
  public int getSerializedSize(byte[] stream, int offset) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int serialize(byte[] stream, int offset, UUID ownerUuid) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void requestDelete() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int deserialize(byte[] stream, int offset) {
    throw new UnsupportedOperationException();
  }

  /**
   * Recalculates real bag size.
   *
   * @return real size
   */
  private int updateSize() {
    this.size = loadElements().size();
    return size;
  }

  private int getChangesSerializedSize() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void replace(OMultiValueChangeEvent<Object, Object> event, Object newValue) {
    // do nothing not needed
  }

  private void addEvent(OIdentifiable key, OIdentifiable identifiable) {
    if (this.owner != null) {
      ORecordInternal.track(this.owner, identifiable);
    }

    if (tracker.isEnabled()) {
      tracker.addNoDirty(key, identifiable);
    } else {
      setDirtyNoChanged();
    }
  }

  private void removeEvent(OIdentifiable removed) {

    if (this.owner != null) {
      ORecordInternal.unTrack(this.owner, removed);
    }

    if (tracker.isEnabled()) {
      tracker.removeNoDirty(removed, removed);
    } else {
      setDirtyNoChanged();
    }
  }

  public void enableTracking(ORecordElement parent) {
    if (!tracker.isEnabled()) {
      tracker.enable();
    }
  }

  public void disableTracking(ORecordElement document) {
    if (tracker.isEnabled()) {
      this.tracker.disable();
      this.dirty = false;
    }
  }

  @Override
  public void transactionClear() {
    tracker.transactionClear();
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
  public <RET> RET setDirty() {
    if (owner != null) {
      owner.setDirty();
    }
    this.dirty = true;
    this.transactionDirty = true;
    return (RET) this;
  }

  public void setTransactionModified(boolean transactionDirty) {
    this.transactionDirty = transactionDirty;
  }

  @Override
  public void setDirtyNoChanged() {
    if (owner != null) owner.setDirtyNoChanged();
    this.dirty = true;
    this.transactionDirty = true;
  }

  @Override
  public OSimpleMultiValueTracker<OIdentifiable, OIdentifiable> getTracker() {
    return tracker;
  }

  @Override
  public void setTracker(OSimpleMultiValueTracker<OIdentifiable, OIdentifiable> tracker) {
    this.tracker.sourceFrom(tracker);
  }

  @Override
  public OMultiValueChangeTimeLine<OIdentifiable, OIdentifiable> getTransactionTimeLine() {
    return this.tracker.getTransactionTimeLine();
  }

  public void setRecordAndField(ORecordId id, String fieldName) {
    this.ownerRecord = id;
    this.fieldName = fieldName;
  }

  public OBonsaiCollectionPointer getCollectionPointer() {
    return collectionPointer;
  }
}
