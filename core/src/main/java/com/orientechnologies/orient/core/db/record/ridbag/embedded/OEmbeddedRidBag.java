/*
 * Copyright 2010-2012 Luca Garulli (l.garulli(at)orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.core.db.record.ridbag.embedded;

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.types.OModifiableInteger;
import com.orientechnologies.common.util.OResettable;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.OMultiValueChangeEvent;
import com.orientechnologies.orient.core.db.record.OMultiValueChangeListener;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBagDelegate;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.OIdentityChangeListener;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OLinkSerializer;

import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;

public class OEmbeddedRidBag implements ORidBagDelegate, OIdentityChangeListener {
  private Map<OIdentifiable, OModifiableInteger>                       newEntries      = new IdentityHashMap<OIdentifiable, OModifiableInteger>();
  private NavigableMap<OIdentifiable, OModifiableInteger>              entries         = new ConcurrentSkipListMap<OIdentifiable, OModifiableInteger>();
  private boolean                                                      convertToRecord = true;
  private int                                                          size            = 0;
  private transient ORecord<?>                                         owner;

  private Set<OMultiValueChangeListener<OIdentifiable, OIdentifiable>> changeListeners = Collections
                                                                                           .newSetFromMap(new WeakHashMap<OMultiValueChangeListener<OIdentifiable, OIdentifiable>, Boolean>());

  @Override
  public void setOwner(ORecord<?> owner) {
    if (owner != null && this.owner != null && !this.owner.equals(owner)) {
      throw new IllegalStateException("This data structure is owned by document " + owner
          + " if you want to use it in other document create new rid bag instance and copy content of current one.");
    }

    this.owner = owner;
  }

  @Override
  public ORecord<?> getOwner() {
    return owner;
  }

  @Override
  public void addAll(Collection<OIdentifiable> values) {
    for (OIdentifiable value : values) {
      add(value);
    }
  }

  @Override
  public void add(OIdentifiable identifiable) {
    if (identifiable.getIdentity().isValid()) {
      final OModifiableInteger counter = entries.get(identifiable);
      if (counter == null)
        entries.put(identifiable, new OModifiableInteger(1));
      else
        counter.increment();
    } else
      newEntries.put(identifiable, new OModifiableInteger(1));

    if (identifiable instanceof ORecord) {
      ORecord record = (ORecord) identifiable;
      record.addIdentityChangeListener(this);
    }
    size++;

    fireCollectionChangedEvent(new OMultiValueChangeEvent<OIdentifiable, OIdentifiable>(OMultiValueChangeEvent.OChangeType.ADD,
        identifiable, identifiable));
  }

  @Override
  public void remove(OIdentifiable identifiable) {
    if (identifiable.getIdentity().isValid()) {
      final OModifiableInteger counter = entries.get(identifiable);
      if (counter == null)
        return;

      counter.decrement();
      if (counter.intValue() < 1) {
        entries.remove(identifiable);
        if (identifiable instanceof ORecord) {
          ORecord record = (ORecord) identifiable;
          record.removeIdentityChangeListener(this);
        }
      }

      size--;

      fireCollectionChangedEvent(new OMultiValueChangeEvent<OIdentifiable, OIdentifiable>(
          OMultiValueChangeEvent.OChangeType.REMOVE, identifiable, null, identifiable));
    } else if (newEntries.remove(identifiable) != null) {
      size--;
      if (identifiable instanceof ORecord) {
        ORecord record = (ORecord) identifiable;
        record.removeIdentityChangeListener(this);
      }
      fireCollectionChangedEvent(new OMultiValueChangeEvent<OIdentifiable, OIdentifiable>(
          OMultiValueChangeEvent.OChangeType.REMOVE, identifiable, null, identifiable));
    }

  }

  @Override
  public boolean isEmpty() {
    return size == 0;
  }

  @Override
  public Iterator<OIdentifiable> iterator() {
    return new EntriesIterator(convertToRecord);
  }

  @Override
  public Iterator<OIdentifiable> rawIterator() {
    return new EntriesIterator(false);
  }

  @Override
  public void convertLinks2Records() {
    final Map<OIdentifiable, OModifiableInteger> convertedEntries = new HashMap<OIdentifiable, OModifiableInteger>();

    for (Map.Entry<OIdentifiable, OModifiableInteger> entry : entries.entrySet()) {
      ORecord record = entry.getKey().getRecord();
      if (record != null)
        convertedEntries.put(record, entry.getValue());
      else
        convertedEntries.put(entry.getKey(), entry.getValue());
    }

    entries.clear();
    entries.putAll(convertedEntries);
  }

  @Override
  public boolean convertRecords2Links() {
    final Map<OIdentifiable, OModifiableInteger> convertedEntries = new HashMap<OIdentifiable, OModifiableInteger>();
    for (Map.Entry<OIdentifiable, OModifiableInteger> entry : entries.entrySet()) {
      if (entry.getKey() instanceof ORecord) {
        final ORecord record = (ORecord) entry.getKey();
        if (record.isDirty() || entry.getKey().getIdentity().isNew()) {
          record.save();
          convertedEntries.put(record.getIdentity(), entry.getValue());
        }
      } else
        convertedEntries.put(entry.getKey().getIdentity(), entry.getValue());
    }

    for (Map.Entry<OIdentifiable, OModifiableInteger> entry : entries.entrySet()) {
      if (entry.getKey() instanceof ORecord) {
        final ORecord record = (ORecord) entry.getKey();
        record.save();
        convertedEntries.put(record.getIdentity(), entry.getValue());
      } else
        convertedEntries.put(entry.getKey().getIdentity(), entry.getValue());
    }

    newEntries.clear();
    entries.clear();
    entries.putAll(convertedEntries);

    return true;
  }

  @Override
  public boolean isAutoConvertToRecord() {
    return convertToRecord;
  }

  @Override
  public void setAutoConvertToRecord(boolean convertToRecord) {
    this.convertToRecord = convertToRecord;
  }

  @Override
  public boolean detach() {
    return convertRecords2Links();
  }

  @Override
  public int size() {
    return size;
  }

  @Override
  public String toString() {
    if (size < 10)
      return OMultiValue.toString(this);
    else
      return "[size=" + size + "]";
  }

  public void addChangeListener(final OMultiValueChangeListener<OIdentifiable, OIdentifiable> changeListener) {
    changeListeners.add(changeListener);
  }

  public void removeRecordChangeListener(final OMultiValueChangeListener<OIdentifiable, OIdentifiable> changeListener) {
    changeListeners.remove(changeListener);
  }

  @Override
  public Object returnOriginalState(List<OMultiValueChangeEvent<OIdentifiable, OIdentifiable>> multiValueChangeEvents) {
    final OEmbeddedRidBag reverted = new OEmbeddedRidBag();
    for (OIdentifiable identifiable : this)
      reverted.add(identifiable);

    final ListIterator<OMultiValueChangeEvent<OIdentifiable, OIdentifiable>> listIterator = multiValueChangeEvents
        .listIterator(multiValueChangeEvents.size());

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
    int size = OIntegerSerializer.INT_SIZE;
    size += newEntries.size() * (OLinkSerializer.RID_SIZE + OIntegerSerializer.INT_SIZE);
    size += entries.size() * (OLinkSerializer.RID_SIZE + OIntegerSerializer.INT_SIZE);

    return size;
  }

  @Override
  public int getSerializedSize(byte[] stream, int offset) {
    return OIntegerSerializer.INSTANCE.deserialize(stream, offset) * (OLinkSerializer.RID_SIZE + OIntegerSerializer.INT_SIZE);
  }

  @Override
  public int serialize(byte[] stream, int offset) {
    for (OIdentifiable identifiable : entries.keySet()) {
      if (identifiable instanceof ORecord) {
        ORecord record = (ORecord) identifiable;
        if (record.getIdentity().isNew() || record.isDirty()) {
          record.save();
        }
      }
    }

    for (OIdentifiable identifiable : newEntries.keySet()) {
      if (identifiable instanceof ORecord) {
        ORecord record = (ORecord) identifiable;
        record.removeIdentityChangeListener(this);
        record.save();
        record.addIdentityChangeListener(this);
        entries.put(identifiable, new OModifiableInteger(1));
      } else
        entries.put(identifiable, new OModifiableInteger(1));
    }

    newEntries.clear();

    OIntegerSerializer.INSTANCE.serialize(entries.size(), stream, offset);
    offset += OIntegerSerializer.INT_SIZE;

    for (Map.Entry<OIdentifiable, OModifiableInteger> entry : entries.entrySet()) {
      OLinkSerializer.INSTANCE.serialize(entry.getKey(), stream, offset);
      offset += OLinkSerializer.RID_SIZE;

      OIntegerSerializer.INSTANCE.serialize(entry.getValue().intValue(), stream, offset);
      offset += OIntegerSerializer.INT_SIZE;
    }

    return offset;
  }

  @Override
  public int deserialize(byte[] stream, int offset) {
    int entriesSize = OIntegerSerializer.INSTANCE.deserialize(stream, offset);
    offset += OIntegerSerializer.INT_SIZE;

    for (int i = 0; i < entriesSize; i++) {
      ORID rid = OLinkSerializer.INSTANCE.deserialize(stream, offset);
      offset += OLinkSerializer.RID_SIZE;

      int counter = OIntegerSerializer.INSTANCE.deserialize(stream, offset);
      offset += OIntegerSerializer.INT_SIZE;

      assert counter > 0;

      size += counter;

      entries.put(rid, new OModifiableInteger(counter));
    }

    return offset;
  }

  @Override
  public void requestDelete() {
  }

  @Override
  public void onIdentityChanged(ORID prevRid, ORecord<?> record) {
    if (!prevRid.isValid()) {
      final OModifiableInteger counter = newEntries.remove(record);
      if (counter != null)
        entries.put(record, counter);
    } else {
      final OModifiableInteger counter = entries.remove(prevRid);
      if (counter != null)
        entries.put(record, counter);
    }
  }

  protected void fireCollectionChangedEvent(final OMultiValueChangeEvent<OIdentifiable, OIdentifiable> event) {
    for (final OMultiValueChangeListener<OIdentifiable, OIdentifiable> changeListener : changeListeners) {
      if (changeListener != null)
        changeListener.onAfterRecordChanged(event);
    }
  }

  @Override
  public Class<?> getGenericClass() {
    return OIdentifiable.class;
  }

  private final class EntriesIterator implements Iterator<OIdentifiable>, OResettable {
    private Iterator<Map.Entry<OIdentifiable, OModifiableInteger>> newEntriesIterator;
    private Iterator<Map.Entry<OIdentifiable, OModifiableInteger>> entriesIterator;

    private Map<OIdentifiable, OModifiableInteger>                 newEntries;

    private final boolean                                          convertToRecord;

    private OIdentifiable                                          currentValue = null;

    private int                                                    currentFinalCounter;
    private int                                                    currentCounter;

    private boolean                                                currentRemoved;

    private EntriesIterator(boolean convertToRecord) {
      reset();
      this.convertToRecord = convertToRecord;
    }

    @Override
    public boolean hasNext() {
      return (currentValue != null && currentCounter < currentFinalCounter) || newEntriesIterator.hasNext()
          || entriesIterator.hasNext();
    }

    @Override
    public OIdentifiable next() {
      currentRemoved = false;

      if (currentCounter < currentFinalCounter) {
        currentCounter++;
        return currentValue;
      }

      Map.Entry<OIdentifiable, OModifiableInteger> nextEntry;
      if (newEntriesIterator.hasNext())
        nextEntry = newEntriesIterator.next();
      else
        nextEntry = entriesIterator.next();

      currentCounter = 1;
      currentFinalCounter = nextEntry.getValue().intValue();
      currentValue = nextEntry.getKey();

      if (convertToRecord)
        return currentValue.getRecord();

      return currentValue;
    }

    @Override
    public void remove() {
      if (currentRemoved)
        throw new IllegalStateException("Current element has already been removed");

      if (currentValue == null)
        throw new IllegalStateException("Next method was not called for given iterator");

      currentRemoved = true;

      if (currentValue.getIdentity().isValid()) {
        OModifiableInteger counter = entries.get(currentValue);
        counter.decrement();
        if (counter.intValue() < 1) {
          entries.remove(currentValue);
          if (currentValue instanceof ORecord) {
            ORecord record = (ORecord) currentValue;
            record.removeIdentityChangeListener(OEmbeddedRidBag.this);
          }
          entriesIterator = entries.tailMap(currentValue, false).entrySet().iterator();
        }
      } else {
        newEntries.remove(currentValue);
        if (currentValue instanceof ORecord) {
          ORecord record = (ORecord) currentValue;
          record.removeIdentityChangeListener(OEmbeddedRidBag.this);
        }
      }

      size--;

      fireCollectionChangedEvent(new OMultiValueChangeEvent<OIdentifiable, OIdentifiable>(
          OMultiValueChangeEvent.OChangeType.REMOVE, currentValue, null, currentValue));
    }

    @Override
    public void reset() {
      this.newEntriesIterator = new IdentityHashMap<OIdentifiable, OModifiableInteger>(OEmbeddedRidBag.this.newEntries).entrySet()
          .iterator();
      this.entriesIterator = OEmbeddedRidBag.this.entries.entrySet().iterator();
      this.newEntries = OEmbeddedRidBag.this.newEntries;

      currentFinalCounter = 0;
      currentCounter = 0;
      currentValue = null;
      currentRemoved = false;

    }
  }
}
