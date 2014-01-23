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
import com.orientechnologies.orient.core.db.record.ridbag.ORidBagDelegate;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.OIdentityChangeListener;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OLinkSerializer;

import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;

public class OEmbeddedRidBag implements ORidBagDelegate, OIdentityChangeListener {
  private byte[]                                                       serializedContent = null;

  private boolean                                                      contentWasChanged = false;
  private boolean                                                      deserialized      = true;

  private Map<OIdentifiable, OModifiableInteger>                       newEntries        = new IdentityHashMap<OIdentifiable, OModifiableInteger>();
  private NavigableMap<ORID, IdentifiableContainer>                    entries           = new ConcurrentSkipListMap<ORID, IdentifiableContainer>();

  private boolean                                                      convertToRecord   = true;
  private int                                                          size              = 0;

  private transient ORecord<?>                                         owner;

  private Set<OMultiValueChangeListener<OIdentifiable, OIdentifiable>> changeListeners   = Collections
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
    for (OIdentifiable value : values)
      add(value);
  }

  @Override
  public void add(OIdentifiable identifiable) {
    if (identifiable.getIdentity().isValid()) {
      final IdentifiableContainer container = entries.get(identifiable.getIdentity());

      if (container == null)
        entries.put(identifiable.getIdentity().copy(), new IdentifiableContainer(identifiable, new OModifiableInteger(1)));
      else
        container.getCounter().increment();
    } else
      newEntries.put(identifiable, new OModifiableInteger(1));

    if (identifiable instanceof ORecord) {
      final ORecord record = (ORecord) identifiable;
      record.addIdentityChangeListener(this);
    }

    size++;
    contentWasChanged = true;

    fireCollectionChangedEvent(new OMultiValueChangeEvent<OIdentifiable, OIdentifiable>(OMultiValueChangeEvent.OChangeType.ADD,
        identifiable, identifiable));
  }

  @Override
  public void remove(OIdentifiable identifiable) {
    doDeserialization();

    if (identifiable.getIdentity().isValid()) {
      final IdentifiableContainer container = entries.get(identifiable.getIdentity());
      if (container == null)
        return;

      final OModifiableInteger counter = container.getCounter();
      counter.decrement();

      if (counter.intValue() < 1) {
        entries.remove(identifiable.getIdentity());

        if (container.getIdentifiable() instanceof ORecord) {
          final ORecord record = (ORecord) container.getIdentifiable();
          record.removeIdentityChangeListener(this);
        }
      }

      size--;
      contentWasChanged = true;

      fireCollectionChangedEvent(new OMultiValueChangeEvent<OIdentifiable, OIdentifiable>(
          OMultiValueChangeEvent.OChangeType.REMOVE, identifiable, null, identifiable));
    } else {
      final OModifiableInteger counter = newEntries.remove(identifiable);
      if (counter != null) {
        size--;
        contentWasChanged = true;

        if (identifiable instanceof ORecord) {
          final ORecord record = (ORecord) identifiable;
          record.removeIdentityChangeListener(this);
        }

        fireCollectionChangedEvent(new OMultiValueChangeEvent<OIdentifiable, OIdentifiable>(
            OMultiValueChangeEvent.OChangeType.REMOVE, identifiable, null, identifiable));
      }
    }

  }

  @Override
  public boolean isEmpty() {
    return size == 0;
  }

  @Override
  public Iterator<OIdentifiable> iterator() {
    doDeserialization();

    return new EntriesIterator(convertToRecord);
  }

  @Override
  public Iterator<OIdentifiable> rawIterator() {
    doDeserialization();

    return new EntriesIterator(false);
  }

  @Override
  public void convertLinks2Records() {
    doDeserialization();

    final Map<ORID, IdentifiableContainer> convertedEntries = new HashMap<ORID, IdentifiableContainer>();

    for (Map.Entry<ORID, IdentifiableContainer> entry : entries.entrySet()) {
      final IdentifiableContainer container = entry.getValue();
      final ORecord record = container.getIdentifiable().getRecord();

      if (record != null)
        convertedEntries.put(entry.getKey(), new IdentifiableContainer(record, container.getCounter()));
      else
        convertedEntries.put(entry.getKey(), entry.getValue());
    }

    entries.clear();
    entries.putAll(convertedEntries);
  }

  @Override
  public boolean convertRecords2Links() {
    final Map<ORID, IdentifiableContainer> convertedEntries = new HashMap<ORID, IdentifiableContainer>();
    boolean identitytWasChanged = false;

    for (Map.Entry<ORID, IdentifiableContainer> entry : entries.entrySet()) {
      final IdentifiableContainer container = entry.getValue();

      if (container.getIdentifiable() instanceof ORecord) {
        final ORecord record = (ORecord) container.getIdentifiable();
        if (record.isDirty() || entry.getKey().getIdentity().isNew()) {
          record.removeIdentityChangeListener(this);
          record.save();
          record.addIdentityChangeListener(this);
          if (!record.getIdentity().equals(entry.getKey()))
            identitytWasChanged = true;

          convertedEntries.put(record.getIdentity(), new IdentifiableContainer(record.getIdentity(), container.getCounter()));
        }
      } else
        convertedEntries.put(entry.getKey(), entry.getValue());
    }

    for (Map.Entry<OIdentifiable, OModifiableInteger> entry : newEntries.entrySet()) {
      if (entry.getKey() instanceof ORecord) {
        identitytWasChanged = true;

        final ORecord record = (ORecord) entry.getKey();
        record.removeIdentityChangeListener(this);
        record.save();
        record.addIdentityChangeListener(this);
        convertedEntries.put(record.getIdentity(), new IdentifiableContainer(record.getIdentity(), entry.getValue()));
      } else
        convertedEntries.put(entry.getKey().getIdentity(), new IdentifiableContainer(entry.getKey(), entry.getValue()));
    }

    if (identitytWasChanged) {
      newEntries.clear();
      entries.clear();
      entries.putAll(convertedEntries);
    }

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
    if (!deserialized)
      return "[size=" + size + "]";

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
    int size;

    if (!deserialized)
      size = serializedContent.length;
    else
      size = 2 * OIntegerSerializer.INT_SIZE;

    size += newEntries.size() * (OLinkSerializer.RID_SIZE + OIntegerSerializer.INT_SIZE);
    size += entries.size() * (OLinkSerializer.RID_SIZE + OIntegerSerializer.INT_SIZE);

    return size;
  }

  @Override
  public int getSerializedSize(byte[] stream, int offset) {
    return OIntegerSerializer.INSTANCE.deserialize(stream, offset) * (OLinkSerializer.RID_SIZE + OIntegerSerializer.INT_SIZE) + 2
        * OIntegerSerializer.INT_SIZE;
  }

  @Override
  public int serialize(byte[] stream, int offset) {
    convertRecords2Links();

    int entriesSize;
    if (!deserialized) {
      entriesSize = OIntegerSerializer.INSTANCE.deserialize(serializedContent, 0);
      System.arraycopy(serializedContent, 0, stream, offset, serializedContent.length);

			if (contentWasChanged) {
				entriesSize += entries.size();
				OIntegerSerializer.INSTANCE.serialize(entriesSize, stream, offset);
				OIntegerSerializer.INSTANCE.serialize(size, stream, offset + OIntegerSerializer.INT_SIZE);
				offset += serializedContent.length;
			} else {
				offset += serializedContent.length;
				return offset;
			}

    } else {
      OIntegerSerializer.INSTANCE.serialize(entries.size(), stream, offset);
      offset += OIntegerSerializer.INT_SIZE;

      OIntegerSerializer.INSTANCE.serialize(size, stream, offset);
      offset += OIntegerSerializer.INT_SIZE;
    }

    for (Map.Entry<ORID, IdentifiableContainer> entry : entries.entrySet()) {
      OLinkSerializer.INSTANCE.serialize(entry.getKey(), stream, offset);
      offset += OLinkSerializer.RID_SIZE;

      OIntegerSerializer.INSTANCE.serialize(entry.getValue().getCounter().intValue(), stream, offset);
      offset += OIntegerSerializer.INT_SIZE;
    }

    return offset;
  }

  @Override
  public int deserialize(byte[] stream, int offset) {
    final int contentSize = getSerializedSize(stream, offset);

    this.size = OIntegerSerializer.INSTANCE.deserialize(stream, offset + OIntegerSerializer.INT_SIZE);

    this.serializedContent = new byte[contentSize];
    System.arraycopy(stream, offset, this.serializedContent, 0, contentSize);
    deserialized = false;

    return offset + contentSize;
  }

  private void doDeserialization() {
    if (deserialized)
      return;

    int offset = 0;
    int entriesSize = OIntegerSerializer.INSTANCE.deserialize(serializedContent, offset);
    offset += 2 * OIntegerSerializer.INT_SIZE;

    for (int i = 0; i < entriesSize; i++) {
      ORID rid = OLinkSerializer.INSTANCE.deserialize(serializedContent, offset);
      offset += OLinkSerializer.RID_SIZE;

      int counter = OIntegerSerializer.INSTANCE.deserialize(serializedContent, offset);
      offset += OIntegerSerializer.INT_SIZE;

      assert counter > 0;

      if (contentWasChanged) {
        final IdentifiableContainer container = entries.get(rid);
        if (container != null)
          container.counter.setValue(container.counter.getValue() + counter);
        else
          entries.put(rid, new IdentifiableContainer(rid, new OModifiableInteger(counter)));
      } else
        entries.put(rid, new IdentifiableContainer(rid, new OModifiableInteger(counter)));
    }

    deserialized = true;
  }

  @Override
  public void requestDelete() {
  }

  @Override
  public void onIdentityChanged(ORID prevRid, ORecord<?> record) {
    if (!prevRid.isValid()) {
      final OModifiableInteger counter = newEntries.remove(record);
      if (counter != null)
        entries.put(record.getIdentity(), new IdentifiableContainer(record, counter));
    } else {
      final IdentifiableContainer container = entries.remove(prevRid);
      if (container != null)
        entries.put(record.getIdentity(), container);
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
    private Iterator<Map.Entry<ORID, IdentifiableContainer>>       entriesIterator;

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

      if (newEntriesIterator.hasNext()) {
        final Map.Entry<OIdentifiable, OModifiableInteger> nextEntry = newEntriesIterator.next();

        currentCounter = 1;
        currentFinalCounter = nextEntry.getValue().intValue();
        currentValue = nextEntry.getKey();
      } else {
        final Map.Entry<ORID, IdentifiableContainer> nextEntry = entriesIterator.next();

        currentCounter = 1;
        currentFinalCounter = nextEntry.getValue().getCounter().intValue();
        currentValue = nextEntry.getValue().getIdentifiable();
      }

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
        final IdentifiableContainer container = entries.get(currentValue.getIdentity());
        final OModifiableInteger counter = container.getCounter();
        counter.decrement();

        if (counter.intValue() < 1) {
          entries.remove(currentValue.getIdentity());

          if (currentValue instanceof ORecord) {
            ORecord record = (ORecord) currentValue;
            record.removeIdentityChangeListener(OEmbeddedRidBag.this);
          }
          entriesIterator = entries.tailMap(currentValue.getIdentity(), false).entrySet().iterator();
        }
      } else {
        newEntries.remove(currentValue);
        if (currentValue instanceof ORecord) {
          ORecord record = (ORecord) currentValue;
          record.removeIdentityChangeListener(OEmbeddedRidBag.this);
        }
      }

      size--;
      contentWasChanged = true;

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

  private static final class IdentifiableContainer {
    private final OIdentifiable      identifiable;
    private final OModifiableInteger counter;

    private IdentifiableContainer(OIdentifiable identifiable, OModifiableInteger counter) {
      this.identifiable = identifiable;
      this.counter = counter;
    }

    private OIdentifiable getIdentifiable() {
      return identifiable;
    }

    private OModifiableInteger getCounter() {
      return counter;
    }
  }
}
