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
package com.orientechnologies.orient.core.db.record.ridbag.embedded;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.util.OCommonConst;
import com.orientechnologies.common.util.OResettable;
import com.orientechnologies.common.util.OSizeable;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.OMultiValueChangeEvent;
import com.orientechnologies.orient.core.db.record.OMultiValueChangeTimeLine;
import com.orientechnologies.orient.core.db.record.ORecordElement;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBagDelegate;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.OSimpleMultiValueTracker;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OLinkSerializer;
import com.orientechnologies.orient.core.storage.ridbag.sbtree.Change;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.NavigableMap;
import java.util.NoSuchElementException;
import java.util.UUID;

public class OEmbeddedRidBag implements ORidBagDelegate {
  private boolean contentWasChanged = false;

  private Object[] entries = OCommonConst.EMPTY_OBJECT_ARRAY;
  private int entriesLength = 0;

  private boolean convertToRecord = true;
  private int size = 0;

  private transient ORecordElement owner;

  private boolean dirty = false;
  private boolean transactionDirty = false;

  private OSimpleMultiValueTracker<OIdentifiable, OIdentifiable> tracker =
      new OSimpleMultiValueTracker<>(this);

  @Override
  public void setSize(int size) {
    this.size = size;
  }

  private static enum Tombstone {
    TOMBSTONE
  }

  public Object[] getEntries() {
    return entries;
  }

  private final class EntriesIterator implements Iterator<OIdentifiable>, OResettable, OSizeable {
    private final boolean convertToRecord;
    private int currentIndex = -1;
    private int nextIndex = -1;
    private boolean currentRemoved;

    private EntriesIterator(boolean convertToRecord) {
      reset();
      this.convertToRecord = convertToRecord;
    }

    @Override
    public boolean hasNext() {
      // we may remove items in ridbag during iteration so we need to be sure that pointed item is
      // not removed.
      if (nextIndex > -1) {
        if (entries[nextIndex] instanceof OIdentifiable) return true;

        nextIndex = nextIndex();
      }

      return nextIndex > -1;
    }

    @Override
    public OIdentifiable next() {
      currentRemoved = false;

      currentIndex = nextIndex;
      if (currentIndex == -1) throw new NoSuchElementException();

      Object nextValue = entries[currentIndex];

      // we may remove items in ridbag during iteration so we need to be sure that pointed item is
      // not removed.
      if (!(nextValue instanceof OIdentifiable)) {
        nextIndex = nextIndex();

        currentIndex = nextIndex;
        if (currentIndex == -1) throw new NoSuchElementException();

        nextValue = entries[currentIndex];
      }

      if (!convertToRecord && nextValue != null) {
        if (((OIdentifiable) nextValue).getIdentity().isPersistent())
          entries[currentIndex] = ((OIdentifiable) nextValue).getIdentity();
      }

      nextIndex = nextIndex();

      final OIdentifiable identifiable = (OIdentifiable) nextValue;
      if (convertToRecord) return identifiable.getRecord();

      return identifiable;
    }

    @Override
    public void remove() {
      if (currentRemoved)
        throw new IllegalStateException("Current element has already been removed");

      if (currentIndex == -1)
        throw new IllegalStateException("Next method was not called for given iterator");

      currentRemoved = true;

      final OIdentifiable nextValue = (OIdentifiable) entries[currentIndex];
      entries[currentIndex] = Tombstone.TOMBSTONE;

      size--;
      contentWasChanged = true;
      removeEvent(nextValue);
    }

    protected void swapValueOnCurrent(OIdentifiable newValue) {
      if (currentRemoved)
        throw new IllegalStateException("Current element has already been removed");

      if (currentIndex == -1)
        throw new IllegalStateException("Next method was not called for given iterator");

      final OIdentifiable oldValue = (OIdentifiable) entries[currentIndex];
      entries[currentIndex] = newValue;

      contentWasChanged = true;

      updateEvent(oldValue, oldValue, newValue);
    }

    @Override
    public void reset() {
      currentIndex = -1;
      nextIndex = -1;
      currentRemoved = false;

      nextIndex = nextIndex();
    }

    @Override
    public int size() {
      return size;
    }

    private int nextIndex() {
      for (int i = currentIndex + 1; i < entriesLength; i++) {
        Object entry = entries[i];
        if (entry instanceof OIdentifiable) return i;
      }

      return -1;
    }
  }

  @Override
  public ORecordElement getOwner() {
    return owner;
  }

  @Override
  public boolean contains(OIdentifiable identifiable) {
    if (identifiable == null) return false;

    for (int i = 0; i < entriesLength; i++) {
      if (identifiable.equals(entries[i])) return true;
    }

    return false;
  }

  @Override
  public void setOwner(ORecordElement owner) {
    if (owner != null && this.owner != null && !this.owner.equals(owner)) {
      throw new IllegalStateException(
          "This data structure is owned by document "
              + owner
              + " if you want to use it in other document create new rid bag instance and copy content of current one.");
    }
    if (this.owner != null) {
      for (int i = 0; i < entriesLength; i++) {
        final Object entry = entries[i];
        if (entry instanceof OIdentifiable) {
          ORecordInternal.unTrack(this.owner, (OIdentifiable) entry);
        }
      }
    }

    this.owner = owner;
    if (this.owner != null) {
      for (int i = 0; i < entriesLength; i++) {
        final Object entry = entries[i];
        if (entry instanceof OIdentifiable) {
          ORecordInternal.track(this.owner, (OIdentifiable) entry);
        }
      }
    }
  }

  @Override
  public void addAll(Collection<OIdentifiable> values) {
    for (OIdentifiable value : values) add(value);
  }

  @Override
  public void add(final OIdentifiable identifiable) {
    if (identifiable == null) {
      throw new IllegalArgumentException("Impossible to add a null identifiable in a ridbag");
    }
    addEntry(identifiable);

    size++;
    contentWasChanged = true;

    addEvent(identifiable, identifiable);
  }

  public OEmbeddedRidBag copy() {
    final OEmbeddedRidBag copy = new OEmbeddedRidBag();
    copy.contentWasChanged = contentWasChanged;
    copy.entries = entries;
    copy.entriesLength = entriesLength;
    copy.convertToRecord = convertToRecord;
    copy.size = size;
    copy.owner = owner;
    copy.tracker = this.tracker;
    return copy;
  }

  @Override
  public void remove(OIdentifiable identifiable) {

    if (removeEntry(identifiable)) {
      size--;
      contentWasChanged = true;

      removeEvent(identifiable);
    }
  }

  /**
   * for internal use only
   *
   * @param index
   * @param newValue
   * @return
   */
  public boolean swap(int index, OIdentifiable newValue) {
    EntriesIterator iter = (EntriesIterator) rawIterator();
    int currIndex = 0;
    while (iter.hasNext()) {
      iter.next();
      if (index == currIndex) {
        iter.swapValueOnCurrent(newValue);
        return true;
      }
      currIndex++;
    }
    return false;
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
    for (int i = 0; i < entriesLength; i++) {
      final Object entry = entries[i];

      if (entry instanceof OIdentifiable) {
        final OIdentifiable identifiable = (OIdentifiable) entry;
        ORecord record = identifiable.getRecord();
        if (record != null) {
          if (this.owner != null) {
            ORecordInternal.unTrack(this.owner, identifiable);
            ORecordInternal.track(this.owner, record);
          }
          entries[i] = record;
        }
      }
    }
  }

  @Override
  public boolean convertRecords2Links() {
    for (int i = 0; i < entriesLength; i++) {
      final Object entry = entries[i];

      if (entry instanceof OIdentifiable) {
        final OIdentifiable identifiable = (OIdentifiable) entry;
        if (identifiable instanceof ORecord) {
          final ORecord record = (ORecord) identifiable;

          entries[i] = record.getIdentity();
        }
      }
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
    if (size < 10) {
      final StringBuilder sb = new StringBuilder(256);
      sb.append('[');
      for (final Iterator<OIdentifiable> it = this.rawIterator(); it.hasNext(); ) {
        try {
          OIdentifiable e = it.next();
          if (e != null) {
            if (sb.length() > 1) sb.append(", ");

            sb.append(e.getIdentity());
          }
        } catch (NoSuchElementException ignore) {
          // IGNORE THIS
        }
      }
      return sb.append(']').toString();

    } else return "[size=" + size + "]";
  }

  @Override
  public Object returnOriginalState(
      List<OMultiValueChangeEvent<OIdentifiable, OIdentifiable>> multiValueChangeEvents) {
    final OEmbeddedRidBag reverted = new OEmbeddedRidBag();
    for (OIdentifiable identifiable : this) reverted.add(identifiable);

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
    int size;

    size = OIntegerSerializer.INT_SIZE;

    size += this.size * OLinkSerializer.RID_SIZE;

    return size;
  }

  @Override
  public int getSerializedSize(byte[] stream, int offset) {
    return OIntegerSerializer.INSTANCE.deserializeLiteral(stream, offset) * OLinkSerializer.RID_SIZE
        + OIntegerSerializer.INT_SIZE;
  }

  @Override
  public int serialize(byte[] stream, int offset, UUID ownerUuid) {
    OIntegerSerializer.INSTANCE.serializeLiteral(size, stream, offset);
    offset += OIntegerSerializer.INT_SIZE;
    ODatabaseDocumentInternal db = ODatabaseRecordThreadLocal.instance().getIfDefined();
    final int totEntries = entries.length;
    for (int i = 0; i < totEntries; ++i) {
      final Object entry = entries[i];
      if (entry instanceof OIdentifiable) {
        OIdentifiable link = (OIdentifiable) entry;
        final ORID rid = link.getIdentity();
        if (db != null && !db.isClosed() && db.getTransaction().isActive()) {
          if (!link.getIdentity().isPersistent()) {
            link = db.getTransaction().getRecord(link.getIdentity());
          }
        }

        if (link == null)
          throw new OSerializationException("Found null entry in ridbag with rid=" + rid);

        entries[i] = link.getIdentity();
        OLinkSerializer.INSTANCE.serialize(link, stream, offset);
        offset += OLinkSerializer.RID_SIZE;
      }
    }

    return offset;
  }

  @Override
  public int deserialize(final byte[] stream, int offset) {
    this.size = OIntegerSerializer.INSTANCE.deserializeLiteral(stream, offset);
    int entriesSize = OIntegerSerializer.INSTANCE.deserializeLiteral(stream, offset);
    offset += OIntegerSerializer.INT_SIZE;

    for (int i = 0; i < entriesSize; i++) {
      ORID rid = OLinkSerializer.INSTANCE.deserialize(stream, offset);
      offset += OLinkSerializer.RID_SIZE;

      OIdentifiable identifiable = null;
      if (rid.isTemporary()) identifiable = rid.getRecord();

      if (identifiable == null) identifiable = rid;

      if (identifiable == null)
        OLogManager.instance()
            .warn(this, "Found null reference during ridbag deserialization (rid=%s)", rid);
      else addInternal(identifiable);
    }

    return offset;
  }

  @Override
  public void requestDelete() {}

  @Override
  public Class<?> getGenericClass() {
    return OIdentifiable.class;
  }

  public boolean addInternal(final OIdentifiable identifiable) {
    addEntry(identifiable);
    if (this.owner != null) ORecordInternal.track(this.owner, identifiable);
    return true;
  }

  public void addEntry(final OIdentifiable identifiable) {
    if (entries.length == entriesLength) {
      if (entriesLength == 0) {
        final int cfgValue =
            OGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.getValueAsInteger();
        entries = new Object[cfgValue > 0 ? Math.min(cfgValue, 40) : 40];
      } else {
        final Object[] oldEntries = entries;
        entries = new Object[entries.length << 1];
        System.arraycopy(oldEntries, 0, entries, 0, oldEntries.length);
      }
    }
    entries[entriesLength] = identifiable;
    entriesLength++;
  }

  private boolean removeEntry(OIdentifiable identifiable) {
    int i = 0;
    for (; i < entriesLength; i++) {
      final Object entry = entries[i];
      if (entry.equals(identifiable)) {
        entries[i] = Tombstone.TOMBSTONE;
        break;
      }
    }

    return i < entriesLength;
  }

  @Override
  public NavigableMap<OIdentifiable, Change> getChanges() {
    return null;
  }

  @Override
  public void replace(OMultiValueChangeEvent<Object, Object> event, Object newValue) {
    // do nothing not needed
  }

  private void addEvent(final OIdentifiable key, final OIdentifiable identifiable) {
    if (this.owner != null) ORecordInternal.track(this.owner, identifiable);

    if (tracker.isEnabled()) {
      tracker.add(key, identifiable);
    } else {
      setDirty();
    }
  }

  private void updateEvent(OIdentifiable key, OIdentifiable oldValue, OIdentifiable newValue) {
    if (this.owner != null) ORecordInternal.unTrack(this.owner, oldValue);

    if (tracker.isEnabled()) {
      tracker.updated(key, oldValue, newValue);
    } else {
      setDirty();
    }
  }

  private void removeEvent(OIdentifiable removed) {
    if (this.owner != null) ORecordInternal.unTrack(this.owner, removed);

    if (tracker.isEnabled()) {
      tracker.remove(removed, removed);
    } else {
      setDirty();
    }
  }

  public void enableTracking(final ORecordElement parent) {
    if (!tracker.isEnabled()) {
      tracker.enable();
    }
  }

  public void disableTracking(final ORecordElement document) {
    if (tracker.isEnabled()) {
      tracker.disable();
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
    return tracker.getTransactionTimeLine();
  }
}
