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

package com.orientechnologies.orient.core.db.record.ridbag.sbtree;

import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.common.types.OModifiableInteger;
import com.orientechnologies.common.util.OResettable;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.OMultiValueChangeEvent;
import com.orientechnologies.orient.core.db.record.OMultiValueChangeListener;
import com.orientechnologies.orient.core.db.record.ORecordElement;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBagDelegate;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.sbtree.OTreeInternal;
import com.orientechnologies.orient.core.index.sbtreebonsai.local.OBonsaiBucketPointer;
import com.orientechnologies.orient.core.index.sbtreebonsai.local.OSBTreeBonsai;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.ORecordSerializationContext;
import com.orientechnologies.orient.core.storage.impl.local.paginated.ORidBagDeleteSerializationOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.ORidBagUpdateSerializationOperation;

/**
 * Persistent Set<OIdentifiable> implementation that uses the SBTree to handle entries in persistent way.
 * 
 * @author <a href="mailto:enisher@gmail.com">Artem Orobets</a>
 */
public class OSBTreeRidBag implements ORidBagDelegate {
  private OBonsaiCollectionPointer                                     collectionPointer;
  private final OSBTreeCollectionManager                               collectionManager;

  private final NavigableMap<OIdentifiable, Change>                    changes             = new ConcurrentSkipListMap<OIdentifiable, Change>();

  /**
   * Entries with not valid id.
   */
  private final IdentityHashMap<OIdentifiable, OModifiableInteger>     newEntries          = new IdentityHashMap<OIdentifiable, OModifiableInteger>();

  private int                                                          size;

  private boolean                                                      autoConvertToRecord = true;

  private Set<OMultiValueChangeListener<OIdentifiable, OIdentifiable>> changeListeners     = Collections
                                                                                               .newSetFromMap(new WeakHashMap<OMultiValueChangeListener<OIdentifiable, OIdentifiable>, Boolean>());
  private transient ORecord<?>                                         owner;

  public OSBTreeRidBag() {
    collectionPointer = null;

    collectionManager = ODatabaseRecordThreadLocal.INSTANCE.get().getSbTreeCollectionManager();
  }

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

  private OSBTreeBonsai<OIdentifiable, Integer> loadTree() {
    if (collectionPointer == null)
      return null;

    return collectionManager.loadSBTree(collectionPointer);
  }

  private void releaseTree() {
    if (collectionPointer == null)
      return;

    collectionManager.releaseSBTree(collectionPointer);
  }

  public Iterator<OIdentifiable> iterator() {
    return new RIDBagIterator(new IdentityHashMap<OIdentifiable, OModifiableInteger>(newEntries), changes,
        collectionPointer != null ? new SBTreeMapEntryIterator(1000) : null, autoConvertToRecord);
  }

  @Override
  public Iterator<OIdentifiable> rawIterator() {
    return new RIDBagIterator(new IdentityHashMap<OIdentifiable, OModifiableInteger>(newEntries), changes,
        collectionPointer != null ? new SBTreeMapEntryIterator(1000) : null, false);
  }

  @Override
  public void convertLinks2Records() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean convertRecords2Links() {
    final Map<OIdentifiable, Change> newChangedValues = new HashMap<OIdentifiable, Change>();
    for (Map.Entry<OIdentifiable, Change> entry : changes.entrySet()) {
      OIdentifiable identifiable = entry.getKey();
      if (identifiable instanceof ORecord) {
        ORID identity = identifiable.getIdentity();
        ORecord record = (ORecord) identifiable;
        if (identity.isNew() || record.isDirty()) {
          record.save();
          identity = record.getIdentity();
        }

        newChangedValues.put(identity, entry.getValue());
      } else
        newChangedValues.put(entry.getKey().getIdentity(), entry.getValue());
    }

    for (Map.Entry<OIdentifiable, Change> entry : newChangedValues.entrySet()) {
      if (entry.getKey() instanceof ORecord) {
        ORecord record = (ORecord) entry.getKey();
        record.save();

        newChangedValues.put(record, entry.getValue());
      } else
        // TODO return before updating all changed values
        return false;
    }

    newEntries.clear();

    changes.clear();
    changes.putAll(newChangedValues);

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
    return convertRecords2Links();
  }

  public void addAll(Collection<OIdentifiable> values) {
    for (OIdentifiable identifiable : values) {
      add(identifiable);
    }
  }

  public void add(OIdentifiable identifiable) {
    if (identifiable.getIdentity().isValid()) {
      Change counter = changes.get(identifiable);
      if (counter == null)
        changes.put(identifiable, new DiffChange(1));
      else {
        if (counter.isUndefined()) {
          counter = getAbsoluteValue(identifiable);
          changes.put(identifiable, counter);
        }
        counter.increment();
      }
    } else {
      OModifiableInteger counter = newEntries.get(identifiable);
      if (counter == null)
        newEntries.put(identifiable, new OModifiableInteger(1));
      else
        counter.increment();
    }

    if (size >= 0)
      size++;

    fireCollectionChangedEvent(new OMultiValueChangeEvent<OIdentifiable, OIdentifiable>(OMultiValueChangeEvent.OChangeType.ADD,
        identifiable, identifiable));
  }

  private AbsoluteChange getAbsoluteValue(OIdentifiable identifiable) {
    final OSBTreeBonsai<OIdentifiable, Integer> tree = loadTree();
    try {
      final Integer oldValue;
      if (tree == null)
        oldValue = null;
      else
        oldValue = tree.get(identifiable);

      final Change change = changes.get(identifiable);

      return new AbsoluteChange(change.applyTo(oldValue));
    } finally {
      releaseTree();
    }
  }

  public void remove(OIdentifiable identifiable) {
    if (removeFromNewEntries(identifiable)) {
      if (size >= 0)
        size--;
    } else {
      final Change counter = changes.get(identifiable);
      if (counter == null) {
        // Not persistent keys can only be in changes or newEntries
        if (identifiable.getIdentity().isPersistent()) {
          changes.put(identifiable, new DiffChange(-1));
          size = -1;
        } else
          // Return immediately to prevent firing of event
          return;
      } else {
        counter.decrement();

        if (size >= 0)
          if (counter.isUndefined())
            size = -1;
          else
            size--;
      }
    }

    fireCollectionChangedEvent(new OMultiValueChangeEvent<OIdentifiable, OIdentifiable>(OMultiValueChangeEvent.OChangeType.REMOVE,
        identifiable, null, identifiable));
  }

  public int size() {
    if (size >= 0)
      return size;
    else {
      return updateSize();
    }
  }

  /**
   * Recalculates real bag size.
   * 
   * @return real size
   */
  private int updateSize() {
    int size = 0;
    if (collectionPointer != null) {
      final OSBTreeBonsai<OIdentifiable, Integer> tree = loadTree();
      try {
        size = tree.getRealBagSize(changes);
      } finally {
        releaseTree();
      }
    } else {
      for (Change change : changes.values()) {
        size += change.applyTo(0);
      }
    }

    for (OModifiableInteger diff : newEntries.values()) {
      size += diff.getValue();
    }

    this.size = size;
    return size;
  }

  @Override
  public String toString() {
    return "[size=" + size + "]";
  }

  public boolean isEmpty() {
    return size() == 0;
  }

  public void addChangeListener(final OMultiValueChangeListener<OIdentifiable, OIdentifiable> changeListener) {
    changeListeners.add(changeListener);
  }

  public void removeRecordChangeListener(final OMultiValueChangeListener<OIdentifiable, OIdentifiable> changeListener) {
    changeListeners.remove(changeListener);
  }

  @Override
  public Class<?> getGenericClass() {
    return OIdentifiable.class;
  }

  @Override
  public Object returnOriginalState(List<OMultiValueChangeEvent<OIdentifiable, OIdentifiable>> multiValueChangeEvents) {
    final OSBTreeRidBag reverted = new OSBTreeRidBag();
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

  protected void fireCollectionChangedEvent(final OMultiValueChangeEvent<OIdentifiable, OIdentifiable> event) {
    for (final OMultiValueChangeListener<OIdentifiable, OIdentifiable> changeListener : changeListeners) {
      if (changeListener != null)
        changeListener.onAfterRecordChanged(event);
    }
  }

  @Override
  public int getSerializedSize() {
    return 2 * OLongSerializer.LONG_SIZE + 2 * OIntegerSerializer.INT_SIZE;
  }

  @Override
  public int getSerializedSize(byte[] stream, int offset) {
    return 2 * OLongSerializer.LONG_SIZE + 2 * OIntegerSerializer.INT_SIZE;
  }

  @Override
  public int serialize(byte[] stream, int offset) {
    for (OIdentifiable identifiable : changes.keySet()) {
      if (identifiable instanceof ORecord) {
        final ORID identity = identifiable.getIdentity();
        final ORecord record = (ORecord) identifiable;
        if (identity.isNew() || record.isDirty()) {
          record.save();
        }
      }
    }

    for (Map.Entry<OIdentifiable, OModifiableInteger> entry : newEntries.entrySet()) {
      OIdentifiable identifiable = entry.getKey();
      assert identifiable instanceof ORecord;
      ((ORecord) identifiable).save();
      Change c = changes.get(identifiable);

      final int delta = entry.getValue().intValue();
      if (c == null)
        changes.put(identifiable, new DiffChange(delta));
      else
        c.applyDiff(delta);
    }
    newEntries.clear();

    final ORecordSerializationContext context = ORecordSerializationContext.getContext();

    // make sure that we really save underlying record.
    if (collectionPointer == null) {
      if (context != null) {
        final int clusterId = getHighLevelDocClusterId();
        assert clusterId > -1;
        final OSBTreeBonsai<OIdentifiable, Integer> treeBonsai = ODatabaseRecordThreadLocal.INSTANCE.get()
            .getSbTreeCollectionManager().createSBTree(clusterId);
        try {
          collectionPointer = new OBonsaiCollectionPointer(treeBonsai.getFileId(), treeBonsai.getRootBucketPointer());
        } finally {
          releaseTree();
        }
      }
    }

    OBonsaiCollectionPointer collectionPointer;
    if (this.collectionPointer != null)
      collectionPointer = this.collectionPointer;
    else
      collectionPointer = new OBonsaiCollectionPointer(-1, new OBonsaiBucketPointer(-1, -1));

    OLongSerializer.INSTANCE.serialize(collectionPointer.getFileId(), stream, offset);
    offset += OLongSerializer.LONG_SIZE;

    OBonsaiBucketPointer rootPointer = collectionPointer.getRootPointer();
    OLongSerializer.INSTANCE.serialize(rootPointer.getPageIndex(), stream, offset);
    offset += OLongSerializer.LONG_SIZE;

    OIntegerSerializer.INSTANCE.serialize(rootPointer.getPageOffset(), stream, offset);
    offset += OIntegerSerializer.INT_SIZE;

    OIntegerSerializer.INSTANCE.serialize(size, stream, offset);
    offset += OIntegerSerializer.INT_SIZE;

    if (context != null)
      context.push(new ORidBagUpdateSerializationOperation(changes, collectionPointer));

    return offset;
  }

  private int getHighLevelDocClusterId() {
    ORecordElement owner = this.owner;
    while (owner != null && owner.getOwner() != null) {
      owner = owner.getOwner();
    }

    if (owner instanceof OIdentifiable)
      return ((OIdentifiable) owner).getIdentity().getClusterId();

    return -1;
  }

  @Override
  public void requestDelete() {
    final ORecordSerializationContext context = ORecordSerializationContext.getContext();
    if (context != null && collectionPointer != null)
      context.push(new ORidBagDeleteSerializationOperation(collectionPointer, this));
  }

  public void confirmDelete() {
    collectionPointer = null;
    changes.clear();
    newEntries.clear();
    size = 0;
    changeListeners.clear();
  }

  @Override
  public int deserialize(byte[] stream, int offset) {
    final long fileId = OLongSerializer.INSTANCE.deserialize(stream, offset);
    offset += OLongSerializer.LONG_SIZE;

    final long pageIndex = OLongSerializer.INSTANCE.deserialize(stream, offset);
    offset += OLongSerializer.LONG_SIZE;

    final int pageOffset = OIntegerSerializer.INSTANCE.deserialize(stream, offset);
    offset += OIntegerSerializer.INT_SIZE;

    final int size = OIntegerSerializer.INSTANCE.deserialize(stream, offset);
    offset += OIntegerSerializer.INT_SIZE;

    collectionPointer = new OBonsaiCollectionPointer(fileId, new OBonsaiBucketPointer(pageIndex, pageOffset));
    this.size = size;

    return offset;
  }

  /**
   * Removes entry with given key from {@link #newEntries}.
   * 
   * @param identifiable
   *          key to remove
   * @return true if entry have been removed
   */
  private boolean removeFromNewEntries(OIdentifiable identifiable) {
    OModifiableInteger counter = newEntries.get(identifiable);
    if (counter == null)
      return false;
    else {
      if (counter.getValue() == 1)
        newEntries.remove(identifiable);
      else
        counter.decrement();
      return true;
    }
  }

  private final class RIDBagIterator implements Iterator<OIdentifiable>, OResettable {
    private final NavigableMap<OIdentifiable, Change>              changedValues;
    private Iterator<Map.Entry<OIdentifiable, OModifiableInteger>> newEntryIterator;
    private Iterator<Map.Entry<OIdentifiable, Change>>             changedValuesIterator;
    private final SBTreeMapEntryIterator                           sbTreeIterator;

    private Map.Entry<OIdentifiable, Change>                       nextChange;
    private Map.Entry<OIdentifiable, Integer>                      nextSBTreeEntry;

    private OIdentifiable                                          currentValue;
    private int                                                    currentFinalCounter;

    private int                                                    currentCounter;

    private final boolean                                          convertToRecord;

    private boolean                                                currentRemoved;

    private RIDBagIterator(IdentityHashMap<OIdentifiable, OModifiableInteger> newEntries,
        NavigableMap<OIdentifiable, Change> changedValues, SBTreeMapEntryIterator sbTreeIterator, boolean convertToRecord) {
      newEntryIterator = newEntries.entrySet().iterator();
      this.changedValues = changedValues;
      this.convertToRecord = convertToRecord;
      this.changedValuesIterator = changedValues.entrySet().iterator();
      this.sbTreeIterator = sbTreeIterator;

      nextChange = nextChangedNotRemovedEntry(changedValuesIterator);

      if (sbTreeIterator != null)
        nextSBTreeEntry = nextChangedNotRemovedSBTreeEntry(sbTreeIterator);
    }

    @Override
    public boolean hasNext() {
      return newEntryIterator.hasNext() || nextChange != null || nextSBTreeEntry != null
          || (currentValue != null && currentCounter < currentFinalCounter);
    }

    @Override
    public OIdentifiable next() {
      currentRemoved = false;
      if (currentCounter < currentFinalCounter) {
        currentCounter++;
        return currentValue;
      }

      if (newEntryIterator.hasNext()) {
        Map.Entry<OIdentifiable, OModifiableInteger> entry = newEntryIterator.next();
        currentValue = entry.getKey();
        currentFinalCounter = entry.getValue().intValue();
        currentCounter = 1;
        return currentValue;
      }

      // TODO when there is some removed entries from tree iterator still iterate thru them
      if (nextChange != null && nextSBTreeEntry != null) {
        if (nextChange.getKey().compareTo(nextSBTreeEntry.getKey()) < 0) {
          currentValue = nextChange.getKey();
          currentFinalCounter = nextChange.getValue().applyTo(0);
          currentCounter = 1;

          nextChange = nextChangedNotRemovedEntry(changedValuesIterator);

          // TODO if condition is always true
        } else if (nextChange.getKey().compareTo(nextSBTreeEntry.getKey()) >= 0) {
          currentValue = nextSBTreeEntry.getKey();
          currentFinalCounter = nextSBTreeEntry.getValue();
          currentCounter = 1;

          nextSBTreeEntry = nextChangedNotRemovedSBTreeEntry(sbTreeIterator);
          if (nextChange != null && nextChange.getKey().equals(currentValue))
            nextChange = nextChangedNotRemovedEntry(changedValuesIterator);
        }
      } else if (nextChange != null) {
        currentValue = nextChange.getKey();
        currentFinalCounter = nextChange.getValue().applyTo(0);
        currentCounter = 1;

        nextChange = nextChangedNotRemovedEntry(changedValuesIterator);
      } else if (nextSBTreeEntry != null) {
        currentValue = nextSBTreeEntry.getKey();
        currentFinalCounter = nextSBTreeEntry.getValue();
        currentCounter = 1;

        nextSBTreeEntry = nextChangedNotRemovedSBTreeEntry(sbTreeIterator);
      } else
        throw new NoSuchElementException();

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

      if (removeFromNewEntries(currentValue)) {
        if (size >= 0)
          size--;
      } else {
        Change counter = changedValues.get(currentValue);
        if (counter != null) {
          counter.decrement();
          if (size >= 0)
            if (counter.isUndefined())
              size = -1;
            else
              size--;
        } else {
          if (nextChange != null) {
            changedValues.put(currentValue, new DiffChange(-1));
            changedValuesIterator = changedValues.tailMap(nextChange.getKey(), false).entrySet().iterator();
          } else {
            changedValues.put(currentValue, new DiffChange(-1));
          }

          size = -1;
        }
      }

      fireCollectionChangedEvent(new OMultiValueChangeEvent<OIdentifiable, OIdentifiable>(
          OMultiValueChangeEvent.OChangeType.REMOVE, currentValue, null, currentValue));
      currentRemoved = true;
    }

    private Map.Entry<OIdentifiable, Change> nextChangedNotRemovedEntry(Iterator<Map.Entry<OIdentifiable, Change>> iterator) {
      Map.Entry<OIdentifiable, Change> entry;

      while (iterator.hasNext()) {
        entry = iterator.next();
        // TODO workaround
        if (entry.getValue().applyTo(0) > 0)
          return entry;
      }

      return null;
    }

    @Override
    public void reset() {
      newEntryIterator = newEntries.entrySet().iterator();

      this.changedValuesIterator = changedValues.entrySet().iterator();
      if (sbTreeIterator != null)
        this.sbTreeIterator.reset();

      nextChange = nextChangedNotRemovedEntry(changedValuesIterator);

      if (sbTreeIterator != null)
        nextSBTreeEntry = nextChangedNotRemovedSBTreeEntry(sbTreeIterator);
    }
  }

  private Map.Entry<OIdentifiable, Integer> nextChangedNotRemovedSBTreeEntry(Iterator<Map.Entry<OIdentifiable, Integer>> iterator) {
    while (iterator.hasNext()) {
      final Map.Entry<OIdentifiable, Integer> entry = iterator.next();
      final Change change = changes.get(entry.getKey());
      if (change == null)
        return entry;

      final int newValue = change.applyTo(entry.getValue());

      if (newValue > 0)
        return new Map.Entry<OIdentifiable, Integer>() {
          @Override
          public OIdentifiable getKey() {
            return entry.getKey();
          }

          @Override
          public Integer getValue() {
            return newValue;
          }

          @Override
          public Integer setValue(Integer value) {
            throw new UnsupportedOperationException();
          }
        };
    }

    return null;
  }

  private final class SBTreeMapEntryIterator implements Iterator<Map.Entry<OIdentifiable, Integer>>, OResettable {
    private LinkedList<Map.Entry<OIdentifiable, Integer>> preFetchedValues;
    private OIdentifiable                                 firstKey;

    private final int                                     prefetchSize;

    public SBTreeMapEntryIterator(int prefetchSize) {
      this.prefetchSize = prefetchSize;

      init();
    }

    private void prefetchData(boolean firstTime) {
      final OSBTreeBonsai<OIdentifiable, Integer> tree = loadTree();
      try {
        tree.loadEntriesMajor(firstKey, firstTime, new OTreeInternal.RangeResultListener<OIdentifiable, Integer>() {
          @Override
          public boolean addResult(final Map.Entry<OIdentifiable, Integer> entry) {
            preFetchedValues.add(new Map.Entry<OIdentifiable, Integer>() {
              @Override
              public OIdentifiable getKey() {
                return entry.getKey();
              }

              @Override
              public Integer getValue() {
                return entry.getValue();
              }

              @Override
              public Integer setValue(Integer v) {
                throw new UnsupportedOperationException("setValue");
              }
            });

            return preFetchedValues.size() <= prefetchSize;
          }
        });
      } finally {
        releaseTree();
      }

      if (preFetchedValues.isEmpty())
        preFetchedValues = null;
      else
        firstKey = preFetchedValues.getLast().getKey();
    }

    @Override
    public boolean hasNext() {
      return preFetchedValues != null;
    }

    @Override
    public Map.Entry<OIdentifiable, Integer> next() {
      final Map.Entry<OIdentifiable, Integer> entry = preFetchedValues.removeFirst();
      if (preFetchedValues.isEmpty())
        prefetchData(false);

      return entry;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void reset() {
      init();
    }

    private void init() {
      OSBTreeBonsai<OIdentifiable, Integer> tree = loadTree();
      try {
        if (tree.size() == 0) {
          this.preFetchedValues = null;
          return;
        }

        firstKey = tree.firstKey();
      } finally {
        releaseTree();
      }

      this.preFetchedValues = new LinkedList<Map.Entry<OIdentifiable, Integer>>();
      prefetchData(true);
    }
  }

  public static interface Change {
    void increment();

    void decrement();

    int applyTo(Integer value);

    /**
     * Checks if put increment operation can be safely performed.
     * 
     * @return true if increment operation can be safely performed.
     */
    boolean isUndefined();

    void applyDiff(int delta);

  }

  private static class DiffChange implements Change {
    private int delta;

    private DiffChange(int delta) {
      this.delta = delta;
    }

    @Override
    public void increment() {
      delta++;
    }

    @Override
    public void decrement() {
      delta--;
    }

    @Override
    public int applyTo(Integer value) {
      int result;
      if (value == null)
        result = delta;
      else
        result = value + delta;

      if (result < 0)
        result = 0;

      return result;
    }

    @Override
    public boolean isUndefined() {
      return delta < 0;
    }

    @Override
    public void applyDiff(int delta) {
      this.delta += delta;
    }
  }

  private static class AbsoluteChange implements Change {
    private int value;

    private AbsoluteChange(int value) {
      this.value = value;

      checkPositive();
    }

    @Override
    public void increment() {
      value++;
    }

    @Override
    public void decrement() {
      value--;

      checkPositive();
    }

    @Override
    public int applyTo(Integer value) {
      return this.value;
    }

    @Override
    public boolean isUndefined() {
      return true;
    }

    @Override
    public void applyDiff(int delta) {
      value += delta;

      checkPositive();
    }

    private final void checkPositive() {
      if (value < 0)
        value = 0;
    }
  }
}
