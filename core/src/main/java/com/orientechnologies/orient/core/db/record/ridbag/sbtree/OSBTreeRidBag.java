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

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.common.types.OModifiableInteger;
import com.orientechnologies.common.util.OResettable;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.OMultiValueChangeEvent;
import com.orientechnologies.orient.core.db.record.OMultiValueChangeListener;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBagDelegate;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.sbtree.OTreeInternal;
import com.orientechnologies.orient.core.index.sbtreebonsai.local.OBonsaiBucketPointer;
import com.orientechnologies.orient.core.index.sbtreebonsai.local.OSBTreeBonsai;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.serialization.OBase64Utils;
import com.orientechnologies.orient.core.storage.impl.local.paginated.ORecordSerializationContext;
import com.orientechnologies.orient.core.storage.impl.local.paginated.ORidBagDeleteSerializationOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.ORidBagUpdateSerializationOperation;

/**
 * Persistent Set<OIdentifiable> implementation that uses the SBTree to handle entries in persistent way.
 * 
 * @author <a href="mailto:enisher@gmail.com">Artem Orobets</a>
 */
public class OSBTreeRidBag implements ORidBagDelegate {
  private OBonsaiBucketPointer                                         rootPointer;
  private final OSBTreeCollectionManager                               collectionManager;

  private final NavigableMap<OIdentifiable, OModifiableInteger>        changedValues       = new ConcurrentSkipListMap<OIdentifiable, OModifiableInteger>();

  /**
   * Entries with not valid id.
   */
  private final IdentityHashMap<OIdentifiable, OModifiableInteger>     newEntries          = new IdentityHashMap<OIdentifiable, OModifiableInteger>();

  private int                                                          size;

  private boolean                                                      autoConvertToRecord = true;

  private Set<OMultiValueChangeListener<OIdentifiable, OIdentifiable>> changeListeners     = Collections
                                                                                               .newSetFromMap(new WeakHashMap<OMultiValueChangeListener<OIdentifiable, OIdentifiable>, Boolean>());

  public OSBTreeRidBag() {
    rootPointer = null;

    collectionManager = ODatabaseRecordThreadLocal.INSTANCE.get().getSbTreeCollectionManager();
  }

  private OSBTreeBonsai<OIdentifiable, Integer> loadTree() {
    if (rootPointer == null)
      return null;

    return collectionManager.loadSBTree(rootPointer);
  }

  private void releaseTree() {
    if (rootPointer == null)
      return;

    collectionManager.releaseSBTree(rootPointer);
  }

  public Iterator<OIdentifiable> iterator() {
    return new RIDBagIterator(new IdentityHashMap<OIdentifiable, OModifiableInteger>(newEntries), changedValues,
        rootPointer != null ? new SBTreeMapEntryIterator(1000) : null, autoConvertToRecord);
  }

  @Override
  public Iterator<OIdentifiable> rawIterator() {
    return new RIDBagIterator(new IdentityHashMap<OIdentifiable, OModifiableInteger>(newEntries), changedValues,
        rootPointer != null ? new SBTreeMapEntryIterator(1000) : null, false);
  }

  @Override
  public void convertLinks2Records() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean convertRecords2Links() {
    final Map<OIdentifiable, OModifiableInteger> newChangedValues = new HashMap<OIdentifiable, OModifiableInteger>();
    for (Map.Entry<OIdentifiable, OModifiableInteger> entry : changedValues.entrySet()) {
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

    for (Map.Entry<OIdentifiable, OModifiableInteger> entry : newChangedValues.entrySet()) {
      if (entry.getKey() instanceof ORecord) {
        ORecord record = (ORecord) entry.getKey();
        record.save();

        newChangedValues.put(record, entry.getValue());
      } else
        return false;
    }

    newEntries.clear();

    changedValues.clear();
    changedValues.putAll(newChangedValues);

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
      OModifiableInteger counter = changedValues.get(identifiable);
      if (counter == null)
        changedValues.put(identifiable, new OModifiableInteger(1));
      else
        counter.increment();
    } else {
      OModifiableInteger counter = newEntries.get(identifiable);
      if (counter == null)
        newEntries.put(identifiable, new OModifiableInteger(1));
      else
        counter.increment();
    }

    size++;

    fireCollectionChangedEvent(new OMultiValueChangeEvent<OIdentifiable, OIdentifiable>(OMultiValueChangeEvent.OChangeType.ADD,
        identifiable, identifiable));
  }

  public void remove(OIdentifiable identifiable) {
    if (!removeFromNewEntries(identifiable)) {
      OModifiableInteger counter = changedValues.get(identifiable);
      if (counter == null) {
        // Not persistent keys can only be in changedValues or newEntries
        if (identifiable.getIdentity().isPersistent())
          changedValues.put(identifiable, new OModifiableInteger(-1));
        else
          // Return immediately to prevent firing of event
          return;
      } else
        counter.decrement();
    }

    size--;

    fireCollectionChangedEvent(new OMultiValueChangeEvent<OIdentifiable, OIdentifiable>(OMultiValueChangeEvent.OChangeType.REMOVE,
        identifiable, null, identifiable));
  }

  public int size() {
    return size;
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
    return OLongSerializer.LONG_SIZE + 2 * OIntegerSerializer.INT_SIZE;
  }

  @Override
  public int getSerializedSize(byte[] stream, int offset) {
    return OLongSerializer.LONG_SIZE + 2 * OIntegerSerializer.INT_SIZE;
  }

  @Override
  public int serialize(byte[] stream, int offset) {
    for (OIdentifiable identifiable : changedValues.keySet()) {
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
      OModifiableInteger v = changedValues.get(identifiable);
      if (v != null)
        entry.getValue().increment(v.getValue());
      changedValues.put(identifiable, entry.getValue());
    }
    newEntries.clear();

    final ORecordSerializationContext context = ORecordSerializationContext.getContext();

    // make sure that we really save underlying record.
    if (rootPointer == null) {
      if (context != null) {
        final OSBTreeBonsai<OIdentifiable, Integer> treeBonsai = ODatabaseRecordThreadLocal.INSTANCE.get()
            .getSbTreeCollectionManager().createSBTree();
        try {
          rootPointer = treeBonsai.getRootBucketPointer();
        } finally {
          releaseTree();
        }
      }
    }

    OBonsaiBucketPointer rootPointer;
    if (this.rootPointer != null)
      rootPointer = this.rootPointer;
    else
      rootPointer = new OBonsaiBucketPointer(-1, -1);

    OLongSerializer.INSTANCE.serialize(rootPointer.getPageIndex(), stream, offset);
    offset += OLongSerializer.LONG_SIZE;

    OIntegerSerializer.INSTANCE.serialize(rootPointer.getPageOffset(), stream, offset);
    offset += OIntegerSerializer.INT_SIZE;

    OIntegerSerializer.INSTANCE.serialize(size, stream, offset);
    offset += OIntegerSerializer.INT_SIZE;

    if (context != null)
      context.push(new ORidBagUpdateSerializationOperation(changedValues, rootPointer));

    return offset;
  }

  @Override
  public void delete() {
    final ORecordSerializationContext context = ORecordSerializationContext.getContext();
    if (context != null && rootPointer != null)
      context.push(new ORidBagDeleteSerializationOperation(rootPointer));

    rootPointer = null;
    changedValues.clear();
    newEntries.clear();
    size = 0;
    changeListeners.clear();
  }

  @Override
  public int deserialize(byte[] stream, int offset) {
    final long pageIndex = OLongSerializer.INSTANCE.deserialize(stream, offset);
    offset += OLongSerializer.LONG_SIZE;

    final int pageOffset = OIntegerSerializer.INSTANCE.deserialize(stream, offset);
    offset += OIntegerSerializer.INT_SIZE;

    final int size = OIntegerSerializer.INSTANCE.deserialize(stream, offset);
    offset += OIntegerSerializer.INT_SIZE;

    rootPointer = new OBonsaiBucketPointer(pageIndex, pageOffset);
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
    private final NavigableMap<OIdentifiable, OModifiableInteger>  changedValues;
    private Iterator<Map.Entry<OIdentifiable, OModifiableInteger>> newEntryIterator;
    private Iterator<Map.Entry<OIdentifiable, OModifiableInteger>> changedValuesIterator;
    private final SBTreeMapEntryIterator                           sbTreeIterator;

    private Map.Entry<OIdentifiable, OModifiableInteger>           nextChangedEntry;
    private Map.Entry<OIdentifiable, Integer>                      nextSBTreeEntry;

    private OIdentifiable                                          currentValue;
    private int                                                    currentFinalCounter;

    private int                                                    currentCounter;

    private final boolean                                          convertToRecord;

    private boolean                                                currentRemoved;

    private RIDBagIterator(IdentityHashMap<OIdentifiable, OModifiableInteger> newEntries,
        NavigableMap<OIdentifiable, OModifiableInteger> changedValues, SBTreeMapEntryIterator sbTreeIterator,
        boolean convertToRecord) {
      newEntryIterator = newEntries.entrySet().iterator();
      this.changedValues = changedValues;
      this.convertToRecord = convertToRecord;
      this.changedValuesIterator = changedValues.entrySet().iterator();
      this.sbTreeIterator = sbTreeIterator;

      nextChangedEntry = nextChangedNotRemovedEntry(changedValuesIterator);

      if (sbTreeIterator != null)
        nextSBTreeEntry = nextChangedNotRemovedSBTreeEntry(sbTreeIterator);
    }

    @Override
    public boolean hasNext() {
      return newEntryIterator.hasNext() || nextChangedEntry != null || nextSBTreeEntry != null
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

      if (nextChangedEntry != null && nextSBTreeEntry != null) {
        if (nextChangedEntry.getKey().compareTo(nextSBTreeEntry.getKey()) < 0) {
          currentValue = nextChangedEntry.getKey();
          currentFinalCounter = nextChangedEntry.getValue().intValue();
          currentCounter = 1;

          nextChangedEntry = nextChangedNotRemovedEntry(changedValuesIterator);

        } else if (nextChangedEntry.getKey().compareTo(nextSBTreeEntry.getKey()) >= 0) {
          currentValue = nextSBTreeEntry.getKey();
          currentFinalCounter = nextSBTreeEntry.getValue();
          currentCounter = 1;

          nextSBTreeEntry = nextChangedNotRemovedSBTreeEntry(sbTreeIterator);
          if (nextChangedEntry != null && nextChangedEntry.getKey().equals(currentValue))
            nextChangedEntry = nextChangedNotRemovedEntry(changedValuesIterator);
        }
      } else if (nextChangedEntry != null) {
        currentValue = nextChangedEntry.getKey();
        currentFinalCounter = nextChangedEntry.getValue().intValue();
        currentCounter = 1;

        nextChangedEntry = nextChangedNotRemovedEntry(changedValuesIterator);
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

      if (!removeFromNewEntries(currentValue)) {
        OModifiableInteger counter = changedValues.get(currentValue);
        if (counter != null)
          counter.decrement();
        else {
          if (nextChangedEntry != null) {
            changedValues.put(currentValue, new OModifiableInteger(-1));
            changedValuesIterator = changedValues.tailMap(nextChangedEntry.getKey(), false).entrySet().iterator();
          } else {
            changedValues.put(currentValue, new OModifiableInteger(-1));
          }
        }
      }

      size--;
      fireCollectionChangedEvent(new OMultiValueChangeEvent<OIdentifiable, OIdentifiable>(
          OMultiValueChangeEvent.OChangeType.REMOVE, currentValue, null, currentValue));
      currentRemoved = true;
    }

    private Map.Entry<OIdentifiable, OModifiableInteger> nextChangedNotRemovedEntry(
        Iterator<Map.Entry<OIdentifiable, OModifiableInteger>> iterator) {
      Map.Entry<OIdentifiable, OModifiableInteger> entry;

      while (iterator.hasNext()) {
        entry = iterator.next();
        if (entry.getValue().intValue() > 0)
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

      nextChangedEntry = nextChangedNotRemovedEntry(changedValuesIterator);

      if (sbTreeIterator != null)
        nextSBTreeEntry = nextChangedNotRemovedSBTreeEntry(sbTreeIterator);
    }
  }

  private Map.Entry<OIdentifiable, Integer> nextChangedNotRemovedSBTreeEntry(Iterator<Map.Entry<OIdentifiable, Integer>> iterator) {
    while (iterator.hasNext()) {
      final Map.Entry<OIdentifiable, Integer> entry = iterator.next();
      final OModifiableInteger changedCounter = changedValues.get(entry.getKey());
      if (changedCounter == null)
        return entry;

      if (entry.getValue() + changedCounter.intValue() > 0)
        return new Map.Entry<OIdentifiable, Integer>() {
          @Override
          public OIdentifiable getKey() {
            return entry.getKey();
          }

          @Override
          public Integer getValue() {
            return entry.getValue() + changedCounter.intValue();
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
}
