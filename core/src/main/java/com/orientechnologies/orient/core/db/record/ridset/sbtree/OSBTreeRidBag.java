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

package com.orientechnologies.orient.core.db.record.ridset.sbtree;

import java.nio.charset.Charset;
import java.util.*;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.common.types.OModifiableInteger;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.*;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.sbtree.OTreeInternal;
import com.orientechnologies.orient.core.index.sbtreebonsai.local.OBonsaiBucketPointer;
import com.orientechnologies.orient.core.index.sbtreebonsai.local.OSBTreeBonsai;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.OBase64Utils;
import com.orientechnologies.orient.core.serialization.OBinaryProtocol;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializerSBTreeIndexRIDContainer;
import com.orientechnologies.orient.core.serialization.serializer.string.OStringBuilderSerializable;
import com.orientechnologies.orient.core.storage.impl.local.paginated.ORecordSerializationContext;
import com.orientechnologies.orient.core.storage.impl.local.paginated.ORidSetUpdateSerializationOperation;

/**
 * Persistent Set<OIdentifiable> implementation that uses the SBTree to handle entries in persistent way.
 * 
 * @author <a href="mailto:enisher@gmail.com">Artem Orobets</a>
 */
public class OSBTreeRidBag implements OStringBuilderSerializable, Iterable<OIdentifiable>, ORecordLazyMultiValue,
    OTrackedMultiValue<OIdentifiable, OIdentifiable> {
  private OBonsaiBucketPointer                                         rootPointer;
  private final OSBTreeCollectionManager                               collectionManager;

  private final NavigableMap<OIdentifiable, OModifiableInteger>        changedValues       = new TreeMap<OIdentifiable, OModifiableInteger>();

  private int                                                          modCount;
  private int                                                          size;

  private boolean                                                      autoConvertToRecord = true;

  private Set<OMultiValueChangeListener<OIdentifiable, OIdentifiable>> changeListeners     = Collections
                                                                                               .newSetFromMap(new WeakHashMap<OMultiValueChangeListener<OIdentifiable, OIdentifiable>, Boolean>());

  public OSBTreeRidBag() {
    rootPointer = null;

    collectionManager = ODatabaseRecordThreadLocal.INSTANCE.get().getSbTreeCollectionManager();
  }

  private OSBTreeRidBag(OBonsaiBucketPointer rootPointer, int size) {
    this.rootPointer = rootPointer;
    this.size = size;

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
    return new RIDBagIterator(changedValues, rootPointer != null ? new SBTreeMapEntryIterator(1000) : null, modCount,
        autoConvertToRecord);
  }

  @Override
  public Iterator<OIdentifiable> rawIterator() {
    return new RIDBagIterator(changedValues, rootPointer != null ? new SBTreeMapEntryIterator(1000) : null, modCount, false);
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
      }
    }

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
    OModifiableInteger counter = changedValues.get(identifiable);
    if (counter == null)
      changedValues.put(identifiable, new OModifiableInteger(1));
    else
      counter.increment();

    size++;
    modCount++;

    fireCollectionChangedEvent(new OMultiValueChangeEvent<OIdentifiable, OIdentifiable>(OMultiValueChangeEvent.OChangeType.ADD,
        identifiable, identifiable));
  }

  public void remove(OIdentifiable identifiable) {
    OModifiableInteger counter = changedValues.get(identifiable);
    if (counter == null)
      changedValues.put(identifiable, new OModifiableInteger(-1));
    else
      counter.decrement();

    size--;
    modCount++;

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
  public OSBTreeRidBag toStream(StringBuilder output) throws OSerializationException {
    if (rootPointer == null) {
      final OSBTreeBonsai<OIdentifiable, Integer> treeBonsai = ODatabaseRecordThreadLocal.INSTANCE.get()
          .getSbTreeCollectionManager().createSBTree();
      try {
        rootPointer = treeBonsai.getRootBucketPointer();
      } finally {
        releaseTree();
      }
    }

    final byte[] stream = new byte[OLongSerializer.LONG_SIZE + 2 * OIntegerSerializer.INT_SIZE];
    int offset = 0;
    OLongSerializer.INSTANCE.serialize(rootPointer.getPageIndex(), stream, 0);
    offset += OLongSerializer.LONG_SIZE;

    OIntegerSerializer.INSTANCE.serialize(rootPointer.getPageOffset(), stream, offset);
    offset += OIntegerSerializer.INT_SIZE;

    OIntegerSerializer.INSTANCE.serialize(size, stream, offset);

    output.append(OBase64Utils.encodeBytes(stream));

    ORecordSerializationContext context = ORecordSerializationContext.getContext();
    context.push(new ORidSetUpdateSerializationOperation(changedValues, rootPointer));
    return this;
  }

  public byte[] toStream() {
    final StringBuilder iOutput = new StringBuilder();
    toStream(iOutput);
    return iOutput.toString().getBytes();
  }

  @Override
  public OStringBuilderSerializable fromStream(StringBuilder iInput) throws OSerializationException {
    fromStream(iInput.toString());
    return this;
  }

  public static OSBTreeRidBag fromStream(String value) {
    final byte[] stream = OBase64Utils.decode(value);

    int offset = 0;
    final long pageIndex = OLongSerializer.INSTANCE.deserialize(stream, offset);
    offset += OLongSerializer.LONG_SIZE;

    final int pageOffset = OIntegerSerializer.INSTANCE.deserialize(stream, offset);
    offset += OIntegerSerializer.INT_SIZE;

    final int size = OIntegerSerializer.INSTANCE.deserialize(stream, offset);

    return new OSBTreeRidBag(new OBonsaiBucketPointer(pageIndex, pageOffset), size);
  }

  private final class RIDBagIterator implements Iterator<OIdentifiable> {
    private final NavigableMap<OIdentifiable, OModifiableInteger>  changedValues;
    private Iterator<Map.Entry<OIdentifiable, OModifiableInteger>> changedValuesIterator;
    private final Iterator<Map.Entry<OIdentifiable, Integer>>      sbTreeIterator;

    private Map.Entry<OIdentifiable, OModifiableInteger>           nextChangedEntry;
    private Map.Entry<OIdentifiable, Integer>                      nextSBTreeEntry;

    private OIdentifiable                                          currentValue;
    private int                                                    currentFinalCounter;

    private int                                                    currentCounter;

    private final int                                              extModCount;
    private final boolean                                          convertToRecord;

    private boolean                                                currentRemoved;

    private RIDBagIterator(NavigableMap<OIdentifiable, OModifiableInteger> changedValues,
        Iterator<Map.Entry<OIdentifiable, Integer>> sbTreeIterator, int extModCount, boolean convertToRecord) {

      this.changedValues = changedValues;
      this.convertToRecord = convertToRecord;
      this.changedValuesIterator = changedValues.entrySet().iterator();
      this.sbTreeIterator = sbTreeIterator;
      this.extModCount = extModCount;

      nextChangedEntry = nextChangedNotRemovedEntry(changedValuesIterator);

      if (sbTreeIterator != null)
        nextSBTreeEntry = nextChangedNotRemovedSBTreeEntry(sbTreeIterator);
    }

    @Override
    public boolean hasNext() {
      return nextChangedEntry != null || nextSBTreeEntry != null || (currentValue != null && currentCounter < currentFinalCounter);

    }

    @Override
    public OIdentifiable next() {
      if (modCount != extModCount)
        throw new ConcurrentModificationException();

      currentRemoved = false;
      if (currentCounter < currentFinalCounter) {
        currentCounter++;
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

  private final class SBTreeMapEntryIterator implements Iterator<Map.Entry<OIdentifiable, Integer>> {
    private LinkedList<Map.Entry<OIdentifiable, Integer>> preFetchedValues;
    private OIdentifiable                                 firstKey;

    private final int                                     prefetchSize;

    public SBTreeMapEntryIterator(int prefetchSize) {
      this.prefetchSize = prefetchSize;

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
  }

}
