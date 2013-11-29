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

import java.util.*;

import com.orientechnologies.common.profiler.OProfilerMBean;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.common.types.OModifiableInteger;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.index.sbtree.OTreeInternal;
import com.orientechnologies.orient.core.index.sbtreebonsai.local.OBonsaiBucketPointer;
import com.orientechnologies.orient.core.index.sbtreebonsai.local.OSBTreeBonsai;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.OBinaryProtocol;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.core.serialization.serializer.string.OStringBuilderSerializable;
import com.orientechnologies.orient.core.storage.impl.local.paginated.ORecordSerializationContext;
import com.orientechnologies.orient.core.storage.impl.local.paginated.ORidSetUpdateSerializationOperation;

/**
 * Persistent Set<OIdentifiable> implementation that uses the SBTree to handle entries in persistent way.
 * 
 * @author <a href="mailto:enisher@gmail.com">Artem Orobets</a>
 */
public class OSBTreeRidBag implements OStringBuilderSerializable, Iterable<OIdentifiable> {
  private static final OProfilerMBean                           PROFILER      = Orient.instance().getProfiler();

  private OBonsaiBucketPointer                                  rootPointer;
  private final OSBTreeCollectionManager                        collectionManager;

  private final NavigableMap<OIdentifiable, OModifiableInteger> changedValues = new TreeMap<OIdentifiable, OModifiableInteger>();

  private boolean                                               clear         = false;

  private int                                                   modCount;
  private int                                                   size;

  public OSBTreeRidBag() {
    rootPointer = null;

    collectionManager = ODatabaseRecordThreadLocal.INSTANCE.get().getSbTreeCollectionManager();
  }

  private OSBTreeRidBag(OBonsaiBucketPointer rootPointer, int size) {
    this.rootPointer = rootPointer;
    this.size = size;

    collectionManager = ODatabaseRecordThreadLocal.INSTANCE.get().getSbTreeCollectionManager();
  }

  public OSBTreeRidBag(Collection<OIdentifiable> value) {
    rootPointer = null;

    collectionManager = ODatabaseRecordThreadLocal.INSTANCE.get().getSbTreeCollectionManager();

    for (OIdentifiable identifiable : value) {
      add(identifiable);
    }
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
    return new RIDBagIterator(changedValues, rootPointer != null ? new SBTreeMapEntryIterator(1000) : null, modCount);
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
  }

  public void remove(OIdentifiable o) {
    OModifiableInteger counter = changedValues.get(o);
    if (counter == null)
      changedValues.put(o, new OModifiableInteger(-1));
    else
      counter.decrement();

    size--;
    modCount++;
  }

  public void clear() {
    clear = true;
    changedValues.clear();

    size = 0;

    modCount++;
  }

  public int size() {
    return size;
  }

  public boolean isEmpty() {
    return size() == 0;
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

    output.append(OBinaryProtocol.bytes2string(stream));

    ORecordSerializationContext context = ORecordSerializationContext.getContext();
    context.push(new ORidSetUpdateSerializationOperation(changedValues, clear, rootPointer));
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
    final byte[] stream = OBinaryProtocol.string2bytes(value);

    int offset = 0;
    final long pageIndex = OLongSerializer.INSTANCE.deserialize(stream, offset);
    offset += OLongSerializer.LONG_SIZE;

    final int pageOffset = OIntegerSerializer.INSTANCE.deserialize(stream, offset);
    offset += OIntegerSerializer.INT_SIZE;

    final int size = OIntegerSerializer.INSTANCE.deserialize(stream, offset);

    return new OSBTreeRidBag(new OBonsaiBucketPointer(pageIndex, pageOffset), size);
  }

  private final class RIDBagIterator implements Iterator<OIdentifiable> {
    private final NavigableMap<OIdentifiable, OModifiableInteger>        changedValues;
    private final Iterator<Map.Entry<OIdentifiable, OModifiableInteger>> changedValuesIterator;
    private final Iterator<Map.Entry<OIdentifiable, Integer>>            sbTreeIterator;

    private Map.Entry<OIdentifiable, OModifiableInteger>                 nextChangedEntry;
    private Map.Entry<OIdentifiable, Integer>                            nextSBTreeEntry;

    private OIdentifiable                                                currentValue;
    private int                                                          currentFinalCounter;

    private int                                                          currentCounter;

    private final int                                                    extModCount;

    private RIDBagIterator(NavigableMap<OIdentifiable, OModifiableInteger> changedValues,
        Iterator<Map.Entry<OIdentifiable, Integer>> sbTreeIterator, int extModCount) {

      this.changedValues = changedValues;
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

      if (currentCounter < currentFinalCounter) {
        currentCounter++;
        return currentValue;
      }

      if (nextChangedEntry != null && nextSBTreeEntry != null) {
        if (nextChangedEntry.getKey().compareTo(nextSBTreeEntry.getKey()) <= 0)
          nextChangedValue();
        else
          nextSBTreeValue();
      } else if (nextChangedEntry != null)
        nextChangedValue();
      else if (nextSBTreeEntry != null)
        nextSBTreeValue();
      else
        throw new NoSuchElementException();

      return currentValue;
    }

    private void nextSBTreeValue() {
      currentValue = nextSBTreeEntry.getKey();
      currentFinalCounter = nextSBTreeEntry.getValue();
      currentCounter = 1;

      nextSBTreeEntry = nextChangedNotRemovedSBTreeEntry(sbTreeIterator);
    }

    private void nextChangedValue() {
      currentValue = nextChangedEntry.getKey();
      currentFinalCounter = nextChangedEntry.getValue().intValue();
      currentCounter = 1;

      nextChangedEntry = nextChangedNotRemovedEntry(changedValuesIterator);
    }

    @Override
    public void remove() {
      currentFinalCounter--;

      if (currentFinalCounter <= 0) {
        if (currentValue.equals(nextChangedEntry.getKey())) {
          nextChangedEntry = nextChangedNotRemovedEntry(changedValuesIterator);
          changedValuesIterator.remove();
        }

        if (currentValue.equals(nextSBTreeEntry.getKey())) {
          nextSBTreeEntry = nextChangedNotRemovedSBTreeEntry(sbTreeIterator);

          OModifiableInteger counter = changedValues.get(nextChangedEntry.getKey());
          if (counter == null)
            changedValues.put(nextSBTreeEntry.getKey(), new OModifiableInteger(-1));
          else
            counter.decrement();
        }

        currentValue = null;
      }
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
    Map.Entry<OIdentifiable, Integer> entry;

    while (iterator.hasNext()) {
      entry = iterator.next();
      OModifiableInteger changedCounter = changedValues.get(entry.getKey());
      if (changedCounter == null || changedCounter.intValue() > 0)
        return entry;
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
