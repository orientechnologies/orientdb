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
import com.orientechnologies.common.util.OSizeable;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordLazyMultiValue;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.index.sbtree.OTreeInternal;
import com.orientechnologies.orient.core.index.sbtreebonsai.local.OBonsaiBucketPointer;
import com.orientechnologies.orient.core.index.sbtreebonsai.local.OSBTreeBonsai;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.core.serialization.serializer.string.OStringBuilderSerializable;
import com.orientechnologies.orient.core.storage.impl.local.paginated.ORecordSerializationContext;
import com.orientechnologies.orient.core.storage.impl.local.paginated.ORidSetUpdateSerializationOperation;

/**
 * Persistent Set<OIdentifiable> implementation that uses the SBTree to handle entries in persistent way.
 * 
 * @author <a href="mailto:enisher@gmail.com">Artem Orobets</a>
 */
public class OSBTreeRIDSet implements Set<OIdentifiable>, OStringBuilderSerializable, ORecordLazyMultiValue, OSizeable {
  private static final OProfilerMBean       PROFILER            = Orient.instance().getProfiler();

  private OBonsaiBucketPointer              rootPointer;

  private boolean                           autoConvertToRecord = true;

  private final OSBTreeCollectionManager    collectionManager;

  private final NavigableSet<OIdentifiable> addValues           = new TreeSet<OIdentifiable>();
  private final Set<OIdentifiable>          removedValues       = new HashSet<OIdentifiable>();

  private boolean                           clear               = false;

  public OSBTreeRIDSet() {
    rootPointer = null;

    collectionManager = ODatabaseRecordThreadLocal.INSTANCE.get().getSbTreeCollectionManager();
  }

  public OSBTreeRIDSet(OBonsaiBucketPointer rootPointer) {
    this.rootPointer = rootPointer;

    collectionManager = ODatabaseRecordThreadLocal.INSTANCE.get().getSbTreeCollectionManager();
  }

  public OSBTreeRIDSet(Collection<OIdentifiable> value) {
    rootPointer = null;

    collectionManager = ODatabaseRecordThreadLocal.INSTANCE.get().getSbTreeCollectionManager();

    addAll(value);
  }

  private OSBTreeBonsai<OIdentifiable, Boolean> loadTree() {
    if (rootPointer == null)
      return null;

    return collectionManager.loadSBTree(rootPointer);
  }

  private void releaseTree() {
    if (rootPointer == null)
      return;

    collectionManager.releaseSBTree(rootPointer);
  }

  @Override
  public int size() {
    OSBTreeBonsai<OIdentifiable, Boolean> tree = loadTree();
    try {
      final int size = addValues.size();
      if (tree != null)
        return size + (int) tree.size() - removedValues.size();

      return size;
    } finally {
      releaseTree();
    }

  }

  @Override
  public boolean isEmpty() {
    return size() == 0;
  }

  @Override
  public boolean contains(Object o) {
    return o instanceof OIdentifiable && contains((OIdentifiable) o);
  }

  public boolean contains(OIdentifiable o) {
    if (addValues.contains(o))
      return true;

    if (removedValues.contains(o))
      return false;

    final OSBTreeBonsai<OIdentifiable, Boolean> tree = loadTree();
    if (tree == null)
      return false;

    try {
      return tree.get(o) != null;
    } finally {
      releaseTree();
    }
  }

  @Override
  public Iterator<OIdentifiable> iterator() {
    return new RIDSetIterator(addValues.iterator(), rootPointer != null ? new SBTreeValuesIterator() : null, removedValues);
  }

  @Override
  public Object[] toArray() {
    // TODO replace with more efficient implementation

    final ArrayList<OIdentifiable> list = new ArrayList<OIdentifiable>(size());

    for (OIdentifiable identifiable : this) {
      if (autoConvertToRecord)
        identifiable = identifiable.getRecord();

      list.add(identifiable);
    }

    return list.toArray();
  }

  @Override
  public <T> T[] toArray(T[] a) {
    // TODO replace with more efficient implementation.

    final ArrayList<OIdentifiable> list = new ArrayList<OIdentifiable>(size());

    for (OIdentifiable identifiable : this) {
      if (autoConvertToRecord)
        identifiable = identifiable.getRecord();

      list.add(identifiable);
    }

    return list.toArray(a);
  }

  @Override
  public boolean add(OIdentifiable identifiable) {
    if (contains(identifiable))
      return false;

    removedValues.remove(identifiable);
    addValues.add(identifiable);

    return true;
  }

  @Override
  public boolean remove(Object o) {
    return o instanceof OIdentifiable && remove((OIdentifiable) o);
  }

  public boolean remove(OIdentifiable o) {
    if (!contains(o))
      return false;

    addValues.remove(o);

    if (rootPointer != null)
      removedValues.add(o);

    return true;
  }

  @Override
  public boolean containsAll(Collection<?> c) {
    for (Object e : c)
      if (!contains(e))
        return false;
    return true;
  }

  @Override
  public boolean addAll(Collection<? extends OIdentifiable> c) {
    boolean modified = false;
    for (OIdentifiable e : c)
      if (add(e))
        modified = true;
    return modified;
  }

  @Override
  public boolean retainAll(Collection<?> c) {
    boolean modified = false;
    Iterator<OIdentifiable> it = iterator();
    while (it.hasNext()) {
      if (!c.contains(it.next())) {
        it.remove();
        modified = true;
      }
    }
    return modified;
  }

  @Override
  public boolean removeAll(Collection<?> c) {
    boolean modified = false;
    for (Object o : c) {
      modified |= remove(o);
    }

    return modified;
  }

  @Override
  public void clear() {
    clear = true;
    addValues.clear();
    removedValues.clear();
  }

  @Override
  public OSBTreeRIDSet toStream(StringBuilder output) throws OSerializationException {
    final long timer = PROFILER.startChrono();

    try {
      output.append(OStringSerializerHelper.LINKSET_PREFIX);

      final ODocument document = new ODocument();
      if (rootPointer == null) {
        final OSBTreeBonsai<OIdentifiable, Boolean> treeBonsai = ODatabaseRecordThreadLocal.INSTANCE.get()
            .getSbTreeCollectionManager().createSBTree();
        try {
          rootPointer = treeBonsai.getRootBucketPointer();
        } finally {
          releaseTree();
        }
      }

      document.field("rootIndex", rootPointer.getPageIndex());
      document.field("rootOffset", rootPointer.getPageOffset());
      output.append(new String(document.toStream()));

      output.append(OStringSerializerHelper.SET_END);

      ORecordSerializationContext context = ORecordSerializationContext.getContext();
      context.push(new ORidSetUpdateSerializationOperation(addValues, removedValues, clear, rootPointer));
    } finally {
      PROFILER.stopChrono(PROFILER.getProcessMetric("mvrbtree.toStream"), "Serialize a MVRBTreeRID", timer);
    }
    return this;
  }

  public byte[] toStream() {
    final StringBuilder iOutput = new StringBuilder();
    toStream(iOutput);
    return iOutput.toString().getBytes();
  }

  @Override
  public OStringBuilderSerializable fromStream(StringBuilder iInput) throws OSerializationException {
    throw new UnsupportedOperationException("unimplemented yet");
  }

  public static OSBTreeRIDSet fromStream(String stream, ORecordInternal<?> owner) {
    stream = stream.substring(OStringSerializerHelper.LINKSET_PREFIX.length(), stream.length() - 1);

    final ODocument doc = new ODocument();
    doc.fromString(stream);
    final OBonsaiBucketPointer rootIndex = new OBonsaiBucketPointer((Long) doc.field("rootIndex"),
        (Integer) doc.field("rootOffset"));

    return new OSBTreeRIDSet(rootIndex);
  }

  @Override
  public Iterator<OIdentifiable> rawIterator() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void convertLinks2Records() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean convertRecords2Links() {
    throw new UnsupportedOperationException();
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

  private final class RIDSetIterator implements Iterator<OIdentifiable> {
    private final Iterator<OIdentifiable> addedValuesIterator;
    private final Iterator<OIdentifiable> sbTreeIterator;

    private final Set<OIdentifiable>      removedValues;

    private OIdentifiable                 nextAdded;
    private OIdentifiable                 nextSBTree;

    private OIdentifiable                 currentValue;

    private RIDSetIterator(Iterator<OIdentifiable> addedValuesIterator, Iterator<OIdentifiable> sbTreeIterator,
        Set<OIdentifiable> removedValues) {
      this.addedValuesIterator = addedValuesIterator;
      this.sbTreeIterator = sbTreeIterator;
      this.removedValues = removedValues;

      nextAdded = nextNotRemovedValue(addedValuesIterator);

      if (sbTreeIterator != null)
        nextSBTree = nextNotRemovedValue(sbTreeIterator);
    }

    @Override
    public boolean hasNext() {
      return nextAdded != null || nextSBTree != null;

    }

    @Override
    public OIdentifiable next() {
      if (nextAdded != null && nextSBTree != null) {
        if (nextAdded.compareTo(nextSBTree) <= 0) {
          currentValue = nextAdded;
          nextAdded = nextNotRemovedValue(addedValuesIterator);
        } else {
          currentValue = nextSBTree;
          nextSBTree = nextNotRemovedValue(sbTreeIterator);
        }
      } else if (nextAdded != null) {
        currentValue = nextAdded;
        nextAdded = nextNotRemovedValue(addedValuesIterator);
      } else if (nextSBTree != null) {
        currentValue = nextSBTree;
        nextSBTree = nextNotRemovedValue(sbTreeIterator);
      } else
        throw new NoSuchElementException();

      return currentValue;
    }

    @Override
    public void remove() {
      if (currentValue.equals(nextAdded)) {
        nextAdded = nextNotRemovedValue(addedValuesIterator);
        addedValuesIterator.remove();
      }
      if (currentValue.equals(nextSBTree)) {
        nextSBTree = nextNotRemovedValue(sbTreeIterator);
        removedValues.add(currentValue);
      }

      currentValue = null;
    }

    private OIdentifiable nextNotRemovedValue(Iterator<OIdentifiable> iterator) {
      OIdentifiable identifiable;
      while (iterator.hasNext()) {
        identifiable = iterator.next();
        if (!removedValues.contains(identifiable))
          return identifiable;
      }

      return null;
    }
  }

  private final class SBTreeValuesIterator implements Iterator<OIdentifiable> {
    private LinkedList<OIdentifiable> preFetchedValues;
    private OIdentifiable             firstKey;
    private OIdentifiable             currentValue;

    public SBTreeValuesIterator() {
      final OSBTreeBonsai<OIdentifiable, Boolean> tree = loadTree();
      try {

        if (tree.size() == 0) {
          this.preFetchedValues = null;
          return;
        }

        this.preFetchedValues = new LinkedList<OIdentifiable>();
        firstKey = tree.firstKey();

        prefetchData(tree, true);
      } finally {
        releaseTree();
      }
    }

    private void prefetchData(OSBTreeBonsai<OIdentifiable, Boolean> tree, boolean firstTime) {
      tree.loadEntriesMajor(firstKey, firstTime, new OTreeInternal.RangeResultListener<OIdentifiable, Boolean>() {
        @Override
        public boolean addResult(final Map.Entry<OIdentifiable, Boolean> entry) {
          preFetchedValues.add(entry.getKey());
          return preFetchedValues.size() <= 1000;
        }
      });

      if (preFetchedValues.isEmpty())
        preFetchedValues = null;
      else
        firstKey = preFetchedValues.getLast();
    }

    @Override
    public boolean hasNext() {
      return preFetchedValues != null;
    }

    @Override
    public OIdentifiable next() {
      final OIdentifiable value = preFetchedValues.removeFirst();
      if (preFetchedValues.isEmpty()) {
        final OSBTreeBonsai<OIdentifiable, Boolean> tree = loadTree();
        try {
          prefetchData(tree, false);
        } finally {
          releaseTree();
        }
      }

      currentValue = value;

      return value;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }
}
