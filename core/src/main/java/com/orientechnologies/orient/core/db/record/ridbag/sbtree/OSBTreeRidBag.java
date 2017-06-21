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

package com.orientechnologies.orient.core.db.record.ridbag.sbtree;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.common.types.OModifiableInteger;
import com.orientechnologies.common.util.OResettable;
import com.orientechnologies.common.util.OSizeable;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.*;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBagDelegate;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.sbtree.OTreeInternal;
import com.orientechnologies.orient.core.index.sbtreebonsai.local.OBonsaiBucketPointer;
import com.orientechnologies.orient.core.index.sbtreebonsai.local.OSBTreeBonsai;
import com.orientechnologies.orient.core.index.sbtreebonsai.local.OSBTreeBonsaiLocal;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OLinkSerializer;
import com.orientechnologies.orient.core.storage.OStorageProxy;
import com.orientechnologies.orient.core.storage.impl.local.paginated.ORecordSerializationContext;
import com.orientechnologies.orient.core.storage.impl.local.paginated.ORidBagDeleteSerializationOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.ORidBagUpdateSerializationOperation;

import java.io.IOException;
import java.io.PrintStream;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Persistent Set<OIdentifiable> implementation that uses the SBTree to handle entries in persistent way.
 *
 * @author Artem Orobets (enisher-at-gmail.com)
 */
public class OSBTreeRidBag implements ORidBagDelegate {
  private final OSBTreeCollectionManager                           collectionManager = ODatabaseRecordThreadLocal.INSTANCE.get()
      .getSbTreeCollectionManager();
  private final NavigableMap<OIdentifiable, Change>                changes           = new ConcurrentSkipListMap<OIdentifiable, Change>();
  /**
   * Entries with not valid id.
   */
  private final IdentityHashMap<OIdentifiable, OModifiableInteger> newEntries        = new IdentityHashMap<OIdentifiable, OModifiableInteger>();
  private OBonsaiCollectionPointer collectionPointer;
  private int                      size;

  private boolean autoConvertToRecord = true;

  private           List<OMultiValueChangeListener<OIdentifiable, OIdentifiable>> changeListeners;
  private transient ORecord                                                       owner;
  private boolean updateOwner = true;

  private final class RIDBagIterator implements Iterator<OIdentifiable>, OResettable, OSizeable, OAutoConvertToRecord {
    private final NavigableMap<OIdentifiable, Change>                    changedValues;
    private final SBTreeMapEntryIterator                                 sbTreeIterator;
    private       boolean                                                convertToRecord;
    private       Iterator<Map.Entry<OIdentifiable, OModifiableInteger>> newEntryIterator;
    private       Iterator<Map.Entry<OIdentifiable, Change>>             changedValuesIterator;
    private       Map.Entry<OIdentifiable, Change>                       nextChange;
    private       Map.Entry<OIdentifiable, Integer>                      nextSBTreeEntry;
    private       OIdentifiable                                          currentValue;
    private       int                                                    currentFinalCounter;
    private       int                                                    currentCounter;
    private       boolean                                                currentRemoved;

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
      return newEntryIterator.hasNext() || nextChange != null || nextSBTreeEntry != null || (currentValue != null
          && currentCounter < currentFinalCounter);
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

      if (nextChange != null && nextSBTreeEntry != null) {
        if (nextChange.getKey().compareTo(nextSBTreeEntry.getKey()) < 0) {
          currentValue = nextChange.getKey();
          currentFinalCounter = nextChange.getValue().applyTo(0);
          currentCounter = 1;

          nextChange = nextChangedNotRemovedEntry(changedValuesIterator);
        } else {
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

      if (OSBTreeRidBag.this.owner != null)
        ORecordInternal.unTrack(OSBTreeRidBag.this.owner, currentValue);

      if (updateOwner)
        fireCollectionChangedEvent(
            new OMultiValueChangeEvent<OIdentifiable, OIdentifiable>(OMultiValueChangeEvent.OChangeType.REMOVE, currentValue, null,
                currentValue, false));
      currentRemoved = true;
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

    @Override
    public int size() {
      return OSBTreeRidBag.this.size();
    }

    @Override
    public boolean isAutoConvertToRecord() {
      return convertToRecord;
    }

    @Override
    public void setAutoConvertToRecord(final boolean convertToRecord) {
      this.convertToRecord = convertToRecord;
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
  }

  private final class SBTreeMapEntryIterator implements Iterator<Map.Entry<OIdentifiable, Integer>>, OResettable {
    private final int                                           prefetchSize;
    private       LinkedList<Map.Entry<OIdentifiable, Integer>> preFetchedValues;
    private       OIdentifiable                                 firstKey;

    public SBTreeMapEntryIterator(int prefetchSize) {
      this.prefetchSize = prefetchSize;

      init();
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

    private void prefetchData(boolean firstTime) {
      final OSBTreeBonsai<OIdentifiable, Integer> tree = loadTree();
      try {
        tree.loadEntriesMajor(firstKey, firstTime, true, new OTreeInternal.RangeResultListener<OIdentifiable, Integer>() {
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

    private void init() {
      OSBTreeBonsai<OIdentifiable, Integer> tree = loadTree();
      try {
        firstKey = tree.firstKey();
      } finally {
        releaseTree();
      }

      if (firstKey == null) {
        this.preFetchedValues = null;
        return;
      }

      this.preFetchedValues = new LinkedList<Map.Entry<OIdentifiable, Integer>>();
      prefetchData(true);
    }
  }

  public OSBTreeRidBag(OBonsaiCollectionPointer pointer, Map<OIdentifiable,Change> changes) {
    this.collectionPointer = pointer;
    this.changes.putAll(changes);
    this.size = -1;
  }

  public OSBTreeRidBag() {
    collectionPointer = null;
  }

  @Override
  public ORecord getOwner() {
    return owner;
  }

  @Override
  public void setOwner(ORecord owner) {
    if (owner != null && this.owner != null && !this.owner.equals(owner)) {
      throw new IllegalStateException("This data structure is owned by document " + owner
          + " if you want to use it in other document create new rid bag instance and copy content of current one.");
    }
    if (this.owner != null) {
      for (OIdentifiable entry : newEntries.keySet()) {
        ORecordInternal.unTrack(this.owner, entry);
      }
      for (OIdentifiable entry : changes.keySet()) {
        ORecordInternal.unTrack(this.owner, entry);
      }
    }

    this.owner = owner;
    if (this.owner != null) {
      for (OIdentifiable entry : newEntries.keySet()) {
        ORecordInternal.track(this.owner, entry);
      }
      for (OIdentifiable entry : changes.keySet()) {
        ORecordInternal.track(this.owner, entry);
      }
    }
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
    TreeMap<OIdentifiable, Change> newChanges = new TreeMap<OIdentifiable, Change>();
    for (Map.Entry<OIdentifiable, Change> entry : changes.entrySet()) {
      final OIdentifiable key = entry.getKey().getRecord();
      if (key != null && this.owner != null) {
        ORecordInternal.unTrack(this.owner, entry.getKey());
        ORecordInternal.track(this.owner, key);
      }
      newChanges.put((key == null) ? entry.getKey() : key, entry.getValue());
    }

    changes.clear();
    changes.putAll(newChanges);
  }

  @Override
  public boolean convertRecords2Links() {
    final Map<OIdentifiable, Change> newChangedValues = new HashMap<OIdentifiable, Change>();
    for (Map.Entry<OIdentifiable, Change> entry : changes.entrySet()) {
      OIdentifiable identifiable = entry.getKey();
      if (identifiable instanceof ORecord) {
        ORID identity = identifiable.getIdentity();
        ORecord record = (ORecord) identifiable;
        identity = record.getIdentity();

        newChangedValues.put(identity, entry.getValue());
      } else
        newChangedValues.put(entry.getKey().getIdentity(), entry.getValue());
    }

    for (Map.Entry<OIdentifiable, Change> entry : newChangedValues.entrySet()) {
      if (entry.getKey() instanceof ORecord) {
        ORecord record = (ORecord) entry.getKey();

        newChangedValues.put(record, entry.getValue());
      } else
        return false;
    }

    newEntries.clear();

    changes.clear();
    changes.putAll(newChangedValues);

    return true;
  }

  public void mergeChanges(OSBTreeRidBag treeRidBag) {
    for (Map.Entry<OIdentifiable, OModifiableInteger> entry : treeRidBag.newEntries.entrySet()) {
      mergeDiffEntry(entry.getKey(), entry.getValue().getValue());
    }

    for (Map.Entry<OIdentifiable, Change> entry : treeRidBag.changes.entrySet()) {
      final OIdentifiable rec = entry.getKey();
      final Change change = entry.getValue();
      final int diff;
      if (change instanceof DiffChange)
        diff = ((DiffChange) change).getValue();
      else if (change instanceof AbsoluteChange)
        diff = ((AbsoluteChange) change).getValue() - getAbsoluteValue(rec).getValue();
      else
        throw new IllegalArgumentException("change type is not supported");

      mergeDiffEntry(rec, diff);
    }
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

  public void add(final OIdentifiable identifiable) {
    if (identifiable == null)
      throw new IllegalArgumentException("Impossible to add a null identifiable in a ridbag");

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
      final OModifiableInteger counter = newEntries.get(identifiable);
      if (counter == null)
        newEntries.put(identifiable, new OModifiableInteger(1));
      else
        counter.increment();
    }

    if (size >= 0)
      size++;

    if (this.owner != null)
      ORecordInternal.track(this.owner, identifiable);

    if (updateOwner)
      fireCollectionChangedEvent(
          new OMultiValueChangeEvent<OIdentifiable, OIdentifiable>(OMultiValueChangeEvent.OChangeType.ADD, identifiable,
              identifiable, null, false));
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

    if (this.owner != null)
      ORecordInternal.unTrack(this.owner, identifiable);

    if (updateOwner)
      fireCollectionChangedEvent(
          new OMultiValueChangeEvent<OIdentifiable, OIdentifiable>(OMultiValueChangeEvent.OChangeType.REMOVE, identifiable, null,
              identifiable, false));
  }

  @Override
  public boolean contains(OIdentifiable identifiable) {
    if (newEntries.containsKey(identifiable))
      return true;

    Change counter = changes.get(identifiable);

    if (counter != null) {
      AbsoluteChange absoluteValue = getAbsoluteValue(identifiable);

      if (counter.isUndefined()) {
        changes.put(identifiable, absoluteValue);
      }

      counter = absoluteValue;
    } else {
      counter = getAbsoluteValue(identifiable);
    }

    return counter.applyTo(0) > 0;
  }

  public int size() {
    if (size >= 0)
      return size;
    else {
      return updateSize();
    }
  }

  @Override
  public String toString() {
    if (size >= 0)
      return "[size=" + size + "]";

    return "[...]";
  }

  public boolean isEmpty() {
    return size() == 0;
  }

  public void addChangeListener(final OMultiValueChangeListener<OIdentifiable, OIdentifiable> changeListener) {
    if (changeListeners == null)
      changeListeners = new LinkedList<OMultiValueChangeListener<OIdentifiable, OIdentifiable>>();
    changeListeners.add(changeListener);
  }

  public void removeRecordChangeListener(final OMultiValueChangeListener<OIdentifiable, OIdentifiable> changeListener) {
    if (changeListeners != null)
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

  @Override
  public int getSerializedSize() {
    int result = 2 * OLongSerializer.LONG_SIZE + 3 * OIntegerSerializer.INT_SIZE;
    if (ODatabaseRecordThreadLocal.INSTANCE.get().getStorage() instanceof OStorageProxy
        || ORecordSerializationContext.getContext() == null)
      result += getChangesSerializedSize();
    return result;
  }

  @Override
  public int getSerializedSize(byte[] stream, int offset) {
    return getSerializedSize();
  }

  @Override
  public int serialize(byte[] stream, int offset, UUID ownerUuid) {
    applyNewEntries();

    final ORecordSerializationContext context;
    boolean remoteMode = ODatabaseRecordThreadLocal.INSTANCE.get().getStorage() instanceof OStorageProxy;
    if (remoteMode) {
      context = null;
    } else
      context = ORecordSerializationContext.getContext();

    // make sure that we really save underlying record.
    if (collectionPointer == null) {
      if (context != null) {
        final int clusterId = getHighLevelDocClusterId();
        assert clusterId > -1;
        collectionPointer = ODatabaseRecordThreadLocal.INSTANCE.get().getSbTreeCollectionManager()
            .createSBTree(clusterId, ownerUuid);
      }
    }

    OBonsaiCollectionPointer collectionPointer;
    if (this.collectionPointer != null)
      collectionPointer = this.collectionPointer;
    else {
      collectionPointer = OBonsaiCollectionPointer.INVALID;
    }

    OLongSerializer.INSTANCE.serializeLiteral(collectionPointer.getFileId(), stream, offset);
    offset += OLongSerializer.LONG_SIZE;

    OBonsaiBucketPointer rootPointer = collectionPointer.getRootPointer();
    OLongSerializer.INSTANCE.serializeLiteral(rootPointer.getPageIndex(), stream, offset);
    offset += OLongSerializer.LONG_SIZE;

    OIntegerSerializer.INSTANCE.serializeLiteral(rootPointer.getPageOffset(), stream, offset);
    offset += OIntegerSerializer.INT_SIZE;

    // Keep this section for binary compatibility with versions older then 1.7.5
    OIntegerSerializer.INSTANCE.serializeLiteral(size, stream, offset);
    offset += OIntegerSerializer.INT_SIZE;

    if (context == null) {
      ChangeSerializationHelper.INSTANCE.serializeChanges(changes, OLinkSerializer.INSTANCE, stream, offset);
    } else {

      ODatabaseDocumentInternal db = ODatabaseRecordThreadLocal.INSTANCE.getIfDefined();
      for (Entry<OIdentifiable, Change> change : this.changes.entrySet()) {
        OIdentifiable key = change.getKey();
        if (db != null && db.getTransaction().isActive()) {
          if (!key.getIdentity().isPersistent()) {
            OIdentifiable newKey = db.getTransaction().getRecord(key.getIdentity());
            if (newKey != null) {
              changes.remove(key);
              changes.put(newKey, change.getValue());
            }
          }
        }
      }
      this.collectionPointer = collectionPointer;
      context.push(new ORidBagUpdateSerializationOperation(changes, collectionPointer));

      // 0-length serialized list of changes
      OIntegerSerializer.INSTANCE.serializeLiteral(0, stream, offset);
      offset += OIntegerSerializer.INT_SIZE;
    }

    return offset;
  }

  private void applyNewEntries() {
    for (Entry<OIdentifiable, OModifiableInteger> entry : newEntries.entrySet()) {
      OIdentifiable identifiable = entry.getKey();
      assert identifiable instanceof ORecord;
      Change c = changes.get(identifiable);

      final int delta = entry.getValue().intValue();
      if (c == null)
        changes.put(identifiable, new DiffChange(delta));
      else
        c.applyDiff(delta);
    }
    newEntries.clear();
  }

  public void clearChanges() {
    changes.clear();
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
    if (changeListeners != null)
      changeListeners.clear();
    changeListeners = null;
  }

  @Override
  public int deserialize(byte[] stream, int offset) {
    final long fileId = OLongSerializer.INSTANCE.deserializeLiteral(stream, offset);
    offset += OLongSerializer.LONG_SIZE;

    final long pageIndex = OLongSerializer.INSTANCE.deserializeLiteral(stream, offset);
    offset += OLongSerializer.LONG_SIZE;

    final int pageOffset = OIntegerSerializer.INSTANCE.deserializeLiteral(stream, offset);
    offset += OIntegerSerializer.INT_SIZE;

    // Cached bag size. Not used after 1.7.5
    offset += OIntegerSerializer.INT_SIZE;

    if (fileId == -1)
      collectionPointer = null;
    else
      collectionPointer = new OBonsaiCollectionPointer(fileId, new OBonsaiBucketPointer(pageIndex, pageOffset));

    this.size = -1;

    changes.putAll(ChangeSerializationHelper.INSTANCE.deserializeChanges(stream, offset));

    offset += OIntegerSerializer.INT_SIZE + (OLinkSerializer.RID_SIZE + Change.SIZE) * changes.size();

    return offset;
  }

  public OBonsaiCollectionPointer getCollectionPointer() {
    return collectionPointer;
  }

  public void setCollectionPointer(OBonsaiCollectionPointer collectionPointer) {
    this.collectionPointer = collectionPointer;
  }

  @Override
  public List<OMultiValueChangeListener<OIdentifiable, OIdentifiable>> getChangeListeners() {
    if (changeListeners == null)
      return Collections.emptyList();
    return Collections.unmodifiableList(changeListeners);
  }

  public void fireCollectionChangedEvent(final OMultiValueChangeEvent<OIdentifiable, OIdentifiable> event) {
    if (changeListeners != null) {
      for (final OMultiValueChangeListener<OIdentifiable, OIdentifiable> changeListener : changeListeners) {
        if (changeListener != null)
          changeListener.onAfterRecordChanged(event);
      }
    }
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

  private void mergeDiffEntry(OIdentifiable key, int diff) {
    if (diff > 0) {
      for (int i = 0; i < diff; i++) {
        add(key);
      }
    } else {
      for (int i = diff; i < 0; i++) {
        remove(key);
      }
    }
  }

  private AbsoluteChange getAbsoluteValue(OIdentifiable identifiable) {
    final OSBTreeBonsai<OIdentifiable, Integer> tree = loadTree();
    try {
      Integer oldValue;

      if (tree == null)
        oldValue = 0;
      else
        oldValue = tree.get(identifiable);

      if (oldValue == null)
        oldValue = 0;

      final Change change = changes.get(identifiable);

      return new AbsoluteChange(change == null ? oldValue : change.applyTo(oldValue));
    } finally {
      releaseTree();
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

  private int getChangesSerializedSize() {
    Set<OIdentifiable> changedIds = new HashSet<OIdentifiable>(changes.keySet());
    changedIds.addAll(newEntries.keySet());
    return ChangeSerializationHelper.INSTANCE.getChangesSerializedSize(changedIds.size());
  }

  private int getHighLevelDocClusterId() {
    ORecordElement owner = this.owner;
    while (owner != null && owner.getOwner() != null) {
      owner = owner.getOwner();
    }

    if (owner != null)
      return ((OIdentifiable) owner).getIdentity().getClusterId();

    return -1;
  }

  /**
   * Removes entry with given key from {@link #newEntries}.
   *
   * @param identifiable key to remove
   *
   * @return true if entry have been removed
   */
  private boolean removeFromNewEntries(final OIdentifiable identifiable) {
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

  public void debugPrint(PrintStream writer) throws IOException {
    OSBTreeBonsai<OIdentifiable, Integer> tree = loadTree();
    if (tree instanceof OSBTreeBonsaiLocal) {
      ((OSBTreeBonsaiLocal) tree).debugPrintBucket(writer);
    }
  }

  public NavigableMap<OIdentifiable, Change> getChanges() {
    applyNewEntries();
    return changes;
  }

  @Override
  public void replace(OMultiValueChangeEvent<Object, Object> event, Object newValue) {
    //do nothing not needed
  }
}
