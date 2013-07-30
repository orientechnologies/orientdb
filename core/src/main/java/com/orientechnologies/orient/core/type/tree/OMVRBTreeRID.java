/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Kersion 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.core.type.tree;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;

import com.orientechnologies.common.collection.OLazyIterator;
import com.orientechnologies.common.collection.OMVRBTreeEntry;
import com.orientechnologies.common.concur.resource.OSharedResourceAdaptiveExternal;
import com.orientechnologies.common.concur.resource.OSharedResourceIterator;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.OLazyRecordIterator;
import com.orientechnologies.orient.core.db.record.OLazyRecordMultiIterator;
import com.orientechnologies.orient.core.db.record.OMultiValueChangeEvent;
import com.orientechnologies.orient.core.db.record.OMultiValueChangeListener;
import com.orientechnologies.orient.core.db.record.ORecordLazyMultiValue;
import com.orientechnologies.orient.core.db.record.OTrackedMultiValue;
import com.orientechnologies.orient.core.exception.OTransactionException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.tx.OTransaction;
import com.orientechnologies.orient.core.type.tree.provider.OMVRBTreeProvider;
import com.orientechnologies.orient.core.type.tree.provider.OMVRBTreeRIDEntryProvider;
import com.orientechnologies.orient.core.type.tree.provider.OMVRBTreeRIDProvider;

/**
 * Persistent MVRB-Tree Set implementation.
 * 
 */
public class OMVRBTreeRID extends OMVRBTreePersistent<OIdentifiable, OIdentifiable> implements
    OTrackedMultiValue<OIdentifiable, OIdentifiable>, ORecordLazyMultiValue {
  private IdentityHashMap<ORecord<?>, Object>                          newEntries;
  private boolean                                                      autoConvertToRecord = true;
  private Set<OMultiValueChangeListener<OIdentifiable, OIdentifiable>> changeListeners     = Collections
                                                                                               .newSetFromMap(new WeakHashMap<OMultiValueChangeListener<OIdentifiable, OIdentifiable>, Boolean>());

  private static final Object                                          NEWMAP_VALUE        = new Object();
  private static final long                                            serialVersionUID    = 1L;
  private static OSharedResourceAdaptiveExternal                       lock                = new OSharedResourceAdaptiveExternal(
                                                                                               OGlobalConfiguration.ENVIRONMENT_CONCURRENT
                                                                                                   .getValueAsBoolean(),
                                                                                               OGlobalConfiguration.MVRBTREE_TIMEOUT
                                                                                                   .getValueAsInteger(), true);

  public OMVRBTreeRID(Collection<OIdentifiable> iInitValues) {
    this();
    putAll(iInitValues);
  }

  public OMVRBTreeRID() {
    this(new OMVRBTreeRIDProvider(null, ODatabaseRecordThreadLocal.INSTANCE.get().getClusterIdByName(
        OMVRBTreeRIDProvider.PERSISTENT_CLASS_NAME)));
  }

  public OMVRBTreeRID(final ODocument iRecord) {
    this(new OMVRBTreeRIDProvider(((OIdentifiable) iRecord.field("root")).getIdentity()));
    load();
  }

  public OMVRBTreeRID(final String iClusterName) {
    this(new OMVRBTreeRIDProvider(iClusterName));
  }

  public OMVRBTreeRID(final OMVRBTreeProvider<OIdentifiable, OIdentifiable> iProvider) {
    super(iProvider);
    ((OMVRBTreeRIDProvider) dataProvider).setTree(this);
  }

  /**
   * Copy constructor
   * 
   * @param iSource
   *          Source object
   */
  public OMVRBTreeRID(final OMVRBTreeRID iSource) {
    super(new OMVRBTreeRIDProvider((OMVRBTreeRIDProvider) iSource.getProvider()));
    ((OMVRBTreeRIDProvider) dataProvider).setTree(this);

    if (iSource.getProvider().isDirty() && ((OMVRBTreeRIDProvider) iSource.getProvider()).isEmbeddedStreaming())
      putAll(iSource.keySet());
    else
      load();
  }

  @Override
  public OMVRBTreePersistent<OIdentifiable, OIdentifiable> setOwner(final ORecord<?> owner) {
    super.setOwner(owner);
    return this;
  }

  @Override
  public OMVRBTreePersistent<OIdentifiable, OIdentifiable> load() {
    lock.acquireExclusiveLock();
    try {

      newEntries = null;
      super.load();
      if (root != null)
        setSize(((OMVRBTreeRIDEntryProvider) ((OMVRBTreeEntryPersistent<OIdentifiable, OIdentifiable>) root).getProvider())
            .getTreeSize());
      else
        setSize(0);
      return this;

    } finally {
      lock.releaseExclusiveLock();
    }
  }

  @Override
  public OIdentifiable internalPut(final OIdentifiable e, final OIdentifiable v) {
    lock.acquireExclusiveLock();
    try {

      if (e == null)
        return null;

      ((OMVRBTreeRIDProvider) dataProvider).lazyUnmarshall();

      if (e.getIdentity().isNew()) {
        final ORecord<?> record = e.getRecord();

        if (record == null)
          throw new OTransactionException("Cannot insert item in mvrb-tree because the transactional item was not found.");

        // ADD IN TEMP LIST
        if (newEntries == null)
          newEntries = new IdentityHashMap<ORecord<?>, Object>();
        else if (newEntries.containsKey(record))
          return record;
        newEntries.put(record, NEWMAP_VALUE);
        setDirty();
        return null;
      }

      final OIdentifiable oldValue = super.internalPut(e, null);

      if (oldValue != null)
        fireCollectionChangedEvent(new OMultiValueChangeEvent<OIdentifiable, OIdentifiable>(OMultiValueChangeEvent.OChangeType.ADD,
            e, v, oldValue));

      return oldValue;

    } finally {
      lock.releaseExclusiveLock();
    }
  }

  public void putAll(final Collection<OIdentifiable> coll) {
    final long timer = PROFILER.startChrono();

    try {
      for (OIdentifiable rid : coll)
        internalPut(rid, null);

      commitChanges();

    } finally {
      PROFILER.stopChrono(PROFILER.getProcessMetric("mvrbtree.putAll"), "Put multiple values in a MVRBTreeRID", timer);
    }
  }

  public OIdentifiable remove(final Object o) {
    lock.acquireExclusiveLock();
    try {

      final OIdentifiable removed;

      if (hasNewItems() && newEntries.containsKey(o)) {
        // REMOVE IT INSIDE NEW ITEMS MAP
        removed = (OIdentifiable) o;
        newEntries.remove(o);
        if (newEntries.size() == 0)
          // EARLY REMOVE THE MAP TO SAVE MEMORY
          newEntries = null;
        setDirty();
      } else {
        if (containsKey(o)) {
          removed = super.remove(o);
          setDirty();
        } else
          removed = null;
      }

      fireCollectionChangedEvent(new OMultiValueChangeEvent<OIdentifiable, OIdentifiable>(
          OMultiValueChangeEvent.OChangeType.REMOVE, (OIdentifiable) o, null, (OIdentifiable) o));

      return removed;

    } finally {
      lock.releaseExclusiveLock();
    }
  }

  public boolean removeAll(final Collection<?> c) {
    lock.acquireExclusiveLock();
    try {

      ((OMVRBTreeRIDProvider) dataProvider).lazyUnmarshall();

      if (hasNewItems()) {
        final Collection<ORecord<?>> v = newEntries.keySet();
        v.removeAll(c);
        if (newEntries.size() == 0)
          newEntries = null;
      }

      boolean modified = false;
      for (Object o : c)
        if (remove(o) != null)
          modified = true;
      return modified;

    } finally {
      lock.releaseExclusiveLock();
    }
  }

  public boolean retainAll(final Collection<?> c) {
    lock.acquireExclusiveLock();
    try {

      ((OMVRBTreeRIDProvider) dataProvider).lazyUnmarshall();
      if (hasNewItems()) {
        final Collection<ORecord<?>> v = newEntries.keySet();
        v.retainAll(c);
        if (newEntries.size() == 0)
          newEntries = null;
      }

      boolean modified = false;
      final Iterator<?> e = iterator();
      while (e.hasNext()) {
        if (!c.contains(e.next())) {
          e.remove();
          modified = true;
        }
      }
      return modified;

    } finally {
      lock.releaseExclusiveLock();
    }
  }

  @Override
  public void clear() {
    lock.acquireExclusiveLock();
    try {

      if (newEntries != null) {
        newEntries.clear();
        newEntries = null;
      }
      setDirty();

      final Map<OIdentifiable, OIdentifiable> origValues;
      if (changeListeners.isEmpty())
        origValues = null;
      else
        origValues = new HashMap<OIdentifiable, OIdentifiable>(this);

      super.clear();

      if (origValues != null) {
        for (final java.util.Map.Entry<OIdentifiable, OIdentifiable> item : origValues.entrySet())
          fireCollectionChangedEvent(new OMultiValueChangeEvent<OIdentifiable, OIdentifiable>(
              OMultiValueChangeEvent.OChangeType.REMOVE, item.getKey(), null, item.getValue()));
      } else
        setDirty();

    } finally {
      lock.releaseExclusiveLock();
    }
  }

  public boolean detach() {
    return saveAllNewEntries();
  }

  @Override
  public int size() {
    lock.acquireExclusiveLock();
    try {

      int tot = getTreeSize();
      if (newEntries != null)
        tot += newEntries.size();
      return tot;

    } finally {
      lock.releaseExclusiveLock();
    }
  }

  public int getTreeSize() {
    lock.acquireExclusiveLock();
    try {

      ((OMVRBTreeRIDProvider) dataProvider).lazyUnmarshall();
      return super.getTreeSize();

    } finally {
      lock.releaseExclusiveLock();
    }
  }

  @Override
  public boolean isEmpty() {
    lock.acquireExclusiveLock();
    try {

      ((OMVRBTreeRIDProvider) dataProvider).lazyUnmarshall();
      boolean empty = super.isEmpty();

      if (empty && newEntries != null)
        empty = newEntries.isEmpty();

      return empty;

    } finally {
      lock.releaseExclusiveLock();
    }
  }

  @Override
  public boolean containsKey(final Object o) {
    lock.acquireExclusiveLock();
    try {

      ((OMVRBTreeRIDProvider) dataProvider).lazyUnmarshall();
      boolean found = super.containsKey(o);

      if (!found && hasNewItems())
        // SEARCH INSIDE NEW ITEMS MAP
        found = newEntries.containsKey(o);

      return found;

    } finally {
      lock.releaseExclusiveLock();
    }
  }

  public Iterator<OIdentifiable> iterator() {
    lock.acquireExclusiveLock();
    try {

      return new OSharedResourceIterator<OIdentifiable>(lock, iterator(autoConvertToRecord));

    } finally {
      lock.releaseExclusiveLock();
    }
  }

  public OLazyIterator<OIdentifiable> iterator(final boolean iAutoConvertToRecord) {
    lock.acquireExclusiveLock();
    try {

      ((OMVRBTreeRIDProvider) dataProvider).lazyUnmarshall();
      if (hasNewItems()) {
        if (super.size() == 0)
          return new OLazyRecordIterator(new HashSet<OIdentifiable>(newEntries.keySet()), iAutoConvertToRecord);

        // MIX PERSISTENT AND NEW TOGETHER
        return new OLazyRecordMultiIterator(null, new Object[] { keySet(), new HashSet<OIdentifiable>(newEntries.keySet()) },
            iAutoConvertToRecord);
      }

      return new OLazyRecordIterator(keySet().iterator(), iAutoConvertToRecord);

    } finally {
      lock.releaseExclusiveLock();
    }
  }

  @Override
  public Set<OIdentifiable> keySet() {
    lock.acquireExclusiveLock();
    try {

      ((OMVRBTreeRIDProvider) dataProvider).lazyUnmarshall();
      return super.keySet();

    } finally {
      lock.releaseExclusiveLock();
    }
  }

  @Override
  public Collection<OIdentifiable> values() {
    lock.acquireExclusiveLock();
    try {

      ((OMVRBTreeRIDProvider) dataProvider).lazyUnmarshall();
      return super.values();

    } finally {
      lock.releaseExclusiveLock();
    }
  }

  public Object[] toArray() {
    lock.acquireExclusiveLock();
    try {

      Object[] result = keySet().toArray();
      if (newEntries != null && !newEntries.isEmpty()) {
        int start = result.length;
        result = Arrays.copyOf(result, start + newEntries.size());

        for (ORecord<?> r : newEntries.keySet()) {
          result[start++] = r;
        }
      }

      return result;

    } finally {
      lock.releaseExclusiveLock();
    }
  }

  @SuppressWarnings("unchecked")
  public <T> T[] toArray(final T[] a) {
    lock.acquireExclusiveLock();
    try {

      T[] result = keySet().toArray(a);

      if (newEntries != null && !newEntries.isEmpty()) {
        int start = result.length;
        result = Arrays.copyOf(result, start + newEntries.size());

        for (ORecord<?> r : newEntries.keySet()) {
          result[start++] = (T) r;
        }
      }

      return result;

    } finally {
      lock.releaseExclusiveLock();
    }
  }

  @Override
  protected void saveTreeNode() {
  }

  @Override
  public int commitChanges() {
    lock.acquireExclusiveLock();
    try {

      if (!((OMVRBTreeRIDProvider) getProvider()).isEmbeddedStreaming()) {
        saveAllNewEntries();
        return super.commitChanges();
      }
      return 0;

    } finally {
      lock.releaseExclusiveLock();
    }
  }

  /**
   * Do nothing since the set is early saved
   */
  public OMVRBTreePersistent<OIdentifiable, OIdentifiable> save() {
    return this;
  }

  @Override
  protected void setSizeDelta(final int iDelta) {
    lock.acquireExclusiveLock();
    try {

      setSize(getTreeSize() + iDelta);

    } finally {
      lock.releaseExclusiveLock();
    }
  }

  /**
   * Notifies to the owner the change
   */
  public void setDirtyOwner() {
    lock.acquireExclusiveLock();
    try {

      if (getOwner() != null)
        getOwner().setDirty();

    } finally {
      lock.releaseExclusiveLock();
    }
  }

  public void onAfterTxCommit() {
    lock.acquireExclusiveLock();
    try {

      final Set<ORID> nodesInMemory = getAllNodesInCache();

      if (nodesInMemory.isEmpty())
        return;

      // FIX THE CACHE CONTENT WITH FINAL RECORD-IDS
      final Set<ORID> keys = new HashSet<ORID>(nodesInMemory);
      OMVRBTreeEntryPersistent<OIdentifiable, OIdentifiable> entry;
      for (ORID rid : keys) {
        if (rid.getClusterPosition().isTemporary()) {
          // FIX IT IN CACHE
          entry = (OMVRBTreeEntryPersistent<OIdentifiable, OIdentifiable>) searchNodeInCache(rid);

          // OVERWRITE IT WITH THE NEW RID
          removeNodeFromCache(rid);
          addNodeInCache(entry);
        }
      }

    } finally {
      lock.releaseExclusiveLock();
    }
  }

  /**
   * Returns true if all the new entries are saved as persistent, otherwise false.
   */
  public boolean saveAllNewEntries() {
    lock.acquireExclusiveLock();
    try {

      if (hasNewItems()) {
        // TRIES TO SAVE THE NEW ENTRIES
        final Set<ORecord<?>> temp = new HashSet<ORecord<?>>(newEntries.keySet());

        for (ORecord<?> record : temp) {
          if (record.getIdentity().isNew())
            record.save();

          if (!record.getIdentity().isNew()) {
            // SAVED CORRECTLY (=NO IN TX): MOVE IT INTO THE PERSISTENT TREE
            if (newEntries != null) {
              newEntries.remove(record);
              if (newEntries.size() == 0)
                newEntries = null;
            }

            // PUT THE ITEM INTO THE TREE
            internalPut(record.getIdentity(), null);
          }
        }

        if (!((OMVRBTreeRIDProvider) dataProvider).isEmbeddedStreaming())
          // SAVE ALL AT THE END
          super.commitChanges();

        if (newEntries != null)
          // SOMETHING IS TEMPORARY YET
          return false;
      }
      return true;

    } finally {
      lock.releaseExclusiveLock();
    }
  }

  public boolean hasNewItems() {
    lock.acquireExclusiveLock();
    try {

      return newEntries != null && !newEntries.isEmpty();

    } finally {
      lock.releaseExclusiveLock();
    }
  }

  @Override
  public String toString() {
    lock.acquireExclusiveLock();
    try {

      ((OMVRBTreeRIDProvider) dataProvider).lazyUnmarshall();
      final StringBuilder buffer = new StringBuilder(super.toString());
      if (hasNewItems()) {
        buffer.append("{new items (");
        buffer.append(newEntries.size());
        buffer.append("): ");
        boolean first = true;
        for (ORecord<?> item : newEntries.keySet()) {
          if (!first) {
            buffer.append(", ");
            first = false;
          }

          if (item != null)
            buffer.append(item.toString());
        }
        buffer.append("}");
      }
      return buffer.toString();

    } finally {
      lock.releaseExclusiveLock();
    }
  }

  @Override
  protected void setRoot(final OMVRBTreeEntry<OIdentifiable, OIdentifiable> iRoot) {
    lock.acquireExclusiveLock();
    try {

      int size = 0;
      if (iRoot != null)
        size = getTreeSize();

      super.setRoot(iRoot);

      if (iRoot != null)
        setSize(size);

    } finally {
      lock.releaseExclusiveLock();
    }
  }

  /**
   * Notifies the changes to the owner if it's embedded.
   */
  @SuppressWarnings("unchecked")
  protected <RET> RET setDirty() {
    lock.acquireExclusiveLock();
    try {

      ((OMVRBTreeRIDProvider) getProvider()).setDirty();

      if (((OMVRBTreeRIDProvider) getProvider()).isEmbeddedStreaming())
        setDirtyOwner();
      else if (ODatabaseRecordThreadLocal.INSTANCE.get().getTransaction().getStatus() != OTransaction.TXSTATUS.BEGUN)
        // SAVE IT RIGHT NOW SINCE IT'S DISCONNECTED FROM OWNER
        save();

      return (RET) this;

    } finally {
      lock.releaseExclusiveLock();
    }
  }

  public IdentityHashMap<ORecord<?>, Object> getTemporaryEntries() {
    return newEntries;
  }

  protected void fireCollectionChangedEvent(final OMultiValueChangeEvent<OIdentifiable, OIdentifiable> event) {
    lock.acquireExclusiveLock();
    try {

      setDirty();
      for (final OMultiValueChangeListener<OIdentifiable, OIdentifiable> changeListener : changeListeners) {
        if (changeListener != null)
          changeListener.onAfterRecordChanged(event);
      }

    } finally {
      lock.releaseExclusiveLock();
    }
  }

  @Override
  public void addChangeListener(OMultiValueChangeListener<OIdentifiable, OIdentifiable> changeListener) {
    lock.acquireExclusiveLock();
    try {

      changeListeners.add(changeListener);

    } finally {
      lock.releaseExclusiveLock();
    }
  }

  @Override
  public void removeRecordChangeListener(OMultiValueChangeListener<OIdentifiable, OIdentifiable> changeListener) {
    lock.acquireExclusiveLock();
    try {

      changeListeners.remove(changeListener);

    } finally {
      lock.releaseExclusiveLock();
    }
  }

  @Override
  public Object returnOriginalState(List<OMultiValueChangeEvent<OIdentifiable, OIdentifiable>> changeEvents) {
    lock.acquireExclusiveLock();
    try {

      final Map<OIdentifiable, OIdentifiable> reverted = new HashMap<OIdentifiable, OIdentifiable>(this);

      final ListIterator<OMultiValueChangeEvent<OIdentifiable, OIdentifiable>> listIterator = changeEvents
          .listIterator(changeEvents.size());

      while (listIterator.hasPrevious()) {
        final OMultiValueChangeEvent<OIdentifiable, OIdentifiable> event = listIterator.previous();
        switch (event.getChangeType()) {
        case ADD:
          reverted.remove(event.getKey());
          break;
        case REMOVE:
          reverted.put(event.getKey(), event.getOldValue());
          break;
        case UPDATE:
          reverted.put(event.getKey(), event.getOldValue());
          break;
        default:
          throw new IllegalArgumentException("Invalid change type : " + event.getChangeType());
        }
      }

      return reverted;

    } finally {
      lock.releaseExclusiveLock();
    }
  }

  @Override
  public Class<?> getGenericClass() {
    return null;
  }

  @Override
  public Iterator<OIdentifiable> rawIterator() {
    lock.acquireExclusiveLock();
    try {

      return new OSharedResourceIterator<OIdentifiable>(lock, iterator(false));

    } finally {
      lock.releaseExclusiveLock();
    }
  }

  @Override
  public void convertLinks2Records() {
  }

  @Override
  public boolean convertRecords2Links() {
    return false;
  }

  @Override
  public boolean isAutoConvertToRecord() {
    return autoConvertToRecord;
  }

  @Override
  public void setAutoConvertToRecord(boolean convertToRecord) {
    autoConvertToRecord = convertToRecord;
  }

  public static OSharedResourceAdaptiveExternal getLock() {
    return lock;
  }
}
