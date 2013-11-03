/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
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
package com.orientechnologies.orient.core.index.engine;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import com.orientechnologies.common.collection.OMVRBTree;
import com.orientechnologies.common.collection.OMVRBTreeEntry;
import com.orientechnologies.common.comparator.ODefaultComparator;
import com.orientechnologies.common.concur.resource.OSharedResourceAdaptiveExternal;
import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.common.profiler.OProfilerMBean;
import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.ODocumentFieldsHashSet;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.index.OIndexEngine;
import com.orientechnologies.orient.core.index.ORuntimeKeyIndexDefinition;
import com.orientechnologies.orient.core.memory.OMemoryWatchDog;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.binary.OBinarySerializerFactory;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.index.OCompositeKeySerializer;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.index.OSimpleKeySerializer;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializer;
import com.orientechnologies.orient.core.type.tree.OMVRBTreeDatabaseLazySave;
import com.orientechnologies.orient.core.type.tree.provider.OMVRBTreeProviderAbstract;

/**
 * @author Andrey Lomakin
 * @since 6/29/13
 */
public final class OMVRBTreeIndexEngine<V> extends OSharedResourceAdaptiveExternal implements OIndexEngine<V> {
  private int                                  maxUpdatesBeforeSave;
  private OMemoryWatchDog.Listener             watchDog;
  private OMVRBTreeDatabaseLazySave<Object, V> map;

  public OMVRBTreeIndexEngine() {
    super(OGlobalConfiguration.ENVIRONMENT_CONCURRENT.getValueAsBoolean(), OGlobalConfiguration.MVRBTREE_TIMEOUT
        .getValueAsInteger(), true);
  }

  @Override
  public void init() {
    acquireExclusiveLock();
    try {
      watchDog = new OMemoryWatchDog.Listener() {
        public void memoryUsageLow(final long iFreeMemory, final long iFreeMemoryPercentage) {
          map.setOptimization(iFreeMemoryPercentage < 10 ? 2 : 1);
        }
      };
    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  public void flush() {
    acquireExclusiveLock();
    try {
      map.lazySave();
    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  public void create(String indexName, OIndexDefinition indexDefinition, String clusterIndexName,
      OStreamSerializer valueSerializer, boolean isAutomatic) {
    acquireExclusiveLock();
    try {
      maxUpdatesBeforeSave = lazyUpdates(isAutomatic);
      if (indexDefinition != null) {
        if (indexDefinition instanceof ORuntimeKeyIndexDefinition) {
          map = new OMVRBTreeDatabaseLazySave<Object, V>(clusterIndexName,
              ((ORuntimeKeyIndexDefinition) indexDefinition).getSerializer(), valueSerializer, 1, maxUpdatesBeforeSave);
        } else {
          final OBinarySerializer<?> keySerializer;
          if (indexDefinition.getTypes().length > 1) {
            keySerializer = OCompositeKeySerializer.INSTANCE;
          } else {
            keySerializer = OBinarySerializerFactory.INSTANCE.getObjectSerializer(indexDefinition.getTypes()[0]);
          }
          map = new OMVRBTreeDatabaseLazySave<Object, V>(clusterIndexName, (OBinarySerializer<Object>) keySerializer,
              valueSerializer, indexDefinition.getTypes().length, maxUpdatesBeforeSave);
        }
      } else
        map = new OMVRBTreeDatabaseLazySave<Object, V>(clusterIndexName, new OSimpleKeySerializer(), valueSerializer, 1,
            maxUpdatesBeforeSave);

      installHooks(indexName);
    } finally {
      releaseExclusiveLock();
    }
  }

  private void installHooks(String indexName) {
    final OProfilerMBean profiler = Orient.instance().getProfiler();
    final String profilerPrefix = profiler.getDatabaseMetric(getDatabase().getName(), "index." + indexName + '.');
    final String profilerMetadataPrefix = "db.*.index.*.";

    profiler.registerHookValue(profilerPrefix + "items", "Index size", OProfiler.METRIC_TYPE.SIZE,
        new OProfiler.OProfilerHookValue() {
          public Object getValue() {
            acquireSharedLock();
            try {
              return map != null ? map.size() : "-";
            } finally {
              releaseSharedLock();
            }
          }
        }, profilerMetadataPrefix + "items");

    profiler.registerHookValue(profilerPrefix + "entryPointSize", "Number of entrypoints in an index", OProfiler.METRIC_TYPE.SIZE,
        new OProfiler.OProfilerHookValue() {
          public Object getValue() {
            return map != null ? map.getEntryPointSize() : "-";
          }
        }, profilerMetadataPrefix + "items");

    profiler.registerHookValue(profilerPrefix + "maxUpdateBeforeSave", "Maximum number of updates in a index before force saving",
        OProfiler.METRIC_TYPE.SIZE, new OProfiler.OProfilerHookValue() {
          public Object getValue() {
            return map != null ? map.getMaxUpdatesBeforeSave() : "-";
          }
        }, profilerMetadataPrefix + "maxUpdateBeforeSave");

    Orient.instance().getMemoryWatchDog().addListener(watchDog);

  }

  @Override
  public void delete() {
    acquireExclusiveLock();
    try {
      if (map != null)
        map.delete();
    } finally {
      releaseExclusiveLock();
    }
  }

  public void load(ORID indexRid, String indexName, OIndexDefinition indexDefinition, boolean isAutomatic) {
    acquireExclusiveLock();
    try {
      maxUpdatesBeforeSave = lazyUpdates(isAutomatic);
      map = new OMVRBTreeDatabaseLazySave<Object, V>(getDatabase(), indexRid, maxUpdatesBeforeSave);
      map.load();
      installHooks(indexName);
    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  public boolean contains(Object key) {
    acquireExclusiveLock();
    try {
      return map.containsKey(key);
    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  public ORID getIdentity() {
    acquireSharedLock();
    try {
      return ((OMVRBTreeProviderAbstract<Object, ?>) map.getProvider()).getRecord().getIdentity();
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public void clear() {
    acquireExclusiveLock();
    try {
      map.clear();
    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  public boolean remove(Object key) {
    acquireExclusiveLock();
    try {
      return map.remove(key) != null;
    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  public int removeValue(OIdentifiable valueToRemove, ValuesTransformer<V> transformer) {
    acquireExclusiveLock();
    try {
      Map<Object, V> entriesToUpdate = new TreeMap<Object, V>(ODefaultComparator.INSTANCE);
      final OMVRBTreeEntry<Object, V> firstEntry = map.getFirstEntry();
      if (firstEntry == null)
        return 0;

      OMVRBTreeEntry<Object, V> entry = firstEntry;
      while (entry != null) {
        final Object key = entry.getKey();
        final V value = entry.getValue();

        if (transformer != null) {
          Collection<OIdentifiable> rids = transformer.transformFromValue(value);
          if (rids.remove(valueToRemove)) {
            entriesToUpdate.put(key, transformer.transformToValue(rids));
          }
        } else if (value.equals(valueToRemove)) {
          entriesToUpdate.put(key, value);
        }

        entry = OMVRBTree.next(entry);
      }

      for (Map.Entry<Object, V> entryToUpdate : entriesToUpdate.entrySet()) {
        V value = entryToUpdate.getValue();
        if (value instanceof Collection) {
          Collection col = (Collection) value;
          if (col.isEmpty())
            map.remove(entryToUpdate.getKey());
          else
            map.put(entryToUpdate.getKey(), value);
        } else
          map.remove(entryToUpdate.getKey());

      }

      return entriesToUpdate.size();
    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  public Iterator<Map.Entry<Object, V>> iterator() {
    acquireExclusiveLock();
    try {
      return map.entrySet().iterator();
    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  public Iterator<Map.Entry<Object, V>> inverseIterator() {
    acquireExclusiveLock();
    try {
      return ((OMVRBTree.EntrySet) map.entrySet()).inverseIterator();
    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  public Iterable<Object> keys() {
    acquireExclusiveLock();
    try {
      return map.keySet();

    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  public void unload() {
    acquireExclusiveLock();
    try {
      map.unload();
    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  public void startTransaction() {
    acquireExclusiveLock();
    try {
      map.setRunningTransaction(true);
    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  public void stopTransaction() {
    acquireExclusiveLock();
    try {
      map.setRunningTransaction(false);
    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  public void afterTxRollback() {
    acquireExclusiveLock();
    try {
      map.unload();
    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  public void afterTxCommit() {
    acquireExclusiveLock();
    try {
      map.onAfterTxCommit();
    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  public void closeDb() {
    acquireExclusiveLock();
    try {
      map.commitChanges(true);

      // TODO: GO IN DEEP WHY THE UNLOAD CAUSE LOOSE OF INDEX ENTRIES!
      // map.unload();
    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  public void close() {
  }

  @Override
  public void beforeTxBegin() {
    acquireExclusiveLock();
    try {
      map.commitChanges(true);
    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  public V get(Object key) {
    acquireExclusiveLock();
    try {
      return map.get(key);
    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  public void put(Object key, V value) {
    acquireExclusiveLock();
    try {
      map.put(key, value);
    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  public Collection<OIdentifiable> getValuesBetween(Object rangeFrom, boolean fromInclusive, Object rangeTo, boolean toInclusive,
      int maxValuesToFetch, ValuesTransformer<V> transformer) {
    acquireExclusiveLock();
    try {
      final OMVRBTreeEntry<Object, V> firstEntry;

      if (fromInclusive)
        firstEntry = map.getCeilingEntry(rangeFrom, OMVRBTree.PartialSearchMode.LOWEST_BOUNDARY);
      else
        firstEntry = map.getHigherEntry(rangeFrom);

      if (firstEntry == null)
        return Collections.emptySet();

      final int firstEntryIndex = map.getPageIndex();

      final OMVRBTreeEntry<Object, V> lastEntry;

      if (toInclusive)
        lastEntry = map.getHigherEntry(rangeTo);
      else
        lastEntry = map.getCeilingEntry(rangeTo, OMVRBTree.PartialSearchMode.LOWEST_BOUNDARY);

      final int lastEntryIndex;

      if (lastEntry != null)
        lastEntryIndex = map.getPageIndex();
      else
        lastEntryIndex = -1;

      OMVRBTreeEntry<Object, V> entry = firstEntry;
      map.setPageIndex(firstEntryIndex);

      final Set<OIdentifiable> result = new HashSet<OIdentifiable>();

      while (entry != null && !(entry == lastEntry && map.getPageIndex() == lastEntryIndex)) {
        final V value = entry.getValue();

        addToResult(transformer, result, value, maxValuesToFetch);

        if (maxValuesToFetch > -1 && result.size() == maxValuesToFetch)
          return result;

        entry = OMVRBTree.next(entry);
      }

      return result;

    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  public Collection<OIdentifiable> getValuesMajor(Object fromKey, boolean isInclusive, int maxValuesToFetch,
      ValuesTransformer<V> transformer) {
    acquireExclusiveLock();
    try {

      final OMVRBTreeEntry<Object, V> firstEntry;
      if (isInclusive)
        firstEntry = map.getCeilingEntry(fromKey, OMVRBTree.PartialSearchMode.LOWEST_BOUNDARY);
      else
        firstEntry = map.getHigherEntry(fromKey);

      if (firstEntry == null)
        return Collections.emptySet();

      OMVRBTreeEntry<Object, V> entry = firstEntry;

      final Set<OIdentifiable> result = new HashSet<OIdentifiable>();

      while (entry != null) {
        final V value = entry.getValue();
        addToResult(transformer, result, value, maxValuesToFetch);

        if (maxValuesToFetch > -1 && result.size() == maxValuesToFetch)
          return result;

        entry = OMVRBTree.next(entry);
      }

      return result;
    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  public Collection<OIdentifiable> getValuesMinor(Object toKey, boolean isInclusive, int maxValuesToFetch,
      ValuesTransformer<V> transformer) {
    acquireExclusiveLock();
    try {
      final OMVRBTreeEntry<Object, V> lastEntry;

      if (isInclusive)
        lastEntry = map.getFloorEntry(toKey, OMVRBTree.PartialSearchMode.HIGHEST_BOUNDARY);
      else
        lastEntry = map.getLowerEntry(toKey);

      if (lastEntry == null)
        return Collections.emptySet();

      OMVRBTreeEntry<Object, V> entry = lastEntry;

      final Set<OIdentifiable> result = new HashSet<OIdentifiable>();

      while (entry != null) {
        V value = entry.getValue();
        addToResult(transformer, result, value, maxValuesToFetch);

        if (maxValuesToFetch > -1 && result.size() == maxValuesToFetch)
          return result;

        entry = OMVRBTree.previous(entry);
      }

      return result;
    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  public Collection<ODocument> getEntriesMajor(Object fromKey, boolean isInclusive, int maxEntriesToFetch,
      ValuesTransformer<V> transformer) {
    acquireExclusiveLock();

    try {
      final OMVRBTreeEntry<Object, V> firstEntry;
      if (isInclusive)
        firstEntry = map.getCeilingEntry(fromKey, OMVRBTree.PartialSearchMode.LOWEST_BOUNDARY);
      else
        firstEntry = map.getHigherEntry(fromKey);

      if (firstEntry == null)
        return Collections.emptySet();

      OMVRBTreeEntry<Object, V> entry = firstEntry;

      final Set<ODocument> result = new ODocumentFieldsHashSet();

      while (entry != null) {
        final Object key = entry.getKey();
        final V value = entry.getValue();
        addToEntriesResult(transformer, result, key, value, maxEntriesToFetch);

        if (maxEntriesToFetch > -1 && result.size() == maxEntriesToFetch)
          return result;

        entry = OMVRBTree.next(entry);
      }

      return result;
    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  public Collection<ODocument> getEntriesMinor(Object toKey, boolean isInclusive, int maxEntriesToFetch,
      ValuesTransformer<V> transformer) {
    acquireExclusiveLock();

    try {
      final OMVRBTreeEntry<Object, V> lastEntry;

      if (isInclusive)
        lastEntry = map.getFloorEntry(toKey, OMVRBTree.PartialSearchMode.HIGHEST_BOUNDARY);
      else
        lastEntry = map.getLowerEntry(toKey);

      if (lastEntry == null)
        return Collections.emptySet();

      OMVRBTreeEntry<Object, V> entry = lastEntry;

      final Set<ODocument> result = new ODocumentFieldsHashSet();

      while (entry != null) {
        final Object key = entry.getKey();
        final V value = entry.getValue();
        addToEntriesResult(transformer, result, key, value, maxEntriesToFetch);

        if (maxEntriesToFetch > -1 && result.size() == maxEntriesToFetch)
          return result;

        entry = OMVRBTree.previous(entry);
      }

      return result;
    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  public Collection<ODocument> getEntriesBetween(Object iRangeFrom, Object iRangeTo, boolean iInclusive, int maxEntriesToFetch,
      ValuesTransformer<V> transformer) {
    acquireExclusiveLock();

    try {
      final OMVRBTreeEntry<Object, V> firstEntry;

      if (iInclusive)
        firstEntry = map.getCeilingEntry(iRangeFrom, OMVRBTree.PartialSearchMode.LOWEST_BOUNDARY);
      else
        firstEntry = map.getHigherEntry(iRangeFrom);

      if (firstEntry == null)
        return Collections.emptySet();

      final int firstEntryIndex = map.getPageIndex();

      final OMVRBTreeEntry<Object, V> lastEntry;

      if (iInclusive)
        lastEntry = map.getHigherEntry(iRangeTo);
      else
        lastEntry = map.getCeilingEntry(iRangeTo, OMVRBTree.PartialSearchMode.LOWEST_BOUNDARY);

      final int lastEntryIndex;

      if (lastEntry != null)
        lastEntryIndex = map.getPageIndex();
      else
        lastEntryIndex = -1;

      OMVRBTreeEntry<Object, V> entry = firstEntry;
      map.setPageIndex(firstEntryIndex);

      final Set<ODocument> result = new ODocumentFieldsHashSet();
      while (entry != null && !(entry == lastEntry && map.getPageIndex() == lastEntryIndex)) {
        final Object key = entry.getKey();
        final V value = entry.getValue();

        addToEntriesResult(transformer, result, key, value, maxEntriesToFetch);

        if (maxEntriesToFetch > -1 && maxEntriesToFetch == result.size())
          return result;

        entry = OMVRBTree.next(entry);
      }

      return result;
    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  public long size(ValuesTransformer<V> valuesTransformer) {
    acquireExclusiveLock();
    try {
      if (valuesTransformer == null)
        return map.size();

      OMVRBTreeEntry<Object, V> rootEntry = map.getRoot();
      long size = 0;

      OMVRBTreeEntry<Object, V> currentEntry = rootEntry;
      map.setPageIndex(0);

      while (currentEntry != null) {
        size += valuesTransformer.transformFromValue(currentEntry.getValue()).size();
        currentEntry = OMVRBTree.next(currentEntry);
      }

      map.setPageIndex(0);
      currentEntry = OMVRBTree.previous(rootEntry);

      while (currentEntry != null) {
        size += valuesTransformer.transformFromValue(currentEntry.getValue()).size();
        currentEntry = OMVRBTree.previous(currentEntry);
      }

      return size;

    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  public long count(Object rangeFrom, boolean fromInclusive, Object rangeTo, boolean toInclusive, int maxValuesToFetch,
      ValuesTransformer<V> transformer) {
    acquireExclusiveLock();
    try {
      final OMVRBTreeEntry<Object, V> firstEntry;

      if (rangeFrom == null)
        firstEntry = (OMVRBTreeEntry<Object, V>) map.firstEntry();
      else if (fromInclusive)
        firstEntry = map.getCeilingEntry(rangeFrom, OMVRBTree.PartialSearchMode.LOWEST_BOUNDARY);
      else
        firstEntry = map.getHigherEntry(rangeFrom);

      if (firstEntry == null)
        return 0;

      long count = 0;
      final int firstEntryIndex = map.getPageIndex();

      final OMVRBTreeEntry<Object, V> lastEntry;

      if (rangeFrom == null)
        lastEntry = (OMVRBTreeEntry<Object, V>) map.lastEntry();
      else if (toInclusive)
        lastEntry = map.getHigherEntry(rangeTo);
      else
        lastEntry = map.getCeilingEntry(rangeTo, OMVRBTree.PartialSearchMode.LOWEST_BOUNDARY);

      final int lastEntryIndex;

      if (lastEntry != null)
        lastEntryIndex = map.getPageIndex();
      else
        lastEntryIndex = -1;

      OMVRBTreeEntry<Object, V> entry = firstEntry;
      map.setPageIndex(firstEntryIndex);

      while (entry != null && !(entry == lastEntry && map.getPageIndex() == lastEntryIndex)) {
        final V value = entry.getValue();
        if (transformer != null)
          count += transformer.transformFromValue(value).size();
        else
          count++;

        if (maxValuesToFetch > -1 && maxValuesToFetch == count)
          return maxValuesToFetch;

        entry = OMVRBTree.next(entry);
      }

      return count;
    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  public Iterator<V> valuesIterator() {
    acquireExclusiveLock();
    try {
      return map.values().iterator();
    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  public Iterator<V> inverseValuesIterator() {
    acquireExclusiveLock();
    try {
      return ((OMVRBTree.Values) map.values()).inverseIterator();
    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  public boolean hasRangeQuerySupport() {
    return true;
  }

  private void addToResult(ValuesTransformer<V> transformer, Set<OIdentifiable> result, V value, int maxValuesToFetch) {
    if (transformer != null) {
      Collection<OIdentifiable> transformResult = transformer.transformFromValue(value);
      for (OIdentifiable transformedValue : transformResult) {

        result.add(transformedValue);
        if (maxValuesToFetch > -1 && result.size() == maxValuesToFetch)
          return;
      }

    } else
      result.add((OIdentifiable) value);
  }

  private void addToEntriesResult(ValuesTransformer<V> transformer, Set<ODocument> result, Object key, V value, int maxValuesToFetch) {
    if (transformer != null) {
      Collection<OIdentifiable> transformResult = transformer.transformFromValue(value);
      for (OIdentifiable transformedValue : transformResult) {
        final ODocument document = new ODocument();
        document.field("key", key);
        document.field("rid", transformedValue.getIdentity());
        document.unsetDirty();

        result.add(document);

        if (maxValuesToFetch > -1 && result.size() == maxValuesToFetch)
          return;
      }

    } else {
      final ODocument document = new ODocument();
      document.field("key", key);
      document.field("rid", ((OIdentifiable) value).getIdentity());
      document.unsetDirty();

      result.add(document);
    }
  }

  private ODatabaseRecord getDatabase() {
    return ODatabaseRecordThreadLocal.INSTANCE.get();
  }

  private int lazyUpdates(boolean isAutomatic) {
    return isAutomatic ? OGlobalConfiguration.INDEX_AUTO_LAZY_UPDATES.getValueAsInteger()
        : OGlobalConfiguration.INDEX_MANUAL_LAZY_UPDATES.getValueAsInteger();
  }
}
