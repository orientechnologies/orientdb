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

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

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
import com.orientechnologies.orient.core.index.*;
import com.orientechnologies.orient.core.index.mvrbtree.OMVRBTree;
import com.orientechnologies.orient.core.index.mvrbtree.OMVRBTreeEntry;
import com.orientechnologies.orient.core.iterator.OEmptyIterator;
import com.orientechnologies.orient.core.memory.OMemoryWatchDog;
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
        public void lowMemory(final long iFreeMemory, final long iFreeMemoryPercentage) {
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
            keySerializer = OBinarySerializerFactory.getInstance().getObjectSerializer(indexDefinition.getTypes()[0]);
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

  @Override
  public void deleteWithoutLoad(String indexName) {
    throw new UnsupportedOperationException("deleteWithoutLoad");
  }

  public void load(ORID indexRid, String indexName, OIndexDefinition indexDefinition, OStreamSerializer valueSerializer,
      boolean isAutomatic) {
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
  public OIndexCursor iterateEntriesBetween(Object rangeFrom, boolean fromInclusive, Object rangeTo, boolean toInclusive,
      boolean ascSortOrder, ValuesTransformer<V> transformer) {
    acquireExclusiveLock();
    try {

      if (fromInclusive)
        rangeFrom = map.enhanceCompositeKey(rangeFrom, OMVRBTree.PartialSearchMode.LOWEST_BOUNDARY);
      else
        rangeFrom = map.enhanceCompositeKey(rangeFrom, OMVRBTree.PartialSearchMode.HIGHEST_BOUNDARY);

      if (toInclusive)
        rangeTo = map.enhanceCompositeKey(rangeTo, OMVRBTree.PartialSearchMode.HIGHEST_BOUNDARY);
      else
        rangeTo = map.enhanceCompositeKey(rangeTo, OMVRBTree.PartialSearchMode.LOWEST_BOUNDARY);

      if (ascSortOrder) {
        return new OMBRBTreeIndexCursor(map.subMap(rangeFrom, fromInclusive, rangeTo, toInclusive).entrySet().iterator(),
            transformer);
      }

      return new OMBRBTreeIndexCursor(map.subMap(rangeFrom, fromInclusive, rangeTo, toInclusive).descendingMap().entrySet()
          .iterator(), transformer);
    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  public OIndexCursor iterateEntriesMajor(Object fromKey, boolean isInclusive, boolean ascSortOrder,
      ValuesTransformer<V> transformer) {
    acquireExclusiveLock();
    try {
      if (isInclusive)
        fromKey = map.enhanceCompositeKey(fromKey, OMVRBTree.PartialSearchMode.LOWEST_BOUNDARY);
      else
        fromKey = map.enhanceCompositeKey(fromKey, OMVRBTree.PartialSearchMode.HIGHEST_BOUNDARY);

      if (ascSortOrder)
        return new OMBRBTreeIndexCursor(map.tailMap(fromKey, isInclusive).entrySet().iterator(), transformer);

      return new OMBRBTreeIndexCursor(map.tailMap(fromKey, isInclusive).descendingMap().entrySet().iterator(), transformer);
    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  public OIndexCursor iterateEntriesMinor(Object toKey, boolean isInclusive, boolean ascSortOrder, ValuesTransformer<V> transformer) {
    acquireExclusiveLock();
    try {
      if (isInclusive)
        toKey = map.enhanceCompositeKey(toKey, OMVRBTree.PartialSearchMode.HIGHEST_BOUNDARY);
      else
        toKey = map.enhanceCompositeKey(toKey, OMVRBTree.PartialSearchMode.LOWEST_BOUNDARY);

      if (ascSortOrder)
        return new OMBRBTreeIndexCursor(map.headMap(toKey, isInclusive).entrySet().iterator(), transformer);

      return new OMBRBTreeIndexCursor(map.headMap(toKey, isInclusive).descendingMap().entrySet().iterator(), transformer);
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
  public Object getFirstKey() {
    acquireExclusiveLock();
    try {
      if (map.getFirstEntry() == null)
        return null;

      return map.getFirstEntry().getFirstKey();
    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  public Object getLastKey() {
    acquireExclusiveLock();
    try {
      return map.getLastEntry().getLastKey();
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
  public OIndexCursor cursor(ValuesTransformer<V> valuesTransformer) {
    acquireExclusiveLock();
    try {
      return new OMBRBTreeIndexCursor(map.entrySet().iterator(), valuesTransformer);
    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  public OIndexCursor descCursor(ValuesTransformer<V> valuesTransformer) {
    throw new UnsupportedOperationException();
  }

  @Override
  public OIndexKeyCursor keyCursor() {
    acquireExclusiveLock();
    try {
      return new OIndexKeyCursor() {
        private final Iterator<Object> keysIterator = map.keySet().iterator();

        @Override
        public Object next(int prefetchSize) {
          if (keysIterator.hasNext())
            return keysIterator.next();

          return null;
        }
      };
    } finally {
      releaseExclusiveLock();
    }

  }

  @Override
  public boolean hasRangeQuerySupport() {
    return true;
  }

  private ODatabaseRecord getDatabase() {
    return ODatabaseRecordThreadLocal.INSTANCE.get();
  }

  private int lazyUpdates(boolean isAutomatic) {
    return isAutomatic ? OGlobalConfiguration.INDEX_AUTO_LAZY_UPDATES.getValueAsInteger()
        : OGlobalConfiguration.INDEX_MANUAL_LAZY_UPDATES.getValueAsInteger();
  }

  private final class OMBRBTreeIndexCursor extends OIndexAbstractCursor {
    private final Iterator<Map.Entry<Object, V>> treeIterator;
    private final ValuesTransformer<V>           valuesTransformer;

    private Iterator<OIdentifiable>              currentIterator = OEmptyIterator.IDENTIFIABLE_INSTANCE;
    private Object                               currentKey      = null;

    private OMBRBTreeIndexCursor(Iterator<Map.Entry<Object, V>> treeIterator, ValuesTransformer<V> valuesTransformer) {
      this.treeIterator = treeIterator;
      this.valuesTransformer = valuesTransformer;
    }

    @Override
    public Map.Entry<Object, OIdentifiable> nextEntry() {
      if (currentIterator.hasNext())
        return nextCollectionEntry();

      if (!treeIterator.hasNext())
        return null;

      if (valuesTransformer == null) {
        final Map.Entry<Object, V> entry = treeIterator.next();
        currentKey = entry.getKey();
        currentIterator = Collections.singletonList((OIdentifiable) entry.getValue()).iterator();
      } else {
        while (!currentIterator.hasNext() && treeIterator.hasNext()) {
          final Map.Entry<Object, V> entry = treeIterator.next();
          currentKey = entry.getKey();
          currentIterator = valuesTransformer.transformFromValue(entry.getValue()).iterator();
        }
      }

      if (!currentIterator.hasNext())
        return null;

      return nextCollectionEntry();

    }

    private Map.Entry<Object, OIdentifiable> nextCollectionEntry() {
      final OIdentifiable value = currentIterator.next();

      return new Map.Entry<Object, OIdentifiable>() {
        @Override
        public Object getKey() {
          return currentKey;
        }

        @Override
        public OIdentifiable getValue() {
          return value;
        }

        @Override
        public OIdentifiable setValue(OIdentifiable value) {
          throw new UnsupportedOperationException("setValue");
        }
      };
    }

  }
}
