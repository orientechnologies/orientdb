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

package com.orientechnologies.orient.core.index.engine;

import java.util.*;

import com.orientechnologies.common.concur.resource.OSharedResourceAdaptiveExternal;
import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.ODocumentFieldsHashSet;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.index.OIndexEngine;
import com.orientechnologies.orient.core.index.ORuntimeKeyIndexDefinition;
import com.orientechnologies.orient.core.index.sbtree.OSBTreeInverseMapEntryIterator;
import com.orientechnologies.orient.core.index.sbtree.OSBTreeMapEntryIterator;
import com.orientechnologies.orient.core.index.sbtree.OTreeInternal;
import com.orientechnologies.orient.core.index.sbtree.local.OSBTree;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ORecordBytes;
import com.orientechnologies.orient.core.serialization.serializer.binary.OBinarySerializerFactory;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.index.OCompositeKeySerializer;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.index.OSimpleKeySerializer;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializer;
import com.orientechnologies.orient.core.storage.impl.local.OStorageLocalAbstract;

/**
 * @author Andrey Lomakin
 * @since 8/30/13
 */
public class OSBTreeIndexEngine<V> extends OSharedResourceAdaptiveExternal implements OIndexEngine<V> {
  public static final String DATA_FILE_EXTENSION = ".sbt";

  private ORID               identity;
  private OSBTree<Object, V> sbTree;

  public OSBTreeIndexEngine() {
    super(OGlobalConfiguration.ENVIRONMENT_CONCURRENT.getValueAsBoolean(), OGlobalConfiguration.MVRBTREE_TIMEOUT
        .getValueAsInteger(), true);
  }

  @Override
  public void init() {
  }

  @Override
  public void flush() {
    acquireSharedLock();
    try {
      sbTree.flush();
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public void create(String indexName, OIndexDefinition indexDefinition, String clusterIndexName,
      OStreamSerializer valueSerializer, boolean isAutomatic) {
    acquireExclusiveLock();
    try {
      final OBinarySerializer keySerializer;
      if (indexDefinition != null) {
        if (indexDefinition instanceof ORuntimeKeyIndexDefinition) {
          sbTree = new OSBTree<Object, V>(DATA_FILE_EXTENSION, 1,
              OGlobalConfiguration.INDEX_DURABLE_IN_NON_TX_MODE.getValueAsBoolean());
          keySerializer = ((ORuntimeKeyIndexDefinition) indexDefinition).getSerializer();
        } else {
          if (indexDefinition.getTypes().length > 1) {
            keySerializer = OCompositeKeySerializer.INSTANCE;
          } else {
            keySerializer = OBinarySerializerFactory.INSTANCE.getObjectSerializer(indexDefinition.getTypes()[0]);
          }
          sbTree = new OSBTree<Object, V>(DATA_FILE_EXTENSION, indexDefinition.getTypes().length,
              OGlobalConfiguration.INDEX_DURABLE_IN_NON_TX_MODE.getValueAsBoolean());
        }
      } else {
        sbTree = new OSBTree<Object, V>(DATA_FILE_EXTENSION, 1,
            OGlobalConfiguration.INDEX_DURABLE_IN_NON_TX_MODE.getValueAsBoolean());
        keySerializer = new OSimpleKeySerializer();
      }

      final ORecordBytes identityRecord = new ORecordBytes();
      ODatabaseRecord database = getDatabase();
      final OStorageLocalAbstract storageLocalAbstract = (OStorageLocalAbstract) database.getStorage().getUnderlying();

      database.save(identityRecord, clusterIndexName);
      identity = identityRecord.getIdentity();

      sbTree.create(indexName, keySerializer, (OBinarySerializer<V>) valueSerializer,
          indexDefinition != null ? indexDefinition.getTypes() : null, storageLocalAbstract);
    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  public void delete() {
    acquireSharedLock();
    try {
      sbTree.delete();
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public void load(ORID indexRid, String indexName, OIndexDefinition indexDefinition, boolean isAutomatic) {
    acquireExclusiveLock();
    try {
      final int keySize;
      if (indexDefinition == null || indexDefinition instanceof ORuntimeKeyIndexDefinition)
        keySize = 1;
      else
        keySize = indexDefinition.getTypes().length;

      sbTree = new OSBTree<Object, V>(DATA_FILE_EXTENSION, keySize,
          OGlobalConfiguration.INDEX_DURABLE_IN_NON_TX_MODE.getValueAsBoolean());

      ODatabaseRecord database = getDatabase();
      final OStorageLocalAbstract storageLocalAbstract = (OStorageLocalAbstract) database.getStorage().getUnderlying();

      sbTree.load(indexName, indexDefinition != null ? indexDefinition.getTypes() : null, storageLocalAbstract);
    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  public boolean contains(Object key) {
    acquireSharedLock();
    try {
      return sbTree.get(key) != null;
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public boolean remove(Object key) {
    acquireSharedLock();
    try {
      return sbTree.remove(key) != null;
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public ORID getIdentity() {
    acquireSharedLock();
    try {
      return identity;
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public void clear() {
    acquireSharedLock();
    try {
      sbTree.clear();
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public Iterator<Map.Entry<Object, V>> iterator() {
    acquireSharedLock();
    try {
      return new OSBTreeMapEntryIterator<Object, V>(sbTree);
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public Iterator<Map.Entry<Object, V>> inverseIterator() {
    acquireSharedLock();
    try {
      return new OSBTreeInverseMapEntryIterator<Object, V>(sbTree);
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public Iterator<V> valuesIterator() {
    acquireSharedLock();
    try {
      return new Iterator<V>() {
        private final OSBTreeMapEntryIterator<Object, V> entryIterator = new OSBTreeMapEntryIterator<Object, V>(sbTree);

        @Override
        public boolean hasNext() {
          return entryIterator.hasNext();
        }

        @Override
        public V next() {
          return entryIterator.next().getValue();
        }

        @Override
        public void remove() {
          entryIterator.remove();
        }
      };
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public Iterator<V> inverseValuesIterator() {
    acquireSharedLock();
    try {
      return new Iterator<V>() {
        private final OSBTreeInverseMapEntryIterator<Object, V> entryIterator = new OSBTreeInverseMapEntryIterator<Object, V>(
                                                                                  sbTree);

        @Override
        public boolean hasNext() {
          return entryIterator.hasNext();
        }

        @Override
        public V next() {
          return entryIterator.next().getValue();
        }

        @Override
        public void remove() {
          entryIterator.remove();
        }
      };
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public Iterable<Object> keys() {
    acquireSharedLock();
    try {
      return new Iterable<Object>() {
        @Override
        public Iterator<Object> iterator() {
          return new Iterator<Object>() {

            final OSBTreeMapEntryIterator<Object, V> entryIterator = new OSBTreeMapEntryIterator<Object, V>(sbTree);

            @Override
            public boolean hasNext() {
              return entryIterator.hasNext();
            }

            @Override
            public Object next() {
              return entryIterator.next().getKey();
            }

            @Override
            public void remove() {
              entryIterator.remove();
            }
          };
        }
      };
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public void unload() {
  }

  @Override
  public void startTransaction() {
  }

  @Override
  public void stopTransaction() {
  }

  @Override
  public void afterTxRollback() {
  }

  @Override
  public void afterTxCommit() {
  }

  @Override
  public void closeDb() {
  }

  @Override
  public void close() {
    acquireSharedLock();
    try {
      sbTree.close();
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public void beforeTxBegin() {
  }

  @Override
  public V get(Object key) {
    acquireSharedLock();
    try {
      return sbTree.get(key);
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public void put(Object key, V value) {
    acquireSharedLock();
    try {
      sbTree.put(key, value);
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public int removeValue(final OIdentifiable value, final ValuesTransformer<V> transformer) {
    acquireExclusiveLock();
    try {
      final Set<Object> keySetToRemove = new HashSet<Object>();

      if (sbTree.size() == 0)
        return 0;

      final Object firstKey = sbTree.firstKey();
      final Object lastKey = sbTree.lastKey();
      sbTree.loadEntriesBetween(firstKey, true, lastKey, true, new OSBTree.RangeResultListener<Object, V>() {
        @Override
        public boolean addResult(Map.Entry<Object, V> entry) {
          if (transformer == null) {
            if (entry.getValue().equals(value))
              keySetToRemove.add(entry.getKey());
          } else {
            Collection<OIdentifiable> identifiables = transformer.transformFromValue(entry.getValue());
            for (OIdentifiable identifiable : identifiables) {
              if (identifiable.equals(value))
                keySetToRemove.add(entry.getKey());
            }
          }
          return true;
        }
      });

      for (Object keyToRemove : keySetToRemove)
        sbTree.remove(keyToRemove);

      return keySetToRemove.size();
    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  public Collection<OIdentifiable> getValuesBetween(Object rangeFrom, boolean fromInclusive, Object rangeTo, boolean toInclusive,
      final int maxValuesToFetch, final ValuesTransformer<V> transformer) {
    acquireSharedLock();
    try {
      final Set<OIdentifiable> result = new HashSet<OIdentifiable>();

      sbTree.loadEntriesBetween(rangeFrom, fromInclusive, rangeTo, toInclusive, new OSBTree.RangeResultListener<Object, V>() {
        @Override
        public boolean addResult(Map.Entry<Object, V> entry) {
          addToResult(transformer, result, entry.getValue(), maxValuesToFetch);

          if (maxValuesToFetch > -1 && result.size() == maxValuesToFetch)
            return false;

          return true;
        }
      });

      return result;
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public Collection<OIdentifiable> getValuesMajor(Object fromKey, boolean isInclusive, final int maxValuesToFetch,
      final ValuesTransformer<V> transformer) {
    acquireSharedLock();
    try {
      final Set<OIdentifiable> result = new HashSet<OIdentifiable>();

      sbTree.loadEntriesMajor(fromKey, isInclusive, new OSBTree.RangeResultListener<Object, V>() {
        @Override
        public boolean addResult(Map.Entry<Object, V> entry) {
          addToResult(transformer, result, entry.getValue(), maxValuesToFetch);

          if (maxValuesToFetch > -1 && result.size() == maxValuesToFetch)
            return false;

          return true;
        }
      });

      return result;
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public Collection<OIdentifiable> getValuesMinor(Object toKey, boolean isInclusive, final int maxValuesToFetch,
      final ValuesTransformer<V> transformer) {
    acquireSharedLock();
    try {
      final Set<OIdentifiable> result = new HashSet<OIdentifiable>();

      sbTree.loadEntriesMinor(toKey, isInclusive, new OSBTree.RangeResultListener<Object, V>() {
        @Override
        public boolean addResult(Map.Entry<Object, V> entry) {
          addToResult(transformer, result, entry.getValue(), maxValuesToFetch);

          if (maxValuesToFetch > -1 && result.size() == maxValuesToFetch)
            return false;

          return true;
        }
      });

      return result;
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public Collection<ODocument> getEntriesMajor(Object fromKey, boolean isInclusive, final int maxEntriesToFetch,
      final ValuesTransformer<V> transformer) {
    acquireSharedLock();
    try {
      final Set<ODocument> result = new ODocumentFieldsHashSet();

      sbTree.loadEntriesMajor(fromKey, isInclusive, new OTreeInternal.RangeResultListener<Object, V>() {
        @Override
        public boolean addResult(Map.Entry<Object, V> entry) {
          final Object key = entry.getKey();
          final V value = entry.getValue();

          addToEntriesResult(transformer, result, key, value, maxEntriesToFetch);

          if (maxEntriesToFetch > -1 && result.size() == maxEntriesToFetch)
            return false;

          return true;
        }
      });

      return result;
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public Collection<ODocument> getEntriesMinor(Object toKey, boolean isInclusive, final int maxEntriesToFetch,
      final ValuesTransformer<V> transformer) {
    acquireSharedLock();
    try {
      final Set<ODocument> result = new ODocumentFieldsHashSet();

      sbTree.loadEntriesMinor(toKey, isInclusive, new OTreeInternal.RangeResultListener<Object, V>() {
        @Override
        public boolean addResult(Map.Entry<Object, V> entry) {
          final Object key = entry.getKey();
          final V value = entry.getValue();

          addToEntriesResult(transformer, result, key, value, maxEntriesToFetch);

          if (maxEntriesToFetch > -1 && result.size() == maxEntriesToFetch)
            return false;

          return true;
        }
      });

      return result;
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public Collection<ODocument> getEntriesBetween(Object rangeFrom, Object rangeTo, boolean inclusive, final int maxEntriesToFetch,
      final ValuesTransformer<V> transformer) {
    acquireSharedLock();
    try {
      final Set<ODocument> result = new ODocumentFieldsHashSet();

      sbTree.loadEntriesBetween(rangeFrom, inclusive, rangeTo, inclusive, new OTreeInternal.RangeResultListener<Object, V>() {
        @Override
        public boolean addResult(Map.Entry<Object, V> entry) {
          final Object key = entry.getKey();
          final V value = entry.getValue();

          addToEntriesResult(transformer, result, key, value, maxEntriesToFetch);

          if (maxEntriesToFetch > -1 && result.size() == maxEntriesToFetch)
            return false;

          return true;
        }
      });

      return result;
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public long size(final ValuesTransformer<V> transformer) {
    acquireSharedLock();
    try {
      if (transformer == null)
        return sbTree.size();
      else {

        final ItemsCounter<V> counter = new ItemsCounter<V>(transformer, -1);

        final Object firstKey = sbTree.firstKey();
        final Object lastKey = sbTree.lastKey();

        if (firstKey != null && lastKey != null) {
          sbTree.loadEntriesBetween(firstKey, true, lastKey, true, counter);
          return counter.count;
        }

        return 0;
      }
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public long count(Object rangeFrom, boolean fromInclusive, Object rangeTo, boolean toInclusive, int maxValuesToFetch,
      ValuesTransformer<V> transformer) {
    acquireSharedLock();
    try {
      final ItemsCounter<V> itemsCounter = new ItemsCounter<V>(transformer, maxValuesToFetch);

      if (rangeTo != null)
        sbTree.loadEntriesBetween(rangeFrom, fromInclusive, rangeTo, toInclusive, itemsCounter);
      else
        sbTree.loadEntriesMajor(rangeFrom, fromInclusive, itemsCounter);

      return itemsCounter.count;
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public boolean hasRangeQuerySupport() {
    return true;
  }

  private ODatabaseRecord getDatabase() {
    return ODatabaseRecordThreadLocal.INSTANCE.get();
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

  private static final class ItemsCounter<V> implements OSBTree.RangeResultListener<Object, V> {
    private final ValuesTransformer<V> valuesTransformer;
    private final int                  maxValuesToFetch;

    private ItemsCounter(ValuesTransformer<V> valuesTransformer, int maxValuesToFetch) {
      this.valuesTransformer = valuesTransformer;
      this.maxValuesToFetch = maxValuesToFetch;
    }

    private int count;

    @Override
    public boolean addResult(Map.Entry<Object, V> entry) {
      if (valuesTransformer != null)
        count += valuesTransformer.transformFromValue(entry.getValue()).size();
      else
        count++;

      if (maxValuesToFetch > 0 && count >= maxValuesToFetch)
        return false;

      return true;
    }
  }
}
