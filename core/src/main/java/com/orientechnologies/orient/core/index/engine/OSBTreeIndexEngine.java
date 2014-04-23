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

import com.orientechnologies.common.concur.resource.OSharedResourceAdaptiveExternal;
import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.*;
import com.orientechnologies.orient.core.index.sbtree.local.OSBTree;
import com.orientechnologies.orient.core.iterator.OEmptyIterator;
import com.orientechnologies.orient.core.record.impl.ORecordBytes;
import com.orientechnologies.orient.core.serialization.serializer.binary.OBinarySerializerFactory;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.index.OCompositeKeySerializer;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.index.OSimpleKeySerializer;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializer;
import com.orientechnologies.orient.core.storage.impl.local.OStorageLocalAbstract;

import java.util.Iterator;
import java.util.Map;

/**
 * @author Andrey Lomakin
 * @since 8/30/13
 */
public class OSBTreeIndexEngine<V> extends OSharedResourceAdaptiveExternal implements OIndexEngine<V> {
  public static final String DATA_FILE_EXTENSION        = ".sbt";
  public static final String NULL_BUCKET_FILE_EXTENSION = ".nbt";

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
              OGlobalConfiguration.INDEX_DURABLE_IN_NON_TX_MODE.getValueAsBoolean(), NULL_BUCKET_FILE_EXTENSION);
          keySerializer = ((ORuntimeKeyIndexDefinition) indexDefinition).getSerializer();
        } else {
          if (indexDefinition.getTypes().length > 1) {
            keySerializer = OCompositeKeySerializer.INSTANCE;
          } else {
            keySerializer = OBinarySerializerFactory.getInstance().getObjectSerializer(indexDefinition.getTypes()[0]);
          }
          sbTree = new OSBTree<Object, V>(DATA_FILE_EXTENSION, indexDefinition.getTypes().length,
              OGlobalConfiguration.INDEX_DURABLE_IN_NON_TX_MODE.getValueAsBoolean(), NULL_BUCKET_FILE_EXTENSION);
        }
      } else {
        sbTree = new OSBTree<Object, V>(DATA_FILE_EXTENSION, 1,
            OGlobalConfiguration.INDEX_DURABLE_IN_NON_TX_MODE.getValueAsBoolean(), NULL_BUCKET_FILE_EXTENSION);
        keySerializer = new OSimpleKeySerializer();
      }

      final ORecordBytes identityRecord = new ORecordBytes();
      ODatabaseRecord database = getDatabase();
      final OStorageLocalAbstract storageLocalAbstract = (OStorageLocalAbstract) database.getStorage().getUnderlying();

      database.save(identityRecord, clusterIndexName);
      identity = identityRecord.getIdentity();

      sbTree.create(indexName, keySerializer, (OBinarySerializer<V>) valueSerializer,
          indexDefinition != null ? indexDefinition.getTypes() : null, storageLocalAbstract, indexDefinition != null
              && !indexDefinition.isNullValuesIgnored());
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
  public void deleteWithoutLoad(String indexName) {
    acquireExclusiveLock();
    try {
      final ODatabaseRecord database = getDatabase();
      final OStorageLocalAbstract storageLocalAbstract = (OStorageLocalAbstract) database.getStorage().getUnderlying();

      sbTree = new OSBTree<Object, V>(DATA_FILE_EXTENSION, 1,
          OGlobalConfiguration.INDEX_DURABLE_IN_NON_TX_MODE.getValueAsBoolean(), NULL_BUCKET_FILE_EXTENSION);
      sbTree.deleteWithoutLoad(indexName, storageLocalAbstract);
    } finally {
      releaseExclusiveLock();
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
          OGlobalConfiguration.INDEX_DURABLE_IN_NON_TX_MODE.getValueAsBoolean(), NULL_BUCKET_FILE_EXTENSION);

      ODatabaseRecord database = getDatabase();
      final OStorageLocalAbstract storageLocalAbstract = (OStorageLocalAbstract) database.getStorage().getUnderlying();

      sbTree.load(indexName, indexDefinition != null ? indexDefinition.getTypes() : null, storageLocalAbstract,
          indexDefinition != null && indexDefinition.isNullValuesIgnored());
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
  public OIndexCursor cursor(ValuesTransformer<V> valuesTransformer) {
    acquireSharedLock();
    try {
      final Object firstKey = sbTree.firstKey();
      if (firstKey == null)
        return new OIndexCursor() {
          @Override
          public Map.Entry<Object, OIdentifiable> next(int prefetchSize) {
            return null;
          }
        };

      return new OSBTreeIndexCursor<V>(sbTree.iterateEntriesMajor(firstKey, true, true), valuesTransformer);
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public OIndexKeyCursor keyCursor() {
    acquireSharedLock();
    try {
      return new OIndexKeyCursor() {
        private final OSBTree.OSBTreeKeyCursor<Object> sbTreeKeyCursor = sbTree.keyCursor();

        @Override
        public Object next(int prefetchSize) {
          return sbTreeKeyCursor.next(prefetchSize);
        }
      };
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
  public Object getFirstKey() {
    acquireSharedLock();
    try {
      return sbTree.firstKey();
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public Object getLastKey() {
    acquireSharedLock();
    try {
      return sbTree.lastKey();
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public OIndexCursor iterateEntriesBetween(Object rangeFrom, boolean fromInclusive, Object rangeTo, boolean toInclusive,
      boolean ascSortOrder, ValuesTransformer<V> transformer) {
    acquireSharedLock();
    try {
      return new OSBTreeIndexCursor<V>(sbTree.iterateEntriesBetween(rangeFrom, fromInclusive, rangeTo, toInclusive, ascSortOrder),
          transformer);
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public OIndexCursor iterateEntriesMajor(Object fromKey, boolean isInclusive, boolean ascSortOrder,
      ValuesTransformer<V> transformer) {
    acquireSharedLock();
    try {
      return new OSBTreeIndexCursor<V>(sbTree.iterateEntriesMajor(fromKey, isInclusive, ascSortOrder), transformer);
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public OIndexCursor iterateEntriesMinor(Object toKey, boolean isInclusive, boolean ascSortOrder, ValuesTransformer<V> transformer) {
    acquireSharedLock();
    try {
      return new OSBTreeIndexCursor<V>(sbTree.iterateEntriesMinor(toKey, isInclusive, ascSortOrder), transformer);

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
        final Object firstKey = sbTree.firstKey();
        final Object lastKey = sbTree.lastKey();

        if (firstKey != null && lastKey != null) {
          int counter = 0;

          final OSBTree.OSBTreeCursor<Object, V> cursor = sbTree.iterateEntriesBetween(firstKey, true, lastKey, true, true);
          Map.Entry<Object, V> entry = cursor.next(-1);
          while (entry != null) {
            counter += transformer.transformFromValue(entry.getValue()).size();
            entry = cursor.next(-1);
          }

          return counter;
        }

        return 0;
      }
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

  private static final class OSBTreeIndexCursor<V> implements OIndexCursor {
    private final OSBTree.OSBTreeCursor<Object, V> treeCursor;
    private final ValuesTransformer<V>             valuesTransformer;

    private Iterator<OIdentifiable>                currentIterator = OEmptyIterator.IDENTIFIABLE_INSTANCE;
    private Object                                 currentKey      = null;

    private OSBTreeIndexCursor(OSBTree.OSBTreeCursor<Object, V> treeCursor, ValuesTransformer<V> valuesTransformer) {
      this.treeCursor = treeCursor;
      this.valuesTransformer = valuesTransformer;
    }

    @Override
    public Map.Entry<Object, OIdentifiable> next(int prefetchSize) {
      if (valuesTransformer == null)
        return (Map.Entry<Object, OIdentifiable>) treeCursor.next(prefetchSize);

      if (currentIterator == null)
        return null;

      while (!currentIterator.hasNext()) {
        Map.Entry<Object, V> entry = treeCursor.next(prefetchSize);
        if (entry == null) {
          currentIterator = null;
          return null;
        }

        currentKey = entry.getKey();
        currentIterator = valuesTransformer.transformFromValue(entry.getValue()).iterator();
      }

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
