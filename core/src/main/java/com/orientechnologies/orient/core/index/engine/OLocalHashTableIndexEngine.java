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

import java.util.*;

import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.*;
import com.orientechnologies.orient.core.index.hashindex.local.OHashIndexBucket;
import com.orientechnologies.orient.core.index.hashindex.local.OLocalHashTable;
import com.orientechnologies.orient.core.index.hashindex.local.OMurmurHash3HashFunction;
import com.orientechnologies.orient.core.iterator.OEmptyIterator;
import com.orientechnologies.orient.core.record.impl.ORecordBytes;
import com.orientechnologies.orient.core.serialization.serializer.binary.OBinarySerializerFactory;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.index.OCompositeKeySerializer;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.index.OSimpleKeySerializer;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializer;
import com.orientechnologies.orient.core.storage.impl.local.OStorageLocalAbstract;

/**
 * @author Andrey Lomakin
 * @since 15.07.13
 */
public final class OLocalHashTableIndexEngine<V> implements OIndexEngine<V> {
  public static final String                     METADATA_FILE_EXTENSION    = ".him";
  public static final String                     TREE_FILE_EXTENSION        = ".hit";
  public static final String                     BUCKET_FILE_EXTENSION      = ".hib";
  public static final String                     NULL_BUCKET_FILE_EXTENSION = ".hnb";

  private final OLocalHashTable<Object, V>       hashTable;
  private final OMurmurHash3HashFunction<Object> hashFunction;

  private volatile ORID                          identity;

  public OLocalHashTableIndexEngine() {
    hashFunction = new OMurmurHash3HashFunction<Object>();
    hashTable = new OLocalHashTable<Object, V>(METADATA_FILE_EXTENSION, TREE_FILE_EXTENSION, BUCKET_FILE_EXTENSION,
        NULL_BUCKET_FILE_EXTENSION, hashFunction);
  }

  @Override
  public void init() {
  }

  @Override
  public void create(String indexName, OIndexDefinition indexDefinition, String clusterIndexName,
      OStreamSerializer valueSerializer, boolean isAutomatic) {
    OBinarySerializer keySerializer;

    if (indexDefinition != null) {
      if (indexDefinition instanceof ORuntimeKeyIndexDefinition) {
        keySerializer = ((ORuntimeKeyIndexDefinition) indexDefinition).getSerializer();
      } else {
        if (indexDefinition.getTypes().length > 1) {
          keySerializer = OCompositeKeySerializer.INSTANCE;
        } else {
          keySerializer = OBinarySerializerFactory.getInstance().getObjectSerializer(indexDefinition.getTypes()[0]);
        }
      }
    } else
      keySerializer = new OSimpleKeySerializer();

    final ODatabaseRecord database = getDatabase();
    final ORecordBytes identityRecord = new ORecordBytes();
    final OStorageLocalAbstract storageLocalAbstract = (OStorageLocalAbstract) database.getStorage();

    database.save(identityRecord, clusterIndexName);
    identity = identityRecord.getIdentity();

    hashFunction.setValueSerializer(keySerializer);
    hashTable.create(indexName, keySerializer, (OBinarySerializer<V>) valueSerializer,
        indexDefinition != null ? indexDefinition.getTypes() : null, storageLocalAbstract, indexDefinition != null
            && !indexDefinition.isNullValuesIgnored());
  }

  @Override
  public void flush() {
    hashTable.flush();
  }

  @Override
  public void deleteWithoutLoad(String indexName) {
    hashTable.deleteWithoutLoad(indexName, (OStorageLocalAbstract) getDatabase().getStorage().getUnderlying());
  }

  @Override
  public void delete() {
    hashTable.delete();
  }

  @Override
  public void load(ORID indexRid, String indexName, OIndexDefinition indexDefinition, OStreamSerializer valueSerializer,
      boolean isAutomatic) {
    identity = indexRid;
    hashTable.load(indexName, indexDefinition != null ? indexDefinition.getTypes() : null, (OStorageLocalAbstract) getDatabase()
        .getStorage().getUnderlying(), indexDefinition != null && !indexDefinition.isNullValuesIgnored());
    hashFunction.setValueSerializer(hashTable.getKeySerializer());
  }

  @Override
  public boolean contains(Object key) {
    return hashTable.get(key) != null;
  }

  @Override
  public boolean remove(Object key) {
    return hashTable.remove(key) != null;
  }

  @Override
  public void clear() {
    hashTable.clear();
  }

  @Override
  public void unload() {
  }

  @Override
  public void closeDb() {
  }

  @Override
  public void close() {
    hashTable.close();
  }

  @Override
  public V get(Object key) {
    return hashTable.get(key);
  }

  @Override
  public void put(Object key, V value) {
    hashTable.put(key, value);
  }

  @Override
  public long size(ValuesTransformer<V> transformer) {
    if (transformer == null)
      return hashTable.size();
    else {
      OHashIndexBucket.Entry<Object, V> firstEntry = hashTable.firstEntry();
      if (firstEntry == null)
        return 0;

      OHashIndexBucket.Entry<Object, V>[] entries = hashTable.ceilingEntries(firstEntry.key);
      long counter = 0;

      while (entries.length > 0) {
        for (OHashIndexBucket.Entry<Object, V> entry : entries)
          counter += transformer.transformFromValue(entry.value).size();

        entries = hashTable.higherEntries(entries[entries.length - 1].key);
      }

      return counter;
    }
  }

  @Override
  public ORID getIdentity() {
    return identity;
  }

  @Override
  public boolean hasRangeQuerySupport() {
    return false;
  }

  @Override
  public OIndexCursor iterateEntriesBetween(Object rangeFrom, boolean fromInclusive, Object rangeTo, boolean toInclusive,
      boolean ascSortOrder, ValuesTransformer<V> transformer) {
    throw new UnsupportedOperationException("iterateEntriesBetween");
  }

  @Override
  public OIndexCursor iterateEntriesMajor(Object fromKey, boolean isInclusive, boolean ascSortOrder,
      ValuesTransformer<V> transformer) {
    throw new UnsupportedOperationException("iterateEntriesMajor");
  }

  @Override
  public OIndexCursor iterateEntriesMinor(Object toKey, boolean isInclusive, boolean ascSortOrder, ValuesTransformer<V> transformer) {
    throw new UnsupportedOperationException("iterateEntriesMinor");
  }

  @Override
  public Object getFirstKey() {
    throw new UnsupportedOperationException("firstKey");
  }

  @Override
  public Object getLastKey() {
    throw new UnsupportedOperationException("lastKey");
  }

  @Override
  public OIndexCursor cursor(final ValuesTransformer<V> valuesTransformer) {
    return new OIndexAbstractCursor() {
      private int                                 nextEntriesIndex;
      private OHashIndexBucket.Entry<Object, V>[] entries;

      private Iterator<OIdentifiable>             currentIterator = new OEmptyIterator<OIdentifiable>();
      private Object                              currentKey;

      {
        OHashIndexBucket.Entry<Object, V> firstEntry = hashTable.firstEntry();
        if (firstEntry == null)
          entries = new OHashIndexBucket.Entry[0];
        else
          entries = hashTable.ceilingEntries(firstEntry.key);

        if (entries.length == 0)
          currentIterator = null;
      }

      @Override
      public Map.Entry<Object, OIdentifiable> nextEntry() {
        if (currentIterator == null)
          return null;

        if (currentIterator.hasNext())
          return nextCursorValue();

        while (currentIterator != null && !currentIterator.hasNext()) {
          if (entries.length == 0) {
            currentIterator = null;
            return null;
          }

          final OHashIndexBucket.Entry<Object, V> bucketEntry = entries[nextEntriesIndex];

          currentKey = bucketEntry.key;

          V value = bucketEntry.value;
          if (valuesTransformer != null)
            currentIterator = valuesTransformer.transformFromValue(value).iterator();
          else
            currentIterator = Collections.singletonList((OIdentifiable) value).iterator();

          nextEntriesIndex++;

          if (nextEntriesIndex >= entries.length) {
            entries = hashTable.higherEntries(entries[entries.length - 1].key);

            nextEntriesIndex = 0;
          }
        }

        if (currentIterator != null && !currentIterator.hasNext())
          return nextCursorValue();

        currentIterator = null;
        return null;
      }

      private Map.Entry<Object, OIdentifiable> nextCursorValue() {
        final OIdentifiable identifiable = currentIterator.next();

        return new Map.Entry<Object, OIdentifiable>() {
          @Override
          public Object getKey() {
            return currentKey;
          }

          @Override
          public OIdentifiable getValue() {
            return identifiable;
          }

          @Override
          public OIdentifiable setValue(OIdentifiable value) {
            throw new UnsupportedOperationException();
          }
        };
      }
    };
  }

  @Override
  public OIndexKeyCursor keyCursor() {
    return new OIndexKeyCursor() {
      private int                                 nextEntriesIndex;
      private OHashIndexBucket.Entry<Object, V>[] entries;

      {
        OHashIndexBucket.Entry<Object, V> firstEntry = hashTable.firstEntry();
        if (firstEntry == null)
          entries = new OHashIndexBucket.Entry[0];
        else
          entries = hashTable.ceilingEntries(firstEntry.key);
      }

      @Override
      public Object next(int prefetchSize) {
        if (entries.length == 0) {
          return null;
        }

        final OHashIndexBucket.Entry<Object, V> bucketEntry = entries[nextEntriesIndex];
        nextEntriesIndex++;
        if (nextEntriesIndex >= entries.length) {
          entries = hashTable.higherEntries(entries[entries.length - 1].key);

          nextEntriesIndex = 0;
        }

        return bucketEntry.key;
      }
    };
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
  public void beforeTxBegin() {
  }

  private ODatabaseRecord getDatabase() {
    return ODatabaseRecordThreadLocal.INSTANCE.get();
  }
}
