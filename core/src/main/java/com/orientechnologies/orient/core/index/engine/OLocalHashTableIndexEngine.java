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
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.index.OIndexEngine;
import com.orientechnologies.orient.core.index.ORuntimeKeyIndexDefinition;
import com.orientechnologies.orient.core.index.hashindex.local.OHashIndexBucket;
import com.orientechnologies.orient.core.index.hashindex.local.OLocalHashTable;
import com.orientechnologies.orient.core.index.hashindex.local.OMurmurHash3HashFunction;
import com.orientechnologies.orient.core.record.impl.ODocument;
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
  public static final String                     METADATA_FILE_EXTENSION = ".him";
  public static final String                     TREE_FILE_EXTENSION     = ".hit";
  public static final String                     BUCKET_FILE_EXTENSION   = ".hib";

  private final OLocalHashTable<Object, V>       hashTable;
  private final OMurmurHash3HashFunction<Object> hashFunction;

  private volatile ORID                          identity;

  public OLocalHashTableIndexEngine() {
    hashFunction = new OMurmurHash3HashFunction<Object>();
    hashTable = new OLocalHashTable<Object, V>(METADATA_FILE_EXTENSION, TREE_FILE_EXTENSION, BUCKET_FILE_EXTENSION, hashFunction);
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
          keySerializer = OBinarySerializerFactory.INSTANCE.getObjectSerializer(indexDefinition.getTypes()[0]);
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
        indexDefinition != null ? indexDefinition.getTypes() : null, storageLocalAbstract);
  }

  @Override
  public void flush() {
    hashTable.flush();
  }

  @Override
  public void delete() {
    hashTable.delete();
  }

  @Override
  public void load(ORID indexRid, String indexName, OIndexDefinition indexDefinition, boolean isAutomatic) {
    identity = indexRid;
    hashTable.load(indexName, indexDefinition != null ? indexDefinition.getTypes() : null, (OStorageLocalAbstract) getDatabase()
        .getStorage().getUnderlying());
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
  public int removeValue(OIdentifiable valueToRemove, ValuesTransformer<V> transformer) {
    Map<Object, V> entriesToUpdate = new HashMap<Object, V>();
    OHashIndexBucket.Entry<Object, V> firstEntry = hashTable.firstEntry();
    if (firstEntry == null)
      return 0;

    OHashIndexBucket.Entry<Object, V>[] entries = hashTable.ceilingEntries(firstEntry.key);

    while (entries.length > 0) {
      for (OHashIndexBucket.Entry<Object, V> entry : entries)
        if (transformer != null) {
          Collection<OIdentifiable> rids = transformer.transformFromValue(entry.value);
          if (rids.remove(valueToRemove))
            entriesToUpdate.put(entry.key, transformer.transformToValue(rids));
        } else if (entry.value.equals(valueToRemove))
          entriesToUpdate.put(entry.key, entry.value);

      entries = hashTable.higherEntries(entries[entries.length - 1].key);
    }

    for (Map.Entry<Object, V> entry : entriesToUpdate.entrySet()) {
      V value = entry.getValue();
      if (value instanceof Collection) {
        Collection col = (Collection) value;
        if (col.isEmpty())
          hashTable.remove(entry.getKey());
        else
          hashTable.put(entry.getKey(), value);
      } else
        hashTable.remove(entry.getKey());
    }

    return entriesToUpdate.size();
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
  public Iterator<Map.Entry<Object, V>> iterator() {
    return new EntriesIterator();
  }

  @Override
  public Iterable<Object> keys() {
    return new KeysIterable();
  }

  @Override
  public Collection<OIdentifiable> getValuesBetween(Object rangeFrom, boolean fromInclusive, Object rangeTo, boolean toInclusive,
      int maxValuesToFetch, ValuesTransformer<V> transformer) {
    throw new UnsupportedOperationException("getValuesBetween");
  }

  @Override
  public Collection<OIdentifiable> getValuesMajor(Object fromKey, boolean isInclusive, int maxValuesToFetch,
      ValuesTransformer<V> transformer) {
    throw new UnsupportedOperationException("getValuesMajor");
  }

  @Override
  public Collection<OIdentifiable> getValuesMinor(Object toKey, boolean isInclusive, int maxValuesToFetch,
      ValuesTransformer<V> transformer) {
    throw new UnsupportedOperationException("getValuesMinor");
  }

  @Override
  public Collection<ODocument> getEntriesMajor(Object fromKey, boolean isInclusive, int maxEntriesToFetch,
      ValuesTransformer<V> transformer) {
    throw new UnsupportedOperationException("getEntriesMajor");
  }

  @Override
  public Collection<ODocument> getEntriesMinor(Object toKey, boolean isInclusive, int maxEntriesToFetch,
      ValuesTransformer<V> transformer) {
    throw new UnsupportedOperationException("getEntriesMinor");
  }

  @Override
  public Collection<ODocument> getEntriesBetween(Object iRangeFrom, Object iRangeTo, boolean iInclusive, int maxEntriesToFetch,
      ValuesTransformer<V> transformer) {
    throw new UnsupportedOperationException("getEntriesBetween");
  }

  @Override
  public long count(Object rangeFrom, boolean fromInclusive, Object rangeTo, boolean toInclusive, int maxValuesToFetch,
      ValuesTransformer<V> transformer) {
    throw new UnsupportedOperationException("count");
  }

  @Override
  public Iterator<V> valuesIterator() {
    throw new UnsupportedOperationException("valuesIterator");
  }

  @Override
  public Iterator<V> inverseValuesIterator() {
    throw new UnsupportedOperationException("inverseValuesIterator");
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

  @Override
  public Iterator<Map.Entry<Object, V>> inverseIterator() {
    throw new UnsupportedOperationException("inverseIterator");
  }

  private ODatabaseRecord getDatabase() {
    return ODatabaseRecordThreadLocal.INSTANCE.get();
  }

  private final class EntriesIterator implements Iterator<Map.Entry<Object, V>> {
    private int                                 size = 0;
    private int                                 nextEntriesIndex;
    private OHashIndexBucket.Entry<Object, V>[] entries;

    private EntriesIterator() {
      OHashIndexBucket.Entry<Object, V> firstEntry = hashTable.firstEntry();
      if (firstEntry == null)
        entries = new OHashIndexBucket.Entry[0];
      else
        entries = hashTable.ceilingEntries(firstEntry.key);

      size += entries.length;
    }

    @Override
    public boolean hasNext() {
      return entries.length > 0;
    }

    @Override
    public Map.Entry<Object, V> next() {
      if (entries.length == 0)
        throw new NoSuchElementException();

      final OHashIndexBucket.Entry<Object, V> bucketEntry = entries[nextEntriesIndex];
      nextEntriesIndex++;

      if (nextEntriesIndex >= entries.length) {
        entries = hashTable.higherEntries(entries[entries.length - 1].key);
        size += entries.length;

        nextEntriesIndex = 0;
      }

      return new Map.Entry<Object, V>() {
        @Override
        public Object getKey() {
          return bucketEntry.key;
        }

        @Override
        public V getValue() {
          return bucketEntry.value;
        }

        @Override
        public V setValue(V value) {
          throw new UnsupportedOperationException("setValue");
        }
      };
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException("remove");
    }
  }

  private final class KeysIterable implements Iterable<Object> {
    @Override
    public Iterator<Object> iterator() {
      final EntriesIterator entriesIterator = new EntriesIterator();
      return new Iterator<Object>() {
        @Override
        public boolean hasNext() {
          return entriesIterator.hasNext();
        }

        @Override
        public Object next() {
          return entriesIterator.next().getKey();
        }

        @Override
        public void remove() {
          entriesIterator.remove();
        }
      };
    }
  }
}
