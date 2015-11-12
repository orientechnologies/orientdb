/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
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
 *  * For more information: http://www.orientechnologies.com
 *
 */
package com.orientechnologies.orient.core.index.engine;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.util.OCommonConst;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.*;
import com.orientechnologies.orient.core.index.hashindex.local.*;
import com.orientechnologies.orient.core.iterator.OEmptyIterator;
import com.orientechnologies.orient.core.serialization.serializer.binary.OBinarySerializerFactory;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.index.OCompositeKeySerializer;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.index.OSimpleKeySerializer;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializer;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;

/**
 * @author Andrey Lomakin
 * @since 15.07.13
 */
public final class OHashTableIndexEngine<V> implements OIndexEngine<V> {
  public static final int                        VERSION                    = 2;

  public static final String                     METADATA_FILE_EXTENSION    = ".him";
  public static final String                     TREE_FILE_EXTENSION        = ".hit";
  public static final String                     BUCKET_FILE_EXTENSION      = ".hib";
  public static final String                     NULL_BUCKET_FILE_EXTENSION = ".hnb";

  private final OHashTable<Object, V>            hashTable;
  private final OMurmurHash3HashFunction<Object> hashFunction;

  private int                                    version;

  public OHashTableIndexEngine(String name, Boolean durableInNonTxMode, OAbstractPaginatedStorage storage, int version) {
    hashFunction = new OMurmurHash3HashFunction<Object>();

    boolean durableInNonTx;
    if (durableInNonTxMode == null)
      durableInNonTx = OGlobalConfiguration.INDEX_DURABLE_IN_NON_TX_MODE.getValueAsBoolean();
    else
      durableInNonTx = durableInNonTxMode;

    this.version = version;
    if (version < 2)
      hashTable = new OLocalHashTable20<Object, V>(name, METADATA_FILE_EXTENSION, TREE_FILE_EXTENSION, BUCKET_FILE_EXTENSION,
          NULL_BUCKET_FILE_EXTENSION, hashFunction, durableInNonTx, storage);
    else
      hashTable = new OLocalHashTable<Object, V>(name, METADATA_FILE_EXTENSION, TREE_FILE_EXTENSION, BUCKET_FILE_EXTENSION,
          NULL_BUCKET_FILE_EXTENSION, hashFunction, durableInNonTx, storage);
  }

  @Override
  public void init() {
  }

  @Override
  public void create(OIndexDefinition indexDefinition, String clusterIndexName, OStreamSerializer valueSerializer,
      boolean isAutomatic) {
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

    hashFunction.setValueSerializer(keySerializer);
    hashTable.create(keySerializer, (OBinarySerializer<V>) valueSerializer, indexDefinition != null ? indexDefinition.getTypes()
        : null, indexDefinition != null && !indexDefinition.isNullValuesIgnored());
  }

  @Override
  public void flush() {
    hashTable.flush();
  }

  @Override
  public void deleteWithoutLoad(String indexName) {
    hashTable.deleteWithoutLoad(indexName, (OAbstractPaginatedStorage) getDatabase().getStorage().getUnderlying());
  }

  @Override
  public void delete() {
    hashTable.delete();
  }

  @Override
  public void load(String indexName, OIndexDefinition indexDefinition, OStreamSerializer valueSerializer, boolean isAutomatic) {
    hashTable.load(indexName, indexDefinition != null ? indexDefinition.getTypes() : null, indexDefinition != null
        && !indexDefinition.isNullValuesIgnored());
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
  public int getVersion() {
    return version;
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
          entries = OCommonConst.EMPTY_BUCKET_ENTRY_ARRAY;
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
  public OIndexCursor descCursor(final ValuesTransformer<V> valuesTransformer) {
    return new OIndexAbstractCursor() {
      private int                                 nextEntriesIndex;
      private OHashIndexBucket.Entry<Object, V>[] entries;

      private Iterator<OIdentifiable>             currentIterator = new OEmptyIterator<OIdentifiable>();
      private Object                              currentKey;

      {
        OHashIndexBucket.Entry<Object, V> lastEntry = hashTable.lastEntry();
        if (lastEntry == null)
          entries = OCommonConst.EMPTY_BUCKET_ENTRY_ARRAY;
        else
          entries = hashTable.floorEntries(lastEntry.key);

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
          if (valuesTransformer != null) {
            currentIterator = valuesTransformer.transformFromValue(value).iterator();
          } else
            currentIterator = Collections.singletonList((OIdentifiable) value).iterator();

          nextEntriesIndex--;

          if (nextEntriesIndex < 0) {
            entries = hashTable.lowerEntries(entries[0].key);

            nextEntriesIndex = entries.length - 1;
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
          entries = OCommonConst.EMPTY_BUCKET_ENTRY_ARRAY;
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

  private ODatabaseDocumentInternal getDatabase() {
    return ODatabaseRecordThreadLocal.INSTANCE.get();
  }
}
