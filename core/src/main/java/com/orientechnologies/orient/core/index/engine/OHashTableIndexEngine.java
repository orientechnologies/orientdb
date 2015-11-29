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

import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.util.OCommonConst;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.index.OIndexAbstractCursor;
import com.orientechnologies.orient.core.index.OIndexCursor;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.index.OIndexEngine;
import com.orientechnologies.orient.core.index.OIndexKeyCursor;
import com.orientechnologies.orient.core.index.hashindex.local.OHashIndexBucket;
import com.orientechnologies.orient.core.index.hashindex.local.OHashTable;
import com.orientechnologies.orient.core.index.hashindex.local.OLocalHashTable;
import com.orientechnologies.orient.core.index.hashindex.local.OLocalHashTable20;
import com.orientechnologies.orient.core.index.hashindex.local.OMurmurHash3HashFunction;
import com.orientechnologies.orient.core.iterator.OEmptyIterator;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * @author Andrey Lomakin
 * @since 15.07.13
 */
public final class OHashTableIndexEngine implements OIndexEngine {
  public static final int VERSION = 2;

  public static final String METADATA_FILE_EXTENSION    = ".him";
  public static final String TREE_FILE_EXTENSION        = ".hit";
  public static final String BUCKET_FILE_EXTENSION      = ".hib";
  public static final String NULL_BUCKET_FILE_EXTENSION = ".hnb";

  private final OHashTable<Object, Object>       hashTable;
  private final OMurmurHash3HashFunction<Object> hashFunction;

  private int version;

  private final String name;

  public OHashTableIndexEngine(String name, Boolean durableInNonTxMode, OAbstractPaginatedStorage storage, int version) {
    hashFunction = new OMurmurHash3HashFunction<Object>();

    boolean durableInNonTx;
    if (durableInNonTxMode == null)
      durableInNonTx = OGlobalConfiguration.INDEX_DURABLE_IN_NON_TX_MODE.getValueAsBoolean();
    else
      durableInNonTx = durableInNonTxMode;

    this.version = version;
    if (version < 2)
      hashTable = new OLocalHashTable20<Object, Object>(name, METADATA_FILE_EXTENSION, TREE_FILE_EXTENSION, BUCKET_FILE_EXTENSION,
          NULL_BUCKET_FILE_EXTENSION, hashFunction, durableInNonTx, storage);
    else
      hashTable = new OLocalHashTable<Object, Object>(name, METADATA_FILE_EXTENSION, TREE_FILE_EXTENSION, BUCKET_FILE_EXTENSION,
          NULL_BUCKET_FILE_EXTENSION, hashFunction, durableInNonTx, storage);

    this.name = name;
  }

  @Override
  public void init(String indexName, String indexType, OIndexDefinition indexDefinition, boolean isAutomatic, ODocument metadata) {
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public void create(OBinarySerializer valueSerializer, boolean isAutomatic, OType[] keyTypes, boolean nullPointerSupport,
                        OBinarySerializer keySerializer, int keySize, Set<String> clustersToIndex, ODocument metadata) {
    hashFunction.setValueSerializer(keySerializer);

    hashTable.create(keySerializer, valueSerializer, keyTypes, nullPointerSupport);
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
  public void load(String indexName, OBinarySerializer valueSerializer, boolean isAutomatic, OBinarySerializer keySerializer,
      OType[] keyTypes, boolean nullPointerSupport, int keySize) {
    hashTable.load(indexName, keyTypes, nullPointerSupport);
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
  public Object get(Object key) {
    return hashTable.get(key);
  }

  @Override
  public void put(Object key, Object value) {
    hashTable.put(key, value);
  }

  @Override
  public long size(ValuesTransformer transformer) {
    if (transformer == null)
      return hashTable.size();
    else {
      OHashIndexBucket.Entry<Object, Object> firstEntry = hashTable.firstEntry();
      if (firstEntry == null)
        return 0;

      OHashIndexBucket.Entry<Object, Object>[] entries = hashTable.ceilingEntries(firstEntry.key);
      long counter = 0;

      while (entries.length > 0) {
        for (OHashIndexBucket.Entry<Object, Object> entry : entries)
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
      boolean ascSortOrder, ValuesTransformer transformer) {
    throw new UnsupportedOperationException("iterateEntriesBetween");
  }

  @Override
  public OIndexCursor iterateEntriesMajor(Object fromKey, boolean isInclusive, boolean ascSortOrder,
      ValuesTransformer transformer) {
    throw new UnsupportedOperationException("iterateEntriesMajor");
  }

  @Override
  public OIndexCursor iterateEntriesMinor(Object toKey, boolean isInclusive, boolean ascSortOrder, ValuesTransformer transformer) {
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
  public OIndexCursor cursor(final ValuesTransformer valuesTransformer) {
    return new OIndexAbstractCursor() {
      private int nextEntriesIndex;
      private OHashIndexBucket.Entry<Object, Object>[] entries;

      private Iterator<OIdentifiable> currentIterator = new OEmptyIterator<OIdentifiable>();
      private Object currentKey;

      {
        OHashIndexBucket.Entry<Object, Object> firstEntry = hashTable.firstEntry();
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

          final OHashIndexBucket.Entry<Object, Object> bucketEntry = entries[nextEntriesIndex];

          currentKey = bucketEntry.key;

          Object value = bucketEntry.value;
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
  public OIndexCursor descCursor(final ValuesTransformer valuesTransformer) {
    return new OIndexAbstractCursor() {
      private int nextEntriesIndex;
      private OHashIndexBucket.Entry<Object, Object>[] entries;

      private Iterator<OIdentifiable> currentIterator = new OEmptyIterator<OIdentifiable>();
      private Object currentKey;

      {
        OHashIndexBucket.Entry<Object, Object> lastEntry = hashTable.lastEntry();
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

          final OHashIndexBucket.Entry<Object, Object> bucketEntry = entries[nextEntriesIndex];

          currentKey = bucketEntry.key;

          Object value = bucketEntry.value;
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
      private int nextEntriesIndex;
      private OHashIndexBucket.Entry<Object, Object>[] entries;

      {
        OHashIndexBucket.Entry<Object, Object> firstEntry = hashTable.firstEntry();
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

        final OHashIndexBucket.Entry<Object, Object> bucketEntry = entries[nextEntriesIndex];
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
