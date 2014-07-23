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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.*;
import com.orientechnologies.orient.core.iterator.OEmptyIterator;
import com.orientechnologies.orient.core.record.impl.ORecordBytes;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializer;

/**
 * @author Andrey Lomakin
 * @since 15.07.13
 */
public class OMemoryHashMapIndexEngine<V> implements OIndexEngine<V> {
  private final ConcurrentMap<Object, V> concurrentHashMap = new ConcurrentHashMap<Object, V>();
  private volatile ORID                  identity;

  @Override
  public void init() {
  }

  @Override
  public void flush() {
  }

  @Override
  public void create(String indexName, OIndexDefinition indexDefinition, String clusterIndexName,
      OStreamSerializer valueSerializer, boolean isAutomatic) {
    final ODatabaseRecord database = getDatabase();
    final ORecordBytes identityRecord = new ORecordBytes();

    database.save(identityRecord, clusterIndexName);
    identity = identityRecord.getIdentity();
  }

  @Override
  public void delete() {
  }

  @Override
  public void deleteWithoutLoad(String indexName) {
  }

  @Override
  public void load(ORID indexRid, String indexName, OIndexDefinition indexDefinition, OStreamSerializer valueSerializer,
      boolean isAutomatic) {
  }

  @Override
  public boolean contains(Object key) {
    return concurrentHashMap.containsKey(key);
  }

  @Override
  public boolean remove(Object key) {
    return concurrentHashMap.remove(key) != null;
  }

  @Override
  public ORID getIdentity() {
    return identity;
  }

  @Override
  public void clear() {
    concurrentHashMap.clear();
  }

  @Override
  public OIndexCursor cursor(final ValuesTransformer<V> valuesTransformer) {
    return new OIndexAbstractCursor() {
      private Iterator<Map.Entry<Object, V>> entryIterator   = concurrentHashMap.entrySet().iterator();

      private Object                         currentKey;
      private Iterator<OIdentifiable>        currentIterator = new OEmptyIterator<OIdentifiable>();

      @Override
      public Map.Entry<Object, OIdentifiable> nextEntry() {
        if (currentIterator == null)
          return null;

        if (currentIterator.hasNext())
          return nextCursorValue();

        while (currentIterator != null && !currentIterator.hasNext()) {
          final Map.Entry<Object, V> entry = entryIterator.next();
          currentKey = entry.getKey();

          final V value = entry.getValue();
          if (valuesTransformer == null)
            currentIterator = Collections.singletonList((OIdentifiable) value).iterator();
          else
            currentIterator = valuesTransformer.transformFromValue(value).iterator();
        }

        if (currentIterator != null && currentIterator.hasNext())
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
  public OIndexCursor descCursor(ValuesTransformer<V> valuesTransformer) {
    throw new UnsupportedOperationException();
  }

  @Override
  public OIndexKeyCursor keyCursor() {
    return new OIndexKeyCursor() {
      private Iterator<Object> keysIterator = concurrentHashMap.keySet().iterator();

      @Override
      public Object next(int prefetchSize) {
        if (!keysIterator.hasNext())
          return null;

        return keysIterator.next();
      }
    };
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
  }

  @Override
  public void beforeTxBegin() {
  }

  @Override
  public V get(Object key) {
    return concurrentHashMap.get(key);
  }

  @Override
  public void put(Object key, V value) {
    concurrentHashMap.put(key, value);
  }

  @Override
  public Object getFirstKey() {
    throw new UnsupportedOperationException("getFirstKey");
  }

  @Override
  public Object getLastKey() {
    throw new UnsupportedOperationException("getLastKey");
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
  public long size(ValuesTransformer<V> transformer) {
    if (transformer == null)
      return concurrentHashMap.size();
    else {
      long counter = 0;
      for (V value : concurrentHashMap.values()) {
        counter += transformer.transformFromValue(value).size();
      }
      return counter;
    }
  }

  @Override
  public boolean hasRangeQuerySupport() {
    return false;
  }

  private ODatabaseRecord getDatabase() {
    return ODatabaseRecordThreadLocal.INSTANCE.get();
  }

}
