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

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.index.OIndexEngine;
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
  public void load(ORID indexRid, String indexName, OIndexDefinition indexDefinition, boolean isAutomatic) {
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
  public Iterator<Map.Entry<Object, V>> iterator() {
    return concurrentHashMap.entrySet().iterator();
  }

  @Override
  public Iterator<Map.Entry<Object, V>> inverseIterator() {
    throw new UnsupportedOperationException("inverseIterator");
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
  public Iterable<Object> keys() {
    return concurrentHashMap.keySet();
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
  public void getValuesBetween(Object rangeFrom, boolean fromInclusive, Object rangeTo, boolean toInclusive,
      ValuesTransformer<V> transformer, ValuesResultListener valuesResultListener) {
    throw new UnsupportedOperationException("getValuesBetween");
  }

  @Override
  public void getValuesMajor(Object fromKey, boolean isInclusive, ValuesTransformer<V> transformer,
      ValuesResultListener valuesResultListener) {
    throw new UnsupportedOperationException("getValuesMajor");
  }

  @Override
  public void getValuesMinor(Object toKey, boolean isInclusive, ValuesTransformer<V> transformer,
      ValuesResultListener valuesResultListener) {
    throw new UnsupportedOperationException("getValuesMinor");
  }

  @Override
  public void getEntriesMajor(Object fromKey, boolean isInclusive, ValuesTransformer<V> transformer,
      EntriesResultListener entriesResultListener) {
    throw new UnsupportedOperationException("getEntriesMajor");
  }

  @Override
  public void getEntriesMinor(Object toKey, boolean isInclusive, ValuesTransformer<V> transformer,
      EntriesResultListener entriesResultListener) {
    throw new UnsupportedOperationException("getEntriesMinor");
  }

  @Override
  public void getEntriesBetween(Object iRangeFrom, Object iRangeTo, boolean iInclusive, ValuesTransformer<V> transformer,
      EntriesResultListener entriesResultListener) {
    throw new UnsupportedOperationException("getEntriesBetween");
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
