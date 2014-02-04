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

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.index.OIndexEngine;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializer;

/**
 * @author Andrey Lomakin
 * @since 18.07.13
 */
public class ORemoteIndexEngine implements OIndexEngine {
  @Override
  public void init() {
  }

  @Override
  public void flush() {
  }

  @Override
  public void create(String indexName, OIndexDefinition indexDefinition, String clusterIndexName,
      OStreamSerializer valueSerializer, boolean isAutomatic) {
  }

  @Override
  public void deleteWithoutLoad(String indexName) {
  }

  @Override
  public void delete() {
  }

  @Override
  public void load(ORID indexRid, String indexName, OIndexDefinition indexDefinition, boolean isAutomatic) {
  }

  @Override
  public boolean contains(Object key) {
    return false;
  }

  @Override
  public boolean remove(Object key) {
    return false;
  }

  @Override
  public ORID getIdentity() {
    return null;
  }

  @Override
  public void clear() {
  }

  @Override
  public Iterator<Map.Entry> iterator() {
    return null;
  }

  @Override
  public Iterator<Map.Entry> inverseIterator() {
    return null;
  }

  @Override
  public Iterator valuesIterator() {
    return null;
  }

  @Override
  public Iterator inverseValuesIterator() {
    return null;
  }

  @Override
  public Iterable<Object> keys() {
    return null;
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
  public Object get(Object key) {
    return null;
  }

  @Override
  public void put(Object key, Object value) {
  }

  @Override
  public void getValuesBetween(Object rangeFrom, boolean fromInclusive, Object rangeTo, boolean toInclusive,
      ValuesTransformer transformer, ValuesResultListener resultListener) {
  }

  @Override
  public void getValuesMajor(Object fromKey, boolean isInclusive, ValuesTransformer transformer,
      ValuesResultListener valuesResultListener) {
  }

  @Override
  public void getValuesMinor(Object toKey, boolean isInclusive, ValuesTransformer transformer,
      ValuesResultListener valuesResultListener) {
  }

  @Override
  public void getEntriesMajor(Object fromKey, boolean isInclusive, ValuesTransformer transformer,
      EntriesResultListener entriesResultListener) {
  }

  @Override
  public void getEntriesMinor(Object toKey, boolean isInclusive, ValuesTransformer transformer,
      EntriesResultListener entriesResultListener) {
  }

  @Override
  public void getEntriesBetween(Object iRangeFrom, Object iRangeTo, boolean iInclusive, ValuesTransformer transformer,
      EntriesResultListener entriesResultListener) {
  }

  @Override
  public long size(ValuesTransformer transformer) {
    return 0;
  }

  @Override
  public boolean hasRangeQuerySupport() {
    return false;
  }
}
