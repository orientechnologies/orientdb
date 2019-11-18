/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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
 *  * For more information: http://orientdb.com
 *
 */
package com.orientechnologies.orient.core.storage.index.engine;

import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.orient.core.encryption.OEncryption;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.IndexCursor;
import com.orientechnologies.orient.core.index.IndexKeySpliterator;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.index.OIndexKeyUpdater;
import com.orientechnologies.orient.core.index.engine.OIndexEngine;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;

import java.util.Map;
import java.util.Spliterator;
import java.util.function.Consumer;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 18.07.13
 */
public class ORemoteIndexEngine implements OIndexEngine {
  private final String name;
  private final int    id;

  public ORemoteIndexEngine(int id, String name) {
    this.id = id;
    this.name = name;
  }

  @Override
  public int getId() {
    return id;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public String getIndexNameByKey(Object key) {
    return name;
  }

  @Override
  public void init(String indexName, String indexType, OIndexDefinition indexDefinition, boolean isAutomatic, ODocument metadata) {
  }

  @Override
  public void flush() {
  }

  @Override
  public void create(OBinarySerializer valueSerializer, boolean isAutomatic, OType[] keyTypes, boolean nullPointerSupport,
      OBinarySerializer keySerializer, int keySize, Map<String, String> engineProperties, OEncryption encryption) {
  }

  @Override
  public void delete() {
  }

  @Override
  public void load(String indexName, OBinarySerializer valueSerializer, boolean isAutomatic, OBinarySerializer keySerializer,
      OType[] keyTypes, boolean nullPointerSupport, int keySize, Map<String, String> engineProperties, OEncryption encryption) {
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
  public void clear() {
  }

  @Override
  public void close() {
  }

  @Override
  public Object get(Object key) {
    return null;
  }

  @Override
  public void put(Object key, Object value) {
  }

  @Override
  public void update(Object key, OIndexKeyUpdater<Object> updater) {

  }

  @Override
  public boolean validatedPut(Object key, ORID value, Validator<Object, ORID> validator) {
    return false;
  }

  @Override
  public Object getFirstKey() {
    return null;
  }

  @Override
  public Object getLastKey() {
    return null;
  }

  @Override
  public IndexCursor iterateEntriesBetween(Object rangeFrom, boolean fromInclusive, Object rangeTo, boolean toInclusive,
      boolean ascSortOrder, ValuesTransformer transformer) {
    return new EntriesBetweenCursor();
  }

  @Override
  public IndexCursor iterateEntriesMajor(Object fromKey, boolean isInclusive, boolean ascSortOrder, ValuesTransformer transformer) {
    return new EntriesMajorCursor();
  }

  @Override
  public IndexCursor iterateEntriesMinor(Object toKey, boolean isInclusive, boolean ascSortOrder, ValuesTransformer transformer) {
    return new EntriesMinorCursor();
  }

  @Override
  public long size(ValuesTransformer transformer) {
    return 0;
  }

  @Override
  public boolean hasRangeQuerySupport() {
    return false;
  }

  @Override
  public IndexCursor cursor(ValuesTransformer valuesTransformer) {
    throw new UnsupportedOperationException("cursor");
  }

  @Override
  public IndexCursor descCursor(ValuesTransformer valuesTransformer) {
    throw new UnsupportedOperationException();
  }

  @Override
  public IndexKeySpliterator keyCursor() {
    throw new UnsupportedOperationException("keySpliterator");
  }

  @Override
  public boolean acquireAtomicExclusiveLock(Object key) {
    throw new UnsupportedOperationException("atomic locking is not supported by remote index engine");
  }

  private static class EntriesBetweenCursor implements IndexCursor {
    @Override
    public boolean tryAdvance(Consumer<? super ORawPair<Object, ORID>> action) {
      return false;
    }

    @Override
    public Spliterator<ORawPair<Object, ORID>> trySplit() {
      return null;
    }

    @Override
    public long estimateSize() {
      return 0;
    }

    @Override
    public int characteristics() {
      return 0;
    }
  }

  private static class EntriesMajorCursor implements IndexCursor {
    @Override
    public boolean tryAdvance(Consumer<? super ORawPair<Object, ORID>> action) {
      return false;
    }

    @Override
    public Spliterator<ORawPair<Object, ORID>> trySplit() {
      return null;
    }

    @Override
    public long estimateSize() {
      return 0;
    }

    @Override
    public int characteristics() {
      return 0;
    }
  }

  private static class EntriesMinorCursor implements IndexCursor {
    @Override
    public boolean tryAdvance(Consumer<? super ORawPair<Object, ORID>> action) {
      return false;
    }

    @Override
    public Spliterator<ORawPair<Object, ORID>> trySplit() {
      return null;
    }

    @Override
    public long estimateSize() {
      return 0;
    }

    @Override
    public int characteristics() {
      return 0;
    }
  }
}
