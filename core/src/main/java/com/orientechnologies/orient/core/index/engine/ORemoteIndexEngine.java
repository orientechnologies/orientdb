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

import java.util.Map;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.*;
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
  public void create(OIndexDefinition indexDefinition, String clusterIndexName, OStreamSerializer valueSerializer,
      boolean isAutomatic) {
  }

  @Override
  public void deleteWithoutLoad(String indexName) {
  }

  @Override
  public void delete() {
  }

  @Override
  public void load(String indexName, OIndexDefinition indexDefinition, OStreamSerializer valueSerializer, boolean isAutomatic) {
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
  public Object getFirstKey() {
    return null;
  }

  @Override
  public Object getLastKey() {
    return null;
  }

  @Override
  public OIndexCursor iterateEntriesBetween(Object rangeFrom, boolean fromInclusive, Object rangeTo, boolean toInclusive,
      boolean ascSortOrder, ValuesTransformer transformer) {
    return new OIndexAbstractCursor() {
      @Override
      public Map.Entry<Object, OIdentifiable> nextEntry() {
        return null;
      }
    };
  }

  @Override
  public OIndexCursor iterateEntriesMajor(Object fromKey, boolean isInclusive, boolean ascSortOrder, ValuesTransformer transformer) {
    return new OIndexAbstractCursor() {
      @Override
      public Map.Entry nextEntry() {
        return null;
      }
    };
  }

  @Override
  public OIndexCursor iterateEntriesMinor(Object toKey, boolean isInclusive, boolean ascSortOrder, ValuesTransformer transformer) {
    return new OIndexAbstractCursor() {
      @Override
      public Map.Entry nextEntry() {
        return null;
      }
    };
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
  public OIndexCursor cursor(ValuesTransformer valuesTransformer) {
    throw new UnsupportedOperationException("cursor");
  }

  @Override
  public OIndexCursor descCursor(ValuesTransformer valuesTransformer) {
    throw new UnsupportedOperationException();
  }

  @Override
  public OIndexKeyCursor keyCursor() {
    throw new UnsupportedOperationException("keyCursor");
  }

  @Override
  public int getVersion() {
    return -1;
  }
}
