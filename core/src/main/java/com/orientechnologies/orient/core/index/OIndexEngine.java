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

package com.orientechnologies.orient.core.index;

import java.util.Collection;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializer;

/**
 * @author Andrey Lomakin
 * @since 6/29/13
 */
public interface OIndexEngine<V> {
  void init();

  void flush();

  void create(OIndexDefinition indexDefinition, String clusterIndexName, OStreamSerializer valueSerializer, boolean isAutomatic);

  void delete();

  void deleteWithoutLoad(String indexName);

  void load(String indexName, OIndexDefinition indexDefinition, OStreamSerializer valueSerializer, boolean isAutomatic);

  boolean contains(Object key);

  boolean remove(Object key);

  void clear();

  void close();

  V get(Object key);

  void put(Object key, V value);

  public Object getFirstKey();

  public Object getLastKey();

  OIndexCursor iterateEntriesBetween(Object rangeFrom, boolean fromInclusive, Object rangeTo, boolean toInclusive,
      boolean ascSortOrder, ValuesTransformer<V> transformer);

  OIndexCursor iterateEntriesMajor(Object fromKey, boolean isInclusive, boolean ascSortOrder, ValuesTransformer<V> transformer);

  OIndexCursor iterateEntriesMinor(final Object toKey, final boolean isInclusive, boolean ascSortOrder,
      ValuesTransformer<V> transformer);

  OIndexCursor cursor(ValuesTransformer<V> valuesTransformer);

  OIndexCursor descCursor(ValuesTransformer<V> valuesTransformer);

  OIndexKeyCursor keyCursor();

  long size(ValuesTransformer<V> transformer);

  boolean hasRangeQuerySupport();

  int getVersion();

  interface ValuesTransformer<V> {
    Collection<OIdentifiable> transformFromValue(V value);
  }
}
