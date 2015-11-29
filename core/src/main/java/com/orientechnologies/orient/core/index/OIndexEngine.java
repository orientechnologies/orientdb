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

import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;

import java.util.Collection;
import java.util.Set;

/**
 * @author Andrey Lomakin
 * @since 6/29/13
 */
public interface OIndexEngine {
  void init(String indexName, String indexType, OIndexDefinition indexDefinition, boolean isAutomatic, ODocument metadata);

  void flush();

  void create(OBinarySerializer valueSerializer, boolean isAutomatic, OType[] keyTypes, boolean nullPointerSupport,
                 OBinarySerializer keySerializer, int keySize, Set<String> clustersToIndex, ODocument metadata);

  void delete();

  void deleteWithoutLoad(String indexName);

  void load(String indexName, OBinarySerializer valueSerializer, boolean isAutomatic, OBinarySerializer keySerializer,
      OType[] keyTypes, boolean nullPointerSupport, int keySize);

  boolean contains(Object key);

  boolean remove(Object key);

  void clear();

  void close();

  Object get(Object key);

  void put(Object key, Object value);

  Object getFirstKey();

  Object getLastKey();

  OIndexCursor iterateEntriesBetween(Object rangeFrom, boolean fromInclusive, Object rangeTo, boolean toInclusive,
      boolean ascSortOrder, ValuesTransformer transformer);

  OIndexCursor iterateEntriesMajor(Object fromKey, boolean isInclusive, boolean ascSortOrder, ValuesTransformer transformer);

  OIndexCursor iterateEntriesMinor(final Object toKey, final boolean isInclusive, boolean ascSortOrder,
      ValuesTransformer transformer);

  OIndexCursor cursor(ValuesTransformer valuesTransformer);

  OIndexCursor descCursor(ValuesTransformer valuesTransformer);

  OIndexKeyCursor keyCursor();

  long size(ValuesTransformer transformer);

  boolean hasRangeQuerySupport();

  int getVersion();

  String getName();

  interface ValuesTransformer {
    Collection<OIdentifiable> transformFromValue(Object value);
  }
}
