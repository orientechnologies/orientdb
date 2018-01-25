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

package com.orientechnologies.orient.core.index;

import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 6/29/13
 */
public interface OIndexEngine {
  void init(String indexName, String indexType, OIndexDefinition indexDefinition, boolean isAutomatic, ODocument metadata);

  void flush();

  void create(OBinarySerializer valueSerializer, boolean isAutomatic, OType[] keyTypes, boolean nullPointerSupport,
      OBinarySerializer keySerializer, int keySize, Set<String> clustersToIndex, Map<String, String> engineProperties,
      ODocument metadata);

  void delete();

  void deleteWithoutLoad(String indexName);

  void load(String indexName, OBinarySerializer valueSerializer, boolean isAutomatic, OBinarySerializer keySerializer,
      OType[] keyTypes, boolean nullPointerSupport, int keySize, Map<String, String> engineProperties);

  boolean contains(Object key);

  boolean remove(Object key);

  void clear();

  void close();

  Object get(Object key);

  void put(Object key, Object value);

  default void update(Object key, OIndexKeyUpdater<Object> value) {
    throw new UnsupportedOperationException();
  }

  /**
   * Puts the given value under the given key into this index engine. Validates the operation using the provided validator.
   *
   * @param key       the key to put the value under.
   * @param value     the value to put.
   * @param validator the operation validator.
   *
   * @return {@code true} if the validator allowed the put, {@code false} otherwise.
   *
   * @see Validator#validate(Object, Object, Object)
   */
  boolean validatedPut(Object key, OIdentifiable value, Validator<Object, OIdentifiable> validator);

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

  /**
   * <p>Acquires exclusive lock in the active atomic operation running on the current thread for this index engine.
   * <p>
   * <p>If this index engine supports a more narrow locking, for example key-based sharding, it may use the provided {@code key} to
   * infer a more narrow lock scope, but that is not a requirement.
   *
   * @param key the index key to lock.
   *
   * @return {@code true} if this index was locked entirely, {@code false} if this index locking is sensitive to the provided {@code
   * key} and only some subset of this index was locked.
   */
  boolean acquireAtomicExclusiveLock(Object key);

  String getIndexNameByKey(Object key);

  interface ValuesTransformer {
    Collection<OIdentifiable> transformFromValue(Object value);
  }

  /**
   * Put operation validator.
   *
   * @param <K> the key type.
   * @param <V> the value type.
   */
  interface Validator<K, V> {

    /**
     * Indicates that a put request should be silently ignored by the store.
     *
     * @see #validate(Object, Object, Object)
     */
    Object IGNORE = new Object();

    /**
     * Validates the put operation for the given key, the old value and the new value. May throw an exception to abort the current
     * put operation with an error.
     *
     * @param key      the put operation key.
     * @param oldValue the old value or {@code null} if no value is currently stored.
     * @param newValue the new value passed to {@link #validatedPut(Object, OIdentifiable, Validator)}.
     *
     * @return the new value to store, may differ from the passed one, or the special {@link #IGNORE} value to silently ignore the
     * put operation request being processed.
     */
    Object validate(K key, V oldValue, V newValue);
  }
}
