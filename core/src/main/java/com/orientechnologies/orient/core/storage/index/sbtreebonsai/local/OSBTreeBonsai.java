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

package com.orientechnologies.orient.core.storage.index.sbtreebonsai.local;

import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.orient.core.storage.cache.OReadCache;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;
import com.orientechnologies.orient.core.storage.index.sbtree.OTreeInternal;
import com.orientechnologies.orient.core.storage.index.sbtree.local.v1.OSBTreeV1;
import com.orientechnologies.orient.core.storage.ridbag.sbtree.Change;
import com.orientechnologies.orient.core.storage.ridbag.sbtree.OBonsaiCollectionPointer;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;

/**
 * The tree that have similar structure to {@link OSBTreeV1} and designed to store small entries.
 * <br>
 * <br>
 * The tree algorithm is the same as in {@link OSBTreeV1}, but it have tiny buckets.<br>
 * The {@link OReadCache} could contain several buckets. That's why there is no huge resource
 * consuming when you have lots of OSBTreeBonsai that contain only few records.<br>
 * <br>
 * <code>
 * +--------------------------------------------------------------------------------------------+<br>
 * | DISK CACHE PAGE                                                                            |<br>
 * |+---------------+ +---------------+ +---------------+ +---------------+ +---------------+   |<br>
 * || Bonsai Bucket | | Bonsai Bucket | | Bonsai Bucket | | Bonsai Bucket | | Bonsai Bucket |...|<br>
 * |+---------------+ +---------------+ +---------------+ +---------------+ +---------------+   |<br>
 * +--------------------------------------------------------------------------------------------+<br>
 * </code>
 *
 * @author Artem Orobets (enisher-at-gmail.com)
 * @since 1.7rc1
 */
public interface OSBTreeBonsai<K, V> extends OTreeInternal<K, V> {
  /**
   * Gets id of file where this bonsai tree is stored.
   *
   * @return id of file in {@link OReadCache}
   */
  long getFileId();

  /** @return the pointer to the root bucket in tree. */
  OBonsaiBucketPointer getRootBucketPointer();

  /** @return pointer to a collection. */
  OBonsaiCollectionPointer getCollectionPointer();

  /**
   * Search for entry with specific key and return its value.
   *
   * @param key
   * @return value associated with given key, NULL if no value is associated.
   */
  V get(K key);

  boolean put(OAtomicOperation atomicOperation, K key, V value) throws IOException;

  /**
   * Deletes all entries from tree.
   *
   * @param atomicOperation
   */
  void clear(OAtomicOperation atomicOperation) throws IOException;

  /**
   * Deletes whole tree. After this operation tree is no longer usable.
   *
   * @param atomicOperation
   */
  void delete(OAtomicOperation atomicOperation);

  boolean isEmpty();

  V remove(OAtomicOperation atomicOperation, K key) throws IOException;

  Collection<V> getValuesMinor(K key, boolean inclusive, int maxValuesToFetch);

  void loadEntriesMinor(K key, boolean inclusive, RangeResultListener<K, V> listener);

  Collection<V> getValuesMajor(K key, boolean inclusive, int maxValuesToFetch);

  void loadEntriesMajor(
      K key, boolean inclusive, boolean ascSortOrder, RangeResultListener<K, V> listener);

  Collection<V> getValuesBetween(
      K keyFrom, boolean fromInclusive, K keyTo, boolean toInclusive, int maxValuesToFetch);

  K firstKey();

  K lastKey();

  void loadEntriesBetween(
      K keyFrom,
      boolean fromInclusive,
      K keyTo,
      boolean toInclusive,
      RangeResultListener<K, V> listener);

  /**
   * Hardcoded method for Bag to avoid creation of extra layer.
   *
   * <p>Don't make any changes to tree.
   *
   * @param changes Bag changes
   * @return real bag size
   */
  int getRealBagSize(Map<K, Change> changes);

  OBinarySerializer<K> getKeySerializer();

  OBinarySerializer<V> getValueSerializer();
}
