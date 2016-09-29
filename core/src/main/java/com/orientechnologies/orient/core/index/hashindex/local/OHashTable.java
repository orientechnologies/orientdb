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
package com.orientechnologies.orient.core.index.hashindex.local;

import com.orientechnologies.common.comparator.ODefaultComparator;
import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.orient.core.metadata.schema.OType;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.Comparator;

/**
 * Created by lomak_000 on 15.04.2015.
 */
public interface OHashTable<K, V> {
  void create(OBinarySerializer<K> keySerializer, OBinarySerializer<V> valueSerializer, OType[] keyTypes,
      boolean nullKeyIsSupported);

  OBinarySerializer<K> getKeySerializer();

  void setKeySerializer(OBinarySerializer<K> keySerializer);

  OBinarySerializer<V> getValueSerializer();

  void setValueSerializer(OBinarySerializer<V> valueSerializer);

  V get(K key);

  void put(K key, V value);

  V remove(K key);

  void clear();

  OHashIndexBucket.Entry<K, V>[] higherEntries(K key);

  OHashIndexBucket.Entry<K, V>[] higherEntries(K key, int limit);

  void load(String name, OType[] keyTypes, boolean nullKeyIsSupported);

  void deleteWithoutLoad(String name);

  OHashIndexBucket.Entry<K, V>[] ceilingEntries(K key);

  OHashIndexBucket.Entry<K, V> firstEntry();

  OHashIndexBucket.Entry<K, V> lastEntry();

  OHashIndexBucket.Entry<K, V>[] lowerEntries(K key);

  OHashIndexBucket.Entry<K, V>[] floorEntries(K key);

  long size();

  void close();

  void delete();

  void flush();

  boolean isNullKeyIsSupported();

  /**
   * Acquires exclusive lock in the active atomic operation running on the current thread for this hash table.
   */
  void acquireAtomicExclusiveLock();

  String getName();

  public static final class BucketPath {
    public final BucketPath parent;
    public final int        hashMapOffset;
    public final int        itemIndex;
    public final int        nodeIndex;
    public final int        nodeGlobalDepth;
    public final int        nodeLocalDepth;

    public BucketPath(BucketPath parent, int hashMapOffset, int itemIndex, int nodeIndex, int nodeLocalDepth, int nodeGlobalDepth) {
      this.parent = parent;
      this.hashMapOffset = hashMapOffset;
      this.itemIndex = itemIndex;
      this.nodeIndex = nodeIndex;
      this.nodeGlobalDepth = nodeGlobalDepth;
      this.nodeLocalDepth = nodeLocalDepth;
    }
  }

  public static final class BucketSplitResult {
    public final long updatedBucketPointer;
    public final long newBucketPointer;
    public final int  newDepth;

    public BucketSplitResult(long updatedBucketPointer, long newBucketPointer, int newDepth) {
      this.updatedBucketPointer = updatedBucketPointer;
      this.newBucketPointer = newBucketPointer;
      this.newDepth = newDepth;
    }
  }

  public static final class NodeSplitResult {
    public final long[]  newNode;
    public final boolean allLeftHashMapsEqual;
    public final boolean allRightHashMapsEqual;

    @SuppressFBWarnings("EI_EXPOSE_REP2")
    public NodeSplitResult(long[] newNode, boolean allLeftHashMapsEqual, boolean allRightHashMapsEqual) {
      this.newNode = newNode;
      this.allLeftHashMapsEqual = allLeftHashMapsEqual;
      this.allRightHashMapsEqual = allRightHashMapsEqual;
    }
  }

  @SuppressFBWarnings(value = "SE_COMPARATOR_SHOULD_BE_SERIALIZABLE")
  final class KeyHashCodeComparator<K> implements Comparator<K> {
    private final Comparator<? super K> comparator = ODefaultComparator.INSTANCE;

    private final OHashFunction<K> keyHashFunction;

    public KeyHashCodeComparator(OHashFunction<K> keyHashFunction) {
      this.keyHashFunction = keyHashFunction;
    }

    @Override
    public int compare(K keyOne, K keyTwo) {
      final long hashCodeOne = keyHashFunction.hashCode(keyOne);
      final long hashCodeTwo = keyHashFunction.hashCode(keyTwo);

      if (greaterThanUnsigned(hashCodeOne, hashCodeTwo))
        return 1;
      if (lessThanUnsigned(hashCodeOne, hashCodeTwo))
        return -1;

      return comparator.compare(keyOne, keyTwo);
    }

    private static boolean lessThanUnsigned(long longOne, long longTwo) {
      return (longOne + Long.MIN_VALUE) < (longTwo + Long.MIN_VALUE);
    }

    private static boolean greaterThanUnsigned(long longOne, long longTwo) {
      return (longOne + Long.MIN_VALUE) > (longTwo + Long.MIN_VALUE);
    }
  }
}
