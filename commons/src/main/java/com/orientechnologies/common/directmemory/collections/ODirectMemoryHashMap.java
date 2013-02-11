/*
 * Copyright 1999-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
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

package com.orientechnologies.common.directmemory.collections;

import com.orientechnologies.common.directmemory.ODirectMemory;
import com.orientechnologies.common.hash.OMurmurHash3;
import com.orientechnologies.common.serialization.types.OBinarySerializer;

/**
 * Implementation of direct memory hash map optimized for CPU cache locality and {@link #get} {@link #put} operations.
 * 
 * It is not array of lists of key/value pairs it is array of list of arrays of "key hash value"/value pairs. Which should provide
 * better CPU cache locality. Also hash codes are calculated not on values itself but on serialized presentation of keys. To avoid
 * deserialization overhead serialized key values are compared at first and if they will be equal deserialized values will be
 * returned.
 * 
 * 
 * @author Andrey Lomakin
 * @since 13.08.12
 */
public class ODirectMemoryHashMap<K, V> {
  private static final int                   SEED             = 362498820;

  private static final int                   BUCKET_SIZE      = 8;
  private static final int                   INITIAL_CAPACITY = 1024;

  private final ODirectMemory                memory;
  private final OBinarySerializer<V>         valueSerializer;
  private final OBinarySerializer<K>         keySerializer;

  private final int                          bucketSize;

  private int                                size;
  private int                                capacity;
  private int                                nextThreshold;

  private ODirectMemoryHashMapBucket<K, V>[] entries;

  public ODirectMemoryHashMap(ODirectMemory memory, OBinarySerializer<V> valueSerializer, OBinarySerializer<K> keySerializer) {
    this(memory, valueSerializer, keySerializer, INITIAL_CAPACITY, BUCKET_SIZE);
  }

  public ODirectMemoryHashMap(ODirectMemory memory, OBinarySerializer<V> valueSerializer, OBinarySerializer<K> keySerializer,
      int initialCapacity, int bucketSize) {
    this.memory = memory;
    this.valueSerializer = valueSerializer;
    this.keySerializer = keySerializer;

    size = 0;

    this.bucketSize = bucketSize;

    capacity = initialCapacity;
    nextThreshold = (int) (capacity * 0.75);

    entries = new ODirectMemoryHashMapBucket[capacity];
  }

  public V get(K key) {
    final byte[] serializedKey = new byte[keySerializer.getObjectSize(key)];
    keySerializer.serialize(key, serializedKey, 0);

    final long hashCode = OMurmurHash3.murmurHash3_x64_64(serializedKey, SEED);
    final long index = index(hashCode);

    final ODirectMemoryHashMapBucket<K, V> bucket = entries[(int) index];
    if (bucket == null)
      return null;

    return bucket.find(serializedKey, hashCode);
  }

  public V remove(K key) {
    final byte[] serializedKey = new byte[keySerializer.getObjectSize(key)];
    keySerializer.serialize(key, serializedKey, 0);

    final long hashCode = OMurmurHash3.murmurHash3_x64_64(serializedKey, SEED);
    final long index = index(hashCode);

    final ODirectMemoryHashMapBucket<K, V> bucket = entries[(int) index];
    if (bucket == null)
      return null;

    final V removedValue = bucket.remove(serializedKey, hashCode);
    if (removedValue != null)
      size--;

    return removedValue;
  }

  public boolean put(K key, V value) {
    final byte[] serializedKey = new byte[keySerializer.getObjectSize(key)];
    keySerializer.serialize(key, serializedKey, 0);

    final long hashCode = OMurmurHash3.murmurHash3_x64_64(serializedKey, SEED);
    final long index = index(hashCode);

    ODirectMemoryHashMapBucket<K, V> bucket = entries[(int) index];
    if (bucket == null) {
      bucket = new ODirectMemoryHashMapBucket<K, V>(memory, bucketSize, valueSerializer);
      entries[(int) index] = bucket;
    }

    if (entries[(int) index].put(serializedKey, hashCode, value))
      size++;

    if (size >= nextThreshold)
      rehash();

    return true;
  }

  private void rehash() {
    final ODirectMemoryHashMapBucket<K, V>[] oldEntries = entries;
    final int oldCapacity = capacity;

    try {
      capacity = capacity << 1;
      entries = new ODirectMemoryHashMapBucket[capacity];

      for (ODirectMemoryHashMapBucket<K, V> oldBucket : oldEntries) {
        if (oldBucket == null)
          continue;

        for (ODirectMemoryHashMapBucket.Entry oldEntry : oldBucket) {
          final long index = index(oldEntry.hashCode);

          ODirectMemoryHashMapBucket<K, V> bucket = entries[(int) index];
          if (bucket == null) {
            bucket = new ODirectMemoryHashMapBucket<K, V>(memory, bucketSize, valueSerializer);
            entries[(int) index] = bucket;
          }

          entries[(int) index].add(oldEntry.key, oldEntry.hashCode, oldEntry.value);
        }
      }

      for (ODirectMemoryHashMapBucket<K, V> oldBucket : oldEntries) {
        if (oldBucket == null)
          continue;

        oldBucket.clear();
      }

      nextThreshold = (int) (capacity * 0.75);
    } catch (OutOfMemoryError e) {
      for (ODirectMemoryHashMapBucket<K, V> bucket : entries)
        if (bucket != null)
          bucket.clear();

      entries = oldEntries;
      capacity = oldCapacity;

      throw e;
    }
  }

  public long size() {
    return size;
  }

  public void clear() {
    for (ODirectMemoryHashMapBucket<K, V> bucket : entries)
      if (bucket != null)
        bucket.clear();

    size = 0;
  }

  @Override
  protected void finalize() throws Throwable {
    super.finalize();
    clear();
  }

  private long index(long hashCode) {
    return hashCode & (capacity - 1);
  }
}
