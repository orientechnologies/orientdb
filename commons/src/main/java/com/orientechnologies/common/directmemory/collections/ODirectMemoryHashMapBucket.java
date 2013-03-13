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

import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

import com.orientechnologies.common.directmemory.ODirectMemory;
import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.serialization.types.OBinaryTypeSerializer;

/**
 * @author Andrey Lomakin
 * @since 2/10/13
 */
public class ODirectMemoryHashMapBucket<K, V> implements Iterable<ODirectMemoryHashMapBucket.Entry> {
  private final long[]                     hashCodes;
  private final long[]                     keyValuePairs;
  private ODirectMemoryHashMapBucket<K, V> nextBucket;

  private final ODirectMemory              directMemory;

  private final OBinarySerializer<V>       valueSerializer;
  private final int                        bucketSize;

  private int                              size;

  public ODirectMemoryHashMapBucket(ODirectMemory directMemory, int bucketSize, OBinarySerializer<V> valueSerializer) {
    this.directMemory = directMemory;
    this.valueSerializer = valueSerializer;
    this.bucketSize = bucketSize;

    hashCodes = new long[bucketSize];
    keyValuePairs = new long[bucketSize * 2];
  }

  public boolean put(byte[] serializedKey, long hashCode, V value) {
    FindResult<K, V> findResult = doFind(serializedKey, hashCode);

    if (findResult == null) {
      final int keySize = OBinaryTypeSerializer.INSTANCE.getObjectSize(serializedKey);
      final long keyPointer = directMemory.allocate(keySize);
      if (keyPointer == ODirectMemory.NULL_POINTER)
        throw new OutOfMemoryError("There is not enough memory to allocate");

      OBinaryTypeSerializer.INSTANCE.serializeInDirectMemory(serializedKey, directMemory, keyPointer);

      final int serializedValueSize = valueSerializer.getObjectSize(value);
      final long valuePointer = directMemory.allocate(serializedValueSize);
      if (valuePointer == ODirectMemory.NULL_POINTER) {
        directMemory.free(keyPointer);
        throw new OutOfMemoryError("There is not enough memory to allocate");
      }

      valueSerializer.serializeInDirectMemory(value, directMemory, valuePointer);

      if (size < bucketSize) {
        hashCodes[size] = hashCode;

        int index = size * 2;
        keyValuePairs[index++] = keyPointer;
        keyValuePairs[index] = valuePointer;

        size++;
      } else {
        if (nextBucket == null)
          nextBucket = new ODirectMemoryHashMapBucket<K, V>(directMemory, bucketSize, valueSerializer);

        nextBucket.add(keyPointer, hashCode, valuePointer);
      }

      return true;
    } else {
      final int serializedValueSize = valueSerializer.getObjectSize(value);
      final long newValuePointer = directMemory.allocate(serializedValueSize);
      if (newValuePointer == ODirectMemory.NULL_POINTER)
        throw new OutOfMemoryError("There is not enough memory to allocate");
      directMemory.set(newValuePointer, value, valueSerializer);

      final long[] foundKeyValuePairs = findResult.foundBucket.keyValuePairs;
      final int valueIndex = 2 * findResult.foundIndex + 1;

      final long oldValuePointer = foundKeyValuePairs[valueIndex];
      directMemory.free(oldValuePointer);

      foundKeyValuePairs[valueIndex] = newValuePointer;

      return false;
    }
  }

  private void add(long keyPointer, long hashCode, long valuePointer) {
    if (size < bucketSize) {
      hashCodes[size] = hashCode;

      int index = size * 2;
      keyValuePairs[index++] = keyPointer;
      keyValuePairs[index] = valuePointer;

      size++;
    } else {
      if (nextBucket == null)
        nextBucket = new ODirectMemoryHashMapBucket<K, V>(directMemory, bucketSize, valueSerializer);

      nextBucket.add(keyPointer, hashCode, valuePointer);
    }
  }

  public void add(byte[] serializedKey, long hashCode, byte[] serializedValue) {
    final int keySize = OBinaryTypeSerializer.INSTANCE.getObjectSize(serializedKey);
    final long keyPointer = directMemory.allocate(keySize);
    if (keyPointer == ODirectMemory.NULL_POINTER)
      throw new OutOfMemoryError("There is not enough memory to allocate");

    OBinaryTypeSerializer.INSTANCE.serializeInDirectMemory(serializedKey, directMemory, keyPointer);

    final long valuePointer = directMemory.allocate(serializedValue);
    if (valuePointer == ODirectMemory.NULL_POINTER) {
      directMemory.free(keyPointer);
      throw new OutOfMemoryError("There is not enough memory to allocate");
    }

    if (size < bucketSize) {
      hashCodes[size] = hashCode;

      int index = size * 2;
      keyValuePairs[index++] = keyPointer;
      keyValuePairs[index] = valuePointer;

      size++;
    } else {
      if (nextBucket == null)
        nextBucket = new ODirectMemoryHashMapBucket<K, V>(directMemory, bucketSize, valueSerializer);

      nextBucket.add(serializedKey, hashCode, serializedValue);
    }
  }

  public V find(byte[] serializedKey, long hashCode) {
    final FindResult<K, V> findResult = doFind(serializedKey, hashCode);
    if (findResult == null)
      return null;
    final long valuePointer = findResult.foundBucket.keyValuePairs[findResult.foundIndex * 2 + 1];
    return directMemory.get(valuePointer, valueSerializer);
  }

  private FindResult<K, V> doFind(byte[] serializedKey, long hashCode) {
    ODirectMemoryHashMapBucket<K, V> currentBucket = this;

    while (currentBucket != null) {
      for (int i = 0; i < currentBucket.size; i++) {
        if (currentBucket.hashCodes[i] == hashCode) {
          final long keyPointer = currentBucket.keyValuePairs[i * 2];
          final byte[] storedKey = directMemory.get(keyPointer, OBinaryTypeSerializer.INSTANCE);
          if (Arrays.equals(serializedKey, storedKey))
            return new FindResult<K, V>(currentBucket, i);
        }
      }

      currentBucket = currentBucket.nextBucket;
    }

    return null;
  }

  public V remove(byte[] serializedKey, long hashCode) {
    ODirectMemoryHashMapBucket<K, V> currentBucket = this;
    ODirectMemoryHashMapBucket<K, V> prevBucket = null;

    while (currentBucket != null) {
      for (int i = 0; i < currentBucket.size; i++) {
        if (currentBucket.hashCodes[i] == hashCode) {
          final long keyPointer = currentBucket.keyValuePairs[i * 2];
          final byte[] storedKey = directMemory.get(keyPointer, OBinaryTypeSerializer.INSTANCE);
          if (Arrays.equals(serializedKey, storedKey)) {
            final long valuePointer = currentBucket.keyValuePairs[i * 2 + 1];
            final V removedValue = directMemory.get(valuePointer, valueSerializer);

            directMemory.free(keyPointer);
            directMemory.free(valuePointer);

            System.arraycopy(currentBucket.hashCodes, i + 1, currentBucket.hashCodes, i, currentBucket.size - (i + 1));
            System.arraycopy(currentBucket.keyValuePairs, (i + 1) * 2, currentBucket.keyValuePairs, i * 2,
                2 * (currentBucket.size - (i + 1)));

            currentBucket.size--;

            if (currentBucket.size == 0 && prevBucket != null)
              prevBucket.nextBucket = currentBucket.nextBucket;

            return removedValue;
          }
        }
      }

      prevBucket = currentBucket;
      currentBucket = currentBucket.nextBucket;
    }

    return null;
  }

  public void clear() {
    ODirectMemoryHashMapBucket<K, V> currentBucket = this;

    while (currentBucket != null) {
      for (int i = 0; i < currentBucket.size; i++) {
        directMemory.free(currentBucket.keyValuePairs[i * 2]);
        directMemory.free(currentBucket.keyValuePairs[i * 2 + 1]);
      }

      currentBucket = currentBucket.nextBucket;
    }

    nextBucket = null;
    size = 0;
  }

  @Override
  public Iterator<Entry> iterator() {
    return new EntreeIterator();
  }

  public static final class Entry {
    public final long   hashCode;
    public final byte[] key;
    public final byte[] value;

    public Entry(long hashCode, byte[] key, byte[] value) {
      this.hashCode = hashCode;
      this.key = key;
      this.value = value;
    }
  }

  private class EntreeIterator implements Iterator<Entry> {
    private ODirectMemoryHashMapBucket<K, V> currentBucket = ODirectMemoryHashMapBucket.this;
    private int                              currentIndex  = 0;

    @Override
    public boolean hasNext() {
      return currentBucket != null && (currentBucket.nextBucket != null || currentIndex < currentBucket.size);
    }

    @Override
    public Entry next() {
      if (!hasNext())
        throw new NoSuchElementException();

      int index = currentIndex * 2;
      final long keyPointer = currentBucket.keyValuePairs[index++];
      final long valuePointer = currentBucket.keyValuePairs[index];
      final long hashCode = currentBucket.hashCodes[currentIndex];

      currentIndex++;
      if (currentIndex >= currentBucket.size) {
        currentBucket = currentBucket.nextBucket;
        currentIndex = 0;
      }

      return new Entry(hashCode, directMemory.get(keyPointer, OBinaryTypeSerializer.INSTANCE), directMemory.get(valuePointer,
          valueSerializer.getObjectSizeInDirectMemory(directMemory, valuePointer)));
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException("Remove operation is unsupported.");
    }
  }

  private static final class FindResult<K, V> {
    private final ODirectMemoryHashMapBucket<K, V> foundBucket;
    private final int                              foundIndex;

    private FindResult(ODirectMemoryHashMapBucket<K, V> foundBucket, int foundIndex) {
      this.foundBucket = foundBucket;
      this.foundIndex = foundIndex;
    }
  }
}
