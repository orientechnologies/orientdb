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

import java.util.ArrayList;
import java.util.List;

import com.orientechnologies.common.directmemory.ODirectMemory;
import com.orientechnologies.common.hash.OMurmurHash3;
import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;

/**
 * Implementation of direct memory hash map, but unlike hash map it can contain {@link Long#MAX_VALUE} items and is optimized for
 * CPU cache locality and {@link #get} {@link #put} operations.
 * 
 * It is not array of lists of key/value pairs it is array of list of arrays of "key hash value"/value pairs. Which should provide
 * better CPU cache locality. Also hash codes are calculated not on values itself but on serialized presentation of keys. To avoid
 * deserialization overhead serialized key values are compared at first and if they will be equal deserialized values will be
 * returned.
 * 
 * {@link #put} operation returns true is there is enough memory to allocate for new data and false otherwise.
 * 
 * @author Andrey Lomakin
 * @since 13.08.12
 */
public class ODirectMemoryHashMap<K, V> {
  private static final int           SEED             = 362498820;

  private static int                 BUCKET_SIZE      = 8;
  private static final int           INITIAL_CAPACITY = 1024;

  private final int                  hashLineSize;

  private final ODirectMemory        memory;
  private final OBinarySerializer<V> valueSerializer;
  private final OBinarySerializer<K> keySerializer;

  private final int                  bucketSize;
  private final int                  hashLinePointerOffset;

  private long                       size;
  private long                       capacity;
  private long                       nextThreshold;

  private int                        entries;

  public ODirectMemoryHashMap(ODirectMemory memory, OBinarySerializer<V> valueSerializer, OBinarySerializer<K> keySerializer) {
    this(memory, valueSerializer, keySerializer, INITIAL_CAPACITY, BUCKET_SIZE);
  }

  public ODirectMemoryHashMap(ODirectMemory memory, OBinarySerializer<V> valueSerializer, OBinarySerializer<K> keySerializer,
      long initialCapacity, int bucketSize) {
    this.memory = memory;
    this.valueSerializer = valueSerializer;
    this.keySerializer = keySerializer;

    size = 0;

    this.bucketSize = bucketSize;
    this.hashLinePointerOffset = OIntegerSerializer.INT_SIZE + bucketSize
        * (OLongSerializer.LONG_SIZE + 2 * OIntegerSerializer.INT_SIZE);
    this.hashLineSize = bucketSize * (OLongSerializer.LONG_SIZE + 2 * OIntegerSerializer.INT_SIZE) + 2
        * OIntegerSerializer.INT_SIZE;

    capacity = initialCapacity;
    nextThreshold = (long) (capacity * 0.75);

    if (!allocateInitialMemory(capacity))
      throw new IllegalStateException("There is n enough memory to allocate");
  }

  private boolean allocateInitialMemory(long capacity) {
    final long hashAllocationSize = capacity * hashLineSize;

    final int entriesPtr = memory.allocate((int) hashAllocationSize);
    if (entriesPtr == ODirectMemory.NULL_POINTER)
      return false;

    for (long i = 0; i < capacity; i++) {
      memory.setInt(entriesPtr, (int) i * hashLineSize, 0);
      memory.setInt(entriesPtr, (int) (i * hashLineSize + hashLinePointerOffset), ODirectMemory.NULL_POINTER);
    }

    entries = entriesPtr;

    return true;
  }

  public V get(K key) {
    final byte[] serializedKey = new byte[keySerializer.getObjectSize(key)];
    keySerializer.serialize(key, serializedKey, 0);

    final long hashCode = OMurmurHash3.murmurHash3_x64_64(serializedKey, SEED);
    final long index = index(hashCode);

    int hashLineSize = getHashLineSize(entries, index);

    mainHashLoop: for (int i = 0; i < hashLineSize; i++) {
      final long currentHash = getHashCode(entries, index, i);

      if (currentHash == hashCode) {
        final byte[] currentKey = getKey(entries, index, i);
        if (currentKey.length != serializedKey.length)
          continue;

        for (int j = 0; j < currentKey.length; j++)
          if (serializedKey[j] != currentKey[j])
            continue mainHashLoop;
        return getValue(entries, index, i);
      }
    }

    int hashLinePtr = getNextHashLinePtr(entries, index);
    while (hashLinePtr != ODirectMemory.NULL_POINTER) {
      hashLineSize = getHashLineSizeFromHashLine(hashLinePtr);

      hashLineLoop: for (int i = 0; i < hashLineSize; i++) {
        final long currentHash = getHashCodeFromHashLine(hashLinePtr, i);
        if (currentHash == hashCode) {
          final byte[] currentKey = getKeyFromHashLine(hashLinePtr, i);
          if (currentKey.length != serializedKey.length)
            continue;

          for (int j = 0; j < currentKey.length; j++)
            if (serializedKey[j] != currentKey[j])
              continue hashLineLoop;
          return getValueFromHashLine(hashLinePtr, i);
        }
      }

      hashLinePtr = getNextHashLinePtrFromHashLine(hashLinePtr);
    }

    return null;
  }

  public boolean put(K key, V value) {
    final byte[] serializedKey = new byte[keySerializer.getObjectSize(key)];
    keySerializer.serialize(key, serializedKey, 0);

    final long hashCode = OMurmurHash3.murmurHash3_x64_64(serializedKey, SEED);
    final long index = index(hashCode);

    int hashLineSize = getHashLineSize(entries, index);

    int prevHashLine = ODirectMemory.NULL_POINTER;

    mainHashLoop: for (int i = 0; i < hashLineSize; i++) {
      final long currentHash = getHashCode(entries, index, i);

      if (currentHash == hashCode) {
        final byte[] currentKey = getKey(entries, index, i);
        if (currentKey.length != serializedKey.length)
          continue;

        for (int j = 0; j < currentKey.length; j++)
          if (serializedKey[j] != currentKey[j])
            continue mainHashLoop;

        final int valuePtr = memory.allocate(valueSerializer.getObjectSize(value));
        if (valuePtr == ODirectMemory.NULL_POINTER)
          return false;

        memory.set(valuePtr, 0, value, valueSerializer);

        return replaceValue(entries, index, i, valuePtr);
      }
    }

    int hashLinePtr = getNextHashLinePtr(entries, index);
    while (hashLinePtr != ODirectMemory.NULL_POINTER) {
      hashLineSize = getHashLineSizeFromHashLine(hashLinePtr);

      hashLineLoop: for (int i = 0; i < hashLineSize; i++) {
        final long currentHash = getHashCodeFromHashLine(hashLinePtr, i);
        if (currentHash == hashCode) {
          final byte[] currentKey = getKeyFromHashLine(hashLinePtr, i);
          if (currentKey.length != serializedKey.length)
            continue;

          for (int j = 0; j < currentKey.length; j++)
            if (serializedKey[j] != currentKey[j])
              continue hashLineLoop;

          final int valuePtr = memory.allocate(valueSerializer.getObjectSize(value));
          if (valuePtr == ODirectMemory.NULL_POINTER)
            return false;

          memory.set(valuePtr, 0, value, valueSerializer);

          return replaceValueInHashLine(hashLinePtr, i, valuePtr);
        }
      }

      prevHashLine = hashLinePtr;
      hashLinePtr = getNextHashLinePtrFromHashLine(hashLinePtr);
    }

    final int keyPtr = memory.allocate(OIntegerSerializer.INT_SIZE + serializedKey.length);
    if (keyPtr == ODirectMemory.NULL_POINTER)
      return false;

    final int valuePtr = memory.allocate(valueSerializer.getObjectSize(value));
    if (valuePtr == ODirectMemory.NULL_POINTER) {
      memory.free(keyPtr);

      return false;
    }

    memory.setInt(keyPtr, 0, serializedKey.length);
    memory.set(keyPtr, OIntegerSerializer.INT_SIZE, serializedKey.length, serializedKey);

    memory.set(valuePtr, 0, value, valueSerializer);

    if (!appendEntry(hashCode, keyPtr, valuePtr, index, prevHashLine))
      return false;

    size++;

    if (size >= nextThreshold)
      rehash();

    return true;
  }

  private void rehash() {
    final long oldCapacity = capacity;
    final int oldEntries = entries;

    capacity = capacity << 1;
    if (!allocateInitialMemory(capacity)) {
      capacity = oldCapacity;
      entries = oldEntries;
    }

    for (long oldIndex = 0; oldIndex < oldCapacity; oldIndex++) {
      int oldHashLineSize = getHashLineSize(oldEntries, oldIndex);
      for (int oldOffset = 0; oldOffset < oldHashLineSize; oldOffset++) {
        final int oldKeyPtr = getKeyPtr(oldEntries, oldIndex, oldOffset);
        final int oldValuePtr = getValuePtr(oldEntries, oldIndex, oldOffset);
        final long oldHashCode = getHashCode(oldEntries, oldIndex, oldOffset);

        if (!rehashEntry(oldHashCode, oldKeyPtr, oldValuePtr)) {
          clearLines(entries, capacity);

          entries = oldEntries;
          capacity = oldCapacity;
        }

      }

      int oldHashLinePtr = getNextHashLinePtr(oldEntries, oldIndex);
      while (oldHashLinePtr != ODirectMemory.NULL_POINTER) {
        oldHashLineSize = getHashLineSizeFromHashLine(oldHashLinePtr);

        for (int oldOffset = 0; oldOffset < oldHashLineSize; oldOffset++) {
          final int oldKeyPtr = getKeyPtrFromHashLine(oldHashLinePtr, oldOffset);
          final long oldHashCode = getHashCodeFromHashLine(oldHashLinePtr, oldOffset);
          final int oldValuePtr = getValuePtrFromHashLine(oldHashLinePtr, oldOffset);

          if (!rehashEntry(oldHashCode, oldKeyPtr, oldValuePtr)) {
            clearLines(entries, capacity);

            entries = oldEntries;
            capacity = oldCapacity;
          }
        }

        oldHashLinePtr = getNextHashLinePtrFromHashLine(oldHashLinePtr);
      }
    }

    nextThreshold = (long) (capacity * 0.75);
    clearLines(oldEntries, oldCapacity);
  }

  public long size() {
    return size;
  }

  public void clear() {
    for (long index = 0; index < capacity; index++) {
      int hashLineSize = getHashLineSize(entries, index);
      for (int offset = 0; offset < hashLineSize; offset++) {
        final int keyPtr = getKeyPtr(entries, index, offset);
        final int valuePtr = getValuePtr(entries, index, offset);

        memory.free(keyPtr);
        memory.free(valuePtr);
      }

      int hashLinePtr = getNextHashLinePtr(entries, index);
      while (hashLinePtr != ODirectMemory.NULL_POINTER) {
        hashLineSize = getHashLineSizeFromHashLine(hashLinePtr);

        for (int oldOffset = 0; oldOffset < hashLineSize; oldOffset++) {
          final int keyPtr = getKeyPtrFromHashLine(hashLinePtr, oldOffset);
          final int valuePtr = getValuePtrFromHashLine(hashLinePtr, oldOffset);

          memory.free(keyPtr);
          memory.free(valuePtr);
        }

        hashLinePtr = getNextHashLinePtrFromHashLine(hashLinePtr);
      }
    }

    clearLines(entries, capacity);

    size = 0;
  }

  @Override
  protected void finalize() throws Throwable {
    super.finalize();
    clear();
  }

  private void clearLines(int ptr, long len) {
    final List<Integer> hashLinePointers = new ArrayList<Integer>();

    for (long i = 0; i < len; i++) {
      int hashLinePtr = getNextHashLinePtr(ptr, i);
      while (hashLinePtr != ODirectMemory.NULL_POINTER) {
        hashLinePointers.add(hashLinePtr);
        hashLinePtr = getNextHashLinePtrFromHashLine(hashLinePtr);
      }
    }

    for (int hashLinePtr : hashLinePointers)
      memory.free(hashLinePtr);

    memory.free(ptr);
  }

  private boolean rehashEntry(final long hashCode, final int keyPtr, int valuePtr) {
    final long index = index(hashCode);

    int hashLinePtr = getNextHashLinePtr(entries, index);
    int prevHashLinePtr = ODirectMemory.NULL_POINTER;

    while (hashLinePtr != ODirectMemory.NULL_POINTER) {
      prevHashLinePtr = hashLinePtr;
      hashLinePtr = getNextHashLinePtrFromHashLine(hashLinePtr);
    }

    return appendEntry(hashCode, keyPtr, valuePtr, index, prevHashLinePtr);
  }

  private boolean appendEntry(long hashCode, int keyPtr, int valuePtr, long index, int currentHashLinePtr) {
    if (currentHashLinePtr == ODirectMemory.NULL_POINTER) {
      int hashLineSize = getHashLineSize(entries, index);

      if (hashLineSize < bucketSize)
        addEntry(entries, index, hashLineSize, hashCode, keyPtr, valuePtr);
      else {
        int hashLinePtr = addHashLine(entries, index);

        if (hashLinePtr == ODirectMemory.NULL_POINTER)
          return false;

        addEntryInHashLine(hashLinePtr, 0, hashCode, keyPtr, valuePtr);
      }
    } else {
      int hashLineSize = getHashLineSizeFromHashLine(currentHashLinePtr);

      if (hashLineSize < bucketSize)
        addEntryInHashLine(currentHashLinePtr, hashLineSize, hashCode, keyPtr, valuePtr);
      else {
        int hashLinePtr = addHashLineInHashLine(currentHashLinePtr);
        if (hashLinePtr == ODirectMemory.NULL_POINTER)
          return false;

        addEntryInHashLine(hashLinePtr, 0, hashCode, keyPtr, valuePtr);
      }
    }

    return true;
  }

  private long index(long hashCode) {
    return hashCode & (capacity - 1);
  }

  private int getHashLineSize(int ptr, long index) {
    final long memoryOffset = index * hashLineSize;
    return memory.getInt(ptr, (int) memoryOffset);
  }

  private int getHashLineSizeFromHashLine(int ptr) {
    return memory.getInt(ptr, 0);
  }

  private long getHashCode(int ptr, long index, int offset) {
    final long memoryOffset = index * hashLineSize + OIntegerSerializer.INT_SIZE + offset * OLongSerializer.LONG_SIZE;
    return memory.getLong(ptr, (int) memoryOffset);
  }

  private long getHashCodeFromHashLine(int hashLinePtr, long offset) {
    final long memoryOffset = OIntegerSerializer.INT_SIZE + offset * OLongSerializer.LONG_SIZE;
    return memory.getLong(hashLinePtr, (int) memoryOffset);
  }

  private int getNextHashLinePtr(int ptr, long index) {
    final long memoryOffset = index * hashLineSize + hashLinePointerOffset;

    return memory.getInt(ptr, (int) memoryOffset);
  }

  private int getNextHashLinePtrFromHashLine(int hashLinePtr) {
    final long memoryOffset = hashLinePointerOffset;
    return memory.getInt(hashLinePtr, (int) memoryOffset);
  }

  private int addHashLine(int ptr, long index) {
    final int hashLinePtr = memory.allocate(hashLineSize);

    if (hashLinePtr == ODirectMemory.NULL_POINTER)
      return hashLinePtr;

    memory.setInt(hashLinePtr, 0, 0);
    memory.setInt(hashLinePtr, hashLinePointerOffset, ODirectMemory.NULL_POINTER);

    memory.setInt(ptr, (int) (index * hashLineSize + hashLinePointerOffset), hashLinePtr);

    return hashLinePtr;
  }

  private int addHashLineInHashLine(int ptr) {
    final int hashLinePtr = memory.allocate(hashLineSize);

    if (hashLinePtr == ODirectMemory.NULL_POINTER)
      return hashLinePtr;

    memory.setInt(hashLinePtr, 0, 0);
    memory.setInt(hashLinePtr, hashLinePointerOffset, ODirectMemory.NULL_POINTER);

    memory.setInt(ptr, hashLinePointerOffset, hashLinePtr);

    return hashLinePtr;
  }

  private byte[] getKey(int ptr, long index, int offset) {
    final int keyPtr = getKeyPtr(ptr, index, offset);
    final int keyLength = memory.getInt(keyPtr, 0);

    return memory.get(keyPtr, OIntegerSerializer.INT_SIZE, keyLength);
  }

  private int getKeyPtr(int ptr, long index, int offset) {
    final long memoryOffset = index * hashLineSize + OIntegerSerializer.INT_SIZE + OLongSerializer.LONG_SIZE * bucketSize + 2
        * offset * OIntegerSerializer.INT_SIZE;

    return memory.getInt(ptr, (int) memoryOffset);
  }

  private byte[] getKeyFromHashLine(int ptr, int offset) {
    final int keyPtr = getKeyPtrFromHashLine(ptr, offset);
    final int keyLength = memory.getInt(keyPtr, 0);

    return memory.get(keyPtr, OIntegerSerializer.INT_SIZE, keyLength);
  }

  private int getKeyPtrFromHashLine(int ptr, int offset) {
    final long memoryOffset = OIntegerSerializer.INT_SIZE + OLongSerializer.LONG_SIZE * bucketSize + 2 * offset
        * OIntegerSerializer.INT_SIZE;

    return memory.getInt(ptr, (int) memoryOffset);
  }

  private V getValue(int ptr, long index, int offset) {
    final int valuePtr = getValuePtr(ptr, index, offset);

    return memory.get(valuePtr, 0, valueSerializer);
  }

  private int getValuePtr(int ptr, long index, int offset) {
    final long memoryOffset = index * hashLineSize + OIntegerSerializer.INT_SIZE + OLongSerializer.LONG_SIZE * bucketSize + 2
        * offset * OIntegerSerializer.INT_SIZE + OIntegerSerializer.INT_SIZE;

    return memory.getInt(ptr, (int) memoryOffset);
  }

  private V getValueFromHashLine(int ptr, int offset) {
    final int valuePtr = getValuePtrFromHashLine(ptr, offset);

    return memory.get(valuePtr, 0, valueSerializer);
  }

  private int getValuePtrFromHashLine(int ptr, int offset) {
    final long memoryOffset = OIntegerSerializer.INT_SIZE + OLongSerializer.LONG_SIZE * bucketSize + 2 * offset
        * OIntegerSerializer.INT_SIZE + OIntegerSerializer.INT_SIZE;

    return memory.getInt(ptr, (int) memoryOffset);
  }

  private boolean replaceValue(int ptr, long index, int offset, int valuePtr) {
    final long memoryOffset = index * hashLineSize + OIntegerSerializer.INT_SIZE + OLongSerializer.LONG_SIZE * bucketSize + 2
        * offset * OIntegerSerializer.INT_SIZE + OIntegerSerializer.INT_SIZE;

    final int oldValuePtr = memory.getInt(ptr, (int) memoryOffset);
    memory.free(oldValuePtr);

    memory.setInt(ptr, (int) memoryOffset, valuePtr);

    return true;
  }

  private boolean replaceValueInHashLine(int ptr, int offset, int valuePtr) {
    final long memoryOffset = OIntegerSerializer.INT_SIZE + OLongSerializer.LONG_SIZE * bucketSize + 2 * offset
        * OIntegerSerializer.INT_SIZE + OIntegerSerializer.INT_SIZE;

    final int oldValuePtr = memory.getInt(ptr, (int) memoryOffset);
    memory.free(oldValuePtr);

    memory.setInt(ptr, (int) memoryOffset, valuePtr);

    return true;
  }

  private void addEntry(int ptr, long index, int offset, long hashCode, int keyPtr, int valuePtr) {
    final long hashCodeMemoryOffset = index * hashLineSize + OIntegerSerializer.INT_SIZE + OLongSerializer.LONG_SIZE * offset;

    final long keyMemoryOffset = index * hashLineSize + OIntegerSerializer.INT_SIZE + OLongSerializer.LONG_SIZE * bucketSize
        + offset * 2 * OIntegerSerializer.INT_SIZE;
    final long valueMemoryOffset = keyMemoryOffset + OIntegerSerializer.INT_SIZE;

    memory.setLong(ptr, (int) hashCodeMemoryOffset, hashCode);
    memory.setInt(ptr, (int) keyMemoryOffset, keyPtr);
    memory.setInt(ptr, (int) valueMemoryOffset, valuePtr);
    memory.setInt(ptr, (int) index * hashLineSize, offset + 1);
  }

  private void addEntryInHashLine(int hashLinePtr, int offset, long hashCode, int keyPtr, int valuePtr) {
    final long hashCodeMemoryOffset = OIntegerSerializer.INT_SIZE + OLongSerializer.LONG_SIZE * offset;

    final long keyMemoryOffset = OIntegerSerializer.INT_SIZE + OLongSerializer.LONG_SIZE * bucketSize + offset * 2
        * OIntegerSerializer.INT_SIZE;
    final long valueMemoryOffset = keyMemoryOffset + OIntegerSerializer.INT_SIZE;

    memory.setLong(hashLinePtr, (int) hashCodeMemoryOffset, hashCode);
    memory.setInt(hashLinePtr, (int) keyMemoryOffset, keyPtr);
    memory.setInt(hashLinePtr, (int) valueMemoryOffset, valuePtr);

    memory.setInt(hashLinePtr, 0, offset + 1);
  }
}
