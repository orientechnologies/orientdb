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

import java.util.AbstractList;
import java.util.List;
import java.util.RandomAccess;

import com.orientechnologies.common.directmemory.ODirectMemory;
import com.orientechnologies.common.serialization.types.OBinarySerializer;

/**
 * Implementation of list which uses {@link ODirectMemory} to store data.
 * 
 * 
 * @author Andrey Lomakin
 * @since 12.08.12
 */
public class ODirectMemoryList<E> extends AbstractList<E> implements List<E>, RandomAccess {
  private final ODirectMemory        memory;
  private final OBinarySerializer<E> serializer;

  private int                        size;
  private long                       elementData;

  public ODirectMemoryList(int initialCapacity, ODirectMemory memory, OBinarySerializer<E> serializer) {
    super();

    if (initialCapacity < 0)
      throw new IllegalArgumentException("Illegal Capacity: " + initialCapacity);

    this.memory = memory;
    this.serializer = serializer;

    this.elementData = allocateSpace(initialCapacity);
  }

  public ODirectMemoryList(ODirectMemory memory, OBinarySerializer<E> serializer) {
    this(16, memory, serializer);
  }

  public int size() {
    return size;
  }

  public boolean isEmpty() {
    return size == 0;
  }

  public boolean contains(Object o) {
    return indexOf(o) >= 0;
  }

  public int indexOf(Object o) {
    if (o == null) {
      for (int i = 0; i < size; i++)
        if (getData(elementData, i) == null)
          return i;
    } else {
      for (int i = 0; i < size; i++)
        if (o.equals(getData(elementData, i)))
          return i;
    }
    return -1;
  }

  public int lastIndexOf(Object o) {
    if (o == null) {
      for (int i = size - 1; i >= 0; i--)
        if (getData(elementData, i) == null)
          return i;
    } else {
      for (int i = size - 1; i >= 0; i--)
        if (o.equals(getData(elementData, i)))
          return i;
    }

    return -1;
  }

  public E get(int index) {
    rangeCheck(index);

    return getData(elementData, index);
  }

  public E set(int index, E element) {
    rangeCheck(index);

    E oldValue = getData(elementData, index);
    setData(elementData, index, element);

    return oldValue;
  }

  public boolean add(E e) {
    ensureCapacity(size + 1);

    setData(elementData, size++, e);
    return true;
  }

  public E remove(int index) {
    if (size == 0)
      return null;

    rangeCheck(index);

    E oldValue = getData(elementData, index);

    doRemove(index);

    return oldValue;
  }

  private void doRemove(int index) {
    modCount++;

    setData(elementData, index, null);
    int numMoved = size - index - 1;
    if (numMoved > 0)
      copyData(elementData, index + 1, index, numMoved);

    size--;
    clearData(elementData, size);
  }

  public boolean remove(Object o) {
    if (size == 0)
      return false;

    if (o == null) {
      for (int index = 0; index < size; index++)
        if (getData(elementData, index) == null) {
          doRemove(index);

          return true;
        }
    } else {
      for (int index = 0; index < size; index++)
        if (o.equals(getData(elementData, index))) {
          doRemove(index);
          return true;
        }
    }
    return false;
  }

  public void clear() {
    modCount++;

    for (int i = 0; i < size; i++)
      setData(elementData, i, null);

    size = 0;
  }

  protected void removeRange(int fromIndex, int toIndex) {
    modCount++;
    int numMoved = size - toIndex;
    for (int i = fromIndex; i < toIndex; i++)
      setData(elementData, i, null);

    copyData(elementData, fromIndex, toIndex, numMoved);

    int newSize = size - (toIndex - fromIndex);
    while (size != newSize)
      clearData(elementData, --size);
  }

  private void ensureCapacity(int minCapacity) {
    modCount++;
    int oldCapacity = memory.getInt(elementData);
    if (minCapacity > oldCapacity) {
      long oldData = elementData;
      int newCapacity = (oldCapacity * 3) / 2 + 1;
      if (newCapacity < minCapacity)
        newCapacity = minCapacity;

      elementData = allocateSpace(newCapacity);

      copyData(oldData, 0, elementData, 0, oldCapacity);
    }
  }

  private void copyData(long ptr, int fromIndex, int toIndex, int len) {
    final long fromOffset = fromIndex * 8 + 4;
    final long toOffset = toIndex * 8 + 4;

    memory.copyData(ptr + fromOffset, ptr + toOffset, len * 8);
  }

  private void copyData(long fromPtr, int fromIndex, long toPtr, int toIndex, int len) {
    final long fromOffset = fromIndex * 8 + 4;
    final long toOffset = toIndex * 8 + 4;

    memory.copyData(fromPtr + fromOffset, toPtr + toOffset, len * 8);
  }

  private void rangeCheck(int index) {
    if (index >= size)
      throw new IndexOutOfBoundsException("Index: " + index + ", Size: " + size);
  }

  private E getData(long ptr, int index) {
    final int offset = index * 8 + 4;

    final long dataPtr = memory.getLong(ptr + offset);

    if (dataPtr == ODirectMemory.NULL_POINTER)
      return null;

    return memory.get(dataPtr, serializer);
  }

  private void setData(long ptr, int index, E data) {
    final long dataPtr;
    if (data != null) {
      dataPtr = memory.allocate(serializer.getObjectSize(data));
      if (dataPtr == ODirectMemory.NULL_POINTER)
        throw new IllegalStateException("There is no enough memory to allocate for item " + data);
      memory.set(dataPtr, data, serializer);
    } else
      dataPtr = ODirectMemory.NULL_POINTER;

    final long offset = index * 8 + 4;

    final long oldPtr = memory.getLong(ptr + offset);
    if (oldPtr != ODirectMemory.NULL_POINTER)
      memory.free(oldPtr);

    memory.setLong(ptr + offset, dataPtr);
  }

  private void clearData(long ptr, int index) {
    final long offset = index * 8 + 4;
    memory.setLong(ptr + offset, ODirectMemory.NULL_POINTER);
  }

  private long allocateSpace(int capacity) {
    final long size = capacity * 8 + 4;
    final long ptr = memory.allocate(size);
    if (ptr == ODirectMemory.NULL_POINTER)
      throw new IllegalStateException("There is no enough memory to allocate for capacity = " + capacity);

    long itemPtr = 4 + ptr;
    for (int i = 0; i < capacity; i++) {
      memory.setLong(itemPtr, ODirectMemory.NULL_POINTER);
      itemPtr += 8;
    }

    memory.setInt(ptr, capacity);

    return ptr;
  }

  @Override
  protected void finalize() throws Throwable {
    super.finalize();
    clear();
    memory.free(elementData);
  }
}
