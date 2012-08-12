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

package com.orientechnologies.common.directmemory;

import com.orientechnologies.common.types.OBinarySerializer;

/**
 * Abstraction of different kind of implementations of non-GC memory, memory that managed not by GC but directly by application.
 * Access to such memory is slower than to "native" Java memory but we get performance gain eliminating GC overhead. The main
 * application of such memory different types of caches.
 * 
 * @author Artem Orobets, Andrey Lomakin
 */
public interface ODirectMemory {
  /**
   * Presentation of null pointer in given memory model.
   */
  public int NULL_POINTER = -1;

  /**
   * Allocates amount of memory that is needed to write passed in byte array and writes it.
   * 
   * @param bytes
   *          Data that is needed to be written.
   * @return Pointer to the allocated piece of memory.
   */
  int allocate(byte[] bytes);

  /**
   * Allocates given amount of memory (in bytes) from pool and returns pointer on allocated memory or {@link #NULL_POINTER} if there
   * is no enough memory in pool.
   * 
   * @param size
   *          Size that is needed to be allocated.
   * @return Pointer to the allocated memory.
   */
  int allocate(int size);

  /**
   * Returns allocated memory back to the pool.
   * 
   * @param pointer
   *          Pointer to the allocated piece of memory.
   */
  void free(int pointer);

  /**
   * Calculates actual size that has been allocated for this entry.
   * 
   * @param pointer
   *          to allocated entry
   * @return actual size of this entry in memory
   */
  int getActualSpace(int pointer);

  /**
   * Reads raw data from given piece of memory.
   * 
   * @param pointer
   *          Memory pointer, returned by {@link #allocate(int)} method.
   * @param offset
   *          Memory offset.
   * @param length
   *          Size of data which should be returned.
   * @return Raw data from given piece of memory.
   */
  byte[] get(int pointer, int offset, int length);

  /**
   * Writes data to the given piece of memory.
   * 
   * @param pointer
   *          Memory pointer, returned by {@link #allocate(int)} method.
   * @param offset
   *          Memory offset.
   * @param length
   *          Size of data which should be written.
   * @param content
   *          Raw data is going to be written.
   */
  void set(int pointer, int offset, int length, byte[] content);

  /**
   * Returns converted data from given piece of memory. This operation is much faster than {@link #get(int, int, int)}.
   * 
   * @param pointer
   *          Memory pointer, returned by {@link #allocate(int)} method.
   * @param offset
   *          Memory offset.
   * @param serializer
   *          Serializer which will be used to convert data from byte array.
   * @param <T>
   *          Data type.
   * @return Data instance.
   */
  <T> T get(int pointer, int offset, OBinarySerializer<T> serializer);

  /**
   * Write data to given piece of memory. This operation is much faster than {@link #set(int, int, int, byte[])}.
   * 
   * @param pointer
   *          Memory pointer, returned by {@link #allocate(int)} method.
   * @param offset
   *          Memory offset.
   * @param serializer
   *          Serializer which will be used to convert data to byte array.
   * @param <T>
   *          Data type.
   */
  <T> void set(int pointer, int offset, T data, OBinarySerializer<T> serializer);

  /**
   * Return <code>int</code> value from given piece of memory.
   * 
   * @param pointer
   *          Memory pointer, returned by {@link #allocate(int)} method.
   * @param offset
   *          Memory offset.
   * @return Int value.
   */
  int getInt(int pointer, int offset);

  /**
   * Write <code>int</code> value to given piece of memory.
   * 
   * @param pointer
   *          Memory pointer, returned by {@link #allocate(int)} method.
   * @param offset
   *          Memory offset.
   */
  void setInt(int pointer, int offset, int value);

  /**
   * Return <code>long</code> value from given piece of memory.
   * 
   * @param pointer
   *          Memory pointer, returned by {@link #allocate(int)} method.
   * @param offset
   *          Memory offset.
   * @return long value.
   */
  long getLong(int pointer, int offset);

  /**
   * Write <code>long</code> value to given piece of memory.
   * 
   * @param pointer
   *          Memory pointer, returned by {@link #allocate(int)} method.
   * @param offset
   *          Memory offset.
   */
  void setLong(int pointer, int offset, long value);

  /**
   * Return <code>byte</code> value from given piece of memory.
   * 
   * @param pointer
   *          Memory pointer, returned by {@link #allocate(int)} method.
   * @param offset
   *          Memory offset.
   * @return byte value.
   */
  byte getByte(int pointer, int offset);

  /**
   * Write <code>byte</code> value to given piece of memory.
   * 
   * @param pointer
   *          Memory pointer, returned by {@link #allocate(int)} method.
   * @param offset
   *          Memory offset.
   */
  void setByte(int pointer, int offset, byte value);

  /**
   * The amount whole direct memory (free and used). This method does not take into account amount of memory that will be needed to
   * perform system operations.
   * 
   * @return The amount whole direct memory (free and used).
   */
  int capacity();

  /**
   * The amount available direct memory, that is, how much memory can be potentially used by users. This method does not take into
   * account amount of memory that will be needed to perform system operations and can be used only for rough estimation of
   * available memory.
   * 
   * @return The amount available direct memory.
   */
  int freeSpace();

  /**
   * Removes all data from the memory.
   */
  void clear();

  /**
   * Performs copying of raw data in memory from one offset to another.
   * 
   * @param srcPointer
   *          Memory pointer, returned by {@link #allocate(int)} method, from which data will be copied.
   * @param fromOffset
   *          Offset in memory from which data will be copied.
   * @param destPointer
   *          Memory pointer to which data will be copied.
   * @param toOffset
   *          Offset in memory to which data will be copied.
   * @param len
   *          Data length.
   */
  void copyData(int srcPointer, int fromOffset, int destPointer, int toOffset, int len);
}
