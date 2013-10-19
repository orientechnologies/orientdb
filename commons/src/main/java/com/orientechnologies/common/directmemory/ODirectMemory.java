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
  public long NULL_POINTER = 0;

  /**
   * Allocates amount of memory that is needed to write passed in byte array and writes it.
   * 
   * @param bytes
   *          Data that is needed to be written.
   * @return Pointer to the allocated piece of memory.
   */
  long allocate(byte[] bytes);

  /**
   * Allocates given amount of memory (in bytes) from pool and returns pointer on allocated memory or {@link #NULL_POINTER} if there
   * is no enough memory in pool.
   * 
   * @param size
   *          Size that is needed to be allocated.
   * @return Pointer to the allocated memory.
   */
  long allocate(long size);

  /**
   * Returns allocated memory back to the pool.
   * 
   * @param pointer
   *          Pointer to the allocated piece of memory.
   */
  void free(long pointer);

  /**
   * Reads raw data from given piece of memory.
   * 
   * 
   * @param pointer
   *          Memory pointer, returned by {@link #allocate(long)} method.
   * @param length
   *          Size of data which should be returned.
   * @return Raw data from given piece of memory.
   */
  byte[] get(long pointer, int length);

  void get(long pointer, byte[] array, int arrayOffset, int length);

  /**
   * Writes data to the given piece of memory.
   * 
   * @param pointer
   *          Memory pointer, returned by {@link #allocate(long)} method.
   * @param content
   * @param arrayOffset
   * @param length
   */
  void set(long pointer, byte[] content, int arrayOffset, int length);

  /**
   * Return <code>int</code> value from given piece of memory.
   * 
   * 
   * @param pointer
   *          Memory pointer, returned by {@link #allocate(long)} method.
   * @return Int value.
   */
  int getInt(long pointer);

  /**
   * Write <code>int</code> value to given piece of memory.
   * 
   * @param pointer
   *          Memory pointer, returned by {@link #allocate(long)} method.
   * 
   */
  void setInt(long pointer, int value);

  void setShort(long pointer, short value);

  short getShort(long pointer);

  /**
   * Return <code>long</code> value from given piece of memory.
   * 
   * 
   * @param pointer
   *          Memory pointer, returned by {@link #allocate(long)} method.
   * @return long value.
   */
  long getLong(long pointer);

  /**
   * Write <code>long</code> value to given piece of memory.
   * 
   * @param pointer
   *          Memory pointer, returned by {@link #allocate(long)} method.
   * 
   */
  void setLong(long pointer, long value);

  /**
   * Return <code>byte</code> value from given piece of memory.
   * 
   * 
   * @param pointer
   *          Memory pointer, returned by {@link #allocate(long)} method.
   * @return byte value.
   */
  byte getByte(long pointer);

  /**
   * Write <code>byte</code> value to given piece of memory.
   * 
   * @param pointer
   *          Memory pointer, returned by {@link #allocate(long)} method.
   * 
   */
  void setByte(long pointer, byte value);

  void setChar(long pointer, char value);

  char getChar(long pointer);

  /**
   * Performs copying of raw data in memory from one position to another.
   * 
   * @param srcPointer
   *          Memory pointer, returned by {@link #allocate(long)} method, from which data will be copied.
   * @param destPointer
   *          Memory pointer to which data will be copied.
   * @param len
   *          Data length.
   */
  void copyData(long srcPointer, long destPointer, long len);
}
