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

package com.orientechnologies.common.serialization.types;

import com.orientechnologies.common.directmemory.ODirectMemoryPointer;

/**
 * This interface is used for serializing OrientDB datatypes in binary format. Serialized content is written into buffer that will
 * contain not only given object presentation but all binary content. Such approach prevents creation of separate byte array for
 * each object and decreased GC overhead.
 * 
 * @author <a href="mailto:gmandnepr@gmail.com">Evgeniy Degtiarenko</a>, Andrey Lomakin
 */
public interface OBinarySerializer<T> {

  /**
   * Obtain size of the serialized object Size is the amount of bites that required for storing object (for example: for storing
   * integer we need 4 bytes)
   * 
   * 
   * @param object
   *          is the object to measure its size
   * @param hints
   *          List of parameters which may be used to choose appropriate serialization approach.
   * @return size of the serialized object
   */
  int getObjectSize(T object, Object... hints);

  /**
   * Return size serialized presentation of given object.
   * 
   * @param stream
   *          Serialized content.
   * @param startPosition
   *          Position from which serialized presentation of given object is stored.
   * @return Size serialized presentation of given object in bytes.
   */
  int getObjectSize(byte[] stream, int startPosition);

  /**
   * Writes object to the stream starting from the startPosition
   * 
   * @param object
   *          is the object to serialize
   * @param stream
   *          is the stream where object will be written
   * @param startPosition
   * @param hints
   *          List of parameters which may be used to choose appropriate serialization approach.
   */
  void serialize(T object, byte[] stream, int startPosition, Object... hints);

  /**
   * Reads object from the stream starting from the startPosition
   * 
   * @param stream
   *          is the stream from object will be read
   * @param startPosition
   *          is the position to start reading from
   * @return instance of the deserialized object
   */
  T deserialize(byte[] stream, int startPosition);

  /**
   * @return Identifier of given serializer.
   */
  byte getId();

  /**
   * @return <code>true</code> if binary presentation of object always has the same length.
   */
  boolean isFixedLength();

  /**
   * @return Length of serialized data if {@link #isFixedLength()} method returns <code>true</code>. If {@link #isFixedLength()}
   *         method return <code>false</code> returned value is undefined.
   */
  int getFixedLength();

  /**
   * Writes object to the stream starting from the startPosition using native acceleration. Serialized object presentation is
   * platform dependant.
   * 
   * @param object
   *          is the object to serialize
   * @param stream
   *          is the stream where object will be written
   * @param startPosition
   * @param hints
   *          List of parameters which may be used to choose appropriate serialization approach.
   */
  void serializeNative(T object, byte[] stream, int startPosition, Object... hints);

  /**
   * Reads object from the stream starting from the startPosition, in case there were serialized using
   * {@link #serializeNative(T, byte[], int, Object...)} method.
   * 
   * @param stream
   *          is the stream from object will be read
   * @param startPosition
   *          is the position to start reading from
   * @return instance of the deserialized object
   */
  T deserializeNative(byte[] stream, int startPosition);

  /**
   * Return size serialized presentation of given object, if it was serialized using
   * {@link #serializeNative(T, byte[], int, Object...)} method.
   * 
   * @param stream
   *          Serialized content.
   * @param startPosition
   *          Position from which serialized presentation of given object is stored.
   * @return Size serialized presentation of given object in bytes.
   */
  int getObjectSizeNative(byte[] stream, int startPosition);

  void serializeInDirectMemory(T object, ODirectMemoryPointer pointer, long offset, Object... hints);

  T deserializeFromDirectMemory(ODirectMemoryPointer pointer, long offset);

  int getObjectSizeInDirectMemory(ODirectMemoryPointer pointer, long offset);

  T prepocess(T value, Object... hints);
}
