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
package com.orientechnologies.orient.core.serialization;

import java.io.Serializable;

import com.orientechnologies.orient.core.exception.OSerializationException;

/**
 * Base interface of serialization.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public interface OSerializableStream extends Serializable {
  /**
   * Marshalls the object. Transforms the current object in byte[] form to being stored or transferred over the network.
   * 
   * @return The byte array representation of the object
   * @see #fromStream(byte[])
   * @throws OSerializationException
   *           if the marshalling does not succeed
   */
  byte[] toStream() throws OSerializationException;

  /**
   * Unmarshalls the object. Fills the current object with the values contained in the byte array representation restoring a
   * previous state. Usually byte[] comes from the storage or network.
   * 
   * @param iStream
   *          byte array representation of the object
   * @return The Object instance itself giving a "fluent interface". Useful to call multiple methods in chain.
   * @throws OSerializationException
   *           if the unmarshalling does not succeed
   */
  OSerializableStream fromStream(byte[] iStream) throws OSerializationException;
}
