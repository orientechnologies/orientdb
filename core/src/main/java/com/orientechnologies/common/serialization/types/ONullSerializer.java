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

package com.orientechnologies.common.serialization.types;

import com.orientechnologies.common.directmemory.ODirectMemoryPointer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWALChangesTree;

/**
 * Serializes and deserializes null values.
 *
 * @author Evgeniy Degtiarenko (gmandnepr-at-gmail.com)
 */
public class ONullSerializer implements OBinarySerializer<Object> {

  public static final byte      ID       = 11;
  public static ONullSerializer INSTANCE = new ONullSerializer();

  public int getObjectSize(final Object object, Object... hints) {
    return 0;
  }

  public void serialize(final Object object, final byte[] stream, final int startPosition, Object... hints) {
    // nothing to serialize
  }

  public Object deserialize(final byte[] stream, final int startPosition) {
    // nothing to deserialize
    return null;
  }

  public int getObjectSize(byte[] stream, int startPosition) {
    return 0;
  }

  public byte getId() {
    return ID;
  }

  public int getObjectSizeNative(byte[] stream, int startPosition) {
    return 0;
  }

  public void serializeNativeObject(Object object, byte[] stream, int startPosition, Object... hints) {
  }

  public Object deserializeNativeObject(byte[] stream, int startPosition) {
    return null;
  }

  @Override
  public void serializeInDirectMemoryObject(Object object, ODirectMemoryPointer pointer, long offset, Object... hints) {
  }

  @Override
  public Object deserializeFromDirectMemoryObject(ODirectMemoryPointer pointer, long offset) {
    return null;
  }

  @Override
  public Object deserializeFromDirectMemoryObject(OWALChangesTree.PointerWrapper wrapper, long offset) {
    return null;
  }

  @Override
  public int getObjectSizeInDirectMemory(ODirectMemoryPointer pointer, long offset) {
    return 0;
  }

  @Override
  public int getObjectSizeInDirectMemory(OWALChangesTree.PointerWrapper wrapper, long offset) {
    return 0;
  }

  public boolean isFixedLength() {
    return true;
  }

  public int getFixedLength() {
    return 0;
  }

  @Override
  public Object preprocess(Object value, Object... hints) {
    return null;
  }
}
