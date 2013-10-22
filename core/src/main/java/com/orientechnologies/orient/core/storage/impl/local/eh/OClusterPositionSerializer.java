/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
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
package com.orientechnologies.orient.core.storage.impl.local.eh;

import com.orientechnologies.common.directmemory.ODirectMemoryPointer;
import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.orient.core.id.OClusterPosition;
import com.orientechnologies.orient.core.id.OClusterPositionFactory;

/**
 * @author Andrey Lomakin
 * @since 13.03.13
 */
public class OClusterPositionSerializer implements OBinarySerializer<OClusterPosition> {
  public static final OClusterPositionSerializer INSTANCE = new OClusterPositionSerializer();
  public static final byte                       ID       = 51;

  @Override
  public int getObjectSize(OClusterPosition object, Object... hints) {
    return OClusterPositionFactory.INSTANCE.getSerializedSize();
  }

  @Override
  public int getObjectSize(byte[] stream, int startPosition) {
    return OClusterPositionFactory.INSTANCE.getSerializedSize();
  }

  @Override
  public void serialize(OClusterPosition object, byte[] stream, int startPosition, Object... hints) {
    final byte[] serializedPosition = object.toStream();
    System.arraycopy(serializedPosition, 0, stream, startPosition, serializedPosition.length);
  }

  @Override
  public OClusterPosition deserialize(byte[] stream, int startPosition) {
    return OClusterPositionFactory.INSTANCE.fromStream(stream, startPosition);
  }

  @Override
  public byte getId() {
    return ID;
  }

  @Override
  public boolean isFixedLength() {
    return true;
  }

  @Override
  public int getFixedLength() {
    return OClusterPositionFactory.INSTANCE.getSerializedSize();
  }

  @Override
  public void serializeNative(OClusterPosition object, byte[] stream, int startPosition, Object... hints) {
    final byte[] serializedPosition = object.toStream();
    System.arraycopy(serializedPosition, 0, stream, startPosition, serializedPosition.length);
  }

  @Override
  public OClusterPosition deserializeNative(byte[] stream, int startPosition) {
    return OClusterPositionFactory.INSTANCE.fromStream(stream, startPosition);
  }

  @Override
  public int getObjectSizeNative(byte[] stream, int startPosition) {
    return OClusterPositionFactory.INSTANCE.getSerializedSize();
  }

  @Override
  public void serializeInDirectMemory(OClusterPosition object, ODirectMemoryPointer pointer, long offset, Object... hints) {
    final byte[] serializedPosition = object.toStream();
    pointer.set(offset, serializedPosition, 0, serializedPosition.length);
  }

  @Override
  public OClusterPosition deserializeFromDirectMemory(ODirectMemoryPointer pointer, long offset) {
    final byte[] serializedPosition = pointer.get(offset, OClusterPositionFactory.INSTANCE.getSerializedSize());
    return OClusterPositionFactory.INSTANCE.fromStream(serializedPosition);
  }

  @Override
  public int getObjectSizeInDirectMemory(ODirectMemoryPointer pointer, long offset) {
    return OClusterPositionFactory.INSTANCE.getSerializedSize();
  }

  @Override
  public OClusterPosition prepocess(OClusterPosition value, Object... hints) {
    return value;
  }
}
