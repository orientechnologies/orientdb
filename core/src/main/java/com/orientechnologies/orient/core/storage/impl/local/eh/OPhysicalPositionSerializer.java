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
package com.orientechnologies.orient.core.storage.impl.local.eh;

import com.orientechnologies.common.directmemory.ODirectMemoryPointer;
import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.storage.OPhysicalPosition;
import com.orientechnologies.orient.core.version.ORecordVersion;
import com.orientechnologies.orient.core.version.OVersionFactory;

/**
 * @author Andrey Lomakin
 * @since 13.03.13
 */
public class OPhysicalPositionSerializer implements OBinarySerializer<OPhysicalPosition> {
  public static final OPhysicalPositionSerializer INSTANCE = new OPhysicalPositionSerializer();
  public static final byte                        ID       = 50;

  @Override
  public int getObjectSize(OPhysicalPosition object, Object... hints) {
    return getFixedLength();
  }

  @Override
  public int getObjectSize(byte[] stream, int startPosition) {
    return getFixedLength();
  }

  @Override
  public void serialize(OPhysicalPosition object, byte[] stream, int startPosition, Object... hints) {
    int position = startPosition;
    OClusterPositionSerializer.INSTANCE.serialize(object.clusterPosition, stream, position);
    position += OClusterPositionSerializer.INSTANCE.getFixedLength();

    object.recordVersion.getSerializer().writeTo(stream, position, object.recordVersion);
    position += OVersionFactory.instance().getVersionSize();

    OIntegerSerializer.INSTANCE.serialize(object.dataSegmentId, stream, position);
    position += OIntegerSerializer.INT_SIZE;

    OIntegerSerializer.INSTANCE.serialize(object.recordSize, stream, position);
    position += OIntegerSerializer.INT_SIZE;

    OLongSerializer.INSTANCE.serialize(object.dataSegmentPos, stream, position);
    position += OLongSerializer.LONG_SIZE;

    OByteSerializer.INSTANCE.serialize(object.recordType, stream, position);
  }

  @Override
  public OPhysicalPosition deserialize(byte[] stream, int startPosition) {
    final OPhysicalPosition physicalPosition = new OPhysicalPosition();
    int position = startPosition;
    physicalPosition.clusterPosition = OClusterPositionSerializer.INSTANCE.deserialize(stream, position);
    position += OClusterPositionSerializer.INSTANCE.getFixedLength();

    final ORecordVersion version = OVersionFactory.instance().createVersion();
    version.getSerializer().readFrom(stream, position, version);
    position += OVersionFactory.instance().getVersionSize();

    physicalPosition.dataSegmentId = OIntegerSerializer.INSTANCE.deserialize(stream, position);
    position += OIntegerSerializer.INT_SIZE;

    physicalPosition.recordSize = OIntegerSerializer.INSTANCE.deserialize(stream, position);
    position += OIntegerSerializer.INT_SIZE;

    physicalPosition.dataSegmentPos = OLongSerializer.INSTANCE.deserialize(stream, position);
    position += OLongSerializer.LONG_SIZE;

    physicalPosition.recordType = OByteSerializer.INSTANCE.deserialize(stream, position);

    return physicalPosition;
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
    final int clusterPositionSize = OClusterPositionSerializer.INSTANCE.getFixedLength();
    final int versionSize = OVersionFactory.instance().getVersionSize();

    return clusterPositionSize + versionSize + 2 * OIntegerSerializer.INT_SIZE + OLongSerializer.LONG_SIZE
        + OByteSerializer.BYTE_SIZE;
  }

  @Override
  public void serializeNative(OPhysicalPosition object, byte[] stream, int startPosition, Object... hints) {
    int position = startPosition;
    OClusterPositionSerializer.INSTANCE.serializeNative(object.clusterPosition, stream, position);
    position += OClusterPositionSerializer.INSTANCE.getFixedLength();

    object.recordVersion.getSerializer().fastWriteTo(stream, position, object.recordVersion);
    position += OVersionFactory.instance().getVersionSize();

    OIntegerSerializer.INSTANCE.serializeNative(object.dataSegmentId, stream, position);
    position += OIntegerSerializer.INT_SIZE;

    OIntegerSerializer.INSTANCE.serializeNative(object.recordSize, stream, position);
    position += OIntegerSerializer.INT_SIZE;

    OLongSerializer.INSTANCE.serializeNative(object.dataSegmentPos, stream, position);
    position += OLongSerializer.LONG_SIZE;

    OByteSerializer.INSTANCE.serializeNative(object.recordType, stream, position);
  }

  @Override
  public OPhysicalPosition deserializeNative(byte[] stream, int startPosition) {
    final OPhysicalPosition physicalPosition = new OPhysicalPosition();
    int position = startPosition;
    physicalPosition.clusterPosition = OClusterPositionSerializer.INSTANCE.deserializeNative(stream, position);
    position += OClusterPositionSerializer.INSTANCE.getFixedLength();

    final ORecordVersion version = OVersionFactory.instance().createVersion();
    version.getSerializer().fastReadFrom(stream, position, version);
    position += OVersionFactory.instance().getVersionSize();

    physicalPosition.dataSegmentId = OIntegerSerializer.INSTANCE.deserializeNative(stream, position);
    position += OIntegerSerializer.INT_SIZE;

    physicalPosition.recordSize = OIntegerSerializer.INSTANCE.deserializeNative(stream, position);
    position += OIntegerSerializer.INT_SIZE;

    physicalPosition.dataSegmentPos = OLongSerializer.INSTANCE.deserializeNative(stream, position);
    position += OLongSerializer.LONG_SIZE;

    physicalPosition.recordType = OByteSerializer.INSTANCE.deserializeNative(stream, position);

    return physicalPosition;
  }

  @Override
  public int getObjectSizeNative(byte[] stream, int startPosition) {
    return getFixedLength();
  }

  @Override
  public void serializeInDirectMemory(OPhysicalPosition object, ODirectMemoryPointer pointer, long offset, Object... hints) {
    long currentOffset = offset;

    OClusterPositionSerializer.INSTANCE.serializeInDirectMemory(object.clusterPosition, pointer, currentOffset);
    currentOffset += OClusterPositionSerializer.INSTANCE.getFixedLength();

    byte[] serializedVersion = new byte[OVersionFactory.instance().getVersionSize()];
    object.recordVersion.getSerializer().fastWriteTo(serializedVersion, 0, object.recordVersion);
    pointer.set(currentOffset, serializedVersion, 0, serializedVersion.length);
    currentOffset += OVersionFactory.instance().getVersionSize();

    OIntegerSerializer.INSTANCE.serializeInDirectMemory(object.dataSegmentId, pointer, currentOffset);
    currentOffset += OIntegerSerializer.INT_SIZE;

    OIntegerSerializer.INSTANCE.serializeInDirectMemory(object.recordSize, pointer, currentOffset);
    currentOffset += OIntegerSerializer.INT_SIZE;

    OLongSerializer.INSTANCE.serializeInDirectMemory(object.dataSegmentPos, pointer, currentOffset);
    currentOffset += OLongSerializer.LONG_SIZE;

    OByteSerializer.INSTANCE.serializeInDirectMemory(object.recordType, pointer, currentOffset);
  }

  @Override
  public OPhysicalPosition deserializeFromDirectMemory(ODirectMemoryPointer pointer, long offset) {
    final OPhysicalPosition physicalPosition = new OPhysicalPosition();

    long currentPointer = offset;
    physicalPosition.clusterPosition = OClusterPositionSerializer.INSTANCE.deserializeFromDirectMemory(pointer, currentPointer);
    currentPointer += OClusterPositionSerializer.INSTANCE.getFixedLength();

    final ORecordVersion version = OVersionFactory.instance().createVersion();
    byte[] serializedVersion = pointer.get(currentPointer, OVersionFactory.instance().getVersionSize());
    version.getSerializer().fastReadFrom(serializedVersion, 0, version);
    physicalPosition.recordVersion = version;
    currentPointer += OVersionFactory.instance().getVersionSize();

    physicalPosition.dataSegmentId = OIntegerSerializer.INSTANCE.deserializeFromDirectMemory(pointer, currentPointer);
    currentPointer += OIntegerSerializer.INT_SIZE;

    OIntegerSerializer.INSTANCE.deserializeFromDirectMemory(pointer, currentPointer);
    currentPointer += OIntegerSerializer.INT_SIZE;

    physicalPosition.dataSegmentPos = OLongSerializer.INSTANCE.deserializeFromDirectMemory(pointer, currentPointer);
    currentPointer += OLongSerializer.LONG_SIZE;

    physicalPosition.recordType = OByteSerializer.INSTANCE.deserializeFromDirectMemory(pointer, currentPointer);

    return physicalPosition;
  }

  @Override
  public int getObjectSizeInDirectMemory(ODirectMemoryPointer pointer, long offset) {
    return getFixedLength();
  }

  @Override
  public OPhysicalPosition prepocess(OPhysicalPosition value, Object... hints) {
    return value;
  }
}
