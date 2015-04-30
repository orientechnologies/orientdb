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
package com.orientechnologies.orient.core.storage.impl.local.eh;

import com.orientechnologies.common.directmemory.ODirectMemoryPointer;
import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.storage.OPhysicalPosition;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWALChangesTree;
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
    OLongSerializer.INSTANCE.serialize(object.clusterPosition, stream, position);
    position += OLongSerializer.INSTANCE.getFixedLength();

    object.recordVersion.getSerializer().writeTo(stream, position, object.recordVersion);
    position += OVersionFactory.instance().getVersionSize();

    OIntegerSerializer.INSTANCE.serializeLiteral(object.recordSize, stream, position);
    position += OIntegerSerializer.INT_SIZE;

    OByteSerializer.INSTANCE.serializeLiteral(object.recordType, stream, position);
  }

  @Override
  public OPhysicalPosition deserialize(byte[] stream, int startPosition) {
    final OPhysicalPosition physicalPosition = new OPhysicalPosition();
    int position = startPosition;
    physicalPosition.clusterPosition = OLongSerializer.INSTANCE.deserialize(stream, position);
    position += OLongSerializer.INSTANCE.getFixedLength();

    final ORecordVersion version = OVersionFactory.instance().createVersion();
    version.getSerializer().readFrom(stream, position, version);
    position += OVersionFactory.instance().getVersionSize();

    physicalPosition.recordSize = OIntegerSerializer.INSTANCE.deserializeLiteral(stream, position);
    position += OIntegerSerializer.INT_SIZE;

    physicalPosition.recordType = OByteSerializer.INSTANCE.deserializeLiteral(stream, position);

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
    final int clusterPositionSize = OLongSerializer.INSTANCE.getFixedLength();
    final int versionSize = OVersionFactory.instance().getVersionSize();

    return clusterPositionSize + versionSize + 2 * OIntegerSerializer.INT_SIZE + OLongSerializer.LONG_SIZE
        + OByteSerializer.BYTE_SIZE;
  }

  @Override
  public void serializeNativeObject(OPhysicalPosition object, byte[] stream, int startPosition, Object... hints) {
    int position = startPosition;
    OLongSerializer.INSTANCE.serializeNativeObject(object.clusterPosition, stream, position);
    position += OLongSerializer.INSTANCE.getFixedLength();

    object.recordVersion.getSerializer().fastWriteTo(stream, position, object.recordVersion);
    position += OVersionFactory.instance().getVersionSize();

    OIntegerSerializer.INSTANCE.serializeNative(object.recordSize, stream, position);
    position += OIntegerSerializer.INT_SIZE;

    OByteSerializer.INSTANCE.serializeNative(object.recordType, stream, position);
  }

  @Override
  public OPhysicalPosition deserializeNativeObject(byte[] stream, int startPosition) {
    final OPhysicalPosition physicalPosition = new OPhysicalPosition();
    int position = startPosition;
    physicalPosition.clusterPosition = OLongSerializer.INSTANCE.deserializeNativeObject(stream, position);
    position += OLongSerializer.INSTANCE.getFixedLength();

    final ORecordVersion version = OVersionFactory.instance().createVersion();
    version.getSerializer().fastReadFrom(stream, position, version);
    position += OVersionFactory.instance().getVersionSize();

    physicalPosition.recordSize = OIntegerSerializer.INSTANCE.deserializeNative(stream, position);
    position += OIntegerSerializer.INT_SIZE;

    physicalPosition.recordType = OByteSerializer.INSTANCE.deserializeNative(stream, position);

    return physicalPosition;
  }

  @Override
  public int getObjectSizeNative(byte[] stream, int startPosition) {
    return getFixedLength();
  }

  @Override
  public void serializeInDirectMemoryObject(OPhysicalPosition object, ODirectMemoryPointer pointer, long offset, Object... hints) {
    long currentOffset = offset;

    OLongSerializer.INSTANCE.serializeInDirectMemoryObject(object.clusterPosition, pointer, currentOffset);
    currentOffset += OLongSerializer.INSTANCE.getFixedLength();

    byte[] serializedVersion = new byte[OVersionFactory.instance().getVersionSize()];
    object.recordVersion.getSerializer().fastWriteTo(serializedVersion, 0, object.recordVersion);
    pointer.set(currentOffset, serializedVersion, 0, serializedVersion.length);
    currentOffset += OVersionFactory.instance().getVersionSize();

    OIntegerSerializer.INSTANCE.serializeInDirectMemory(object.recordSize, pointer, currentOffset);
    currentOffset += OIntegerSerializer.INT_SIZE;

    OByteSerializer.INSTANCE.serializeInDirectMemory(object.recordType, pointer, currentOffset);
  }

  @Override
  public OPhysicalPosition deserializeFromDirectMemoryObject(ODirectMemoryPointer pointer, long offset) {
    final OPhysicalPosition physicalPosition = new OPhysicalPosition();

    long currentPointer = offset;
    physicalPosition.clusterPosition = OLongSerializer.INSTANCE.deserializeFromDirectMemoryObject(pointer, currentPointer);
    currentPointer += OLongSerializer.INSTANCE.getFixedLength();

    final ORecordVersion version = OVersionFactory.instance().createVersion();
    byte[] serializedVersion = pointer.get(currentPointer, OVersionFactory.instance().getVersionSize());
    version.getSerializer().fastReadFrom(serializedVersion, 0, version);
    physicalPosition.recordVersion = version;
    currentPointer += OVersionFactory.instance().getVersionSize();

    OIntegerSerializer.INSTANCE.deserializeFromDirectMemory(pointer, currentPointer);
    currentPointer += OIntegerSerializer.INT_SIZE;

    physicalPosition.recordType = OByteSerializer.INSTANCE.deserializeFromDirectMemory(pointer, currentPointer);

    return physicalPosition;
  }

  @Override
  public OPhysicalPosition deserializeFromDirectMemoryObject(OWALChangesTree.PointerWrapper wrapper, long offset) {
    final OPhysicalPosition physicalPosition = new OPhysicalPosition();

    long currentPointer = offset;
    physicalPosition.clusterPosition = OLongSerializer.INSTANCE.deserializeFromDirectMemoryObject(wrapper, currentPointer);
    currentPointer += OLongSerializer.INSTANCE.getFixedLength();

    final ORecordVersion version = OVersionFactory.instance().createVersion();
    byte[] serializedVersion = wrapper.get(currentPointer, OVersionFactory.instance().getVersionSize());
    version.getSerializer().fastReadFrom(serializedVersion, 0, version);
    physicalPosition.recordVersion = version;
    currentPointer += OVersionFactory.instance().getVersionSize();

    OIntegerSerializer.INSTANCE.deserializeFromDirectMemory(wrapper, currentPointer);
    currentPointer += OIntegerSerializer.INT_SIZE;

    physicalPosition.recordType = OByteSerializer.INSTANCE.deserializeFromDirectMemory(wrapper, currentPointer);

    return physicalPosition;
  }

  @Override
  public int getObjectSizeInDirectMemory(ODirectMemoryPointer pointer, long offset) {
    return getFixedLength();
  }

  @Override
  public int getObjectSizeInDirectMemory(OWALChangesTree.PointerWrapper wrapper, long offset) {
    return getFixedLength();
  }

  @Override
  public OPhysicalPosition preprocess(OPhysicalPosition value, Object... hints) {
    return value;
  }
}
