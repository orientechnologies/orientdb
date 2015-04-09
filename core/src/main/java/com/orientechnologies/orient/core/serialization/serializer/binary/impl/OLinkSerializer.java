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

package com.orientechnologies.orient.core.serialization.serializer.binary.impl;

import static com.orientechnologies.orient.core.serialization.OBinaryProtocol.bytes2long;
import static com.orientechnologies.orient.core.serialization.OBinaryProtocol.bytes2short;
import static com.orientechnologies.orient.core.serialization.OBinaryProtocol.long2bytes;
import static com.orientechnologies.orient.core.serialization.OBinaryProtocol.short2bytes;

import com.orientechnologies.common.directmemory.ODirectMemoryPointer;
import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.common.serialization.types.OShortSerializer;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWALChangesTree;

/**
 * Serializer for {@link com.orientechnologies.orient.core.metadata.schema.OType#LINK}
 *
 * @author Ilya Bershadskiy (ibersh20-at-gmail.com)
 * @since 07.02.12
 */
public class OLinkSerializer implements OBinarySerializer<OIdentifiable> {
  public static final byte      ID               = 9;
  private static final int      CLUSTER_POS_SIZE = OLongSerializer.LONG_SIZE;
  public static final int       RID_SIZE         = OShortSerializer.SHORT_SIZE + CLUSTER_POS_SIZE;
  public static OLinkSerializer INSTANCE         = new OLinkSerializer();

  public int getObjectSize(final OIdentifiable rid, Object... hints) {
    return RID_SIZE;
  }

  public void serialize(final OIdentifiable rid, final byte[] stream, final int startPosition, Object... hints) {
    final ORID r = rid.getIdentity();
    short2bytes((short) r.getClusterId(), stream, startPosition);
    long2bytes(r.getClusterPosition(), stream, startPosition + OShortSerializer.SHORT_SIZE);
  }

  public ORecordId deserialize(final byte[] stream, final int startPosition) {
    return new ORecordId(bytes2short(stream, startPosition), bytes2long(stream, startPosition + OShortSerializer.SHORT_SIZE));
  }

  public int getObjectSize(final byte[] stream, final int startPosition) {
    return RID_SIZE;
  }

  public byte getId() {
    return ID;
  }

  public int getObjectSizeNative(byte[] stream, int startPosition) {
    return RID_SIZE;
  }

  public void serializeNativeObject(OIdentifiable rid, byte[] stream, int startPosition, Object... hints) {
    final ORID r = rid.getIdentity();

    OShortSerializer.INSTANCE.serializeNative((short) r.getClusterId(), stream, startPosition);
    // Wrong implementation but needed for binary compatibility should be used serializeNative
    OLongSerializer.INSTANCE.serialize(r.getClusterPosition(), stream, startPosition + OShortSerializer.SHORT_SIZE);
  }

  public ORecordId deserializeNativeObject(byte[] stream, int startPosition) {
    final int clusterId = OShortSerializer.INSTANCE.deserializeNative(stream, startPosition);
    // Wrong implementation but needed for binary compatibility should be used deserializeNative
    final long clusterPosition = OLongSerializer.INSTANCE.deserialize(stream, startPosition + OShortSerializer.SHORT_SIZE);
    return new ORecordId(clusterId, clusterPosition);
  }

  @Override
  public void serializeInDirectMemoryObject(OIdentifiable rid, ODirectMemoryPointer pointer, long offset, Object... hints) {
    final ORID r = rid.getIdentity();

    OShortSerializer.INSTANCE.serializeInDirectMemory((short) r.getClusterId(), pointer, offset);
    // Wrong implementation but needed for binary compatibility
    byte[] stream = new byte[OLongSerializer.LONG_SIZE];
    OLongSerializer.INSTANCE.serialize(r.getClusterPosition(), stream, 0);
    pointer.set(offset + OShortSerializer.SHORT_SIZE, stream, 0, OLongSerializer.LONG_SIZE);
    // OLongSerializer.INSTANCE.serializeInDirectMemory(r.getClusterPosition(), pointer, offset + OShortSerializer.SHORT_SIZE);
  }

  @Override
  public OIdentifiable deserializeFromDirectMemoryObject(ODirectMemoryPointer pointer, long offset) {
    final int clusterId = OShortSerializer.INSTANCE.deserializeFromDirectMemory(pointer, offset);

    // Wrong implementation but needed for binary compatibility
    final long clusterPosition = OLongSerializer.INSTANCE.deserialize(
        pointer.get(offset + OShortSerializer.SHORT_SIZE, OLongSerializer.LONG_SIZE), 0);

    // final long clusterPosition = OLongSerializer.INSTANCE
    // .deserializeFromDirectMemory(pointer, offset + OShortSerializer.SHORT_SIZE);

    return new ORecordId(clusterId, clusterPosition);
  }

  @Override
  public OIdentifiable deserializeFromDirectMemoryObject(OWALChangesTree.PointerWrapper wrapper, long offset) {
    final int clusterId = OShortSerializer.INSTANCE.deserializeFromDirectMemory(wrapper, offset);

    // Wrong implementation but needed for binary compatibility
    final long clusterPosition = OLongSerializer.INSTANCE.deserialize(
        wrapper.get(offset + OShortSerializer.SHORT_SIZE, OLongSerializer.LONG_SIZE), 0);

    // final long clusterPosition = OLongSerializer.INSTANCE
    // .deserializeFromDirectMemory(pointer, offset + OShortSerializer.SHORT_SIZE);

    return new ORecordId(clusterId, clusterPosition);
  }

  @Override
  public int getObjectSizeInDirectMemory(ODirectMemoryPointer pointer, long offset) {
    return RID_SIZE;
  }

  @Override
  public int getObjectSizeInDirectMemory(OWALChangesTree.PointerWrapper wrapper, long offset) {
    return RID_SIZE;
  }

  public boolean isFixedLength() {
    return true;
  }

  public int getFixedLength() {
    return RID_SIZE;
  }

  @Override
  public OIdentifiable preprocess(OIdentifiable value, Object... hints) {
    if (value == null)
      return null;
    else
      return value.getIdentity();
  }
}
