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

package com.orientechnologies.orient.core.serialization.serializer.binary.impl;

import static com.orientechnologies.orient.core.serialization.OBinaryProtocol.bytes2short;
import static com.orientechnologies.orient.core.serialization.OBinaryProtocol.short2bytes;

import com.orientechnologies.common.directmemory.ODirectMemoryPointer;
import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.serialization.types.OShortSerializer;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.OClusterPosition;
import com.orientechnologies.orient.core.id.OClusterPositionFactory;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;

/**
 * Serializer for {@link com.orientechnologies.orient.core.metadata.schema.OType#LINK}
 * 
 * @author ibershadskiy <a href="mailto:ibersh20@gmail.com">Ilya Bershadskiy</a>
 * @since 07.02.12
 */
public class OLinkSerializer implements OBinarySerializer<OIdentifiable> {
  private static final int      CLUSTER_POS_SIZE = OClusterPositionFactory.INSTANCE.getSerializedSize();

  public static OLinkSerializer INSTANCE         = new OLinkSerializer();
  public static final byte      ID               = 9;
  public static final int       RID_SIZE         = OShortSerializer.SHORT_SIZE + CLUSTER_POS_SIZE;

  public int getObjectSize(final OIdentifiable rid, Object... hints) {
    return RID_SIZE;
  }

  public void serialize(final OIdentifiable rid, final byte[] stream, final int startPosition, Object... hints) {
    ORID r = rid.getIdentity();
    short2bytes((short) r.getClusterId(), stream, startPosition);

    System.arraycopy(r.getClusterPosition().toStream(), 0, stream, startPosition + OShortSerializer.SHORT_SIZE, CLUSTER_POS_SIZE);
  }

  public ORecordId deserialize(final byte[] stream, final int startPosition) {
    return new ORecordId(bytes2short(stream, startPosition), OClusterPositionFactory.INSTANCE.fromStream(stream, startPosition
        + OShortSerializer.SHORT_SIZE));
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

  public void serializeNative(OIdentifiable rid, byte[] stream, int startPosition, Object... hints) {
    ORID r = rid.getIdentity();

    OShortSerializer.INSTANCE.serializeNative((short) r.getClusterId(), stream, startPosition);

    System.arraycopy(r.getClusterPosition().toStream(), 0, stream, startPosition + OShortSerializer.SHORT_SIZE, CLUSTER_POS_SIZE);
  }

  public ORecordId deserializeNative(byte[] stream, int startPosition) {
    int clusterId = OShortSerializer.INSTANCE.deserializeNative(stream, startPosition);
    OClusterPosition clusterPosition = OClusterPositionFactory.INSTANCE.fromStream(stream, startPosition
        + OShortSerializer.SHORT_SIZE);

    return new ORecordId(clusterId, clusterPosition);
  }

  @Override
  public void serializeInDirectMemory(OIdentifiable rid, ODirectMemoryPointer pointer, long offset, Object... hints) {
    ORID r = rid.getIdentity();

    OShortSerializer.INSTANCE.serializeInDirectMemory((short) r.getClusterId(), pointer, offset);

    pointer.set(offset + OShortSerializer.SHORT_SIZE, r.getClusterPosition().toStream(), 0, CLUSTER_POS_SIZE);
  }

  @Override
  public OIdentifiable deserializeFromDirectMemory(ODirectMemoryPointer pointer, long offset) {
    int clusterId = OShortSerializer.INSTANCE.deserializeFromDirectMemory(pointer, offset);
    OClusterPosition clusterPosition = OClusterPositionFactory.INSTANCE.fromStream(pointer.get(
        offset + OShortSerializer.SHORT_SIZE, CLUSTER_POS_SIZE));

    return new ORecordId(clusterId, clusterPosition);
  }

  @Override
  public int getObjectSizeInDirectMemory(ODirectMemoryPointer pointer, long offset) {
    return RID_SIZE;
  }

  public boolean isFixedLength() {
    return true;
  }

  public int getFixedLength() {
    return RID_SIZE;
  }

  @Override
  public OIdentifiable prepocess(OIdentifiable value, Object... hints) {
    return value.getIdentity();
  }
}
