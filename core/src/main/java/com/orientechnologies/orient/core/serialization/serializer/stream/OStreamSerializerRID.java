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
package com.orientechnologies.orient.core.serialization.serializer.stream;

import java.io.IOException;

import com.orientechnologies.common.directmemory.ODirectMemoryPointer;
import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OLinkSerializer;

public class OStreamSerializerRID implements OStreamSerializer, OBinarySerializer<OIdentifiable> {
  public static final String               NAME     = "p";
  public static final OStreamSerializerRID INSTANCE = new OStreamSerializerRID();
  public static final byte                 ID       = 16;

  public String getName() {
    return NAME;
  }

  public Object fromStream(final byte[] iStream) throws IOException {
    if (iStream == null)
      return null;

    return new ORecordId().fromStream(iStream);
  }

  public byte[] toStream(final Object iObject) throws IOException {
    if (iObject == null)
      return null;

    return ((OIdentifiable) iObject).getIdentity().toStream();
  }

  public int getObjectSize(OIdentifiable object, Object... hints) {
    return OLinkSerializer.INSTANCE.getObjectSize(object.getIdentity());
  }

  public void serialize(OIdentifiable object, byte[] stream, int startPosition, Object... hints) {
    OLinkSerializer.INSTANCE.serialize(object.getIdentity(), stream, startPosition);
  }

  public ORID deserialize(byte[] stream, int startPosition) {
    return OLinkSerializer.INSTANCE.deserialize(stream, startPosition);
  }

  public int getObjectSize(byte[] stream, int startPosition) {
    return OLinkSerializer.INSTANCE.getObjectSize(stream, startPosition);
  }

  public byte getId() {
    return ID;
  }

  public int getObjectSizeNative(byte[] stream, int startPosition) {
    return OLinkSerializer.INSTANCE.getObjectSizeNative(stream, startPosition);
  }

  public void serializeNative(OIdentifiable object, byte[] stream, int startPosition, Object... hints) {
    OLinkSerializer.INSTANCE.serializeNative(object.getIdentity(), stream, startPosition);
  }

  public OIdentifiable deserializeNative(byte[] stream, int startPosition) {
    return OLinkSerializer.INSTANCE.deserializeNative(stream, startPosition);
  }

  @Override
  public void serializeInDirectMemory(OIdentifiable object, ODirectMemoryPointer pointer, long offset, Object... hints) {
    OLinkSerializer.INSTANCE.serializeInDirectMemory(object, pointer, offset);
  }

  @Override
  public OIdentifiable deserializeFromDirectMemory(ODirectMemoryPointer pointer, long offset) {
    return OLinkSerializer.INSTANCE.deserializeFromDirectMemory(pointer, offset);
  }

  @Override
  public int getObjectSizeInDirectMemory(ODirectMemoryPointer pointer, long offset) {
    return OLinkSerializer.INSTANCE.getObjectSizeInDirectMemory(pointer, offset);
  }

  public boolean isFixedLength() {
    return true;
  }

  public int getFixedLength() {
    return OLinkSerializer.RID_SIZE;
  }

  @Override
  public OIdentifiable prepocess(OIdentifiable value, Object... hints) {
    return value;
  }
}
