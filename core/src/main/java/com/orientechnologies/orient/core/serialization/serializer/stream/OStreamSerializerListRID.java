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
import com.orientechnologies.common.serialization.types.OBinaryTypeSerializer;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.serialization.OBinaryProtocol;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializerFactory;
import com.orientechnologies.orient.core.serialization.serializer.record.string.ORecordSerializerSchemaAware2CSV;
import com.orientechnologies.orient.core.type.tree.OMVRBTreeRIDSet;

public class OStreamSerializerListRID implements OStreamSerializer, OBinarySerializer<OMVRBTreeRIDSet> {
  public static final String                            NAME     = "y";
  public static final OStreamSerializerListRID          INSTANCE = new OStreamSerializerListRID();
  private static final ORecordSerializerSchemaAware2CSV FORMAT   = (ORecordSerializerSchemaAware2CSV) ORecordSerializerFactory
                                                                     .instance().getFormat(ORecordSerializerSchemaAware2CSV.NAME);

  public static final byte                              ID       = 19;

  public Object fromStream(final byte[] iStream) throws IOException {
    if (iStream == null)
      return null;

    final String s = OBinaryProtocol.bytes2string(iStream);

    return FORMAT.embeddedCollectionFromStream(null, OType.EMBEDDEDSET, null, OType.LINK, s);
  }

  public byte[] toStream(final Object iObject) throws IOException {
    if (iObject == null)
      return null;

    return ((OMVRBTreeRIDSet) iObject).toStream();
  }

  public String getName() {
    return NAME;
  }

  @Override
  public int getObjectSize(OMVRBTreeRIDSet object, Object... hints) {
    final byte[] serializedSet = object.toStream();
    return OBinaryTypeSerializer.INSTANCE.getObjectSize(serializedSet);
  }

  @Override
  public int getObjectSize(byte[] stream, int startPosition) {
    return OBinaryTypeSerializer.INSTANCE.getObjectSize(stream, startPosition);
  }

  @Override
  public void serialize(OMVRBTreeRIDSet object, byte[] stream, int startPosition, Object... hints) {
    final byte[] serializedSet = object.toStream();
    OBinaryTypeSerializer.INSTANCE.serialize(serializedSet, stream, startPosition);
  }

  @Override
  public OMVRBTreeRIDSet deserialize(byte[] stream, int startPosition) {
    final byte[] serializedSet = OBinaryTypeSerializer.INSTANCE.deserialize(stream, startPosition);

    final String s = OBinaryProtocol.bytes2string(serializedSet);

    return (OMVRBTreeRIDSet) FORMAT.embeddedCollectionFromStream(null, OType.EMBEDDEDSET, null, OType.LINK, s);
  }

  @Override
  public byte getId() {
    return ID;
  }

  @Override
  public boolean isFixedLength() {
    return false;
  }

  @Override
  public int getFixedLength() {
    return 0;
  }

  @Override
  public void serializeNative(OMVRBTreeRIDSet object, byte[] stream, int startPosition, Object... hints) {
    final byte[] serializedSet = object.toStream();
    OBinaryTypeSerializer.INSTANCE.serializeNative(serializedSet, stream, startPosition);

  }

  @Override
  public OMVRBTreeRIDSet deserializeNative(byte[] stream, int startPosition) {
    final byte[] serializedSet = OBinaryTypeSerializer.INSTANCE.deserializeNative(stream, startPosition);

    final String s = OBinaryProtocol.bytes2string(serializedSet);

    return (OMVRBTreeRIDSet) FORMAT.embeddedCollectionFromStream(null, OType.EMBEDDEDSET, null, OType.LINK, s);
  }

  @Override
  public int getObjectSizeNative(byte[] stream, int startPosition) {
    return OBinaryTypeSerializer.INSTANCE.getObjectSizeNative(stream, startPosition);
  }

  @Override
  public void serializeInDirectMemory(OMVRBTreeRIDSet object, ODirectMemoryPointer pointer, long offset, Object... hints) {
    final byte[] serializedSet = object.toStream();
    OBinaryTypeSerializer.INSTANCE.serializeInDirectMemory(serializedSet, pointer, offset);
  }

  @Override
  public OMVRBTreeRIDSet deserializeFromDirectMemory(ODirectMemoryPointer pointer, long offset) {
    final byte[] serializedSet = OBinaryTypeSerializer.INSTANCE.deserializeFromDirectMemory(pointer, offset);

    final String s = OBinaryProtocol.bytes2string(serializedSet);

    return (OMVRBTreeRIDSet) FORMAT.embeddedCollectionFromStream(null, OType.EMBEDDEDSET, null, OType.LINK, s);
  }

  @Override
  public int getObjectSizeInDirectMemory(ODirectMemoryPointer pointer, long offset) {
    return OBinaryTypeSerializer.INSTANCE.getObjectSizeInDirectMemory(pointer, offset);
  }

  @Override
  public OMVRBTreeRIDSet prepocess(OMVRBTreeRIDSet value, Object... hints) {
    return value;
  }
}
