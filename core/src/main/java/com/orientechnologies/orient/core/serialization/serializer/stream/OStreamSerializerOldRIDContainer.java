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

import com.orientechnologies.common.directmemory.ODirectMemoryPointer;
import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.serialization.types.OBinaryTypeSerializer;
import com.orientechnologies.orient.core.db.record.ridbag.sbtree.OIndexRIDContainer;
import com.orientechnologies.orient.core.db.record.ridbag.sbtree.OIndexRIDContainerSBTree;
import com.orientechnologies.orient.core.index.sbtreebonsai.local.OBonsaiBucketPointer;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.OBinaryProtocol;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializerFactory;
import com.orientechnologies.orient.core.serialization.serializer.record.string.ORecordSerializerSchemaAware2CSV;

import java.io.IOException;

/**
 * Serializer of OIndexRIDContainer for back-compatibility with v1.6.1.
 */
public class OStreamSerializerOldRIDContainer implements OStreamSerializer, OBinarySerializer<OIndexRIDContainer> {
  public static final String                            NAME     = "ic";
  public static final OStreamSerializerOldRIDContainer  INSTANCE = new OStreamSerializerOldRIDContainer();
  private static final ORecordSerializerSchemaAware2CSV FORMAT   = (ORecordSerializerSchemaAware2CSV) ORecordSerializerFactory
                                                                     .instance().getFormat(ORecordSerializerSchemaAware2CSV.NAME);

  public static final byte                              ID       = 20;

  public Object fromStream(final byte[] iStream) throws IOException {
    if (iStream == null)
      return null;

    final String s = OBinaryProtocol.bytes2string(iStream);

    return containerFromStream(s);
  }

  public byte[] toStream(final Object object) throws IOException {
    if (object == null)
      return null;

    return containerToStream((OIndexRIDContainer) object);
  }

  public String getName() {
    return NAME;
  }

  @Override
  public int getObjectSize(OIndexRIDContainer object, Object... hints) {
    final byte[] serializedSet = containerToStream(object);
    return OBinaryTypeSerializer.INSTANCE.getObjectSize(serializedSet);
  }

  @Override
  public int getObjectSize(byte[] stream, int startPosition) {
    return OBinaryTypeSerializer.INSTANCE.getObjectSize(stream, startPosition);
  }

  @Override
  public void serialize(OIndexRIDContainer object, byte[] stream, int startPosition, Object... hints) {
    final byte[] serializedSet = containerToStream(object);
    OBinaryTypeSerializer.INSTANCE.serialize(serializedSet, stream, startPosition);
  }

  @Override
  public OIndexRIDContainer deserialize(byte[] stream, int startPosition) {
    final byte[] serializedSet = OBinaryTypeSerializer.INSTANCE.deserialize(stream, startPosition);

    final String s = OBinaryProtocol.bytes2string(serializedSet);

    if (s.startsWith("<#@")) {
      return containerFromStream(s);
    }

    return (OIndexRIDContainer) FORMAT.embeddedCollectionFromStream(null, OType.EMBEDDEDSET, null, OType.LINK, s);
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
  public void serializeNative(OIndexRIDContainer object, byte[] stream, int startPosition, Object... hints) {
    final byte[] serializedSet = containerToStream(object);
    OBinaryTypeSerializer.INSTANCE.serializeNative(serializedSet, stream, startPosition);

  }

  @Override
  public OIndexRIDContainer deserializeNative(byte[] stream, int startPosition) {
    final byte[] serializedSet = OBinaryTypeSerializer.INSTANCE.deserializeNative(stream, startPosition);

    final String s = OBinaryProtocol.bytes2string(serializedSet);

    if (s.startsWith("<#@")) {
      return containerFromStream(s);
    }

    return (OIndexRIDContainer) FORMAT.embeddedCollectionFromStream(null, OType.EMBEDDEDSET, null, OType.LINK, s);
  }

  @Override
  public int getObjectSizeNative(byte[] stream, int startPosition) {
    return OBinaryTypeSerializer.INSTANCE.getObjectSizeNative(stream, startPosition);
  }

  @Override
  public void serializeInDirectMemory(OIndexRIDContainer object, ODirectMemoryPointer pointer, long offset, Object... hints) {
    final byte[] serializedSet = containerToStream(object);
    OBinaryTypeSerializer.INSTANCE.serializeInDirectMemory(serializedSet, pointer, offset);
  }

  @Override
  public OIndexRIDContainer deserializeFromDirectMemory(ODirectMemoryPointer pointer, long offset) {
    final byte[] serializedSet = OBinaryTypeSerializer.INSTANCE.deserializeFromDirectMemory(pointer, offset);

    final String s = OBinaryProtocol.bytes2string(serializedSet);

    if (s.startsWith("<#@")) {
      return containerFromStream(s);
    }

    return (OIndexRIDContainer) FORMAT.embeddedCollectionFromStream(null, OType.EMBEDDEDSET, null, OType.LINK, s);
  }

  @Override
  public int getObjectSizeInDirectMemory(ODirectMemoryPointer pointer, long offset) {
    return OBinaryTypeSerializer.INSTANCE.getObjectSizeInDirectMemory(pointer, offset);
  }

  @Override
  public OIndexRIDContainer preprocess(OIndexRIDContainer value, Object... hints) {
    return value;
  }

  private byte[] containerToStream(OIndexRIDContainer object) {
    StringBuilder iOutput = new StringBuilder();
    iOutput.append(OStringSerializerHelper.LINKSET_PREFIX);

    object.checkNotEmbedded();
    final OIndexRIDContainerSBTree tree = ((OIndexRIDContainerSBTree) object.getUnderlying());

    final ODocument document = new ODocument();
    document.field("rootIndex", tree.getRootPointer().getPageIndex());
    document.field("rootOffset", tree.getRootPointer().getPageOffset());
    document.field("file", tree.getName());
    iOutput.append(new String(document.toStream()));

    iOutput.append(OStringSerializerHelper.SET_END);
    return iOutput.toString().getBytes();
  }

  private OIndexRIDContainer containerFromStream(String stream) {
    stream = stream.substring(OStringSerializerHelper.LINKSET_PREFIX.length(), stream.length() - 1);

    final ODocument doc = new ODocument();
    doc.fromString(stream);
    final OBonsaiBucketPointer rootPointer = new OBonsaiBucketPointer((Long) doc.field("rootIndex"),
        (Integer) doc.field("rootOffset"));
    final String fileName = doc.field("file");

    return new OIndexRIDContainer(fileName, new OIndexRIDContainerSBTree(fileName, rootPointer), false);
  }
}
