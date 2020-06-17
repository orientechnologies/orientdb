/*
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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

package com.orientechnologies.common.serialization.types;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWALChangesTree;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.junit.Assert;
import org.junit.Before;

/**
 * @author Ilya Bershadskiy (ibersh20-at-gmail.com)
 * @since 20.01.12
 */
public class BinarySerializerTest {
  byte[] stream;
  private int FIELD_SIZE;
  private byte[] OBJECT;
  private OBinaryTypeSerializer binarySerializer;

  @Before
  public void beforeClass() {
    binarySerializer = new OBinaryTypeSerializer();
    OBJECT = new byte[] {1, 2, 3, 4, 5, 6};
    FIELD_SIZE = OBJECT.length + OIntegerSerializer.INT_SIZE;
    stream = new byte[FIELD_SIZE];
  }

  public void testFieldSize() {
    Assert.assertEquals(binarySerializer.getObjectSize(OBJECT), FIELD_SIZE);
  }

  public void testSerialize() {
    binarySerializer.serialize(OBJECT, stream, 0);
    Assert.assertEquals(binarySerializer.deserialize(stream, 0), OBJECT);
  }

  public void testSerializeNative() {
    binarySerializer.serializeNativeObject(OBJECT, stream, 0);
    Assert.assertEquals(binarySerializer.deserializeNativeObject(stream, 0), OBJECT);
  }

  public void testNativeByteBufferCompatibility() {
    binarySerializer.serializeNativeObject(OBJECT, stream, 0);

    ByteBuffer buffer = ByteBuffer.allocateDirect(stream.length).order(ByteOrder.nativeOrder());
    buffer.position(0);
    buffer.put(stream);

    buffer.position(0);
    Assert.assertEquals(binarySerializer.deserializeFromByteBufferObject(buffer), OBJECT);
  }

  public void testSerializeByteBuffer() {
    final int serializationOffset = 5;
    final ByteBuffer buffer = ByteBuffer.allocate(FIELD_SIZE + serializationOffset);
    buffer.position(serializationOffset);

    binarySerializer.serializeInByteBufferObject(OBJECT, buffer);

    final int binarySize = buffer.position() - serializationOffset;
    buffer.position(serializationOffset);
    Assert.assertEquals(binarySerializer.getObjectSizeInByteBuffer(buffer), binarySize);

    buffer.position(serializationOffset);
    Assert.assertEquals(binarySerializer.getObjectSizeInByteBuffer(buffer), FIELD_SIZE);

    buffer.position(serializationOffset);
    final byte[] result = binarySerializer.deserializeFromByteBufferObject(buffer);

    Assert.assertEquals(result, OBJECT);
  }

  public void testSerializeInWalChanges() {
    final int serializationOffset = 5;
    final ByteBuffer buffer =
        ByteBuffer.allocateDirect(FIELD_SIZE + serializationOffset).order(ByteOrder.nativeOrder());

    final byte[] data = new byte[FIELD_SIZE];
    final OWALChangesTree walChangesTree = new OWALChangesTree();
    binarySerializer.serializeNativeObject(OBJECT, data, 0);

    walChangesTree.add(data, serializationOffset);

    Assert.assertEquals(
        binarySerializer.getObjectSizeInByteBuffer(buffer, walChangesTree, serializationOffset),
        FIELD_SIZE);
    Assert.assertEquals(
        binarySerializer.deserializeFromByteBufferObject(
            buffer, walChangesTree, serializationOffset),
        OBJECT);
  }
}
