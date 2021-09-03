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

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWALChanges;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWALChangesTree;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.junit.Assert;
import org.junit.Before;

/**
 * @author Ilya Bershadskiy (ibersh20-at-gmail.com)
 * @since 18.01.12
 */
public class BooleanSerializerTest {
  private static final int FIELD_SIZE = 1;
  private static final Boolean OBJECT_TRUE = true;
  private static final Boolean OBJECT_FALSE = false;
  byte[] stream = new byte[FIELD_SIZE];
  private OBooleanSerializer booleanSerializer;

  @Before
  public void beforeClass() {
    booleanSerializer = new OBooleanSerializer();
  }

  public void testFieldSize() {
    Assert.assertEquals(booleanSerializer.getObjectSize(null), FIELD_SIZE);
  }

  public void testSerialize() {
    booleanSerializer.serialize(OBJECT_TRUE, stream, 0);
    Assert.assertEquals(booleanSerializer.deserialize(stream, 0), OBJECT_TRUE);
    booleanSerializer.serialize(OBJECT_FALSE, stream, 0);
    Assert.assertEquals(booleanSerializer.deserialize(stream, 0), OBJECT_FALSE);
  }

  public void testSerializeNative() {
    booleanSerializer.serializeNative(OBJECT_TRUE, stream, 0);
    Assert.assertEquals(booleanSerializer.deserializeNativeObject(stream, 0), OBJECT_TRUE);
    booleanSerializer.serializeNative(OBJECT_FALSE, stream, 0);
    Assert.assertEquals(booleanSerializer.deserializeNativeObject(stream, 0), OBJECT_FALSE);
  }

  public void testNativeDirectMemoryCompatibility() {
    booleanSerializer.serializeNative(OBJECT_TRUE, stream, 0);

    ByteBuffer buffer = ByteBuffer.allocateDirect(stream.length).order(ByteOrder.nativeOrder());
    buffer.position(0);
    buffer.put(stream);

    buffer.position(0);
    Assert.assertEquals(booleanSerializer.deserializeFromByteBufferObject(buffer), OBJECT_TRUE);

    booleanSerializer.serializeNative(OBJECT_FALSE, stream, 0);
    buffer = ByteBuffer.allocateDirect(stream.length).order(ByteOrder.nativeOrder());
    Assert.assertEquals(booleanSerializer.deserializeFromByteBufferObject(buffer), OBJECT_FALSE);
  }

  public void testSerializationByteBuffer() {
    final int serializationOffset = 5;

    ByteBuffer buffer = ByteBuffer.allocate(FIELD_SIZE + serializationOffset);

    buffer.position(serializationOffset);
    booleanSerializer.serializeInByteBufferObject(OBJECT_TRUE, buffer);

    int binarySize = buffer.position() - serializationOffset;
    Assert.assertEquals(binarySize, FIELD_SIZE);

    buffer.position(serializationOffset);
    Assert.assertEquals(booleanSerializer.getObjectSizeInByteBuffer(buffer), FIELD_SIZE);

    buffer.position(serializationOffset);
    Assert.assertEquals(booleanSerializer.deserializeFromByteBufferObject(buffer), OBJECT_TRUE);

    buffer = ByteBuffer.allocate(FIELD_SIZE + serializationOffset);

    buffer.position(serializationOffset);
    booleanSerializer.serializeInByteBufferObject(OBJECT_FALSE, buffer);

    binarySize = buffer.position() - serializationOffset;
    Assert.assertEquals(binarySize, FIELD_SIZE);

    buffer.position(serializationOffset);
    Assert.assertEquals(booleanSerializer.getObjectSizeInByteBuffer(buffer), FIELD_SIZE);

    buffer.position(serializationOffset);
    Assert.assertEquals(booleanSerializer.deserializeFromByteBufferObject(buffer), OBJECT_FALSE);
  }

  public void testSerializationWalChanges() {
    final int serializationOffset = 5;

    byte[] data = new byte[FIELD_SIZE];
    ByteBuffer buffer =
        ByteBuffer.allocateDirect(FIELD_SIZE + serializationOffset).order(ByteOrder.nativeOrder());

    booleanSerializer.serializeNative(OBJECT_TRUE, data, 0);
    OWALChanges walChanges = new OWALChangesTree();
    walChanges.setBinaryValue(buffer, data, serializationOffset);

    Assert.assertEquals(
        booleanSerializer.getObjectSizeInByteBuffer(buffer, walChanges, serializationOffset),
        FIELD_SIZE);
    Assert.assertEquals(
        booleanSerializer.deserializeFromByteBufferObject(buffer, walChanges, serializationOffset),
        OBJECT_TRUE);

    booleanSerializer.serializeNative(OBJECT_FALSE, data, 0);
    walChanges.setBinaryValue(buffer, data, 0);

    Assert.assertEquals(
        booleanSerializer.getObjectSizeInByteBuffer(buffer, walChanges, serializationOffset),
        FIELD_SIZE);
    Assert.assertEquals(
        booleanSerializer.deserializeFromByteBufferObject(buffer, walChanges, serializationOffset),
        OBJECT_FALSE);
  }
}
