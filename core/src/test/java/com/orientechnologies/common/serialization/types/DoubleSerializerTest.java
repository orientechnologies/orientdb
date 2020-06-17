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
import org.junit.Test;

/**
 * @author Ilya Bershadskiy (ibersh20-at-gmail.com)
 * @since 18.01.12
 */
public class DoubleSerializerTest {
  private static final int FIELD_SIZE = 8;
  private static final Double OBJECT = Math.PI;
  byte[] stream = new byte[FIELD_SIZE];
  private ODoubleSerializer doubleSerializer;

  @Before
  public void beforeClass() {
    doubleSerializer = new ODoubleSerializer();
  }

  @Test
  public void testFieldSize() {
    Assert.assertEquals(doubleSerializer.getObjectSize(null), FIELD_SIZE);
  }

  @Test
  public void testSerialize() {
    doubleSerializer.serialize(OBJECT, stream, 0);
    Assert.assertEquals(doubleSerializer.deserialize(stream, 0), OBJECT);
  }

  @Test
  public void testSerializeNative() {
    doubleSerializer.serializeNative(OBJECT, stream, 0);
    Double v = doubleSerializer.deserializeNative(stream, 0);
    Assert.assertEquals(v, OBJECT);
  }

  @Test
  public void testNativeDirectMemoryCompatibility() {
    doubleSerializer.serializeNative(OBJECT, stream, 0);

    ByteBuffer buffer = ByteBuffer.allocateDirect(stream.length).order(ByteOrder.nativeOrder());
    buffer.put(stream);
    buffer.position(0);

    Assert.assertEquals(doubleSerializer.deserializeFromByteBufferObject(buffer), OBJECT);
  }

  @Test
  public void testSerializeInByteBuffer() {
    final int serializationOffset = 5;
    ByteBuffer buffer = ByteBuffer.allocate(FIELD_SIZE + serializationOffset);

    buffer.position(5);
    doubleSerializer.serializeInByteBufferObject(OBJECT, buffer);

    final int binarySize = buffer.position() - serializationOffset;
    Assert.assertEquals(binarySize, FIELD_SIZE);

    buffer.position(serializationOffset);
    Assert.assertEquals(doubleSerializer.getObjectSizeInByteBuffer(buffer), FIELD_SIZE);

    buffer.position(serializationOffset);
    Assert.assertEquals(doubleSerializer.deserializeFromByteBufferObject(buffer), OBJECT);

    Assert.assertEquals(buffer.position() - serializationOffset, FIELD_SIZE);
  }

  @Test
  public void testSerializeWALChanges() {
    final int serializationOffset = 5;
    final ByteBuffer buffer =
        ByteBuffer.allocateDirect(FIELD_SIZE + serializationOffset).order(ByteOrder.nativeOrder());
    final byte[] data = new byte[FIELD_SIZE];
    doubleSerializer.serializeNativeObject(OBJECT, data, 0);

    final OWALChanges walChanges = new OWALChangesTree();
    walChanges.setBinaryValue(buffer, data, serializationOffset);

    Assert.assertEquals(
        doubleSerializer.getObjectSizeInByteBuffer(buffer, walChanges, serializationOffset),
        FIELD_SIZE);
    Assert.assertEquals(
        doubleSerializer.deserializeFromByteBufferObject(buffer, walChanges, serializationOffset),
        OBJECT);
  }
}
