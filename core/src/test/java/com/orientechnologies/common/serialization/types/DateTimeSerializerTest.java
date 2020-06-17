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
import java.util.Date;
import org.junit.Assert;
import org.junit.Before;

/**
 * @author Ilya Bershadskiy (ibersh20-at-gmail.com)
 * @since 20.01.12
 */
public class DateTimeSerializerTest {
  private static final int FIELD_SIZE = 8;
  private static final Date OBJECT = new Date();
  private ODateTimeSerializer dateTimeSerializer;
  private static final byte[] stream = new byte[FIELD_SIZE];

  @Before
  public void beforeClass() {
    dateTimeSerializer = new ODateTimeSerializer();
  }

  public void testFieldSize() {
    Assert.assertEquals(dateTimeSerializer.getObjectSize(OBJECT), FIELD_SIZE);
  }

  public void testSerialize() {
    dateTimeSerializer.serialize(OBJECT, stream, 0);
    Assert.assertEquals(dateTimeSerializer.deserialize(stream, 0), OBJECT);
  }

  public void testSerializeNative() {
    dateTimeSerializer.serializeNativeObject(OBJECT, stream, 0);
    Assert.assertEquals(dateTimeSerializer.deserializeNativeObject(stream, 0), OBJECT);
  }

  public void testNativeDirectMemoryCompatibility() {
    dateTimeSerializer.serializeNativeObject(OBJECT, stream, 0);

    ByteBuffer buffer = ByteBuffer.allocateDirect(stream.length).order(ByteOrder.nativeOrder());
    buffer.put(stream);
    buffer.position(0);

    Assert.assertEquals(dateTimeSerializer.deserializeFromByteBufferObject(buffer), OBJECT);
  }

  public void testSerializeInByteBuffer() {
    final int serializationOffset = 5;
    final ByteBuffer buffer = ByteBuffer.allocate(FIELD_SIZE + serializationOffset);

    buffer.position(serializationOffset);
    dateTimeSerializer.serializeInByteBufferObject(OBJECT, buffer);

    final int binarySize = buffer.position() - serializationOffset;
    Assert.assertEquals(binarySize, FIELD_SIZE);

    buffer.position(serializationOffset);
    Assert.assertEquals(dateTimeSerializer.getObjectSizeInByteBuffer(buffer), FIELD_SIZE);

    buffer.position(serializationOffset);
    Assert.assertEquals(dateTimeSerializer.deserializeFromByteBufferObject(buffer), OBJECT);

    Assert.assertEquals(buffer.position() - serializationOffset, FIELD_SIZE);
  }

  public void testSerializeWALChanges() {
    final int serializationOffset = 5;

    final ByteBuffer buffer =
        ByteBuffer.allocateDirect(FIELD_SIZE + serializationOffset).order(ByteOrder.nativeOrder());
    final byte[] data = new byte[FIELD_SIZE];
    dateTimeSerializer.serializeNativeObject(OBJECT, data, 0);
    final OWALChanges walChanges = new OWALChangesTree();
    walChanges.setBinaryValue(buffer, data, serializationOffset);

    Assert.assertEquals(
        dateTimeSerializer.getObjectSizeInByteBuffer(buffer, walChanges, serializationOffset),
        FIELD_SIZE);
    Assert.assertEquals(
        dateTimeSerializer.deserializeFromByteBufferObject(buffer, walChanges, serializationOffset),
        OBJECT);
  }
}
