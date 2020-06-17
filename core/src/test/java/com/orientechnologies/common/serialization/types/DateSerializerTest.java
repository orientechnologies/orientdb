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
import java.util.Calendar;
import java.util.Date;
import org.junit.Assert;
import org.junit.Before;

/**
 * @author Ilya Bershadskiy (ibersh20-at-gmail.com)
 * @since 20.01.12
 */
public class DateSerializerTest {
  private static final int FIELD_SIZE = 8;
  private final byte[] stream = new byte[FIELD_SIZE];
  private final Date OBJECT = new Date();
  private ODateSerializer dateSerializer;

  @Before
  public void beforeClass() {
    dateSerializer = new ODateSerializer();
  }

  public void testFieldSize() {
    Assert.assertEquals(dateSerializer.getObjectSize(OBJECT), FIELD_SIZE);
  }

  public void testSerialize() {
    dateSerializer.serialize(OBJECT, stream, 0);
    Calendar calendar = Calendar.getInstance();
    calendar.setTime(OBJECT);
    calendar.set(Calendar.HOUR_OF_DAY, 0);
    calendar.set(Calendar.MINUTE, 0);
    calendar.set(Calendar.SECOND, 0);
    calendar.set(Calendar.MILLISECOND, 0);
    Assert.assertEquals(dateSerializer.deserialize(stream, 0), calendar.getTime());
  }

  public void testSerializeNative() {
    dateSerializer.serializeNativeObject(OBJECT, stream, 0);
    Calendar calendar = Calendar.getInstance();
    calendar.setTime(OBJECT);
    calendar.set(Calendar.HOUR_OF_DAY, 0);
    calendar.set(Calendar.MINUTE, 0);
    calendar.set(Calendar.SECOND, 0);
    calendar.set(Calendar.MILLISECOND, 0);
    Assert.assertEquals(dateSerializer.deserializeNativeObject(stream, 0), calendar.getTime());
  }

  public void testNativeDirectMemoryCompatibility() {
    dateSerializer.serializeNativeObject(OBJECT, stream, 0);
    Calendar calendar = Calendar.getInstance();
    calendar.setTime(OBJECT);
    calendar.set(Calendar.HOUR_OF_DAY, 0);
    calendar.set(Calendar.MINUTE, 0);
    calendar.set(Calendar.SECOND, 0);
    calendar.set(Calendar.MILLISECOND, 0);

    ByteBuffer buffer = ByteBuffer.allocateDirect(stream.length).order(ByteOrder.nativeOrder());
    buffer.position(0);
    buffer.put(stream);
    buffer.position(0);

    Assert.assertEquals(dateSerializer.deserializeFromByteBufferObject(buffer), calendar.getTime());
  }

  public void testSerializeInByteBuffer() {
    final int serializationOffset = 5;

    final ByteBuffer buffer = ByteBuffer.allocate(FIELD_SIZE + serializationOffset);

    Calendar calendar = Calendar.getInstance();
    calendar.setTime(OBJECT);
    calendar.set(Calendar.HOUR_OF_DAY, 0);
    calendar.set(Calendar.MINUTE, 0);
    calendar.set(Calendar.SECOND, 0);
    calendar.set(Calendar.MILLISECOND, 0);

    buffer.position(serializationOffset);
    dateSerializer.serializeInByteBufferObject(OBJECT, buffer);

    final int binarySize = buffer.position() - serializationOffset;
    Assert.assertEquals(binarySize, FIELD_SIZE);

    buffer.position(serializationOffset);
    Assert.assertEquals(dateSerializer.getObjectSizeInByteBuffer(buffer), FIELD_SIZE);

    buffer.position(serializationOffset);
    Assert.assertEquals(dateSerializer.deserializeFromByteBufferObject(buffer), calendar.getTime());

    Assert.assertEquals(buffer.position() - serializationOffset, binarySize);
  }

  public void testSerializeWALChanges() {
    final int serializationOffset = 5;
    final ByteBuffer buffer =
        ByteBuffer.allocateDirect(FIELD_SIZE + serializationOffset).order(ByteOrder.nativeOrder());
    final byte[] data = new byte[FIELD_SIZE];
    dateSerializer.serializeNativeObject(OBJECT, data, 0);
    final OWALChanges walChanges = new OWALChangesTree();
    walChanges.setBinaryValue(buffer, data, serializationOffset);

    Calendar calendar = Calendar.getInstance();
    calendar.setTime(OBJECT);
    calendar.set(Calendar.HOUR_OF_DAY, 0);
    calendar.set(Calendar.MINUTE, 0);
    calendar.set(Calendar.SECOND, 0);
    calendar.set(Calendar.MILLISECOND, 0);

    Assert.assertEquals(
        dateSerializer.getObjectSizeInByteBuffer(buffer, walChanges, serializationOffset),
        FIELD_SIZE);
    Assert.assertEquals(
        dateSerializer.deserializeFromByteBufferObject(buffer, walChanges, serializationOffset),
        calendar.getTime());
  }
}
