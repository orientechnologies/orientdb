package com.orientechnologies.common.serialization.types;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.util.UUID;

@Test
public class OUUIDSerializerTest {
  private static final int  FIELD_SIZE = 16;
  private static final UUID OBJECT     = UUID.randomUUID();
  private OUUIDSerializer uuidSerializer;

  @BeforeClass
  public void beforeClass() {
    uuidSerializer = new OUUIDSerializer();
  }

  public void testFieldSize() {
    Assert.assertEquals(uuidSerializer.getObjectSize(OBJECT), FIELD_SIZE);
  }

  public void testSerializationInByteBuffer() {
    final int serializationOffset = 5;
    final ByteBuffer buffer = ByteBuffer.allocate(FIELD_SIZE + serializationOffset);
    buffer.position(serializationOffset);

    uuidSerializer.serializeInByteBufferObject(OBJECT, buffer);

    final int binarySize = buffer.position() - serializationOffset;
    Assert.assertEquals(binarySize, FIELD_SIZE);

    buffer.position(serializationOffset);
    Assert.assertEquals(uuidSerializer.getObjectSizeInByteBuffer(buffer), FIELD_SIZE);

    buffer.position(serializationOffset);
    Assert.assertEquals(uuidSerializer.deserializeFromByteBufferObject(buffer), OBJECT);

    Assert.assertEquals(buffer.position() - serializationOffset, FIELD_SIZE);
  }

}
