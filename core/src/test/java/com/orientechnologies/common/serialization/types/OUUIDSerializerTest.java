package com.orientechnologies.common.serialization.types;

import org.junit.Assert;import org.junit.Before; import org.junit.Test;
import java.nio.ByteBuffer;
import java.util.UUID;

public class OUUIDSerializerTest {
  private static final int  FIELD_SIZE = 16;
  private static final UUID OBJECT     = UUID.randomUUID();
  private OUUIDSerializer uuidSerializer;

  @Before
  public void beforeClass() {
    uuidSerializer = new OUUIDSerializer();
  }

  @Test
  public void testFieldSize() {
    Assert.assertEquals(uuidSerializer.getObjectSize(OBJECT), FIELD_SIZE);
  }

  @Test
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
