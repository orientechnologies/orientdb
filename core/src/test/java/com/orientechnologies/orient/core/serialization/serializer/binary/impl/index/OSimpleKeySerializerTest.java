package com.orientechnologies.orient.core.serialization.serializer.binary.impl.index;

import org.junit.Assert;import org.junit.Before; import org.junit.Test;
import java.nio.ByteBuffer;

public class OSimpleKeySerializerTest {
  private static final int    FIELD_SIZE = 9;
  private static final Double OBJECT     = Math.PI;
  private OSimpleKeySerializer<Double> simpleKeySerializer;
  byte[] stream = new byte[FIELD_SIZE];

  @Before
  public void beforeClass() {
    simpleKeySerializer = new OSimpleKeySerializer<Double>();
  }

  @Test
  public void testFieldSize() {
    Assert.assertEquals(simpleKeySerializer.getObjectSize(OBJECT), FIELD_SIZE);
  }

  @Test
  public void testSerializeInByteBuffer() {
    final int serializationOffset = 5;

    final ByteBuffer buffer = ByteBuffer.allocate(FIELD_SIZE + serializationOffset);
    buffer.position(serializationOffset);

    simpleKeySerializer.serializeInByteBufferObject(OBJECT, buffer);

    final int binarySize = buffer.position() - serializationOffset;
    Assert.assertEquals(binarySize, FIELD_SIZE);

    buffer.position(serializationOffset);
    Assert.assertEquals(simpleKeySerializer.getObjectSizeInByteBuffer(buffer), FIELD_SIZE);

    buffer.position(serializationOffset);
    Assert.assertEquals(simpleKeySerializer.deserializeFromByteBufferObject(buffer), OBJECT);

    Assert.assertEquals(buffer.position() - serializationOffset, FIELD_SIZE);
  }
}
