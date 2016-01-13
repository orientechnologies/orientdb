package com.orientechnologies.orient.core.serialization.serializer.binary.impl.index;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;

@Test
public class OSimpleKeySerializerTest {
  private static final int    FIELD_SIZE = 9;
  private static final Double OBJECT     = Math.PI;
  private OSimpleKeySerializer<Double> simpleKeySerializer;
  byte[] stream = new byte[FIELD_SIZE];

  @BeforeClass
  public void beforeClass() {
    simpleKeySerializer = new OSimpleKeySerializer<Double>();
  }

  public void testFieldSize() {
    Assert.assertEquals(simpleKeySerializer.getObjectSize(OBJECT), FIELD_SIZE);
  }

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
