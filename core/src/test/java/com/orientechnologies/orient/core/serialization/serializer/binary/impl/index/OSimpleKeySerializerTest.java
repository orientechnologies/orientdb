package com.orientechnologies.orient.core.serialization.serializer.binary.impl.index;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWALChanges;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWALPageChangesPortion;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class OSimpleKeySerializerTest {

  private static final int FIELD_SIZE = 9;
  private static final Double OBJECT = Math.PI;
  private OSimpleKeySerializer<Double> simpleKeySerializer;

  @Before
  public void beforeClass() {
    simpleKeySerializer = new OSimpleKeySerializer<>();
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

  @Test
  public void testSerializeInImmutableByteBufferPosition() {
    final int serializationOffset = 5;

    final ByteBuffer buffer = ByteBuffer.allocate(FIELD_SIZE + serializationOffset);
    buffer.position(serializationOffset);

    simpleKeySerializer.serializeInByteBufferObject(OBJECT, buffer);

    final int binarySize = buffer.position() - serializationOffset;
    Assert.assertEquals(binarySize, FIELD_SIZE);

    buffer.position(0);
    Assert.assertEquals(
        simpleKeySerializer.getObjectSizeInByteBuffer(serializationOffset, buffer), FIELD_SIZE);
    Assert.assertEquals(0, buffer.position());

    Assert.assertEquals(
        simpleKeySerializer.deserializeFromByteBufferObject(serializationOffset, buffer), OBJECT);
    Assert.assertEquals(0, buffer.position());
  }

  @Test
  public void testSerializeWALChanges() {
    final int serializationOffset = 5;

    final ByteBuffer buffer =
        ByteBuffer.allocateDirect(
                FIELD_SIZE + serializationOffset + OWALPageChangesPortion.PORTION_BYTES)
            .order(ByteOrder.nativeOrder());
    final byte[] data = new byte[FIELD_SIZE];

    simpleKeySerializer.serializeNativeObject(OBJECT, data, 0);
    final OWALChanges walChanges = new OWALPageChangesPortion();
    walChanges.setBinaryValue(buffer, data, serializationOffset);

    Assert.assertEquals(
        simpleKeySerializer.getObjectSizeInByteBuffer(buffer, walChanges, serializationOffset),
        FIELD_SIZE);
    Assert.assertEquals(
        simpleKeySerializer.deserializeFromByteBufferObject(
            buffer, walChanges, serializationOffset),
        OBJECT);
    Assert.assertEquals(0, buffer.position());
  }
}
