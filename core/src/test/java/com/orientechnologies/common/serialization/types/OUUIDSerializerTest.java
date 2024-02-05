package com.orientechnologies.common.serialization.types;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWALChanges;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWALPageChangesPortion;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.UUID;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class OUUIDSerializerTest {

  private static final int FIELD_SIZE = 16;
  private static final UUID OBJECT = UUID.randomUUID();
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

  @Test
  public void testSerializationInImmutableByteBufferPosition() {
    final int serializationOffset = 5;
    final ByteBuffer buffer = ByteBuffer.allocate(FIELD_SIZE + serializationOffset);
    buffer.position(serializationOffset);

    uuidSerializer.serializeInByteBufferObject(OBJECT, buffer);

    final int binarySize = buffer.position() - serializationOffset;
    Assert.assertEquals(binarySize, FIELD_SIZE);

    buffer.position(0);
    Assert.assertEquals(
        uuidSerializer.getObjectSizeInByteBuffer(serializationOffset, buffer), FIELD_SIZE);
    Assert.assertEquals(0, buffer.position());

    Assert.assertEquals(
        uuidSerializer.deserializeFromByteBufferObject(serializationOffset, buffer), OBJECT);
    Assert.assertEquals(0, buffer.position());
  }

  @Test
  public void testsSerializationWALChanges() {
    final int serializationOffset = 5;

    final ByteBuffer buffer =
        ByteBuffer.allocateDirect(
                FIELD_SIZE + serializationOffset + OWALPageChangesPortion.PORTION_BYTES)
            .order(ByteOrder.nativeOrder());
    final byte[] data = new byte[FIELD_SIZE];

    uuidSerializer.serializeNativeObject(OBJECT, data, 0);

    OWALChanges walChanges = new OWALPageChangesPortion();
    walChanges.setBinaryValue(buffer, data, serializationOffset);

    Assert.assertEquals(
        uuidSerializer.getObjectSizeInByteBuffer(buffer, walChanges, serializationOffset),
        FIELD_SIZE);
    Assert.assertEquals(
        uuidSerializer.deserializeFromByteBufferObject(buffer, walChanges, serializationOffset),
        OBJECT);

    Assert.assertEquals(0, buffer.position());
  }
}
