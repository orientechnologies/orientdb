package com.orientechnologies.orient.core.serialization.serializer.stream;

import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.common.serialization.types.OShortSerializer;
import com.orientechnologies.orient.core.id.ORecordId;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;

@Test
public class OStreamSerializerRIDTest {
  private static final int  FIELD_SIZE = OShortSerializer.SHORT_SIZE + OLongSerializer.LONG_SIZE;
  private static final int  clusterId  = 5;
  private static final long position   = 100500L;
  private ORecordId            OBJECT;
  private OStreamSerializerRID streamSerializerRID;

  @BeforeClass
  public void beforeClass() {
    OBJECT = new ORecordId(clusterId, position);
    streamSerializerRID = new OStreamSerializerRID();
  }

  public void testFieldSize() {
    Assert.assertEquals(streamSerializerRID.getObjectSize(OBJECT), FIELD_SIZE);
  }

  public void testSerializeInByteBuffer() {
    final int serializationOffset = 5;

    final ByteBuffer buffer = ByteBuffer.allocate(FIELD_SIZE + serializationOffset);

    buffer.position(serializationOffset);
    streamSerializerRID.serializeInByteBufferObject(OBJECT, buffer);

    final int binarySize = buffer.position() - serializationOffset;
    Assert.assertEquals(binarySize, FIELD_SIZE);

    buffer.position(serializationOffset);
    Assert.assertEquals(streamSerializerRID.getObjectSizeInByteBuffer(buffer), FIELD_SIZE);

    buffer.position(serializationOffset);
    Assert.assertEquals(streamSerializerRID.deserializeFromByteBufferObject(buffer), OBJECT);

    Assert.assertEquals(buffer.position() - serializationOffset, FIELD_SIZE);
  }
}
