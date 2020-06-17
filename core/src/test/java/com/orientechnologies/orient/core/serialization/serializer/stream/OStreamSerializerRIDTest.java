package com.orientechnologies.orient.core.serialization.serializer.stream;

import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.common.serialization.types.OShortSerializer;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWALChanges;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWALChangesTree;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.junit.Assert;
import org.junit.Before;

public class OStreamSerializerRIDTest {
  private static final int FIELD_SIZE = OShortSerializer.SHORT_SIZE + OLongSerializer.LONG_SIZE;
  private static final int clusterId = 5;
  private static final long position = 100500L;
  private ORecordId OBJECT;
  private OStreamSerializerRID streamSerializerRID;

  @Before
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

  public void testsSerializeWALChanges() {
    final int serializationOffset = 5;

    final ByteBuffer buffer =
        ByteBuffer.allocateDirect(FIELD_SIZE + serializationOffset).order(ByteOrder.nativeOrder());
    final byte[] data = new byte[FIELD_SIZE];
    streamSerializerRID.serializeNativeObject(OBJECT, data, 0);

    final OWALChanges walChanges = new OWALChangesTree();
    walChanges.setBinaryValue(buffer, data, serializationOffset);

    Assert.assertEquals(
        streamSerializerRID.getObjectSizeInByteBuffer(buffer, walChanges, serializationOffset),
        FIELD_SIZE);
    Assert.assertEquals(
        streamSerializerRID.deserializeFromByteBufferObject(
            buffer, walChanges, serializationOffset),
        OBJECT);
  }
}
