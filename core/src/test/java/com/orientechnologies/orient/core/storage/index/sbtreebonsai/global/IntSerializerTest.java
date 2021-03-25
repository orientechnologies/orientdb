package com.orientechnologies.orient.core.storage.index.sbtreebonsai.global;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWALChanges;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWALPageChangesPortion;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.junit.Assert;
import org.junit.Test;

public class IntSerializerTest {

  @Test
  public void serializeOneByteTest() {
    final int value = 0xE5;

    serializationTest(value);
  }

  @Test
  public void serializeTwoBytesTest() {
    final int value = 0xE4_E5;

    serializationTest(value);
  }

  @Test
  public void serializeThreeBytesTest() {
    final int value = 0xA5_E4_E5;

    serializationTest(value);
  }

  @Test
  public void serializeFourBytesTest() {
    final int value = 0xFE_A5_E4_E5;

    serializationTest(value);
  }

  private void serializationTest(int value) {
    final IntSerializer serializer = new IntSerializer();

    final int size = serializer.getObjectSize(value);
    final byte[] stream = new byte[size + 3];

    serializer.serialize(value, stream, 3, (Object[]) null);

    final int serializedSize = serializer.getObjectSize(stream, 3);
    Assert.assertEquals(size, serializedSize);

    final int deserialized = serializer.deserialize(stream, 3);

    Assert.assertEquals(value, deserialized);
  }

  @Test
  public void serializePrimitiveOneByteTest() {
    final int value = 0xE5;

    primitiveSerializationTest(value);
  }

  @Test
  public void serializePrimitiveTwoBytesTest() {
    final int value = 0xE4_E5;

    primitiveSerializationTest(value);
  }

  @Test
  public void serializePrimitiveThreeBytesTest() {
    final int value = 0xA5_E4_E5;

    primitiveSerializationTest(value);
  }

  @Test
  public void serializePrimitiveFourBytesTest() {
    final int value = 0xFE_A5_E4_E5;

    primitiveSerializationTest(value);
  }

  private void primitiveSerializationTest(int value) {
    final IntSerializer serializer = new IntSerializer();

    final int size = serializer.getObjectSize(value);
    final byte[] stream = new byte[size + 3];

    final int position = serializer.serializePrimitive(stream, 3, value);
    Assert.assertEquals(size + 3, position);

    final int serializedSize = serializer.getObjectSize(stream, 3);
    Assert.assertEquals(size, serializedSize);

    final int deserialized = serializer.deserialize(stream, 3);

    Assert.assertEquals(value, deserialized);
  }

  @Test
  public void serializeNativeOneByteTest() {
    final int value = 0xE5;

    nativeSerializationTest(value);
  }

  @Test
  public void serializeNativeTwoBytesTest() {
    final int value = 0xE4_E5;

    nativeSerializationTest(value);
  }

  @Test
  public void serializeNativeThreeBytesTest() {
    final int value = 0xA5_E4_E5;

    nativeSerializationTest(value);
  }

  @Test
  public void serializeNativeFourBytesTest() {
    final int value = 0xFE_A5_E4_E5;

    nativeSerializationTest(value);
  }

  private void nativeSerializationTest(int value) {
    final IntSerializer serializer = new IntSerializer();

    final int size = serializer.getObjectSize(value);
    final byte[] stream = new byte[size + 3];

    serializer.serializeNativeObject(value, stream, 3, (Object[]) null);

    final int serializedSize = serializer.getObjectSize(stream, 3);
    Assert.assertEquals(size, serializedSize);

    final int deserialized = serializer.deserializeNativeObject(stream, 3);

    Assert.assertEquals(value, deserialized);
  }

  @Test
  public void serializeByteBufferOneByteTest() {
    final int value = 0xE5;

    byteBufferSerializationTest(value);
  }

  @Test
  public void serializeByteBufferTwoBytesTest() {
    final int value = 0xE4_E5;

    byteBufferSerializationTest(value);
  }

  @Test
  public void serializeByteBufferThreeBytesTest() {
    final int value = 0xA5_E4_E5;

    byteBufferSerializationTest(value);
  }

  @Test
  public void serializeByteBufferFourBytesTest() {
    final int value = 0xFE_A5_E4_E5;

    byteBufferSerializationTest(value);
  }

  private void byteBufferSerializationTest(int value) {
    final IntSerializer serializer = new IntSerializer();

    final int size = serializer.getObjectSize(value);
    final ByteBuffer byteBuffer = ByteBuffer.allocate(size + 3);

    byteBuffer.position(3);

    serializer.serializeInByteBufferObject(value, byteBuffer, (Object[]) null);
    Assert.assertEquals(size + 3, byteBuffer.position());

    byteBuffer.position(3);
    final int serializedSize = serializer.getObjectSizeInByteBuffer(byteBuffer);
    Assert.assertEquals(size, serializedSize);

    byteBuffer.position(3);
    final int deserialized = serializer.deserializeFromByteBufferObject(byteBuffer);

    Assert.assertEquals(value, deserialized);
  }

  @Test
  public void serializeChangesOneByteTest() {
    final int value = 0xE5;

    changeTrackingSerializationTest(value);
  }

  @Test
  public void serializeChangesTwoBytesTest() {
    final int value = 0xE4_E5;

    changeTrackingSerializationTest(value);
  }

  @Test
  public void serializeChangesThreeBytesTest() {
    final int value = 0xA5_E4_E5;

    changeTrackingSerializationTest(value);
  }

  @Test
  public void serializeByteChangesFourBytesTest() {
    final int value = 0xFE_A5_E4_E5;

    changeTrackingSerializationTest(value);
  }

  private void changeTrackingSerializationTest(int value) {
    final IntSerializer serializer = new IntSerializer();
    final OWALChanges walChanges = new OWALPageChangesPortion();

    final int size = serializer.getObjectSize(value);
    final ByteBuffer byteBuffer =
        ByteBuffer.allocate(OGlobalConfiguration.DISK_CACHE_PAGE_SIZE.getValueAsInteger() * 1024)
            .order(ByteOrder.nativeOrder());
    final byte[] serializedValue = new byte[size];
    serializer.serializeNativeObject(value, serializedValue, 0);
    walChanges.setBinaryValue(byteBuffer, serializedValue, 3);

    final int serializedSize = serializer.getObjectSizeInByteBuffer(byteBuffer, walChanges, 3);
    Assert.assertEquals(size, serializedSize);

    final int deserialized = serializer.deserializeFromByteBufferObject(byteBuffer, walChanges, 3);

    Assert.assertEquals(value, deserialized);
  }
}
