package com.orientechnologies.common.serialization.types;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWALChanges;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class OUTF8Serializer implements OBinarySerializer<String> {
  private static final int INT_MASK = 0xFFFF;

  public static final OUTF8Serializer INSTANCE = new OUTF8Serializer();
  public static final byte ID = 25;

  @Override
  public int getObjectSize(String object, Object... hints) {
    final byte[] encoded = object.getBytes(StandardCharsets.UTF_8);
    return OShortSerializer.SHORT_SIZE + encoded.length;
  }

  @Override
  public int getObjectSize(byte[] stream, int startPosition) {
    return (OShortSerializer.INSTANCE.deserialize(stream, startPosition) & INT_MASK)
        + OShortSerializer.SHORT_SIZE;
  }

  @Override
  public void serialize(String object, byte[] stream, int startPosition, Object... hints) {
    final byte[] encoded = object.getBytes(StandardCharsets.UTF_8);
    OShortSerializer.INSTANCE.serialize((short) encoded.length, stream, startPosition);
    startPosition += OShortSerializer.SHORT_SIZE;

    System.arraycopy(encoded, 0, stream, startPosition, encoded.length);
  }

  @Override
  public String deserialize(byte[] stream, int startPosition) {
    final int encodedSize = OShortSerializer.INSTANCE.deserialize(stream, startPosition) & INT_MASK;
    startPosition += OShortSerializer.SHORT_SIZE;

    final byte[] encoded = new byte[encodedSize];
    System.arraycopy(stream, startPosition, encoded, 0, encodedSize);
    return new String(encoded, StandardCharsets.UTF_8);
  }

  @Override
  public byte getId() {
    return ID;
  }

  @Override
  public boolean isFixedLength() {
    return false;
  }

  @Override
  public int getFixedLength() {
    return 0;
  }

  @Override
  public void serializeNativeObject(
      String object, byte[] stream, int startPosition, Object... hints) {
    final byte[] encoded = object.getBytes(StandardCharsets.UTF_8);
    OShortSerializer.INSTANCE.serializeNative((short) encoded.length, stream, startPosition);
    startPosition += OShortSerializer.SHORT_SIZE;

    System.arraycopy(encoded, 0, stream, startPosition, encoded.length);
  }

  @Override
  public String deserializeNativeObject(byte[] stream, int startPosition) {
    final int encodedSize =
        OShortSerializer.INSTANCE.deserializeNative(stream, startPosition) & INT_MASK;
    startPosition += OShortSerializer.SHORT_SIZE;

    final byte[] encoded = new byte[encodedSize];
    System.arraycopy(stream, startPosition, encoded, 0, encodedSize);
    return new String(encoded, StandardCharsets.UTF_8);
  }

  @Override
  public int getObjectSizeNative(byte[] stream, int startPosition) {
    return (OShortSerializer.INSTANCE.deserializeNative(stream, startPosition) & INT_MASK)
        + OShortSerializer.SHORT_SIZE;
  }

  @Override
  public String preprocess(String value, Object... hints) {
    return value;
  }

  @Override
  public void serializeInByteBufferObject(String object, ByteBuffer buffer, Object... hints) {
    final byte[] encoded = object.getBytes(Charset.forName("UTF-8"));
    buffer.putShort((short) encoded.length);

    buffer.put(encoded);
  }

  @Override
  public String deserializeFromByteBufferObject(ByteBuffer buffer) {
    final int encodedSize = buffer.getShort() & INT_MASK;

    final byte[] encoded = new byte[encodedSize];
    buffer.get(encoded);
    return new String(encoded, StandardCharsets.UTF_8);
  }

  @Override
  public int getObjectSizeInByteBuffer(ByteBuffer buffer) {
    return (buffer.getShort() & INT_MASK) + OShortSerializer.SHORT_SIZE;
  }

  @Override
  public String deserializeFromByteBufferObject(
      ByteBuffer buffer, OWALChanges walChanges, int offset) {
    final int encodedSize = walChanges.getShortValue(buffer, offset) & INT_MASK;
    offset += OShortSerializer.SHORT_SIZE;

    final byte[] encoded = walChanges.getBinaryValue(buffer, offset, encodedSize);
    return new String(encoded, StandardCharsets.UTF_8);
  }

  @Override
  public int getObjectSizeInByteBuffer(ByteBuffer buffer, OWALChanges walChanges, int offset) {
    return (walChanges.getShortValue(buffer, offset) & INT_MASK) + OShortSerializer.SHORT_SIZE;
  }

  @Override
  public byte[] serializeNativeAsWhole(String object, Object... hints) {
    final byte[] encoded = object.getBytes(StandardCharsets.UTF_8);
    final byte[] result = new byte[encoded.length + OShortSerializer.SHORT_SIZE];

    OShortSerializer.INSTANCE.serializeNative((short) encoded.length, result, 0);
    System.arraycopy(encoded, 0, result, OShortSerializer.SHORT_SIZE, encoded.length);
    return result;
  }
}
