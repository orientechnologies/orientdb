package com.orientechnologies.common.serialization.types;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWALChanges;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

public class OUTF8Serializer implements OBinarySerializer<String> {
  public static final OUTF8Serializer INSTANCE = new OUTF8Serializer();
  public static final byte            ID       = 25;

  @Override
  public int getObjectSize(String object, Object... hints) {
    final byte[] encoded = object.getBytes(Charset.forName("UTF-8"));
    return OIntegerSerializer.INT_SIZE + encoded.length;
  }

  @Override
  public int getObjectSize(byte[] stream, int startPosition) {
    return OIntegerSerializer.INSTANCE.deserialize(stream, startPosition) + OIntegerSerializer.INT_SIZE;
  }

  @Override
  public void serialize(String object, byte[] stream, int startPosition, Object... hints) {
    final byte[] encoded = object.getBytes(Charset.forName("UTF-8"));
    OIntegerSerializer.INSTANCE.serialize(encoded.length, stream, startPosition);
    startPosition += OIntegerSerializer.INT_SIZE;

    System.arraycopy(encoded, 0, stream, startPosition, encoded.length);
  }

  @Override
  public String deserialize(byte[] stream, int startPosition) {
    final int encodedSize = OIntegerSerializer.INSTANCE.deserialize(stream, startPosition);
    startPosition += OIntegerSerializer.INT_SIZE;

    final byte[] encoded = new byte[encodedSize];
    System.arraycopy(stream, startPosition, encoded, 0, encodedSize);
    try {
      return new String(encoded, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new IllegalStateException(e);
    }
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
  public void serializeNativeObject(String object, byte[] stream, int startPosition, Object... hints) {
    final byte[] encoded = object.getBytes(Charset.forName("UTF-8"));
    OIntegerSerializer.INSTANCE.serializeNative(encoded.length, stream, startPosition);
    startPosition += OIntegerSerializer.INT_SIZE;

    System.arraycopy(encoded, 0, stream, startPosition, encoded.length);
  }

  @Override
  public String deserializeNativeObject(byte[] stream, int startPosition) {
    final int encodedSize = OIntegerSerializer.INSTANCE.deserializeNative(stream, startPosition);
    startPosition += OIntegerSerializer.INT_SIZE;

    final byte[] encoded = new byte[encodedSize];
    System.arraycopy(stream, startPosition, encoded, 0, encodedSize);
    try {
      return new String(encoded, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public int getObjectSizeNative(byte[] stream, int startPosition) {
    return OIntegerSerializer.INSTANCE.deserializeNative(stream, startPosition) + OIntegerSerializer.INT_SIZE;
  }

  @Override
  public String preprocess(String value, Object... hints) {
    return value;
  }

  @Override
  public void serializeInByteBufferObject(String object, ByteBuffer buffer, Object... hints) {
    final byte[] encoded = object.getBytes(Charset.forName("UTF-8"));
    buffer.putInt(encoded.length);

    buffer.put(encoded);
  }

  @Override
  public String deserializeFromByteBufferObject(ByteBuffer buffer) {
    final int encodedSize = buffer.getInt();

    final byte[] encoded = new byte[encodedSize];
    buffer.get(encoded);
    try {
      return new String(encoded, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public int getObjectSizeInByteBuffer(ByteBuffer buffer) {
    return buffer.getInt() + OIntegerSerializer.INT_SIZE;
  }

  @Override
  public String deserializeFromByteBufferObject(ByteBuffer buffer, OWALChanges walChanges, int offset) {
    final int encodedSize = walChanges.getIntValue(buffer, offset);
    offset += OIntegerSerializer.INT_SIZE;

    final byte[] encoded = walChanges.getBinaryValue(buffer, offset, encodedSize);
    try {
      return new String(encoded, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public int getObjectSizeInByteBuffer(ByteBuffer buffer, OWALChanges walChanges, int offset) {
    return walChanges.getIntValue(buffer, offset) + OIntegerSerializer.INT_SIZE;
  }

  @Override
  public byte[] serializeNativeAsWhole(String object, Object... hints) {
    final byte[] encoded = object.getBytes(Charset.forName("UTF-8"));
    final byte[] result = new byte[encoded.length + OIntegerSerializer.INT_SIZE];

    OIntegerSerializer.INSTANCE.serializeNative(encoded.length, result, 0);
    System.arraycopy(encoded, 0, result, OIntegerSerializer.INT_SIZE, encoded.length);
    return result;
  }
}
