package com.orientechnologies.orient.core.serialization.serializer.binary.impl;

import static com.orientechnologies.orient.core.serialization.OBinaryProtocol.bytes2short;
import static com.orientechnologies.orient.core.serialization.OBinaryProtocol.short2bytes;

import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OShortSerializer;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWALChanges;
import java.nio.ByteBuffer;

public class OCompactedLinkSerializer implements OBinarySerializer<OIdentifiable> {
  public static final byte ID = 22;
  public static final OCompactedLinkSerializer INSTANCE = new OCompactedLinkSerializer();

  @Override
  public int getObjectSize(OIdentifiable rid, Object... hints) {
    final ORID r = rid.getIdentity();

    int size = OShortSerializer.SHORT_SIZE + OByteSerializer.BYTE_SIZE;

    final int zeroBits = Long.numberOfLeadingZeros(r.getClusterPosition());
    final int zerosTillFullByte = zeroBits & 7;
    final int numberSize = 8 - (zeroBits - zerosTillFullByte) / 8;
    size += numberSize;

    return size;
  }

  @Override
  public int getObjectSize(byte[] stream, int startPosition) {
    return stream[startPosition + OShortSerializer.SHORT_SIZE]
        + OByteSerializer.BYTE_SIZE
        + OShortSerializer.SHORT_SIZE;
  }

  @Override
  public void serialize(OIdentifiable rid, byte[] stream, int startPosition, Object... hints) {
    final ORID r = rid.getIdentity();

    final int zeroBits = Long.numberOfLeadingZeros(r.getClusterPosition());
    final int zerosTillFullByte = zeroBits & 7;
    final int numberSize = 8 - (zeroBits - zerosTillFullByte) / 8;

    short2bytes((short) r.getClusterId(), stream, startPosition);
    startPosition += OShortSerializer.SHORT_SIZE;

    stream[startPosition] = (byte) numberSize;
    startPosition++;

    long clusterPosition = r.getClusterPosition();
    for (int i = 0; i < numberSize; i++) {
      stream[startPosition + i] = (byte) ((0xFF) & clusterPosition);
      clusterPosition = clusterPosition >>> 8;
    }
  }

  @Override
  public OIdentifiable deserialize(byte[] stream, int startPosition) {
    final int cluster = bytes2short(stream, startPosition);
    startPosition += OShortSerializer.SHORT_SIZE;

    final int numberSize = stream[startPosition];
    startPosition++;

    long position = 0;
    for (int i = 0; i < numberSize; i++) {
      position = position | ((0xFF & stream[startPosition + i]) << (i * 8));
    }

    return new ORecordId(cluster, position);
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
      OIdentifiable rid, byte[] stream, int startPosition, Object... hints) {
    final ORID r = rid.getIdentity();

    OShortSerializer.INSTANCE.serializeNative((short) r.getClusterId(), stream, startPosition);
    startPosition += OShortSerializer.SHORT_SIZE;

    final int zeroBits = Long.numberOfLeadingZeros(r.getClusterPosition());
    final int zerosTillFullByte = zeroBits & 7;
    final int numberSize = 8 - (zeroBits - zerosTillFullByte) / 8;

    stream[startPosition] = (byte) numberSize;
    startPosition++;

    long clusterPosition = r.getClusterPosition();
    for (int i = 0; i < numberSize; i++) {
      stream[startPosition + i] = (byte) ((0xFF) & clusterPosition);
      clusterPosition = clusterPosition >>> 8;
    }
  }

  @Override
  public OIdentifiable deserializeNativeObject(byte[] stream, int startPosition) {
    final int cluster = OShortSerializer.INSTANCE.deserializeNativeObject(stream, startPosition);
    startPosition += OShortSerializer.SHORT_SIZE;

    final int numberSize = stream[startPosition];
    startPosition++;

    long position = 0;
    for (int i = 0; i < numberSize; i++) {
      position = position | ((0xFF & stream[startPosition + i]) << (i * 8));
    }

    return new ORecordId(cluster, position);
  }

  @Override
  public int getObjectSizeNative(byte[] stream, int startPosition) {
    return stream[startPosition + OShortSerializer.SHORT_SIZE]
        + OByteSerializer.BYTE_SIZE
        + OShortSerializer.SHORT_SIZE;
  }

  @Override
  public OIdentifiable preprocess(OIdentifiable value, Object... hints) {
    return value.getIdentity();
  }

  @Override
  public void serializeInByteBufferObject(OIdentifiable rid, ByteBuffer buffer, Object... hints) {
    final ORID r = rid.getIdentity();
    buffer.putShort((short) r.getClusterId());

    final int zeroBits = Long.numberOfLeadingZeros(r.getClusterPosition());
    final int zerosTillFullByte = zeroBits & 7;
    final int numberSize = 8 - (zeroBits - zerosTillFullByte) / 8;

    buffer.put((byte) numberSize);

    final byte[] number = new byte[numberSize];

    long clusterPosition = r.getClusterPosition();
    for (int i = 0; i < numberSize; i++) {
      number[i] = (byte) ((0xFF) & clusterPosition);
      clusterPosition = clusterPosition >>> 8;
    }

    buffer.put(number);
  }

  @Override
  public OIdentifiable deserializeFromByteBufferObject(ByteBuffer buffer) {
    final int cluster = buffer.getShort();

    final int numberSize = buffer.get();
    final byte[] number = new byte[numberSize];
    buffer.get(number);

    long position = 0;
    for (int i = 0; i < numberSize; i++) {
      position = position | ((0xFF & number[i]) << (i * 8));
    }

    return new ORecordId(cluster, position);
  }

  @Override
  public int getObjectSizeInByteBuffer(ByteBuffer buffer) {
    return buffer.get(buffer.position() + OShortSerializer.SHORT_SIZE)
        + OByteSerializer.BYTE_SIZE
        + OShortSerializer.SHORT_SIZE;
  }

  @Override
  public OIdentifiable deserializeFromByteBufferObject(
      ByteBuffer buffer, OWALChanges walChanges, int offset) {
    final int cluster = walChanges.getShortValue(buffer, offset);
    offset += OShortSerializer.SHORT_SIZE;

    final int numberSize = walChanges.getByteValue(buffer, offset);
    offset++;

    final byte[] number = walChanges.getBinaryValue(buffer, offset, numberSize);

    long position = 0;
    for (int i = 0; i < numberSize; i++) {
      position = position | ((0xFF & number[i]) << (i * 8));
    }

    return new ORecordId(cluster, position);
  }

  @Override
  public int getObjectSizeInByteBuffer(ByteBuffer buffer, OWALChanges walChanges, int offset) {
    return walChanges.getByteValue(buffer, offset + OShortSerializer.SHORT_SIZE)
        + OByteSerializer.BYTE_SIZE
        + OShortSerializer.SHORT_SIZE;
  }
}
