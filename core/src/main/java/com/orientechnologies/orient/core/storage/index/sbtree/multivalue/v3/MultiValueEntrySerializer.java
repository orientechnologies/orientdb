package com.orientechnologies.orient.core.storage.index.sbtree.multivalue.v3;

import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.common.serialization.types.OShortSerializer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWALChanges;

import java.nio.ByteBuffer;

public class MultiValueEntrySerializer implements OBinarySerializer<OMultiValueEntry> {
  public static final int                       ID       = 27;
  public static final MultiValueEntrySerializer INSTANCE = new MultiValueEntrySerializer();

  @Override
  public int getObjectSize(final OMultiValueEntry object, final Object... hints) {
    return 2 * OLongSerializer.LONG_SIZE + OShortSerializer.SHORT_SIZE;
  }

  @Override
  public int getObjectSize(final byte[] stream, final int startPosition) {
    return 2 * OLongSerializer.LONG_SIZE + OShortSerializer.SHORT_SIZE;

  }

  @Override
  public void serialize(final OMultiValueEntry object, final byte[] stream, final int startPosition, final Object... hints) {
    int pos = startPosition;
    OLongSerializer.INSTANCE.serialize(object.id, stream, pos);
    pos += OLongSerializer.LONG_SIZE;

    OShortSerializer.INSTANCE.serialize((short) object.clusterId, stream, pos);
    pos += OShortSerializer.SHORT_SIZE;

    OLongSerializer.INSTANCE.serialize(object.clusterPosition, stream, pos);
  }

  @Override
  public OMultiValueEntry deserialize(final byte[] stream, final int startPosition) {
    int pos = startPosition;
    final long id = OLongSerializer.INSTANCE.deserialize(stream, pos);
    pos += OLongSerializer.LONG_SIZE;

    final int clusterId = OShortSerializer.INSTANCE.deserialize(stream, pos);
    pos += OShortSerializer.SHORT_SIZE;

    final long clusterPosition = OLongSerializer.INSTANCE.deserialize(stream, pos);
    return new OMultiValueEntry(id, clusterId, clusterPosition);
  }

  @Override
  public byte getId() {
    return ID;
  }

  @Override
  public boolean isFixedLength() {
    return true;
  }

  @Override
  public int getFixedLength() {
    return 2 * OLongSerializer.LONG_SIZE + OShortSerializer.SHORT_SIZE;
  }

  @Override
  public void serializeNativeObject(final OMultiValueEntry object, final byte[] stream, final int startPosition,
      final Object... hints) {
    int pos = startPosition;
    OLongSerializer.INSTANCE.serializeNative(object.id, stream, pos);
    pos += OLongSerializer.LONG_SIZE;

    OShortSerializer.INSTANCE.serializeNative((short) object.clusterId, stream, pos);
    pos += OShortSerializer.SHORT_SIZE;

    OLongSerializer.INSTANCE.serializeNative(object.clusterPosition, stream, pos);
  }

  @Override
  public OMultiValueEntry deserializeNativeObject(final byte[] stream, final int startPosition) {
    int pos = startPosition;
    final long id = OLongSerializer.INSTANCE.deserializeNative(stream, pos);
    pos += OLongSerializer.LONG_SIZE;

    final int clusterId = OShortSerializer.INSTANCE.deserializeNative(stream, pos);
    pos += OShortSerializer.SHORT_SIZE;

    final long clusterPosition = OLongSerializer.INSTANCE.deserializeNative(stream, pos);
    return new OMultiValueEntry(id, clusterId, clusterPosition);
  }

  @Override
  public int getObjectSizeNative(final byte[] stream, final int startPosition) {
    return 2 * OLongSerializer.LONG_SIZE + OShortSerializer.SHORT_SIZE;
  }

  @Override
  public OMultiValueEntry preprocess(final OMultiValueEntry value, final Object... hints) {
    return value;
  }

  @Override
  public void serializeInByteBufferObject(final OMultiValueEntry object, final ByteBuffer buffer, final Object... hints) {
    buffer.putLong(object.id);
    buffer.putShort((short) object.clusterId);
    buffer.putLong(object.clusterPosition);
  }

  @Override
  public OMultiValueEntry deserializeFromByteBufferObject(final ByteBuffer buffer) {
    final long id = buffer.getLong();
    final int clusterId = buffer.getShort();
    final long clusterPosition = buffer.getLong();

    return new OMultiValueEntry(id, clusterId, clusterPosition);
  }

  @Override
  public int getObjectSizeInByteBuffer(final ByteBuffer buffer) {
    return 2 * OLongSerializer.LONG_SIZE + OShortSerializer.SHORT_SIZE;
  }

  @Override
  public OMultiValueEntry deserializeFromByteBufferObject(final ByteBuffer buffer, final OWALChanges walChanges, final int offset) {
    int position = offset;

    final long id = walChanges.getLongValue(buffer, position);
    position += OLongSerializer.LONG_SIZE;

    final int clusterId = walChanges.getShortValue(buffer, position);
    position += OShortSerializer.SHORT_SIZE;

    final long clusterPosition = walChanges.getLongValue(buffer, position);

    return new OMultiValueEntry(id, clusterId, clusterPosition);
  }

  @Override
  public int getObjectSizeInByteBuffer(final ByteBuffer buffer, final OWALChanges walChanges, final int offset) {
    return 2 * OLongSerializer.LONG_SIZE + OShortSerializer.SHORT_SIZE;
  }
}
