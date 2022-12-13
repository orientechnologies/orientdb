package com.orientechnologies.orient.core.serialization.serializer.binary.impl.index;

import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.common.serialization.types.OShortSerializer;
import com.orientechnologies.common.serialization.types.OUTF8Serializer;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OCompositeKey;
import com.orientechnologies.orient.core.index.OIndexException;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OCompactedLinkSerializer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWALChanges;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

public final class CompositeKeySerializer implements OBinarySerializer<OCompositeKey> {
  public int getObjectSize(OCompositeKey compositeKey, Object... hints) {
    final OType[] types = (OType[]) hints;
    final List<Object> keys = compositeKey.getKeys();

    int size = 0;
    for (int i = 0; i < keys.size(); i++) {
      final OType type = types[i];
      final Object key = keys.get(i);

      size += OByteSerializer.BYTE_SIZE;
      if (key != null) {
        size += sizeOfKey(type, key);
      }
    }

    return size + 2 * OIntegerSerializer.INT_SIZE;
  }

  private static int sizeOfKey(final OType type, final Object key) {
    switch (type) {
      case BOOLEAN:
      case BYTE:
        return 1;
      case DATE:
      case DATETIME:
      case DOUBLE:
      case LONG:
        return OLongSerializer.LONG_SIZE;
      case BINARY:
        return ((byte[]) key).length + OIntegerSerializer.INT_SIZE;
      case DECIMAL:
        final BigDecimal bigDecimal = ((BigDecimal) key);
        return 2 * OIntegerSerializer.INT_SIZE + bigDecimal.unscaledValue().toByteArray().length;
      case FLOAT:
      case INTEGER:
        return OIntegerSerializer.INT_SIZE;
      case LINK:
        return OCompactedLinkSerializer.INSTANCE.getObjectSize((ORID) key);
      case SHORT:
        return OShortSerializer.SHORT_SIZE;
      case STRING:
        return OUTF8Serializer.INSTANCE.getObjectSize((String) key);
      default:
        throw new OIndexException("Unsupported key type " + type);
    }
  }

  public void serialize(
      OCompositeKey compositeKey, byte[] stream, int startPosition, Object... hints) {
    final ByteBuffer buffer = ByteBuffer.wrap(stream);
    buffer.position(startPosition);

    serialize(compositeKey, buffer, (OType[]) hints);
  }

  private static void serialize(OCompositeKey compositeKey, ByteBuffer buffer, OType[] types) {
    final List<Object> keys = compositeKey.getKeys();
    final int startPosition = buffer.position();
    buffer.position(startPosition + OIntegerSerializer.INT_SIZE);

    buffer.putInt(types.length);

    for (int i = 0; i < types.length; i++) {
      final OType type = types[i];
      final Object key = keys.get(i);

      if (key == null) {
        buffer.put((byte) (-(type.getId() + 1)));
      } else {
        buffer.put((byte) type.getId());
        serializeKeyToByteBuffer(buffer, type, key);
      }
    }

    buffer.putInt(startPosition, buffer.position() - startPosition);
  }

  private static void serializeKeyToByteBuffer(
      final ByteBuffer buffer, final OType type, final Object key) {
    switch (type) {
      case BINARY:
        final byte[] array = (byte[]) key;
        buffer.putInt(array.length);
        buffer.put(array);
        return;
      case BOOLEAN:
        buffer.put((Boolean) key ? (byte) 1 : 0);
        return;
      case BYTE:
        buffer.put((Byte) key);
        return;
      case DATE:
      case DATETIME:
        buffer.putLong(((Date) key).getTime());
        return;
      case DECIMAL:
        final BigDecimal decimal = (BigDecimal) key;
        buffer.putInt(decimal.scale());
        final byte[] unscaledValue = decimal.unscaledValue().toByteArray();
        buffer.putInt(unscaledValue.length);
        buffer.put(unscaledValue);
        return;
      case DOUBLE:
        buffer.putLong(Double.doubleToLongBits((Double) key));
        return;
      case FLOAT:
        buffer.putInt(Float.floatToIntBits((Float) key));
        return;
      case INTEGER:
        buffer.putInt((Integer) key);
        return;
      case LINK:
        OCompactedLinkSerializer.INSTANCE.serializeInByteBufferObject((ORID) key, buffer);
        return;
      case LONG:
        buffer.putLong((Long) key);
        return;
      case SHORT:
        buffer.putShort((Short) key);
        return;
      case STRING:
        OUTF8Serializer.INSTANCE.serializeInByteBufferObject((String) key, buffer);
        return;
      default:
        throw new OIndexException("Unsupported index type " + type);
    }
  }

  public OCompositeKey deserialize(byte[] stream, int startPosition) {
    final ByteBuffer buffer = ByteBuffer.wrap(stream);
    buffer.position(startPosition);

    return deserialize(buffer);
  }

  private static OCompositeKey deserialize(ByteBuffer buffer) {
    buffer.position(buffer.position() + OIntegerSerializer.INT_SIZE);

    final int keyLen = buffer.getInt();
    OCompositeKey keys = new OCompositeKey(keyLen);
    for (int i = 0; i < keyLen; i++) {
      final byte typeId = buffer.get();
      if (typeId < 0) {
        keys.addKey(null);
      } else {
        final OType type = OType.getById(typeId);
        assert type != null;
        keys.addKey(deserializeKeyFromByteBuffer(buffer, type));
      }
    }

    return keys;
  }

  private static Object deserializeKeyFromByteBuffer(final ByteBuffer buffer, final OType type) {
    switch (type) {
      case BINARY:
        final int len = buffer.getInt();
        final byte[] array = new byte[len];
        buffer.get(array);
        return array;
      case BOOLEAN:
        return buffer.get() > 0;
      case BYTE:
        return buffer.get();
      case DATE:
      case DATETIME:
        return new Date(buffer.getLong());
      case DECIMAL:
        final int scale = buffer.getInt();
        final int unscaledValueLen = buffer.getInt();
        final byte[] unscaledValue = new byte[unscaledValueLen];
        buffer.get(unscaledValue);
        return new BigDecimal(new BigInteger(unscaledValue), scale);
      case DOUBLE:
        return Double.longBitsToDouble(buffer.getLong());
      case FLOAT:
        return Float.intBitsToFloat(buffer.getInt());
      case INTEGER:
        return buffer.getInt();
      case LINK:
        return OCompactedLinkSerializer.INSTANCE.deserializeFromByteBufferObject(buffer);
      case LONG:
        return buffer.getLong();
      case SHORT:
        return buffer.getShort();
      case STRING:
        return OUTF8Serializer.INSTANCE.deserializeFromByteBufferObject(buffer);
      default:
        throw new OIndexException("Unsupported index type " + type);
    }
  }

  private static Object deserializeKeyFromByteBuffer(
      final int offset, final ByteBuffer buffer, final OType type, final OWALChanges walChanges) {
    switch (type) {
      case BINARY:
        final int len = walChanges.getIntValue(buffer, offset);
        return walChanges.getBinaryValue(buffer, offset + OIntegerSerializer.INT_SIZE, len);
      case BOOLEAN:
        return walChanges.getByteValue(buffer, offset) > 0;
      case BYTE:
        return walChanges.getByteValue(buffer, offset);
      case DATE:
      case DATETIME:
        return new Date(walChanges.getLongValue(buffer, offset));
      case DECIMAL:
        final int scale = walChanges.getIntValue(buffer, offset);
        final int unscaledValueLen =
            walChanges.getIntValue(buffer, offset + OIntegerSerializer.INT_SIZE);
        final byte[] unscaledValue =
            walChanges.getBinaryValue(
                buffer, offset + 2 * OIntegerSerializer.INT_SIZE, unscaledValueLen);
        return new BigDecimal(new BigInteger(unscaledValue), scale);
      case DOUBLE:
        return Double.longBitsToDouble(walChanges.getLongValue(buffer, offset));
      case FLOAT:
        return Float.intBitsToFloat(walChanges.getIntValue(buffer, offset));
      case INTEGER:
        return walChanges.getIntValue(buffer, offset);
      case LINK:
        return OCompactedLinkSerializer.INSTANCE.deserializeFromByteBufferObject(
            buffer, walChanges, offset);
      case LONG:
        return walChanges.getLongValue(buffer, offset);
      case SHORT:
        return walChanges.getShortValue(buffer, offset);
      case STRING:
        return OUTF8Serializer.INSTANCE.deserializeFromByteBufferObject(buffer, walChanges, offset);
      default:
        throw new OIndexException("Unsupported index type " + type);
    }
  }

  public int getObjectSize(byte[] stream, int startPosition) {
    //noinspection RedundantCast
    return ((ByteBuffer) ByteBuffer.wrap(stream).position(startPosition)).getInt();
  }

  public byte getId() {
    return -1;
  }

  public int getObjectSizeNative(byte[] stream, int startPosition) {
    //noinspection RedundantCast
    return ((ByteBuffer)
            ByteBuffer.wrap(stream).order(ByteOrder.nativeOrder()).position(startPosition))
        .getInt();
  }

  public void serializeNativeObject(
      OCompositeKey compositeKey, byte[] stream, int startPosition, Object... hints) {
    @SuppressWarnings("RedundantCast")
    final ByteBuffer buffer =
        (ByteBuffer) ByteBuffer.wrap(stream).order(ByteOrder.nativeOrder()).position(startPosition);
    serialize(compositeKey, buffer, (OType[]) hints);
  }

  public OCompositeKey deserializeNativeObject(byte[] stream, int startPosition) {
    @SuppressWarnings("RedundantCast")
    final ByteBuffer buffer =
        (ByteBuffer) ByteBuffer.wrap(stream).order(ByteOrder.nativeOrder()).position(startPosition);
    return deserialize(buffer);
  }

  public boolean isFixedLength() {
    return false;
  }

  public int getFixedLength() {
    return 0;
  }

  @Override
  public OCompositeKey preprocess(OCompositeKey value, Object... hints) {
    if (value == null) {
      return null;
    }

    final OType[] types = (OType[]) hints;
    final List<Object> keys = value.getKeys();

    boolean preprocess = false;
    for (int i = 0; i < keys.size(); i++) {
      final OType type = types[i];

      if (type == OType.DATE || (type == OType.LINK && !(keys.get(i) instanceof ORID))) {
        preprocess = true;
        break;
      }
    }

    if (!preprocess) {
      return value;
    }

    final OCompositeKey compositeKey = new OCompositeKey();

    for (int i = 0; i < keys.size(); i++) {
      final Object key = keys.get(i);
      final OType type = types[i];
      if (key != null) {
        if (type == OType.DATE) {
          final Calendar calendar = Calendar.getInstance();
          calendar.setTime((Date) key);
          calendar.set(Calendar.HOUR_OF_DAY, 0);
          calendar.set(Calendar.MINUTE, 0);
          calendar.set(Calendar.SECOND, 0);
          calendar.set(Calendar.MILLISECOND, 0);

          compositeKey.addKey(calendar.getTime());
        } else if (type == OType.LINK) {
          compositeKey.addKey(((OIdentifiable) key).getIdentity());
        } else {
          compositeKey.addKey(key);
        }
      } else {
        compositeKey.addKey(null);
      }
    }

    return compositeKey;
  }

  /** {@inheritDoc} */
  @Override
  public void serializeInByteBufferObject(
      OCompositeKey object, ByteBuffer buffer, Object... hints) {
    serialize(object, buffer, (OType[]) hints);
  }

  /** {@inheritDoc} */
  @Override
  public OCompositeKey deserializeFromByteBufferObject(ByteBuffer buffer) {
    return deserialize(buffer);
  }

  /** {@inheritDoc} */
  @Override
  public int getObjectSizeInByteBuffer(ByteBuffer buffer) {
    return buffer.getInt();
  }

  /** {@inheritDoc} */
  @Override
  public OCompositeKey deserializeFromByteBufferObject(
      ByteBuffer buffer, OWALChanges walChanges, int offset) {
    offset += OIntegerSerializer.INT_SIZE;

    final int keyLen = walChanges.getIntValue(buffer, offset);
    offset += OIntegerSerializer.INT_SIZE;

    final List<Object> keys = new ArrayList<>(keyLen);
    for (int i = 0; i < keyLen; i++) {
      final byte typeId = walChanges.getByteValue(buffer, offset);
      offset += OByteSerializer.BYTE_SIZE;

      if (typeId < 0) {
        keys.add(null);
      } else {
        final OType type = OType.getById(typeId);
        assert type != null;
        final Object key = deserializeKeyFromByteBuffer(offset, buffer, type, walChanges);
        offset += sizeOfKey(type, key);
        keys.add(key);
      }
    }

    return new OCompositeKey(keys);
  }

  /** {@inheritDoc} */
  @Override
  public int getObjectSizeInByteBuffer(ByteBuffer buffer, OWALChanges walChanges, int offset) {
    return walChanges.getIntValue(buffer, offset);
  }
}
