package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

import java.util.UUID;

import com.orientechnologies.common.serialization.types.OLongSerializer;

/**
 * @author Andrey Lomakin
 * @since 06.06.13
 */
public class OOperationUnitId {
  public static final int SERIALIZED_SIZE = 2 * OLongSerializer.LONG_SIZE;

  private UUID            uuid;

  public static OOperationUnitId generateId() {
    OOperationUnitId operationUnitId = new OOperationUnitId();
    operationUnitId.uuid = UUID.randomUUID();

    return operationUnitId;
  }

  public OOperationUnitId() {
  }

  public OOperationUnitId(UUID uuid) {
    this.uuid = uuid;
  }

  public int toStream(byte[] content, int offset) {
    OLongSerializer.INSTANCE.serializeNative(uuid.getMostSignificantBits(), content, offset);
    offset += OLongSerializer.LONG_SIZE;

    OLongSerializer.INSTANCE.serializeNative(uuid.getLeastSignificantBits(), content, offset);
    offset += OLongSerializer.LONG_SIZE;

    return offset;
  }

  public int fromStream(byte[] content, int offset) {
    long most = OLongSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OLongSerializer.LONG_SIZE;

    long least = OLongSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OLongSerializer.LONG_SIZE;

    uuid = new UUID(most, least);

    return offset;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    OOperationUnitId that = (OOperationUnitId) o;

    if (!uuid.equals(that.uuid))
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    return uuid.hashCode();
  }

  @Override
  public String toString() {
    return "OOperationUnitId{" + "uuid=" + uuid + "} ";
  }
}
