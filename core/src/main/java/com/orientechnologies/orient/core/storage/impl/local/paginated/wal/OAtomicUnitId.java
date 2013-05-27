package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

import java.util.UUID;

import com.orientechnologies.common.serialization.types.OLongSerializer;

/**
 * @author Andrey Lomakin
 * @since 24.05.13
 */
public final class OAtomicUnitId implements Comparable<OAtomicUnitId> {

  public static final int SERIALIZED_SIZE = 2 * OLongSerializer.LONG_SIZE;

  private UUID            uuid;

  public static OAtomicUnitId generateId() {
    return new OAtomicUnitId(UUID.randomUUID());
  }

  public OAtomicUnitId() {
  }

  private OAtomicUnitId(UUID uuid) {
    this.uuid = uuid;
  }

  @Override
  public int compareTo(OAtomicUnitId other) {
    return uuid.compareTo(other.uuid);
  }

  public void toStream(byte[] stream, int pos) {
    OLongSerializer.INSTANCE.serializeNative(uuid.getMostSignificantBits(), stream, pos);
    pos += OLongSerializer.LONG_SIZE;

    OLongSerializer.INSTANCE.serializeNative(uuid.getLeastSignificantBits(), stream, pos);
  }

  public void fromStream(byte[] stream, int pos) {
    long most = OLongSerializer.INSTANCE.deserializeNative(stream, pos);
    pos += OLongSerializer.LONG_SIZE;

    long least = OLongSerializer.INSTANCE.deserializeNative(stream, pos);

    uuid = new UUID(most, least);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    OAtomicUnitId that = (OAtomicUnitId) o;

    if (!uuid.equals(that.uuid))
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    return uuid.hashCode();
  }
}
