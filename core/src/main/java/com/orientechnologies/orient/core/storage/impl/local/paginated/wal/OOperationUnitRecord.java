package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;

/**
 * @author Andrey Lomakin
 * @since 30.05.13
 */
public abstract class OOperationUnitRecord implements OWALRecord {
  private OLogSequenceNumber prevLsn;

  protected OOperationUnitRecord() {
  }

  protected OOperationUnitRecord(OLogSequenceNumber prevLsn) {
    this.prevLsn = prevLsn;
  }

  public OLogSequenceNumber getPrevLsn() {
    return prevLsn;
  }

  @Override
  public int toStream(byte[] content, int offset) {
    if (prevLsn == null) {
      content[offset] = 0;
      offset++;

      return offset;
    }

    content[offset] = 1;
    offset++;

    OIntegerSerializer.INSTANCE.serializeNative(prevLsn.getSegment(), content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    OLongSerializer.INSTANCE.serializeNative(prevLsn.getPosition(), content, offset);
    offset += OLongSerializer.LONG_SIZE;

    return offset;
  }

  @Override
  public int fromStream(byte[] content, int offset) {
    if (content[offset] == 0) {
      offset++;
      return offset;
    }

    offset++;

    int segment = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    long position = OLongSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OLongSerializer.LONG_SIZE;

    prevLsn = new OLogSequenceNumber(segment, position);

    return offset;
  }

  @Override
  public int serializedSize() {
    if (prevLsn == null)
      return OByteSerializer.BYTE_SIZE;

    return OIntegerSerializer.INT_SIZE + OLongSerializer.LONG_SIZE + OByteSerializer.BYTE_SIZE;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    OOperationUnitRecord that = (OOperationUnitRecord) o;

    if (prevLsn != null ? !prevLsn.equals(that.prevLsn) : that.prevLsn != null)
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    return prevLsn != null ? prevLsn.hashCode() : 0;
  }

  @Override
  public String toString() {
    return "OOperationUnitRecord{" + "prevLsn=" + prevLsn + '}';
  }
}
