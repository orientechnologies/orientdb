package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;

/**
 * @author Andrey Lomakin
 * @since 14.05.13
 */
public abstract class OAbstractCheckPointStartRecord implements OWALRecord {
  private OLogSequenceNumber previousCheckpoint;

  protected OAbstractCheckPointStartRecord() {
  }

  protected OAbstractCheckPointStartRecord(OLogSequenceNumber previousCheckpoint) {
    this.previousCheckpoint = previousCheckpoint;
  }

  public OLogSequenceNumber getPreviousCheckpoint() {
    return previousCheckpoint;
  }

  @Override
  public int toStream(byte[] content, int offset) {
    if (previousCheckpoint == null) {
      content[offset] = 0;
      offset++;
      return offset;
    }

    content[offset] = 1;
    offset++;

    OIntegerSerializer.INSTANCE.serializeNative(previousCheckpoint.getSegment(), content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    OLongSerializer.INSTANCE.serializeNative(previousCheckpoint.getPosition(), content, offset);
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

    previousCheckpoint = new OLogSequenceNumber(segment, position);

    return offset;
  }

  @Override
  public int serializedSize() {
    if (previousCheckpoint == null)
      return 1;

    return OIntegerSerializer.INT_SIZE + OLongSerializer.LONG_SIZE + 1;
  }

  @Override
  public boolean isUpdateMasterRecord() {
    return true;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    OAbstractCheckPointStartRecord that = (OAbstractCheckPointStartRecord) o;

    if (previousCheckpoint != null ? !previousCheckpoint.equals(that.previousCheckpoint) : that.previousCheckpoint != null)
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    return previousCheckpoint != null ? previousCheckpoint.hashCode() : 0;
  }
}
