package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;

/**
 * @author Andrey Lomakin
 * @since 5/1/13
 */
public class OClusterStateRecord extends OOperationUnitRecord implements OClusterAwareWALRecord {
  private OLogSequenceNumber lsn;

  private long               size;
  private long               recordsSize;

  private long               prevSize;
  private long               prevRecordsSize;

  private int                clusterId;

  public OClusterStateRecord() {
  }

  public OClusterStateRecord(long size, long recordsSize, int clusterId, long prevSize, long prevRecordsSize,
      OOperationUnitId operationUnitId) {
    super(operationUnitId);

    this.size = size;
    this.recordsSize = recordsSize;
    this.clusterId = clusterId;

    this.prevSize = prevSize;
    this.prevRecordsSize = prevRecordsSize;
  }

  public long getSize() {
    return size;
  }

  public long getRecordsSize() {
    return recordsSize;
  }

  public int getClusterId() {
    return clusterId;
  }

  public long getPrevSize() {
    return prevSize;
  }

  public long getPrevRecordsSize() {
    return prevRecordsSize;
  }

  @Override
  public int toStream(byte[] content, int offset) {
    offset = super.toStream(content, offset);

    OLongSerializer.INSTANCE.serializeNative(prevSize, content, offset);
    offset += OLongSerializer.LONG_SIZE;

    OLongSerializer.INSTANCE.serializeNative(prevRecordsSize, content, offset);
    offset += OLongSerializer.LONG_SIZE;

    OLongSerializer.INSTANCE.serializeNative(size, content, offset);
    offset += OLongSerializer.LONG_SIZE;

    OLongSerializer.INSTANCE.serializeNative(recordsSize, content, offset);
    offset += OLongSerializer.LONG_SIZE;

    OIntegerSerializer.INSTANCE.serializeNative(clusterId, content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    return offset;
  }

  @Override
  public int fromStream(byte[] content, int offset) {
    offset = super.fromStream(content, offset);

    prevSize = OLongSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OLongSerializer.LONG_SIZE;

    prevRecordsSize = OLongSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OLongSerializer.LONG_SIZE;

    size = OLongSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OLongSerializer.LONG_SIZE;

    recordsSize = OLongSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OLongSerializer.LONG_SIZE;

    clusterId = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    return offset;
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + 4 * OLongSerializer.LONG_SIZE + OIntegerSerializer.INT_SIZE;
  }

  @Override
  public boolean isUpdateMasterRecord() {
    return false;
  }

  @Override
  public OLogSequenceNumber getLsn() {
    return lsn;
  }

  @Override
  public void setLsn(OLogSequenceNumber lsn) {
    this.lsn = lsn;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    if (!super.equals(o))
      return false;

    OClusterStateRecord that = (OClusterStateRecord) o;

    if (clusterId != that.clusterId)
      return false;
    if (prevRecordsSize != that.prevRecordsSize)
      return false;
    if (prevSize != that.prevSize)
      return false;
    if (recordsSize != that.recordsSize)
      return false;
    if (size != that.size)
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + (int) (size ^ (size >>> 32));
    result = 31 * result + (int) (recordsSize ^ (recordsSize >>> 32));
    result = 31 * result + (int) (prevSize ^ (prevSize >>> 32));
    result = 31 * result + (int) (prevRecordsSize ^ (prevRecordsSize >>> 32));
    result = 31 * result + clusterId;
    return result;
  }
}
