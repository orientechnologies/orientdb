package com.orientechnologies.orient.core.storage.impl.local.paginated;

import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.common.serialization.types.OStringSerializer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWALRecord;

/**
 * @author Andrey Lomakin
 * @since 5/1/13
 */
public class OClusterStateRecord implements OWALRecord {
  private OLogSequenceNumber lsn;

  private long               size;
  private long               recordsSize;
  private String             name;

  public OClusterStateRecord() {
  }

  public OClusterStateRecord(long size, long recordsSize, String name) {
    this.size = size;
    this.recordsSize = recordsSize;
    this.name = name;
  }

  @Override
  public int toStream(byte[] content, int offset) {
    OLongSerializer.INSTANCE.serializeNative(size, content, offset);
    offset += OLongSerializer.LONG_SIZE;

    OLongSerializer.INSTANCE.serializeNative(recordsSize, content, offset);
    offset += OLongSerializer.LONG_SIZE;

    OStringSerializer.INSTANCE.serializeNative(name, content, offset);
    offset += OStringSerializer.INSTANCE.getObjectSize(name);

    return offset;
  }

  @Override
  public int fromStream(byte[] content, int offset) {
    size = OLongSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OLongSerializer.LONG_SIZE;

    recordsSize = OLongSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OLongSerializer.LONG_SIZE;

    name = OStringSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OStringSerializer.INSTANCE.getObjectSize(name);

    return offset;
  }

  @Override
  public int serializedSize() {
    return 2 * OLongSerializer.LONG_SIZE + OStringSerializer.INSTANCE.getObjectSize(name);
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

    OClusterStateRecord that = (OClusterStateRecord) o;

    if (recordsSize != that.recordsSize)
      return false;
    if (size != that.size)
      return false;
    if (!name.equals(that.name))
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = (int) (size ^ (size >>> 32));
    result = 31 * result + (int) (recordsSize ^ (recordsSize >>> 32));
    result = 31 * result + name.hashCode();
    return result;
  }
}
