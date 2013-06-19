package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;

/**
 * @author Andrey Lomakin
 * @since 14.06.13
 */
public class OFreePageChangeRecord extends OOperationUnitRecord implements OClusterAwareWALRecord {
  private OLogSequenceNumber lsn;

  private int                clusterId;
  private int                freePageIndex;

  private long               prevPageIndex;
  private long               pageIndex;

  public OFreePageChangeRecord() {
  }

  public OFreePageChangeRecord(OOperationUnitId operationUnitId, int clusterId, int freePageIndex, long prevPageIndex,
      long pageIndex) {
    super(operationUnitId);
    this.clusterId = clusterId;
    this.freePageIndex = freePageIndex;
    this.prevPageIndex = prevPageIndex;
    this.pageIndex = pageIndex;
  }

  public int getFreePageIndex() {
    return freePageIndex;
  }

  public long getPrevPageIndex() {
    return prevPageIndex;
  }

  public long getPageIndex() {
    return pageIndex;
  }

  @Override
  public int getClusterId() {
    return clusterId;
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
  public int toStream(byte[] content, int offset) {
    offset = super.toStream(content, offset);

    OIntegerSerializer.INSTANCE.serializeNative(clusterId, content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    OIntegerSerializer.INSTANCE.serializeNative(freePageIndex, content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    OLongSerializer.INSTANCE.serializeNative(prevPageIndex, content, offset);
    offset += OLongSerializer.LONG_SIZE;

    OLongSerializer.INSTANCE.serializeNative(pageIndex, content, offset);
    offset += OLongSerializer.LONG_SIZE;

    return offset;
  }

  @Override
  public int fromStream(byte[] content, int offset) {
    offset = super.fromStream(content, offset);

    clusterId = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    freePageIndex = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    prevPageIndex = OLongSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OLongSerializer.LONG_SIZE;

    pageIndex = OLongSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OLongSerializer.LONG_SIZE;

    return offset;
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + 2 * OIntegerSerializer.INT_SIZE + 2 * OLongSerializer.LONG_SIZE;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    if (!super.equals(o))
      return false;

    OFreePageChangeRecord that = (OFreePageChangeRecord) o;

    if (clusterId != that.clusterId)
      return false;
    if (freePageIndex != that.freePageIndex)
      return false;
    if (pageIndex != that.pageIndex)
      return false;
    if (prevPageIndex != that.prevPageIndex)
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + clusterId;
    result = 31 * result + freePageIndex;
    result = 31 * result + (int) (prevPageIndex ^ (prevPageIndex >>> 32));
    result = 31 * result + (int) (pageIndex ^ (pageIndex >>> 32));
    return result;
  }

  @Override
  public String toString() {
    return "OFreePageChangeRecord{" + "lsn=" + lsn + ", clusterId=" + clusterId + ", freePageIndex=" + freePageIndex
        + ", prevPageIndex=" + prevPageIndex + ", pageIndex=" + pageIndex + "} " + super.toString();
  }
}
