package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.common.serialization.types.OStringSerializer;

/**
 * @author Andrey Lomakin
 * @since 29.04.13
 */
public abstract class OAbstractWALRecord implements OWALRecord {
  private OLogSequenceNumber lsn;

  private long               pageIndex;
  private String             fileName;

  protected OAbstractWALRecord() {
  }

  protected OAbstractWALRecord(long pageIndex, String fileName) {
    this.pageIndex = pageIndex;
    this.fileName = fileName;
  }

  @Override
  public int toStream(byte[] content, int offset) {
    OLongSerializer.INSTANCE.serializeNative(pageIndex, content, offset);
    offset += OLongSerializer.LONG_SIZE;

    OStringSerializer.INSTANCE.serializeNative(fileName, content, offset);
    offset += OStringSerializer.INSTANCE.getObjectSize(fileName);

    return offset;
  }

  @Override
  public int fromStream(byte[] content, int offset) {
    pageIndex = OLongSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OLongSerializer.LONG_SIZE;

    fileName = OStringSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OStringSerializer.INSTANCE.getObjectSize(fileName);

    return offset;
  }

  @Override
  public int serializedSize() {
    return OLongSerializer.LONG_SIZE + OStringSerializer.INSTANCE.getObjectSize(fileName);
  }

  @Override
  public long getPageIndex() {
    return pageIndex;
  }

  @Override
  public String getFileName() {
    return fileName;
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

    OAbstractWALRecord that = (OAbstractWALRecord) o;

    if (pageIndex != that.pageIndex)
      return false;
    if (!fileName.equals(that.fileName))
      return false;
    if (!lsn.equals(that.lsn))
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = lsn.hashCode();
    result = 31 * result + (int) (pageIndex ^ (pageIndex >>> 32));
    result = 31 * result + fileName.hashCode();
    return result;
  }
}
