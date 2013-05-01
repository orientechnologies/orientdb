package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

import java.util.HashSet;
import java.util.Set;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.common.serialization.types.OStringSerializer;

/**
 * @author Andrey Lomakin
 * @since 5/1/13
 */
public class ODirtyPagesRecord implements OWALRecord {
  private OLogSequenceNumber lsn;

  private Set<ODirtyPage>    dirtyPages;

  public ODirtyPagesRecord() {
  }

  public ODirtyPagesRecord(Set<ODirtyPage> dirtyPages) {
    this.dirtyPages = dirtyPages;
  }

  @Override
  public int toStream(byte[] content, int offset) {
    OIntegerSerializer.INSTANCE.serializeNative(dirtyPages.size(), content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    for (ODirtyPage dirtyPage : dirtyPages) {
      OLongSerializer.INSTANCE.serializeNative(dirtyPage.getPageIndex(), content, offset);
      offset += OLongSerializer.LONG_SIZE;

      OStringSerializer.INSTANCE.serializeNative(dirtyPage.getFileName(), content, offset);
      offset += OStringSerializer.INSTANCE.getObjectSize(dirtyPage.getFileName());

      OIntegerSerializer.INSTANCE.serializeNative(dirtyPage.getLsn().getSegment(), content, offset);
      offset += OIntegerSerializer.INT_SIZE;

      OLongSerializer.INSTANCE.serializeNative(dirtyPage.getLsn().getPosition(), content, offset);
      offset += OLongSerializer.LONG_SIZE;
    }

    return offset;
  }

  @Override
  public int fromStream(byte[] content, int offset) {
    int size = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    dirtyPages = new HashSet<ODirtyPage>();

    for (int i = 0; i < size; i++) {
      long pageIndex = OLongSerializer.INSTANCE.deserializeNative(content, offset);
      offset += OLongSerializer.LONG_SIZE;

      String fileName = OStringSerializer.INSTANCE.deserializeNative(content, offset);
      offset += OStringSerializer.INSTANCE.getObjectSize(fileName);

      int segment = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
      offset += OIntegerSerializer.INT_SIZE;

      long position = OLongSerializer.INSTANCE.deserializeNative(content, offset);
      offset += OLongSerializer.LONG_SIZE;

      dirtyPages.add(new ODirtyPage(fileName, pageIndex, new OLogSequenceNumber(segment, position)));
    }

    return offset;
  }

  @Override
  public int serializedSize() {
    int size = OIntegerSerializer.INT_SIZE;

    for (ODirtyPage dirtyPage : dirtyPages) {
      size += 2 * OLongSerializer.LONG_SIZE + OIntegerSerializer.INT_SIZE;
      size += OStringSerializer.INSTANCE.getObjectSize(dirtyPage.getFileName());
    }

    return size;
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

    ODirtyPagesRecord that = (ODirtyPagesRecord) o;

    if (!dirtyPages.equals(that.dirtyPages))
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    return dirtyPages.hashCode();
  }
}
