package com.orientechnologies.orient.core.storage.impl.local.paginated;

import java.io.IOException;

import com.orientechnologies.common.directmemory.ODirectMemory;
import com.orientechnologies.common.directmemory.ODirectMemoryFactory;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OPageChanges;

/**
 * @author Andrey Lomakin
 * @since 16.08.13
 */
public class ODurablePage {
  protected static final int    MAGIC_NUMBER_OFFSET = 0;
  protected static final int    CRC32_OFFSET        = MAGIC_NUMBER_OFFSET + OLongSerializer.LONG_SIZE;

  public static final int       WAL_SEGMENT_OFFSET  = CRC32_OFFSET + OIntegerSerializer.INT_SIZE;
  public static final int       WAL_POSITION_OFFSET = WAL_SEGMENT_OFFSET + OLongSerializer.LONG_SIZE;
  public static final int       MAX_PAGE_SIZE_BYTES = OGlobalConfiguration.DISK_CACHE_PAGE_SIZE.getValueAsInteger() * 1024;

  protected static final int    NEXT_FREE_POSITION  = WAL_POSITION_OFFSET + OLongSerializer.LONG_SIZE;

  protected final ODirectMemory directMemory        = ODirectMemoryFactory.INSTANCE.directMemory();

  protected OPageChanges        pageChanges         = new OPageChanges();

  protected final long          pagePointer;
  protected final TrackMode     trackMode;

  public ODurablePage(long pagePointer, TrackMode trackMode) {
    this.pagePointer = pagePointer;
    this.trackMode = trackMode;
  }

  public static OLogSequenceNumber getLogSequenceNumberFromPage(ODirectMemory directMemory, long dataPointer) {
    final long segment = OLongSerializer.INSTANCE.deserializeFromDirectMemory(directMemory, dataPointer + WAL_SEGMENT_OFFSET);
    final long position = OLongSerializer.INSTANCE.deserializeFromDirectMemory(directMemory, dataPointer + WAL_POSITION_OFFSET);

    return new OLogSequenceNumber(segment, position);
  }

  public static enum TrackMode {
    NONE, FULL, ROLLBACK_ONLY
  }

  public int getIntValue(int pageOffset) {
    return OIntegerSerializer.INSTANCE.deserializeFromDirectMemory(directMemory, pagePointer + pageOffset);
  }

  public long getLongValue(int pageOffset) {
    return OLongSerializer.INSTANCE.deserializeFromDirectMemory(directMemory, pagePointer + pageOffset);
  }

  public byte[] getBinaryValue(int pageOffset, int valLen) {
    return directMemory.get(pagePointer + pageOffset, valLen);
  }

  public byte getByteValue(int pageOffset) {
    return directMemory.getByte(pagePointer + pageOffset);
  }

  public void setIntValue(int pageOffset, int value) throws IOException {
    if (trackMode.equals(TrackMode.FULL)) {
      byte[] oldValues = directMemory.get(pagePointer + pageOffset, OIntegerSerializer.INT_SIZE);
      OIntegerSerializer.INSTANCE.serializeInDirectMemory(value, directMemory, pagePointer + pageOffset);
      byte[] newValues = directMemory.get(pagePointer + pageOffset, OIntegerSerializer.INT_SIZE);

      pageChanges.addChanges(pageOffset, newValues, oldValues);
    } else if (trackMode.equals(TrackMode.ROLLBACK_ONLY)) {
      byte[] oldValues = directMemory.get(pagePointer + pageOffset, OIntegerSerializer.INT_SIZE);
      OIntegerSerializer.INSTANCE.serializeInDirectMemory(value, directMemory, pagePointer + pageOffset);

      pageChanges.addChanges(pageOffset, null, oldValues);
    } else
      OIntegerSerializer.INSTANCE.serializeInDirectMemory(value, directMemory, pagePointer + pageOffset);
  }

  public void setByteValue(int pageOffset, byte value) {
    if (trackMode.equals(TrackMode.FULL)) {
      byte[] oldValues = new byte[] { directMemory.getByte(pagePointer + pageOffset) };
      directMemory.setByte(pagePointer + pageOffset, value);
      byte[] newValues = new byte[] { directMemory.getByte(pagePointer + pageOffset) };

      pageChanges.addChanges(pageOffset, newValues, oldValues);
    } else if (trackMode.equals(TrackMode.ROLLBACK_ONLY)) {
      byte[] oldValues = new byte[] { directMemory.getByte(pagePointer + pageOffset) };
      directMemory.setByte(pagePointer + pageOffset, value);

      pageChanges.addChanges(pageOffset, null, oldValues);
    } else
      directMemory.setByte(pagePointer + pageOffset, value);
  }

  public void setLongValue(int pageOffset, long value) throws IOException {
    if (trackMode.equals(TrackMode.FULL)) {
      byte[] oldValues = directMemory.get(pagePointer + pageOffset, OLongSerializer.LONG_SIZE);
      OLongSerializer.INSTANCE.serializeInDirectMemory(value, directMemory, pagePointer + pageOffset);
      byte[] newValues = directMemory.get(pagePointer + pageOffset, OLongSerializer.LONG_SIZE);

      pageChanges.addChanges(pageOffset, newValues, oldValues);
    } else if (trackMode.equals(TrackMode.ROLLBACK_ONLY)) {
      byte[] oldValues = directMemory.get(pagePointer + pageOffset, OLongSerializer.LONG_SIZE);
      OLongSerializer.INSTANCE.serializeInDirectMemory(value, directMemory, pagePointer + pageOffset);

      pageChanges.addChanges(pageOffset, null, oldValues);
    } else
      OLongSerializer.INSTANCE.serializeInDirectMemory(value, directMemory, pagePointer + pageOffset);
  }

  public void setBinaryValue(int pageOffset, byte[] value) throws IOException {
    if (value.length == 0)
      return;

    if (trackMode.equals(TrackMode.FULL)) {
      byte[] oldValues = directMemory.get(pagePointer + pageOffset, value.length);
      directMemory.set(pagePointer + pageOffset, value, 0, value.length);

      pageChanges.addChanges(pageOffset, value, oldValues);
    } else if (trackMode.equals(TrackMode.ROLLBACK_ONLY)) {
      byte[] oldValues = directMemory.get(pagePointer + pageOffset, value.length);
      directMemory.set(pagePointer + pageOffset, value, 0, value.length);

      pageChanges.addChanges(pageOffset, null, oldValues);
    } else
      directMemory.set(pagePointer + pageOffset, value, 0, value.length);
  }

  public void copyData(int from, int to, int len) throws IOException {
    if (len == 0)
      return;

    if (trackMode.equals(TrackMode.FULL)) {
      byte[] content = directMemory.get(pagePointer + from, len);
      byte[] oldContent = directMemory.get(pagePointer + to, len);

      directMemory.copyData(pagePointer + from, pagePointer + to, len);

      pageChanges.addChanges(to, content, oldContent);
    } else if (trackMode.equals(TrackMode.ROLLBACK_ONLY)) {
      byte[] oldContent = directMemory.get(pagePointer + to, len);

      directMemory.copyData(pagePointer + from, pagePointer + to, len);

      pageChanges.addChanges(to, null, oldContent);

    } else
      directMemory.copyData(pagePointer + from, pagePointer + to, len);
  }

  public OPageChanges getPageChanges() {
    return pageChanges;
  }

  public void restoreChanges(OPageChanges pageChanges) {
    pageChanges.applyChanges(pagePointer);
  }

  public void revertChanges(OPageChanges pageChanges) {
    pageChanges.revertChanges(pagePointer);
  }

  public OLogSequenceNumber getLsn() {
    final long segment = getLongValue(WAL_SEGMENT_OFFSET);
    final long position = getLongValue(WAL_POSITION_OFFSET);

    return new OLogSequenceNumber(segment, position);
  }

  public void setLsn(OLogSequenceNumber lsn) {
    OLongSerializer.INSTANCE.serializeInDirectMemory(lsn.getSegment(), directMemory, pagePointer + WAL_SEGMENT_OFFSET);
    OLongSerializer.INSTANCE.serializeInDirectMemory(lsn.getPosition(), directMemory, pagePointer + WAL_POSITION_OFFSET);
  }
}
