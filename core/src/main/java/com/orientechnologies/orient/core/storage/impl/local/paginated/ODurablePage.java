package com.orientechnologies.orient.core.storage.impl.local.paginated;

import java.io.IOException;

import com.orientechnologies.common.directmemory.ODirectMemoryPointer;
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
  protected static final int           MAGIC_NUMBER_OFFSET = 0;
  protected static final int           CRC32_OFFSET        = MAGIC_NUMBER_OFFSET + OLongSerializer.LONG_SIZE;

  public static final int              WAL_SEGMENT_OFFSET  = CRC32_OFFSET + OIntegerSerializer.INT_SIZE;
  public static final int              WAL_POSITION_OFFSET = WAL_SEGMENT_OFFSET + OLongSerializer.LONG_SIZE;
  public static final int              MAX_PAGE_SIZE_BYTES = OGlobalConfiguration.DISK_CACHE_PAGE_SIZE.getValueAsInteger() * 1024;

  protected static final int           NEXT_FREE_POSITION  = WAL_POSITION_OFFSET + OLongSerializer.LONG_SIZE;

  protected OPageChanges               pageChanges         = new OPageChanges();

  protected final ODirectMemoryPointer pagePointer;
  protected final TrackMode            trackMode;

  public ODurablePage(ODirectMemoryPointer pagePointer, TrackMode trackMode) {
    this.pagePointer = pagePointer;
    this.trackMode = trackMode;
  }

  public static OLogSequenceNumber getLogSequenceNumberFromPage(ODirectMemoryPointer dataPointer) {
    final long segment = OLongSerializer.INSTANCE.deserializeFromDirectMemory(dataPointer, WAL_SEGMENT_OFFSET);
    final long position = OLongSerializer.INSTANCE.deserializeFromDirectMemory(dataPointer, WAL_POSITION_OFFSET);

    return new OLogSequenceNumber(segment, position);
  }

  public static enum TrackMode {
    NONE, FULL, ROLLBACK_ONLY
  }

  public int getIntValue(int pageOffset) {
    return OIntegerSerializer.INSTANCE.deserializeFromDirectMemory(pagePointer, pageOffset);
  }

  public long getLongValue(int pageOffset) {
    return OLongSerializer.INSTANCE.deserializeFromDirectMemory(pagePointer, pageOffset);
  }

  public byte[] getBinaryValue(int pageOffset, int valLen) {
    return pagePointer.get(pageOffset, valLen);
  }

  public byte getByteValue(int pageOffset) {
    return pagePointer.getByte(pageOffset);
  }

  public void setIntValue(int pageOffset, int value) throws IOException {
    if (trackMode.equals(TrackMode.FULL)) {
      byte[] oldValues = pagePointer.get(pageOffset, OIntegerSerializer.INT_SIZE);
      OIntegerSerializer.INSTANCE.serializeInDirectMemory(value, pagePointer, pageOffset);
      byte[] newValues = pagePointer.get(pageOffset, OIntegerSerializer.INT_SIZE);

      pageChanges.addChanges(pageOffset, newValues, oldValues);
    } else if (trackMode.equals(TrackMode.ROLLBACK_ONLY)) {
      byte[] oldValues = pagePointer.get(pageOffset, OIntegerSerializer.INT_SIZE);
      OIntegerSerializer.INSTANCE.serializeInDirectMemory(value, pagePointer, pageOffset);

      pageChanges.addChanges(pageOffset, null, oldValues);
    } else
      OIntegerSerializer.INSTANCE.serializeInDirectMemory(value, pagePointer, pageOffset);
  }

  public void setByteValue(int pageOffset, byte value) {
    if (trackMode.equals(TrackMode.FULL)) {
      byte[] oldValues = new byte[] { pagePointer.getByte(pageOffset) };
      pagePointer.setByte(pageOffset, value);
      byte[] newValues = new byte[] { pagePointer.getByte(pageOffset) };

      pageChanges.addChanges(pageOffset, newValues, oldValues);
    } else if (trackMode.equals(TrackMode.ROLLBACK_ONLY)) {
      byte[] oldValues = new byte[] { pagePointer.getByte(pageOffset) };
      pagePointer.setByte(pageOffset, value);

      pageChanges.addChanges(pageOffset, null, oldValues);
    } else
      pagePointer.setByte(pageOffset, value);
  }

  public void setLongValue(int pageOffset, long value) throws IOException {
    if (trackMode.equals(TrackMode.FULL)) {
      byte[] oldValues = pagePointer.get(pageOffset, OLongSerializer.LONG_SIZE);
      OLongSerializer.INSTANCE.serializeInDirectMemory(value, pagePointer, pageOffset);
      byte[] newValues = pagePointer.get(pageOffset, OLongSerializer.LONG_SIZE);

      pageChanges.addChanges(pageOffset, newValues, oldValues);
    } else if (trackMode.equals(TrackMode.ROLLBACK_ONLY)) {
      byte[] oldValues = pagePointer.get(pageOffset, OLongSerializer.LONG_SIZE);
      OLongSerializer.INSTANCE.serializeInDirectMemory(value, pagePointer, pageOffset);

      pageChanges.addChanges(pageOffset, null, oldValues);
    } else
      OLongSerializer.INSTANCE.serializeInDirectMemory(value, pagePointer, pageOffset);
  }

  public void setBinaryValue(int pageOffset, byte[] value) throws IOException {
    if (value.length == 0)
      return;

    if (trackMode.equals(TrackMode.FULL)) {
      byte[] oldValues = pagePointer.get(pageOffset, value.length);
      pagePointer.set(pageOffset, value, 0, value.length);

      pageChanges.addChanges(pageOffset, value, oldValues);
    } else if (trackMode.equals(TrackMode.ROLLBACK_ONLY)) {
      byte[] oldValues = pagePointer.get(pageOffset, value.length);
      pagePointer.set(pageOffset, value, 0, value.length);

      pageChanges.addChanges(pageOffset, null, oldValues);
    } else
      pagePointer.set(pageOffset, value, 0, value.length);
  }

  public void copyData(int from, int to, int len) throws IOException {
    if (len == 0)
      return;

    if (trackMode.equals(TrackMode.FULL)) {
      byte[] content = pagePointer.get(from, len);
      byte[] oldContent = pagePointer.get(to, len);

      pagePointer.copyData(from, pagePointer, to, len);

      pageChanges.addChanges(to, content, oldContent);
    } else if (trackMode.equals(TrackMode.ROLLBACK_ONLY)) {
      byte[] oldContent = pagePointer.get(to, len);

      pagePointer.copyData(from, pagePointer, to, len);

      pageChanges.addChanges(to, null, oldContent);

    } else
      pagePointer.copyData(from, pagePointer, to, len);
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
    OLongSerializer.INSTANCE.serializeInDirectMemory(lsn.getSegment(), pagePointer, WAL_SEGMENT_OFFSET);
    OLongSerializer.INSTANCE.serializeInDirectMemory(lsn.getPosition(), pagePointer, WAL_POSITION_OFFSET);
  }
}
