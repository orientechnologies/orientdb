package com.orientechnologies.orient.core.storage.impl.local.paginated;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.orientechnologies.common.directmemory.ODirectMemory;
import com.orientechnologies.common.directmemory.ODirectMemoryFactory;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.updatePageRecord.OBinaryFullPageDiff;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.updatePageRecord.OBinaryPageDiff;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.updatePageRecord.OIntFullPageDiff;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.updatePageRecord.OIntPageDiff;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.updatePageRecord.OLongFullPageDiff;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.updatePageRecord.OLongPageDiff;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.updatePageRecord.OPageDiff;

/**
 * @author LomakiA <a href="mailto:Andrey.Lomakin@exigenservices.com">Andrey Lomakin</a>
 * @since 16.08.13
 */
public abstract class OAbstractPLocalPage {

  protected static final int    MAGIC_NUMBER_OFFSET = 0;
  protected static final int    CRC32_OFFSET        = MAGIC_NUMBER_OFFSET + OLongSerializer.LONG_SIZE;

  protected static final int    WAL_SEGMENT_OFFSET  = CRC32_OFFSET + OIntegerSerializer.INT_SIZE;
  protected static final int    WAL_POSITION_OFFSET = WAL_SEGMENT_OFFSET + OIntegerSerializer.INT_SIZE;

  protected final ODirectMemory directMemory        = ODirectMemoryFactory.INSTANCE.directMemory();

  protected List<OPageDiff<?>>  pageChanges         = new ArrayList<OPageDiff<?>>();

  protected final long          pagePointer;
  protected final TrackMode     trackMode;

  protected OAbstractPLocalPage(long pagePointer, TrackMode trackMode) {
    this.pagePointer = pagePointer;
    this.trackMode = trackMode;
  }

  public static OLogSequenceNumber getLogSequenceNumberFromPage(ODirectMemory directMemory, long dataPointer) {
    final long position = OLongSerializer.INSTANCE.deserializeFromDirectMemory(directMemory, dataPointer
        + OLongSerializer.LONG_SIZE + (2 * OIntegerSerializer.INT_SIZE));
    final int segment = OIntegerSerializer.INSTANCE.deserializeFromDirectMemory(directMemory, dataPointer
        + OLongSerializer.LONG_SIZE + OIntegerSerializer.INT_SIZE);

    return new OLogSequenceNumber(segment, position);
  }

  public static enum TrackMode {
    NONE, FORWARD, BOTH
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
    if (trackMode.equals(TrackMode.FORWARD))
      pageChanges.add(new OIntPageDiff(value, pageOffset));
    else if (trackMode.equals(TrackMode.BOTH)) {
      int oldValue = OIntegerSerializer.INSTANCE.deserializeFromDirectMemory(directMemory, pagePointer + pageOffset);
      pageChanges.add(new OIntFullPageDiff(value, pageOffset, oldValue));
    }

    OIntegerSerializer.INSTANCE.serializeInDirectMemory(value, directMemory, pagePointer + pageOffset);
  }

  public void setByteValue(int pageOffset, byte value) {
    if (trackMode.equals(TrackMode.FORWARD)) {
      pageChanges.add(new OBinaryPageDiff(new byte[] { value }, pageOffset));
    } else if (trackMode.equals(TrackMode.BOTH)) {
      byte oldValue = directMemory.getByte(pagePointer + pageOffset);

      pageChanges.add(new OBinaryFullPageDiff(new byte[] { value }, pageOffset, new byte[] { oldValue }));
    }

    directMemory.setByte(pagePointer + pageOffset, value);
  }

  public void setLongValue(int pageOffset, long value) throws IOException {
    if (trackMode.equals(TrackMode.FORWARD))
      pageChanges.add(new OLongPageDiff(value, pageOffset));
    else if (trackMode.equals(TrackMode.BOTH)) {
      long oldValue = OLongSerializer.INSTANCE.deserializeFromDirectMemory(directMemory, pagePointer + pageOffset);
      pageChanges.add(new OLongFullPageDiff(value, pageOffset, oldValue));
    }

    OLongSerializer.INSTANCE.serializeInDirectMemory(value, directMemory, pagePointer + pageOffset);
  }

  public void setBinaryValue(int pageOffset, byte[] value) throws IOException {
    if (value.length == 0)
      return;

    if (trackMode.equals(TrackMode.FORWARD))
      pageChanges.add(new OBinaryPageDiff(value, pageOffset));
    else if (trackMode.equals(TrackMode.BOTH)) {
      byte[] oldValue = directMemory.get(pagePointer + pageOffset, value.length);
      pageChanges.add(new OBinaryFullPageDiff(value, pageOffset, oldValue));
      directMemory.set(pagePointer + pageOffset, value, 0, value.length);
    }

    directMemory.set(pagePointer + pageOffset, value, 0, value.length);
  }

  public void copyData(int from, int to, int len) throws IOException {
    if (len == 0)
      return;

    if (trackMode.equals(TrackMode.FORWARD)) {
      byte[] content = directMemory.get(pagePointer + from, len);
      pageChanges.add(new OBinaryPageDiff(content, to));
    } else if (trackMode.equals(TrackMode.BOTH)) {
      byte[] content = directMemory.get(pagePointer + from, len);
      byte[] oldContent = directMemory.get(pagePointer + to, len);

      pageChanges.add(new OBinaryFullPageDiff(content, to, oldContent));
    }

    directMemory.copyData(pagePointer + from, pagePointer + to, len);
  }

}
