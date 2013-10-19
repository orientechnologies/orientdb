package com.orientechnologies.orient.core.index.sbtreebonsai.local;

import java.io.IOException;

import com.orientechnologies.common.directmemory.ODirectMemoryPointer;
import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;

/**
 * @author <a href="mailto:enisher@gmail.com">Artem Orobets</a>
 */
public class OSysBucket extends OBonsaiBucketAbstract {
  private static final int  SYS_MAGIC_OFFSET        = WAL_POSITION_OFFSET + OLongSerializer.LONG_SIZE;
  private static final int  FREE_SPACE_OFFSET       = SYS_MAGIC_OFFSET + OByteSerializer.BYTE_SIZE;
  private static final int  FREE_LIST_HEAD_OFFSET   = FREE_SPACE_OFFSET + OBonsaiBucketPointer.SIZE;
  private static final int  FREE_LIST_LENGTH_OFFSET = FREE_LIST_HEAD_OFFSET + OBonsaiBucketPointer.SIZE;

  private static final byte SYS_MAGIC               = (byte) 41;

  public OSysBucket(ODirectMemoryPointer pagePointer, TrackMode trackMode) {
    super(pagePointer, trackMode);
  }

  public void init() throws IOException {
    setByteValue(SYS_MAGIC_OFFSET, SYS_MAGIC);
    setBucketPointer(FREE_SPACE_OFFSET, new OBonsaiBucketPointer(0, OSBTreeBonsaiBucket.MAX_BUCKET_SIZE_BYTES));
    setBucketPointer(FREE_LIST_HEAD_OFFSET, OBonsaiBucketPointer.NULL);
    setLongValue(FREE_LIST_LENGTH_OFFSET, 0L);
  }

  public boolean isInitialized() {
    return getByteValue(SYS_MAGIC_OFFSET) != 41;
  }

  public long freeListLength() {
    return getLongValue(FREE_LIST_LENGTH_OFFSET);
  }

  public void setFreeListLength(long length) throws IOException {
    setLongValue(FREE_LIST_LENGTH_OFFSET, length);
  }

  public OBonsaiBucketPointer getFreeSpacePointer() {
    return getBucketPointer(FREE_SPACE_OFFSET);
  }

  public void setFreeSpacePointer(OBonsaiBucketPointer pointer) throws IOException {
    setBucketPointer(FREE_SPACE_OFFSET, pointer);
  }

  public OBonsaiBucketPointer getFreeListHead() {
    return getBucketPointer(FREE_LIST_HEAD_OFFSET);
  }

  public void setFreeListHead(OBonsaiBucketPointer pointer) throws IOException {
    setBucketPointer(FREE_LIST_HEAD_OFFSET, pointer);
  }
}
