package com.orientechnologies.orient.core.storage.cache.local.doublewritelog;

import com.orientechnologies.common.directmemory.OByteBufferPool;
import com.orientechnologies.common.directmemory.OPointer;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;

/** Stub for double write log */
public class DoubleWriteLogNoOP implements DoubleWriteLog {
  @Override
  public boolean write(ByteBuffer[] buffers, int[] fileId, int[] pageIndex) {
    return false;
  }

  @Override
  public void truncate() {}

  @Override
  public void open(String storageName, Path storagePath, int pageSize) {}

  @Override
  public OPointer loadPage(int fileId, int pageIndex, OByteBufferPool bufferPool) {
    return null;
  }

  @Override
  public void restoreModeOn() throws IOException {}

  @Override
  public void restoreModeOff() {}

  @Override
  public void close() throws IOException {}

  @Override
  public void startCheckpoint() {}

  @Override
  public void endCheckpoint() {}
}
