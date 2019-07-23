package com.orientechnologies.orient.core.storage.cache.local.doublewritelog;

import java.nio.ByteBuffer;
import java.nio.file.Path;

/**
 * No OP page log which is used as a stub till implementation of double write pattern will be finished.
 */
public class DoubleWriteLogNoOP implements DoubleWriteLog {
  @Override
  public boolean write(ByteBuffer[] buffers, long fileId, int pageIndex) {
    return false;
  }

  @Override
  public void truncate() {

  }

  @Override
  public void open(String storageName, Path storagePath) {

  }

  @Override
  public ByteBuffer loadPage(long fileId, int pageIndex) {
    return null;
  }
}
