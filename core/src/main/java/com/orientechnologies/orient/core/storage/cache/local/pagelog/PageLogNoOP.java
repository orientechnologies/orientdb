package com.orientechnologies.orient.core.storage.cache.local.pagelog;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;

/**
 * No OP page log which is used as a stub till implementation of double write pattern will be finished.
 */
public class PageLogNoOP implements PageLog {
  @Override
  public boolean write(ByteBuffer[] buffers) throws IOException {
    return false;
  }

  @Override
  public void truncate() throws IOException {

  }

  @Override
  public void close() throws IOException {

  }

  @Override
  public void open(Path storagePath) throws IOException {

  }
}
