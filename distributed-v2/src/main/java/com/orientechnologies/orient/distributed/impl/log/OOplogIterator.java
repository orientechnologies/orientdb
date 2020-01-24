package com.orientechnologies.orient.distributed.impl.log;

import java.io.Closeable;
import java.util.Iterator;

public interface OOplogIterator extends Closeable, Iterator<OOperationLogEntry> {

  @Override
  void close();
}
