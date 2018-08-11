package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.cas;

public interface OSegmentOverflowListener {
  void onSegmentOverflow(long segment);
}