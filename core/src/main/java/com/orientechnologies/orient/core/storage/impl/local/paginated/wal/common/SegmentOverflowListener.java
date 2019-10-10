package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.common;

public interface SegmentOverflowListener {
  void onSegmentOverflow(long segment);
}