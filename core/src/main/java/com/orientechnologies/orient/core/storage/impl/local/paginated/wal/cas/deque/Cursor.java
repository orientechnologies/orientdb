package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.cas.deque;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWALRecord;

public class Cursor {
  final Node       node;
  final int        pageIndex;
  final OWALRecord record;

  public Cursor(Node node, int pageIndex, OWALRecord record) {
    this.node = node;
    this.pageIndex = pageIndex;
    this.record = record;
  }

  public OWALRecord getRecord() {
    return record;
  }
}
