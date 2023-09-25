package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.cas;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;

public final class WrittenUpTo {
  private final OLogSequenceNumber lsn;
  private final long position;

  public WrittenUpTo(final OLogSequenceNumber lsn, final long position) {
    this.lsn = lsn;
    this.position = position;
  }

  public OLogSequenceNumber getLsn() {
    return lsn;
  }

  public long getPosition() {
    return position;
  }
}
