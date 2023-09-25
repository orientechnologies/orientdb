package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.cas;

final class RecordsWriter implements Runnable {
  /** */
  private final CASDiskWriteAheadLog wal;

  private final boolean forceSync;
  private final boolean fullWrite;

  RecordsWriter(
      CASDiskWriteAheadLog casDiskWriteAheadLog, final boolean forceSync, final boolean fullWrite) {
    wal = casDiskWriteAheadLog;
    this.forceSync = forceSync;
    this.fullWrite = fullWrite;
  }

  @Override
  public void run() {
    wal.executeWriteRecords(forceSync, fullWrite);
  }
}
