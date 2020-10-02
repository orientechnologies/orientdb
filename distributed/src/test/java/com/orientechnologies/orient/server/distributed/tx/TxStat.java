package com.orientechnologies.orient.server.distributed.tx;

public class TxStat {
  public final long beginTimestamp;
  public final long commitTimestamp; // -1 means never committed.
  public final int retries;

  public TxStat(long beginTimestamp, long commitTimestamp, int retries) {
    this.beginTimestamp = beginTimestamp;
    this.commitTimestamp = commitTimestamp;
    this.retries = retries;
  }

  public boolean isCommitted() {
    return commitTimestamp != -1;
  }

  public long getLatency() {
    return commitTimestamp - beginTimestamp;
  }
}
