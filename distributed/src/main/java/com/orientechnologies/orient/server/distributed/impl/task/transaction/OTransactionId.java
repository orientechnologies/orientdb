package com.orientechnologies.orient.server.distributed.impl.task.transaction;

public class OTransactionId {
  private int  position;
  private long sequence;

  public OTransactionId(int position, long sequence) {
    this.position = position;
    this.sequence = sequence;
  }

  public int getPosition() {
    return position;
  }

  public long getSequence() {
    return sequence;
  }
}
