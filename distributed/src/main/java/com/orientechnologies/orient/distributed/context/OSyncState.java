package com.orientechnologies.orient.distributed.context;

import com.orientechnologies.orient.core.transaction.ODatabaseId;
import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.distributed.db.OReceiverImputStream;
import com.orientechnologies.orient.distributed.db.OSyncMode;

public class OSyncState {

  private final ODatabaseId dbId;
  private final OSyncId syncId;
  private final ONodeId from;
  private final ONodeId to;
  private final OSyncMode mode;
  private volatile int messageCount = 0;
  private volatile long totalsize = 0;
  private volatile OReceiverImputStream receiver;
  private boolean canNext = false;

  public OSyncState(ODatabaseId dbId, OSyncId syncId, ONodeId from, ONodeId to, OSyncMode mode) {
    this.dbId = dbId;
    this.syncId = syncId;
    this.from = from;
    this.to = to;
    this.mode = mode;
  }

  public synchronized void transaferd(long size) {
    this.messageCount += 1;
    this.totalsize += size;
  }

  public synchronized int getMessageCount() {
    return messageCount;
  }

  public synchronized long getTotalsize() {
    return totalsize;
  }

  public ODatabaseId getDbId() {
    return dbId;
  }

  public ONodeId getFrom() {
    return from;
  }

  public ONodeId getTo() {
    return to;
  }

  public OSyncMode getMode() {
    return mode;
  }

  public OSyncId getSyncId() {
    return syncId;
  }

  public boolean isIncremental() {
    return OSyncMode.IncrementalBackup.equals(this.mode);
  }

  public synchronized void receiveData(byte[] data) {
    receiver.receive(data);
  }

  public synchronized void setReceiver(OReceiverImputStream receiver) {
    this.receiver = receiver;
  }

  public synchronized void waitForNext() throws InterruptedException {
    while (!canNext) {
      this.wait();
    }
    canNext = false;
  }

  public synchronized void requestNext() {
    canNext = true;
    this.notifyAll();
  }
}
