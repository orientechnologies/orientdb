package com.orientechnologies.orient.distributed.context;

import com.orientechnologies.orient.core.transaction.ODatabaseId;
import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.distributed.db.OReceiverInputStream;
import com.orientechnologies.orient.distributed.db.OSyncMode;

public class OSyncState {

  private final ODatabaseId dbId;
  private final OSyncId syncId;
  private final ONodeId from;
  private final ONodeId to;
  private final OSyncMode mode;
  private volatile int messageCount = 0;
  private volatile long totalsize = 0;
  private volatile OReceiverInputStream receiver;
  private volatile boolean canNext = false;
  private volatile boolean close = false;

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

  public synchronized void receiveData(byte[] data, boolean finished) {
    receiver.receive(data, finished);
  }

  public synchronized void setReceiver(OReceiverInputStream receiver) {
    this.receiver = receiver;
  }

  public boolean isClose() {
    return close;
  }

  public synchronized void waitForNext() throws InterruptedException {
    while (!canNext && !close) {
      // TODO: use timeout !
      this.wait();
    }
    canNext = false;
  }

  public synchronized void requestNext(boolean close) {
    canNext = true;
    this.close = close;
    this.notifyAll();
  }
}
