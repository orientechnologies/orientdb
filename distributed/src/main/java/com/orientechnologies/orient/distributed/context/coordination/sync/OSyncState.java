package com.orientechnologies.orient.distributed.context.coordination.sync;

import com.orientechnologies.orient.core.transaction.ODatabaseId;
import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.core.tx.OTransactionSequenceStatus;
import com.orientechnologies.orient.distributed.db.OReceiverInputStream;
import com.orientechnologies.orient.distributed.db.OSyncMode;
import com.orientechnologies.orient.server.distributed.OLoggerDistributed;
import java.util.Optional;

public class OSyncState {
  private static final OLoggerDistributed logger = OLoggerDistributed.logger(OSyncState.class);

  private final ODatabaseId dbId;
  private final OSyncId syncId;
  private final ONodeId sender;
  private final ONodeId receiver;
  private final OSyncMode mode;
  private final Optional<OTransactionSequenceStatus> sequenceStatus;
  private volatile int messageCount = 0;
  private volatile long totalsize = 0;
  private volatile OReceiverInputStream receiverStream;
  private volatile boolean canNext = false;
  private volatile boolean close = false;

  public OSyncState(
      ODatabaseId dbId,
      OSyncId syncId,
      ONodeId sender,
      ONodeId receiver,
      OSyncMode mode,
      Optional<OTransactionSequenceStatus> sequenceStatus) {
    this.dbId = dbId;
    this.syncId = syncId;
    this.sender = sender;
    this.receiver = receiver;
    this.mode = mode;
    this.sequenceStatus = sequenceStatus;
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

  public ONodeId getSender() {
    return sender;
  }

  public ONodeId getReceiver() {
    return receiver;
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
    logger.debug("receiving buffer size %d finished %b", data.length, finished);
    receiverStream.receive(data, finished);
    if (finished) {
      this.close = true;
    }
  }

  public synchronized void setReceiver(OReceiverInputStream receiver) {
    this.receiverStream = receiver;
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
    logger.debug("requesting next buffer");
    canNext = true;
    this.close = close;
    this.notifyAll();
  }

  public Optional<OTransactionSequenceStatus> getSequenceStatus() {
    return sequenceStatus;
  }

  public synchronized void close() {
    this.close = true;
    this.notifyAll();
  }
}
