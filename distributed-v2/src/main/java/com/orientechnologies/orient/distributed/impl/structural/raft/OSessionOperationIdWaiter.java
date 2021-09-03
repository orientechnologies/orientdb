package com.orientechnologies.orient.distributed.impl.structural.raft;

import com.orientechnologies.common.concur.lock.OInterruptedException;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.distributed.impl.coordinator.transaction.OSessionOperationId;

public class OSessionOperationIdWaiter {

  private volatile OSessionOperationId lastReceived;

  public synchronized void notify(OSessionOperationId id) {
    this.lastReceived = id;
    this.notifyAll();
  }

  public synchronized void waitIfNeeded(OSessionOperationId id) {
    while (lastReceived != null && id.getSequential() > lastReceived.getSequential()) {
      try {
        this.wait();
      } catch (InterruptedException e) {
        throw OException.wrapException(
            new OInterruptedException("Interrupted waiting for apply of own requests"), e);
      }
    }
  }
}
