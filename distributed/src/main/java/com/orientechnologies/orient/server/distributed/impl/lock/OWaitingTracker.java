package com.orientechnologies.orient.server.distributed.impl.lock;

import java.util.List;

class OWaitingTracker {
  private int waitingCount = 0;
  private final OnLocksAcquired toExecute;
  private List<OLockGuard> guards;

  public OWaitingTracker(OnLocksAcquired toExecute) {
    this.toExecute = toExecute;
  }

  public void waitOne() {
    this.waitingCount++;
  }

  public void unlockOne() {
    this.waitingCount--;
    this.acquireIfNoWaiting();
  }

  public void acquireIfNoWaiting() {
    assert this.waitingCount >= 0;
    if (this.waitingCount == 0) {
      this.toExecute.execute(guards);
    }
  }

  public void setGuards(List<OLockGuard> guards) {
    this.guards = guards;
  }
}
