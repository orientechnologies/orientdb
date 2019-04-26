package com.orientechnologies.orient.distributed.impl.structural.raft;

import com.orientechnologies.orient.distributed.impl.coordinator.OLogId;

import java.util.TimerTask;

class ORaftOperationTimeoutTimerTask extends TimerTask {
  private       OStructuralMaster master;
  private final OLogId            id;

  public ORaftOperationTimeoutTimerTask(OStructuralMaster master, OLogId id) {
    this.master = master;
    this.id = id;
  }

  @Override
  public void run() {
    master.operationTimeout(id, this);
  }

}
