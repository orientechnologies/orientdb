package com.orientechnologies.orient.distributed.impl.structural.raft;

import com.orientechnologies.orient.distributed.impl.log.OLogId;
import java.util.TimerTask;

class ORaftOperationTimeoutTimerTask extends TimerTask {
  private OStructuralLeader leader;
  private final OLogId id;

  public ORaftOperationTimeoutTimerTask(OStructuralLeader leader, OLogId id) {
    this.leader = leader;
    this.id = id;
  }

  @Override
  public void run() {
    leader.operationTimeout(id, this);
  }
}
