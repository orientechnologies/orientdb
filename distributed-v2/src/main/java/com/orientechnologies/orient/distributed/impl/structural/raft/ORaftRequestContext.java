package com.orientechnologies.orient.distributed.impl.structural.raft;

import com.orientechnologies.orient.core.db.config.ONodeIdentity;
import java.util.HashSet;
import java.util.Set;

public class ORaftRequestContext {
  private Set<ONodeIdentity> answers = new HashSet<>();
  private ORaftOperation operation;
  private int quorum;
  private OStructuralLeader.OpFinished finished;

  public ORaftRequestContext(
      ORaftOperation operation, int quorum, OStructuralLeader.OpFinished finished) {
    this.finished = finished;
    this.operation = operation;
    this.quorum = quorum;
  }

  public boolean ack(ONodeIdentity node, OStructuralLeader context) {
    answers.add(node);
    if (answers.size() >= quorum) {
      operation.apply(context.getOrientDB());
      finished.finished();
      return true;
    }
    return false;
  }

  public boolean timeout() {
    return true;
  }
}
