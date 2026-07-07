package com.orientechnologies.orient.distributed.context.simulator;

import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import com.orientechnologies.orient.distributed.context.coordination.OCoordinatedDistributedOps;
import com.orientechnologies.orient.distributed.context.coordination.OResponseCollector;
import com.orientechnologies.orient.distributed.context.coordination.OResponseCollectorImpl;
import com.orientechnologies.orient.distributed.context.coordination.action.OCompleteAction;
import com.orientechnologies.orient.distributed.context.coordination.result.OAcceptResult;
import java.util.Optional;
import java.util.Set;

public class TestAction implements OCompleteAction {
  boolean success = false;
  boolean failure = false;
  boolean complete = false;
  private Optional<OAcceptResult> result;
  private final OCoordinatedDistributedOps ops;

  public TestAction(OCoordinatedDistributedOps ops) {
    this.ops = ops;
  }

  @Override
  public void success(OTransactionIdPromise promise, Set<ONodeId> expected) {
    success = true;
  }

  @Override
  public void failure(
      OTransactionIdPromise promise, Set<ONodeId> expected, Optional<OAcceptResult> result) {
    failure = true;
    this.result = result;
  }

  @Override
  public void complete(
      OTransactionIdPromise promise, Set<ONodeId> nodes, Optional<OAcceptResult> result) {
    this.complete = true;
    this.result = result;
    this.ops.completeExecution(promise);
  }

  @Override
  public OResponseCollector newResponseCollector(
      OTransactionIdPromise promise, int quorum, Set<ONodeId> nodes) {
    return new OResponseCollectorImpl(this, promise, quorum, nodes);
  }

  public Optional<OAcceptResult> getResult() {
    return result;
  }

  public boolean isFailure() {
    return failure;
  }

  public boolean isComplete() {
    return complete;
  }

  public boolean isSuccess() {
    return success;
  }
}
