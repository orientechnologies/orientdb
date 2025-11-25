package com.orientechnologies.orient.distributed.context;

import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import com.orientechnologies.orient.distributed.context.coordination.message.OConfirmOp;
import com.orientechnologies.orient.distributed.context.coordination.message.OFailOp;
import com.orientechnologies.orient.distributed.context.coordination.message.operation.OOperationMessage;
import com.orientechnologies.orient.distributed.context.coordination.result.OAcceptResult;
import com.orientechnologies.orient.distributed.db.OCompleteExecution;
import com.orientechnologies.orient.distributed.db.OrientDBDistributed;
import java.util.Optional;
import java.util.Set;

public final class OStandardCompleteAction implements OCompleteAction {
  private final OrientDBDistributed context;
  private OOperationMessage operation;

  private OCompleteExecution execution;

  public OStandardCompleteAction(
      OrientDBDistributed context, OOperationMessage operation, OCompleteExecution execution) {
    this.context = context;
    this.operation = operation;
    this.execution = execution;
  }

  @Override
  public void success(OTransactionIdPromise promise, Set<ONodeId> all) {
    this.context.sendMessage(all, new OConfirmOp(promise));
  }

  @Override
  public void failure(
      OTransactionIdPromise promise, Set<ONodeId> all, Optional<OAcceptResult> result) {
    this.context.sendMessage(all, new OFailOp(promise));
    if (result.isPresent() && result.get().consensusRetry()) {
      var delay = execution.getRetryInfo().nextRetry();
      if (delay.isPresent()) {
        this.context.retryOperation(operation, this, delay.get());
      } else {
        this.execution.complete(result);
      }
    } else {
      this.execution.complete(result);
    }
  }

  @Override
  public void complete(
      OTransactionIdPromise promise, Set<ONodeId> nodes, Optional<OAcceptResult> result) {
    this.execution.complete(result);
  }
}
