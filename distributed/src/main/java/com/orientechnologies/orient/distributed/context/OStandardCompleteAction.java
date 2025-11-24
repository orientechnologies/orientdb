package com.orientechnologies.orient.distributed.context;

import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import com.orientechnologies.orient.distributed.context.coordination.message.OConfirmOp;
import com.orientechnologies.orient.distributed.context.coordination.message.OFailOp;
import com.orientechnologies.orient.distributed.context.coordination.message.operation.OOperationMessage;
import com.orientechnologies.orient.distributed.context.coordination.result.OAcceptResult;
import com.orientechnologies.orient.distributed.db.OrientDBDistributed;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

public final class OStandardCompleteAction implements OCompleteAction {
  private final OrientDBDistributed context;
  private OOperationMessage operation;
  private CompletableFuture<Optional<OAcceptResult>> result;
  private ORetryInfo retry;

  public OStandardCompleteAction(
      OrientDBDistributed context, OOperationMessage operation, ORetryInfo retry) {
    this.context = context;
    this.operation = operation;
    this.retry = retry;
    this.result = new CompletableFuture<Optional<OAcceptResult>>();
  }

  @Override
  public void success(OTransactionIdPromise promise, Set<ONodeId> all) {
    this.context.sendMessage(all, new OConfirmOp(promise));
  }

  @Override
  public void failure(
      OTransactionIdPromise promise, Set<ONodeId> all, Optional<OAcceptResult> result) {
    this.context.sendMessage(all, new OFailOp(promise));
    if (result.isPresent() && result.get().executeRetry()) {
      var delay = retry.nextRetry();
      if (delay.isPresent()) {
        this.context.retryOperation(operation, this, delay.get());
      } else {
        this.result.complete(result);
      }
    } else {
      this.result.complete(result);
    }
  }

  @Override
  public void complete(
      OTransactionIdPromise promise, Set<ONodeId> nodes, Optional<OAcceptResult> result) {
    this.result.complete(result);
  }

  public Future<Optional<OAcceptResult>> getResult() {
    return result;
  }
}
