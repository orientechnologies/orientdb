package com.orientechnologies.orient.distributed.context;

import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import com.orientechnologies.orient.distributed.context.coordination.message.OConfirmOp;
import com.orientechnologies.orient.distributed.context.coordination.message.OFailOp;
import com.orientechnologies.orient.distributed.context.coordination.message.operation.OOperationMessage;
import com.orientechnologies.orient.distributed.context.coordination.result.OAcceptResult;
import com.orientechnologies.orient.distributed.db.OCompleteExecution;
import com.orientechnologies.orient.distributed.db.OrientDBDistributed;
import com.orientechnologies.orient.server.distributed.OLoggerDistributed;
import java.util.Optional;
import java.util.Set;

public final class OStandardCompleteAction implements OCompleteAction {

  private static final OLoggerDistributed logger =
      OLoggerDistributed.logger(OStandardCompleteAction.class);

  private final OrientDBDistributed context;
  private final OOperationMessage operation;
  private final OCompleteExecution execution;

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
        logger.debug("retry coordination for result %s, delay %d", result, delay.get());
        this.context.retryOperation(operation, this, delay.get());
      } else {
        logger.debug("complete coordination with %s", result);
        this.execution.complete(result);
      }
    } else {
      logger.debug("complete coordination with %s", result);
      this.execution.complete(result);
    }
  }

  @Override
  public void complete(
      OTransactionIdPromise promise, Set<ONodeId> nodes, Optional<OAcceptResult> result) {
    logger.debug("complete coordination with %s", result.toString());
    this.execution.complete(result);
  }
}
