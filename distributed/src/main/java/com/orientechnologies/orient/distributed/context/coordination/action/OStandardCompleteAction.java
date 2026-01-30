package com.orientechnologies.orient.distributed.context.coordination.action;

import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import com.orientechnologies.orient.distributed.context.coordination.OResponseCollector;
import com.orientechnologies.orient.distributed.context.coordination.OResponseCollectorImpl;
import com.orientechnologies.orient.distributed.context.coordination.message.OConfirmOp;
import com.orientechnologies.orient.distributed.context.coordination.message.OFailOp;
import com.orientechnologies.orient.distributed.context.coordination.message.operation.OOperationMessage;
import com.orientechnologies.orient.distributed.context.coordination.result.OAcceptResult;
import com.orientechnologies.orient.distributed.db.OCompleteExecution;
import com.orientechnologies.orient.distributed.db.OrientDBDistributed;
import com.orientechnologies.orient.server.distributed.OLoggerDistributed;
import java.text.MessageFormat;
import java.util.Optional;
import java.util.Set;

public class OStandardCompleteAction implements OCompleteAction {

  private static final OLoggerDistributed logger =
      OLoggerDistributed.logger(OStandardCompleteAction.class);

  protected final OrientDBDistributed context;
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
        logger.infoNode(
            context.getNodeId(),
            "failed coordination of %s whit result %s, scheduling retry with delay %d",
            operation,
            result,
            delay.get());
        this.context.retryOperation(operation, this, delay.get());
      } else {
        logger.infoNode(
            context.getNodeId(), "failed coordination of %s with %s", operation, result);
        this.execution.complete(result);
      }
    } else {
      if (result.isPresent()) {
        logger.infoNode(
            context.getNodeId(), "failed coordination of %s with %s", operation, result);
      } else {
        logger.debugNode(
            context.getNodeId(), "complete coordination of %s with %s", operation, result);
      }
      this.execution.complete(result);
    }
  }

  @Override
  public void complete(
      OTransactionIdPromise promise, Set<ONodeId> nodes, Optional<OAcceptResult> result) {
    logger.debugNode(context.getNodeId(), "complete coordination %s with %s", operation, result);
    this.execution.complete(result);
  }

  @Override
  public OResponseCollector newResponseCollector(
      OTransactionIdPromise promise, int quorum, Set<ONodeId> nodes) {
    return new OResponseCollectorImpl(this, promise, quorum, nodes);
  }

  @Override
  public String toString() {
    return MessageFormat.format(
        "Complete Action For {0} state {1}", this.operation, this.execution);
  }
}
