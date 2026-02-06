package com.orientechnologies.orient.distributed.context.coordination.action;

import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import com.orientechnologies.orient.distributed.context.coordination.OResponseCollector;
import com.orientechnologies.orient.distributed.context.coordination.OResponseCollectorMerge;
import com.orientechnologies.orient.distributed.context.coordination.message.OMergeConfirmOp;
import com.orientechnologies.orient.distributed.context.coordination.message.OMergeFailOp;
import com.orientechnologies.orient.distributed.context.coordination.message.operation.OOperationMessage;
import com.orientechnologies.orient.distributed.context.coordination.result.OAcceptResult;
import com.orientechnologies.orient.distributed.db.OCompleteExecution;
import com.orientechnologies.orient.distributed.db.OrientDBDistributed;
import com.orientechnologies.orient.server.distributed.OLoggerDistributed;
import java.util.Optional;
import java.util.Set;

public class OMergeCompleteAction extends OStandardCompleteAction implements OCompleteAction {

  private static final OLoggerDistributed logger =
      OLoggerDistributed.logger(OMergeCompleteAction.class);

  private final ONodeId mergeNode;

  public OMergeCompleteAction(
      OrientDBDistributed context,
      OOperationMessage operation,
      OCompleteExecution execution,
      ONodeId mergeNode) {
    super(context, operation, execution);
    this.mergeNode = mergeNode;
  }

  @Override
  public OResponseCollector newResponseCollector(
      OTransactionIdPromise promise, int quorum, Set<ONodeId> nodes) {
    return new OResponseCollectorMerge(this, promise, quorum, nodes, mergeNode);
  }

  @Override
  protected void retryOperation(int delay) {
    this.context.retryMergeOperationMessages(mergeNode, operation, this, delay);
  }

  @Override
  public void success(OTransactionIdPromise promise, Set<ONodeId> all) {
    this.context.sendMessage(mergeNode, new OMergeConfirmOp(promise));
    logger.debugNode(
        context.getNodeId(), "sending success merge promise %s to %s", promise, mergeNode);
    super.success(promise, all);
  }

  @Override
  public void failure(
      OTransactionIdPromise promise, Set<ONodeId> all, Optional<OAcceptResult> result) {
    this.context.sendMessage(mergeNode, new OMergeFailOp(promise));
    logger.debugNode(
        context.getNodeId(), "sending fail merge promise %s to %s", promise, mergeNode);
    super.failure(promise, all, result);
  }
}
