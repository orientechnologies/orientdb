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
import java.util.Optional;
import java.util.Set;

public class OMergeCompleteAction extends OStandardCompleteAction implements OCompleteAction {

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
  public void success(OTransactionIdPromise promise, Set<ONodeId> all) {
    this.context.sendMessage(mergeNode, new OMergeConfirmOp(promise));
    super.success(promise, all);
  }

  @Override
  public void failure(
      OTransactionIdPromise promise, Set<ONodeId> all, Optional<OAcceptResult> result) {
    this.context.sendMessage(mergeNode, new OMergeFailOp(promise));
    super.failure(promise, all, result);
  }
}
