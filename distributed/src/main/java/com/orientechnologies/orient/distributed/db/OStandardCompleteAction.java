package com.orientechnologies.orient.distributed.db;

import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import com.orientechnologies.orient.distributed.context.OCompleteAction;
import com.orientechnologies.orient.distributed.context.coordination.message.OConfirmOp;
import com.orientechnologies.orient.distributed.context.coordination.message.OFailOp;
import com.orientechnologies.orient.distributed.context.coordination.message.operation.OOperationMessage;
import com.orientechnologies.orient.distributed.context.coordination.result.OAcceptResult;
import java.util.Optional;
import java.util.Set;

public final class OStandardCompleteAction implements OCompleteAction {
  private final OrientDBDistributed context;
  private OOperationMessage operation;
  private int retryCountDown;
  private int delay;

  public OStandardCompleteAction(
      OrientDBDistributed context, OOperationMessage operation, int retryCountDown, int delay) {
    this.context = context;
    this.operation = operation;
    this.retryCountDown = retryCountDown;
    this.delay = delay;
  }

  @Override
  public void success(OTransactionIdPromise promise, Set<ONodeId> all) {
    this.context.sendMessage(all, new OConfirmOp(promise));
  }

  @Override
  public void failure(
      OTransactionIdPromise promise, Set<ONodeId> all, Optional<OAcceptResult> result) {
    this.context.sendMessage(all, new OFailOp(promise));
    if (result.isPresent()) {
      if (result.get().canRetry()) {
        this.context.retryOperation(operation, retryCountDown, delay);
      }
    }
  }
}
