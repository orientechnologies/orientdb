package com.orientechnologies.orient.distributed.db;

import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import com.orientechnologies.orient.distributed.context.OCompleteAction;
import com.orientechnologies.orient.distributed.context.coordination.message.OConfirmOp;
import com.orientechnologies.orient.distributed.context.coordination.message.OFailOp;
import java.util.Set;

final class OStandardCompleteAction implements OCompleteAction {
  private final OrientDBDistributed context;

  public OStandardCompleteAction(OrientDBDistributed context) {
    this.context = context;
  }

  @Override
  public void success(OTransactionIdPromise promise, Set<ONodeId> all) {
    this.context.sendMessage(all, new OConfirmOp(promise));
  }

  @Override
  public void failure(OTransactionIdPromise promise, Set<ONodeId> all) {
    this.context.sendMessage(all, new OFailOp(promise));
  }
}
