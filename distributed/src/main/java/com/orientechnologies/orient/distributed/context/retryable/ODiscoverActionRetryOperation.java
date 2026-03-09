package com.orientechnologies.orient.distributed.context.retryable;

import com.orientechnologies.orient.distributed.context.coordination.message.state.ONodeStateNetwork;
import com.orientechnologies.orient.distributed.context.coordination.topology.ODiscoverAction;
import com.orientechnologies.orient.distributed.db.OCompleteExecution;
import com.orientechnologies.orient.distributed.db.OrientDBDistributed;

public record ODiscoverActionRetryOperation(ODiscoverAction action, ONodeStateNetwork state)
    implements ORetryOperation {

  @Override
  public void execute(OrientDBDistributed ctx, OCompleteExecution complete) {
    action.execute(ctx, state, complete);
  }
}
