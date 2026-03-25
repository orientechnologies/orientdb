package com.orientechnologies.orient.distributed.context.retryable;

import com.orientechnologies.orient.distributed.context.coordination.message.state.ONodeStateNetwork;
import com.orientechnologies.orient.distributed.context.coordination.result.OAcceptResult;
import com.orientechnologies.orient.distributed.context.coordination.result.OCannotMerge;
import com.orientechnologies.orient.distributed.context.coordination.topology.ODiscoverAction;
import com.orientechnologies.orient.distributed.db.OCompleteExecution;
import com.orientechnologies.orient.distributed.db.OrientDBDistributed;
import java.util.Optional;

public record ODiscoverActionRetryOperation(ODiscoverAction action, ONodeStateNetwork state)
    implements ORetryOperation {

  @Override
  public void execute(
      OrientDBDistributed ctx,
      OCompleteExecution complete,
      Optional<OAcceptResult> previousResult) {
    var st = state;
    if (previousResult.isPresent() && previousResult.get() instanceof OCannotMerge) {
      st = ((OCannotMerge) previousResult.get()).currentState();
    }
    action.execute(ctx, st, complete);
  }
}
