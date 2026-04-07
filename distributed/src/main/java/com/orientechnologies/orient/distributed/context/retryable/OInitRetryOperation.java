package com.orientechnologies.orient.distributed.context.retryable;

import com.orientechnologies.orient.distributed.context.coordination.result.OAcceptResult;
import com.orientechnologies.orient.distributed.context.coordination.topology.ODiscoverAction;
import com.orientechnologies.orient.distributed.db.OCompleteExecution;
import com.orientechnologies.orient.distributed.db.OrientDBDistributed;
import java.util.Optional;

public record OInitRetryOperation(ODiscoverAction action) implements ORetryOperation {

  @Override
  public void execute(
      OrientDBDistributed context,
      OCompleteExecution execution,
      Optional<OAcceptResult> previousResult) {
    action.execute(context, null, execution);
  }
}
