package com.orientechnologies.orient.distributed.context.retryable;

import com.orientechnologies.orient.core.transaction.ODatabaseId;
import com.orientechnologies.orient.distributed.context.coordination.message.operation.OSetDatabaseQuorum;
import com.orientechnologies.orient.distributed.context.coordination.result.OAcceptResult;
import com.orientechnologies.orient.distributed.db.OCompleteExecution;
import com.orientechnologies.orient.distributed.db.OrientDBDistributed;
import java.util.Optional;

public record OSetDatabaseQuorumRetryOperation(ODatabaseId dbId, int newQuorum)
    implements ORetryOperation {

  @Override
  public void execute(
      OrientDBDistributed context,
      OCompleteExecution execution,
      Optional<OAcceptResult> previousResult) {
    var version = context.getNodeState().getOps().nextDatabaseVersion(dbId);
    context.coordinatedOperation(new OSetDatabaseQuorum(dbId, newQuorum, version), execution);
  }
}
