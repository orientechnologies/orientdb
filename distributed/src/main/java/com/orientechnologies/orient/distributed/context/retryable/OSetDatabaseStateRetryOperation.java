package com.orientechnologies.orient.distributed.context.retryable;

import com.orientechnologies.orient.core.transaction.ODatabaseId;
import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.distributed.context.coordination.dbs.ODatabaseState;
import com.orientechnologies.orient.distributed.context.coordination.message.operation.OSetDatabaseState;
import com.orientechnologies.orient.distributed.context.coordination.result.OAcceptResult;
import com.orientechnologies.orient.distributed.db.OCompleteExecution;
import com.orientechnologies.orient.distributed.db.OrientDBDistributed;
import java.util.Optional;

public record OSetDatabaseStateRetryOperation(ONodeId node, ODatabaseId dbId, ODatabaseState state)
    implements ORetryOperation {

  @Override
  public void execute(
      OrientDBDistributed ctx,
      OCompleteExecution complete,
      Optional<OAcceptResult> previousResult) {
    var version = ctx.getNodeState().getOps().nextDatabaseVersion(dbId);
    ctx.coordinatedOperation(new OSetDatabaseState(dbId, node, state, version), complete);
  }
}
