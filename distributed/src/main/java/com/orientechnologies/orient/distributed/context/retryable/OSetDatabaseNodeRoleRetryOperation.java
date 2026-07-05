package com.orientechnologies.orient.distributed.context.retryable;

import com.orientechnologies.orient.core.transaction.ODatabaseId;
import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.core.transaction.ONodeRole;
import com.orientechnologies.orient.distributed.context.coordination.message.operation.OSetDatabaseMemberRole;
import com.orientechnologies.orient.distributed.context.coordination.result.OAcceptResult;
import com.orientechnologies.orient.distributed.db.OCompleteExecution;
import com.orientechnologies.orient.distributed.db.OrientDBDistributed;
import java.util.Optional;

public record OSetDatabaseNodeRoleRetryOperation(ODatabaseId dbId, ONodeId node, ONodeRole role)
    implements ORetryOperation {

  @Override
  public void execute(
      OrientDBDistributed ctx,
      OCompleteExecution execution,
      Optional<OAcceptResult> previousResult) {
    var version = ctx.getNodeState().getOps().nextDatabaseVersion(dbId);
    ctx.coordinatedOperation(new OSetDatabaseMemberRole(dbId, node, role, version), execution);
  }
}
