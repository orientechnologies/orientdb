package com.orientechnologies.orient.distributed.context.retryable;

import com.orientechnologies.orient.core.transaction.ODatabaseId;
import com.orientechnologies.orient.distributed.context.coordination.message.operation.OAddDatabaseMembers;
import com.orientechnologies.orient.distributed.context.coordination.message.operation.OAddNodeInfo;
import com.orientechnologies.orient.distributed.context.coordination.result.OAcceptResult;
import com.orientechnologies.orient.distributed.db.OCompleteExecution;
import com.orientechnologies.orient.distributed.db.OrientDBDistributed;
import java.util.List;
import java.util.Optional;

public record OAddDatabaseMembersRetryOperation(List<OAddNodeInfo> nodes, ODatabaseId id)
    implements ORetryOperation {
  @Override
  public void execute(
      OrientDBDistributed ctx, OCompleteExecution op, Optional<OAcceptResult> previousResult) {
    var version = ctx.getNodeState().getOps().nextDatabaseVersion(id);
    ctx.coordinatedOperation(new OAddDatabaseMembers(version, id, nodes), op);
  }
}
