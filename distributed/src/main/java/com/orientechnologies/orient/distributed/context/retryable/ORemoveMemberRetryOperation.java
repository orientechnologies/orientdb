package com.orientechnologies.orient.distributed.context.retryable;

import com.orientechnologies.orient.core.transaction.ODatabaseId;
import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.distributed.context.coordination.message.operation.ORemoveDatabaseMember;
import com.orientechnologies.orient.distributed.db.OCompleteExecution;
import com.orientechnologies.orient.distributed.db.OrientDBDistributed;
import java.util.List;

public record ORemoveMemberRetryOperation(ODatabaseId databaseId, List<ONodeId> nodes)
    implements ORetryOperation {

  @Override
  public void execute(OrientDBDistributed context, OCompleteExecution execution) {
    var version = context.getNodeState().getOps().nextDatabaseVersion(databaseId);
    context.coordinatedOperation(new ORemoveDatabaseMember(databaseId, nodes, version), execution);
  }
}
