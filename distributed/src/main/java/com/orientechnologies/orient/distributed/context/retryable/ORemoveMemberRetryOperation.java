package com.orientechnologies.orient.distributed.context.retryable;

import com.orientechnologies.orient.core.id.ODatabaseId;
import com.orientechnologies.orient.core.id.ONodeId;
import com.orientechnologies.orient.distributed.context.coordination.message.operation.ORemoveDatabaseMembers;
import com.orientechnologies.orient.distributed.context.coordination.result.OAcceptResult;
import com.orientechnologies.orient.distributed.db.OCompleteExecution;
import com.orientechnologies.orient.distributed.db.OrientDBDistributed;
import java.util.List;
import java.util.Optional;

public record ORemoveMemberRetryOperation(ODatabaseId databaseId, List<ONodeId> nodes)
    implements ORetryOperation {

  @Override
  public void execute(
      OrientDBDistributed context,
      OCompleteExecution execution,
      Optional<OAcceptResult> previousResult) {
    var version = context.getNodeState().getOps().nextDatabaseVersion(databaseId);
    context.coordinatedOperation(new ORemoveDatabaseMembers(databaseId, nodes, version), execution);
  }
}
