package com.orientechnologies.orient.distributed.context.retryable;

import com.orientechnologies.orient.core.db.config.OAddNodeInfo;
import com.orientechnologies.orient.core.transaction.ODatabaseId;
import com.orientechnologies.orient.distributed.context.coordination.OCoordinatedDistributedOps;
import com.orientechnologies.orient.distributed.context.coordination.message.operation.OAddDatabaseMembers;
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
    OCoordinatedDistributedOps ops = ctx.getNodeState().getOps();
    var version = ops.nextDatabaseVersion(id);
    List<OAddNodeInfo> nodes =
        this.nodes.stream()
            .filter((n) -> !ops.getDatabaseTopology().getMembers(id).contains(n.node()))
            .toList();
    if (!nodes.isEmpty()) {
      ctx.coordinatedOperation(new OAddDatabaseMembers(version, id, nodes), op);
    }
  }
}
