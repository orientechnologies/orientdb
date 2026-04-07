package com.orientechnologies.orient.distributed.context.retryable;

import com.orientechnologies.orient.core.transaction.ODatabaseId;
import com.orientechnologies.orient.distributed.context.coordination.dbs.ONodeRole;
import com.orientechnologies.orient.distributed.context.coordination.message.operation.OAddNodeInfo;
import com.orientechnologies.orient.distributed.context.coordination.message.operation.ODeclareDbMessage;
import com.orientechnologies.orient.distributed.context.coordination.result.OAcceptResult;
import com.orientechnologies.orient.distributed.db.OCompleteExecution;
import com.orientechnologies.orient.distributed.db.OrientDBDistributed;
import java.util.Optional;
import java.util.stream.Collectors;

public record ODeclareDatabaseRetryOperation(ODatabaseId dbId, String name)
    implements ORetryOperation {
  @Override
  public void execute(
      OrientDBDistributed ctx, OCompleteExecution cmplete, Optional<OAcceptResult> previousResult) {
    var members =
        ctx.getNodeState().getNetworkMembers().stream()
            .map((n) -> new OAddNodeInfo(n, ONodeRole.Main))
            .collect(Collectors.toSet());
    int minimumQuorum = members.size() / 2 + 1;

    ctx.coordinatedOperation(new ODeclareDbMessage(name, dbId, members, minimumQuorum), cmplete);
  }
}
