package com.orientechnologies.orient.distributed.context.retryable;

import com.orientechnologies.orient.core.db.OCreateDatabaseParameters;
import com.orientechnologies.orient.core.db.config.OAddNodeInfo;
import com.orientechnologies.orient.core.transaction.ODatabaseId;
import com.orientechnologies.orient.distributed.context.coordination.message.operation.ODeclareDbMessage;
import com.orientechnologies.orient.distributed.context.coordination.result.OAcceptResult;
import com.orientechnologies.orient.distributed.db.OCompleteExecution;
import com.orientechnologies.orient.distributed.db.OrientDBDistributed;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public record ODeclareDatabaseRetryOperation(
    ODatabaseId dbId, String name, OCreateDatabaseParameters parameters)
    implements ORetryOperation {
  @Override
  public void execute(
      OrientDBDistributed ctx, OCompleteExecution cmplete, Optional<OAcceptResult> previousResult) {
    Set<OAddNodeInfo> members;
    Optional<Set<OAddNodeInfo>> parMembers =
        Optional.ofNullable(this.parameters()).flatMap(OCreateDatabaseParameters::members);
    if (parMembers.isEmpty()) {
      members =
          ctx.getNodeState().getNetworkMembers().stream()
              .map(OAddNodeInfo::main)
              .collect(Collectors.toSet());
    } else {
      members = parMembers.get();
    }
    int minimumQuorum = members.size() / 2 + 1;

    ctx.coordinatedOperation(new ODeclareDbMessage(name, dbId, members, minimumQuorum), cmplete);
  }
}
