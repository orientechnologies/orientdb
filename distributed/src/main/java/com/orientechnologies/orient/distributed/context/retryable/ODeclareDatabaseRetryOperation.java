package com.orientechnologies.orient.distributed.context.retryable;

import com.orientechnologies.orient.core.transaction.ODatabaseId;
import com.orientechnologies.orient.distributed.context.coordination.message.operation.OAddNodeInfo;
import com.orientechnologies.orient.distributed.context.coordination.message.operation.ODeclareDbMessage;
import com.orientechnologies.orient.distributed.db.OCompleteExecution;
import com.orientechnologies.orient.distributed.db.OrientDBDistributed;
import java.util.Set;

public record ODeclareDatabaseRetryOperation(
    ODatabaseId dbId, String name, Set<OAddNodeInfo> currentMembers, int minimumQuorum)
    implements ORetryOperation {
  @Override
  public void execute(OrientDBDistributed ctx, OCompleteExecution cmplete) {
    ctx.coordinatedOperation(
        new ODeclareDbMessage(name, dbId, currentMembers, minimumQuorum), cmplete);
  }
}
