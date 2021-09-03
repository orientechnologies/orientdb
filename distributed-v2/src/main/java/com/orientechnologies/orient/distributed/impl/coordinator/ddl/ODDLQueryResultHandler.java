package com.orientechnologies.orient.distributed.impl.coordinator.ddl;

import com.orientechnologies.orient.core.db.config.ONodeIdentity;
import com.orientechnologies.orient.distributed.impl.coordinator.ODistributedCoordinator;
import com.orientechnologies.orient.distributed.impl.coordinator.ONodeResponse;
import com.orientechnologies.orient.distributed.impl.coordinator.ORequestContext;
import com.orientechnologies.orient.distributed.impl.coordinator.OResponseHandler;
import com.orientechnologies.orient.distributed.impl.coordinator.transaction.OSessionOperationId;

public class ODDLQueryResultHandler implements OResponseHandler {

  private ONodeIdentity sender;
  private OSessionOperationId operationId;

  public ODDLQueryResultHandler(ONodeIdentity sender, OSessionOperationId operationId) {
    this.sender = sender;
    this.operationId = operationId;
  }

  @Override
  public boolean receive(
      ODistributedCoordinator coordinator,
      ORequestContext context,
      ONodeIdentity member,
      ONodeResponse response) {
    if (context.getInvolvedMembers().size() == context.getResponses().size()) {
      coordinator.reply(sender, operationId, new ODDLQuerySubmitResponse());
    }
    return false;
  }

  @Override
  public boolean timeout(ODistributedCoordinator coordinator, ORequestContext context) {
    return false;
  }
}
