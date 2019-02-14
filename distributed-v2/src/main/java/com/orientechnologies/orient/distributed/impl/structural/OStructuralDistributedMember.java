package com.orientechnologies.orient.distributed.impl.structural;

import com.orientechnologies.orient.core.db.config.ONodeIdentity;
import com.orientechnologies.orient.distributed.impl.coordinator.ODistributedChannel;
import com.orientechnologies.orient.distributed.impl.coordinator.OLogId;
import com.orientechnologies.orient.distributed.impl.coordinator.transaction.OSessionOperationId;

public class OStructuralDistributedMember {
  private final ONodeIdentity       identity;
  private final ODistributedChannel channel;

  public OStructuralDistributedMember(ONodeIdentity identity, ODistributedChannel channel) {
    this.identity = identity;
    this.channel = channel;
  }

  public String getName() {
    return identity.getName();
  }

  public ONodeIdentity getIdentity() {
    return identity;
  }

  public ODistributedChannel getChannel() {
    return channel;
  }

  public void sendResponse(OLogId opId, OStructuralNodeResponse response) {
    channel.sendResponse(opId, response);
  }

  public void sendRequest(OLogId id, OStructuralNodeRequest nodeRequest) {
    channel.sendRequest(id, nodeRequest);
  }

  public void reply(OSessionOperationId operationId, OStructuralSubmitResponse response) {
    channel.reply(operationId, response);
  }

  public void submit(OSessionOperationId operationId, OStructuralSubmitRequest request) {
    channel.submit(operationId, request);
  }
}
