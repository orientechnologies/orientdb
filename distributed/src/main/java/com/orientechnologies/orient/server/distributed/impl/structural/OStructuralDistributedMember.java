package com.orientechnologies.orient.server.distributed.impl.structural;

import com.orientechnologies.orient.server.distributed.impl.coordinator.ODistributedChannel;
import com.orientechnologies.orient.server.distributed.impl.coordinator.OLogId;
import com.orientechnologies.orient.server.distributed.impl.coordinator.transaction.OSessionOperationId;

public class OStructuralDistributedMember {
  private final String              name;
  private final ODistributedChannel channel;

  public OStructuralDistributedMember(String name, ODistributedChannel channel) {
    this.name = name;
    this.channel = channel;
  }

  public String getName() {
    return name;
  }

  public ODistributedChannel getChannel() {
    return channel;
  }

  public void sendResponse(OLogId opId, OStructuralNodeResponse response) {
    channel.sendResponse(opId,response);
  }

  public void sendRequest(OLogId id, OStructuralNodeRequest nodeRequest) {
    channel.sendRequest(id,nodeRequest);
  }

  public void reply(OSessionOperationId operationId, OStructuralSubmitResponse response) {
    channel.reply(operationId,response);
  }

  public void submit(OSessionOperationId operationId, OStructuralSubmitRequest request) {
    channel.submit(operationId,request);
  }
}
