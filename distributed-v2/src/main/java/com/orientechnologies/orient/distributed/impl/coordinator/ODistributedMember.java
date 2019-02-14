package com.orientechnologies.orient.distributed.impl.coordinator;

import com.orientechnologies.orient.core.db.config.ONodeIdentity;
import com.orientechnologies.orient.distributed.impl.coordinator.transaction.OSessionOperationId;

public class ODistributedMember {

  private ONodeIdentity       nodeIdentity;
  private String              database;
  private ODistributedChannel channel;

  public ODistributedMember(ONodeIdentity nodeIdentity, String database, ODistributedChannel channel) {
    this.nodeIdentity = nodeIdentity;
    this.database = database;
    this.channel = channel;
  }

  public ONodeIdentity getNodeIdentity() {
    return nodeIdentity;
  }

  public void sendRequest(OLogId id, ONodeRequest nodeRequest) {
    channel.sendRequest(database, id, nodeRequest);
  }

  public void reply(OSessionOperationId operationId, OSubmitResponse response) {
    channel.reply(database, operationId, response);
  }

  public void sendResponse(OLogId opId, ONodeResponse response) {
    channel.sendResponse(database, opId, response);
  }

  public void submit(OSessionOperationId operationId, OSubmitRequest request) {
    channel.submit(database, operationId, request);
  }
}
