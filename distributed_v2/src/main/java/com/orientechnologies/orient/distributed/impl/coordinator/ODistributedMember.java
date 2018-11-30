package com.orientechnologies.orient.distributed.impl.coordinator;

import com.orientechnologies.orient.distributed.impl.coordinator.transaction.OSessionOperationId;

public class ODistributedMember {

  private String              name;
  private String              database;
  private ODistributedChannel channel;

  public ODistributedMember(String name, String database, ODistributedChannel channel) {
    this.name = name;
    this.database = database;
    this.channel = channel;
  }

  public String getName() {
    return name;
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
