package com.orientechnologies.orient.server.distributed.impl.coordinator;

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

  public void reply(OSubmitResponse response) {
    channel.reply(database, response);
  }

  public void sendResponse(OLogId opId, ONodeResponse response) {
    channel.sendResponse(database, opId, response);
  }
}
