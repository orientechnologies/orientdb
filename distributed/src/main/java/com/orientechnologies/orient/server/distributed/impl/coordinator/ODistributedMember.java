package com.orientechnologies.orient.server.distributed.impl.coordinator;

public class ODistributedMember {

  private String              name;
  private ODistributedChannel channel;

  public ODistributedMember(String name, ODistributedChannel channel) {
    this.name = name;
    this.channel = channel;
  }

  public String getName() {
    return name;
  }

  public void sendRequest(OLogId id, ONodeRequest nodeRequest) {
    channel.sendRequest(id, nodeRequest);
  }

  public void reply(OSubmitResponse response) {
    channel.reply(response);
  }

  public void sendResponse(OLogId opId, ONodeResponse response) {
    channel.sendResponse(opId, response);
  }
}
