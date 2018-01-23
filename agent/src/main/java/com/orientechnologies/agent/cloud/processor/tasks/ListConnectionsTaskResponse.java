package com.orientechnologies.agent.cloud.processor.tasks;

import com.orientechnologies.orientdb.cloud.protocol.ServerConnections;

public class ListConnectionsTaskResponse extends AbstractRPCTaskResponse<ServerConnections> {

  public ListConnectionsTaskResponse() {
  }

  public ListConnectionsTaskResponse(ServerConnections connections) {
    super(connections);
  }

  @Override
  protected Class<ServerConnections> getPayloadType() {
    return ServerConnections.class;
  }

}
