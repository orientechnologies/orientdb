package com.orientechnologies.orient.server.distributed.impl.coordinator.network;

import com.orientechnologies.orient.server.distributed.ORemoteServerController;
import com.orientechnologies.orient.server.distributed.impl.coordinator.*;

public class ODistributedChannelBinaryProtocol implements ODistributedChannel {

  private String                  nodeName;
  private ORemoteServerController controller;

  public ODistributedChannelBinaryProtocol(String nodeName, ORemoteServerController remoteServer) {
    this.nodeName = nodeName;
    this.controller = remoteServer;
  }

  @Override
  public void sendRequest(String database, OLogId id, ONodeRequest nodeRequest) {
    controller.sendBinaryRequest(new OOperationRequest(nodeName, database, id, nodeRequest));
  }

  @Override
  public void sendResponse(String database, OLogId id, ONodeResponse nodeResponse) {
    controller.sendBinaryRequest(new OOperationResponse(nodeName, database, id, nodeResponse));
  }

  @Override
  public void submit(String database, OSubmitRequest request) {
    controller.sendBinaryRequest(new ONetworkSubmitRequest(nodeName, database, request));
  }

  @Override
  public void reply(String database, OSubmitResponse response) {
    controller.sendBinaryRequest(new ONetworkSubmitResponse(nodeName, database, response));
  }

}
