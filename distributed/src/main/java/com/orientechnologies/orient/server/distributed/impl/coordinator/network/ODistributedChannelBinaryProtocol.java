package com.orientechnologies.orient.server.distributed.impl.coordinator.network;

import com.orientechnologies.orient.server.distributed.ORemoteServerController;
import com.orientechnologies.orient.server.distributed.impl.coordinator.*;
import com.orientechnologies.orient.server.distributed.impl.coordinator.network.ONetworkSubmitRequest;
import com.orientechnologies.orient.server.distributed.impl.coordinator.network.ONetworkSubmitResponse;
import com.orientechnologies.orient.server.distributed.impl.coordinator.network.OOperationRequest;
import com.orientechnologies.orient.server.distributed.impl.coordinator.network.OOperationResponse;

public class ODistributedChannelBinaryProtocol implements ODistributedChannel {

  private ORemoteServerController controller;

  public ODistributedChannelBinaryProtocol(ORemoteServerController controller) {
    this.controller = controller;
  }

  @Override
  public void sendRequest(OLogId id, ONodeRequest nodeRequest) {
    controller.sendBinaryRequest(new OOperationRequest(id, nodeRequest));
  }

  @Override
  public void sendResponse(OLogId id, ONodeResponse nodeResponse) {
    controller.sendBinaryRequest(new OOperationResponse(id, nodeResponse));

  }

  @Override
  public void submit(OSubmitRequest request) {
    controller.sendBinaryRequest(new ONetworkSubmitRequest(request));
  }

  @Override
  public void reply(OSubmitResponse response) {
    controller.sendBinaryRequest(new ONetworkSubmitResponse(response));
  }

}
