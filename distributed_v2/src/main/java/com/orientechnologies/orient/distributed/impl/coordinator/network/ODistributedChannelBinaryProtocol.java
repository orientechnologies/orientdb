package com.orientechnologies.orient.distributed.impl.coordinator.network;

import com.orientechnologies.orient.server.distributed.ORemoteServerController;
import com.orientechnologies.orient.distributed.impl.coordinator.*;
import com.orientechnologies.orient.distributed.impl.coordinator.transaction.OSessionOperationId;
import com.orientechnologies.orient.distributed.impl.structural.OStructuralNodeRequest;
import com.orientechnologies.orient.distributed.impl.structural.OStructuralNodeResponse;
import com.orientechnologies.orient.distributed.impl.structural.OStructuralSubmitRequest;
import com.orientechnologies.orient.distributed.impl.structural.OStructuralSubmitResponse;

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
  public void sendResponse(OLogId opId, OStructuralNodeResponse response) {
    controller.sendBinaryRequest(new OStructuralOperationResponse(nodeName, opId, response));
  }

  @Override
  public void sendRequest(OLogId id, OStructuralNodeRequest request) {

  }

  @Override
  public void reply(OSessionOperationId operationId, OStructuralSubmitResponse response) {

  }

  @Override
  public void submit(OSessionOperationId operationId, OStructuralSubmitRequest request) {

  }

  @Override
  public void submit(String database, OSessionOperationId operationId, OSubmitRequest request) {
    controller.sendBinaryRequest(new ONetworkSubmitRequest(nodeName, database, operationId, request));
  }

  @Override
  public void reply(String database, OSessionOperationId operationId, OSubmitResponse response) {
    controller.sendBinaryRequest(new ONetworkSubmitResponse(nodeName, database, operationId, response));
  }

}
