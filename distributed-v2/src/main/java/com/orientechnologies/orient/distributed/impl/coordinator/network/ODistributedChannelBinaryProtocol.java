package com.orientechnologies.orient.distributed.impl.coordinator.network;

import com.orientechnologies.orient.core.db.config.ONodeIdentity;
import com.orientechnologies.orient.distributed.impl.structural.raft.OFullConfiguration;
import com.orientechnologies.orient.distributed.impl.structural.raft.ORaftOperation;
import com.orientechnologies.orient.server.distributed.ORemoteServerController;
import com.orientechnologies.orient.distributed.impl.coordinator.*;
import com.orientechnologies.orient.distributed.impl.coordinator.transaction.OSessionOperationId;
import com.orientechnologies.orient.distributed.impl.structural.OStructuralNodeRequest;
import com.orientechnologies.orient.distributed.impl.structural.OStructuralNodeResponse;
import com.orientechnologies.orient.distributed.impl.structural.OStructuralSubmitRequest;
import com.orientechnologies.orient.distributed.impl.structural.OStructuralSubmitResponse;

public class ODistributedChannelBinaryProtocol implements ODistributedChannel {

  private ONodeIdentity           nodeIdentity;
  private ORemoteServerController controller;

  public ODistributedChannelBinaryProtocol(ONodeIdentity nodeIdentity, ORemoteServerController remoteServer) {
    this.nodeIdentity = nodeIdentity;
    this.controller = remoteServer;
  }

  @Override
  public void sendRequest(String database, OLogId id, ONodeRequest nodeRequest) {
    controller.sendBinaryRequest(new OOperationRequest(nodeIdentity, database, id, nodeRequest));
  }

  @Override
  public void sendResponse(String database, OLogId id, ONodeResponse nodeResponse) {
    controller.sendBinaryRequest(new OOperationResponse(nodeIdentity, database, id, nodeResponse));
  }

  @Override
  public void sendResponse(OLogId opId, OStructuralNodeResponse response) {
    controller.sendBinaryRequest(new OStructuralOperationResponse(nodeIdentity, opId, response));
  }

  @Override
  public void sendRequest(OLogId id, OStructuralNodeRequest request) {
    controller.sendBinaryRequest(new OStructuralOperationRequest(nodeIdentity, id, request));
  }

  @Override
  public void reply(OSessionOperationId operationId, OStructuralSubmitResponse response) {
    controller.sendBinaryRequest(new ONetworkStructuralSubmitResponse(nodeIdentity, operationId, response));
  }

  @Override
  public void submit(OSessionOperationId operationId, OStructuralSubmitRequest request) {
    controller.sendBinaryRequest(new ONetworkStructuralSubmitRequest(nodeIdentity, operationId, request));
  }

  @Override
  public void submit(String database, OSessionOperationId operationId, OSubmitRequest request) {
    controller.sendBinaryRequest(new ONetworkSubmitRequest(nodeIdentity, database, operationId, request));
  }

  @Override
  public void reply(String database, OSessionOperationId operationId, OSubmitResponse response) {
    controller.sendBinaryRequest(new ONetworkSubmitResponse(nodeIdentity, database, operationId, response));
  }

  @Override
  public void propagate(OLogId id, ORaftOperation operation) {
    controller.sendBinaryRequest(new ONetworkPropagate(nodeIdentity, id, operation));
  }

  @Override
  public void ack(OLogId logId) {
    controller.sendBinaryRequest(new ONetworkAck(nodeIdentity, logId));
  }

  @Override
  public void confirm(OLogId id) {
    controller.sendBinaryRequest(new ONetworkConfirm(nodeIdentity, id));
  }

  @Override
  public void send(OFullConfiguration fullConfiguration) {
    //TODO:
  }

  public void close() {
    controller.close();
  }
}
