package com.orientechnologies.orient.distributed.impl.network.binary;

import com.orientechnologies.orient.core.db.config.ONodeIdentity;
import com.orientechnologies.orient.distributed.impl.coordinator.network.*;
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
    controller.sendBinaryRequest(new OBinaryDistributedMessage(nodeIdentity, new OOperationRequest(database, id, nodeRequest)));
  }

  @Override
  public void sendResponse(String database, OLogId id, ONodeResponse nodeResponse) {
    controller.sendBinaryRequest(new OBinaryDistributedMessage(nodeIdentity, new OOperationResponse(database, id, nodeResponse)));
  }

  @Override
  public void sendResponse(OLogId opId, OStructuralNodeResponse response) {
  }

  @Override
  public void sendRequest(OLogId id, OStructuralNodeRequest request) {
  }

  @Override
  public void reply(OSessionOperationId operationId, OStructuralSubmitResponse response) {
    controller.sendBinaryRequest(
        new OBinaryDistributedMessage(nodeIdentity, new ONetworkStructuralSubmitResponse(operationId, response)));
  }

  @Override
  public void submit(OSessionOperationId operationId, OStructuralSubmitRequest request) {
    controller
        .sendBinaryRequest(new OBinaryDistributedMessage(nodeIdentity, new ONetworkStructuralSubmitRequest(operationId, request)));
  }

  @Override
  public void submit(String database, OSessionOperationId operationId, OSubmitRequest request) {
    controller
        .sendBinaryRequest(new OBinaryDistributedMessage(nodeIdentity, new ONetworkSubmitRequest(database, operationId, request)));
  }

  @Override
  public void reply(String database, OSessionOperationId operationId, OSubmitResponse response) {
    controller.sendBinaryRequest(
        new OBinaryDistributedMessage(nodeIdentity, new ONetworkSubmitResponse(database, operationId, response)));
  }

  @Override
  public void propagate(OLogId id, ORaftOperation operation) {
    controller.sendBinaryRequest(new OBinaryDistributedMessage(nodeIdentity, new ONetworkPropagate(id, operation)));
  }

  @Override
  public void ack(OLogId logId) {
    controller.sendBinaryRequest(new OBinaryDistributedMessage(nodeIdentity, new ONetworkAck(logId)));
  }

  @Override
  public void confirm(OLogId id) {
    controller.sendBinaryRequest(new OBinaryDistributedMessage(nodeIdentity, new ONetworkConfirm(id)));
  }

  @Override
  public void send(OFullConfiguration fullConfiguration) {
    //TODO:
  }

  public void close() {
    controller.close();
  }
}
