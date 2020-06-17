package com.orientechnologies.orient.distributed.network.binary;

import com.orientechnologies.orient.core.db.config.ONodeIdentity;
import com.orientechnologies.orient.distributed.impl.coordinator.ODistributedChannel;
import com.orientechnologies.orient.distributed.impl.coordinator.ONodeRequest;
import com.orientechnologies.orient.distributed.impl.coordinator.ONodeResponse;
import com.orientechnologies.orient.distributed.impl.coordinator.OSubmitRequest;
import com.orientechnologies.orient.distributed.impl.coordinator.OSubmitResponse;
import com.orientechnologies.orient.distributed.impl.coordinator.network.ONetworkAck;
import com.orientechnologies.orient.distributed.impl.coordinator.network.ONetworkConfirm;
import com.orientechnologies.orient.distributed.impl.coordinator.network.ONetworkOperation;
import com.orientechnologies.orient.distributed.impl.coordinator.network.ONetworkPropagate;
import com.orientechnologies.orient.distributed.impl.coordinator.network.ONetworkStructuralSubmitRequest;
import com.orientechnologies.orient.distributed.impl.coordinator.network.ONetworkStructuralSubmitResponse;
import com.orientechnologies.orient.distributed.impl.coordinator.network.ONetworkSubmitRequest;
import com.orientechnologies.orient.distributed.impl.coordinator.network.ONetworkSubmitResponse;
import com.orientechnologies.orient.distributed.impl.coordinator.network.OOperationRequest;
import com.orientechnologies.orient.distributed.impl.coordinator.network.OOperationResponse;
import com.orientechnologies.orient.distributed.impl.coordinator.transaction.OSessionOperationId;
import com.orientechnologies.orient.distributed.impl.log.OLogId;
import com.orientechnologies.orient.distributed.impl.structural.operations.OOperation;
import com.orientechnologies.orient.distributed.impl.structural.raft.ORaftOperation;
import com.orientechnologies.orient.distributed.impl.structural.submit.OStructuralSubmitRequest;
import com.orientechnologies.orient.distributed.impl.structural.submit.OStructuralSubmitResponse;
import com.orientechnologies.orient.server.distributed.ORemoteServerController;

public class ODistributedChannelBinaryProtocol implements ODistributedChannel {

  private ONodeIdentity nodeIdentity;
  private ORemoteServerController controller;

  public ODistributedChannelBinaryProtocol(
      ONodeIdentity nodeIdentity, ORemoteServerController remoteServer) {
    this.nodeIdentity = nodeIdentity;
    this.controller = remoteServer;
  }

  @Override
  public void sendRequest(String database, OLogId id, ONodeRequest nodeRequest) {
    controller.sendBinaryRequest(
        new OBinaryDistributedMessage(
            nodeIdentity, new OOperationRequest(database, id, nodeRequest)));
  }

  @Override
  public void sendResponse(String database, OLogId id, ONodeResponse nodeResponse) {
    controller.sendBinaryRequest(
        new OBinaryDistributedMessage(
            nodeIdentity, new OOperationResponse(database, id, nodeResponse)));
  }

  @Override
  public void reply(OSessionOperationId operationId, OStructuralSubmitResponse response) {
    controller.sendBinaryRequest(
        new OBinaryDistributedMessage(
            nodeIdentity, new ONetworkStructuralSubmitResponse(operationId, response)));
  }

  @Override
  public void submit(OSessionOperationId operationId, OStructuralSubmitRequest request) {
    controller.sendBinaryRequest(
        new OBinaryDistributedMessage(
            nodeIdentity, new ONetworkStructuralSubmitRequest(operationId, request)));
  }

  @Override
  public void submit(String database, OSessionOperationId operationId, OSubmitRequest request) {
    controller.sendBinaryRequest(
        new OBinaryDistributedMessage(
            nodeIdentity, new ONetworkSubmitRequest(database, operationId, request)));
  }

  @Override
  public void reply(String database, OSessionOperationId operationId, OSubmitResponse response) {
    controller.sendBinaryRequest(
        new OBinaryDistributedMessage(
            nodeIdentity, new ONetworkSubmitResponse(database, operationId, response)));
  }

  @Override
  public void propagate(OLogId id, ORaftOperation operation) {
    controller.sendBinaryRequest(
        new OBinaryDistributedMessage(nodeIdentity, new ONetworkPropagate(id, operation)));
  }

  @Override
  public void ack(OLogId logId) {
    controller.sendBinaryRequest(
        new OBinaryDistributedMessage(nodeIdentity, new ONetworkAck(logId)));
  }

  @Override
  public void confirm(OLogId id) {
    controller.sendBinaryRequest(
        new OBinaryDistributedMessage(nodeIdentity, new ONetworkConfirm(id)));
  }

  @Override
  public void send(OOperation fullConfiguration) {
    controller.sendBinaryRequest(
        new OBinaryDistributedMessage(nodeIdentity, new ONetworkOperation(fullConfiguration)));
    // TODO:
  }

  public void close() {
    controller.close();
  }
}
