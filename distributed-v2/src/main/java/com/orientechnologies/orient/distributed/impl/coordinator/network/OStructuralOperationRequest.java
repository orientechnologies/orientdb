package com.orientechnologies.orient.distributed.impl.coordinator.network;

import com.orientechnologies.orient.client.binary.OBinaryRequestExecutor;
import com.orientechnologies.orient.client.remote.OBinaryRequest;
import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.orientechnologies.orient.core.db.config.ONodeIdentity;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataInput;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataOutput;
import com.orientechnologies.orient.distributed.impl.coordinator.OCoordinateMessagesFactory;
import com.orientechnologies.orient.distributed.impl.coordinator.OLogId;
import com.orientechnologies.orient.distributed.impl.structural.OStructuralNodeRequest;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import static com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol.DISTRIBUTED_STRUCTURAL_OPERATION_REQUEST;

public class OStructuralOperationRequest implements OBinaryRequest, ODistributedExecutable {
  private ONodeIdentity              senderNode;
  private OLogId                     id;
  private OStructuralNodeRequest     request;
  private OCoordinateMessagesFactory factory;

  public OStructuralOperationRequest(ONodeIdentity senderNode, OLogId id, OStructuralNodeRequest request) {
    this.senderNode = senderNode;
    this.id = id;
    this.request = request;
  }

  public OStructuralOperationRequest(OCoordinateMessagesFactory coordinateMessagesFactory) {
    this.factory = coordinateMessagesFactory;
  }

  @Override
  public void write(OChannelDataOutput network, OStorageRemoteSession session) throws IOException {
    DataOutputStream output = new DataOutputStream(network.getDataOutput());
    senderNode.write(output);
    OLogId.serialize(id, output);
    output.writeInt(request.getRequestType());
    request.serialize(output);
  }

  @Override
  public void read(OChannelDataInput channel, int protocolVersion, ORecordSerializer serializer) throws IOException {
    DataInputStream input = new DataInputStream(channel.getDataInput());
    senderNode = new ONodeIdentity();
    senderNode.read(input);
    id = OLogId.deserialize(input);
    int requestType = input.readInt();
    request = factory.createStructuralOperationRequest(requestType);
    request.deserialize(input);
  }

  @Override
  public byte getCommand() {
    return DISTRIBUTED_STRUCTURAL_OPERATION_REQUEST;
  }

  @Override
  public OBinaryResponse createResponse() {
    return null;
  }

  @Override
  public OBinaryResponse execute(OBinaryRequestExecutor executor) {
    return null;
  }

  @Override
  public String getDescription() {
    return "Distributed Operation Request/Response";
  }

  @Override
  public boolean requireDatabaseSession() {
    return false;
  }

  @Override
  public void executeDistributed(OCoordinatedExecutor executor) {
    executor.executeStructuralOperationRequest(this);
  }

  public OLogId getId() {
    return id;
  }

  public OStructuralNodeRequest getRequest() {
    return request;
  }

  public ONodeIdentity getSenderNode() {
    return senderNode;
  }
}
