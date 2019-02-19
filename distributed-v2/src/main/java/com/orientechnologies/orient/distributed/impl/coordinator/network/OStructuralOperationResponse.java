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
import com.orientechnologies.orient.distributed.impl.structural.OStructuralNodeResponse;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import static com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol.DISTRIBUTED_STRUCTURAL_OPERATION_RESPONSE;

public class OStructuralOperationResponse implements OBinaryRequest, ODistributedExecutable {
  private OLogId                     id;
  private OStructuralNodeResponse    response;
  private ONodeIdentity              senderNode;
  private OCoordinateMessagesFactory factory;

  public OStructuralOperationResponse(ONodeIdentity senderNode, OLogId id, OStructuralNodeResponse response) {
    this.id = id;
    this.senderNode = senderNode;
    this.response = response;
  }

  public OStructuralOperationResponse(OCoordinateMessagesFactory coordinateMessagesFactory) {
    this.factory = coordinateMessagesFactory;
  }

  @Override
  public void write(OChannelDataOutput network, OStorageRemoteSession session) throws IOException {
    DataOutputStream output = new DataOutputStream(network.getDataOutput());
    senderNode.serialize(output);
    OLogId.serialize(id, output);
    output.writeInt(response.getResponseType());
    response.serialize(output);
  }

  @Override
  public void read(OChannelDataInput channel, int protocolVersion, ORecordSerializer serializer) throws IOException {
    DataInputStream input = new DataInputStream(channel.getDataInput());
    senderNode = new ONodeIdentity();
    senderNode.deserialize(input);
    id = OLogId.deserialize(input);
    int responseType = input.readInt();
    response = factory.createStructuralOperationResponse(responseType);
    response.deserialize(input);
  }

  @Override
  public byte getCommand() {
    return DISTRIBUTED_STRUCTURAL_OPERATION_RESPONSE;
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
    return null;
  }

  @Override
  public boolean requireDatabaseSession() {
    return false;
  }

  @Override
  public void executeDistributed(OCoordinatedExecutor executor) {
    executor.executeStructuralOperationResponse(this);
  }

  public OStructuralNodeResponse getResponse() {
    return response;
  }

  public OLogId getId() {
    return id;
  }

  public ONodeIdentity getSenderNode() {
    return senderNode;
  }
}
