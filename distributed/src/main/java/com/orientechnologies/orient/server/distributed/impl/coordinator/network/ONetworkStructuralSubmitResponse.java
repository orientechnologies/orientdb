package com.orientechnologies.orient.server.distributed.impl.coordinator.network;

import com.orientechnologies.orient.client.binary.OBinaryRequestExecutor;
import com.orientechnologies.orient.client.remote.OBinaryRequest;
import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataInput;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataOutput;
import com.orientechnologies.orient.server.distributed.impl.coordinator.OCoordinateMessagesFactory;
import com.orientechnologies.orient.server.distributed.impl.coordinator.OSubmitResponse;
import com.orientechnologies.orient.server.distributed.impl.coordinator.transaction.OSessionOperationId;
import com.orientechnologies.orient.server.distributed.impl.structural.OStructuralSubmitResponse;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import static com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol.DISTRIBUTED_STRUCTURAL_SUBMIT_RESPONSE;
import static com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol.DISTRIBUTED_SUBMIT_RESPONSE;

public class ONetworkStructuralSubmitResponse implements OBinaryRequest, ODistributedExecutable {
  private String                     senderNode;
  private OSessionOperationId        operationId;
  private OStructuralSubmitResponse  response;
  private OCoordinateMessagesFactory factory;

  public ONetworkStructuralSubmitResponse(String senderNode, OSessionOperationId operationId, OStructuralSubmitResponse response) {
    this.senderNode = senderNode;
    this.operationId = operationId;
    this.response = response;
  }

  public ONetworkStructuralSubmitResponse(OCoordinateMessagesFactory coordinateMessagesFactory) {
    this.factory = coordinateMessagesFactory;
  }

  @Override
  public void write(OChannelDataOutput network, OStorageRemoteSession session) throws IOException {
    DataOutputStream output = new DataOutputStream(network.getDataOutput());
    operationId.serialize(output);
    output.writeUTF(senderNode);
    output.writeInt(response.getResponseType());
    response.serialize(output);
  }

  @Override
  public void read(OChannelDataInput channel, int protocolVersion, ORecordSerializer serializer) throws IOException {
    DataInputStream input = new DataInputStream(channel.getDataInput());
    operationId = new OSessionOperationId();
    operationId.deserialize(input);
    senderNode = input.readUTF();
    int responseType = input.readInt();
    response = factory.createStructuralSubmitResponse(responseType);
    response.deserialize(input);
  }

  @Override
  public byte getCommand() {
    return DISTRIBUTED_STRUCTURAL_SUBMIT_RESPONSE;
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
    return "execution response from coordinator";
  }

  public boolean requireDatabaseSession() {
    return false;
  }

  @Override
  public void executeDistributed(OCoordinatedExecutor executor) {
    executor.executeStructuralSubmitResponse(this);
  }

  public OStructuralSubmitResponse getResponse() {
    return response;
  }

  public String getSenderNode() {
    return senderNode;
  }

  public OSessionOperationId getOperationId() {
    return operationId;
  }
}
