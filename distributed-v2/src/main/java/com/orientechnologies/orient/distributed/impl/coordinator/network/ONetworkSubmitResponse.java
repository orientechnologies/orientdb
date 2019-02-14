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
import com.orientechnologies.orient.distributed.impl.coordinator.OSubmitResponse;
import com.orientechnologies.orient.distributed.impl.coordinator.transaction.OSessionOperationId;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import static com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol.DISTRIBUTED_SUBMIT_RESPONSE;

public class ONetworkSubmitResponse implements OBinaryRequest, ODistributedExecutable {
  private ONodeIdentity              senderNode;
  private String                     database;
  private OSessionOperationId        operationId;
  private OSubmitResponse            response;
  private OCoordinateMessagesFactory factory;

  public ONetworkSubmitResponse(ONodeIdentity senderNode, String database, OSessionOperationId operationId,
      OSubmitResponse response) {
    this.senderNode = senderNode;
    this.database = database;
    this.operationId = operationId;
    this.response = response;
  }

  public ONetworkSubmitResponse(OCoordinateMessagesFactory coordinateMessagesFactory) {
    this.factory = coordinateMessagesFactory;
  }

  @Override
  public void write(OChannelDataOutput network, OStorageRemoteSession session) throws IOException {
    DataOutputStream output = new DataOutputStream(network.getDataOutput());
    operationId.serialize(output);
    senderNode.write(output);
    output.writeUTF(database);
    output.writeInt(response.getResponseType());
    response.serialize(output);
  }

  @Override
  public void read(OChannelDataInput channel, int protocolVersion, ORecordSerializer serializer) throws IOException {
    DataInputStream input = new DataInputStream(channel.getDataInput());
    operationId = new OSessionOperationId();
    operationId.deserialize(input);
    senderNode = new ONodeIdentity();
    senderNode.read(input);
    database = input.readUTF();
    int responseType = input.readInt();
    response = factory.createSubmitResponse(responseType);
    response.deserialize(input);
  }

  @Override
  public byte getCommand() {
    return DISTRIBUTED_SUBMIT_RESPONSE;
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
    executor.executeSubmitResponse(this);
  }

  public OSubmitResponse getResponse() {
    return response;
  }

  public String getDatabase() {
    return database;
  }

  public ONodeIdentity getSenderNode() {
    return senderNode;
  }

  public OSessionOperationId getOperationId() {
    return operationId;
  }
}
