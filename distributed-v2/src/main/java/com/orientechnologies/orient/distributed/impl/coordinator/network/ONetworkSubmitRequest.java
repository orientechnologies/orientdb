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
import com.orientechnologies.orient.distributed.impl.coordinator.OSubmitRequest;
import com.orientechnologies.orient.distributed.impl.coordinator.transaction.OSessionOperationId;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import static com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol.DISTRIBUTED_SUBMIT_REQUEST;

public class ONetworkSubmitRequest implements OBinaryRequest, ODistributedExecutable {
  private ONodeIdentity              senderNode;
  private String                     database;
  private OSubmitRequest             request;
  private OCoordinateMessagesFactory factory;
  private OSessionOperationId        operationId;

  public ONetworkSubmitRequest(ONodeIdentity senderNode, String database, OSessionOperationId operationId, OSubmitRequest request) {
    this.database = database;
    this.request = request;
    this.senderNode = senderNode;
    this.operationId = operationId;
  }

  public ONetworkSubmitRequest(OCoordinateMessagesFactory coordinateMessagesFactory) {
    factory = coordinateMessagesFactory;
  }

  @Override
  public void write(OChannelDataOutput network, OStorageRemoteSession session) throws IOException {
    DataOutputStream output = new DataOutputStream(network.getDataOutput());
    this.operationId.serialize(output);
    senderNode.write(output);
    output.writeUTF(database);
    output.writeInt(request.getRequestType());
    request.serialize(output);
  }

  @Override
  public void read(OChannelDataInput channel, int protocolVersion, ORecordSerializer serializer) throws IOException {
    DataInputStream input = new DataInputStream(channel.getDataInput());
    this.operationId = new OSessionOperationId();
    this.operationId.deserialize(input);
    senderNode = new ONodeIdentity();
    senderNode.read(input);
    database = input.readUTF();
    int requestType = input.readInt();
    request = factory.createSubmitRequest(requestType);
    request.deserialize(input);
  }

  @Override
  public byte getCommand() {
    return DISTRIBUTED_SUBMIT_REQUEST;
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
    return "Execution request to the coordinator";
  }

  @Override
  public boolean requireDatabaseSession() {
    return false;
  }

  @Override
  public void executeDistributed(OCoordinatedExecutor executor) {
    executor.executeSubmitRequest(this);
  }

  public OSubmitRequest getRequest() {
    return request;
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
