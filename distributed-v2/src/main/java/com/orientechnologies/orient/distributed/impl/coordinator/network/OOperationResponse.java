package com.orientechnologies.orient.distributed.impl.coordinator.network;

import com.orientechnologies.orient.client.binary.OBinaryRequestExecutor;
import com.orientechnologies.orient.client.remote.OBinaryRequest;
import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataInput;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataOutput;
import com.orientechnologies.orient.distributed.impl.coordinator.OCoordinateMessagesFactory;
import com.orientechnologies.orient.distributed.impl.coordinator.OLogId;
import com.orientechnologies.orient.distributed.impl.coordinator.ONodeResponse;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import static com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol.DISTRIBUTED_OPERATION_RESPONSE;

public class OOperationResponse implements OBinaryRequest, ODistributedExecutable {
  private OLogId                     id;
  private ONodeResponse              response;
  private String                     senderNode;
  private String                     database;
  private OCoordinateMessagesFactory factory;

  public OOperationResponse(String senderNode, String database, OLogId id, ONodeResponse response) {
    this.id = id;
    this.senderNode = senderNode;
    this.response = response;
    this.database = database;
  }

  public OOperationResponse(OCoordinateMessagesFactory coordinateMessagesFactory) {
    this.factory = coordinateMessagesFactory;
  }

  @Override
  public void write(OChannelDataOutput network, OStorageRemoteSession session) throws IOException {
    DataOutputStream output = new DataOutputStream(network.getDataOutput());
    output.writeUTF(senderNode);
    output.writeUTF(database);
    OLogId.serialize(id, output);
    output.writeInt(response.getResponseType());
    response.serialize(output);
  }

  @Override
  public void read(OChannelDataInput channel, int protocolVersion, ORecordSerializer serializer) throws IOException {
    DataInputStream input = new DataInputStream(channel.getDataInput());
    senderNode = input.readUTF();
    database = input.readUTF();
    id = OLogId.deserialize(input);
    int responseType = input.readInt();
    response = factory.createOperationResponse(responseType);
    response.deserialize(input);
  }

  @Override
  public byte getCommand() {
    return DISTRIBUTED_OPERATION_RESPONSE;
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
    executor.executeOperationResponse(this);
  }

  public ONodeResponse getResponse() {
    return response;
  }

  public OLogId getId() {
    return id;
  }

  public String getDatabase() {
    return database;
  }

  public String getSenderNode() {
    return senderNode;
  }
}
