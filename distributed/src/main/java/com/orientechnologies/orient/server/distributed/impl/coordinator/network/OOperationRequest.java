package com.orientechnologies.orient.server.distributed.impl.coordinator.network;

import com.orientechnologies.orient.client.binary.OBinaryRequestExecutor;
import com.orientechnologies.orient.client.remote.OBinaryRequest;
import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataInput;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataOutput;
import com.orientechnologies.orient.server.distributed.impl.coordinator.OLogId;
import com.orientechnologies.orient.server.distributed.impl.coordinator.ONodeRequest;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import static com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol.DISTRIBUTED_OPERATION_REQUEST;

public class OOperationRequest implements OBinaryRequest, ODistributedExecutable {
  private OLogId       id;
  private ONodeRequest request;

  public OOperationRequest(OLogId id, ONodeRequest request) {
    this.id = id;
    this.request = request;
  }

  public OOperationRequest() {

  }

  @Override
  public void write(OChannelDataOutput network, OStorageRemoteSession session) throws IOException {
    DataOutputStream output = new DataOutputStream(network.getDataOutput());
    OLogId.serialize(id, output);
    //TODO: write request kind/id.
    request.serialize(output);
  }

  @Override
  public void read(OChannelDataInput channel, int protocolVersion, ORecordSerializer serializer) throws IOException {
    DataInputStream input = new DataInputStream(channel.getDataInput());
    id = OLogId.deserialize(input);
    request = null;// TODO: read the type id and create the instance.
    request.deserialize(input);
  }

  @Override
  public byte getCommand() {
    return DISTRIBUTED_OPERATION_REQUEST;
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
      executor.executeOperationRequest(this);
  }

  public OLogId getId() {
    return id;
  }

  public ONodeRequest getRequest() {
    return request;
  }
}
