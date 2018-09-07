package com.orientechnologies.orient.server.distributed.impl.coordinator.network;

import com.orientechnologies.orient.client.binary.OBinaryRequestExecutor;
import com.orientechnologies.orient.client.remote.OBinaryRequest;
import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataInput;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataOutput;
import com.orientechnologies.orient.server.distributed.impl.coordinator.OSubmitRequest;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import static com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol.DISTRIBUTED_SUBMIT_REQUEST;

public class ONetworkSubmitRequest implements OBinaryRequest, ODistributedExecutable {
  private OSubmitRequest request;

  public ONetworkSubmitRequest(OSubmitRequest request) {
    this.request = request;
  }

  public ONetworkSubmitRequest() {

  }

  @Override
  public void write(OChannelDataOutput network, OStorageRemoteSession session) throws IOException {
    request.serialize(new DataOutputStream(network.getDataOutput()));
  }

  @Override
  public void read(OChannelDataInput channel, int protocolVersion, ORecordSerializer serializer) throws IOException {
    request = null;// TODO: create instance from a ID.
    request.deserialize(new DataInputStream(channel.getDataInput()));
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
}
