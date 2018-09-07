package com.orientechnologies.orient.server.distributed.impl.coordinator.network;

import com.orientechnologies.orient.client.binary.OBinaryRequestExecutor;
import com.orientechnologies.orient.client.remote.OBinaryRequest;
import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataInput;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataOutput;
import com.orientechnologies.orient.server.distributed.impl.coordinator.OSubmitResponse;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import static com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol.DISTRIBUTED_SUBMIT_RESPONSE;

public class ONetworkSubmitResponse implements OBinaryRequest,ODistributedExecutable {
  private OSubmitResponse response;

  public ONetworkSubmitResponse() {
  }

  public ONetworkSubmitResponse(OSubmitResponse response) {
    this.response = response;
  }

  @Override
  public void write(OChannelDataOutput network, OStorageRemoteSession session) throws IOException {
    response.serialize(new DataOutputStream(network.getDataOutput()));
  }

  @Override
  public void read(OChannelDataInput channel, int protocolVersion, ORecordSerializer serializer) throws IOException {
    response = null;//TODO: create instance from factory.
    response.deserialize(new DataInputStream(channel.getDataInput()));
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
}
