package com.orientechnologies.orient.distributed.impl.coordinator.network;

import com.orientechnologies.orient.client.binary.OBinaryRequestExecutor;
import com.orientechnologies.orient.client.remote.OBinaryRequest;
import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.orientechnologies.orient.core.db.config.ONodeIdentity;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.distributed.impl.coordinator.OCoordinateMessagesFactory;
import com.orientechnologies.orient.distributed.impl.coordinator.OLogId;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataInput;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataOutput;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import static com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol.DISTRIBUTED_ACK_RESPONSE;
import static com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol.DISTRIBUTED_PROPAGATE_REQUEST;

public class ONetworkAck implements OBinaryRequest, ODistributedExecutable {
  private ONodeIdentity nodeIdentity;
  private OLogId        logId;

  public ONetworkAck(ONodeIdentity nodeIdentity, OLogId logId) {
    this.nodeIdentity = nodeIdentity;
    this.logId = logId;
  }

  public ONetworkAck() {
  }

  @Override
  public void write(OChannelDataOutput network, OStorageRemoteSession session) throws IOException {
    DataOutputStream output = new DataOutputStream(network.getDataOutput());
    nodeIdentity.serialize(output);
    OLogId.serialize(logId, output);
  }

  @Override
  public void read(OChannelDataInput channel, int protocolVersion, ORecordSerializer serializer) throws IOException {
    DataInputStream input = new DataInputStream(channel.getDataInput());
    nodeIdentity = new ONodeIdentity();
    nodeIdentity.deserialize(input);
    logId = OLogId.deserialize(input);
  }

  @Override
  public byte getCommand() {
    return DISTRIBUTED_ACK_RESPONSE;
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
    return "ack the log a distributed operation";
  }

  @Override
  public boolean requireDatabaseSession() {
    return false;
  }

  @Override
  public void executeDistributed(OCoordinatedExecutor executor) {
    executor.executeAck(this);
  }

  public OLogId getLogId() {
    return logId;
  }

}
