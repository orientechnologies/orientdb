package com.orientechnologies.orient.distributed.impl.coordinator.network;

import com.orientechnologies.orient.client.binary.OBinaryRequestExecutor;
import com.orientechnologies.orient.client.remote.OBinaryRequest;
import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.orientechnologies.orient.core.db.config.ONodeIdentity;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.distributed.impl.coordinator.OCoordinateMessagesFactory;
import com.orientechnologies.orient.distributed.impl.coordinator.OLogId;
import com.orientechnologies.orient.distributed.impl.structural.raft.ORaftOperation;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataInput;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataOutput;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import static com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol.DISTRIBUTED_PROPAGATE_REQUEST;

public class ONetworkPropagate implements OBinaryRequest, ODistributedExecutable {
  private OCoordinateMessagesFactory factory;
  private ONodeIdentity              senderNode;
  private OLogId                     id;
  private ORaftOperation             operation;

  public ONetworkPropagate(OCoordinateMessagesFactory coordinateMessagesFactory) {
    factory = coordinateMessagesFactory;
  }

  public ONetworkPropagate(ONodeIdentity senderNode, OLogId id, ORaftOperation operation) {
    this.senderNode = senderNode;
    this.id = id;
    this.operation = operation;
  }

  @Override
  public void write(OChannelDataOutput network, OStorageRemoteSession session) throws IOException {
    DataOutputStream output = new DataOutputStream(network.getDataOutput());
    senderNode.serialize(output);
    OLogId.serialize(id, output);
    output.writeInt(operation.getRequestType());
    operation.serialize(output);
  }

  @Override
  public void read(OChannelDataInput channel, int protocolVersion, ORecordSerializer serializer) throws IOException {
    DataInputStream input = new DataInputStream(channel.getDataInput());
    senderNode = new ONodeIdentity();
    senderNode.deserialize(input);
    id = OLogId.deserialize(input);
    int requestType = input.readInt();
    operation = factory.createRaftOperation(requestType);
    operation.deserialize(input);
  }

  @Override
  public byte getCommand() {
    return DISTRIBUTED_PROPAGATE_REQUEST;
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
    return "log a distributed operation";
  }

  @Override
  public boolean requireDatabaseSession() {
    return false;
  }

  @Override
  public void executeDistributed(OCoordinatedExecutor executor) {
    executor.executePropagate(this);
  }

  public ONodeIdentity getSenderNode() {
    return senderNode;
  }

  public OLogId getId() {
    return id;
  }

  public ORaftOperation getOperation() {
    return operation;
  }

}
