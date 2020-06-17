package com.orientechnologies.orient.distributed.network.binary;

import static com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol.COORDINATED_DISTRIBUTED_MESSAGE;

import com.orientechnologies.orient.client.binary.OBinaryRequestExecutor;
import com.orientechnologies.orient.client.remote.OBinaryRequest;
import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.orientechnologies.orient.core.db.config.ONodeIdentity;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.distributed.impl.coordinator.network.OCoordinatedExecutor;
import com.orientechnologies.orient.distributed.impl.coordinator.network.ODistributedExecutable;
import com.orientechnologies.orient.distributed.impl.coordinator.network.ODistributedMessage;
import com.orientechnologies.orient.distributed.impl.coordinator.network.ONetworkAck;
import com.orientechnologies.orient.distributed.impl.coordinator.network.ONetworkConfirm;
import com.orientechnologies.orient.distributed.impl.coordinator.network.ONetworkOperation;
import com.orientechnologies.orient.distributed.impl.coordinator.network.ONetworkPropagate;
import com.orientechnologies.orient.distributed.impl.coordinator.network.ONetworkStructuralSubmitRequest;
import com.orientechnologies.orient.distributed.impl.coordinator.network.ONetworkStructuralSubmitResponse;
import com.orientechnologies.orient.distributed.impl.coordinator.network.ONetworkSubmitRequest;
import com.orientechnologies.orient.distributed.impl.coordinator.network.ONetworkSubmitResponse;
import com.orientechnologies.orient.distributed.impl.coordinator.network.OOperationRequest;
import com.orientechnologies.orient.distributed.impl.coordinator.network.OOperationResponse;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataInput;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataOutput;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class OBinaryDistributedMessage
    implements OBinaryRequest<OBinaryResponse>, ODistributedExecutable {
  private ODistributedMessage message;
  private ONodeIdentity senderNode;

  public static final byte DISTRIBUTED_SUBMIT_REQUEST = 103;
  public static final byte DISTRIBUTED_SUBMIT_RESPONSE = 104;
  public static final byte DISTRIBUTED_OPERATION_REQUEST = 105;
  public static final byte DISTRIBUTED_OPERATION_RESPONSE = 106;
  public static final byte DISTRIBUTED_PROPAGATE_REQUEST = 107;
  public static final byte DISTRIBUTED_ACK_RESPONSE = 108;
  public static final byte DISTRIBUTED_CONFIRM_REQUEST = 109;
  public static final byte DISTRIBUTED_STRUCTURAL_SUBMIT_REQUEST = 115;
  public static final byte DISTRIBUTED_STRUCTURAL_SUBMIT_RESPONSE = 116;
  public static final byte DISTRIBUTED_OPERATION = 117;

  public OBinaryDistributedMessage() {}

  private ODistributedMessage newDistributedRequest(int requestType) {
    switch (requestType) {
      case DISTRIBUTED_SUBMIT_REQUEST:
        return new ONetworkSubmitRequest();
      case DISTRIBUTED_SUBMIT_RESPONSE:
        return new ONetworkSubmitResponse();
      case DISTRIBUTED_OPERATION_REQUEST:
        return new OOperationRequest();
      case DISTRIBUTED_OPERATION_RESPONSE:
        return new OOperationResponse();
      case DISTRIBUTED_STRUCTURAL_SUBMIT_REQUEST:
        return new ONetworkStructuralSubmitRequest();
      case DISTRIBUTED_STRUCTURAL_SUBMIT_RESPONSE:
        return new ONetworkStructuralSubmitResponse();
      case DISTRIBUTED_PROPAGATE_REQUEST:
        return new ONetworkPropagate();
      case DISTRIBUTED_ACK_RESPONSE:
        return new ONetworkAck();
      case DISTRIBUTED_CONFIRM_REQUEST:
        return new ONetworkConfirm();
      case DISTRIBUTED_OPERATION:
        return new ONetworkOperation();
    }
    return null;
  }

  public OBinaryDistributedMessage(ONodeIdentity sender, ODistributedMessage message) {
    this.senderNode = sender;
    this.message = message;
  }

  @Override
  public void write(OChannelDataOutput network, OStorageRemoteSession session) throws IOException {
    DataOutputStream output = new DataOutputStream(network.getDataOutput());
    senderNode.serialize(output);
    network.writeByte(message.getCommand());
    message.write(output);
  }

  @Override
  public void read(OChannelDataInput channel, int protocolVersion, ORecordSerializer serializer)
      throws IOException {
    DataInputStream input = new DataInputStream(channel.getDataInput());
    senderNode = new ONodeIdentity();
    senderNode.deserialize(input);
    byte messageType = input.readByte();
    this.message = newDistributedRequest(messageType);
    this.message.read(input);
  }

  @Override
  public byte getCommand() {
    return COORDINATED_DISTRIBUTED_MESSAGE;
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
  public void executeDistributed(OCoordinatedExecutor executor) {
    this.message.execute(senderNode, executor);
  }

  @Override
  public String getDescription() {
    return "Distributed message id:" + message.getCommand();
  }

  @Override
  public boolean requireDatabaseSession() {
    return false;
  }
}
