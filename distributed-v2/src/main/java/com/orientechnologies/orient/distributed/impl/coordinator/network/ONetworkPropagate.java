package com.orientechnologies.orient.distributed.impl.coordinator.network;

import static com.orientechnologies.orient.distributed.network.binary.OBinaryDistributedMessage.DISTRIBUTED_PROPAGATE_REQUEST;

import com.orientechnologies.orient.core.db.config.ONodeIdentity;
import com.orientechnologies.orient.distributed.impl.coordinator.OCoordinateMessagesFactory;
import com.orientechnologies.orient.distributed.impl.log.OLogId;
import com.orientechnologies.orient.distributed.impl.structural.raft.ORaftOperation;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ONetworkPropagate implements ODistributedMessage {
  private OLogId id;
  private ORaftOperation operation;

  public ONetworkPropagate() {}

  public ONetworkPropagate(OLogId id, ORaftOperation operation) {
    this.id = id;
    this.operation = operation;
  }

  @Override
  public void write(DataOutput output) throws IOException {
    OLogId.serialize(id, output);
    output.writeInt(operation.getRequestType());
    operation.serialize(output);
  }

  @Override
  public void read(DataInput input) throws IOException {
    id = OLogId.deserialize(input);
    int requestType = input.readInt();
    operation = OCoordinateMessagesFactory.createRaftOperation(requestType);
    operation.deserialize(input);
  }

  @Override
  public byte getCommand() {
    return DISTRIBUTED_PROPAGATE_REQUEST;
  }

  @Override
  public void execute(ONodeIdentity sender, OCoordinatedExecutor executor) {
    executor.executePropagate(sender, id, operation);
  }

  public OLogId getId() {
    return id;
  }

  public ORaftOperation getOperation() {
    return operation;
  }
}
