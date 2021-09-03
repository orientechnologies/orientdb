package com.orientechnologies.orient.distributed.impl.coordinator.network;

import static com.orientechnologies.orient.distributed.network.binary.OBinaryDistributedMessage.DISTRIBUTED_OPERATION;

import com.orientechnologies.orient.core.db.config.ONodeIdentity;
import com.orientechnologies.orient.distributed.impl.coordinator.OCoordinateMessagesFactory;
import com.orientechnologies.orient.distributed.impl.structural.operations.OOperation;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ONetworkOperation implements ODistributedMessage {
  private OOperation operation;

  public ONetworkOperation(OOperation operation) {
    this.operation = operation;
  }

  public ONetworkOperation() {}

  @Override
  public void write(DataOutput output) throws IOException {
    int opId = operation.getOperationId();
    output.writeInt(opId);
    operation.serialize(output);
  }

  @Override
  public void read(DataInput input) throws IOException {
    int opId = input.readInt();
    operation = OCoordinateMessagesFactory.createOperation(opId);
    operation.deserialize(input);
  }

  @Override
  public byte getCommand() {
    return DISTRIBUTED_OPERATION;
  }

  @Override
  public void execute(ONodeIdentity sender, OCoordinatedExecutor executor) {
    executor.executeOperation(sender, operation);
  }

  protected OOperation getOperation() {
    return operation;
  }
}
