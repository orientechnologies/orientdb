package com.orientechnologies.orient.distributed.impl.coordinator.network;

import static com.orientechnologies.orient.distributed.network.binary.OBinaryDistributedMessage.DISTRIBUTED_STRUCTURAL_SUBMIT_RESPONSE;

import com.orientechnologies.orient.core.db.config.ONodeIdentity;
import com.orientechnologies.orient.distributed.impl.coordinator.OCoordinateMessagesFactory;
import com.orientechnologies.orient.distributed.impl.coordinator.transaction.OSessionOperationId;
import com.orientechnologies.orient.distributed.impl.structural.submit.OStructuralSubmitResponse;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ONetworkStructuralSubmitResponse implements ODistributedMessage {
  private OSessionOperationId operationId;
  private OStructuralSubmitResponse response;

  public ONetworkStructuralSubmitResponse(
      OSessionOperationId operationId, OStructuralSubmitResponse response) {
    this.operationId = operationId;
    this.response = response;
  }

  public ONetworkStructuralSubmitResponse() {}

  @Override
  public void write(DataOutput output) throws IOException {
    operationId.serialize(output);
    output.writeInt(response.getResponseType());
    response.serialize(output);
  }

  @Override
  public void read(DataInput input) throws IOException {
    operationId = new OSessionOperationId();
    operationId.deserialize(input);
    int responseType = input.readInt();
    response = OCoordinateMessagesFactory.createStructuralSubmitResponse(responseType);
    response.deserialize(input);
  }

  @Override
  public void execute(ONodeIdentity sender, OCoordinatedExecutor executor) {
    executor.executeStructuralSubmitResponse(sender, operationId, response);
  }

  @Override
  public byte getCommand() {
    return DISTRIBUTED_STRUCTURAL_SUBMIT_RESPONSE;
  }

  public OStructuralSubmitResponse getResponse() {
    return response;
  }

  public OSessionOperationId getOperationId() {
    return operationId;
  }
}
