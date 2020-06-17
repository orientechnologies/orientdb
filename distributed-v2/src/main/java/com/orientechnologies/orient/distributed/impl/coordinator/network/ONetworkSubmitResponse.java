package com.orientechnologies.orient.distributed.impl.coordinator.network;

import static com.orientechnologies.orient.distributed.network.binary.OBinaryDistributedMessage.DISTRIBUTED_SUBMIT_RESPONSE;

import com.orientechnologies.orient.core.db.config.ONodeIdentity;
import com.orientechnologies.orient.distributed.impl.coordinator.OCoordinateMessagesFactory;
import com.orientechnologies.orient.distributed.impl.coordinator.OSubmitResponse;
import com.orientechnologies.orient.distributed.impl.coordinator.transaction.OSessionOperationId;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ONetworkSubmitResponse implements ODistributedMessage {
  private String database;
  private OSessionOperationId operationId;
  private OSubmitResponse response;

  public ONetworkSubmitResponse(
      String database, OSessionOperationId operationId, OSubmitResponse response) {
    this.database = database;
    this.operationId = operationId;
    this.response = response;
  }

  public ONetworkSubmitResponse() {}

  @Override
  public void write(DataOutput output) throws IOException {
    operationId.serialize(output);
    output.writeUTF(database);
    output.writeInt(response.getResponseType());
    response.serialize(output);
  }

  @Override
  public void read(DataInput input) throws IOException {
    operationId = new OSessionOperationId();
    operationId.deserialize(input);
    database = input.readUTF();
    int responseType = input.readInt();
    response = OCoordinateMessagesFactory.createSubmitResponse(responseType);
    response.deserialize(input);
  }

  @Override
  public void execute(ONodeIdentity sender, OCoordinatedExecutor executor) {
    executor.executeSubmitResponse(sender, database, operationId, response);
  }

  @Override
  public byte getCommand() {
    return DISTRIBUTED_SUBMIT_RESPONSE;
  }

  public OSubmitResponse getResponse() {
    return response;
  }

  public String getDatabase() {
    return database;
  }

  public OSessionOperationId getOperationId() {
    return operationId;
  }
}
