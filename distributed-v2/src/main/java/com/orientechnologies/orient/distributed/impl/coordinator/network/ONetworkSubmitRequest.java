package com.orientechnologies.orient.distributed.impl.coordinator.network;

import static com.orientechnologies.orient.distributed.network.binary.OBinaryDistributedMessage.DISTRIBUTED_SUBMIT_REQUEST;

import com.orientechnologies.orient.core.db.config.ONodeIdentity;
import com.orientechnologies.orient.distributed.impl.coordinator.OCoordinateMessagesFactory;
import com.orientechnologies.orient.distributed.impl.coordinator.OSubmitRequest;
import com.orientechnologies.orient.distributed.impl.coordinator.transaction.OSessionOperationId;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ONetworkSubmitRequest implements ODistributedMessage {
  private String database;
  private OSubmitRequest request;
  private OSessionOperationId operationId;

  public ONetworkSubmitRequest(
      String database, OSessionOperationId operationId, OSubmitRequest request) {
    this.database = database;
    this.request = request;
    this.operationId = operationId;
  }

  public ONetworkSubmitRequest() {}

  @Override
  public void write(DataOutput output) throws IOException {
    this.operationId.serialize(output);
    output.writeUTF(database);
    output.writeInt(request.getRequestType());
    request.serialize(output);
  }

  @Override
  public void read(DataInput input) throws IOException {
    this.operationId = new OSessionOperationId();
    this.operationId.deserialize(input);
    database = input.readUTF();
    int requestType = input.readInt();
    request = OCoordinateMessagesFactory.createSubmitRequest(requestType);
    request.deserialize(input);
  }

  @Override
  public byte getCommand() {
    return DISTRIBUTED_SUBMIT_REQUEST;
  }

  public OSubmitRequest getRequest() {
    return request;
  }

  public String getDatabase() {
    return database;
  }

  @Override
  public void execute(ONodeIdentity sender, OCoordinatedExecutor executor) {
    executor.executeSubmitRequest(sender, database, operationId, request);
  }

  public OSessionOperationId getOperationId() {
    return operationId;
  }
}
