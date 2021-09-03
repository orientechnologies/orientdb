package com.orientechnologies.orient.distributed.impl.coordinator.network;

import static com.orientechnologies.orient.distributed.network.binary.OBinaryDistributedMessage.DISTRIBUTED_OPERATION_REQUEST;

import com.orientechnologies.orient.core.db.config.ONodeIdentity;
import com.orientechnologies.orient.distributed.impl.coordinator.OCoordinateMessagesFactory;
import com.orientechnologies.orient.distributed.impl.coordinator.ONodeRequest;
import com.orientechnologies.orient.distributed.impl.log.OLogId;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OOperationRequest implements ODistributedMessage {
  private String database;
  private OLogId id;
  private ONodeRequest request;

  public OOperationRequest(String database, OLogId id, ONodeRequest request) {
    this.database = database;
    this.id = id;
    this.request = request;
  }

  public OOperationRequest() {}

  @Override
  public void read(DataInput input) throws IOException {
    database = input.readUTF();
    id = OLogId.deserialize(input);
    int requestType = input.readInt();
    request = OCoordinateMessagesFactory.createOperationRequest(requestType);
    request.deserialize(input);
  }

  @Override
  public byte getCommand() {
    return DISTRIBUTED_OPERATION_REQUEST;
  }

  @Override
  public void write(DataOutput output) throws IOException {
    output.writeUTF(database);
    OLogId.serialize(id, output);
    output.writeInt(request.getRequestType());
    request.serialize(output);
  }

  @Override
  public void execute(ONodeIdentity sender, OCoordinatedExecutor executor) {
    executor.executeOperationRequest(sender, database, id, request);
  }

  public OLogId getId() {
    return id;
  }

  public ONodeRequest getRequest() {
    return request;
  }

  public String getDatabase() {
    return database;
  }
}
