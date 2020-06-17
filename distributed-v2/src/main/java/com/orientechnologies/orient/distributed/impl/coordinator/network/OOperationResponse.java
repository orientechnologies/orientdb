package com.orientechnologies.orient.distributed.impl.coordinator.network;

import static com.orientechnologies.orient.distributed.network.binary.OBinaryDistributedMessage.DISTRIBUTED_OPERATION_RESPONSE;

import com.orientechnologies.orient.core.db.config.ONodeIdentity;
import com.orientechnologies.orient.distributed.impl.coordinator.OCoordinateMessagesFactory;
import com.orientechnologies.orient.distributed.impl.coordinator.ONodeResponse;
import com.orientechnologies.orient.distributed.impl.log.OLogId;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OOperationResponse implements ODistributedMessage {
  private OLogId id;
  private ONodeResponse response;
  private String database;

  public OOperationResponse(String database, OLogId id, ONodeResponse response) {
    this.id = id;
    this.response = response;
    this.database = database;
  }

  public OOperationResponse() {}

  @Override
  public void write(DataOutput output) throws IOException {
    output.writeUTF(database);
    OLogId.serialize(id, output);
    output.writeInt(response.getResponseType());
    response.serialize(output);
  }

  @Override
  public void read(DataInput input) throws IOException {
    database = input.readUTF();
    id = OLogId.deserialize(input);
    int responseType = input.readInt();
    response = OCoordinateMessagesFactory.createOperationResponse(responseType);
    response.deserialize(input);
  }

  @Override
  public void execute(ONodeIdentity sender, OCoordinatedExecutor executor) {
    executor.executeOperationResponse(sender, database, id, response);
  }

  @Override
  public byte getCommand() {
    return DISTRIBUTED_OPERATION_RESPONSE;
  }

  public ONodeResponse getResponse() {
    return response;
  }

  public OLogId getId() {
    return id;
  }

  public String getDatabase() {
    return database;
  }
}
