package com.orientechnologies.orient.distributed.impl.structural.raft;

import com.orientechnologies.orient.distributed.OrientDBDistributed;
import com.orientechnologies.orient.distributed.impl.coordinator.transaction.OSessionOperationId;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ODropDatabase implements ORaftOperation {
  private OSessionOperationId operationId;
  private String              database;

  public ODropDatabase(OSessionOperationId operationId, String database) {
    this.operationId = operationId;
    this.database = database;

  }

  public ODropDatabase() {

  }

  @Override
  public void apply(OrientDBDistributed context) {
    context.internalDropDatabase(database);
  }

  @Override
  public void serialize(DataOutput output) throws IOException {
    operationId.serialize(output);
    output.writeUTF(database);
  }

  @Override
  public int getRequestType() {
    return 0;
  }

  @Override
  public void deserialize(DataInput input) throws IOException {
    this.operationId = new OSessionOperationId();
    this.operationId.deserialize(input);
    this.database = input.readUTF();
  }
}
