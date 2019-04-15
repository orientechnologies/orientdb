package com.orientechnologies.orient.distributed.impl.structural.operations;

import com.orientechnologies.orient.distributed.impl.coordinator.OCoordinateMessagesFactory;
import com.orientechnologies.orient.distributed.impl.structural.OOperationContext;
import com.orientechnologies.orient.distributed.impl.structural.OStructuralNodeRequest;
import com.orientechnologies.orient.distributed.impl.structural.OStructuralNodeResponse;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ODropDatabaseOperationRequest implements OStructuralNodeRequest {
  private String database;

  public ODropDatabaseOperationRequest(String database) {
    this.database = database;
  }

  public ODropDatabaseOperationRequest() {
    
  }

  @Override
  public OStructuralNodeResponse execute(OOperationContext context) {
    context.getOrientDB().internalDropDatabase(database);
    return new ODropDatabaseOperationResponse();
  }

  @Override
  public void serialize(DataOutput output) throws IOException {
    output.writeUTF(database);
  }

  @Override
  public void deserialize(DataInput input) throws IOException {
    database = input.readUTF();
  }

  @Override
  public int getRequestType() {
    return OCoordinateMessagesFactory.DROP_DATABASE_REQUEST;
  }
}
