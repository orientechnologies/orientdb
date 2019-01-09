package com.orientechnologies.orient.distributed.impl.structural;

import com.orientechnologies.orient.distributed.OrientDBDistributed;
import com.orientechnologies.orient.distributed.impl.coordinator.OCoordinateMessagesFactory;
import com.orientechnologies.orient.distributed.impl.coordinator.OLogId;

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
  public OStructuralNodeResponse execute(OStructuralDistributedMember nodeFrom, OLogId opId,
      OStructuralDistributedExecutor executor, OrientDBDistributed context) {
    context.internalDropDatabase(database);
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
