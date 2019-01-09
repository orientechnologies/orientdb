package com.orientechnologies.orient.distributed.impl.structural;

import com.orientechnologies.orient.distributed.OrientDBDistributed;
import com.orientechnologies.orient.distributed.impl.coordinator.OCoordinateMessagesFactory;
import com.orientechnologies.orient.distributed.impl.coordinator.transaction.OSessionOperationId;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ODropDatabaseSubmitRequest implements OStructuralSubmitRequest {
  private String database;

  public ODropDatabaseSubmitRequest(String database) {
    this.database = database;
  }

  public ODropDatabaseSubmitRequest() {
  }

  @Override
  public void begin(OStructuralDistributedMember sender, OSessionOperationId operationId, OStructuralCoordinator coordinator,
      OrientDBDistributed context) {

    coordinator
        .sendOperation(this, new ODropDatabaseOperationRequest(database), new ODropDatabaseResponseHandler(sender, operationId));
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
    return OCoordinateMessagesFactory.DROP_DATABASE_SUBMIT_REQUEST;
  }
}
