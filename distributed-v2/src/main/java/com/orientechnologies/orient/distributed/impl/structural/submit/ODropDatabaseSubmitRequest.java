package com.orientechnologies.orient.distributed.impl.structural.submit;

import com.orientechnologies.orient.core.db.config.ONodeIdentity;
import com.orientechnologies.orient.distributed.impl.coordinator.OCoordinateMessagesFactory;
import com.orientechnologies.orient.distributed.impl.coordinator.transaction.OSessionOperationId;
import com.orientechnologies.orient.distributed.impl.structural.raft.OLeaderContext;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Optional;

public class ODropDatabaseSubmitRequest implements OStructuralSubmitRequest {
  private String database;

  public ODropDatabaseSubmitRequest(String database) {
    this.database = database;
  }

  public ODropDatabaseSubmitRequest() {}

  @Override
  public void begin(
      Optional<ONodeIdentity> requester, OSessionOperationId id, OLeaderContext context) {
    context.dropDatabase(requester, id, database);
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

  public String getDatabase() {
    return database;
  }
}
