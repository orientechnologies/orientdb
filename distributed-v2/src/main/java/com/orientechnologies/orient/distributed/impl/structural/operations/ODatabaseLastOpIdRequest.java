package com.orientechnologies.orient.distributed.impl.structural.operations;

import static com.orientechnologies.orient.distributed.impl.coordinator.OCoordinateMessagesFactory.DATABASE_LAST_OPLOG_ID_RESPONSE;

import com.orientechnologies.orient.core.db.config.ONodeIdentity;
import com.orientechnologies.orient.distributed.OrientDBDistributed;
import com.orientechnologies.orient.distributed.impl.log.OLogId;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Optional;
import java.util.UUID;

public class ODatabaseLastOpIdRequest implements OOperation {
  private String database;
  private UUID electionId;

  public ODatabaseLastOpIdRequest() {}

  public ODatabaseLastOpIdRequest(String database, UUID electionId) {
    this.database = database;
    this.electionId = electionId;
  }

  @Override
  public void apply(ONodeIdentity sender, OrientDBDistributed context) {
    OLogId id = context.getDistributedContext(database).getOpLog().lastPersistentLog();
    context
        .getNetworkManager()
        .send(sender, new ODatabaseLastOpIdResponse(database, electionId, Optional.ofNullable(id)));
  }

  @Override
  public void deserialize(DataInput input) throws IOException {
    this.database = input.readUTF();
    long most = input.readLong();
    long least = input.readLong();
    this.electionId = new UUID(most, least);
  }

  @Override
  public void serialize(DataOutput output) throws IOException {
    output.writeUTF(database);
    output.writeLong(electionId.getMostSignificantBits());
    output.writeLong(electionId.getLeastSignificantBits());
  }

  @Override
  public int getOperationId() {
    return DATABASE_LAST_OPLOG_ID_RESPONSE;
  }
}
