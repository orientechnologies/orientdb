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

public class ODatabaseLastOpIdResponse implements OOperation {
  private String database;
  private UUID electionId;
  private Optional<OLogId> id;

  public ODatabaseLastOpIdResponse(String database, UUID electionId, Optional<OLogId> id) {
    this.database = database;
    this.electionId = electionId;
    this.id = id;
  }

  public ODatabaseLastOpIdResponse() {}

  @Override
  public void apply(ONodeIdentity sender, OrientDBDistributed context) {
    Optional<OLogId> electedId = context.getElections().received(sender, database, electionId, id);
    if (electedId.isPresent()) {
      context
          .getNetworkManager()
          .sendAll(
              context.getActiveNodes(),
              new ODatabaseLastValidRequest(this.database, this.electionId, electedId.get()));
    }
  }

  @Override
  public void deserialize(DataInput input) throws IOException {
    this.database = input.readUTF();
    long most = input.readLong();
    long least = input.readLong();
    this.electionId = new UUID(most, least);
    this.id = OLogId.deserializeOptional(input);
  }

  @Override
  public void serialize(DataOutput output) throws IOException {
    output.writeUTF(this.database);
    output.writeLong(electionId.getMostSignificantBits());
    output.writeLong(electionId.getLeastSignificantBits());
    OLogId.serializeOptional(id, output);
  }

  @Override
  public int getOperationId() {
    return DATABASE_LAST_OPLOG_ID_RESPONSE;
  }
}
