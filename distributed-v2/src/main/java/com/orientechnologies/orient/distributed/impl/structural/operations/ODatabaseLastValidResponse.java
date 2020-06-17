package com.orientechnologies.orient.distributed.impl.structural.operations;

import static com.orientechnologies.orient.distributed.impl.coordinator.OCoordinateMessagesFactory.DATABASE_LAST_VALID_OPLOG_ID_RESPONSE;

import com.orientechnologies.orient.core.db.config.ONodeIdentity;
import com.orientechnologies.orient.distributed.OrientDBDistributed;
import com.orientechnologies.orient.distributed.impl.log.OLogId;
import com.orientechnologies.orient.distributed.impl.metadata.OElectionReply;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Optional;
import java.util.UUID;

public class ODatabaseLastValidResponse implements OOperation {
  private String database;
  private UUID electionId;
  private Optional<OLogId> id;

  public ODatabaseLastValidResponse(String database, UUID electionId, Optional<OLogId> id) {
    this.database = database;
    this.electionId = electionId;
    this.id = id;
  }

  public ODatabaseLastValidResponse() {}

  @Override
  public void apply(ONodeIdentity sender, OrientDBDistributed context) {
    Optional<OElectionReply> elected =
        context.getElections().receivedLastLog(sender, electionId, database, id);
    if (elected.isPresent()) {
      OElectionReply val = elected.get();
      context
          .getNetworkManager()
          .sendAll(
              context.getActiveNodes(),
              new ODatabaseLeaderElected(this.database, val.getSender(), val.getId()));
    }
  }

  @Override
  public void deserialize(DataInput input) throws IOException {
    database = input.readUTF();
    long most = input.readLong();
    long least = input.readLong();
    this.electionId = new UUID(most, least);
    id = OLogId.deserializeOptional(input);
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
    return DATABASE_LAST_VALID_OPLOG_ID_RESPONSE;
  }
}
