package com.orientechnologies.orient.distributed.impl.structural.operations;

import static com.orientechnologies.orient.distributed.impl.coordinator.OCoordinateMessagesFactory.DATABASE_LAST_VALID_OPLOG_ID_REQUEST;

import com.orientechnologies.orient.core.db.config.ONodeIdentity;
import com.orientechnologies.orient.distributed.OrientDBDistributed;
import com.orientechnologies.orient.distributed.impl.log.OLogId;
import com.orientechnologies.orient.distributed.impl.log.OOplogIterator;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Optional;
import java.util.UUID;

public class ODatabaseLastValidRequest implements OOperation {
  private String database;
  private UUID electionId;
  private OLogId oLogId;

  public ODatabaseLastValidRequest(String database, UUID electionId, OLogId oLogId) {
    this.database = database;
    this.electionId = electionId;
    this.oLogId = oLogId;
  }

  public ODatabaseLastValidRequest() {}

  @Override
  public void apply(ONodeIdentity sender, OrientDBDistributed context) {
    Optional<OOplogIterator> res =
        context.getDistributedContext(this.database).getOpLog().searchFrom(oLogId);
    Optional<OLogId> id;
    if (res.isPresent()) {
      if (res.get().hasNext()) {
        id = Optional.of(res.get().next().getLogId());
      } else {
        id = Optional.empty();
      }
    } else {
      id = Optional.empty();
    }
    context
        .getNetworkManager()
        .send(sender, new ODatabaseLastValidResponse(this.database, this.electionId, id));
  }

  @Override
  public void deserialize(DataInput input) throws IOException {
    this.database = input.readUTF();
    long most = input.readLong();
    long least = input.readLong();
    this.electionId = new UUID(most, least);
    this.oLogId = OLogId.deserialize(input);
  }

  @Override
  public void serialize(DataOutput output) throws IOException {
    output.writeUTF(database);
    output.writeLong(electionId.getMostSignificantBits());
    output.writeLong(electionId.getLeastSignificantBits());
    OLogId.serialize(this.oLogId, output);
  }

  @Override
  public int getOperationId() {
    return DATABASE_LAST_VALID_OPLOG_ID_REQUEST;
  }
}
