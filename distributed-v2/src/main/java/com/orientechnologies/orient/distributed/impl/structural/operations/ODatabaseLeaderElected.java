package com.orientechnologies.orient.distributed.impl.structural.operations;

import static com.orientechnologies.orient.distributed.impl.coordinator.OCoordinateMessagesFactory.DATABASE_LEADER_ELECTED;

import com.orientechnologies.orient.core.db.config.ONodeIdentity;
import com.orientechnologies.orient.distributed.OrientDBDistributed;
import com.orientechnologies.orient.distributed.impl.log.OLogId;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Optional;

public class ODatabaseLeaderElected implements OOperation {
  private String database;
  private ONodeIdentity leader;
  private Optional<OLogId> id;

  public ODatabaseLeaderElected() {}

  public ODatabaseLeaderElected(String database, ONodeIdentity leader, Optional<OLogId> id) {
    this.database = database;
    this.leader = leader;
    this.id = id;
  }

  @Override
  public void apply(ONodeIdentity sender, OrientDBDistributed context) {
    context.getRequestHandler().setDatabaseLeader(leader, database, id.get());
  }

  @Override
  public void deserialize(DataInput input) throws IOException {
    database = input.readUTF();
    leader = new ONodeIdentity();
    leader.deserialize(input);
    id = OLogId.deserializeOptional(input);
  }

  @Override
  public void serialize(DataOutput output) throws IOException {
    output.writeUTF(this.database);
    this.leader.serialize(output);
    OLogId.serializeOptional(id, output);
  }

  @Override
  public int getOperationId() {
    return DATABASE_LEADER_ELECTED;
  }

  public String getDatabase() {
    return database;
  }

  public ONodeIdentity getLeader() {
    return leader;
  }

  public Optional<OLogId> getId() {
    return id;
  }
}
