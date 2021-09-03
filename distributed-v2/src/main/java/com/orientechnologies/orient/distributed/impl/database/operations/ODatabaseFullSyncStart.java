package com.orientechnologies.orient.distributed.impl.database.operations;

import static com.orientechnologies.orient.distributed.impl.coordinator.OCoordinateMessagesFactory.DATABASE_FULL_SYNC_START;

import com.orientechnologies.orient.core.db.config.ONodeIdentity;
import com.orientechnologies.orient.distributed.OrientDBDistributed;
import com.orientechnologies.orient.distributed.impl.structural.operations.OOperation;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.UUID;

public class ODatabaseFullSyncStart implements OOperation {
  private String database;
  private UUID uuid;
  private boolean incremental;

  public ODatabaseFullSyncStart(String database, UUID uuid, boolean incremental) {
    this.database = database;
    this.uuid = uuid;
    this.incremental = incremental;
  }

  public ODatabaseFullSyncStart() {}

  @Override
  public void apply(ONodeIdentity sender, OrientDBDistributed context) {
    context.startFullSync(database, uuid, incremental);
  }

  @Override
  public void deserialize(DataInput input) throws IOException {
    database = input.readUTF();
    long most = input.readLong();
    long least = input.readLong();
    uuid = new UUID(most, least);
    incremental = input.readBoolean();
  }

  @Override
  public void serialize(DataOutput output) throws IOException {
    output.writeUTF(this.database);
    output.writeLong(uuid.getMostSignificantBits());
    output.writeLong(uuid.getLeastSignificantBits());
    output.writeBoolean(incremental);
  }

  @Override
  public int getOperationId() {
    return DATABASE_FULL_SYNC_START;
  }
}
