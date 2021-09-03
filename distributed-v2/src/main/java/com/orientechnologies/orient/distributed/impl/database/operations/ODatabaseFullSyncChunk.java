package com.orientechnologies.orient.distributed.impl.database.operations;

import static com.orientechnologies.orient.distributed.impl.coordinator.OCoordinateMessagesFactory.DATABASE_FULL_SYNC_CHUNK;

import com.orientechnologies.orient.core.db.config.ONodeIdentity;
import com.orientechnologies.orient.distributed.OrientDBDistributed;
import com.orientechnologies.orient.distributed.impl.structural.operations.OOperation;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.UUID;

public class ODatabaseFullSyncChunk implements OOperation {
  private String database;
  private UUID uuid;
  private byte[] bytes;
  private int len;

  public ODatabaseFullSyncChunk(String database, UUID uuid, byte[] b, int len) {
    this.database = database;
    this.uuid = uuid;
    this.bytes = b;
    this.len = len;
  }

  public ODatabaseFullSyncChunk() {}

  @Override
  public void apply(ONodeIdentity sender, OrientDBDistributed context) {
    context.syncChunk(database, uuid, bytes, len);
  }

  @Override
  public void deserialize(DataInput input) throws IOException {
    database = input.readUTF();
    long most = input.readLong();
    long least = input.readLong();
    uuid = new UUID(most, least);
    len = input.readInt();
    bytes = new byte[len];
    input.readFully(bytes);
  }

  @Override
  public void serialize(DataOutput output) throws IOException {
    output.writeUTF(this.database);
    output.writeLong(uuid.getMostSignificantBits());
    output.writeLong(uuid.getLeastSignificantBits());
    output.writeInt(len);
    output.write(bytes, 0, len);
  }

  @Override
  public int getOperationId() {
    return DATABASE_FULL_SYNC_CHUNK;
  }
}
