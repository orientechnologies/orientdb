package com.orientechnologies.orient.distributed.context.coordination.sync;

import com.orientechnologies.orient.core.id.ODatabaseId;
import com.orientechnologies.orient.core.id.ONodeId;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.UUID;

public class OSyncId {
  private final ODatabaseId dbId;
  private final ONodeId receiver;
  private final UUID generation;

  public OSyncId(ODatabaseId dbId, ONodeId receiver) {
    this(dbId, receiver, UUID.randomUUID());
  }

  private OSyncId(ODatabaseId dbId, ONodeId receiver, UUID generation) {
    this.dbId = dbId;
    this.receiver = receiver;
    this.generation = generation;
  }

  public static OSyncId readNetwork(DataInput input) throws IOException {
    var db = ODatabaseId.readNetwork(input);
    var receiver = ONodeId.readNetwork(input);
    var generation = new UUID(input.readLong(), input.readLong());
    return new OSyncId(db, receiver, generation);
  }

  public void writeNetwork(DataOutput out) throws IOException {
    this.dbId.writeNetwork(out);
    this.receiver.writeNetwork(out);
    out.writeLong(generation.getMostSignificantBits());
    out.writeLong(generation.getLeastSignificantBits());
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((dbId == null) ? 0 : dbId.hashCode());
    result = prime * result + ((receiver == null) ? 0 : receiver.hashCode());
    result = prime * result + generation.hashCode();
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null) return false;
    if (getClass() != obj.getClass()) return false;
    OSyncId other = (OSyncId) obj;
    if (dbId == null) {
      if (other.dbId != null) return false;
    } else if (!dbId.equals(other.dbId)) return false;
    if (receiver == null) {
      if (other.receiver != null) return false;
    } else if (!receiver.equals(other.receiver)) return false;
    if (!generation.equals(other.generation)) return false;
    return true;
  }

  @Override
  public String toString() {
    return "OSyncId [dbId=" + dbId + ", receiver=" + receiver + ", generation=" + generation + "]";
  }

  public ODatabaseId getDbId() {
    return dbId;
  }

  public ONodeId getReceiver() {
    return receiver;
  }
}
