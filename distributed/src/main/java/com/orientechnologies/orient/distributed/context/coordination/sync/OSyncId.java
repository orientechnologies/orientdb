package com.orientechnologies.orient.distributed.context.coordination.sync;

import com.orientechnologies.orient.core.transaction.ODatabaseId;
import com.orientechnologies.orient.core.transaction.ONodeId;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OSyncId {
  private final ODatabaseId dbId;
  private final ONodeId receiver;

  public OSyncId(ODatabaseId dbId, ONodeId receiver) {
    super();
    this.dbId = dbId;
    this.receiver = receiver;
  }

  public static OSyncId readNetwork(DataInput input) throws IOException {
    var db = ODatabaseId.readNetwork(input);
    var receiver = ONodeId.readNetwork(input);
    return new OSyncId(db, receiver);
  }

  public void writeNetwork(DataOutput out) throws IOException {
    this.dbId.writeNetwork(out);
    this.receiver.writeNetwork(out);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((dbId == null) ? 0 : dbId.hashCode());
    result = prime * result + ((receiver == null) ? 0 : receiver.hashCode());
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
    return true;
  }

  @Override
  public String toString() {
    return "OSyncId [dbId=" + dbId + ", receiver=" + receiver + "]";
  }

  public ODatabaseId getDbId() {
    return dbId;
  }

  public ONodeId getReceiver() {
    return receiver;
  }
}
