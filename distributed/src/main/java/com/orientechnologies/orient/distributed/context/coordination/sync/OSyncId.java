package com.orientechnologies.orient.distributed.context.coordination.sync;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.UUID;

public class OSyncId {
  private final String id;

  public String getId() {
    return id;
  }

  public OSyncId() {
    super();
    this.id = UUID.randomUUID().toString();
  }

  public OSyncId(String id) {
    super();
    this.id = id;
  }

  public static OSyncId readNetwork(DataInput input) throws IOException {
    String node = input.readUTF();
    return new OSyncId(node);
  }

  public void writeNetwork(DataOutput out) throws IOException {
    out.writeUTF(id);
  }

  @Override
  public String toString() {
    return "OSyncId [id=" + id + "]";
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((id == null) ? 0 : id.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null) return false;
    if (getClass() != obj.getClass()) return false;
    OSyncId other = (OSyncId) obj;
    if (id == null) {
      if (other.id != null) return false;
    } else if (!id.equals(other.id)) return false;
    return true;
  }
}
