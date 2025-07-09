package com.orientechnologies.orient.core.tx;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ONodeId {
  // TODO: replace this with a uuid
  private final String node;

  public String getNode() {
    return node;
  }

  public ONodeId(String node) {
    super();
    this.node = node;
  }

  public static ONodeId readNetwork(DataInput input) throws IOException {
    String node = input.readUTF();
    return new ONodeId(node);
  }

  public void writeNetwork(DataOutput out) throws IOException {
    out.writeUTF(node);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((node == null) ? 0 : node.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null) return false;
    if (getClass() != obj.getClass()) return false;
    ONodeId other = (ONodeId) obj;
    if (node == null) {
      if (other.node != null) return false;
    } else if (!node.equals(other.node)) return false;
    return true;
  }

  @Override
  public String toString() {
    return "ONodeId [node=" + node + "]";
  }
}
