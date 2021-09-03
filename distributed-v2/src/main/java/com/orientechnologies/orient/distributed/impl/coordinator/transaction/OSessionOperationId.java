package com.orientechnologies.orient.distributed.impl.coordinator.transaction;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;
import java.util.UUID;

public class OSessionOperationId {
  private String nodeId;
  private long sequential;

  public OSessionOperationId() {
    // TODO: remove this and replace with correct initialization.
    nodeId = UUID.randomUUID().toString();
    sequential = 0;
  }

  public OSessionOperationId(String nodeId) {
    this.nodeId = nodeId;
    this.sequential = 0;
  }

  protected OSessionOperationId(String nodeId, long sequential) {
    this.nodeId = nodeId;
    this.sequential = sequential;
  }

  public void serialize(DataOutput output) throws IOException {
    output.writeUTF(nodeId);
    output.writeLong(sequential);
  }

  public void deserialize(DataInput input) throws IOException {
    this.nodeId = input.readUTF();
    this.sequential = input.readLong();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    OSessionOperationId that = (OSessionOperationId) o;
    return Objects.equals(nodeId, that.nodeId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(nodeId);
  }

  public OSessionOperationId next() {
    long next = sequential;
    if (next == Long.MAX_VALUE) {
      next = 0;
    }
    return new OSessionOperationId(this.nodeId, next);
  }

  public long getSequential() {
    return sequential;
  }

  public String getNodeId() {
    return nodeId;
  }
}
