package com.orientechnologies.orient.distributed.impl.coordinator.transaction;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;
import java.util.UUID;

public class OSessionOperationId {
  private UUID uuid;

  public OSessionOperationId() {
    init();
  }

  public void init() {
    uuid = UUID.randomUUID();
  }

  public void serialize(DataOutput output) throws IOException {
    output.writeLong(uuid.getMostSignificantBits());
    output.writeLong(uuid.getLeastSignificantBits());
  }

  public void deserialize(DataInput input) throws IOException {
    long most = input.readLong();
    long least = input.readLong();
    this.uuid = new UUID(most, least);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    OSessionOperationId that = (OSessionOperationId) o;
    return Objects.equals(uuid, that.uuid);
  }

  @Override
  public int hashCode() {

    return Objects.hash(uuid);
  }
}
