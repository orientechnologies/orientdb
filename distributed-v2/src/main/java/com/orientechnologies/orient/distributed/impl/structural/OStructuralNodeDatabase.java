package com.orientechnologies.orient.distributed.impl.structural;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.UUID;

public class OStructuralNodeDatabase implements Cloneable {

  public OStructuralNodeDatabase() {}

  public enum NodeMode {
    ACTIVE,
    REPLICA,
  }

  private UUID uuid;
  private String name;
  private NodeMode mode;

  public OStructuralNodeDatabase(UUID uuid, String name, NodeMode mode) {
    this.uuid = uuid;
    this.name = name;
    this.mode = mode;
  }

  public UUID getUuid() {
    return uuid;
  }

  public String getName() {
    return name;
  }

  public NodeMode getMode() {
    return mode;
  }

  public void deserialize(DataInput input) throws IOException {
    String id = input.readUTF();
    if (id != null) {
      uuid = UUID.fromString(id);
    }
    name = input.readUTF();
    mode = NodeMode.valueOf(input.readUTF());
  }

  public void serialize(DataOutput output) throws IOException {
    output.writeUTF(uuid.toString());
    output.writeUTF(name);
    output.writeUTF(mode.name());
  }

  public void distributedSerialize(DataOutput output) throws IOException {
    serialize(output);
  }
}
