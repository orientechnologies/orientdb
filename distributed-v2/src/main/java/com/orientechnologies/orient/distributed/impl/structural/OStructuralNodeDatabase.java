package com.orientechnologies.orient.distributed.impl.structural;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OStructuralNodeDatabase {

  protected OStructuralNodeDatabase() {

  }


  public enum NodeMode {
    ACTIVE, REPLICA,
  }

  private String   id;
  private String   name;
  private NodeMode mode;

  public OStructuralNodeDatabase(String id, String name, NodeMode mode) {
    this.id = id;
    this.name = name;
    this.mode = mode;
  }

  public String getId() {
    return id;
  }

  public String getName() {
    return name;
  }

  public NodeMode getMode() {
    return mode;
  }

  public void deserialize(DataInput input) throws IOException {
    id = input.readUTF();
    name = input.readUTF();
    mode = NodeMode.valueOf(input.readUTF());
  }

  public void serialize(DataOutput output) throws IOException {
    output.writeUTF(id);
    output.writeUTF(name);
    output.writeUTF(mode.name());
  }

  public void distributedSerialize(DataOutput output) throws IOException {
    serialize(output);
  }


}
