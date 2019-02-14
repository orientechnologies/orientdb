package com.orientechnologies.orient.distributed.impl.structural;

public class OStructuralNodeDatabase {
  public enum NodeMode {
    ACTIVE, REPLICA,
  }

  private String   id;
  private String   name;
  private NodeMode mode;

  public String getId() {
    return id;
  }

  public String getName() {
    return name;
  }

  public NodeMode getMode() {
    return mode;
  }
}
