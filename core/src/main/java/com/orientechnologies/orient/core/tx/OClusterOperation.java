package com.orientechnologies.orient.core.tx;

public class OClusterOperation {

  public enum Type {
    CREATE,
    DROP,
  }

  private String name;
  private Type type;

  public OClusterOperation(String name, Type type) {
    super();
    this.name = name;
    this.type = type;
  }

  public String getName() {
    return name;
  }

  public Type getType() {
    return type;
  }
}
