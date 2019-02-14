package com.orientechnologies.orient.core.db.config;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class ONodeIdentity {
  private String id;
  private String name;

  public ONodeIdentity(String id, String name) {
    this.id = id;
    this.name = name;
  }

  public ONodeIdentity() {

  }

  public String getId() {
    return id;
  }

  public String getName() {
    return name;
  }

  public void setId(String id) {
    this.id = id;
  }

  protected void setName(String name) {
    this.name = name;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    ONodeIdentity that = (ONodeIdentity) o;
    return Objects.equals(id, that.id);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id);
  }

  @Override
  public String toString() {
    return name + '{' + id + '}';
  }

  public void write(DataOutput output) throws IOException {
    output.writeUTF(id);
    output.writeUTF(name);
  }

  public void read(DataInput input) throws IOException {
    id = input.readUTF();
    name = input.readUTF();
  }

}
