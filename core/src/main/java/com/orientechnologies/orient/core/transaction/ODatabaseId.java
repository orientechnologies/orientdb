package com.orientechnologies.orient.core.transaction;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResult;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.UUID;

public class ODatabaseId {

  private final String id;

  public ODatabaseId() {
    this.id = UUID.randomUUID().toString();
  }

  public ODatabaseId(String id) {
    this.id = id;
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
    ODatabaseId other = (ODatabaseId) obj;
    if (id == null) {
      if (other.id != null) return false;
    } else if (!id.equals(other.id)) return false;
    return true;
  }

  public String getId() {
    return id;
  }

  @Override
  public String toString() {
    return "ODatabaseId [id=" + id + "]";
  }

  public static ODatabaseId readNetwork(DataInput input) throws IOException {
    String node = input.readUTF();
    return new ODatabaseId(node);
  }

  public void writeNetwork(DataOutput out) throws IOException {
    out.writeUTF(id);
  }

  public ODocument toDocument() {
    ODocument doc = new ODocument();
    doc.setProperty("serializationVersion", 1);
    doc.setProperty("id", id);
    return doc;
  }

  public static ODatabaseId readResult(OResult e) {
    assert (int) e.getProperty("serializationVersion") == 1;
    String node = e.getProperty("id");
    return new ODatabaseId(node);
  }
}
