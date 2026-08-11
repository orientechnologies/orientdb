package com.orientechnologies.orient.core.id;

import com.orientechnologies.common.log.OLogger;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResult;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.UUID;

public class ODatabaseId {
  private static final OLogger logger = OLogger.get(ODatabaseId.class);
  private final String name;
  private final String id;

  private ODatabaseId(String name) {
    this.name = name;
    this.id = UUID.randomUUID().toString();
  }

  private ODatabaseId(String name, String id) {
    this.name = name;
    this.id = id;
  }

  public static ODatabaseId newRandom(String name) {
    return new ODatabaseId(name);
  }

  public static ODatabaseId fromStored(String name, String id) {
    return new ODatabaseId(name, id);
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
    if (logger.isDebugEnabled()) {
      return "[" + name + "|" + id + "]";
    } else {
      return "[" + name + "]";
    }
  }

  public static ODatabaseId readNetwork(DataInput input) throws IOException {
    String node = input.readUTF();
    String name = input.readUTF();
    return new ODatabaseId(name, node);
  }

  public void writeNetwork(DataOutput out) throws IOException {
    out.writeUTF(id);
    out.writeUTF(name);
  }

  public ODocument toDocument() {
    ODocument doc = new ODocument();
    doc.setProperty("serializationVersion", 1);
    doc.setProperty("id", id);
    doc.setProperty("name", name);
    return doc;
  }

  public static ODatabaseId readResult(OResult e) {
    assert (int) e.getProperty("serializationVersion") == 1;
    String node = e.getProperty("id");
    String name = e.getProperty("name");
    return new ODatabaseId(name, node);
  }
}
