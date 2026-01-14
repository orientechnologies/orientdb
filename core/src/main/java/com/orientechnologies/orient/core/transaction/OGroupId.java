package com.orientechnologies.orient.core.transaction;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResult;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OGroupId {

  private final String id;

  public OGroupId(String id) {
    super();
    this.id = id;
  }

  public String getId() {
    return id;
  }

  public static OGroupId readNetwork(DataInput input) throws IOException {
    String id = input.readUTF();
    return new OGroupId(id);
  }

  public void writeNetwork(DataOutput out) throws IOException {
    out.writeUTF(id);
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
    OGroupId other = (OGroupId) obj;
    if (id == null) {
      if (other.id != null) return false;
    } else if (!id.equals(other.id)) return false;
    return true;
  }

  public ODocument toDocument() {
    ODocument doc = new ODocument();
    doc.setProperty("serializationVersion", 1);
    doc.setProperty("groupId", id);
    return doc;
  }

  public static OGroupId readResult(OResult e) {
    assert (int) e.getProperty("serializationVersion") == 1;
    String groupId = e.getProperty("groupId");
    return new OGroupId(groupId);
  }

  @Override
  public String toString() {
    return "Group(" + id + ")";
  }
}
