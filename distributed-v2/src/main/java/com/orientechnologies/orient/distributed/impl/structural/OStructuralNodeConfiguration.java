package com.orientechnologies.orient.distributed.impl.structural;

import com.orientechnologies.orient.core.db.config.ONodeIdentity;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class OStructuralNodeConfiguration {

  private ONodeIdentity                 identity;
  private List<OStructuralNodeDatabase> databases;

  public OStructuralNodeConfiguration(ONodeIdentity identity) {
    this.identity = identity;
    this.databases = new ArrayList<>();
  }

  protected OStructuralNodeConfiguration() {

  }

  public void deserialize(DataInput input) throws IOException {
    identity = new ONodeIdentity();
    identity.deserialize(input);
    int nDatabases = input.readInt();
    databases = new ArrayList<>(nDatabases);
    while (nDatabases-- > 0) {
      OStructuralNodeDatabase db = new OStructuralNodeDatabase();
      db.deserialize(input);
      databases.add(db);
    }
  }

  public ONodeIdentity getIdentity() {
    return identity;
  }

  public void serialize(DataOutput output) throws IOException {
    identity.serialize(output);
    output.writeInt(databases.size());
    for (OStructuralNodeDatabase database : databases) {
      database.serialize(output);
    }
  }

  public void addDatabase(OStructuralNodeDatabase database) {
    databases.add(database);
  }
}
