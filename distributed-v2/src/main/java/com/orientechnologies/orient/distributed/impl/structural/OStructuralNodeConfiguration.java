package com.orientechnologies.orient.distributed.impl.structural;

import com.orientechnologies.orient.core.db.config.ONodeIdentity;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class OStructuralNodeConfiguration implements Cloneable {

  private ONodeIdentity identity;
  private Map<UUID, OStructuralNodeDatabase> databases;

  public OStructuralNodeConfiguration(ONodeIdentity identity) {
    this.identity = identity;
    this.databases = new HashMap<>();
  }

  protected OStructuralNodeConfiguration() {}

  public void deserialize(DataInput input) throws IOException {
    identity = new ONodeIdentity();
    identity.deserialize(input);
    int nDatabases = input.readInt();
    databases = new HashMap<>(nDatabases);
    while (nDatabases-- > 0) {
      OStructuralNodeDatabase db = new OStructuralNodeDatabase();
      db.deserialize(input);
      databases.put(db.getUuid(), db);
    }
  }

  public ONodeIdentity getIdentity() {
    return identity;
  }

  public void serialize(DataOutput output) throws IOException {
    identity.serialize(output);
    output.writeInt(databases.size());
    for (OStructuralNodeDatabase database : databases.values()) {
      database.serialize(output);
    }
  }

  public void addDatabase(OStructuralNodeDatabase database) {
    databases.put(database.getUuid(), database);
  }

  public OStructuralNodeDatabase getDatabase(UUID database) {
    return databases.get(database);
  }

  public Collection<OStructuralNodeDatabase> getDatabases() {
    return databases.values();
  }
}
