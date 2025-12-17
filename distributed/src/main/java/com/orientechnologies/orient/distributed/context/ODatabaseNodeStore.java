package com.orientechnologies.orient.distributed.context;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.transaction.ONodeId;

public class ODatabaseNodeStore {
  private final ONodeId id;
  private final ONodeRole role;

  public ODatabaseNodeStore(ONodeId id, ONodeRole role) {
    this.id = id;
    this.role = role;
  }

  public static ODatabaseNodeStore fromResult(OResult d) {
    ONodeId nodeId = ONodeId.readResult(d.getProperty("node"));
    ONodeRole role = ONodeRole.valueOf(d.getProperty("role"));
    return new ODatabaseNodeStore(nodeId, role);
  }

  public ODocument toDocument() {
    ODocument doc = new ODocument();
    doc.setProperty("node", id.toDocument());
    doc.setProperty("role", role.name());
    return doc;
  }

  public ONodeId getId() {
    return id;
  }

  public ONodeRole getRole() {
    return role;
  }
}
