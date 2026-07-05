package com.orientechnologies.orient.distributed.context;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.core.transaction.ONodeRole;

public record ODatabaseNodeStore(ONodeId id, ONodeRole role) {

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
}
