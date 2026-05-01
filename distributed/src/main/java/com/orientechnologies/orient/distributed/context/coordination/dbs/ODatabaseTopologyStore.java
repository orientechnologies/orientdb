package com.orientechnologies.orient.distributed.context.coordination.dbs;

import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.transaction.ODatabaseId;
import com.orientechnologies.orient.distributed.context.ODatabaseNodeStore;
import com.orientechnologies.orient.distributed.context.coordination.OVersion;
import java.util.List;
import java.util.stream.Collectors;

public record ODatabaseTopologyStore(
    List<ODatabaseNodeStore> nodes, ODatabaseId id, String name, OVersion version, int quorum) {

  public static ODatabaseTopologyStore fromResult(OResult d) {
    String name = d.getProperty("name");
    ODatabaseId id = ODatabaseId.readResult(d.getProperty("id"));
    int quorum = d.getProperty("quorum");
    var version = OVersion.fromResult(d);
    List<OResult> res = d.getProperty("nodes");
    var nodes = res.stream().map((x) -> ODatabaseNodeStore.fromResult(x)).toList();

    return new ODatabaseTopologyStore(nodes, id, name, version, quorum);
  }

  public ODocument toDocument() {
    ODocument doc = new ODocument();
    doc.setProperty("name", name);
    doc.setProperty("id", id.toDocument());
    doc.setProperty("quorum", quorum);
    doc.setProperty(
        "nodes",
        this.nodes.stream().map((x) -> x.toDocument()).collect(Collectors.toList()),
        OType.EMBEDDEDLIST);
    this.version.toElement(doc);
    return doc;
  }
}
