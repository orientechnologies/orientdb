package com.orientechnologies.orient.distributed.context;

import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.transaction.ODatabaseId;
import java.util.List;
import java.util.stream.Collectors;

public class ODatabaseTopologyStore {

  private final List<ODatabaseNodeStore> nodes;
  private final ODatabaseId id;
  private final String name;
  private final long version;
  private final int quorum;

  public ODatabaseTopologyStore(
      List<ODatabaseNodeStore> nodes, ODatabaseId id, String name, long version, int quorum) {
    super();
    this.nodes = nodes;
    this.id = id;
    this.name = name;
    this.version = version;
    this.quorum = quorum;
  }

  public static ODatabaseTopologyStore fromResult(OResult d) {
    String name = d.getProperty("name");
    ODatabaseId id = ODatabaseId.readResult(d.getProperty("id"));
    int quorum = d.getProperty("quorum");
    long version = d.getProperty("version");
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
    doc.setProperty("version", version);
    return doc;
  }

  public ODatabaseId getId() {
    return id;
  }

  public String getName() {
    return name;
  }

  public int getQuorum() {
    return quorum;
  }

  public long getVersion() {
    return version;
  }

  public List<ODatabaseNodeStore> getNodes() {
    return nodes;
  }
}
