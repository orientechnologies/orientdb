package com.orientechnologies.orient.server.distributed.config;

import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.distributed.ONodeConfig;
import com.orientechnologies.orient.server.distributed.ODistributedConfiguration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class OClusterConfiguration {

  private final ODocument doc;

  public OClusterConfiguration() {
    this.doc = new ODocument();
  }

  public void setLocalName(String name) {
    doc.setProperty("localName", name);
  }

  public void setLocalId(String nodeUuid) {
    doc.setProperty("localId", nodeUuid);
  }

  public void setDatabaseConfiguration(ODistributedConfiguration dbCfg) {
    doc.setProperty("database", dbCfg.getDocument(), OType.EMBEDDED);
  }

  public void setDatabaseConfiguration(ODocument doc) {
    doc.setProperty("database", doc, OType.EMBEDDED);
  }

  public ODocument getDocument() {
    return doc;
  }

  public void addMember(ONodeConfig nodeConfig) {
    List<ODocument> members = doc.getProperty("members");
    if (members == null) {
      members = new ArrayList<>();
      doc.setProperty("members", members);
    }
    members.add(nodeConfig.getConfig().copy());
  }

  public List<ONodeConfig> getMembers() {
    List<ODocument> members = doc.getProperty("members");
    if (members == null) {
      return null;
    } else {
      return members.stream().map((x) -> new ONodeConfig(x)).toList();
    }
  }

  public void setClusterStats(Map<String, ODocument> payload) {
    doc.setProperty("clusterStats", payload);
  }

  public void setDatabaseStatus(ODocument calculateDBStatus) {
    doc.setProperty("databasesStatus", calculateDBStatus);
  }

  public void setDistributed(boolean b) {
    doc.setProperty("distributed", b);
  }
}
