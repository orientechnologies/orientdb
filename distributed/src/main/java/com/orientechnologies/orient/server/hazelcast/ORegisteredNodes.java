package com.orientechnologies.orient.server.hazelcast;

import com.orientechnologies.orient.core.record.impl.ODocument;
import java.util.List;
import java.util.Map;

public class ORegisteredNodes {

  public final ODocument doc;

  public ORegisteredNodes(String jsonInfo) {
    this.doc = new ODocument();
    if (jsonInfo != null) {
      this.doc.fromJSON(jsonInfo);
    }
  }

  public String toJSON() {
    return this.doc.toJSON();
  }

  public List<String> getIds() {
    return doc.getProperty("ids");
  }

  public Map<String, Integer> getNames() {
    return doc.getProperty("names");
  }

  public void setIds(List<String> registeredNodeById) {
    doc.setProperty("ids", registeredNodeById);
  }

  public void setNames(Map<String, Integer> registeredNodeByName) {
    doc.setProperty("names", registeredNodeByName);
  }
}
