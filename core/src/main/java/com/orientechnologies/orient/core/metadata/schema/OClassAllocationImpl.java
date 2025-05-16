package com.orientechnologies.orient.core.metadata.schema;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OClassAllocationImpl implements OClassAllocation {

  private Map<String, List<String>> nodeClusters = new HashMap<>();

  public ODocument serialize() {
    ODocument doc = new ODocument();
    for (Map.Entry<String, List<String>> entries : nodeClusters.entrySet()) {
      ODocument cd = new ODocument();
      cd.setProperty("clusters", entries.getValue());
      doc.setProperty(entries.getKey(), cd);
    }
    return doc;
  }

  public void deserialize(ODocument source) {
    for (String node : source.getPropertyNames()) {
      ODocument cd = source.getProperty(node);
      this.nodeClusters.put(node, cd.getProperty("clusters"));
    }
  }

  public void addNodeClusters(String node, List<String> clusters) {
    List<String> cl = nodeClusters.get(node);
    if (cl == null) {
      nodeClusters.put(node, new ArrayList<>(clusters));
    } else {
      cl.addAll(clusters);
    }
  }

  public void removeNodeClusters(String node, List<String> clusters) {
    List<String> cl = nodeClusters.get(node);
    if (cl != null) {
      cl.removeAll(clusters);
      if (cl.isEmpty()) {
        nodeClusters.remove(node);
      }
    }
  }

  public void removeClusters(List<String> clusters) {
    for (Map.Entry<String, List<String>> e : nodeClusters.entrySet()) {
      e.getValue().removeAll(clusters);
    }
  }

  public ODocument toNeworkStream(ODatabaseDocumentInternal database) {
    ODocument doc = new ODocument();
    for (Map.Entry<String, List<String>> entries : nodeClusters.entrySet()) {
      ODocument cd = new ODocument();
      cd.setProperty("clusters", entries.getValue());
      doc.setProperty(entries.getKey(), cd);
    }
    return doc;
  }

  @Override
  public List<String> getAllocationClusters(String node) {
    return nodeClusters.get(node);
  }

  @Override
  public List<String> getDefinedNodes() {
    return new ArrayList<>(this.nodeClusters.keySet());
  }
}
