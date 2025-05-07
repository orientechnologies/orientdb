package com.orientechnologies.orient.core.metadata.schema;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OImmutableClassAllocation implements OClassAllocation {

  private final Map<String, List<String>> nodeClusters;

  public OImmutableClassAllocation(OClassAllocation allocations) {

    List<String> nodes = allocations.getDefinedNodes();
    Map<String, List<String>> map = new HashMap<>();
    for (String node : nodes) {
      map.put(node, Collections.unmodifiableList(allocations.getAllocationClusters(node)));
    }
    this.nodeClusters = Collections.unmodifiableMap(map);
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
