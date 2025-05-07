package com.orientechnologies.orient.core.metadata.schema;

import java.util.List;

public interface OClassAllocation {

  List<String> getAllocationClusters(String node);

  List<String> getDefinedNodes();
}
