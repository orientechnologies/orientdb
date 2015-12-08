package com.orientechnologies.orient.graph.blueprints;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;

public class BlueprintsConcurrentGraphChangesTestTx extends BlueprintsConcurrentGraphChangesTestNoTx {
  protected OrientBaseGraph getGraph() {
    OGlobalConfiguration.SQL_GRAPH_CONSISTENCY_MODE.setValue("tx");
    return new OrientGraph(URL);
  }
}
