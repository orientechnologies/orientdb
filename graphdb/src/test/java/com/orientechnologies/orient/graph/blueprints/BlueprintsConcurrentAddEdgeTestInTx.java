package com.orientechnologies.orient.graph.blueprints;

import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;

public class BlueprintsConcurrentAddEdgeTestInTx extends BlueprintsConcurrentAddEdgeTestNoTx {
  protected OrientBaseGraph getGraph() {
    return new OrientGraph(URL);
  }
}
