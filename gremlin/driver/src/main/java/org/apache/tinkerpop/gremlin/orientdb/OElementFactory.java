package org.apache.tinkerpop.gremlin.orientdb;

import com.orientechnologies.orient.core.record.OEdge;
import com.orientechnologies.orient.core.record.OVertex;

/** Created by Enrico Risa on 31/08/2017. */
public class OElementFactory {

  private final OGraph graph;

  public OElementFactory(OGraph graph) {
    this.graph = graph;
  }

  public OrientEdge wrapEdge(OEdge edge) {
    return new OrientEdge(graph, edge);
  }

  public OrientVertex wrapVertex(OVertex vertex) {
    return new OrientVertex(graph, vertex);
  }

  public OrientVertex createVertex(String label) {
    return new OrientVertex(graph, label);
  }
}
