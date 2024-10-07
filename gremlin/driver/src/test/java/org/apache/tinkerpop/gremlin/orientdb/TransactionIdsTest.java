package org.apache.tinkerpop.gremlin.orientdb;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

public class TransactionIdsTest {

  @Test
  public void withTransaction() {
    final String labelVertex = "VertexLabel";
    OrientGraph graph = new OrientGraphFactory("memory:myGraph").getTx();
    Vertex v1 = graph.addVertex(labelVertex);
    graph.tx().commit();

    v1 = graph.traversal().V().next();

    Vertex v2 = graph.addVertex(labelVertex);
    graph.tx().commit();

    GraphTraversal<Vertex, Edge> traversal =
        graph.traversal().V(v2.id()).outE().as("edge").otherV().hasId(v2).select("edge");
    traversal.hasNext();

    graph.close();
  }

  @Test
  public void withoutTransaction() {
    final String labelVertex = "VertexLabel";
    OrientGraph graph = new OrientGraphFactory("memory:myGraph").getNoTx();
    Vertex v1 = graph.addVertex(labelVertex);
    graph.tx().commit();

    v1 = graph.traversal().V().next();

    Vertex v2 = graph.addVertex(labelVertex);

    GraphTraversal<Vertex, Edge> traversal =
        graph.traversal().V(v2.id()).outE().as("edge").otherV().hasId(v2).select("edge");
    traversal.hasNext();

    graph.close();
  }
}
