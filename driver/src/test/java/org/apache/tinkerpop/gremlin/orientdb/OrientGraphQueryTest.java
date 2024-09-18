package org.apache.tinkerpop.gremlin.orientdb;

import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

/** Created by Enrico Risa on 11/08/2017. */
public class OrientGraphQueryTest extends OrientGraphBaseTest {

  @Test
  public void shouldCountVerticesEdges() {

    OrientGraph graph = factory.getNoTx();

    initGraph(graph);

    // Count on V
    Long count = graph.traversal().V().count().toList().get(0);
    Assert.assertEquals(Long.valueOf(4), count);

    count = graph.traversal().V().hasLabel("Person").count().toList().get(0);
    Assert.assertEquals(Long.valueOf(2), count);

    count = graph.traversal().V().hasLabel("Animal").count().toList().get(0);
    Assert.assertEquals(Long.valueOf(2), count);

    count = graph.traversal().V().hasLabel("Animal", "Person").count().toList().get(0);
    Assert.assertEquals(Long.valueOf(4), count);

    // Count on E

    count = graph.traversal().E().count().toList().get(0);
    Assert.assertEquals(Long.valueOf(3), count);

    count = graph.traversal().E().hasLabel("HasFriend").count().toList().get(0);
    Assert.assertEquals(Long.valueOf(1), count);

    count = graph.traversal().E().hasLabel("HasAnimal").count().toList().get(0);
    Assert.assertEquals(Long.valueOf(2), count);

    count = graph.traversal().E().hasLabel("HasAnimal", "HasFriend").count().toList().get(0);
    Assert.assertEquals(Long.valueOf(3), count);

    // Inverted Count

    count = graph.traversal().V().hasLabel("HasFriend").count().toList().get(0);
    Assert.assertEquals(Long.valueOf(0), count);

    count = graph.traversal().E().hasLabel("Person").count().toList().get(0);
    Assert.assertEquals(Long.valueOf(0), count);

    // More Complex Count

    count =
        graph
            .traversal()
            .V()
            .has("Person", "name", "Jon")
            .out("HasFriend", "HasAnimal")
            .count()
            .toList()
            .get(0);
    Assert.assertEquals(Long.valueOf(2), count);

    // With Polymorphism

    count = graph.traversal().V().has("Person", "name", "Jon").out("E").count().toList().get(0);
    Assert.assertEquals(Long.valueOf(2), count);

    // With Base Class V/E

    count = graph.traversal().V().has("name", "Jon").count().toList().get(0);
    Assert.assertEquals(Long.valueOf(1), count);

    count = graph.traversal().E().has("name", "Jon").count().toList().get(0);
    Assert.assertEquals(Long.valueOf(0), count);

    count = graph.traversal().V().has("name", "Jon").out("E").count().toList().get(0);
    Assert.assertEquals(Long.valueOf(2), count);

    count = graph.traversal().E().has("marker", 10).count().toList().get(0);
    Assert.assertEquals(Long.valueOf(1), count);

    count = graph.traversal().V().has("marker", 10).count().toList().get(0);
    Assert.assertEquals(Long.valueOf(0), count);
  }

  @Test
  public void shouldCountVerticesEdgesOnTXRollback() {

    OrientGraph graph = factory.getTx();

    // Count on V
    Long count = graph.traversal().V().count().toList().get(0);

    Assert.assertEquals(Long.valueOf(0), count);

    graph.addVertex("name", "Jon");

    count = graph.traversal().V().count().toList().get(0);

    Assert.assertEquals(Long.valueOf(1), count);

    graph.tx().rollback();

    count = graph.traversal().V().count().toList().get(0);

    Assert.assertEquals(Long.valueOf(0), count);

    graph.close();
  }

  @Test
  public void shouldExecuteTraversalWithSpecialCharacters() {

    OrientGraph graph = factory.getTx();

    // Count on V
    Long count = graph.traversal().V().count().toList().get(0);

    Assert.assertEquals(Long.valueOf(0), count);

    graph.addVertex("identifier", 1);

    count = graph.traversal().V().count().toList().get(0);

    Assert.assertEquals(Long.valueOf(1), count);

    graph.tx().commit();

    count = graph.traversal().V().has("~identifier", 1).count().toList().get(0);
    Assert.assertEquals(Long.valueOf(0), count);

    graph.close();
  }

  @Test
  public void shouldNotBlowWithWrongClass() {

    OrientGraph graph = factory.getNoTx();

    try {

      initGraph(graph);

      Integer count = graph.traversal().V().hasLabel("Wrong").toList().size();

      Assert.assertEquals(Integer.valueOf(0), count);

      // Count on Person + Wrong Class

      count = graph.traversal().V().hasLabel("Person", "Wrong").toList().size();

      Assert.assertEquals(Integer.valueOf(2), count);
    } finally {
      graph.close();
    }
  }

  @Test
  public void hasIdWithString() {
    final String labelVertex = "VertexLabel";
    OrientGraph graph = factory.getTx();
    Vertex v1 = graph.addVertex(labelVertex);

    graph.tx().commit();

    Assert.assertEquals(1, graph.traversal().V().hasId(v1.id()).toList().size());

    graph.close();
  }

  @Test
  @Ignore
  public void hasIdWithVertex() {
    final String labelVertex = "VertexLabel";
    OrientGraph graph = factory.getTx();
    Vertex v1 = graph.addVertex(labelVertex);

    graph.tx().commit();

    Assert.assertEquals(1, graph.traversal().V().hasId(v1).toList().size());

    graph.close();
  }

  @Test
  public void shouldCountVerticesEdgesOnTXCommit() {

    OrientGraph graph = factory.getTx();

    // Count on V
    Long count = graph.traversal().V().count().toList().get(0);

    Assert.assertEquals(Long.valueOf(0), count);

    graph.addVertex("name", "Jon");

    count = graph.traversal().V().count().toList().get(0);

    Assert.assertEquals(Long.valueOf(1), count);

    graph.tx().commit();

    count = graph.traversal().V().count().toList().get(0);

    Assert.assertEquals(Long.valueOf(1), count);

    graph.close();
  }

  @Test
  public void shouldWorkWithTwoLabels() {

    OrientGraph graph = factory.getTx();

    graph.createVertexClass("Person");

    // Count on V
    Long count = graph.traversal().V().count().toList().get(0);

    Assert.assertEquals(Long.valueOf(0), count);

    graph.addVertex(T.label, "Person", "name", "Jon");

    count =
        graph
            .traversal()
            .V()
            .hasLabel("Person")
            .has("name", "Jon")
            .hasLabel("Person")
            .has("name", "Jon")
            .count()
            .toList()
            .get(0);

    Assert.assertEquals(Long.valueOf(1), count);

    graph.close();
  }

  protected void initGraph(OrientGraph graph) {

    graph.createVertexClass("Person");
    graph.createVertexClass("Animal");
    graph.createEdgeClass("HasFriend");
    graph.createEdgeClass("HasAnimal");

    Vertex v1 = graph.addVertex(T.label, "Person", "name", "Jon");
    Vertex v2 = graph.addVertex(T.label, "Person", "name", "Frank");

    v1.addEdge("HasFriend", v2);

    Vertex v3 = graph.addVertex(T.label, "Animal", "name", "Foo");
    Vertex v4 = graph.addVertex(T.label, "Animal", "name", "Bar");

    v1.addEdge("HasAnimal", v3, "marker", 10);
    v2.addEdge("HasAnimal", v4);
  }
}
