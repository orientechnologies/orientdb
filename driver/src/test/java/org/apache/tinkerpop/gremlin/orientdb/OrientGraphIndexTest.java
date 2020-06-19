package org.apache.tinkerpop.gremlin.orientdb;

import static junit.framework.TestCase.assertTrue;
import static org.apache.tinkerpop.gremlin.structure.T.label;

import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.storage.ORecordDuplicatedException;
import java.util.*;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.orientdb.traversal.step.sideeffect.OrientGraphStep;
import org.apache.tinkerpop.gremlin.orientdb.traversal.strategy.optimization.OrientGraphStepStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;

public class OrientGraphIndexTest {

  public static final String URL = "memory:" + OrientGraphIndexTest.class.getSimpleName();

  private OrientGraph newGraph() {
    return new OrientGraphFactory(URL + UUID.randomUUID(), "admin", "admin").getNoTx();
  }

  String vertexLabel1 = "SomeVertexLabel1";
  String vertexLabel2 = "SomeVertexLabel2";

  String edgeLabel1 = "SomeEdgeLabel1";
  String edgeLabel2 = "SomeEdgeLabel2";

  String key = "indexedKey";

  @Test
  public void vertexUniqueConstraint() {
    OrientGraph graph = newGraph();
    createVertexIndexLabel(graph, vertexLabel1);
    String value = "value1";

    graph.addVertex(label, vertexLabel1, key, value);
    graph.addVertex(label, vertexLabel2, key, value);

    // no duplicates allowed for vertex with label1
    try {
      graph.addVertex(label, vertexLabel1, key, value);
      Assert.fail("must throw duplicate key here!");
    } catch (ORecordDuplicatedException e) {
      // ok
    }

    // allow duplicate for vertex with label2
    graph.addVertex(label, vertexLabel2, key, value);
  }

  @Test
  public void edgeUniqueConstraint() {
    OrientGraph graph = newGraph();
    createUniqueEdgeIndex(graph, edgeLabel1);
    String value = "value1";

    Vertex v1 = graph.addVertex(label, vertexLabel1);
    Vertex v2 = graph.addVertex(label, vertexLabel1);
    v1.addEdge(edgeLabel1, v2, key, value);

    // no duplicates allowed for edge with label1
    try {
      v1.addEdge(edgeLabel1, v2, key, value);
      Assert.fail("must throw duplicate key here!");
    } catch (ORecordDuplicatedException e) {
      // ok
    }

    // allow duplicate for vertex with label2
    v2.addEdge(edgeLabel2, v1, key, value);
  }

  @Test
  public void vertexUniqueIndexLookupWithValue() {
    OrientGraph graph = newGraph();
    createVertexIndexLabel(graph, vertexLabel1);
    String value = "value1";

    // verify index created
    Assert.assertEquals(
        graph.getIndexedKeys(Vertex.class, vertexLabel1),
        new HashSet<>(Collections.singletonList(key)));
    Assert.assertEquals(
        graph.getIndexedKeys(Vertex.class, vertexLabel2), new HashSet<>(Collections.emptyList()));
    Assert.assertEquals(
        graph.getIndexedKeys(Edge.class, vertexLabel1), new HashSet<>(Collections.emptyList()));

    Vertex v1 = graph.addVertex(label, vertexLabel1, key, value);
    Vertex v2 = graph.addVertex(label, vertexLabel2, key, value);

    // looking deep into the internals here - I can't find a nicer way to
    // auto verify that an index is actually used
    GraphTraversal<Vertex, Vertex> traversal =
        graph.traversal().V().has(label, P.eq(vertexLabel1)).has(key, P.eq(value));

    Assert.assertEquals(1, usedIndexes(graph, traversal));

    OIndex index = findUsedIndex(traversal).iterator().next().index;
    Assert.assertEquals(1, index.getSize());
    Assert.assertEquals(v1.id(), index.get(value));

    List<Vertex> result = traversal.toList();
    Assert.assertEquals(1, result.size());
    Assert.assertEquals(v1.id(), result.get(0).id());
  }

  @Test
  public void vertexUniqueIndexLookupWithValueInMidTraversal() {
    OrientGraph graph = newGraph();
    createVertexIndexLabel(graph, vertexLabel1);
    createVertexIndexLabel(graph, vertexLabel2);
    String value = "value1";

    // verify index created
    Assert.assertEquals(
        graph.getIndexedKeys(Vertex.class, vertexLabel1),
        new HashSet<>(Collections.singletonList(key)));
    Assert.assertEquals(
        graph.getIndexedKeys(Vertex.class, vertexLabel2),
        new HashSet<>(Collections.singletonList(key)));
    Assert.assertEquals(
        graph.getIndexedKeys(Edge.class, vertexLabel1), new HashSet<>(Collections.emptyList()));

    Vertex v1 = graph.addVertex(label, vertexLabel1, key, value);
    Vertex v2 = graph.addVertex(label, vertexLabel2, key, value);

    // looking deep into the internals here - I can't find a nicer way to
    // auto verify that an index is actually used
    GraphTraversal<Vertex, Edge> traversal =
        graph
            .traversal()
            .V()
            .has(label, P.eq(vertexLabel1))
            .has(key, P.eq(value))
            .as("first")
            .V()
            .has(label, P.eq(vertexLabel2))
            .has(key, P.eq(value))
            .as("second")
            .addE(edgeLabel1);

    Assert.assertEquals(2, usedIndexes(graph, traversal));

    List<Edge> result = traversal.toList();
    Assert.assertEquals(1, result.size());

    Assert.assertEquals(edgeLabel1, result.get(0).label());
  }

  @Test
  public void vertexUniqueIndexLookupWithMultipleLabels() {
    final String label1 = "label1";
    final String label2 = "label2";
    final String label3 = "label3";

    final String value1 = "value1";

    OrientGraph graph = newGraph();
    createVertexIndexLabel(graph, label1);
    createVertexIndexLabel(graph, label2);
    createVertexIndexLabel(graph, label3);

    // Check that property (key) is indexed on multiple labels
    Assert.assertEquals(
        graph.getIndexedKeys(Vertex.class, label1), new HashSet<>(Collections.singletonList(key)));
    Assert.assertEquals(
        graph.getIndexedKeys(Vertex.class, label2), new HashSet<>(Collections.singletonList(key)));
    Assert.assertEquals(
        graph.getIndexedKeys(Vertex.class, label3), new HashSet<>(Collections.singletonList(key)));

    Vertex v1 = graph.addVertex(label, label1, key, value1);
    Vertex v2 = graph.addVertex(label, label2, key, value1);
    Vertex v3 = graph.addVertex(label, label3, key, value1);

    GraphTraversal<Vertex, Vertex> traversal =
        graph.traversal().V().hasLabel(label1, label2, label3).has(key, value1);

    Assert.assertEquals(3, usedIndexes(graph, traversal));

    Set<OrientIndexQuery> indicies = findUsedIndex(traversal);
    Assert.assertEquals(3, indicies.size());

    assertTrue(valueFound(indicies, v1, value1));
    assertTrue(valueFound(indicies, v2, value1));
    assertTrue(valueFound(indicies, v3, value1));
  }

  private static boolean valueFound(Set<OrientIndexQuery> indicies, Vertex v, String value) {
    for (OrientIndexQuery index : indicies) {
      if (v.id().equals(index.index.get(value))) {
        return true;
      }
    }
    return false;
  }

  // TODO Enable when it's fixed
  //  @Test
  public void vertexUniqueIndexLookupWithMultipleValues() {
    OrientGraph graph = newGraph();
    createVertexIndexLabel(graph, vertexLabel1);
    // verify index created
    Assert.assertEquals(
        graph.getIndexedKeys(Vertex.class, vertexLabel1),
        new HashSet<>(Collections.singletonList(key)));

    String value1 = "value1";
    String value2 = "value2";
    String value3 = "value3";

    Vertex v1 = graph.addVertex(label, vertexLabel1, key, value1);
    Vertex v2 = graph.addVertex(label, vertexLabel1, key, value2);
    Vertex v3 = graph.addVertex(label, vertexLabel1, key, value3);

    // looking deep into the internals here - I can't find a nicer way to
    // auto verify that an index is actually used
    // GraphTraversal<Vertex, Vertex> traversal = graph.traversal().V().has(T.label,
    // P.eq(vertexLabel1)).has(key, P.eq(value1));
    GraphTraversal<Vertex, Vertex> traversal =
        graph.traversal().V().has(label, P.eq(vertexLabel1)).has(key, P.within(value1, value2));

    Assert.assertEquals(1, usedIndexes(graph, traversal));

    OIndex index = findUsedIndex(traversal).iterator().next().index;
    Assert.assertEquals(3, index.getSize());
    Assert.assertEquals(v1.id(), index.get(value1));
    Assert.assertEquals(v2.id(), index.get(value2));

    List<Vertex> result = traversal.toList();
    Assert.assertEquals(2, result.size());
    Assert.assertEquals(v1.id(), result.get(0).id());
    Assert.assertEquals(v2.id(), result.get(1).id());
  }

  @Test
  public void edgeUniqueIndexLookupWithValue() {
    OrientGraph graph = newGraph();
    createUniqueEdgeIndex(graph, edgeLabel1);
    String value = "value1";

    Assert.assertEquals(
        graph.getIndexedKeys(Edge.class, edgeLabel1),
        new HashSet<>(Collections.singletonList(key)));
    Assert.assertEquals(
        graph.getIndexedKeys(Edge.class, edgeLabel2), new HashSet<>(Collections.emptyList()));
    Assert.assertEquals(
        graph.getIndexedKeys(Vertex.class, vertexLabel1), new HashSet<>(Collections.emptyList()));

    Vertex v1 = graph.addVertex(label, vertexLabel1);
    Vertex v2 = graph.addVertex(label, vertexLabel1);
    Edge e1 = v1.addEdge(edgeLabel1, v2, key, value);
    Edge e2 = v1.addEdge(edgeLabel2, v2, key, value);

    {
      // Verify that the traversal hits the index for the edges with label1
      GraphTraversal<Edge, Edge> traversal1 =
          graph.traversal().E().has(label, P.eq(edgeLabel1)).has(key, P.eq(value));

      Assert.assertEquals(1, usedIndexes(graph, traversal1));

      // TODO Remove old legacy index
      Set<OrientIndexQuery> orientIndexQueries = findUsedIndex(traversal1);
      Assert.assertFalse(orientIndexQueries.isEmpty());

      orientIndexQueries.forEach(
          orientIndexQuery -> {
            OIndex index = orientIndexQuery.index;
            Assert.assertEquals(1, index.getSize());
            Assert.assertEquals(e1.id(), index.get(value));

            List<Edge> result1 = traversal1.toList();
            Assert.assertEquals(1, result1.size());
            Assert.assertEquals(e1.id(), result1.get(0).id());
          });
    }

    {
      // Verify that the traversal doesn't try to hit the index for the edges with label2
      GraphTraversal<Edge, Edge> traversal2 =
          graph.traversal().E().has(label, P.eq(edgeLabel2)).has(key, P.eq(value));

      Assert.assertEquals(0, usedIndexes(graph, traversal2));

      Assert.assertTrue(findUsedIndex(traversal2).isEmpty());

      List<Edge> result2 = traversal2.toList();
      Assert.assertEquals(1, result2.size());
      Assert.assertEquals(e2.id(), result2.get(0).id());
    }
  }

  @Test
  public void edgeNotUniqueIndexLookupWithValue() {
    OrientGraph graph = newGraph();

    createNotUniqueEdgeIndex(graph, edgeLabel1);

    String value = "value1";

    Assert.assertEquals(
        graph.getIndexedKeys(Edge.class, edgeLabel1),
        new HashSet<>(Collections.singletonList(key)));
    Assert.assertEquals(
        graph.getIndexedKeys(Edge.class, edgeLabel2), new HashSet<>(Collections.emptyList()));
    Assert.assertEquals(
        graph.getIndexedKeys(Vertex.class, vertexLabel1), new HashSet<>(Collections.emptyList()));

    Vertex v1 = graph.addVertex(label, vertexLabel1);
    Vertex v2 = graph.addVertex(label, vertexLabel1);
    Edge e1 = v1.addEdge(edgeLabel1, v2, key, value);
    Edge e2 = v1.addEdge(edgeLabel1, v2, key, value);
    Edge e3 = v1.addEdge(edgeLabel1, v2);

    // Verify that the traversal hits the index for the edges with label1
    GraphTraversal<Edge, Edge> traversal1 =
        graph.traversal().E().has(label, P.eq(edgeLabel1)).has(key, P.eq(value));
    Set<OrientIndexQuery> orientIndexQueries = findUsedIndex(traversal1);
    Assert.assertEquals(1, usedIndexes(graph, traversal1));
    Assert.assertFalse(orientIndexQueries.isEmpty());

    orientIndexQueries.forEach(
        orientIndexQuery -> {
          OIndex index = orientIndexQuery.index;
          Assert.assertEquals(3, index.getInternal().size());
          Assert.assertEquals(2, ((Collection) index.get(value)).size());
          Assert.assertTrue(((Collection) index.get(value)).contains(e1.id()));
          Assert.assertTrue(((Collection) index.get(value)).contains(e2.id()));
          Assert.assertFalse(((Collection) index.get(value)).contains(e3.id()));

          List<Edge> result1 = traversal1.toList();
          Assert.assertEquals(2, result1.size());
          Assert.assertTrue(result1.stream().map(Edge::id).anyMatch(e1.id()::equals));
          Assert.assertTrue(result1.stream().map(Edge::id).anyMatch(e2.id()::equals));
          Assert.assertFalse(result1.stream().map(Edge::id).anyMatch(e3.id()::equals));
        });
  }

  private Set<OrientIndexQuery> findUsedIndex(GraphTraversal<?, ?> traversal) {
    OrientGraphStepStrategy.instance().apply(traversal.asAdmin());

    @SuppressWarnings("rawtypes")
    OrientGraphStep orientGraphStep = (OrientGraphStep) traversal.asAdmin().getStartStep();

    return orientGraphStep.findIndex();
  }

  private int usedIndexes(OrientGraph graph, GraphTraversal<?, ?> traversal) {
    OrientGraphStepStrategy.instance().apply(traversal.asAdmin());

    List<Step> steps = traversal.asAdmin().getSteps();

    int idx = 0;
    for (Step step : steps) {

      if (step instanceof OrientGraphStep) {
        OrientGraphStep orientGraphStep = (OrientGraphStep) step;
        Optional<OrientGraphQuery> optional = orientGraphStep.buildQuery();
        Optional<Integer> index = optional.map(query -> query.usedIndexes(graph));
        idx += index.isPresent() ? index.get() : 0;
      }
    }

    return idx;
  }

  private void createVertexIndexLabel(OrientGraph graph, String vertexLabel) {
    Configuration config = new BaseConfiguration();
    config.setProperty("type", "UNIQUE");
    config.setProperty("keytype", OType.STRING);
    graph.createVertexIndex(key, vertexLabel, config);
  }

  private void createUniqueEdgeIndex(OrientGraph graph, String label) {
    Configuration config = new BaseConfiguration();
    config.setProperty("type", OClass.INDEX_TYPE.UNIQUE.name());
    config.setProperty("keytype", OType.STRING);
    graph.createEdgeIndex(key, label, config);
  }

  private void createNotUniqueEdgeIndex(OrientGraph graph, String label) {
    Configuration config = new BaseConfiguration();
    config.setProperty("type", OClass.INDEX_TYPE.NOTUNIQUE.name());
    config.setProperty("keytype", OType.STRING);
    graph.createEdgeIndex(key, label, config);
  }
}
