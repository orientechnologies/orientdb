package org.apache.tinkerpop.gremlin.orientdb;

import static com.google.common.collect.Lists.newArrayList;
import static java.util.stream.Collectors.toMap;
import static org.apache.tinkerpop.gremlin.structure.Transaction.CLOSE_BEHAVIOR.COMMIT;
import static org.apache.tinkerpop.gremlin.structure.Transaction.CLOSE_BEHAVIOR.ROLLBACK;
import static org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality.single;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.exception.OAcquireTimeoutException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.tinkerpop.gremlin.structure.*;
import org.junit.Assert;
import org.junit.Test;

public class OrientGraphTest {

  protected OrientGraphFactory graphFactory() {
    return new OrientGraphFactory(
        "memory:tinkerpop-" + ThreadLocalRandom.current().nextInt(Integer.MAX_VALUE));
  }

  public static final String TEST_VALUE = "SomeValue";

  @Test
  public void testGraphTransactions() throws Exception {
    OrientGraphBaseFactory graphFactory = graphFactory();
    Object id;
    try (Graph graph = graphFactory.getTx()) {
      try (Transaction tx = graph.tx()) {
        tx.onClose(COMMIT);
        Vertex vertex = graph.addVertex();
        id = vertex.id();
        vertex.property("test", TEST_VALUE);
      }
    }
    assertNotNull("A vertex should have been created in the first transaction", id);

    // Modify property and rollback
    try (Graph graph = graphFactory.getTx()) {
      try (Transaction tx = graph.tx()) {
        tx.onClose(ROLLBACK);
        Vertex vertex = graph.vertices(id).next();
        assertNotNull(vertex);
        vertex.property("test", "changed");
      }

      try (Transaction tx = graph.tx()) {
        tx.onClose(ROLLBACK);
        Vertex vertex = graph.vertices(id).next();
        assertNotNull(vertex);
        vertex.property("test", "changed");
      }
    }

    try (Graph graph = graphFactory.getTx()) {
      Vertex vertex = graph.vertices(id).next();
      assertEquals(
          "The property value should not have been changed.", TEST_VALUE, vertex.value("test"));
    }

    // 1. Modify property rollback, 2. Modify property commit
    try (Graph graph = graphFactory.getTx()) {
      try (Transaction tx = graph.tx()) {
        tx.onClose(ROLLBACK);
        Vertex vertex = graph.vertices(id).next();
        assertNotNull(vertex);
        vertex.property("test", "changed");
      }

      try (Transaction tx = graph.tx()) {
        tx.onClose(COMMIT);
        Vertex vertex = graph.vertices(id).next();
        assertNotNull(vertex);
        vertex.property("test", "changed");
      }
    }

    try (Graph graph = graphFactory.getTx()) {
      Vertex vertex = graph.vertices(id).next();
      assertEquals(
          "The property value should not have been changed.", "changed", vertex.value("test"));
    }
  }

  @Test
  public void testGraphTransactionOnNoTrxOrientGraph() throws Exception {
    OrientGraphBaseFactory graphFactory = graphFactory();
    Object id;
    try (Graph graph = graphFactory.getTx()) {
      try (Transaction tx = graph.tx()) {
        tx.onClose(COMMIT);
        Vertex vertex = graph.addVertex();
        id = vertex.id();
        vertex.property("test", TEST_VALUE);
      }
    }
    assertNotNull("A vertex should have been created in the first transaction", id);

    try (Graph graph = graphFactory.getNoTx()) {
      try (Transaction tx = graph.tx()) {
        tx.onClose(ROLLBACK);
        Vertex vertex = graph.vertices(id).next();
        assertNotNull(vertex);
        vertex.property("test", "changed");
      }
    }

    try (Graph graph = graphFactory.getTx()) {
      Vertex vertex = graph.vertices(id).next();
      assertEquals(
          "The property value should have been changed since the graph was used in noTx mode and all actions are atomic.",
          "changed",
          vertex.value("test"));
    }
  }

  @Test
  public void testGraph() throws Exception {
    Graph graph = graphFactory().getNoTx();
    performBasicTests(graph);
    graph.close();
  }

  @Test
  public void testPooledGraph() throws Exception {
    Graph graph = graphFactory().setupPool(5).getNoTx();
    performBasicTests(graph);
    graph.close();
  }

  @Test
  public void testMTPooledGraph() throws Exception {
    int THREADS = 2;
    OrientGraphBaseFactory factory = graphFactory().setupPool(THREADS);
    for (int i = 0; i < THREADS; i++) {
      factory.getTx();
    }

    boolean success = false;
    try {
      factory.getTx();
    } catch (OAcquireTimeoutException e) {
      success = true;
    }
    Assert.assertTrue(success);
  }

  @Test
  public void testTransactionalGraph() throws Exception {
    Graph graph = graphFactory().getTx();
    performBasicTests(graph);
    graph.close();
  }

  @Test
  public void testPooledTransactionalGraph() throws Exception {
    Graph graph = graphFactory().setupPool(5).getTx();
    performBasicTests(graph);
    graph.close();
  }

  @Test
  public void testUnprefixedLabelGraph() throws Exception {
    Graph graph = graphFactory().setLabelAsClassName(true).getNoTx();
    assertEquals(true, graph.configuration().getBoolean(OrientGraph.CONFIG_LABEL_AS_CLASSNAME));

    performBasicTests(graph);

    Vertex vertex = graph.addVertex("VERTEX_LABEL");
    assertEquals("VERTEX_LABEL", vertex.label());

    try {
      graph.addVertex("EDGE_LABEL");
      Assert.fail("must throw unable to create different super class");
    } catch (IllegalArgumentException e) {
      assertTrue(
          e.getMessage().startsWith("unable to create class 'EDGE_LABEL' as subclass of 'V'"));
    }

    graph.close();
  }

  protected void performBasicTests(Graph graph) {
    Vertex vertex = graph.addVertex();
    assertNotNull(vertex);
    assertNotNull(vertex.id());

    vertex.property("test", TEST_VALUE);
    assertEquals(TEST_VALUE, vertex.value("test"));

    Property<String> property = vertex.property("test");
    assertNotNull(property);
    assertTrue(property.isPresent());
    assertEquals(TEST_VALUE, property.value());
    property.remove();
    assertFalse(property.isPresent());

    // Create test vertices for edge
    Vertex vertexA = graph.addVertex();
    Vertex vertexB = graph.addVertex();
    Edge edge = vertexA.addEdge("EDGE_LABEL", vertexB);
    assertEquals("EDGE_LABEL", edge.label());

    // Test edge properties
    assertNotNull(edge.property("test", TEST_VALUE));
    Property<String> edgeProperty = edge.property("test");
    assertNotNull(edgeProperty);
    assertTrue(edgeProperty.isPresent());
    assertEquals(TEST_VALUE, edgeProperty.value());
    edgeProperty.remove();
    assertFalse(edgeProperty.isPresent());

    edge.property("test", TEST_VALUE);
    assertEquals(TEST_VALUE, edge.value("test"));

    // Check vertices of edge
    Vertex out = edge.outVertex();
    assertNotNull(out);
    assertEquals(vertexA.id(), out.id());

    Vertex in = edge.inVertex();
    assertNotNull(in);
    assertEquals(vertexB.id(), in.id());
  }

  @Test
  public void testStaticIterator() throws Exception {
    try (Graph graph = graphFactory().getTx()) {
      Vertex v1 = graph.addVertex();

      Iterator<Vertex> iterator = graph.vertices();

      // v2 should not be returned by the Iterator
      Vertex v2 = graph.addVertex();

      assertTrue(iterator.hasNext());
      iterator.next();
      assertFalse(iterator.hasNext());

      graph.close();
    }
  }

  @Test
  public void testMetaProperties() throws Exception {
    try (Graph graph = graphFactory().getTx()) {
      Vertex v1 = graph.addVertex();
      VertexProperty<String> prop =
          v1.property(
              single, "key", "value", "meta_key", "meta_value", "meta_key_2", "meta_value_2");

      Map<String, String> keysValues =
          StreamUtils.asStream(prop.properties())
              .collect(toMap(p -> p.key(), p -> (String) p.value()));
      assertThat(keysValues, hasEntry("meta_key", "meta_value"));
      assertThat(keysValues, hasEntry("meta_key_2", "meta_value_2"));

      Map<String, Property<?>> props =
          StreamUtils.asStream(prop.properties()).collect(toMap(p -> p.key(), p -> p));

      props.get("meta_key_2").remove();

      keysValues =
          StreamUtils.asStream(prop.properties())
              .collect(toMap(p -> p.key(), p -> (String) p.value()));
      assertThat(keysValues, hasEntry("meta_key", "meta_value"));
      assertThat(keysValues, not(hasEntry("meta_key_2", "meta_value_2")));

      props.get("meta_key").remove();

      keysValues =
          StreamUtils.asStream(prop.properties())
              .collect(toMap(p -> p.key(), p -> (String) p.value()));
      assertThat(keysValues, not(hasEntry("meta_key", "meta_value")));
      assertThat(keysValues, not(hasEntry("meta_key_2", "meta_value_2")));

      graph.close();
    }
  }

  @Test
  public void removeVertex() throws Exception {
    try (Graph graph = graphFactory().getTx()) {
      Vertex v1 = graph.addVertex();
      Vertex v2 = graph.addVertex();
      v1.addEdge("label1", v2);
      v2.addEdge("label2", v1);

      assertThat(newArrayList(v2.edges(Direction.IN, "label1")), hasSize(1));
      assertThat(newArrayList(v2.edges(Direction.OUT, "label2")), hasSize(1));

      v1.remove();

      assertThat(newArrayList(v2.edges(Direction.IN, "label1")), hasSize(0));
      assertThat(newArrayList(v2.edges(Direction.OUT, "label2")), hasSize(0));
    }
  }

  @Test
  public void checkClassNameConstruction() {
    String edgeLabel = "edge_label";
    String vertexLabel = "vertex_label";
    OrientGraphBaseFactory factory = new OrientGraphFactory("memory:myGraph");
    OrientGraph graph = factory.getNoTx();

    graph.createVertexClass(vertexLabel);
    graph.createEdgeClass(edgeLabel);

    graph.database().browseClass(vertexLabel);
    graph.database().browseClass(edgeLabel);
  }

  // is should open a memory db
  @Test
  public void openWithEmptyConfig() {
    OrientGraph graph = OrientGraph.open(new BaseConfiguration());

    graph.addVertex(T.label, "V", "name", "Foo");

    Assert.assertEquals(new Long(1), graph.traversal().V().count().toList().iterator().next());

    graph.drop();
  }

  // is should open a memory db
  @Test
  public void openWithMemoryDatabase() {
    OrientGraph graph = OrientGraph.open();

    try {
      graph.addVertex(T.label, "V", "name", "Foo");

      Assert.assertEquals(new Long(1), graph.traversal().V().count().toList().iterator().next());
    } finally {
      graph.drop();
    }
  }

  // is should open a plocal db
  @Test
  public void openWithLocalDatabase() {
    OrientGraph graph = OrientGraph.open("embedded:./target/databases/openWithLocalDatabase");

    try {
      graph.addVertex(T.label, "V", "name", "Foo");

      Assert.assertEquals(new Long(1), graph.traversal().V().count().toList().iterator().next());
    } finally {
      graph.drop();
    }
  }

  @Test
  public void openWithConfiguration() {

    BaseConfiguration config = new BaseConfiguration();

    config.setProperty(OrientGraph.CONFIG_URL, "embedded:./target/databases/openWithLocalDatabase");
    config.setProperty(OrientGraph.CONFIG_TRANSACTIONAL, true);

    OrientGraph graph = OrientGraph.open(config);

    try {

      assertEquals(true, graph.features().graph().supportsTransactions());
      Assert.assertEquals(false, graph.tx().isOpen());
      OrientVertex vertex = (OrientVertex) graph.addVertex(T.label, "V", "name", "Foo");
      Assert.assertEquals(true, graph.tx().isOpen());
      Assert.assertEquals(true, vertex.getRawElement().getIdentity().isNew());
      graph.tx().commit();
      Assert.assertEquals(new Long(1), graph.traversal().V().count().toList().iterator().next());
    } finally {
      graph.drop();
    }
  }

  @Test
  public void shouldOpenAFactoryInMemory() {

    OrientGraphFactory factory = new OrientGraphFactory();

    OrientGraph graph = factory.getNoTx();
    graph.addVertex(T.label, "V", "name", "Foo");

    Assert.assertEquals(new Long(1), graph.traversal().V().count().toList().iterator().next());

    factory.drop();
  }

  @Test
  public void checkMemoryDrop() {

    OrientDB orientDB =
        new OrientDB(
            "embedded:",
            OrientDBConfig.builder()
                .addConfig(OGlobalConfiguration.CREATE_DEFAULT_USERS, false)
                .build());
    OrientGraphFactory factory =
        new OrientGraphFactory(orientDB, "_dropDB", ODatabaseType.MEMORY, "admin", "admin");

    factory.getNoTx();

    Assert.assertEquals(true, orientDB.exists("_dropDB"));

    factory.drop();
  }
}
