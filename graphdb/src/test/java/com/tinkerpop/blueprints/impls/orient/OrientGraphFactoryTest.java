package com.tinkerpop.blueprints.impls.orient;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** @author Luca Garulli (l.garulli--(at)--orientdb.com) (http://orientdb.com) */
@RunWith(JUnit4.class)
public class OrientGraphFactoryTest {

  @BeforeClass
  public static void setUp() {
    OrientBaseGraph.clearInitStack();
  }

  public void createTxPool() {
    OrientGraph graph = new OrientGraph("memory:testPool");
    graph.shutdown();

    OrientGraphFactory factory = new OrientGraphFactory("memory:testPool");
    factory.setupPool(5, 10);
    OrientBaseGraph g = factory.getTx();
    assertEquals(g.getClass(), OrientGraph.class);
    assertSame(g, OrientBaseGraph.getActiveGraph());
    g.shutdown();
    assertNull(OrientBaseGraph.getActiveGraph());
    factory.close();
  }

  @Test
  public void createTxPoolNestedCreations() {
    OrientGraph graph = new OrientGraph("memory:testPool");
    graph.shutdown();

    OrientGraphFactory factory = new OrientGraphFactory("memory:testPool");
    factory.setupPool(5, 10);

    OrientBaseGraph g = factory.getTx();
    assertEquals(g.getClass(), OrientGraph.class);
    assertSame(g, OrientBaseGraph.getActiveGraph());
    OrientBaseGraph g1 = factory.getTx();
    assertSame(g1, OrientBaseGraph.getActiveGraph());
    g1.shutdown();

    assertSame(g, OrientBaseGraph.getActiveGraph());

    g.shutdown();
    assertNull(OrientBaseGraph.getActiveGraph());
    factory.close();
  }

  @Test
  public void createNoTx() {
    OrientGraphFactory factory = new OrientGraphFactory("memory:testPool");
    OrientBaseGraph g = factory.getNoTx();
    assertSame(g, OrientBaseGraph.getActiveGraph());
    assertEquals(g.getClass(), OrientGraphNoTx.class);
    assertEquals(g.getRawGraph().getClass().getSuperclass(), ODatabaseDocumentTx.class);
    g.shutdown();
    assertNull(OrientBaseGraph.getActiveGraph());
    factory.close();
  }

  @Test
  public void createNoTxPool() {
    OrientGraph graph = new OrientGraph("memory:testPool");
    graph.shutdown();

    OrientGraphFactory factory = new OrientGraphFactory("memory:testPool");
    factory.setupPool(5, 10);
    OrientBaseGraph g = factory.getNoTx();
    assertSame(g, OrientBaseGraph.getActiveGraph());
    assertEquals(g.getClass(), OrientGraphNoTx.class);
    g.shutdown();
    assertNull(OrientBaseGraph.getActiveGraph());
    factory.close();
  }

  @Test
  public void dynamicType() {
    OrientGraphFactory factory = new OrientGraphFactory("memory:testPool");
    OrientBaseGraph g = factory.getTx();
    assertEquals(g.getClass(), OrientGraph.class);
    assertEquals(g.getRawGraph().getClass().getSuperclass(), ODatabaseDocumentTx.class);
    g.shutdown();

    g = factory.getNoTx();
    assertEquals(g.getClass(), OrientGraphNoTx.class);
    assertEquals(g.getRawGraph().getClass().getSuperclass(), ODatabaseDocumentTx.class);
    g.shutdown();
    factory.close();
  }

  @Test
  public void releaseThreadLocal() {
    OrientGraphFactory factory = new OrientGraphFactory("memory:testPool");
    OrientBaseGraph createGraph = factory.getTx();
    createGraph.shutdown();
    factory.setupPool(10, 20);
    OrientBaseGraph g = factory.getTx();
    g.addVertex(null);
    g.commit();
    g.shutdown();
    assertNull(ODatabaseRecordThreadLocal.instance().getIfDefined());
    factory.close();
  }

  @Test
  public void textReqTx() {
    final OrientGraphFactory gfactory = new OrientGraphFactory("memory:testPool");
    gfactory.setRequireTransaction(false);
    gfactory.declareIntent(new OIntentMassiveInsert());

    OrientGraph g = gfactory.getTx();
    OrientVertex v1 = g.addVertex(null);
    OrientVertex v2 = g.addVertex(null);
    v1.addEdge("E", v2);

    g.shutdown();
    gfactory.close();
  }

  @Test
  public void testCreateGraphByOrientGraphFactory() {
    OrientGraphFactory factory = new OrientGraphFactory("memory:test01").setupPool(1, 10);
    OrientGraph graph = factory.getTx();
    assertNotNull(graph);
    graph.shutdown();
  }
}
