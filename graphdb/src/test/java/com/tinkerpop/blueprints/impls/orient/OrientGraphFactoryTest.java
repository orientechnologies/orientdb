package com.tinkerpop.blueprints.impls.orient;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTxPooled;
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert;

/**
 * 
 * @author Luca Garulli (http://www.orientechnologies.com)
 */
@RunWith(JUnit4.class)
public class OrientGraphFactoryTest {
  @Test
  public void createTx() {
    OrientGraphFactory factory = new OrientGraphFactory("memory:testPool");
    OrientBaseGraph g = factory.getTx();
    assertEquals(g.getClass(), OrientGraph.class);
    assertEquals(g.getRawGraph().getClass(), ODatabaseDocumentTx.class);
    g.shutdown();
    factory.close();
  }

  @Test
  public void createNoTx() {
    OrientGraphFactory factory = new OrientGraphFactory("memory:testPool");
    OrientBaseGraph g = factory.getNoTx();
    assertEquals(g.getClass(), OrientGraphNoTx.class);
    assertEquals(g.getRawGraph().getClass(), ODatabaseDocumentTx.class);
    g.shutdown();
    factory.close();
  }

  @Test
  public void dynamicType() {
    OrientGraphFactory factory = new OrientGraphFactory("memory:testPool");
    OrientBaseGraph g = factory.getTx();
    assertEquals(g.getClass(), OrientGraph.class);
    assertEquals(g.getRawGraph().getClass(), ODatabaseDocumentTx.class);
    g.shutdown();

    g = factory.getNoTx();
    assertEquals(g.getClass(), OrientGraphNoTx.class);
    assertEquals(g.getRawGraph().getClass(), ODatabaseDocumentTx.class);
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
    assertNull(ODatabaseRecordThreadLocal.INSTANCE.getIfDefined());
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
}
