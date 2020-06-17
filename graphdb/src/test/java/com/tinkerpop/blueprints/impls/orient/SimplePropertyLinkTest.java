package com.tinkerpop.blueprints.impls.orient;

import static org.junit.Assert.assertTrue;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** Created by tglman on 28/04/16. */
public class SimplePropertyLinkTest {

  private OrientGraph graph;

  @Before
  public void before() {
    graph = new OrientGraph("memory:" + SimplePropertyLinkTest.class.getSimpleName());
  }

  @After
  public void after() {
    graph.drop();
  }

  @Test
  public void testSimplePropertyLink() {
    OrientVertex v1 = graph.addVertex(null);
    OrientVertex v2 = graph.addVertex(null);
    v1.setProperty("link", v2);
    v1.save();
    v2.save();
    graph.commit();
    graph.getRawGraph().getLocalCache().clear();
    assertTrue(
        ((OIdentifiable) graph.getVertex(v1.getIdentity()).getProperty("link"))
            .getIdentity()
            .isPersistent());
  }
}
