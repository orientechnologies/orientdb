package com.tinkerpop.blueprints.impls.orient;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * Created by tglman on 28/04/16.
 */
public class SimplePropertyLinkTest {

  @Test
  public void testSimplePropertyLink() {
    OrientGraph graph = new OrientGraph("memory:" + SimplePropertyLinkTest.class.getSimpleName());
    OrientVertex v1 = graph.addVertex(null);
    OrientVertex v2 = graph.addVertex(null);
    v1.setProperty("link", v2);
    v1.save();
    v2.save();
    graph.commit();
    graph.getRawGraph().getLocalCache().clear();
    assertTrue(((OIdentifiable) graph.getVertex(v1.getIdentity()).getProperty("link")).getIdentity().isPersistent());

  }

}
