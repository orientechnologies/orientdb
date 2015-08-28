package com.tinkerpop.blueprints.impls.orient;

import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.tinkerpop.blueprints.Vertex;

public class OrientVertexWithEmbeddedListTest {

  @Test
  public void testVertexWithEmbeddedList() {
    final String url = "memory:" + this.getClass().getSimpleName();
    OrientGraphFactory factory = new OrientGraphFactory(url, "admin", "admin");
    OrientGraphNoTx gn = factory.getNoTx();
    gn.createVertexType("testv");
    gn.shutdown();
    OrientGraph g = factory.getTx();
    try {
      Vertex v = g.addVertex("class:testv");
      v.setProperty("name", "test");
      ODocument d1 = new ODocument();
      d1.field("f", "one");
      ODocument d2 = new ODocument();
      d2.field("f", "two");
      List<ODocument> ds = new ArrayList<ODocument>();
      ds.add(d1);
      ds.add(d2);
      v.setProperty("docs", ds);
    } finally {
      g.shutdown();
    }
    g = factory.getTx();
    try {
      Iterable<Vertex> vs = g.getVertices();
      Vertex v = vs.iterator().next();
      Assert.assertEquals(v.getProperty("name"), "test");

      Iterable<ODocument> ds = v.getProperty("docs");
      
      int counter = 0;
      for (ODocument d : ds) {
        Assert.assertNotNull(d.field("f"));
        counter++;
      }
      Assert.assertEquals(2, counter);
    } finally {
      g.drop();
    }
  }

}
