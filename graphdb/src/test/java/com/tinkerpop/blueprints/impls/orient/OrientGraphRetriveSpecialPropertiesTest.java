package com.tinkerpop.blueprints.impls.orient;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.tinkerpop.blueprints.Vertex;
import java.util.Arrays;
import java.util.HashSet;
import org.junit.Assert;
import org.junit.Test;

public class OrientGraphRetriveSpecialPropertiesTest {

  @Test
  public void testVertexInAProperty() {
    final String url = "memory:" + this.getClass().getSimpleName();
    OrientGraph graph = new OrientGraph(url);
    graph.drop();
    graph = new OrientGraph(url);

    Vertex vertexa = graph.addVertex(null);
    vertexa.setProperty("test", "testa");
    vertexa.setProperty("test1", "testb");

    graph.commit();
    graph.getRawGraph().close();
    graph.getRawGraph().open("admin", "admin");
    Vertex vertb = graph.getVertex(vertexa.getId());
    Assert.assertEquals("V", vertb.getProperty("@class"));
    Assert.assertTrue(vertb.getProperty("@rid") instanceof OIdentifiable);

    Assert.assertEquals(
        vertb.getPropertyKeys(), new HashSet<String>(Arrays.asList("test", "test1")));

    graph.drop();
  }
}
