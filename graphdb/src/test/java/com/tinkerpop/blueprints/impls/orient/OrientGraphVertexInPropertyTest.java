package com.tinkerpop.blueprints.impls.orient;

import org.junit.Assert;
import org.junit.Test;

import com.orientechnologies.orient.core.metadata.schema.OType;
import com.tinkerpop.blueprints.Vertex;

public class OrientGraphVertexInPropertyTest {

  @Test
  public void testVertexInAProperty() {
    final String url = "memory:" + this.getClass().getSimpleName();
    OrientGraph graph = new OrientGraph(url);
    graph.drop();
    graph = new OrientGraph(url);

    Vertex vertexa = graph.addVertex(null);
    Object aid = vertexa.getId();
    Vertex vertexb = graph.addVertex(null);
    Object id = vertexb.getId();
    vertexb.setProperty("test", vertexa);

    graph.commit();
    graph.getRawGraph().close();
    graph.getRawGraph().open("admin", "admin");
    Vertex vertb = graph.getVertex(id);
    Assert.assertNotEquals(OType.CUSTOM, ((OrientVertex) vertb).getRecord().fieldType("test"));
    Assert.assertEquals(aid, ((Vertex) vertb.getProperty("test")).getId());
    graph.drop();
  }

  @Test
  public void testVertexOTypeDetection() {

    Assert.assertEquals(OType.LINK, OType.getTypeByClass(OrientVertex.class));
    Assert.assertEquals(OType.LINK, OType.getTypeByClass(OrientEdge.class));

    Assert.assertEquals(OType.LINK, OType.getTypeByValue(new OrientVertex()));
    Assert.assertEquals(OType.LINK, OType.getTypeByValue(new OrientEdge()));

  }

}
