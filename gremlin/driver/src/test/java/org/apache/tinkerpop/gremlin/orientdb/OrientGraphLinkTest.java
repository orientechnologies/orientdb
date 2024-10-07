package org.apache.tinkerpop.gremlin.orientdb;

import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;

/** Created by Luigi Dell'Aquila (l.dellaquila - at - orientdb.com) */
public class OrientGraphLinkTest extends OrientGraphBaseTest {

  @Test
  public void testLinks() {

    OrientGraph db = factory.getNoTx();

    Vertex vertex = db.addVertex(T.label, getClass().getSimpleName(), "name", "John");
    Vertex vertex1 = db.addVertex(T.label, getClass().getSimpleName(), "name", "Luke");
    vertex.property("friend", vertex1);

    db.close();

    Object rid = vertex.id();
    db = factory.getNoTx();

    Vertex v = db.vertices(rid).next();
    Object val = v.value("friend");
    Assert.assertTrue(val instanceof OrientVertex);
    Assert.assertEquals("Luke", ((OrientVertex) val).value("name"));
    db.close();
  }
}
