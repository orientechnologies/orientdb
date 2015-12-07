package com.orientechnologies.orient.graph;

import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.orientechnologies.orient.core.metadata.schema.OType;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientEdge;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

public class TestFailOperationOnRemovedElement {
  private OrientGraph grap;

  @Before
  public void before() {
    grap = new OrientGraph("memory:" + TestFailOperationOnRemovedElement.class.getSimpleName());
  }

  @After
  public void after() {
    grap.drop();
  }

  @Test(expected = ORecordNotFoundException.class)
  public void testAddEdgeOnRemovedVertexSameTransaction() {
    Vertex v = grap.addVertex(null);
    Vertex v1 = grap.addVertex(null);

    v.remove();
    v.addEdge("test", v1);
  }

  @Test(expected = ORecordNotFoundException.class)
  public void testAddEdgeOnRemovedVertex() {
    Vertex v = grap.addVertex(null);
    Vertex v1 = grap.addVertex(null);
    grap.commit();

    v.remove();
    v.addEdge("test", v1);
  }

  @Test(expected = ORecordNotFoundException.class)
  public void testAddEdgeToRemovedVertex() {
    Vertex v = grap.addVertex(null);
    Vertex v1 = grap.addVertex(null);
    grap.commit();

    v1.remove();
    v.addEdge("test", v1);
  }

  @Test(expected = ORecordNotFoundException.class)
  public void testSetPropertyOnRemovedVertex() {
    Vertex v = grap.addVertex(null);
    grap.commit();

    v.remove();
    v.setProperty("test", "aaaa");
  }

  @Test(expected = ORecordNotFoundException.class)
  public void testSetPropertyTypeOnRemovedVertex() {
    OrientVertex v = grap.addVertex(null);
    grap.commit();

    v.remove();
    v.setProperty("test", "aaaa", OType.STRING);
  }

  @Test(expected = ORecordNotFoundException.class)
  public void testSetPropertiesOnRemovedVertex() {
    OrientVertex v = grap.addVertex(null);
    grap.commit();

    v.remove();
    v.setProperties("test", "aaaa");
  }

  @Test(expected = ORecordNotFoundException.class)
  public void testDoubleRemoveVertex() {
    OrientVertex v = grap.addVertex(null);
    grap.commit();

    v.remove();
    v.remove();
  }

  @Test(expected = ORecordNotFoundException.class)
  public void testMoveToOfRemovedVertex() {
    OrientVertex v = grap.addVertex(null);
    grap.commit();

    v.remove();
    v.moveTo("test", "test");
  }

  @Test(expected = ORecordNotFoundException.class)
  public void testSetPropertiesRemovedEdge() {
    OrientVertex v = grap.addVertex(null);
    OrientVertex v1 = grap.addVertex(null);
    OrientEdge e = (OrientEdge) v.addEdge("test", v1);
    grap.commit();

    e.remove();
    e.setProperties("test", "test");
  }

  @Test(expected = ORecordNotFoundException.class)
  public void testSetPropertyRemovedEdge() {
    OrientVertex v = grap.addVertex(null);
    OrientVertex v1 = grap.addVertex(null);
    OrientEdge e = (OrientEdge) v.addEdge("test", v1);
    grap.commit();

    e.remove();
    e.setProperties("test", "test");
  }

  @Test(expected = ORecordNotFoundException.class)
  public void testDoubleRemovedEdge() {
    OrientVertex v = grap.addVertex(null);
    OrientVertex v1 = grap.addVertex(null);
    OrientEdge e = (OrientEdge) v.addEdge("test", v1);
    grap.commit();

    e.remove();
    e.remove();
  }

  @Test(expected = ORecordNotFoundException.class)
  public void testPropertyTypeRemovedEdge() {
    OrientVertex v = grap.addVertex(null);
    OrientVertex v1 = grap.addVertex(null);
    OrientEdge e = (OrientEdge) v.addEdge("test", v1);
    grap.commit();

    e.remove();
    e.setProperty("test", "test", OType.STRING);
  }

}
