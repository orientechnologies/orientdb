package com.orientechnologies.orient.core.db.graph;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.orientechnologies.orient.core.OCreateDatabaseUtil;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.record.ODirection;
import com.orientechnologies.orient.core.record.OEdge;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.OVertex;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/** Created by tglman on 20/02/17. */
public class TestGraphElementDelete {

  private OrientDB orientDB;
  private ODatabaseDocument database;

  @Before
  public void before() {
    orientDB =
        OCreateDatabaseUtil.createDatabase("test", "embedded:", OCreateDatabaseUtil.TYPE_MEMORY);
    database = orientDB.open("test", "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
  }

  @After
  public void after() {
    database.close();
    orientDB.close();
  }

  @Test
  public void testDeleteVertex() {

    OVertex vertex = database.newVertex("V");
    OVertex vertex1 = database.newVertex("V");
    OEdge edge = vertex.addEdge(vertex1, "E");
    database.save(edge);

    database.delete(vertex);

    assertNull(database.load(edge.getIdentity()));
  }

  @Test
  public void testDeleteEdge() {

    OVertex vertex = database.newVertex("V");
    OVertex vertex1 = database.newVertex("V");
    OEdge edge = vertex.addEdge(vertex1, "E");
    database.save(edge);

    database.delete(edge);

    assertFalse(vertex.getEdges(ODirection.OUT, "E").iterator().hasNext());
  }

  @Test
  public void testDeleteEdgeConcurrentModification() {

    OVertex vertex = database.newVertex("V");
    OVertex vertex1 = database.newVertex("V");
    OEdge edge = vertex.addEdge(vertex1, "E");
    database.save(edge);
    database.getLocalCache().clear();
    OElement instance = database.load(edge.getIdentity());
    instance.setProperty("one", "two");
    database.save(instance);
    try {
      database.delete(edge);
      Assert.fail();
    } catch (OConcurrentModificationException e) {

    }

    assertNotNull(database.load(edge.getIdentity()));
    assertNotNull(database.load(vertex.getIdentity()));
    assertNotNull(database.load(vertex1.getIdentity()));
    assertTrue(
        ((OVertex) database.load(vertex.getIdentity()))
            .getEdges(ODirection.OUT, "E")
            .iterator()
            .hasNext());
    assertTrue(
        ((OVertex) database.load(vertex1.getIdentity()))
            .getEdges(ODirection.IN, "E")
            .iterator()
            .hasNext());
  }
}
