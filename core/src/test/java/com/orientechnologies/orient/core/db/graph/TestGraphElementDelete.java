package com.orientechnologies.orient.core.db.graph;

import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.record.ODirection;
import com.orientechnologies.orient.core.record.OEdge;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.OVertex;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

/**
 * Created by tglman on 20/02/17.
 */
public class TestGraphElementDelete {

  private OrientDB          orientDB;
  private ODatabaseDocument database;

  @Before
  public void before() {
    orientDB = new OrientDB("embedded:", OrientDBConfig.defaultConfig());
    orientDB.createIfNotExists("test", ODatabaseType.MEMORY);
    database = orientDB.open("test", "admin", "admin");

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

}
