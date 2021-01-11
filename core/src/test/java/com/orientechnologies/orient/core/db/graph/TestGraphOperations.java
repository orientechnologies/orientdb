package com.orientechnologies.orient.core.db.graph;

import com.orientechnologies.orient.core.OCreateDatabaseUtil;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.OEdge;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.storage.ORecordDuplicatedException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/** Created by tglman on 20/02/17. */
public class TestGraphOperations {

  private OrientDB orientDB;
  private ODatabaseDocument database;

  @Before
  public void before() {
    orientDB =
        OCreateDatabaseUtil.createDatabase(
            "TestGraphOperations", "embedded:", OCreateDatabaseUtil.TYPE_MEMORY);
    database =
        orientDB.open("TestGraphOperations", "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
  }

  @After
  public void after() {
    database.close();
    orientDB.close();
  }

  @Test
  public void testEdgeUniqueConstraint() {

    database.createVertexClass("TestVertex");

    OClass testLabel = database.createEdgeClass("TestLabel");

    OProperty key = testLabel.createProperty("key", OType.STRING);

    key.createIndex(OClass.INDEX_TYPE.UNIQUE);

    OVertex vertex = database.newVertex("TestVertex");

    OVertex vertex1 = database.newVertex("TestVertex");

    OEdge edge = vertex.addEdge(vertex1, "TestLabel");

    edge.setProperty("key", "unique");
    database.save(vertex);

    try {
      edge = vertex.addEdge(vertex1, "TestLabel");
      edge.setProperty("key", "unique");
      database.save(edge);
      Assert.fail("It should not be inserted  a duplicated edge");
    } catch (ORecordDuplicatedException e) {

    }

    edge = vertex.addEdge(vertex1, "TestLabel");

    edge.setProperty("key", "notunique");

    database.save(edge);
  }
}
