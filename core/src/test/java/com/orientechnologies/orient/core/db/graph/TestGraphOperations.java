package com.orientechnologies.orient.core.db.graph;

import com.orientechnologies.BaseMemoryDatabase;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.OEdge;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.storage.ORecordDuplicatedException;
import org.junit.Assert;
import org.junit.Test;

/** Created by tglman on 20/02/17. */
public class TestGraphOperations extends BaseMemoryDatabase {

  @Test
  public void testEdgeUniqueConstraint() {

    db.createVertexClass("TestVertex");

    OClass testLabel = db.createEdgeClass("TestLabel");

    OProperty key = testLabel.createProperty("key", OType.STRING);

    key.createIndex(OClass.INDEX_TYPE.UNIQUE);

    OVertex vertex = db.newVertex("TestVertex");

    OVertex vertex1 = db.newVertex("TestVertex");

    OEdge edge = vertex.addEdge(vertex1, "TestLabel");

    edge.setProperty("key", "unique");
    db.save(vertex);

    try {
      edge = vertex.addEdge(vertex1, "TestLabel");
      edge.setProperty("key", "unique");
      db.save(edge);
      Assert.fail("It should not be inserted  a duplicated edge");
    } catch (ORecordDuplicatedException e) {

    }

    edge = vertex.addEdge(vertex1, "TestLabel");

    edge.setProperty("key", "notunique");

    db.save(edge);
  }
}
