package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.BaseMemoryDatabase;
import com.orientechnologies.orient.core.record.OVertex;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
public class ODeleteVertexStatementExecutionTest extends BaseMemoryDatabase {

  @Test
  public void testDeleteSingleVertex() {
    String className = "testDeleteSingleVertex";
    db.createVertexClass(className);
    for (int i = 0; i < 10; i++) {
      OVertex v1 = db.newVertex(className);
      v1.setProperty("name", "a" + i);
      v1.save();
    }

    db.command("DELETE VERTEX " + className + " WHERE name = 'a3'").close();
    OResultSet rs = db.query("SELECT FROM " + className);
    Assert.assertEquals(9, rs.stream().count());
    rs.close();
  }

  @Test
  public void testDeleteAllVertices() {
    String className = "testDeleteAllVertices";
    db.createVertexClass(className);
    for (int i = 0; i < 10; i++) {
      OVertex v1 = db.newVertex(className);
      v1.setProperty("name", "a" + i);
      v1.save();
    }

    db.command("DELETE VERTEX " + className).close();
    OResultSet rs = db.query("SELECT FROM " + className);
    Assert.assertEquals(0, rs.stream().count());
    rs.close();
  }

  @Test
  public void testFilterClass() {
    String className1 = "testDeleteAllVertices1";
    db.createVertexClass(className1);
    for (int i = 0; i < 10; i++) {
      OVertex v1 = db.newVertex(className1);
      v1.setProperty("name", "a" + i);
      v1.save();
    }

    String className2 = "testDeleteAllVertices2";
    db.createVertexClass(className2);
    for (int i = 0; i < 10; i++) {
      OVertex v1 = db.newVertex(className2);
      v1.setProperty("name", "a" + i);
      v1.save();
    }

    db.command("DELETE VERTEX " + className1).close();
    OResultSet rs = db.query("SELECT FROM " + className1);
    Assert.assertEquals(0, rs.stream().count());
    rs.close();

    rs = db.query("SELECT FROM " + className2);
    Assert.assertEquals(10, rs.stream().count());
    rs.close();
  }
}
