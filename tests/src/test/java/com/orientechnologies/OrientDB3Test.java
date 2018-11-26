package com.orientechnologies;

import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.OEdge;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class OrientDB3Test {
  final String dbUrl = "remote:localhost";

  final String dbName = "demo";

  OrientDB orientDB;

  final String teacher = "Teacher";

  final String student = "Student";

  final String teach = "teach";

  final String name = "name";

  OClass edge;

  @Before
  public void createDatabase() {
    this.orientDB = new OrientDB(dbUrl, "root", "root", OrientDBConfig.defaultConfig());
    orientDB.createIfNotExists(dbName, ODatabaseType.PLOCAL);
    createSchema();
  }

  private void createSchema() {
    ODatabaseSession db = getSession();

    OClass t = db.getClass(teacher);
    if (t == null) {
      System.out.println("Create teacher class");
      t = db.createVertexClass(teacher);
    }
    if (t.getProperty(name) == null) {
      System.out.println("Create name property for teacher");
      t.createProperty(name, OType.STRING);
    }

    OClass s = db.getClass(student);
    if (s == null) {
      System.out.println("Create student class");
      s = db.createVertexClass(student);
    }
    if (s.getProperty(name) == null) {
      System.out.println("Create name property for student");
      s.createProperty(name, OType.STRING);
    }

    if ((edge = db.getClass(teach)) == null) {
      System.out.println("Create edge for teacher and student");
      edge = db.createEdgeClass(teach);
    }
    db.close();
  }

  ODatabaseSession getSession() {
    return orientDB.open(dbName, "admin", "admin");
  }

  @Test
  public void testMixJavaApiAndSqlWithTraverseInTransaction() {
    try (ODatabaseSession db = getSession()) {
      String teacherName = "teacher1";
      String studentName = "student1";
      String query1 = "TRAVERSE in,out from " + teacher + " while name = ?";
      String query2 = "select from (traverse out('" + teach + "')  from (select from Teacher where name=?)) where @class='" + student + "'";

      db.begin();
      OVertex teacherV = db.newVertex(teacher);
      teacherV.setProperty(name, teacherName);

      OVertex studentV = db.newVertex(student);
      studentV.setProperty(name, studentName);

      OEdge teachE = db.newEdge(teacherV, studentV, edge);
      //            teacherV.save();
      //            studentV.save();
      teachE.save();

      System.out.println("----------------------");
      System.out.println("After saving and before committing, the query1 results:");

      OResultSet rs = db.query(query1, teacherName);
      while (rs.hasNext()) {
        OResult row = rs.next();
        System.out.println(row);
      }
      rs.close();
      System.out.println("----------------------\n\n");

      System.out.println("----------------------");
      System.out.println("After saving and before committing, the query2 results:");
      rs = db.query(query2, teacherName);
      while (rs.hasNext()) {
        OResult row = rs.next();
        System.out.println(row);
      }
      rs.close();
      System.out.println("----------------------\n\n");

      db.commit();

      System.out.println("----------------------");
      System.out.println("After committing, the query1 results:");

      rs = db.query(query1, teacherName);
      while (rs.hasNext()) {
        OResult row = rs.next();
        System.out.println(row);
      }
      rs.close();
      System.out.println("----------------------\n\n");

      System.out.println("----------------------");
      System.out.println("After committing, the query2 results:");
      rs = db.query(query2, teacherName);
      while (rs.hasNext()) {
        OResult row = rs.next();
        System.out.println(row);
      }
      rs.close();
      System.out.println("----------------------\n\n");

    }
  }

  @After
  public void releaseResource() {
    if (orientDB != null) {
      orientDB.close();
    }
  }
}