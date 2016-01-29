package com.orientechnologies.orient.core.metadata.schema;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertEquals;

public class CreateClassMultipleClusterTest {

  private ODatabaseDocument db;

  @BeforeMethod
  public void before() {
    db = new ODatabaseDocumentTx("memory:" + CreateClassMultipleClusterTest.class.getSimpleName());
    db.create();
  }

  @AfterMethod
  public void after() {
    db.drop();
  }

  @Test
  public void testCreateClassSQL() {
    db.command(new OCommandSQL("drop class V")).execute();
    db.command(new OCommandSQL("create class V clusters 16")).execute();
    db.command(new OCommandSQL("create class X extends V clusters 32")).execute();

    final OClass clazzV = db.getMetadata().getSchema().getClass("V");
    assertEquals(16, clazzV.getClusterIds().length);

    final OClass clazzX = db.getMetadata().getSchema().getClass("X");
    assertEquals(32, clazzX.getClusterIds().length);

  }

  @Test
  public void testCreateClassSQLSpecifiedClusters() {
    int s = db.addCluster("second");
    int t = db.addCluster("third");
    db.command(new OCommandSQL("drop class V")).execute();
    db.command(new OCommandSQL("create class V cluster " + s + "," + t)).execute();

    final OClass clazzV = db.getMetadata().getSchema().getClass("V");
    assertEquals(2, clazzV.getClusterIds().length);

    assertEquals(s, clazzV.getClusterIds()[0]);
    assertEquals(t, clazzV.getClusterIds()[1]);
  }
}
