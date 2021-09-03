package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/** @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com) */
public class OCreateIndexStatementExecutionTest {
  static ODatabaseDocumentInternal db;

  @BeforeClass
  public static void beforeClass() {
    db = new ODatabaseDocumentTx("memory:OCreateIndexStatementExecutionTest");
    db.create();
  }

  @AfterClass
  public static void afterClass() {
    db.close();
  }

  @Test
  public void testPlain() {
    String className = "testPlain";
    OClass clazz = db.getMetadata().getSchema().createClass(className);
    clazz.createProperty("name", OType.STRING);

    Assert.assertNull(db.getMetadata().getIndexManagerInternal().getIndex(db, className + ".name"));
    OResultSet result =
        db.command("create index " + className + ".name on " + className + " (name) notunique");
    Assert.assertTrue(result.hasNext());
    OResult next = result.next();
    Assert.assertFalse(result.hasNext());
    Assert.assertNotNull(next);
    result.close();
    OIndex idx = db.getMetadata().getIndexManagerInternal().getIndex(db, className + ".name");
    Assert.assertNotNull(idx);
    Assert.assertFalse(idx.isUnique());
  }

  @Test
  public void testIfNotExists() {
    String className = "testIfNotExists";
    OClass clazz = db.getMetadata().getSchema().createClass(className);
    clazz.createProperty("name", OType.STRING);

    Assert.assertNull(db.getMetadata().getIndexManagerInternal().getIndex(db, className + ".name"));
    OResultSet result =
        db.command(
            "create index "
                + className
                + ".name IF NOT EXISTS on "
                + className
                + " (name) notunique");
    Assert.assertTrue(result.hasNext());
    OResult next = result.next();
    Assert.assertFalse(result.hasNext());
    Assert.assertNotNull(next);
    result.close();
    OIndex idx = db.getMetadata().getIndexManagerInternal().getIndex(db, className + ".name");
    Assert.assertNotNull(idx);
    Assert.assertFalse(idx.isUnique());

    result =
        db.command(
            "create index "
                + className
                + ".name IF NOT EXISTS on "
                + className
                + " (name) notunique");
    Assert.assertFalse(result.hasNext());
    result.close();
  }
}
