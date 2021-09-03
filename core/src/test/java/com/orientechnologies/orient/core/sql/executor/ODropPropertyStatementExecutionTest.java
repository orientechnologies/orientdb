package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/** @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com) */
public class ODropPropertyStatementExecutionTest {
  static ODatabaseDocument db;

  @BeforeClass
  public static void beforeClass() {
    db = new ODatabaseDocumentTx("memory:ODropPropertyStatementExecutionTest");
    db.create();
  }

  @AfterClass
  public static void afterClass() {
    db.close();
  }

  @Test
  public void testPlain() {
    String className = "testPlain";
    String propertyName = "foo";
    OSchema schema = db.getMetadata().getSchema();
    schema.createClass(className).createProperty(propertyName, OType.STRING);

    schema.reload();
    Assert.assertNotNull(schema.getClass(className).getProperty(propertyName));
    OResultSet result = db.command("drop property " + className + "." + propertyName);
    Assert.assertTrue(result.hasNext());
    OResult next = result.next();
    Assert.assertEquals("drop property", next.getProperty("operation"));
    Assert.assertFalse(result.hasNext());
    result.close();

    schema.reload();
    Assert.assertNull(schema.getClass(className).getProperty(propertyName));
  }

  @Test
  public void testDropIndexForce() {
    String className = "testDropIndexForce";
    String propertyName = "foo";
    OSchema schema = db.getMetadata().getSchema();
    schema
        .createClass(className)
        .createProperty(propertyName, OType.STRING)
        .createIndex(OClass.INDEX_TYPE.NOTUNIQUE);

    schema.reload();
    Assert.assertNotNull(schema.getClass(className).getProperty(propertyName));
    OResultSet result = db.command("drop property " + className + "." + propertyName + " force");
    for (int i = 0; i < 2; i++) {
      Assert.assertTrue(result.hasNext());
      result.next();
    }

    Assert.assertFalse(result.hasNext());

    result.close();

    schema.reload();
    Assert.assertNull(schema.getClass(className).getProperty(propertyName));
  }

  @Test
  public void testDropIndex() {

    String className = "testDropIndex";
    String propertyName = "foo";
    OSchema schema = db.getMetadata().getSchema();
    schema
        .createClass(className)
        .createProperty(propertyName, OType.STRING)
        .createIndex(OClass.INDEX_TYPE.NOTUNIQUE);

    schema.reload();
    Assert.assertNotNull(schema.getClass(className).getProperty(propertyName));
    try {
      db.command("drop property " + className + "." + propertyName);
      Assert.fail();
    } catch (OCommandExecutionException e) {
    } catch (Exception e) {
      Assert.fail();
    }
  }
}
