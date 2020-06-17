package com.orientechnologies.orient.core.sql.executor;

import static com.orientechnologies.orient.core.sql.executor.ExecutionPlanPrintUtils.printExecutionPlan;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/** @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com) */
public class OAlterPropertyStatementExecutionTest {
  static ODatabaseDocument db;

  @BeforeClass
  public static void beforeClass() {
    db = new ODatabaseDocumentTx("memory:OAlterPropertyStatementExecutionTest");
    db.create();
  }

  @AfterClass
  public static void afterClass() {
    db.close();
  }

  @Test
  public void testSetProperty() {
    String className = "testSetProperty";
    OClass clazz = db.getMetadata().getSchema().createClass(className);
    OProperty prop = clazz.createProperty("name", OType.STRING);
    prop.setMax("15");

    OResultSet result = db.command("alter property " + className + ".name max 30");
    printExecutionPlan(null, result);
    Object currentValue = prop.getMax();

    Assert.assertNotNull(result);
    Assert.assertTrue(result.hasNext());
    OResult next = result.next();
    Assert.assertNotNull(next);
    Assert.assertEquals("15", next.getProperty("oldValue"));
    Assert.assertEquals("30", currentValue);
    Assert.assertEquals(currentValue, next.getProperty("newValue"));
    result.close();
  }

  @Test
  public void testSetCustom() {
    String className = "testSetCustom";
    OClass clazz = db.getMetadata().getSchema().createClass(className);
    OProperty prop = clazz.createProperty("name", OType.STRING);
    prop.setCustom("foo", "bar");

    OResultSet result = db.command("alter property " + className + ".name custom foo='baz'");
    printExecutionPlan(null, result);
    Object currentValue = prop.getCustom("foo");

    Assert.assertNotNull(result);
    Assert.assertTrue(result.hasNext());
    OResult next = result.next();
    Assert.assertNotNull(next);
    Assert.assertEquals("bar", next.getProperty("oldValue"));
    Assert.assertEquals("baz", currentValue);
    Assert.assertEquals(currentValue, next.getProperty("newValue"));
    result.close();
  }
}
