package com.orientechnologies.orient.core.sql.executor;

import static com.orientechnologies.orient.core.sql.executor.ExecutionPlanPrintUtils.printExecutionPlan;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.viewmanager.ViewCreationListener;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.metadata.schema.OViewConfig;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OSQLEngine;
import com.orientechnologies.orient.core.sql.functions.OSQLFunction;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/** @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com) */
public class OSelectStatementExecutionTest {
  static ODatabaseDocument db;

  @BeforeClass
  public static void beforeClass() {
    db = new ODatabaseDocumentTx("memory:OSelectStatementExecutionTest");
    db.create();
  }

  @AfterClass
  public static void afterClass() {
    db.close();
  }

  @Test
  public void testSelectNoTarget() {
    OResultSet result = db.query("select 1 as one, 2 as two, 2+3");
    Assert.assertTrue(result.hasNext());
    OResult item = result.next();
    Assert.assertNotNull(item);
    Assert.assertEquals(1, item.<Object>getProperty("one"));
    Assert.assertEquals(2, item.<Object>getProperty("two"));
    Assert.assertEquals(5, item.<Object>getProperty("2 + 3"));
    printExecutionPlan(result);

    result.close();
  }

  @Test
  public void testGroupByCount() {
    db.getMetadata().getSchema().createClass("InputTx");

    for (int i = 0; i < 100; i++) {
      final String hash = UUID.randomUUID().toString();
      db.command("insert into InputTx set address = '" + hash + "'");

      // CREATE RANDOM NUMBER OF COPIES final int random = new Random().nextInt(10);
      final int random = new Random().nextInt(10);
      for (int j = 0; j < random; j++) {
        db.command("insert into InputTx set address = '" + hash + "'");
      }
    }

    final OResultSet result =
        db.query(
            "select address, count(*) as occurrencies from InputTx where address is not null group by address limit 10");
    while (result.hasNext()) {
      final OResult row = result.next();
      Assert.assertNotNull(row.getProperty("address")); // <== FALSE!
      Assert.assertNotNull(row.getProperty("occurrencies"));
    }
    result.close();
  }

  @Test
  public void testSelectNoTargetSkip() {
    OResultSet result = db.query("select 1 as one, 2 as two, 2+3 skip 1");
    Assert.assertFalse(result.hasNext());
    printExecutionPlan(result);

    result.close();
  }

  @Test
  public void testSelectNoTargetSkipZero() {
    OResultSet result = db.query("select 1 as one, 2 as two, 2+3 skip 0");
    Assert.assertTrue(result.hasNext());
    OResult item = result.next();
    Assert.assertNotNull(item);
    Assert.assertEquals(1, item.<Object>getProperty("one"));
    Assert.assertEquals(2, item.<Object>getProperty("two"));
    Assert.assertEquals(5, item.<Object>getProperty("2 + 3"));
    printExecutionPlan(result);

    result.close();
  }

  @Test
  public void testSelectNoTargetLimit0() {
    OResultSet result = db.query("select 1 as one, 2 as two, 2+3 limit 0");
    Assert.assertFalse(result.hasNext());
    printExecutionPlan(result);

    result.close();
  }

  @Test
  public void testSelectNoTargetLimit1() {
    OResultSet result = db.query("select 1 as one, 2 as two, 2+3 limit 1");
    Assert.assertTrue(result.hasNext());
    OResult item = result.next();
    Assert.assertNotNull(item);
    Assert.assertEquals(1, item.<Object>getProperty("one"));
    Assert.assertEquals(2, item.<Object>getProperty("two"));
    Assert.assertEquals(5, item.<Object>getProperty("2 + 3"));
    printExecutionPlan(result);

    result.close();
  }

  @Test
  public void testSelectNoTargetLimitx() {
    OResultSet result = db.query("select 1 as one, 2 as two, 2+3 skip 0 limit 0");
    printExecutionPlan(result);
    result.close();
  }

  @Test
  public void testSelectFullScan1() {
    String className = "TestSelectFullScan1";
    db.getMetadata().getSchema().createClass(className);
    for (int i = 0; i < 100000; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("name", "name" + i);
      doc.setProperty("surname", "surname" + i);
      doc.save();
    }
    OResultSet result = db.query("select from " + className);
    for (int i = 0; i < 100000; i++) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Assert.assertNotNull(item);
      Assert.assertTrue(("" + item.getProperty("name")).startsWith("name"));
    }
    Assert.assertFalse(result.hasNext());
    printExecutionPlan(result);
    result.close();
  }

  @Test
  public void testSelectFullScanOrderByRidAsc() {
    String className = "testSelectFullScanOrderByRidAsc";
    db.getMetadata().getSchema().createClass(className);
    for (int i = 0; i < 100000; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("name", "name" + i);
      doc.setProperty("surname", "surname" + i);
      doc.save();
    }
    OResultSet result = db.query("select from " + className + " ORDER BY @rid ASC");
    printExecutionPlan(result);
    OIdentifiable lastItem = null;
    for (int i = 0; i < 100000; i++) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Assert.assertNotNull(item);
      Assert.assertTrue(("" + item.getProperty("name")).startsWith("name"));
      if (lastItem != null) {
        Assert.assertTrue(
            lastItem.getIdentity().compareTo(item.getElement().get().getIdentity()) < 0);
      }
      lastItem = item.getElement().get();
    }
    Assert.assertFalse(result.hasNext());

    result.close();
  }

  @Test
  public void testSelectFullScanOrderByRidDesc() {
    String className = "testSelectFullScanOrderByRidDesc";
    db.getMetadata().getSchema().createClass(className);
    for (int i = 0; i < 100000; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("name", "name" + i);
      doc.setProperty("surname", "surname" + i);
      doc.save();
    }
    OResultSet result = db.query("select from " + className + " ORDER BY @rid DESC");
    printExecutionPlan(result);
    OIdentifiable lastItem = null;
    for (int i = 0; i < 100000; i++) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Assert.assertNotNull(item);
      Assert.assertTrue(("" + item.getProperty("name")).startsWith("name"));
      if (lastItem != null) {
        Assert.assertTrue(
            lastItem.getIdentity().compareTo(item.getElement().get().getIdentity()) > 0);
      }
      lastItem = item.getElement().get();
    }
    Assert.assertFalse(result.hasNext());

    result.close();
  }

  @Test
  public void testSelectFullScanLimit1() {
    String className = "testSelectFullScanLimit1";
    db.getMetadata().getSchema().createClass(className);
    for (int i = 0; i < 300; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("name", "name" + i);
      doc.setProperty("surname", "surname" + i);
      doc.save();
    }
    OResultSet result = db.query("select from " + className + " limit 10");
    printExecutionPlan(result);

    for (int i = 0; i < 10; i++) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Assert.assertNotNull(item);
      Assert.assertTrue(("" + item.getProperty("name")).startsWith("name"));
    }
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testSelectFullScanSkipLimit1() {
    String className = "testSelectFullScanSkipLimit1";
    db.getMetadata().getSchema().createClass(className);
    for (int i = 0; i < 300; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("name", "name" + i);
      doc.setProperty("surname", "surname" + i);
      doc.save();
    }
    OResultSet result = db.query("select from " + className + " skip 100 limit 10");
    printExecutionPlan(result);

    for (int i = 0; i < 10; i++) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Assert.assertNotNull(item);
      Assert.assertTrue(("" + item.getProperty("name")).startsWith("name"));
    }
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testSelectOrderByDesc() {
    String className = "testSelectOrderByDesc";
    db.getMetadata().getSchema().createClass(className);
    for (int i = 0; i < 30; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("name", "name" + i);
      doc.setProperty("surname", "surname" + i);
      doc.save();
    }
    OResultSet result = db.query("select from " + className + " order by surname desc");
    printExecutionPlan(result);

    String lastSurname = null;
    for (int i = 0; i < 30; i++) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Assert.assertNotNull(item);
      String thisSurname = item.getProperty("surname");
      if (lastSurname != null) {
        Assert.assertTrue(lastSurname.compareTo(thisSurname) >= 0);
      }
      lastSurname = thisSurname;
    }
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testSelectOrderByAsc() {
    String className = "testSelectOrderByAsc";
    db.getMetadata().getSchema().createClass(className);
    for (int i = 0; i < 30; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("name", "name" + i);
      doc.setProperty("surname", "surname" + i);
      doc.save();
    }
    OResultSet result = db.query("select from " + className + " order by surname asc");
    printExecutionPlan(result);

    String lastSurname = null;
    for (int i = 0; i < 30; i++) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Assert.assertNotNull(item);
      String thisSurname = item.getProperty("surname");
      if (lastSurname != null) {
        Assert.assertTrue(lastSurname.compareTo(thisSurname) <= 0);
      }
      lastSurname = thisSurname;
    }
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testSelectOrderByMassiveAsc() {
    String className = "testSelectOrderByMassiveAsc";
    db.getMetadata().getSchema().createClass(className);
    for (int i = 0; i < 100000; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("name", "name" + i);
      doc.setProperty("surname", "surname" + i % 100);
      doc.save();
    }
    long begin = System.nanoTime();
    OResultSet result = db.query("select from " + className + " order by surname asc limit 100");
    printExecutionPlan(result);

    for (int i = 0; i < 100; i++) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Assert.assertNotNull(item);
      Assert.assertEquals("surname0", item.getProperty("surname"));
    }
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testSelectOrderWithProjections() {
    String className = "testSelectOrderWithProjections";
    db.getMetadata().getSchema().createClass(className);
    for (int i = 0; i < 100; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("name", "name" + i % 10);
      doc.setProperty("surname", "surname" + i % 10);
      doc.save();
    }
    long begin = System.nanoTime();
    OResultSet result = db.query("select name from " + className + " order by surname asc");
    printExecutionPlan(result);

    String lastName = null;
    for (int i = 0; i < 100; i++) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Assert.assertNotNull(item);
      String name = item.getProperty("name");
      Assert.assertNotNull(name);
      if (i > 0) {
        Assert.assertTrue(name.compareTo(lastName) >= 0);
      }
      lastName = name;
    }
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testSelectOrderWithProjections2() {
    String className = "testSelectOrderWithProjections2";
    db.getMetadata().getSchema().createClass(className);
    for (int i = 0; i < 100; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("name", "name" + i % 10);
      doc.setProperty("surname", "surname" + i % 10);
      doc.save();
    }
    long begin = System.nanoTime();
    OResultSet result =
        db.query("select name from " + className + " order by name asc, surname asc");
    printExecutionPlan(result);

    String lastName = null;
    for (int i = 0; i < 100; i++) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Assert.assertNotNull(item);
      String name = item.getProperty("name");
      Assert.assertNotNull(name);
      if (i > 0) {
        Assert.assertTrue(name.compareTo(lastName) >= 0);
      }
      lastName = name;
    }
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testSelectFullScanWithFilter1() {
    String className = "testSelectFullScanWithFilter1";
    db.getMetadata().getSchema().createClass(className);
    for (int i = 0; i < 300; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("name", "name" + i);
      doc.setProperty("surname", "surname" + i);
      doc.save();
    }
    OResultSet result =
        db.query("select from " + className + " where name = 'name1' or name = 'name7' ");
    printExecutionPlan(result);

    for (int i = 0; i < 2; i++) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Assert.assertNotNull(item);
      Object name = item.getProperty("name");
      Assert.assertTrue("name1".equals(name) || "name7".equals(name));
    }
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testSelectFullScanWithFilter2() {
    String className = "testSelectFullScanWithFilter2";
    db.getMetadata().getSchema().createClass(className);
    for (int i = 0; i < 300; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("name", "name" + i);
      doc.setProperty("surname", "surname" + i);
      doc.save();
    }
    OResultSet result = db.query("select from " + className + " where name <> 'name1' ");
    printExecutionPlan(result);

    for (int i = 0; i < 299; i++) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Assert.assertNotNull(item);
      Object name = item.getProperty("name");
      Assert.assertFalse("name1".equals(name));
    }
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testProjections() {
    String className = "testProjections";
    db.getMetadata().getSchema().createClass(className);
    for (int i = 0; i < 300; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("name", "name" + i);
      doc.setProperty("surname", "surname" + i);
      doc.save();
    }
    OResultSet result = db.query("select name from " + className);
    printExecutionPlan(result);

    for (int i = 0; i < 300; i++) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Assert.assertNotNull(item);
      String name = item.getProperty("name");
      String surname = item.getProperty("surname");
      Assert.assertNotNull(name);
      Assert.assertTrue(name.startsWith("name"));
      Assert.assertNull(surname);
      Assert.assertFalse(item.getElement().isPresent());
    }
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testCountStar() {
    String className = "testCountStar";
    db.getMetadata().getSchema().createClass(className);

    for (int i = 0; i < 7; i++) {
      ODocument doc = new ODocument(className);
      doc.save();
    }
    try {
      OResultSet result = db.query("select count(*) from " + className);
      printExecutionPlan(result);
      Assert.assertNotNull(result);
      Assert.assertTrue(result.hasNext());
      OResult next = result.next();
      Assert.assertNotNull(next);
      Assert.assertEquals(7L, (Object) next.getProperty("count(*)"));
      Assert.assertFalse(result.hasNext());
      result.close();
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    }
  }

  @Test
  public void testCountStar2() {
    String className = "testCountStar2";
    db.getMetadata().getSchema().createClass(className);

    for (int i = 0; i < 10; i++) {
      ODocument doc = new ODocument(className);
      doc.setProperty("name", "name" + (i % 5));
      doc.save();
    }
    try {
      OResultSet result = db.query("select count(*), name from " + className + " group by name");
      printExecutionPlan(result);
      Assert.assertNotNull(result);
      for (int i = 0; i < 5; i++) {
        Assert.assertTrue(result.hasNext());
        OResult next = result.next();
        Assert.assertNotNull(next);
        Assert.assertEquals(2L, (Object) next.getProperty("count(*)"));
      }
      Assert.assertFalse(result.hasNext());
      result.close();
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    }
  }

  @Test
  public void testCountStarEmptyNoIndex() {
    String className = "testCountStarEmptyNoIndex";
    db.getMetadata().getSchema().createClass(className);

    OElement elem = db.newElement(className);
    elem.setProperty("name", "bar");
    elem.save();

    try {
      OResultSet result = db.query("select count(*) from " + className + " where name = 'foo'");
      printExecutionPlan(result);
      Assert.assertNotNull(result);
      Assert.assertTrue(result.hasNext());
      OResult next = result.next();
      Assert.assertNotNull(next);
      Assert.assertEquals(0L, (Object) next.getProperty("count(*)"));
      Assert.assertFalse(result.hasNext());
      result.close();
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    }
  }

  @Test
  public void testCountStarEmptyNoIndexWithAlias() {
    String className = "testCountStarEmptyNoIndexWithAlias";
    db.getMetadata().getSchema().createClass(className);

    OElement elem = db.newElement(className);
    elem.setProperty("name", "bar");
    elem.save();

    try {
      OResultSet result =
          db.query("select count(*) as a from " + className + " where name = 'foo'");
      printExecutionPlan(result);
      Assert.assertNotNull(result);
      Assert.assertTrue(result.hasNext());
      OResult next = result.next();
      Assert.assertNotNull(next);
      Assert.assertEquals(0L, (Object) next.getProperty("a"));
      Assert.assertFalse(result.hasNext());
      result.close();
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    }
  }

  @Test
  public void testAggretateMixedWithNonAggregate() {
    String className = "testAggretateMixedWithNonAggregate";
    db.getMetadata().getSchema().createClass(className);

    try {
      db.query(
              "select max(a) + max(b) + pippo + pluto as foo, max(d) + max(e), f from " + className)
          .close();
      Assert.fail();
    } catch (OCommandExecutionException x) {

    } catch (Exception e) {
      Assert.fail();
    }
  }

  @Test
  public void testAggretateMixedWithNonAggregateInCollection() {
    String className = "testAggretateMixedWithNonAggregateInCollection";
    db.getMetadata().getSchema().createClass(className);

    try {
      db.query("select [max(a), max(b), foo] from " + className).close();
      Assert.fail();
    } catch (OCommandExecutionException x) {

    } catch (Exception e) {
      Assert.fail();
    }
  }

  @Test
  public void testAggretateInCollection() {
    String className = "testAggretateInCollection";
    db.getMetadata().getSchema().createClass(className);

    try {
      String query = "select [max(a), max(b)] from " + className;
      OResultSet result = db.query(query);
      printExecutionPlan(query, result);
      result.close();
    } catch (Exception x) {
      Assert.fail();
    }
  }

  @Test
  public void testAggretateMixedWithNonAggregateConstants() {
    String className = "testAggretateMixedWithNonAggregateConstants";
    db.getMetadata().getSchema().createClass(className);

    try {
      OResultSet result =
          db.query(
              "select max(a + b) + (max(b + c * 2) + 1 + 2) * 3 as foo, max(d) + max(e), f from "
                  + className);
      printExecutionPlan(result);
      result.close();
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    }
  }

  @Test
  public void testAggregateSum() {
    String className = "testAggregateSum";
    db.getMetadata().getSchema().createClass(className);
    for (int i = 0; i < 10; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("name", "name" + i);
      doc.setProperty("val", i);
      doc.save();
    }
    OResultSet result = db.query("select sum(val) from " + className);
    printExecutionPlan(result);
    Assert.assertTrue(result.hasNext());
    OResult item = result.next();
    Assert.assertNotNull(item);
    Assert.assertEquals(45, (Object) item.getProperty("sum(val)"));

    result.close();
  }

  @Test
  public void testAggregateSumGroupBy() {
    String className = "testAggregateSumGroupBy";
    db.getMetadata().getSchema().createClass(className);
    for (int i = 0; i < 10; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("type", i % 2 == 0 ? "even" : "odd");
      doc.setProperty("val", i);
      doc.save();
    }
    OResultSet result = db.query("select sum(val), type from " + className + " group by type");
    printExecutionPlan(result);
    boolean evenFound = false;
    boolean oddFound = false;
    for (int i = 0; i < 2; i++) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Assert.assertNotNull(item);
      if ("even".equals(item.getProperty("type"))) {
        Assert.assertEquals(20, item.<Object>getProperty("sum(val)"));
        evenFound = true;
      } else if ("odd".equals(item.getProperty("type"))) {
        Assert.assertEquals(25, item.<Object>getProperty("sum(val)"));
        oddFound = true;
      }
    }
    Assert.assertFalse(result.hasNext());
    Assert.assertTrue(evenFound);
    Assert.assertTrue(oddFound);
    result.close();
  }

  @Test
  public void testAggregateSumMaxMinGroupBy() {
    String className = "testAggregateSumMaxMinGroupBy";
    db.getMetadata().getSchema().createClass(className);
    for (int i = 0; i < 10; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("type", i % 2 == 0 ? "even" : "odd");
      doc.setProperty("val", i);
      doc.save();
    }
    OResultSet result =
        db.query("select sum(val), max(val), min(val), type from " + className + " group by type");
    printExecutionPlan(result);
    boolean evenFound = false;
    boolean oddFound = false;
    for (int i = 0; i < 2; i++) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Assert.assertNotNull(item);
      if ("even".equals(item.getProperty("type"))) {
        Assert.assertEquals(20, item.<Object>getProperty("sum(val)"));
        Assert.assertEquals(8, item.<Object>getProperty("max(val)"));
        Assert.assertEquals(0, item.<Object>getProperty("min(val)"));
        evenFound = true;
      } else if ("odd".equals(item.getProperty("type"))) {
        Assert.assertEquals(25, item.<Object>getProperty("sum(val)"));
        Assert.assertEquals(9, item.<Object>getProperty("max(val)"));
        Assert.assertEquals(1, item.<Object>getProperty("min(val)"));
        oddFound = true;
      }
    }
    Assert.assertFalse(result.hasNext());
    Assert.assertTrue(evenFound);
    Assert.assertTrue(oddFound);
    result.close();
  }

  @Test
  public void testAggregateSumNoGroupByInProjection() {
    String className = "testAggregateSumNoGroupByInProjection";
    db.getMetadata().getSchema().createClass(className);
    for (int i = 0; i < 10; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("type", i % 2 == 0 ? "even" : "odd");
      doc.setProperty("val", i);
      doc.save();
    }
    OResultSet result = db.query("select sum(val) from " + className + " group by type");
    printExecutionPlan(result);
    boolean evenFound = false;
    boolean oddFound = false;
    for (int i = 0; i < 2; i++) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Assert.assertNotNull(item);
      Object sum = item.getProperty("sum(val)");
      if (sum.equals(20)) {
        evenFound = true;
      } else if (sum.equals(25)) {
        oddFound = true;
      }
    }
    Assert.assertFalse(result.hasNext());
    Assert.assertTrue(evenFound);
    Assert.assertTrue(oddFound);
    result.close();
  }

  @Test
  public void testAggregateSumNoGroupByInProjection2() {
    String className = "testAggregateSumNoGroupByInProjection2";
    db.getMetadata().getSchema().createClass(className);
    for (int i = 0; i < 10; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("type", i % 2 == 0 ? "dd1" : "dd2");
      doc.setProperty("val", i);
      doc.save();
    }
    OResultSet result =
        db.query("select sum(val) from " + className + " group by type.substring(0,1)");
    printExecutionPlan(result);
    for (int i = 0; i < 1; i++) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Assert.assertNotNull(item);
      Object sum = item.getProperty("sum(val)");
      Assert.assertEquals(45, sum);
    }
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testFetchFromClusterNumber() {
    String className = "testFetchFromClusterNumber";
    OSchema schema = db.getMetadata().getSchema();
    OClass clazz = schema.createClass(className);
    int targetCluster = clazz.getClusterIds()[0];
    String targetClusterName = db.getClusterNameById(targetCluster);

    for (int i = 0; i < 10; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("val", i);
      doc.save(targetClusterName);
    }
    OResultSet result = db.query("select from cluster:" + targetClusterName);
    printExecutionPlan(result);
    int sum = 0;
    for (int i = 0; i < 10; i++) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Integer val = item.getProperty("val");
      Assert.assertNotNull(val);
      sum += val;
    }
    Assert.assertEquals(45, sum);
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testFetchFromClusterNumberOrderByRidDesc() {
    String className = "testFetchFromClusterNumberOrderByRidDesc";
    OSchema schema = db.getMetadata().getSchema();
    OClass clazz = schema.createClass(className);
    int targetCluster = clazz.getClusterIds()[0];
    String targetClusterName = db.getClusterNameById(targetCluster);

    for (int i = 0; i < 10; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("val", i);
      doc.save(targetClusterName);
    }
    OResultSet result =
        db.query("select from cluster:" + targetClusterName + " order by @rid desc");
    printExecutionPlan(result);
    int sum = 0;
    for (int i = 0; i < 10; i++) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Integer val = item.getProperty("val");
      Assert.assertEquals(i, 9 - val);
    }

    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testFetchFromClusterNumberOrderByRidAsc() {
    String className = "testFetchFromClusterNumberOrderByRidAsc";
    OSchema schema = db.getMetadata().getSchema();
    OClass clazz = schema.createClass(className);
    int targetCluster = clazz.getClusterIds()[0];
    String targetClusterName = db.getClusterNameById(targetCluster);

    for (int i = 0; i < 10; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("val", i);
      doc.save(targetClusterName);
    }
    OResultSet result = db.query("select from cluster:" + targetClusterName + " order by @rid asc");
    printExecutionPlan(result);
    int sum = 0;
    for (int i = 0; i < 10; i++) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Integer val = item.getProperty("val");
      Assert.assertEquals((Object) i, val);
    }

    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testFetchFromClustersNumberOrderByRidAsc() {
    String className = "testFetchFromClustersNumberOrderByRidAsc";
    OSchema schema = db.getMetadata().getSchema();
    OClass clazz = schema.createClass(className);
    if (clazz.getClusterIds().length < 2) {
      clazz.addCluster("testFetchFromClustersNumberOrderByRidAsc_2");
    }
    int targetCluster = clazz.getClusterIds()[0];
    String targetClusterName = db.getClusterNameById(targetCluster);

    int targetCluster2 = clazz.getClusterIds()[1];
    String targetClusterName2 = db.getClusterNameById(targetCluster2);

    for (int i = 0; i < 10; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("val", i);
      doc.save(targetClusterName);
    }
    for (int i = 0; i < 10; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("val", i);
      doc.save(targetClusterName2);
    }

    OResultSet result =
        db.query(
            "select from cluster:["
                + targetClusterName
                + ", "
                + targetClusterName2
                + "] order by @rid asc");
    printExecutionPlan(result);

    for (int i = 0; i < 20; i++) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Integer val = item.getProperty("val");
      Assert.assertEquals((Object) (i % 10), val);
    }

    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testQueryAsTarget() {
    String className = "testQueryAsTarget";
    OSchema schema = db.getMetadata().getSchema();
    OClass clazz = schema.createClass(className);

    for (int i = 0; i < 10; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("val", i);
      doc.save();
    }

    OResultSet result =
        db.query("select from (select from " + className + " where val > 2)  where val < 8");
    printExecutionPlan(result);

    for (int i = 0; i < 5; i++) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Integer val = item.getProperty("val");
      Assert.assertTrue(val > 2);
      Assert.assertTrue(val < 8);
    }
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testQuerySchema() {
    OResultSet result = db.query("select from metadata:schema");
    printExecutionPlan(result);

    for (int i = 0; i < 1; i++) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Assert.assertNotNull(item.getProperty("classes"));
    }
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testQueryMetadataIndexManager() {
    OResultSet result = db.query("select from metadata:indexmanager");
    printExecutionPlan(result);
    for (int i = 0; i < 1; i++) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Assert.assertNotNull(item.getProperty("indexes"));
    }
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testQueryMetadataIndexManager2() {
    OResultSet result = db.query("select expand(indexes) from metadata:indexmanager");
    printExecutionPlan(result);
    Assert.assertTrue(result.hasNext());
    result.close();
  }

  @Test
  public void testQueryMetadataDatabase() {
    OResultSet result = db.query("select from metadata:database");
    printExecutionPlan(result);

    Assert.assertTrue(result.hasNext());
    OResult item = result.next();
    Assert.assertEquals(
        OSelectStatementExecutionTest.class.getSimpleName(), item.getProperty("name"));
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testQueryMetadataStorage() {
    OResultSet result = db.query("select from metadata:storage");
    printExecutionPlan(result);

    Assert.assertTrue(result.hasNext());
    OResult item = result.next();
    Assert.assertEquals(
        OSelectStatementExecutionTest.class.getSimpleName(), item.getProperty("name"));
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testNonExistingRids() {
    OResultSet result = db.query("select from #0:100000000");
    printExecutionPlan(result);
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testFetchFromSingleRid() {
    OResultSet result = db.query("select from #0:1");
    printExecutionPlan(result);
    Assert.assertTrue(result.hasNext());
    Assert.assertNotNull(result.next());
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testFetchFromSingleRid2() {
    OResultSet result = db.query("select from [#0:1]");
    printExecutionPlan(result);
    Assert.assertTrue(result.hasNext());
    Assert.assertNotNull(result.next());
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testFetchFromSingleRidParam() {
    OResultSet result = db.query("select from ?", new ORecordId(0, 1));
    printExecutionPlan(result);
    Assert.assertTrue(result.hasNext());
    Assert.assertNotNull(result.next());
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testFetchFromSingleRid3() {
    ODocument document = new ODocument();
    document.save(db.getClusterNameById(0));

    OResultSet result = db.query("select from [#0:1, #0:2]");
    printExecutionPlan(result);
    Assert.assertTrue(result.hasNext());
    Assert.assertNotNull(result.next());
    Assert.assertTrue(result.hasNext());
    Assert.assertNotNull(result.next());
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testFetchFromSingleRid4() {
    ODocument document = new ODocument();
    document.save(db.getClusterNameById(0));

    OResultSet result = db.query("select from [#0:1, #0:2, #0:100000]");
    printExecutionPlan(result);
    Assert.assertTrue(result.hasNext());
    Assert.assertNotNull(result.next());
    Assert.assertTrue(result.hasNext());
    Assert.assertNotNull(result.next());
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testFetchFromClassWithIndex() {
    String className = "testFetchFromClassWithIndex";
    OClass clazz = db.getMetadata().getSchema().createClass(className);
    clazz.createProperty("name", OType.STRING);
    clazz.createIndex(className + ".name", OClass.INDEX_TYPE.NOTUNIQUE, "name");

    for (int i = 0; i < 10; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("name", "name" + i);
      doc.save();
    }

    OResultSet result = db.query("select from " + className + " where name = 'name2'");
    printExecutionPlan(result);

    Assert.assertTrue(result.hasNext());
    OResult next = result.next();
    Assert.assertNotNull(next);
    Assert.assertEquals("name2", next.getProperty("name"));

    Assert.assertFalse(result.hasNext());

    Optional<OExecutionPlan> p = result.getExecutionPlan();
    Assert.assertTrue(p.isPresent());
    OExecutionPlan p2 = p.get();
    Assert.assertTrue(p2 instanceof OSelectExecutionPlan);
    OSelectExecutionPlan plan = (OSelectExecutionPlan) p2;
    Assert.assertEquals(FetchFromIndexStep.class, plan.getSteps().get(0).getClass());
    result.close();
  }

  @Test
  public void testFetchFromIndex() {
    boolean oldAllowManual = OGlobalConfiguration.INDEX_ALLOW_MANUAL_INDEXES.getValueAsBoolean();
    OGlobalConfiguration.INDEX_ALLOW_MANUAL_INDEXES.setValue(true);
    String className = "testFetchFromIndex";
    OClass clazz = db.getMetadata().getSchema().createClass(className);
    clazz.createProperty("name", OType.STRING);
    String indexName = className + ".name";
    clazz.createIndex(indexName, OClass.INDEX_TYPE.NOTUNIQUE, "name");

    for (int i = 0; i < 10; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("name", "name" + i);
      doc.save();
    }

    OResultSet result = db.query("select from index:" + indexName + " where key = 'name2'");
    printExecutionPlan(result);

    Assert.assertTrue(result.hasNext());
    OResult next = result.next();
    Assert.assertNotNull(next);

    Assert.assertFalse(result.hasNext());

    Optional<OExecutionPlan> p = result.getExecutionPlan();
    Assert.assertTrue(p.isPresent());
    OExecutionPlan p2 = p.get();
    Assert.assertTrue(p2 instanceof OSelectExecutionPlan);
    OSelectExecutionPlan plan = (OSelectExecutionPlan) p2;
    Assert.assertEquals(FetchFromIndexStep.class, plan.getSteps().get(0).getClass());
    result.close();
    OGlobalConfiguration.INDEX_ALLOW_MANUAL_INDEXES.setValue(oldAllowManual);
  }

  @Test
  public void testFetchFromClassWithIndexes() {
    String className = "testFetchFromClassWithIndexes";
    OClass clazz = db.getMetadata().getSchema().createClass(className);
    clazz.createProperty("name", OType.STRING);
    clazz.createProperty("surname", OType.STRING);
    clazz.createIndex(className + ".name", OClass.INDEX_TYPE.NOTUNIQUE, "name");
    clazz.createIndex(className + ".surname", OClass.INDEX_TYPE.NOTUNIQUE, "surname");

    for (int i = 0; i < 10; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("name", "name" + i);
      doc.setProperty("surname", "surname" + i);
      doc.save();
    }

    OResultSet result =
        db.query("select from " + className + " where name = 'name2' or surname = 'surname3'");
    printExecutionPlan(result);

    Assert.assertTrue(result.hasNext());
    for (int i = 0; i < 2; i++) {
      OResult next = result.next();
      Assert.assertNotNull(next);
      Assert.assertTrue(
          "name2".equals(next.getProperty("name"))
              || ("surname3".equals(next.getProperty("surname"))));
    }

    Assert.assertFalse(result.hasNext());

    Optional<OExecutionPlan> p = result.getExecutionPlan();
    Assert.assertTrue(p.isPresent());
    OExecutionPlan p2 = p.get();
    Assert.assertTrue(p2 instanceof OSelectExecutionPlan);
    OSelectExecutionPlan plan = (OSelectExecutionPlan) p2;
    Assert.assertEquals(ParallelExecStep.class, plan.getSteps().get(0).getClass());
    ParallelExecStep parallel = (ParallelExecStep) plan.getSteps().get(0);
    Assert.assertEquals(2, parallel.getSubExecutionPlans().size());
    result.close();
  }

  @Test
  public void testFetchFromClassWithIndexes2() {
    String className = "testFetchFromClassWithIndexes2";
    OClass clazz = db.getMetadata().getSchema().createClass(className);
    clazz.createProperty("name", OType.STRING);
    clazz.createProperty("surname", OType.STRING);
    clazz.createIndex(className + ".name", OClass.INDEX_TYPE.NOTUNIQUE, "name");
    clazz.createIndex(className + ".surname", OClass.INDEX_TYPE.NOTUNIQUE, "surname");

    for (int i = 0; i < 10; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("name", "name" + i);
      doc.setProperty("surname", "surname" + i);
      doc.save();
    }

    OResultSet result =
        db.query(
            "select from "
                + className
                + " where foo is not null and (name = 'name2' or surname = 'surname3')");
    printExecutionPlan(result);

    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testFetchFromClassWithIndexes3() {
    String className = "testFetchFromClassWithIndexes3";
    OClass clazz = db.getMetadata().getSchema().createClass(className);
    clazz.createProperty("name", OType.STRING);
    clazz.createProperty("surname", OType.STRING);
    clazz.createIndex(className + ".name", OClass.INDEX_TYPE.NOTUNIQUE, "name");
    clazz.createIndex(className + ".surname", OClass.INDEX_TYPE.NOTUNIQUE, "surname");

    for (int i = 0; i < 10; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("name", "name" + i);
      doc.setProperty("surname", "surname" + i);
      doc.setProperty("foo", i);
      doc.save();
    }

    OResultSet result =
        db.query(
            "select from "
                + className
                + " where foo < 100 and (name = 'name2' or surname = 'surname3')");
    printExecutionPlan(result);

    Assert.assertTrue(result.hasNext());
    for (int i = 0; i < 2; i++) {
      OResult next = result.next();
      Assert.assertNotNull(next);
      Assert.assertTrue(
          "name2".equals(next.getProperty("name"))
              || ("surname3".equals(next.getProperty("surname"))));
    }

    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testFetchFromClassWithIndexes4() {
    String className = "testFetchFromClassWithIndexes4";
    OClass clazz = db.getMetadata().getSchema().createClass(className);
    clazz.createProperty("name", OType.STRING);
    clazz.createProperty("surname", OType.STRING);
    clazz.createIndex(className + ".name", OClass.INDEX_TYPE.NOTUNIQUE, "name");
    clazz.createIndex(className + ".surname", OClass.INDEX_TYPE.NOTUNIQUE, "surname");

    for (int i = 0; i < 10; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("name", "name" + i);
      doc.setProperty("surname", "surname" + i);
      doc.setProperty("foo", i);
      doc.save();
    }

    OResultSet result =
        db.query(
            "select from "
                + className
                + " where foo < 100 and ((name = 'name2' and foo < 20) or surname = 'surname3') and ( 4<5 and foo < 50)");
    printExecutionPlan(result);

    Assert.assertTrue(result.hasNext());
    for (int i = 0; i < 2; i++) {
      OResult next = result.next();
      Assert.assertNotNull(next);
      Assert.assertTrue(
          "name2".equals(next.getProperty("name"))
              || ("surname3".equals(next.getProperty("surname"))));
    }

    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testFetchFromClassWithIndexes5() {
    String className = "testFetchFromClassWithIndexes5";
    OClass clazz = db.getMetadata().getSchema().createClass(className);
    clazz.createProperty("name", OType.STRING);
    clazz.createProperty("surname", OType.STRING);
    clazz.createIndex(className + ".name_surname", OClass.INDEX_TYPE.NOTUNIQUE, "name", "surname");

    for (int i = 0; i < 10; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("name", "name" + i);
      doc.setProperty("surname", "surname" + i);
      doc.setProperty("foo", i);
      doc.save();
    }

    OResultSet result =
        db.query("select from " + className + " where name = 'name3' and surname >= 'surname1'");
    printExecutionPlan(result);

    Assert.assertTrue(result.hasNext());
    for (int i = 0; i < 1; i++) {
      OResult next = result.next();
      Assert.assertNotNull(next);
      Assert.assertEquals("name3", next.getProperty("name"));
    }

    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testFetchFromClassWithIndexes6() {
    String className = "testFetchFromClassWithIndexes6";
    OClass clazz = db.getMetadata().getSchema().createClass(className);
    clazz.createProperty("name", OType.STRING);
    clazz.createProperty("surname", OType.STRING);
    clazz.createIndex(className + ".name_surname", OClass.INDEX_TYPE.NOTUNIQUE, "name", "surname");

    for (int i = 0; i < 10; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("name", "name" + i);
      doc.setProperty("surname", "surname" + i);
      doc.setProperty("foo", i);
      doc.save();
    }

    OResultSet result =
        db.query("select from " + className + " where name = 'name3' and surname > 'surname3'");
    printExecutionPlan(result);

    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testFetchFromClassWithIndexes7() {
    String className = "testFetchFromClassWithIndexes7";
    OClass clazz = db.getMetadata().getSchema().createClass(className);
    clazz.createProperty("name", OType.STRING);
    clazz.createProperty("surname", OType.STRING);
    clazz.createIndex(className + ".name_surname", OClass.INDEX_TYPE.NOTUNIQUE, "name", "surname");

    for (int i = 0; i < 10; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("name", "name" + i);
      doc.setProperty("surname", "surname" + i);
      doc.setProperty("foo", i);
      doc.save();
    }

    OResultSet result =
        db.query("select from " + className + " where name = 'name3' and surname >= 'surname3'");
    printExecutionPlan(result);
    for (int i = 0; i < 1; i++) {
      OResult next = result.next();
      Assert.assertNotNull(next);
      Assert.assertEquals("name3", next.getProperty("name"));
    }
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testFetchFromClassWithIndexes8() {
    String className = "testFetchFromClassWithIndexes8";
    OClass clazz = db.getMetadata().getSchema().createClass(className);
    clazz.createProperty("name", OType.STRING);
    clazz.createProperty("surname", OType.STRING);
    clazz.createIndex(className + ".name_surname", OClass.INDEX_TYPE.NOTUNIQUE, "name", "surname");

    for (int i = 0; i < 10; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("name", "name" + i);
      doc.setProperty("surname", "surname" + i);
      doc.setProperty("foo", i);
      doc.save();
    }

    OResultSet result =
        db.query("select from " + className + " where name = 'name3' and surname < 'surname3'");
    printExecutionPlan(result);

    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testFetchFromClassWithIndexes9() {
    String className = "testFetchFromClassWithIndexes9";
    OClass clazz = db.getMetadata().getSchema().createClass(className);
    clazz.createProperty("name", OType.STRING);
    clazz.createProperty("surname", OType.STRING);
    clazz.createIndex(className + ".name_surname", OClass.INDEX_TYPE.NOTUNIQUE, "name", "surname");

    for (int i = 0; i < 10; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("name", "name" + i);
      doc.setProperty("surname", "surname" + i);
      doc.setProperty("foo", i);
      doc.save();
    }

    OResultSet result =
        db.query("select from " + className + " where name = 'name3' and surname <= 'surname3'");
    printExecutionPlan(result);
    for (int i = 0; i < 1; i++) {
      OResult next = result.next();
      Assert.assertNotNull(next);
      Assert.assertEquals("name3", next.getProperty("name"));
    }
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testFetchFromClassWithIndexes10() {
    String className = "testFetchFromClassWithIndexes10";
    OClass clazz = db.getMetadata().getSchema().createClass(className);
    clazz.createProperty("name", OType.STRING);
    clazz.createProperty("surname", OType.STRING);
    clazz.createIndex(className + ".name_surname", OClass.INDEX_TYPE.NOTUNIQUE, "name", "surname");

    for (int i = 0; i < 10; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("name", "name" + i);
      doc.setProperty("surname", "surname" + i);
      doc.setProperty("foo", i);
      doc.save();
    }

    OResultSet result = db.query("select from " + className + " where name > 'name3' ");
    printExecutionPlan(result);
    for (int i = 0; i < 6; i++) {
      Assert.assertTrue(result.hasNext());
      OResult next = result.next();
      Assert.assertNotNull(next);
    }
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testFetchFromClassWithIndexes11() {
    String className = "testFetchFromClassWithIndexes11";
    OClass clazz = db.getMetadata().getSchema().createClass(className);
    clazz.createProperty("name", OType.STRING);
    clazz.createProperty("surname", OType.STRING);
    clazz.createIndex(className + ".name_surname", OClass.INDEX_TYPE.NOTUNIQUE, "name", "surname");

    for (int i = 0; i < 10; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("name", "name" + i);
      doc.setProperty("surname", "surname" + i);
      doc.setProperty("foo", i);
      doc.save();
    }

    OResultSet result = db.query("select from " + className + " where name >= 'name3' ");
    printExecutionPlan(result);
    for (int i = 0; i < 7; i++) {
      OResult next = result.next();
      Assert.assertNotNull(next);
    }
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testFetchFromClassWithIndexes12() {
    String className = "testFetchFromClassWithIndexes12";
    OClass clazz = db.getMetadata().getSchema().createClass(className);
    clazz.createProperty("name", OType.STRING);
    clazz.createProperty("surname", OType.STRING);
    clazz.createIndex(className + ".name_surname", OClass.INDEX_TYPE.NOTUNIQUE, "name", "surname");

    for (int i = 0; i < 10; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("name", "name" + i);
      doc.setProperty("surname", "surname" + i);
      doc.setProperty("foo", i);
      doc.save();
    }

    OResultSet result = db.query("select from " + className + " where name < 'name3' ");
    printExecutionPlan(result);
    for (int i = 0; i < 3; i++) {
      OResult next = result.next();
      Assert.assertNotNull(next);
    }
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testFetchFromClassWithIndexes13() {
    String className = "testFetchFromClassWithIndexes13";
    OClass clazz = db.getMetadata().getSchema().createClass(className);
    clazz.createProperty("name", OType.STRING);
    clazz.createProperty("surname", OType.STRING);
    clazz.createIndex(className + ".name_surname", OClass.INDEX_TYPE.NOTUNIQUE, "name", "surname");

    for (int i = 0; i < 10; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("name", "name" + i);
      doc.setProperty("surname", "surname" + i);
      doc.setProperty("foo", i);
      doc.save();
    }

    OResultSet result = db.query("select from " + className + " where name <= 'name3' ");
    printExecutionPlan(result);
    for (int i = 0; i < 4; i++) {
      OResult next = result.next();
      Assert.assertNotNull(next);
    }
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testFetchFromClassWithIndexes14() {
    String className = "testFetchFromClassWithIndexes14";
    OClass clazz = db.getMetadata().getSchema().createClass(className);
    clazz.createProperty("name", OType.STRING);
    clazz.createProperty("surname", OType.STRING);
    clazz.createIndex(className + ".name_surname", OClass.INDEX_TYPE.NOTUNIQUE, "name", "surname");

    for (int i = 0; i < 10; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("name", "name" + i);
      doc.setProperty("surname", "surname" + i);
      doc.setProperty("foo", i);
      doc.save();
    }

    OResultSet result =
        db.query("select from " + className + " where name > 'name3' and name < 'name5'");
    printExecutionPlan(result);
    for (int i = 0; i < 1; i++) {
      OResult next = result.next();
      Assert.assertNotNull(next);
    }
    Assert.assertFalse(result.hasNext());
    OSelectExecutionPlan plan = (OSelectExecutionPlan) result.getExecutionPlan().get();
    Assert.assertEquals(
        1, plan.getSteps().stream().filter(step -> step instanceof FetchFromIndexStep).count());
    result.close();
  }

  @Test
  public void testFetchFromClassWithIndexes15() {
    String className = "testFetchFromClassWithIndexes15";
    OClass clazz = db.getMetadata().getSchema().createClass(className);
    clazz.createProperty("name", OType.STRING);
    clazz.createProperty("surname", OType.STRING);
    clazz.createIndex(className + ".name_surname", OClass.INDEX_TYPE.NOTUNIQUE, "name", "surname");

    for (int i = 0; i < 10; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("name", "name" + i);
      doc.setProperty("surname", "surname" + i);
      doc.setProperty("foo", i);
      doc.save();
    }

    OResultSet result =
        db.query(
            "select from "
                + className
                + " where name > 'name6' and name = 'name3' and surname > 'surname2' and surname < 'surname5' ");
    printExecutionPlan(result);
    Assert.assertFalse(result.hasNext());
    OSelectExecutionPlan plan = (OSelectExecutionPlan) result.getExecutionPlan().get();
    Assert.assertEquals(
        1, plan.getSteps().stream().filter(step -> step instanceof FetchFromIndexStep).count());
    result.close();
  }

  @Test
  public void testFetchFromClassWithHashIndexes1() {
    String className = "testFetchFromClassWithHashIndexes1";
    OClass clazz = db.getMetadata().getSchema().createClass(className);
    clazz.createProperty("name", OType.STRING);
    clazz.createProperty("surname", OType.STRING);
    clazz.createIndex(
        className + ".name_surname", OClass.INDEX_TYPE.NOTUNIQUE_HASH_INDEX, "name", "surname");

    for (int i = 0; i < 10; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("name", "name" + i);
      doc.setProperty("surname", "surname" + i);
      doc.setProperty("foo", i);
      doc.save();
    }

    OResultSet result =
        db.query("select from " + className + " where name = 'name6' and surname = 'surname6' ");
    printExecutionPlan(result);

    for (int i = 0; i < 1; i++) {
      Assert.assertTrue(result.hasNext());
      OResult next = result.next();
      Assert.assertNotNull(next);
    }
    Assert.assertFalse(result.hasNext());
    OSelectExecutionPlan plan = (OSelectExecutionPlan) result.getExecutionPlan().get();
    Assert.assertEquals(
        1, plan.getSteps().stream().filter(step -> step instanceof FetchFromIndexStep).count());
    result.close();
  }

  @Test
  public void testFetchFromClassWithHashIndexes2() {
    String className = "testFetchFromClassWithHashIndexes2";
    OClass clazz = db.getMetadata().getSchema().createClass(className);
    clazz.createProperty("name", OType.STRING);
    clazz.createProperty("surname", OType.STRING);
    clazz.createIndex(
        className + ".name_surname", OClass.INDEX_TYPE.NOTUNIQUE_HASH_INDEX, "name", "surname");

    for (int i = 0; i < 10; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("name", "name" + i);
      doc.setProperty("surname", "surname" + i);
      doc.setProperty("foo", i);
      doc.save();
    }

    OResultSet result =
        db.query("select from " + className + " where name = 'name6' and surname >= 'surname6' ");
    printExecutionPlan(result);

    for (int i = 0; i < 1; i++) {
      Assert.assertTrue(result.hasNext());
      OResult next = result.next();
      Assert.assertNotNull(next);
    }
    Assert.assertFalse(result.hasNext());
    OSelectExecutionPlan plan = (OSelectExecutionPlan) result.getExecutionPlan().get();
    Assert.assertEquals(
        FetchFromClassExecutionStep.class, plan.getSteps().get(0).getClass()); // index not used
    result.close();
  }

  @Test
  public void testExpand1() {
    String childClassName = "testExpand1_child";
    String parentClassName = "testExpand1_parent";
    OClass childClass = db.getMetadata().getSchema().createClass(childClassName);
    OClass parentClass = db.getMetadata().getSchema().createClass(parentClassName);

    int count = 10;
    for (int i = 0; i < count; i++) {
      ODocument doc = db.newInstance(childClassName);
      doc.setProperty("name", "name" + i);
      doc.setProperty("surname", "surname" + i);
      doc.setProperty("foo", i);
      doc.save();

      ODocument parent = new ODocument(parentClassName);
      parent.setProperty("linked", doc);
      parent.save();
    }

    OResultSet result = db.query("select expand(linked) from " + parentClassName);
    printExecutionPlan(result);

    for (int i = 0; i < count; i++) {
      Assert.assertTrue(result.hasNext());
      OResult next = result.next();
      Assert.assertNotNull(next);
    }
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testExpand2() {
    String childClassName = "testExpand2_child";
    String parentClassName = "testExpand2_parent";
    OClass childClass = db.getMetadata().getSchema().createClass(childClassName);
    OClass parentClass = db.getMetadata().getSchema().createClass(parentClassName);

    int count = 10;
    int collSize = 11;
    for (int i = 0; i < count; i++) {
      List coll = new ArrayList<>();
      for (int j = 0; j < collSize; j++) {
        ODocument doc = db.newInstance(childClassName);
        doc.setProperty("name", "name" + i);
        doc.save();
        coll.add(doc);
      }

      ODocument parent = new ODocument(parentClassName);
      parent.setProperty("linked", coll);
      parent.save();
    }

    OResultSet result = db.query("select expand(linked) from " + parentClassName);
    printExecutionPlan(result);

    for (int i = 0; i < count * collSize; i++) {
      Assert.assertTrue(result.hasNext());
      OResult next = result.next();
      Assert.assertNotNull(next);
    }
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testExpand3() {
    String childClassName = "testExpand3_child";
    String parentClassName = "testExpand3_parent";
    OClass childClass = db.getMetadata().getSchema().createClass(childClassName);
    OClass parentClass = db.getMetadata().getSchema().createClass(parentClassName);

    int count = 30;
    int collSize = 7;
    for (int i = 0; i < count; i++) {
      List coll = new ArrayList<>();
      for (int j = 0; j < collSize; j++) {
        ODocument doc = db.newInstance(childClassName);
        doc.setProperty("name", "name" + j);
        doc.save();
        coll.add(doc);
      }

      ODocument parent = new ODocument(parentClassName);
      parent.setProperty("linked", coll);
      parent.save();
    }

    OResultSet result =
        db.query("select expand(linked) from " + parentClassName + " order by name");
    printExecutionPlan(result);

    String last = null;
    for (int i = 0; i < count * collSize; i++) {
      Assert.assertTrue(result.hasNext());
      OResult next = result.next();
      if (i > 0) {
        Assert.assertTrue(last.compareTo(next.getProperty("name")) <= 0);
      }
      last = next.getProperty("name");
      Assert.assertNotNull(next);
    }
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testDistinct1() {
    String className = "testDistinct1";
    OClass clazz = db.getMetadata().getSchema().createClass(className);
    clazz.createProperty("name", OType.STRING);
    clazz.createProperty("surname", OType.STRING);

    for (int i = 0; i < 30; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("name", "name" + i % 10);
      doc.setProperty("surname", "surname" + i % 10);
      doc.save();
    }

    OResultSet result = db.query("select distinct name, surname from " + className);
    printExecutionPlan(result);

    for (int i = 0; i < 10; i++) {
      Assert.assertTrue(result.hasNext());
      OResult next = result.next();
      Assert.assertNotNull(next);
    }
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testDistinct2() {
    String className = "testDistinct2";
    OClass clazz = db.getMetadata().getSchema().createClass(className);
    clazz.createProperty("name", OType.STRING);
    clazz.createProperty("surname", OType.STRING);

    for (int i = 0; i < 30; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("name", "name" + i % 10);
      doc.setProperty("surname", "surname" + i % 10);
      doc.save();
    }

    OResultSet result = db.query("select distinct(name) from " + className);
    printExecutionPlan(result);

    for (int i = 0; i < 10; i++) {
      Assert.assertTrue(result.hasNext());
      OResult next = result.next();
      Assert.assertNotNull(next);
    }
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testLet1() {
    OResultSet result = db.query("select $a as one, $b as two let $a = 1, $b = 1+1");
    Assert.assertTrue(result.hasNext());
    OResult item = result.next();
    Assert.assertNotNull(item);
    Assert.assertEquals(1, item.<Object>getProperty("one"));
    Assert.assertEquals(2, item.<Object>getProperty("two"));
    printExecutionPlan(result);
    result.close();
  }

  @Test
  public void testLet1Long() {
    OResultSet result = db.query("select $a as one, $b as two let $a = 1L, $b = 1L+1");
    Assert.assertTrue(result.hasNext());
    OResult item = result.next();
    Assert.assertNotNull(item);
    Assert.assertEquals(1l, item.<Object>getProperty("one"));
    Assert.assertEquals(2l, item.<Object>getProperty("two"));
    printExecutionPlan(result);
    result.close();
  }

  @Test
  public void testLet2() {
    OResultSet result = db.query("select $a as one let $a = (select 1 as a)");
    printExecutionPlan(result);
    Assert.assertTrue(result.hasNext());
    OResult item = result.next();
    Assert.assertNotNull(item);
    Object one = item.getProperty("one");
    Assert.assertTrue(one instanceof List);
    Assert.assertEquals(1, ((List) one).size());
    Object x = ((List) one).get(0);
    Assert.assertTrue(x instanceof OResult);
    Assert.assertEquals(1, (Object) ((OResult) x).getProperty("a"));
    result.close();
  }

  @Test
  public void testLet3() {
    OResultSet result = db.query("select $a[0].foo as one let $a = (select 1 as foo)");
    printExecutionPlan(result);
    Assert.assertTrue(result.hasNext());
    OResult item = result.next();
    Assert.assertNotNull(item);
    Object one = item.getProperty("one");
    Assert.assertEquals(1, one);
    result.close();
  }

  @Test
  public void testLet4() {
    String className = "testLet4";
    db.getMetadata().getSchema().createClass(className);

    for (int i = 0; i < 10; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("name", "name" + i);
      doc.setProperty("surname", "surname" + i);
      doc.save();
    }

    OResultSet result =
        db.query(
            "select name, surname, $nameAndSurname as fullname from "
                + className
                + " let $nameAndSurname = name + ' ' + surname");
    printExecutionPlan(result);
    for (int i = 0; i < 10; i++) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Assert.assertNotNull(item);
      Assert.assertEquals(
          item.getProperty("fullname"),
          item.getProperty("name") + " " + item.getProperty("surname"));
    }
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testLet5() {
    String className = "testLet5";
    db.getMetadata().getSchema().createClass(className);

    for (int i = 0; i < 10; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("name", "name" + i);
      doc.setProperty("surname", "surname" + i);
      doc.save();
    }

    OResultSet result =
        db.query(
            "select from "
                + className
                + " where name in (select name from "
                + className
                + " where name = 'name1')");
    printExecutionPlan(result);
    for (int i = 0; i < 1; i++) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Assert.assertNotNull(item);
      Assert.assertEquals("name1", item.getProperty("name"));
    }
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testLet6() {
    String className = "testLet6";
    db.getMetadata().getSchema().createClass(className);

    for (int i = 0; i < 10; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("name", "name" + i);
      doc.setProperty("surname", "surname" + i);
      doc.save();
    }

    OResultSet result =
        db.query(
            "select $foo as name from "
                + className
                + " let $foo = (select name from "
                + className
                + " where name = $parent.$current.name)");
    printExecutionPlan(result);
    for (int i = 0; i < 10; i++) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Assert.assertNotNull(item);
      Assert.assertNotNull(item.getProperty("name"));
      Assert.assertTrue(item.getProperty("name") instanceof Collection);
    }
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testLet7() {
    String className = "testLet7";
    db.getMetadata().getSchema().createClass(className);

    for (int i = 0; i < 10; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("name", "name" + i);
      doc.setProperty("surname", "surname" + i);
      doc.save();
    }

    OResultSet result =
        db.query(
            "select $bar as name from "
                + className
                + " "
                + "let $foo = (select name from "
                + className
                + " where name = $parent.$current.name),"
                + "$bar = $foo[0].name");
    printExecutionPlan(result);
    for (int i = 0; i < 10; i++) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Assert.assertNotNull(item);
      Assert.assertNotNull(item.getProperty("name"));
      Assert.assertTrue(item.getProperty("name") instanceof String);
    }
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testLetWithTraverseFunction() {
    String vertexClassName = "testLetWithTraverseFunction";
    String edgeClassName = "testLetWithTraverseFunctioEdge";

    OClass vertexClass = db.createVertexClass(vertexClassName);

    OVertex doc1 = db.newVertex(vertexClass);
    doc1.setProperty("name", "A");
    doc1.save();

    OVertex doc2 = db.newVertex(vertexClass);
    doc2.setProperty("name", "B");
    doc2.save();
    ORID doc2Id = doc2.getIdentity();

    OClass edgeClass = db.createEdgeClass(edgeClassName);

    db.newEdge(doc1, doc2, edgeClass);
    String queryString =
        "SELECT $x, name FROM " + vertexClassName + " let $x = out(\"" + edgeClassName + "\")";
    OResultSet resultSet = db.query(queryString);
    int counter = 0;
    while (resultSet.hasNext()) {
      OResult result = resultSet.next();
      Iterable edge = result.getProperty("$x");
      Iterator<OIdentifiable> iter = edge.iterator();
      while (iter.hasNext()) {
        OVertex toVertex = db.load(iter.next().getIdentity());
        if (doc2Id.equals(toVertex.getIdentity())) {
          ++counter;
        }
      }
    }
    Assert.assertEquals(1, counter);
    resultSet.close();
  }

  @Test
  public void testLetVariableSubqueryProjectionFetchFromClassTarget_9695() {
    String className = "testLetVariableSubqueryProjectionFetchFromClassTarget_9695";
    db.getMetadata().getSchema().createClass(className);

    for (int i = 0; i < 10; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("i", i);
      doc.setProperty("iSeq", new int[] {i, 2 * i, 4 * i});
      doc.save();
    }
    OResultSet result =
        db.query(
            "select $current.*, $b.*, $b.@class from (select 1 as sqa, @class as sqc from "
                + className
                + " LIMIT 2)\n"
                + "let $b = $current");
    printExecutionPlan(result);
    Assert.assertTrue(result.hasNext());
    OResult item = result.next();
    Assert.assertNotNull(item);
    Object currentProperty = item.getProperty("$current.*");
    Assert.assertTrue(currentProperty instanceof OResult);
    final OResult currentResult = (OResult) currentProperty;
    Assert.assertTrue(currentResult.isProjection());
    Assert.assertEquals(Integer.valueOf(1), currentResult.<Integer>getProperty("sqa"));
    Assert.assertEquals(className, currentResult.getProperty("sqc"));
    Object bProperty = item.getProperty("$b.*");
    Assert.assertTrue(bProperty instanceof OResult);
    final OResult bResult = (OResult) bProperty;
    Assert.assertTrue(bResult.isProjection());
    Assert.assertEquals(Integer.valueOf(1), bResult.<Integer>getProperty("sqa"));
    Assert.assertEquals(className, bResult.getProperty("sqc"));
    result.close();
  }

  @Test
  public void testUnwind1() {
    String className = "testUnwind1";
    db.getMetadata().getSchema().createClass(className);

    for (int i = 0; i < 10; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("i", i);
      doc.setProperty("iSeq", new int[] {i, 2 * i, 4 * i});
      doc.save();
    }

    OResultSet result = db.query("select i, iSeq from " + className + " unwind iSeq");
    printExecutionPlan(result);
    for (int i = 0; i < 30; i++) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Assert.assertNotNull(item);
      Assert.assertNotNull(item.getProperty("i"));
      Assert.assertNotNull(item.getProperty("iSeq"));
      Integer first = item.getProperty("i");
      Integer second = item.getProperty("iSeq");
      Assert.assertTrue(first + second == 0 || second.intValue() % first.intValue() == 0);
    }
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testUnwind2() {
    String className = "testUnwind2";
    db.getMetadata().getSchema().createClass(className);

    for (int i = 0; i < 10; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("i", i);
      List<Integer> iSeq = new ArrayList<>();
      iSeq.add(i);
      iSeq.add(i * 2);
      iSeq.add(i * 4);
      doc.setProperty("iSeq", iSeq);
      doc.save();
    }

    OResultSet result = db.query("select i, iSeq from " + className + " unwind iSeq");
    printExecutionPlan(result);
    for (int i = 0; i < 30; i++) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Assert.assertNotNull(item);
      Assert.assertNotNull(item.getProperty("i"));
      Assert.assertNotNull(item.getProperty("iSeq"));
      Integer first = item.getProperty("i");
      Integer second = item.getProperty("iSeq");
      Assert.assertTrue(first + second == 0 || second.intValue() % first.intValue() == 0);
    }
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testFetchFromSubclassIndexes1() {
    String parent = "testFetchFromSubclassIndexes1_parent";
    String child1 = "testFetchFromSubclassIndexes1_child1";
    String child2 = "testFetchFromSubclassIndexes1_child2";
    OClass parentClass = db.getMetadata().getSchema().createClass(parent);
    OClass childClass1 = db.getMetadata().getSchema().createClass(child1, parentClass);
    OClass childClass2 = db.getMetadata().getSchema().createClass(child2, parentClass);

    parentClass.createProperty("name", OType.STRING);
    childClass1.createIndex(child1 + ".name", OClass.INDEX_TYPE.NOTUNIQUE, "name");
    childClass2.createIndex(child2 + ".name", OClass.INDEX_TYPE.NOTUNIQUE, "name");

    for (int i = 0; i < 10; i++) {
      ODocument doc = db.newInstance(child1);
      doc.setProperty("name", "name" + i);
      doc.save();
    }

    for (int i = 0; i < 10; i++) {
      ODocument doc = db.newInstance(child2);
      doc.setProperty("name", "name" + i);
      doc.save();
    }

    OResultSet result = db.query("select from " + parent + " where name = 'name1'");
    printExecutionPlan(result);
    OInternalExecutionPlan plan = (OInternalExecutionPlan) result.getExecutionPlan().get();
    Assert.assertTrue(plan.getSteps().get(0) instanceof ParallelExecStep);
    for (int i = 0; i < 2; i++) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Assert.assertNotNull(item);
    }
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testFetchFromSubclassIndexes2() {
    String parent = "testFetchFromSubclassIndexes2_parent";
    String child1 = "testFetchFromSubclassIndexes2_child1";
    String child2 = "testFetchFromSubclassIndexes2_child2";
    OClass parentClass = db.getMetadata().getSchema().createClass(parent);
    OClass childClass1 = db.getMetadata().getSchema().createClass(child1, parentClass);
    OClass childClass2 = db.getMetadata().getSchema().createClass(child2, parentClass);

    parentClass.createProperty("name", OType.STRING);
    childClass1.createIndex(child1 + ".name", OClass.INDEX_TYPE.NOTUNIQUE, "name");
    childClass2.createIndex(child2 + ".name", OClass.INDEX_TYPE.NOTUNIQUE, "name");

    for (int i = 0; i < 10; i++) {
      ODocument doc = db.newInstance(child1);
      doc.setProperty("name", "name" + i);
      doc.setProperty("surname", "surname" + i);
      doc.save();
    }

    for (int i = 0; i < 10; i++) {
      ODocument doc = db.newInstance(child2);
      doc.setProperty("name", "name" + i);
      doc.setProperty("surname", "surname" + i);
      doc.save();
    }

    OResultSet result =
        db.query("select from " + parent + " where name = 'name1' and surname = 'surname1'");
    printExecutionPlan(result);
    OInternalExecutionPlan plan = (OInternalExecutionPlan) result.getExecutionPlan().get();
    Assert.assertTrue(plan.getSteps().get(0) instanceof ParallelExecStep);
    for (int i = 0; i < 2; i++) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Assert.assertNotNull(item);
    }
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testFetchFromSubclassIndexes3() {
    String parent = "testFetchFromSubclassIndexes3_parent";
    String child1 = "testFetchFromSubclassIndexes3_child1";
    String child2 = "testFetchFromSubclassIndexes3_child2";
    OClass parentClass = db.getMetadata().getSchema().createClass(parent);
    OClass childClass1 = db.getMetadata().getSchema().createClass(child1, parentClass);
    OClass childClass2 = db.getMetadata().getSchema().createClass(child2, parentClass);

    parentClass.createProperty("name", OType.STRING);
    childClass1.createIndex(child1 + ".name", OClass.INDEX_TYPE.NOTUNIQUE, "name");

    for (int i = 0; i < 10; i++) {
      ODocument doc = db.newInstance(child1);
      doc.setProperty("name", "name" + i);
      doc.setProperty("surname", "surname" + i);
      doc.save();
    }

    for (int i = 0; i < 10; i++) {
      ODocument doc = db.newInstance(child2);
      doc.setProperty("name", "name" + i);
      doc.setProperty("surname", "surname" + i);
      doc.save();
    }

    OResultSet result =
        db.query("select from " + parent + " where name = 'name1' and surname = 'surname1'");
    printExecutionPlan(result);
    OInternalExecutionPlan plan = (OInternalExecutionPlan) result.getExecutionPlan().get();
    Assert.assertTrue(
        plan.getSteps().get(0) instanceof FetchFromClassExecutionStep); // no index used
    for (int i = 0; i < 2; i++) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Assert.assertNotNull(item);
    }
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testFetchFromSubclassIndexes4() {
    String parent = "testFetchFromSubclassIndexes4_parent";
    String child1 = "testFetchFromSubclassIndexes4_child1";
    String child2 = "testFetchFromSubclassIndexes4_child2";
    OClass parentClass = db.getMetadata().getSchema().createClass(parent);
    OClass childClass1 = db.getMetadata().getSchema().createClass(child1, parentClass);
    OClass childClass2 = db.getMetadata().getSchema().createClass(child2, parentClass);

    parentClass.createProperty("name", OType.STRING);
    childClass1.createIndex(child1 + ".name", OClass.INDEX_TYPE.NOTUNIQUE, "name");
    childClass2.createIndex(child2 + ".name", OClass.INDEX_TYPE.NOTUNIQUE, "name");

    ODocument parentdoc = db.newInstance(parent);
    parentdoc.setProperty("name", "foo");
    parentdoc.save();

    for (int i = 0; i < 10; i++) {
      ODocument doc = db.newInstance(child1);
      doc.setProperty("name", "name" + i);
      doc.setProperty("surname", "surname" + i);
      doc.save();
    }

    for (int i = 0; i < 10; i++) {
      ODocument doc = db.newInstance(child2);
      doc.setProperty("name", "name" + i);
      doc.setProperty("surname", "surname" + i);
      doc.save();
    }

    OResultSet result =
        db.query("select from " + parent + " where name = 'name1' and surname = 'surname1'");
    printExecutionPlan(result);
    OInternalExecutionPlan plan = (OInternalExecutionPlan) result.getExecutionPlan().get();
    Assert.assertTrue(
        plan.getSteps().get(0)
            instanceof
            FetchFromClassExecutionStep); // no index, because the superclass is not empty
    for (int i = 0; i < 2; i++) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Assert.assertNotNull(item);
    }
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testFetchFromSubSubclassIndexes() {
    String parent = "testFetchFromSubSubclassIndexes_parent";
    String child1 = "testFetchFromSubSubclassIndexes_child1";
    String child2 = "testFetchFromSubSubclassIndexes_child2";
    String child2_1 = "testFetchFromSubSubclassIndexes_child2_1";
    String child2_2 = "testFetchFromSubSubclassIndexes_child2_2";
    OClass parentClass = db.getMetadata().getSchema().createClass(parent);
    OClass childClass1 = db.getMetadata().getSchema().createClass(child1, parentClass);
    OClass childClass2 = db.getMetadata().getSchema().createClass(child2, parentClass);
    OClass childClass2_1 = db.getMetadata().getSchema().createClass(child2_1, childClass2);
    OClass childClass2_2 = db.getMetadata().getSchema().createClass(child2_2, childClass2);

    parentClass.createProperty("name", OType.STRING);
    childClass1.createIndex(child1 + ".name", OClass.INDEX_TYPE.NOTUNIQUE, "name");
    childClass2_1.createIndex(child2_1 + ".name", OClass.INDEX_TYPE.NOTUNIQUE, "name");
    childClass2_2.createIndex(child2_2 + ".name", OClass.INDEX_TYPE.NOTUNIQUE, "name");

    for (int i = 0; i < 10; i++) {
      ODocument doc = db.newInstance(child1);
      doc.setProperty("name", "name" + i);
      doc.setProperty("surname", "surname" + i);
      doc.save();
    }

    for (int i = 0; i < 10; i++) {
      ODocument doc = db.newInstance(child2_1);
      doc.setProperty("name", "name" + i);
      doc.setProperty("surname", "surname" + i);
      doc.save();
    }

    for (int i = 0; i < 10; i++) {
      ODocument doc = db.newInstance(child2_2);
      doc.setProperty("name", "name" + i);
      doc.setProperty("surname", "surname" + i);
      doc.save();
    }

    OResultSet result =
        db.query("select from " + parent + " where name = 'name1' and surname = 'surname1'");
    printExecutionPlan(result);
    OInternalExecutionPlan plan = (OInternalExecutionPlan) result.getExecutionPlan().get();
    Assert.assertTrue(plan.getSteps().get(0) instanceof ParallelExecStep);
    for (int i = 0; i < 3; i++) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Assert.assertNotNull(item);
    }
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testFetchFromSubSubclassIndexesWithDiamond() {
    String parent = "testFetchFromSubSubclassIndexesWithDiamond_parent";
    String child1 = "testFetchFromSubSubclassIndexesWithDiamond_child1";
    String child2 = "testFetchFromSubSubclassIndexesWithDiamond_child2";
    String child12 = "testFetchFromSubSubclassIndexesWithDiamond_child12";

    OClass parentClass = db.getMetadata().getSchema().createClass(parent);
    OClass childClass1 = db.getMetadata().getSchema().createClass(child1, parentClass);
    OClass childClass2 = db.getMetadata().getSchema().createClass(child2, parentClass);
    OClass childClass12 =
        db.getMetadata().getSchema().createClass(child12, childClass1, childClass2);

    parentClass.createProperty("name", OType.STRING);
    childClass1.createIndex(child1 + ".name", OClass.INDEX_TYPE.NOTUNIQUE, "name");
    childClass2.createIndex(child2 + ".name", OClass.INDEX_TYPE.NOTUNIQUE, "name");

    for (int i = 0; i < 10; i++) {
      ODocument doc = db.newInstance(child1);
      doc.setProperty("name", "name" + i);
      doc.setProperty("surname", "surname" + i);
      doc.save();
    }

    for (int i = 0; i < 10; i++) {
      ODocument doc = db.newInstance(child2);
      doc.setProperty("name", "name" + i);
      doc.setProperty("surname", "surname" + i);
      doc.save();
    }

    for (int i = 0; i < 10; i++) {
      ODocument doc = db.newInstance(child12);
      doc.setProperty("name", "name" + i);
      doc.setProperty("surname", "surname" + i);
      doc.save();
    }

    OResultSet result =
        db.query("select from " + parent + " where name = 'name1' and surname = 'surname1'");
    printExecutionPlan(result);
    OInternalExecutionPlan plan = (OInternalExecutionPlan) result.getExecutionPlan().get();
    Assert.assertTrue(plan.getSteps().get(0) instanceof FetchFromClassExecutionStep);
    for (int i = 0; i < 3; i++) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Assert.assertNotNull(item);
    }
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testIndexPlusSort1() {
    String className = "testIndexPlusSort1";
    OClass clazz = db.getMetadata().getSchema().createClass(className);
    clazz.createProperty("name", OType.STRING);
    clazz.createProperty("surname", OType.STRING);
    db.command(
            "create index "
                + className
                + ".name_surname on "
                + className
                + " (name, surname) NOTUNIQUE")
        .close();

    for (int i = 0; i < 10; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("name", "name" + i % 3);
      doc.setProperty("surname", "surname" + i);
      doc.save();
    }

    OResultSet result =
        db.query("select from " + className + " where name = 'name1' order by surname ASC");
    printExecutionPlan(result);
    String lastSurname = null;
    for (int i = 0; i < 3; i++) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Assert.assertNotNull(item);
      Assert.assertNotNull(item.getProperty("surname"));

      String surname = item.getProperty("surname");
      if (i > 0) {
        Assert.assertTrue(surname.compareTo(lastSurname) > 0);
      }
      lastSurname = surname;
    }
    Assert.assertFalse(result.hasNext());
    OExecutionPlan plan = result.getExecutionPlan().get();
    Assert.assertEquals(
        1, plan.getSteps().stream().filter(step -> step instanceof FetchFromIndexStep).count());
    Assert.assertEquals(
        0, plan.getSteps().stream().filter(step -> step instanceof OrderByStep).count());
    result.close();
  }

  @Test
  public void testIndexPlusSort2() {
    String className = "testIndexPlusSort2";
    OClass clazz = db.getMetadata().getSchema().createClass(className);
    clazz.createProperty("name", OType.STRING);
    clazz.createProperty("surname", OType.STRING);
    db.command(
            "create index "
                + className
                + ".name_surname on "
                + className
                + " (name, surname) NOTUNIQUE")
        .close();

    for (int i = 0; i < 10; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("name", "name" + i % 3);
      doc.setProperty("surname", "surname" + i);
      doc.save();
    }

    OResultSet result =
        db.query("select from " + className + " where name = 'name1' order by surname DESC");
    printExecutionPlan(result);
    String lastSurname = null;
    for (int i = 0; i < 3; i++) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Assert.assertNotNull(item);
      Assert.assertNotNull(item.getProperty("surname"));

      String surname = item.getProperty("surname");
      if (i > 0) {
        Assert.assertTrue(surname.compareTo(lastSurname) < 0);
      }
      lastSurname = surname;
    }
    Assert.assertFalse(result.hasNext());
    OExecutionPlan plan = result.getExecutionPlan().get();
    Assert.assertEquals(
        1, plan.getSteps().stream().filter(step -> step instanceof FetchFromIndexStep).count());
    Assert.assertEquals(
        0, plan.getSteps().stream().filter(step -> step instanceof OrderByStep).count());
    result.close();
  }

  @Test
  public void testIndexPlusSort3() {
    String className = "testIndexPlusSort3";
    OClass clazz = db.getMetadata().getSchema().createClass(className);
    clazz.createProperty("name", OType.STRING);
    clazz.createProperty("surname", OType.STRING);
    db.command(
            "create index "
                + className
                + ".name_surname on "
                + className
                + " (name, surname) NOTUNIQUE")
        .close();

    for (int i = 0; i < 10; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("name", "name" + i % 3);
      doc.setProperty("surname", "surname" + i);
      doc.save();
    }

    OResultSet result =
        db.query(
            "select from " + className + " where name = 'name1' order by name DESC, surname DESC");
    printExecutionPlan(result);
    String lastSurname = null;
    for (int i = 0; i < 3; i++) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Assert.assertNotNull(item);
      Assert.assertNotNull(item.getProperty("surname"));

      String surname = item.getProperty("surname");
      if (i > 0) {
        Assert.assertTrue(((String) item.getProperty("surname")).compareTo(lastSurname) < 0);
      }
      lastSurname = surname;
    }
    Assert.assertFalse(result.hasNext());
    OExecutionPlan plan = result.getExecutionPlan().get();
    Assert.assertEquals(
        1, plan.getSteps().stream().filter(step -> step instanceof FetchFromIndexStep).count());
    Assert.assertEquals(
        0, plan.getSteps().stream().filter(step -> step instanceof OrderByStep).count());
    result.close();
  }

  @Test
  public void testIndexPlusSort4() {
    String className = "testIndexPlusSort4";
    OClass clazz = db.getMetadata().getSchema().createClass(className);
    clazz.createProperty("name", OType.STRING);
    clazz.createProperty("surname", OType.STRING);
    db.command(
            "create index "
                + className
                + ".name_surname on "
                + className
                + " (name, surname) NOTUNIQUE")
        .close();

    for (int i = 0; i < 10; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("name", "name" + i % 3);
      doc.setProperty("surname", "surname" + i);
      doc.save();
    }

    OResultSet result =
        db.query(
            "select from " + className + " where name = 'name1' order by name ASC, surname ASC");
    printExecutionPlan(result);
    String lastSurname = null;
    for (int i = 0; i < 3; i++) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Assert.assertNotNull(item);
      Assert.assertNotNull(item.getProperty("surname"));

      String surname = item.getProperty("surname");
      if (i > 0) {
        Assert.assertTrue(surname.compareTo(lastSurname) > 0);
      }
      lastSurname = surname;
    }
    Assert.assertFalse(result.hasNext());
    OExecutionPlan plan = result.getExecutionPlan().get();
    Assert.assertEquals(
        1, plan.getSteps().stream().filter(step -> step instanceof FetchFromIndexStep).count());
    Assert.assertEquals(
        0, plan.getSteps().stream().filter(step -> step instanceof OrderByStep).count());
    result.close();
  }

  @Test
  public void testIndexPlusSort5() {
    String className = "testIndexPlusSort5";
    OClass clazz = db.getMetadata().getSchema().createClass(className);
    clazz.createProperty("name", OType.STRING);
    clazz.createProperty("surname", OType.STRING);
    clazz.createProperty("address", OType.STRING);
    db.command(
            "create index "
                + className
                + ".name_surname on "
                + className
                + " (name, surname, address) NOTUNIQUE")
        .close();

    for (int i = 0; i < 10; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("name", "name" + i % 3);
      doc.setProperty("surname", "surname" + i);
      doc.save();
    }

    OResultSet result =
        db.query("select from " + className + " where name = 'name1' order by surname ASC");
    printExecutionPlan(result);
    String lastSurname = null;
    for (int i = 0; i < 3; i++) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Assert.assertNotNull(item);
      Assert.assertNotNull(item.getProperty("surname"));
      String surname = item.getProperty("surname");
      if (i > 0) {
        Assert.assertTrue(surname.compareTo(lastSurname) > 0);
      }
      lastSurname = surname;
    }
    Assert.assertFalse(result.hasNext());
    OExecutionPlan plan = result.getExecutionPlan().get();
    Assert.assertEquals(
        1, plan.getSteps().stream().filter(step -> step instanceof FetchFromIndexStep).count());
    Assert.assertEquals(
        0, plan.getSteps().stream().filter(step -> step instanceof OrderByStep).count());
    result.close();
  }

  @Test
  public void testIndexPlusSort6() {
    String className = "testIndexPlusSort6";
    OClass clazz = db.getMetadata().getSchema().createClass(className);
    clazz.createProperty("name", OType.STRING);
    clazz.createProperty("surname", OType.STRING);
    clazz.createProperty("address", OType.STRING);
    db.command(
            "create index "
                + className
                + ".name_surname on "
                + className
                + " (name, surname, address) NOTUNIQUE")
        .close();

    for (int i = 0; i < 10; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("name", "name" + i % 3);
      doc.setProperty("surname", "surname" + i);
      doc.save();
    }

    OResultSet result =
        db.query("select from " + className + " where name = 'name1' order by surname DESC");
    printExecutionPlan(result);
    String lastSurname = null;
    for (int i = 0; i < 3; i++) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Assert.assertNotNull(item);
      Assert.assertNotNull(item.getProperty("surname"));
      String surname = item.getProperty("surname");
      if (i > 0) {
        Assert.assertTrue(surname.compareTo(lastSurname) < 0);
      }
      lastSurname = surname;
    }
    Assert.assertFalse(result.hasNext());
    OExecutionPlan plan = result.getExecutionPlan().get();
    Assert.assertEquals(
        1, plan.getSteps().stream().filter(step -> step instanceof FetchFromIndexStep).count());
    Assert.assertEquals(
        0, plan.getSteps().stream().filter(step -> step instanceof OrderByStep).count());
    result.close();
  }

  @Test
  public void testIndexPlusSort7() {
    String className = "testIndexPlusSort7";
    OClass clazz = db.getMetadata().getSchema().createClass(className);
    clazz.createProperty("name", OType.STRING);
    clazz.createProperty("surname", OType.STRING);
    clazz.createProperty("address", OType.STRING);
    db.command(
            "create index "
                + className
                + ".name_surname on "
                + className
                + " (name, surname, address) NOTUNIQUE")
        .close();

    for (int i = 0; i < 10; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("name", "name" + i % 3);
      doc.setProperty("surname", "surname" + i);
      doc.save();
    }

    OResultSet result =
        db.query("select from " + className + " where name = 'name1' order by address DESC");
    printExecutionPlan(result);

    for (int i = 0; i < 3; i++) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Assert.assertNotNull(item);
      Assert.assertNotNull(item.getProperty("surname"));
    }
    Assert.assertFalse(result.hasNext());
    boolean orderStepFound = false;
    for (OExecutionStep step : result.getExecutionPlan().get().getSteps()) {
      if (step instanceof OrderByStep) {
        orderStepFound = true;
        break;
      }
    }
    Assert.assertTrue(orderStepFound);
    result.close();
  }

  @Test
  public void testIndexPlusSort8() {
    String className = "testIndexPlusSort8";
    OClass clazz = db.getMetadata().getSchema().createClass(className);
    clazz.createProperty("name", OType.STRING);
    clazz.createProperty("surname", OType.STRING);
    db.command(
            "create index "
                + className
                + ".name_surname on "
                + className
                + " (name, surname) NOTUNIQUE")
        .close();

    for (int i = 0; i < 10; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("name", "name" + i % 3);
      doc.setProperty("surname", "surname" + i);
      doc.save();
    }

    OResultSet result =
        db.query(
            "select from " + className + " where name = 'name1' order by name ASC, surname DESC");
    printExecutionPlan(result);
    for (int i = 0; i < 3; i++) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Assert.assertNotNull(item);
      Assert.assertNotNull(item.getProperty("surname"));
    }
    Assert.assertFalse(result.hasNext());
    Assert.assertFalse(result.hasNext());
    boolean orderStepFound = false;
    for (OExecutionStep step : result.getExecutionPlan().get().getSteps()) {
      if (step instanceof OrderByStep) {
        orderStepFound = true;
        break;
      }
    }
    Assert.assertTrue(orderStepFound);
    result.close();
  }

  @Test
  public void testIndexPlusSort9() {
    String className = "testIndexPlusSort9";
    OClass clazz = db.getMetadata().getSchema().createClass(className);
    clazz.createProperty("name", OType.STRING);
    clazz.createProperty("surname", OType.STRING);
    db.command(
            "create index "
                + className
                + ".name_surname on "
                + className
                + " (name, surname) NOTUNIQUE")
        .close();

    for (int i = 0; i < 10; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("name", "name" + i % 3);
      doc.setProperty("surname", "surname" + i);
      doc.save();
    }

    OResultSet result = db.query("select from " + className + " order by name , surname ASC");
    printExecutionPlan(result);
    for (int i = 0; i < 10; i++) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Assert.assertNotNull(item);
      Assert.assertNotNull(item.getProperty("surname"));
    }
    Assert.assertFalse(result.hasNext());
    Assert.assertFalse(result.hasNext());
    boolean orderStepFound = false;
    for (OExecutionStep step : result.getExecutionPlan().get().getSteps()) {
      if (step instanceof OrderByStep) {
        orderStepFound = true;
        break;
      }
    }
    Assert.assertFalse(orderStepFound);
    result.close();
  }

  @Test
  public void testIndexPlusSort10() {
    String className = "testIndexPlusSort10";
    OClass clazz = db.getMetadata().getSchema().createClass(className);
    clazz.createProperty("name", OType.STRING);
    clazz.createProperty("surname", OType.STRING);
    db.command(
            "create index "
                + className
                + ".name_surname on "
                + className
                + " (name, surname) NOTUNIQUE")
        .close();

    for (int i = 0; i < 10; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("name", "name" + i % 3);
      doc.setProperty("surname", "surname" + i);
      doc.save();
    }

    OResultSet result = db.query("select from " + className + " order by name desc, surname desc");
    printExecutionPlan(result);
    for (int i = 0; i < 10; i++) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Assert.assertNotNull(item);
      Assert.assertNotNull(item.getProperty("surname"));
    }
    Assert.assertFalse(result.hasNext());
    Assert.assertFalse(result.hasNext());
    boolean orderStepFound = false;
    for (OExecutionStep step : result.getExecutionPlan().get().getSteps()) {
      if (step instanceof OrderByStep) {
        orderStepFound = true;
        break;
      }
    }
    Assert.assertFalse(orderStepFound);
    result.close();
  }

  @Test
  public void testIndexPlusSort11() {
    String className = "testIndexPlusSort11";
    OClass clazz = db.getMetadata().getSchema().createClass(className);
    clazz.createProperty("name", OType.STRING);
    clazz.createProperty("surname", OType.STRING);
    db.command(
            "create index "
                + className
                + ".name_surname on "
                + className
                + " (name, surname) NOTUNIQUE")
        .close();

    for (int i = 0; i < 10; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("name", "name" + i % 3);
      doc.setProperty("surname", "surname" + i);
      doc.save();
    }

    OResultSet result = db.query("select from " + className + " order by name asc, surname desc");
    printExecutionPlan(result);
    for (int i = 0; i < 10; i++) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Assert.assertNotNull(item);
      Assert.assertNotNull(item.getProperty("surname"));
    }
    Assert.assertFalse(result.hasNext());
    Assert.assertFalse(result.hasNext());
    boolean orderStepFound = false;
    for (OExecutionStep step : result.getExecutionPlan().get().getSteps()) {
      if (step instanceof OrderByStep) {
        orderStepFound = true;
        break;
      }
    }
    Assert.assertTrue(orderStepFound);
    result.close();
  }

  @Test
  public void testIndexPlusSort12() {
    String className = "testIndexPlusSort12";
    OClass clazz = db.getMetadata().getSchema().createClass(className);
    clazz.createProperty("name", OType.STRING);
    clazz.createProperty("surname", OType.STRING);
    db.command(
            "create index "
                + className
                + ".name_surname on "
                + className
                + " (name, surname) NOTUNIQUE")
        .close();

    for (int i = 0; i < 10; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("name", "name" + i % 3);
      doc.setProperty("surname", "surname" + i);
      doc.save();
    }

    OResultSet result = db.query("select from " + className + " order by name");
    printExecutionPlan(result);
    String last = null;
    for (int i = 0; i < 10; i++) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Assert.assertNotNull(item);
      Assert.assertNotNull(item.getProperty("name"));
      String name = item.getProperty("name");
      if (i > 0) {
        Assert.assertTrue(name.compareTo(last) >= 0);
      }
      last = name;
    }
    Assert.assertFalse(result.hasNext());
    Assert.assertFalse(result.hasNext());
    boolean orderStepFound = false;
    for (OExecutionStep step : result.getExecutionPlan().get().getSteps()) {
      if (step instanceof OrderByStep) {
        orderStepFound = true;
        break;
      }
    }
    Assert.assertFalse(orderStepFound);
    result.close();
  }

  @Test
  public void testSelectFromStringParam() {
    String className = "testSelectFromStringParam";
    db.getMetadata().getSchema().createClass(className);
    for (int i = 0; i < 10; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("name", "name" + i);
      doc.save();
    }
    OResultSet result = db.query("select from ?", className);
    printExecutionPlan(result);

    for (int i = 0; i < 10; i++) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Assert.assertNotNull(item);
      Assert.assertTrue(("" + item.getProperty("name")).startsWith("name"));
    }
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testSelectFromStringNamedParam() {
    String className = "testSelectFromStringNamedParam";
    db.getMetadata().getSchema().createClass(className);
    for (int i = 0; i < 10; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("name", "name" + i);
      doc.save();
    }
    Map<Object, Object> params = new HashMap<>();
    params.put("target", className);
    OResultSet result = db.query("select from :target", params);
    printExecutionPlan(result);

    for (int i = 0; i < 10; i++) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Assert.assertNotNull(item);
      Assert.assertTrue(("" + item.getProperty("name")).startsWith("name"));
    }
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testMatches() {
    String className = "testMatches";
    db.getMetadata().getSchema().createClass(className);
    for (int i = 0; i < 10; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("name", "name" + i);
      doc.save();
    }
    OResultSet result = db.query("select from " + className + " where name matches 'name1'");
    printExecutionPlan(result);

    for (int i = 0; i < 1; i++) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Assert.assertNotNull(item);
      Assert.assertEquals(item.getProperty("name"), "name1");
    }
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testRange() {
    String className = "testRange";
    db.getMetadata().getSchema().createClass(className);

    ODocument doc = db.newInstance(className);
    doc.setProperty("name", new String[] {"a", "b", "c", "d"});
    doc.save();

    OResultSet result = db.query("select name[0..3] as names from " + className);
    printExecutionPlan(result);

    for (int i = 0; i < 1; i++) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Assert.assertNotNull(item);
      Object names = item.getProperty("names");
      if (names == null) {
        Assert.fail();
      }
      if (names instanceof Collection) {
        Assert.assertEquals(3, ((Collection) names).size());
        Iterator iter = ((Collection) names).iterator();
        Assert.assertEquals("a", iter.next());
        Assert.assertEquals("b", iter.next());
        Assert.assertEquals("c", iter.next());
      } else if (names.getClass().isArray()) {
        Assert.assertEquals(3, Array.getLength(names));
      } else {
        Assert.fail();
      }
    }
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testRangeParams1() {
    String className = "testRangeParams1";
    db.getMetadata().getSchema().createClass(className);

    ODocument doc = db.newInstance(className);
    doc.setProperty("name", new String[] {"a", "b", "c", "d"});
    doc.save();

    OResultSet result = db.query("select name[?..?] as names from " + className, 0, 3);
    printExecutionPlan(result);

    for (int i = 0; i < 1; i++) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Assert.assertNotNull(item);
      Object names = item.getProperty("names");
      if (names == null) {
        Assert.fail();
      }
      if (names instanceof Collection) {
        Assert.assertEquals(3, ((Collection) names).size());
        Iterator iter = ((Collection) names).iterator();
        Assert.assertEquals("a", iter.next());
        Assert.assertEquals("b", iter.next());
        Assert.assertEquals("c", iter.next());
      } else if (names.getClass().isArray()) {
        Assert.assertEquals(3, Array.getLength(names));
      } else {
        Assert.fail();
      }
    }
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testRangeParams2() {
    String className = "testRangeParams2";
    db.getMetadata().getSchema().createClass(className);

    ODocument doc = db.newInstance(className);
    doc.setProperty("name", new String[] {"a", "b", "c", "d"});
    doc.save();

    Map<String, Object> params = new HashMap<>();
    params.put("a", 0);
    params.put("b", 3);
    OResultSet result = db.query("select name[:a..:b] as names from " + className, params);
    printExecutionPlan(result);

    for (int i = 0; i < 1; i++) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Assert.assertNotNull(item);
      Object names = item.getProperty("names");
      if (names == null) {
        Assert.fail();
      }
      if (names instanceof Collection) {
        Assert.assertEquals(3, ((Collection) names).size());
        Iterator iter = ((Collection) names).iterator();
        Assert.assertEquals("a", iter.next());
        Assert.assertEquals("b", iter.next());
        Assert.assertEquals("c", iter.next());
      } else if (names.getClass().isArray()) {
        Assert.assertEquals(3, Array.getLength(names));
      } else {
        Assert.fail();
      }
    }
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testEllipsis() {
    String className = "testEllipsis";
    db.getMetadata().getSchema().createClass(className);

    ODocument doc = db.newInstance(className);
    doc.setProperty("name", new String[] {"a", "b", "c", "d"});
    doc.save();

    OResultSet result = db.query("select name[0...2] as names from " + className);
    printExecutionPlan(result);

    for (int i = 0; i < 1; i++) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Assert.assertNotNull(item);
      Object names = item.getProperty("names");
      if (names == null) {
        Assert.fail();
      }
      if (names instanceof Collection) {
        Assert.assertEquals(3, ((Collection) names).size());
        Iterator iter = ((Collection) names).iterator();
        Assert.assertEquals("a", iter.next());
        Assert.assertEquals("b", iter.next());
        Assert.assertEquals("c", iter.next());
      } else if (names.getClass().isArray()) {
        Assert.assertEquals(3, Array.getLength(names));
        Assert.assertEquals("a", Array.get(names, 0));
        Assert.assertEquals("b", Array.get(names, 1));
        Assert.assertEquals("c", Array.get(names, 2));
      } else {
        Assert.fail();
      }
    }
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testNewRid() {
    OResultSet result = db.query("select {\"@rid\":\"#12:0\"} as theRid ");
    Assert.assertTrue(result.hasNext());
    OResult item = result.next();
    Object rid = item.getProperty("theRid");
    Assert.assertTrue(rid instanceof OIdentifiable);
    OIdentifiable id = (OIdentifiable) rid;
    Assert.assertEquals(12, id.getIdentity().getClusterId());
    Assert.assertEquals(0L, id.getIdentity().getClusterPosition());
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testNestedProjections1() {
    String className = "testNestedProjections1";
    db.command("create class " + className).close();
    OElement elem1 = db.newElement(className);
    elem1.setProperty("name", "a");
    elem1.save();

    OElement elem2 = db.newElement(className);
    elem2.setProperty("name", "b");
    elem2.setProperty("surname", "lkj");
    elem2.save();

    OElement elem3 = db.newElement(className);
    elem3.setProperty("name", "c");
    elem3.save();

    OElement elem4 = db.newElement(className);
    elem4.setProperty("name", "d");
    elem4.setProperty("elem1", elem1);
    elem4.setProperty("elem2", elem2);
    elem4.setProperty("elem3", elem3);
    elem4.save();

    OResultSet result =
        db.query(
            "select name, elem1:{*}, elem2:{!surname} from " + className + " where name = 'd'");
    Assert.assertTrue(result.hasNext());
    OResult item = result.next();
    Assert.assertNotNull(item);
    // TODO refine this!
    Assert.assertTrue(item.getProperty("elem1") instanceof OResult);
    Assert.assertEquals("a", ((OResult) item.getProperty("elem1")).getProperty("name"));
    printExecutionPlan(result);

    result.close();
  }

  @Test
  public void testSimpleCollectionFiltering() {
    String className = "testSimpleCollectionFiltering";
    db.command("create class " + className).close();
    OElement elem1 = db.newElement(className);
    List<String> coll = new ArrayList<>();
    coll.add("foo");
    coll.add("bar");
    coll.add("baz");
    elem1.setProperty("coll", coll);
    elem1.save();

    OResultSet result = db.query("select coll[='foo'] as filtered from " + className);
    Assert.assertTrue(result.hasNext());
    OResult item = result.next();
    List res = item.getProperty("filtered");
    Assert.assertEquals(1, res.size());
    Assert.assertEquals("foo", res.get(0));
    result.close();

    result = db.query("select coll[<'ccc'] as filtered from " + className);
    Assert.assertTrue(result.hasNext());
    item = result.next();
    res = item.getProperty("filtered");
    Assert.assertEquals(2, res.size());
    result.close();

    result = db.query("select coll[LIKE 'ba%'] as filtered from " + className);
    Assert.assertTrue(result.hasNext());
    item = result.next();
    res = item.getProperty("filtered");
    Assert.assertEquals(2, res.size());
    result.close();

    result = db.query("select coll[in ['bar']] as filtered from " + className);
    Assert.assertTrue(result.hasNext());
    item = result.next();
    res = item.getProperty("filtered");
    Assert.assertEquals(1, res.size());
    Assert.assertEquals("bar", res.get(0));
    result.close();
  }

  @Test
  public void testContaninsWithConversion() {
    String className = "testContaninsWithConversion";
    db.command("create class " + className).close();
    OElement elem1 = db.newElement(className);
    List<Long> coll = new ArrayList<>();
    coll.add(1L);
    coll.add(3L);
    coll.add(5L);
    elem1.setProperty("coll", coll);
    elem1.save();

    OElement elem2 = db.newElement(className);
    coll = new ArrayList<>();
    coll.add(2L);
    coll.add(4L);
    coll.add(6L);
    elem2.setProperty("coll", coll);
    elem2.save();

    OResultSet result = db.query("select from " + className + " where coll contains 1");
    Assert.assertTrue(result.hasNext());
    result.next();
    Assert.assertFalse(result.hasNext());
    result.close();

    result = db.query("select from " + className + " where coll contains 1L");
    Assert.assertTrue(result.hasNext());
    result.next();
    Assert.assertFalse(result.hasNext());
    result.close();

    result = db.query("select from " + className + " where coll contains 12L");
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testIndexPrefixUsage() {
    // issue #7636
    String className = "testIndexPrefixUsage";
    db.command("create class " + className).close();
    db.command("create property " + className + ".id LONG").close();
    db.command("create property " + className + ".name STRING").close();
    db.command("create index " + className + ".id_name on " + className + "(id, name) UNIQUE")
        .close();
    db.command("insert into " + className + " set id = 1 , name = 'Bar'").close();

    OResultSet result = db.query("select from " + className + " where name = 'Bar'");
    Assert.assertTrue(result.hasNext());
    result.next();
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testNamedParams() {
    String className = "testNamedParams";
    db.command("create class " + className).close();
    db.command("insert into " + className + " set name = 'Foo', surname = 'Fox'").close();
    db.command("insert into " + className + " set name = 'Bar', surname = 'Bax'").close();

    Map<String, Object> params = new HashMap<>();
    params.put("p1", "Foo");
    params.put("p2", "Fox");
    OResultSet result =
        db.query("select from " + className + " where name = :p1 and surname = :p2", params);
    Assert.assertTrue(result.hasNext());
    result.next();
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testNamedParamsWithIndex() {
    String className = "testNamedParamsWithIndex";
    db.command("create class " + className).close();
    db.command("create property " + className + ".name STRING").close();
    db.command("create index " + className + ".name ON " + className + " (name) NOTUNIQUE").close();
    db.command("insert into " + className + " set name = 'Foo'").close();
    db.command("insert into " + className + " set name = 'Bar'").close();

    Map<String, Object> params = new HashMap<>();
    params.put("p1", "Foo");
    OResultSet result = db.query("select from " + className + " where name = :p1", params);
    Assert.assertTrue(result.hasNext());
    result.next();
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testIsDefined() {
    String className = "testIsDefined";
    db.command("create class " + className).close();
    db.command("insert into " + className + " set name = 'Foo'").close();
    db.command("insert into " + className + " set sur = 'Bar'").close();
    db.command("insert into " + className + " set sur = 'Barz'").close();

    OResultSet result = db.query("select from " + className + " where name is defined");
    Assert.assertTrue(result.hasNext());
    result.next();
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testIsNotDefined() {
    String className = "testIsNotDefined";
    db.command("create class " + className).close();
    db.command("insert into " + className + " set name = 'Foo'").close();
    db.command("insert into " + className + " set name = null, sur = 'Bar'").close();
    db.command("insert into " + className + " set sur = 'Barz'").close();

    OResultSet result = db.query("select from " + className + " where name is not defined");
    Assert.assertTrue(result.hasNext());
    result.next();
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testRidPagination1() {
    String className = "testRidPagination1";
    OClass clazz = db.createClassIfNotExist(className);
    int[] clusterIds = new int[clazz.getClusterIds().length];
    if (clusterIds.length < 3) {
      return;
    }
    System.arraycopy(clazz.getClusterIds(), 0, clusterIds, 0, clusterIds.length);
    Arrays.sort(clusterIds);

    for (int i = 0; i < clusterIds.length; i++) {
      OElement elem = db.newElement(className);
      elem.setProperty("cid", clusterIds[i]);
      elem.save(db.getClusterNameById(clusterIds[i]));
    }

    OResultSet result =
        db.query("select from " + className + " where @rid >= #" + clusterIds[1] + ":0");
    OExecutionPlan execPlan = result.getExecutionPlan().get();
    for (OExecutionStep oExecutionStep : execPlan.getSteps()) {
      if (oExecutionStep instanceof FetchFromClassExecutionStep) {
        Assert.assertEquals(clusterIds.length, oExecutionStep.getSubSteps().size());
        // clusters - 1 + fetch from tx...
      }
    }
    int count = 0;
    while (result.hasNext()) {
      count++;
      result.next();
    }
    result.close();
    Assert.assertEquals(clusterIds.length - 1, count);
  }

  @Test
  public void testRidPagination2() {
    String className = "testRidPagination2";
    OClass clazz = db.createClassIfNotExist(className);
    int[] clusterIds = new int[clazz.getClusterIds().length];
    if (clusterIds.length < 3) {
      return;
    }
    System.arraycopy(clazz.getClusterIds(), 0, clusterIds, 0, clusterIds.length);
    Arrays.sort(clusterIds);

    for (int i = 0; i < clusterIds.length; i++) {
      OElement elem = db.newElement(className);
      elem.setProperty("cid", clusterIds[i]);
      elem.save(db.getClusterNameById(clusterIds[i]));
    }

    Map<String, Object> params = new HashMap<>();
    params.put("rid", new ORecordId(clusterIds[1], 0));
    OResultSet result = db.query("select from " + className + " where @rid >= :rid", params);
    OExecutionPlan execPlan = result.getExecutionPlan().get();
    for (OExecutionStep oExecutionStep : execPlan.getSteps()) {
      if (oExecutionStep instanceof FetchFromClassExecutionStep) {
        Assert.assertEquals(clusterIds.length, oExecutionStep.getSubSteps().size());
        // clusters - 1 + fetch from tx...
      }
    }
    int count = 0;
    while (result.hasNext()) {
      count++;
      result.next();
    }
    result.close();
    Assert.assertEquals(clusterIds.length - 1, count);
  }

  @Test
  public void testContainsWithSubquery() {
    String className = "testContainsWithSubquery";
    OClass clazz1 = db.createClassIfNotExist(className + 1);
    OClass clazz2 = db.createClassIfNotExist(className + 2);
    clazz2.createProperty("tags", OType.EMBEDDEDLIST);

    db.command("insert into " + className + 1 + "  set name = 'foo'");

    db.command("insert into " + className + 2 + "  set tags = ['foo', 'bar']");
    db.command("insert into " + className + 2 + "  set tags = ['baz', 'bar']");
    db.command("insert into " + className + 2 + "  set tags = ['foo']");

    try (OResultSet result =
        db.query(
            "select from "
                + className
                + 2
                + " where tags contains (select from "
                + className
                + 1
                + " where name = 'foo')")) {

      Assert.assertTrue(result.hasNext());
      result.next();
      Assert.assertTrue(result.hasNext());
      result.next();
      Assert.assertFalse(result.hasNext());
    }
  }

  @Test
  public void testInWithSubquery() {
    String className = "testInWithSubquery";
    OClass clazz1 = db.createClassIfNotExist(className + 1);
    OClass clazz2 = db.createClassIfNotExist(className + 2);
    clazz2.createProperty("tags", OType.EMBEDDEDLIST);

    db.command("insert into " + className + 1 + "  set name = 'foo'");

    db.command("insert into " + className + 2 + "  set tags = ['foo', 'bar']");
    db.command("insert into " + className + 2 + "  set tags = ['baz', 'bar']");
    db.command("insert into " + className + 2 + "  set tags = ['foo']");

    try (OResultSet result =
        db.query(
            "select from "
                + className
                + 2
                + " where (select from "
                + className
                + 1
                + " where name = 'foo') in tags")) {

      Assert.assertTrue(result.hasNext());
      result.next();
      Assert.assertTrue(result.hasNext());
      result.next();
      Assert.assertFalse(result.hasNext());
    }
  }

  @Test
  public void testContainsAny() {
    String className = "testContainsAny";
    OClass clazz = db.createClassIfNotExist(className);
    clazz.createProperty("tags", OType.EMBEDDEDLIST, OType.STRING);

    db.command("insert into " + className + "  set tags = ['foo', 'bar']");
    db.command("insert into " + className + "  set tags = ['bbb', 'FFF']");

    try (OResultSet result =
        db.query("select from " + className + " where tags containsany ['foo','baz']")) {
      Assert.assertTrue(result.hasNext());
      result.next();
      Assert.assertFalse(result.hasNext());
    }

    try (OResultSet result =
        db.query("select from " + className + " where tags containsany ['foo','bar']")) {
      Assert.assertTrue(result.hasNext());
      result.next();
      Assert.assertFalse(result.hasNext());
    }

    try (OResultSet result =
        db.query("select from " + className + " where tags containsany ['foo','bbb']")) {
      Assert.assertTrue(result.hasNext());
      result.next();
      Assert.assertTrue(result.hasNext());
      result.next();
      Assert.assertFalse(result.hasNext());
    }

    try (OResultSet result =
        db.query("select from " + className + " where tags containsany ['xx','baz']")) {
      Assert.assertFalse(result.hasNext());
    }

    try (OResultSet result = db.query("select from " + className + " where tags containsany []")) {
      Assert.assertFalse(result.hasNext());
    }
  }

  @Test
  public void testContainsAnyWithIndex() {
    String className = "testContainsAnyWithIndex";
    OClass clazz = db.createClassIfNotExist(className);
    OProperty prop = clazz.createProperty("tags", OType.EMBEDDEDLIST, OType.STRING);
    prop.createIndex(OClass.INDEX_TYPE.NOTUNIQUE);

    db.command("insert into " + className + "  set tags = ['foo', 'bar']");
    db.command("insert into " + className + "  set tags = ['bbb', 'FFF']");

    try (OResultSet result =
        db.query("select from " + className + " where tags containsany ['foo','baz']")) {
      Assert.assertTrue(result.hasNext());
      result.next();
      Assert.assertFalse(result.hasNext());
      Assert.assertTrue(
          result.getExecutionPlan().get().getSteps().stream()
              .anyMatch(x -> x instanceof FetchFromIndexStep));
    }

    try (OResultSet result =
        db.query("select from " + className + " where tags containsany ['foo','bar']")) {
      Assert.assertTrue(result.hasNext());
      result.next();
      Assert.assertFalse(result.hasNext());
      Assert.assertTrue(
          result.getExecutionPlan().get().getSteps().stream()
              .anyMatch(x -> x instanceof FetchFromIndexStep));
    }

    try (OResultSet result =
        db.query("select from " + className + " where tags containsany ['foo','bbb']")) {
      Assert.assertTrue(result.hasNext());
      result.next();
      Assert.assertTrue(result.hasNext());
      result.next();
      Assert.assertFalse(result.hasNext());
      Assert.assertTrue(
          result.getExecutionPlan().get().getSteps().stream()
              .anyMatch(x -> x instanceof FetchFromIndexStep));
    }

    try (OResultSet result =
        db.query("select from " + className + " where tags containsany ['xx','baz']")) {
      Assert.assertFalse(result.hasNext());
      Assert.assertTrue(
          result.getExecutionPlan().get().getSteps().stream()
              .anyMatch(x -> x instanceof FetchFromIndexStep));
    }

    try (OResultSet result = db.query("select from " + className + " where tags containsany []")) {
      Assert.assertFalse(result.hasNext());
      Assert.assertTrue(
          result.getExecutionPlan().get().getSteps().stream()
              .anyMatch(x -> x instanceof FetchFromIndexStep));
    }
  }

  @Test
  public void testContainsAll() {
    String className = "testContainsAll";
    OClass clazz = db.createClassIfNotExist(className);
    clazz.createProperty("tags", OType.EMBEDDEDLIST, OType.STRING);

    db.command("insert into " + className + "  set tags = ['foo', 'bar']");
    db.command("insert into " + className + "  set tags = ['foo', 'FFF']");

    try (OResultSet result =
        db.query("select from " + className + " where tags containsall ['foo','bar']")) {
      Assert.assertTrue(result.hasNext());
      result.next();
      Assert.assertFalse(result.hasNext());
    }

    try (OResultSet result =
        db.query("select from " + className + " where tags containsall ['foo']")) {
      Assert.assertTrue(result.hasNext());
      result.next();
      Assert.assertTrue(result.hasNext());
      result.next();
      Assert.assertFalse(result.hasNext());
    }
  }

  @Test
  public void testBetween() {
    String className = "testBetween";
    OClass clazz = db.createClassIfNotExist(className);

    db.command("insert into " + className + "  set name = 'foo1', val = 1");
    db.command("insert into " + className + "  set name = 'foo2', val = 2");
    db.command("insert into " + className + "  set name = 'foo3', val = 3");
    db.command("insert into " + className + "  set name = 'foo4', val = 4");

    try (OResultSet result = db.query("select from " + className + " where val between 2 and 3")) {
      Assert.assertTrue(result.hasNext());
      result.next();
      Assert.assertTrue(result.hasNext());
      result.next();
      Assert.assertFalse(result.hasNext());
    }
  }

  @Test
  public void testInWithIndex() {
    String className = "testInWithIndex";
    OClass clazz = db.createClassIfNotExist(className);
    OProperty prop = clazz.createProperty("tag", OType.STRING);
    prop.createIndex(OClass.INDEX_TYPE.NOTUNIQUE);

    db.command("insert into " + className + "  set tag = 'foo'");
    db.command("insert into " + className + "  set tag = 'bar'");

    try (OResultSet result = db.query("select from " + className + " where tag in ['foo','baz']")) {
      Assert.assertTrue(result.hasNext());
      result.next();
      Assert.assertFalse(result.hasNext());
      Assert.assertTrue(
          result.getExecutionPlan().get().getSteps().stream()
              .anyMatch(x -> x instanceof FetchFromIndexStep));
    }

    try (OResultSet result = db.query("select from " + className + " where tag in ['foo','bar']")) {
      Assert.assertTrue(result.hasNext());
      result.next();
      Assert.assertTrue(result.hasNext());
      result.next();
      Assert.assertFalse(result.hasNext());
      Assert.assertTrue(
          result.getExecutionPlan().get().getSteps().stream()
              .anyMatch(x -> x instanceof FetchFromIndexStep));
    }

    try (OResultSet result = db.query("select from " + className + " where tag in []")) {
      Assert.assertFalse(result.hasNext());
      Assert.assertTrue(
          result.getExecutionPlan().get().getSteps().stream()
              .anyMatch(x -> x instanceof FetchFromIndexStep));
    }

    List<String> params = new ArrayList<>();
    params.add("foo");
    params.add("bar");
    try (OResultSet result = db.query("select from " + className + " where tag in (?)", params)) {
      Assert.assertTrue(result.hasNext());
      result.next();
      Assert.assertTrue(result.hasNext());
      result.next();
      Assert.assertFalse(result.hasNext());
      Assert.assertTrue(
          result.getExecutionPlan().get().getSteps().stream()
              .anyMatch(x -> x instanceof FetchFromIndexStep));
    }
  }

  @Test
  public void testIndexChain() {
    String className1 = "testIndexChain1";
    String className2 = "testIndexChain2";
    String className3 = "testIndexChain3";

    OClass clazz3 = db.createClassIfNotExist(className3);
    OProperty prop = clazz3.createProperty("name", OType.STRING);
    prop.createIndex(OClass.INDEX_TYPE.NOTUNIQUE);

    OClass clazz2 = db.createClassIfNotExist(className2);
    prop = clazz2.createProperty("next", OType.LINK, clazz3);
    prop.createIndex(OClass.INDEX_TYPE.NOTUNIQUE);

    OClass clazz1 = db.createClassIfNotExist(className1);
    prop = clazz1.createProperty("next", OType.LINK, clazz2);
    prop.createIndex(OClass.INDEX_TYPE.NOTUNIQUE);

    OElement elem3 = db.newElement(className3);
    elem3.setProperty("name", "John");
    elem3.save();

    OElement elem2 = db.newElement(className2);
    elem2.setProperty("next", elem3);
    elem2.save();

    OElement elem1 = db.newElement(className1);
    elem1.setProperty("next", elem2);
    elem1.setProperty("name", "right");
    elem1.save();

    elem1 = db.newElement(className1);
    elem1.setProperty("name", "wrong");
    elem1.save();

    try (OResultSet result =
        db.query("select from " + className1 + " where next.next.name = ?", "John")) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Assert.assertEquals("right", item.getProperty("name"));
      Assert.assertFalse(result.hasNext());
      Assert.assertTrue(
          result.getExecutionPlan().get().getSteps().stream()
              .anyMatch(x -> x instanceof FetchFromIndexStep));
    }
  }

  @Test
  public void testIndexChainWithContainsAny() {
    String className1 = "testIndexChainWithContainsAny1";
    String className2 = "testIndexChainWithContainsAny2";
    String className3 = "testIndexChainWithContainsAny3";

    OClass clazz3 = db.createClassIfNotExist(className3);
    OProperty prop = clazz3.createProperty("name", OType.STRING);
    prop.createIndex(OClass.INDEX_TYPE.NOTUNIQUE);

    OClass clazz2 = db.createClassIfNotExist(className2);
    prop = clazz2.createProperty("next", OType.LINKSET, clazz3);
    prop.createIndex(OClass.INDEX_TYPE.NOTUNIQUE);

    OClass clazz1 = db.createClassIfNotExist(className1);
    prop = clazz1.createProperty("next", OType.LINKSET, clazz2);
    prop.createIndex(OClass.INDEX_TYPE.NOTUNIQUE);

    OElement elem3 = db.newElement(className3);
    elem3.setProperty("name", "John");
    elem3.save();

    OElement elemFoo = db.newElement(className3);
    elemFoo.setProperty("foo", "bar");
    elemFoo.save();

    OElement elem2 = db.newElement(className2);
    List<OElement> elems3 = new ArrayList<>();
    elems3.add(elem3);
    elems3.add(elemFoo);
    elem2.setProperty("next", elems3);
    elem2.save();

    OElement elem1 = db.newElement(className1);
    List<OElement> elems2 = new ArrayList<>();
    elems2.add(elem2);
    elem1.setProperty("next", elems2);
    elem1.setProperty("name", "right");
    elem1.save();

    elem1 = db.newElement(className1);
    elem1.setProperty("name", "wrong");
    elem1.save();

    try (OResultSet result =
        db.query("select from " + className1 + " where next.next.name CONTAINSANY ['John']")) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Assert.assertEquals("right", item.getProperty("name"));
      Assert.assertFalse(result.hasNext());
      Assert.assertTrue(
          result.getExecutionPlan().get().getSteps().stream()
              .anyMatch(x -> x instanceof FetchFromIndexStep));
    }
  }

  @Test
  public void testQueryView() throws InterruptedException {
    String className = "testQueryView_Class";
    String viewName = "testQueryView_View";
    db.createClass(className);
    for (int i = 0; i < 10; i++) {
      OElement elem = db.newElement(className);
      elem.setProperty("counter", i);
      elem.save();
    }

    OViewConfig cfg = new OViewConfig(viewName, "SELECT FROM " + className);
    final CountDownLatch latch = new CountDownLatch(1);
    db.getMetadata()
        .getSchema()
        .createView(
            cfg,
            new ViewCreationListener() {

              @Override
              public void afterCreate(ODatabaseSession database, String viewName) {
                latch.countDown();
              }

              @Override
              public void onError(String viewName, Exception exception) {
                latch.countDown();
              }
            });

    latch.await();

    OResultSet result = db.query("SELECT FROM " + viewName);
    int count =
        result.stream().map(x -> (Integer) x.getProperty("counter")).reduce((x, y) -> x + y).get();
    Assert.assertEquals(45, count);
    result.close();
  }

  @Test
  public void testMapByKeyIndex() {
    String className = "testMapByKeyIndex";

    OClass clazz1 = db.createClassIfNotExist(className);
    OProperty prop = clazz1.createProperty("themap", OType.EMBEDDEDMAP);

    db.command(
        "CREATE INDEX " + className + ".themap ON " + className + "(themap by key) NOTUNIQUE");

    for (int i = 0; i < 100; i++) {
      Map<String, Object> theMap = new HashMap<>();
      theMap.put("key" + i, "val" + i);
      OElement elem1 = db.newElement(className);
      elem1.setProperty("themap", theMap);
      elem1.save();
    }

    try (OResultSet result =
        db.query("select from " + className + " where themap CONTAINSKEY ?", "key10")) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Map<String, Object> map = item.getProperty("themap");
      Assert.assertEquals("key10", map.keySet().iterator().next());
      Assert.assertFalse(result.hasNext());
      Assert.assertTrue(
          result.getExecutionPlan().get().getSteps().stream()
              .anyMatch(x -> x instanceof FetchFromIndexStep));
    }
  }

  @Test
  public void testMapByKeyIndexMultiple() {
    String className = "testMapByKeyIndexMultiple";

    OClass clazz1 = db.createClassIfNotExist(className);
    clazz1.createProperty("themap", OType.EMBEDDEDMAP);
    clazz1.createProperty("thestring", OType.STRING);

    db.command(
        "CREATE INDEX "
            + className
            + ".themap_thestring ON "
            + className
            + "(themap by key, thestring) NOTUNIQUE");

    for (int i = 0; i < 100; i++) {
      Map<String, Object> theMap = new HashMap<>();
      theMap.put("key" + i, "val" + i);
      OElement elem1 = db.newElement(className);
      elem1.setProperty("themap", theMap);
      elem1.setProperty("thestring", "thestring" + i);
      elem1.save();
    }

    try (OResultSet result =
        db.query(
            "select from " + className + " where themap CONTAINSKEY ? AND thestring = ?",
            "key10",
            "thestring10")) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Map<String, Object> map = item.getProperty("themap");
      Assert.assertEquals("key10", map.keySet().iterator().next());
      Assert.assertFalse(result.hasNext());
      Assert.assertTrue(
          result.getExecutionPlan().get().getSteps().stream()
              .anyMatch(x -> x instanceof FetchFromIndexStep));
    }
  }

  @Test
  public void testMapByValueIndex() {
    String className = "testMapByValueIndex";

    OClass clazz1 = db.createClassIfNotExist(className);
    OProperty prop = clazz1.createProperty("themap", OType.EMBEDDEDMAP, OType.STRING);

    db.command(
        "CREATE INDEX " + className + ".themap ON " + className + "(themap by value) NOTUNIQUE");

    for (int i = 0; i < 100; i++) {
      Map<String, Object> theMap = new HashMap<>();
      theMap.put("key" + i, "val" + i);
      OElement elem1 = db.newElement(className);
      elem1.setProperty("themap", theMap);
      elem1.save();
    }

    try (OResultSet result =
        db.query("select from " + className + " where themap CONTAINSVALUE ?", "val10")) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Map<String, Object> map = item.getProperty("themap");
      Assert.assertEquals("key10", map.keySet().iterator().next());
      Assert.assertFalse(result.hasNext());
      Assert.assertTrue(
          result.getExecutionPlan().get().getSteps().stream()
              .anyMatch(x -> x instanceof FetchFromIndexStep));
    }
  }

  @Test
  public void testListOfMapsContains() {
    String className = "testListOfMapsContains";

    OClass clazz1 = db.createClassIfNotExist(className);
    OProperty prop = clazz1.createProperty("thelist", OType.EMBEDDEDLIST, OType.EMBEDDEDMAP);

    db.command("INSERT INTO " + className + " SET thelist = [{name:\"Jack\"}]").close();
    db.command("INSERT INTO " + className + " SET thelist = [{name:\"Joe\"}]").close();

    try (OResultSet result =
        db.query("select from " + className + " where thelist CONTAINS ( name = ?)", "Jack")) {
      Assert.assertTrue(result.hasNext());
      result.next();
      Assert.assertFalse(result.hasNext());
    }
  }

  @Test
  public void testOrderByWithCollate() {
    String className = "testOrderByWithCollate";

    OClass clazz1 = db.createClassIfNotExist(className);

    db.command("INSERT INTO " + className + " SET name = 'A', idx = 0").close();
    db.command("INSERT INTO " + className + " SET name = 'C', idx = 2").close();
    db.command("INSERT INTO " + className + " SET name = 'E', idx = 4").close();
    db.command("INSERT INTO " + className + " SET name = 'b', idx = 1").close();
    db.command("INSERT INTO " + className + " SET name = 'd', idx = 3").close();

    try (OResultSet result =
        db.query("select from " + className + " order by name asc collate ci")) {
      for (int i = 0; i < 5; i++) {
        Assert.assertTrue(result.hasNext());
        OResult item = result.next();
        int val = item.getProperty("idx");
        Assert.assertEquals(i, val);
      }
      Assert.assertFalse(result.hasNext());
    }
  }

  @Test
  public void testContainsEmptyCollection() {
    String className = "testContainsEmptyCollection";

    db.createClassIfNotExist(className);

    db.command("INSERT INTO " + className + " content {\"name\": \"jack\", \"age\": 22}").close();
    db.command(
            "INSERT INTO "
                + className
                + " content {\"name\": \"rose\", \"age\": 22, \"test\": [[]]}")
        .close();
    db.command(
            "INSERT INTO "
                + className
                + " content {\"name\": \"rose\", \"age\": 22, \"test\": [[1]]}")
        .close();
    db.command(
            "INSERT INTO "
                + className
                + " content {\"name\": \"pete\", \"age\": 22, \"test\": [{}]}")
        .close();
    db.command(
            "INSERT INTO "
                + className
                + " content {\"name\": \"david\", \"age\": 22, \"test\": [\"hello\"]}")
        .close();

    try (OResultSet result = db.query("select from " + className + " where test contains []")) {
      Assert.assertTrue(result.hasNext());
      result.next();
      Assert.assertFalse(result.hasNext());
    }
  }

  @Test
  public void testContainsCollection() {
    String className = "testContainsCollection";

    db.createClassIfNotExist(className);

    db.command("INSERT INTO " + className + " content {\"name\": \"jack\", \"age\": 22}").close();
    db.command(
            "INSERT INTO "
                + className
                + " content {\"name\": \"rose\", \"age\": 22, \"test\": [[]]}")
        .close();
    db.command(
            "INSERT INTO "
                + className
                + " content {\"name\": \"rose\", \"age\": 22, \"test\": [[1]]}")
        .close();
    db.command(
            "INSERT INTO "
                + className
                + " content {\"name\": \"pete\", \"age\": 22, \"test\": [{}]}")
        .close();
    db.command(
            "INSERT INTO "
                + className
                + " content {\"name\": \"david\", \"age\": 22, \"test\": [\"hello\"]}")
        .close();

    try (OResultSet result = db.query("select from " + className + " where test contains [1]")) {
      Assert.assertTrue(result.hasNext());
      result.next();
      Assert.assertFalse(result.hasNext());
    }
  }

  @Test
  public void testHeapLimitForOrderBy() {
    Long oldValue = OGlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.getValueAsLong();
    try {
      OGlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.setValue(3);

      String className = "testHeapLimitForOrderBy";

      db.createClassIfNotExist(className);

      db.command("INSERT INTO " + className + " set name = 'a'").close();
      db.command("INSERT INTO " + className + " set name = 'b'").close();
      db.command("INSERT INTO " + className + " set name = 'c'").close();
      db.command("INSERT INTO " + className + " set name = 'd'").close();

      try {
        try (OResultSet result = db.query("select from " + className + " ORDER BY name")) {
          result.forEachRemaining(x -> x.getProperty("name"));
        }
        Assert.fail();
      } catch (OCommandExecutionException ex) {
      }
    } finally {
      OGlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.setValue(oldValue);
    }
  }

  @Test
  public void testXor() {
    try (OResultSet result = db.query("select 15 ^ 4 as foo")) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Assert.assertEquals(11, (int) item.getProperty("foo"));
      Assert.assertFalse(result.hasNext());
    }
  }

  @Test
  public void testLike() {
    String className = "testLike";

    db.createClassIfNotExist(className);

    db.command("INSERT INTO " + className + " content {\"name\": \"foobarbaz\"}").close();
    db.command("INSERT INTO " + className + " content {\"name\": \"test[]{}()|*^.test\"}").close();

    try (OResultSet result = db.query("select from " + className + " where name LIKE 'foo%'")) {
      Assert.assertTrue(result.hasNext());
      result.next();
      Assert.assertFalse(result.hasNext());
    }
    try (OResultSet result =
        db.query("select from " + className + " where name LIKE '%foo%baz%'")) {
      Assert.assertTrue(result.hasNext());
      result.next();
      Assert.assertFalse(result.hasNext());
    }
    try (OResultSet result = db.query("select from " + className + " where name LIKE '%bar%'")) {
      Assert.assertTrue(result.hasNext());
      result.next();
      Assert.assertFalse(result.hasNext());
    }

    try (OResultSet result = db.query("select from " + className + " where name LIKE 'bar%'")) {
      Assert.assertFalse(result.hasNext());
    }

    try (OResultSet result = db.query("select from " + className + " where name LIKE '%bar'")) {
      Assert.assertFalse(result.hasNext());
    }

    String specialChars = "[]{}()|*^.";
    for (char c : specialChars.toCharArray()) {
      try (OResultSet result =
          db.query("select from " + className + " where name LIKE '%" + c + "%'")) {
        Assert.assertTrue(result.hasNext());
        result.next();
        Assert.assertFalse(result.hasNext());
      }
    }
  }

  @Test
  public void testCountGroupBy() {
    // issue #9288
    String className = "testCountGroupBy";
    db.getMetadata().getSchema().createClass(className);
    for (int i = 0; i < 10; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("type", i % 2 == 0 ? "even" : "odd");
      doc.setProperty("val", i);
      doc.save();
    }
    OResultSet result = db.query("select count(val) as count from " + className + " limit 3");
    printExecutionPlan(result);

    Assert.assertTrue(result.hasNext());
    OResult item = result.next();
    Assert.assertEquals(10L, (long) item.getProperty("count"));
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testTimeout() {
    String className = "testTimeout";
    final String funcitonName = getClass().getSimpleName() + "_sleep";
    db.getMetadata().getSchema().createClass(className);

    OSQLEngine.getInstance()
        .registerFunction(
            funcitonName,
            new OSQLFunction() {

              @Override
              public Object execute(
                  Object iThis,
                  OIdentifiable iCurrentRecord,
                  Object iCurrentResult,
                  Object[] iParams,
                  OCommandContext iContext) {
                try {
                  Thread.sleep(5);
                } catch (InterruptedException e) {
                }
                return null;
              }

              @Override
              public void config(Object[] configuredParameters) {}

              @Override
              public boolean aggregateResults() {
                return false;
              }

              @Override
              public boolean filterResult() {
                return false;
              }

              @Override
              public String getName() {
                return funcitonName;
              }

              @Override
              public int getMinParams() {
                return 0;
              }

              @Override
              public int getMaxParams() {
                return 0;
              }

              @Override
              public String getSyntax() {
                return "";
              }

              @Override
              public Object getResult() {
                return null;
              }

              @Override
              public void setResult(Object iResult) {}

              @Override
              public boolean shouldMergeDistributedResult() {
                return false;
              }

              @Override
              public Object mergeDistributedResult(List<Object> resultsToMerge) {
                return null;
              }
            });
    for (int i = 0; i < 3; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("type", i % 2 == 0 ? "even" : "odd");
      doc.setProperty("val", i);
      doc.save();
    }
    try (OResultSet result =
        db.query("select " + funcitonName + "(), * from " + className + " timeout 1")) {
      while (result.hasNext()) {
        result.next();
      }
      Assert.fail();
    } catch (OTimeoutException ex) {

    }

    try (OResultSet result =
        db.query("select " + funcitonName + "(), * from " + className + " timeout 100")) {
      while (result.hasNext()) {
        result.next();
      }
    } catch (OTimeoutException ex) {
      Assert.fail();
    }
  }

  @Test
  public void testSimpleRangeQueryWithIndexGTE() {
    final String className = "testSimpleRangeQueryWithIndexGTE";
    final OClass clazz = db.getMetadata().getSchema().getOrCreateClass(className);
    final OProperty prop = clazz.createProperty("name", OType.STRING);
    prop.createIndex(OClass.INDEX_TYPE.NOTUNIQUE);

    for (int i = 0; i < 10; i++) {
      final ODocument doc = db.newInstance(className);
      doc.setProperty("name", "name" + i);
      doc.save();
    }
    final OResultSet result = db.query("select from " + className + " WHERE name >= 'name5'");
    printExecutionPlan(result);

    for (int i = 0; i < 5; i++) {
      Assert.assertTrue(result.hasNext());
      result.next();
    }
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testSimpleRangeQueryWithIndexLTE() {
    final String className = "testSimpleRangeQueryWithIndexLTE";
    final OClass clazz = db.getMetadata().getSchema().getOrCreateClass(className);
    final OProperty prop = clazz.createProperty("name", OType.STRING);
    prop.createIndex(OClass.INDEX_TYPE.NOTUNIQUE);

    for (int i = 0; i < 10; i++) {
      final ODocument doc = db.newInstance(className);
      doc.setProperty("name", "name" + i);
      doc.save();
    }
    final OResultSet result = db.query("select from " + className + " WHERE name <= 'name5'");
    printExecutionPlan(result);

    for (int i = 0; i < 6; i++) {
      Assert.assertTrue(result.hasNext());
      result.next();
    }
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testSimpleRangeQueryWithOutIndex() {
    final String className = "testSimpleRangeQueryWithOutIndex";
    final OClass clazz = db.getMetadata().getSchema().getOrCreateClass(className);
    final OProperty prop = clazz.createProperty("name", OType.STRING);
    // Hash Index skipped for range query
    prop.createIndex(OClass.INDEX_TYPE.NOTUNIQUE_HASH_INDEX);

    for (int i = 0; i < 10; i++) {
      final ODocument doc = db.newInstance(className);
      doc.setProperty("name", "name" + i);
      doc.save();
    }
    final OResultSet result = db.query("select from " + className + " WHERE name >= 'name5'");
    printExecutionPlan(result);

    for (int i = 0; i < 5; i++) {
      Assert.assertTrue(result.hasNext());
      result.next();
    }
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testComplexIndexChain() {

    // A -b-> B -c-> C -d-> D.name
    //               C.name

    String classNamePrefix = "testComplexIndexChain_";
    OClass a = db.getMetadata().getSchema().createClass(classNamePrefix + "A");
    OClass b = db.getMetadata().getSchema().createClass(classNamePrefix + "C");
    OClass c = db.getMetadata().getSchema().createClass(classNamePrefix + "B");
    OClass d = db.getMetadata().getSchema().createClass(classNamePrefix + "D");

    a.createProperty("b", OType.LINK, b).createIndex(OClass.INDEX_TYPE.NOTUNIQUE);
    b.createProperty("c", OType.LINK, c).createIndex(OClass.INDEX_TYPE.NOTUNIQUE);
    c.createProperty("d", OType.LINK, d).createIndex(OClass.INDEX_TYPE.NOTUNIQUE);
    c.createProperty("name", OType.STRING).createIndex(OClass.INDEX_TYPE.NOTUNIQUE);
    d.createProperty("name", OType.STRING).createIndex(OClass.INDEX_TYPE.NOTUNIQUE);

    OElement dDoc = db.newElement(d.getName());
    dDoc.setProperty("name", "foo");
    dDoc.save();

    OElement cDoc = db.newElement(c.getName());
    cDoc.setProperty("name", "foo");
    cDoc.setProperty("d", dDoc);
    cDoc.save();

    OElement bDoc = db.newElement(b.getName());
    bDoc.setProperty("c", cDoc);
    bDoc.save();

    OElement aDoc = db.newElement(a.getName());
    aDoc.setProperty("b", bDoc);
    aDoc.save();

    try (OResultSet rs =
        db.query(
            "SELECT FROM "
                + classNamePrefix
                + "A WHERE b.c.name IN ['foo'] AND b.c.d.name IN ['foo']")) {
      Assert.assertTrue(rs.hasNext());
    }

    try (OResultSet rs =
        db.query(
            "SELECT FROM " + classNamePrefix + "A WHERE b.c.name = 'foo' AND b.c.d.name = 'foo'")) {
      Assert.assertTrue(rs.hasNext());
      Assert.assertTrue(
          rs.getExecutionPlan().get().getSteps().stream()
              .anyMatch(x -> x instanceof FetchFromIndexStep));
    }
  }

  @Test
  public void testIndexWithSubquery() {
    String classNamePrefix = "testIndexWithSubquery_";
    db.command("create class " + classNamePrefix + "Ownership extends V abstract;").close();
    db.command("create class " + classNamePrefix + "User extends V;").close();
    db.command("create property " + classNamePrefix + "User.id String;").close();
    db.command(
            "create index "
                + classNamePrefix
                + "User.id ON "
                + classNamePrefix
                + "User(id) unique;")
        .close();
    db.command(
            "create class " + classNamePrefix + "Report extends " + classNamePrefix + "Ownership;")
        .close();
    db.command("create property " + classNamePrefix + "Report.id String;").close();
    db.command("create property " + classNamePrefix + "Report.label String;").close();
    db.command("create property " + classNamePrefix + "Report.format String;").close();
    db.command("create property " + classNamePrefix + "Report.source String;").close();
    db.command("create class " + classNamePrefix + "hasOwnership extends E;").close();
    db.command("insert into " + classNamePrefix + "User content {id:\"admin\"};");
    db.command(
            "insert into "
                + classNamePrefix
                + "Report content {format:\"PDF\", id:\"rep1\", label:\"Report 1\", source:\"Report1.src\"};")
        .close();
    db.command(
            "insert into "
                + classNamePrefix
                + "Report content {format:\"CSV\", id:\"rep2\", label:\"Report 2\", source:\"Report2.src\"};")
        .close();
    db.command(
            "create edge "
                + classNamePrefix
                + "hasOwnership from (select from "
                + classNamePrefix
                + "User) to (select from "
                + classNamePrefix
                + "Report);")
        .close();

    try (OResultSet rs =
        db.query(
            "select from "
                + classNamePrefix
                + "Report where id in (select out('"
                + classNamePrefix
                + "hasOwnership').id from "
                + classNamePrefix
                + "User where id = 'admin');")) {
      Assert.assertTrue(rs.hasNext());
      rs.next();
      Assert.assertTrue(rs.hasNext());
      rs.next();
      Assert.assertFalse(rs.hasNext());
    }

    db.command(
            "create index "
                + classNamePrefix
                + "Report.id ON "
                + classNamePrefix
                + "Report(id) unique;")
        .close();

    try (OResultSet rs =
        db.query(
            "select from "
                + classNamePrefix
                + "Report where id in (select out('"
                + classNamePrefix
                + "hasOwnership').id from "
                + classNamePrefix
                + "User where id = 'admin');")) {
      Assert.assertTrue(rs.hasNext());
      rs.next();
      Assert.assertTrue(rs.hasNext());
      rs.next();
      Assert.assertFalse(rs.hasNext());
    }
  }

  @Test
  public void testExclude() {
    String className = "TestExclude";
    db.getMetadata().getSchema().createClass(className);
    ODocument doc = db.newInstance(className);
    doc.setProperty("name", "foo");
    doc.setProperty("surname", "bar");
    doc.save();

    OResultSet result = db.query("select *, !surname from " + className);
    Assert.assertTrue(result.hasNext());
    OResult item = result.next();
    Assert.assertNotNull(item);
    Assert.assertEquals("foo", item.getProperty("name"));
    Assert.assertNull(item.getProperty("surname"));

    printExecutionPlan(result);
    result.close();
  }

  @Test
  public void testOrderByLet() {
    String className = "testOrderByLet";
    db.getMetadata().getSchema().createClass(className);
    ODocument doc = db.newInstance(className);
    doc.setProperty("name", "abbb");
    doc.save();

    doc = db.newInstance(className);
    doc.setProperty("name", "baaa");
    doc.save();

    try (OResultSet result =
        db.query(
            "select from "
                + className
                + " LET $order = name.substring(1) ORDER BY $order ASC LIMIT 1")) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Assert.assertNotNull(item);
      Assert.assertEquals("baaa", item.getProperty("name"));
    }
    try (OResultSet result =
        db.query(
            "select from "
                + className
                + " LET $order = name.substring(1) ORDER BY $order DESC LIMIT 1")) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Assert.assertNotNull(item);
      Assert.assertEquals("abbb", item.getProperty("name"));
    }
  }

  @Test
  public void testMapToJson() {
    String className = "testMapToJson";
    db.command("create class " + className).close();
    db.command("create property " + className + ".themap embeddedmap").close();
    db.command(
            "insert into "
                + className
                + " set name = 'foo', themap = {\"foo bar\":\"baz\", \"riz\":\"faz\"}")
        .close();
    try (OResultSet rs = db.query("select themap.tojson() as x from " + className)) {
      Assert.assertTrue(rs.hasNext());
      OResult item = rs.next();
      Assert.assertTrue(((String) item.getProperty("x")).contains("foo bar"));
    }
  }
}
