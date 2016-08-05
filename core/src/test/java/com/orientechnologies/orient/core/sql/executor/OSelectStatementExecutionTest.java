package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.*;

/**
 * @author Luigi Dell'Aquila
 */
public class OSelectStatementExecutionTest {
  static ODatabaseDocument db;

  @BeforeClass public static void beforeClass() {

    db = new ODatabaseDocumentTx("memory:OSelectStatementExecutionTest");
    db.create();
  }

  @AfterClass public static void afterClass() {
    db.close();
  }

  @Test public void testSelectNoTarget() {
    OTodoResultSet result = db.query("select 1 as one, 2 as two, 2+3");
    Assert.assertTrue(result.hasNext());
    OResult item = result.next();
    Assert.assertNotNull(item);
    Assert.assertEquals(1L, item.<Object>getProperty("one"));
    Assert.assertEquals(2L, item.<Object>getProperty("two"));
    Assert.assertEquals(5L, item.<Object>getProperty("2 + 3"));
    printExecutionPlan(result);

    result.close();
  }

  @Test public void testSelectNoTargetSkip() {
    OTodoResultSet result = db.query("select 1 as one, 2 as two, 2+3 skip 1");
    Assert.assertFalse(result.hasNext());
    printExecutionPlan(result);

    result.close();
  }

  @Test public void testSelectNoTargetSkipZero() {
    OTodoResultSet result = db.query("select 1 as one, 2 as two, 2+3 skip 0");
    Assert.assertTrue(result.hasNext());
    OResult item = result.next();
    Assert.assertNotNull(item);
    Assert.assertEquals(1L, item.<Object>getProperty("one"));
    Assert.assertEquals(2L, item.<Object>getProperty("two"));
    Assert.assertEquals(5L, item.<Object>getProperty("2 + 3"));
    printExecutionPlan(result);

    result.close();
  }

  @Test public void testSelectNoTargetLimit0() {
    OTodoResultSet result = db.query("select 1 as one, 2 as two, 2+3 limit 0");
    Assert.assertFalse(result.hasNext());
    printExecutionPlan(result);

    result.close();
  }

  @Test public void testSelectNoTargetLimit1() {
    OTodoResultSet result = db.query("select 1 as one, 2 as two, 2+3 limit 1");
    Assert.assertTrue(result.hasNext());
    OResult item = result.next();
    Assert.assertNotNull(item);
    Assert.assertEquals(1L, item.<Object>getProperty("one"));
    Assert.assertEquals(2L, item.<Object>getProperty("two"));
    Assert.assertEquals(5L, item.<Object>getProperty("2 + 3"));
    printExecutionPlan(result);

    result.close();
  }

  @Test public void testSelectNoTargetLimitx() {
    OTodoResultSet result = db.query("select 1 as one, 2 as two, 2+3 skip 0 limit 0");
    printExecutionPlan(result);
  }

  @Test public void testSelectFullScan1() {
    String className = "TestSelectFullScan1";
    db.getMetadata().getSchema().createClass(className);
    for (int i = 0; i < 100000; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("name", "name" + i);
      doc.setProperty("surname", "surname" + i);
      doc.save();
    }
    OTodoResultSet result = db.query("select from " + className);
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

  @Test public void testSelectFullScanOrderByRidAsc() {
    String className = "testSelectFullScanOrderByRidAsc";
    db.getMetadata().getSchema().createClass(className);
    for (int i = 0; i < 100000; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("name", "name" + i);
      doc.setProperty("surname", "surname" + i);
      doc.save();
    }
    OTodoResultSet result = db.query("select from " + className + " ORDER BY @rid ASC");
    printExecutionPlan(result);
    OIdentifiable lastItem = null;
    for (int i = 0; i < 100000; i++) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Assert.assertNotNull(item);
      Assert.assertTrue(("" + item.getProperty("name")).startsWith("name"));
      if (lastItem != null) {
        Assert.assertTrue(lastItem.getIdentity().compareTo(item.getElement().getIdentity()) < 0);
      }
      lastItem = item.getElement();
    }
    Assert.assertFalse(result.hasNext());

    result.close();
  }

  @Test public void testSelectFullScanOrderByRidDesc() {
    String className = "testSelectFullScanOrderByRidDesc";
    db.getMetadata().getSchema().createClass(className);
    for (int i = 0; i < 100000; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("name", "name" + i);
      doc.setProperty("surname", "surname" + i);
      doc.save();
    }
    OTodoResultSet result = db.query("select from " + className + " ORDER BY @rid DESC");
    printExecutionPlan(result);
    OIdentifiable lastItem = null;
    for (int i = 0; i < 100000; i++) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Assert.assertNotNull(item);
      Assert.assertTrue(("" + item.getProperty("name")).startsWith("name"));
      if (lastItem != null) {
        Assert.assertTrue(lastItem.getIdentity().compareTo(item.getElement().getIdentity()) > 0);
      }
      lastItem = item.getElement();
    }
    Assert.assertFalse(result.hasNext());

    result.close();
  }

  @Test public void testSelectFullScanLimit1() {
    String className = "testSelectFullScanLimit1";
    db.getMetadata().getSchema().createClass(className);
    for (int i = 0; i < 300; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("name", "name" + i);
      doc.setProperty("surname", "surname" + i);
      doc.save();
    }
    OTodoResultSet result = db.query("select from " + className + " limit 10");
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

  @Test public void testSelectFullScanSkipLimit1() {
    String className = "testSelectFullScanSkipLimit1";
    db.getMetadata().getSchema().createClass(className);
    for (int i = 0; i < 300; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("name", "name" + i);
      doc.setProperty("surname", "surname" + i);
      doc.save();
    }
    OTodoResultSet result = db.query("select from " + className + " skip 100 limit 10");
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

  @Test public void testSelectOrderByDesc() {
    String className = "testSelectOrderByDesc";
    db.getMetadata().getSchema().createClass(className);
    for (int i = 0; i < 30; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("name", "name" + i);
      doc.setProperty("surname", "surname" + i);
      doc.save();
    }
    OTodoResultSet result = db.query("select from " + className + " order by surname desc");
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

  @Test public void testSelectOrderByAsc() {
    String className = "testSelectOrderByAsc";
    db.getMetadata().getSchema().createClass(className);
    for (int i = 0; i < 30; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("name", "name" + i);
      doc.setProperty("surname", "surname" + i);
      doc.save();
    }
    OTodoResultSet result = db.query("select from " + className + " order by surname asc");
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

  @Test public void testSelectOrderByMassiveAsc() {
    String className = "testSelectOrderByMassiveAsc";
    db.getMetadata().getSchema().createClass(className);
    for (int i = 0; i < 100000; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("name", "name" + i);
      doc.setProperty("surname", "surname" + i % 100);
      doc.save();
    }
    long begin = System.nanoTime();
    OTodoResultSet result = db.query("select from " + className + " order by surname asc limit 100");
    System.out.println("elapsed: " + (System.nanoTime() - begin));
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

  @Test public void testSelectFullScanWithFilter1() {
    String className = "testSelectFullScanWithFilter1";
    db.getMetadata().getSchema().createClass(className);
    for (int i = 0; i < 300; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("name", "name" + i);
      doc.setProperty("surname", "surname" + i);
      doc.save();
    }
    OTodoResultSet result = db.query("select from " + className + " where name = 'name1' or name = 'name7' ");
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

  @Test public void testSelectFullScanWithFilter2() {
    String className = "testSelectFullScanWithFilter2";
    db.getMetadata().getSchema().createClass(className);
    for (int i = 0; i < 300; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("name", "name" + i);
      doc.setProperty("surname", "surname" + i);
      doc.save();
    }
    OTodoResultSet result = db.query("select from " + className + " where name <> 'name1' ");
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

  @Test public void testProjections() {
    String className = "testProjections";
    db.getMetadata().getSchema().createClass(className);
    for (int i = 0; i < 300; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("name", "name" + i);
      doc.setProperty("surname", "surname" + i);
      doc.save();
    }
    OTodoResultSet result = db.query("select name from " + className);
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
      Assert.assertNull(item.getElement());
    }
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test public void testCountStar() {
    String className = "testCountStar";
    db.getMetadata().getSchema().createClass(className);

    try {
      OTodoResultSet result = db.query("select count(*) from " + className);
      printExecutionPlan(result);
    } catch (Exception e) {
      Assert.fail();
    }
  }

  @Test public void testAggretateMixedWithNonAggregate() {
    String className = "testAggretateMixedWithNonAggregate";
    db.getMetadata().getSchema().createClass(className);

    try {
      db.query("select max(a) + max(b) + pippo + pluto as foo, max(d) + max(e), f from " + className);
      Assert.fail();
    } catch (OCommandExecutionException x) {

    } catch (Exception e) {
      Assert.fail();
    }
  }

  @Test public void testAggretateMixedWithNonAggregateInCollection() {
    String className = "testAggretateMixedWithNonAggregateInCollection";
    db.getMetadata().getSchema().createClass(className);

    try {
      db.query("select [max(a), max(b), foo] from " + className);
      Assert.fail();
    } catch (OCommandExecutionException x) {

    } catch (Exception e) {
      Assert.fail();
    }
  }

  @Test public void testAggretateInCollection() {
    String className = "testAggretateInCollection";
    db.getMetadata().getSchema().createClass(className);

    try {
      String query = "select [max(a), max(b)] from " + className;
      OTodoResultSet result = db.query(query);
      printExecutionPlan(query, result);
    } catch (Exception x) {
      Assert.fail();
    }
  }

  @Test public void testAggretateMixedWithNonAggregateConstants() {
    String className = "testAggretateMixedWithNonAggregateConstants";
    db.getMetadata().getSchema().createClass(className);

    try {
      OTodoResultSet result = db
          .query("select max(a + b) + (max(b + c * 2) + 1 + 2) * 3 as foo, max(d) + max(e), f from " + className);
      printExecutionPlan(result);
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    }
  }

  @Test public void testAggregateSum() {
    String className = "testAggregateSum";
    db.getMetadata().getSchema().createClass(className);
    for (int i = 0; i < 10; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("name", "name" + i);
      doc.setProperty("val", i);
      doc.save();
    }
    OTodoResultSet result = db.query("select sum(val) from " + className);
    printExecutionPlan(result);
    Assert.assertTrue(result.hasNext());
    OResult item = result.next();
    Assert.assertNotNull(item);
    Assert.assertEquals(45, (Object) item.getProperty("sum(val)"));

    result.close();
  }

  @Test public void testAggregateSumGroupBy() {
    String className = "testAggregateSumGroupBy";
    db.getMetadata().getSchema().createClass(className);
    for (int i = 0; i < 10; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("type", i % 2 == 0 ? "even" : "odd");
      doc.setProperty("val", i);
      doc.save();
    }
    OTodoResultSet result = db.query("select sum(val), type from " + className + " group by type");
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

  @Test public void testAggregateSumMaxMinGroupBy() {
    String className = "testAggregateSumMaxMinGroupBy";
    db.getMetadata().getSchema().createClass(className);
    for (int i = 0; i < 10; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("type", i % 2 == 0 ? "even" : "odd");
      doc.setProperty("val", i);
      doc.save();
    }
    OTodoResultSet result = db.query("select sum(val), max(val), min(val), type from " + className + " group by type");
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

  @Test public void testAggregateSumNoGroupByInProjection() {
    String className = "testAggregateSumNoGroupByInProjection";
    db.getMetadata().getSchema().createClass(className);
    for (int i = 0; i < 10; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("type", i % 2 == 0 ? "even" : "odd");
      doc.setProperty("val", i);
      doc.save();
    }
    OTodoResultSet result = db.query("select sum(val) from " + className + " group by type");
    printExecutionPlan(result);
    boolean evenFound = false;
    boolean oddFound = false;
    for (int i = 0; i < 2; i++) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Assert.assertNotNull(item);
      Object sum = item.<Object>getProperty("sum(val)");
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

  @Test public void testAggregateSumNoGroupByInProjection2() {
    String className = "testAggregateSumNoGroupByInProjection2";
    db.getMetadata().getSchema().createClass(className);
    for (int i = 0; i < 10; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("type", i % 2 == 0 ? "dd1" : "dd2");
      doc.setProperty("val", i);
      doc.save();
    }
    OTodoResultSet result = db.query("select sum(val) from " + className + " group by type.substring(0,1)");
    printExecutionPlan(result);
    for (int i = 0; i < 1; i++) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Assert.assertNotNull(item);
      Object sum = item.<Object>getProperty("sum(val)");
      Assert.assertEquals(45, sum);

    }
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test public void testFetchFromClusterNumber() {
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
    OTodoResultSet result = db.query("select from cluster:" + targetClusterName);
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

  @Test public void testFetchFromClusterNumberOrderByRidDesc() {
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
    OTodoResultSet result = db.query("select from cluster:" + targetClusterName + " order by @rid desc");
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

  @Test public void testFetchFromClusterNumberOrderByRidAsc() {
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
    OTodoResultSet result = db.query("select from cluster:" + targetClusterName + " order by @rid asc");
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

  @Test public void testFetchFromClustersNumberOrderByRidAsc() {
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

    OTodoResultSet result = db
        .query("select from cluster:[" + targetClusterName + ", " + targetClusterName2 + "] order by @rid asc");
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

  @Test public void testQueryAsTarget() {
    String className = "testQueryAsTarget";
    OSchema schema = db.getMetadata().getSchema();
    OClass clazz = schema.createClass(className);

    for (int i = 0; i < 10; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("val", i);
      doc.save();
    }

    OTodoResultSet result = db.query("select from (select from " + className + " where val > 2)  where val < 8");
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

  @Test public void testQuerySchema() {
    OTodoResultSet result = db.query("select from metadata:schema");
    printExecutionPlan(result);

    for (int i = 0; i < 1; i++) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Assert.assertNotNull(item.getProperty("classes"));
    }
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test public void testQueryMetadataIndexManager() {
    OTodoResultSet result = db.query("select from metadata:indexmanager");
    printExecutionPlan(result);
    for (int i = 0; i < 1; i++) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Assert.assertNotNull(item.getProperty("indexes"));
    }
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test public void testQueryMetadataIndexManager2() {
    OTodoResultSet result = db.query("select expand(indexes) from metadata:indexmanager");
    printExecutionPlan(result);
    Assert.assertTrue(result.hasNext());
    result.close();
  }

  @Test public void testNonExistingRids() {
    OTodoResultSet result = db.query("select from #0:100000000");
    printExecutionPlan(result);
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test public void testFetchFromSingleRid() {
    OTodoResultSet result = db.query("select from #0:1");
    printExecutionPlan(result);
    Assert.assertTrue(result.hasNext());
    Assert.assertNotNull(result.next());
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test public void testFetchFromSingleRid2() {
    OTodoResultSet result = db.query("select from [#0:1]");
    printExecutionPlan(result);
    Assert.assertTrue(result.hasNext());
    Assert.assertNotNull(result.next());
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test public void testFetchFromSingleRid3() {
    OTodoResultSet result = db.query("select from [#0:1, #0:2]");
    printExecutionPlan(result);
    Assert.assertTrue(result.hasNext());
    Assert.assertNotNull(result.next());
    Assert.assertTrue(result.hasNext());
    Assert.assertNotNull(result.next());
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test public void testFetchFromSingleRid4() {
    OTodoResultSet result = db.query("select from [#0:1, #0:2, #0:100000]");
    printExecutionPlan(result);
    Assert.assertTrue(result.hasNext());
    Assert.assertNotNull(result.next());
    Assert.assertTrue(result.hasNext());
    Assert.assertNotNull(result.next());
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test public void testFetchFromIndex() {
    String className = "testFetchFromIndex";
    OClass clazz = db.getMetadata().getSchema().createClass(className);
    clazz.createProperty("name", OType.STRING);
    clazz.createIndex(className + ".name", OClass.INDEX_TYPE.NOTUNIQUE, "name");

    for (int i = 0; i < 10; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("name", "name" + i);
      doc.save();
    }

    OTodoResultSet result = db.query("select from index:" + className + ".name where key = 'name1'");
    printExecutionPlan(result);
    Assert.assertTrue(result.hasNext());
    Assert.assertNotNull(result.next());
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test public void testFetchFromIndexMajor() {
    String className = "testFetchFromIndexMajor";
    OClass clazz = db.getMetadata().getSchema().createClass(className);
    clazz.createProperty("name", OType.STRING);
    clazz.createIndex(className + ".name", OClass.INDEX_TYPE.NOTUNIQUE, "name");

    for (int i = 0; i < 10; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("name", "name" + i);
      doc.save();
    }

    OTodoResultSet result = db.query("select from index:" + className + ".name where key > 'name1'");
    printExecutionPlan(result);
    for (int i = 0; i < 8; i++) {
      Assert.assertTrue(result.hasNext());
      OResult next = result.next();
      Assert.assertNotNull(next);
    }
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test public void testFetchFromIndexMajorEqual() {
    String className = "testFetchFromIndexMajorEqual";
    OClass clazz = db.getMetadata().getSchema().createClass(className);
    clazz.createProperty("name", OType.STRING);
    clazz.createIndex(className + ".name", OClass.INDEX_TYPE.NOTUNIQUE, "name");

    for (int i = 0; i < 10; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("name", "name" + i);
      doc.save();
    }

    OTodoResultSet result = db.query("select from index:" + className + ".name where key >= 'name1'");
    printExecutionPlan(result);
    for (int i = 0; i < 9; i++) {
      Assert.assertTrue(result.hasNext());
      OResult next = result.next();
      Assert.assertNotNull(next);
    }
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test public void testFetchFromIndexMinor() {
    String className = "testFetchFromIndexMinor";
    OClass clazz = db.getMetadata().getSchema().createClass(className);
    clazz.createProperty("name", OType.STRING);
    clazz.createIndex(className + ".name", OClass.INDEX_TYPE.NOTUNIQUE, "name");

    for (int i = 0; i < 10; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("name", "name" + i);
      doc.save();
    }

    OTodoResultSet result = db.query("select from index:" + className + ".name where key < 'name3'");
    printExecutionPlan(result);
    for (int i = 0; i < 3; i++) {
      Assert.assertTrue(result.hasNext());
      OResult next = result.next();
      Assert.assertNotNull(next);
    }
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test public void testFetchFromIndexLessOrEqual() {
    String className = "testFetchFromIndexLessOrEqual";
    OClass clazz = db.getMetadata().getSchema().createClass(className);
    clazz.createProperty("name", OType.STRING);
    clazz.createIndex(className + ".name", OClass.INDEX_TYPE.NOTUNIQUE, "name");

    for (int i = 0; i < 10; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("name", "name" + i);
      doc.save();
    }

    OTodoResultSet result = db.query("select from index:" + className + ".name where key <= 'name3'");
    printExecutionPlan(result);
    for (int i = 0; i < 4; i++) {
      Assert.assertTrue(result.hasNext());
      OResult next = result.next();
      Assert.assertNotNull(next);
    }
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test public void testFetchFromIndexBetween() {
    String className = "testFetchFromIndexBetween";
    OClass clazz = db.getMetadata().getSchema().createClass(className);
    clazz.createProperty("name", OType.STRING);
    clazz.createIndex(className + ".name", OClass.INDEX_TYPE.NOTUNIQUE, "name");

    for (int i = 0; i < 10; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("name", "name" + i);
      doc.save();
    }

    OTodoResultSet result = db.query("select from index:" + className + ".name where key between 'name1' and 'name5'");
    printExecutionPlan(result);
    for (int i = 0; i < 5; i++) {
      Assert.assertTrue(result.hasNext());
      OResult next = result.next();
      Assert.assertNotNull(next);
    }
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test public void testFetchFromIndexWithoutConditions1() {
    String className = "testFetchFromIndexWithoutConditions1";
    OClass clazz = db.getMetadata().getSchema().createClass(className);
    clazz.createProperty("name", OType.STRING);
    clazz.createIndex(className + ".name", OClass.INDEX_TYPE.NOTUNIQUE, "name");

    for (int i = 0; i < 10; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("name", "name" + i);
      doc.save();
    }

    OTodoResultSet result = db.query("select from index:" + className + ".name");
    printExecutionPlan(result);
    for (int i = 0; i < 10; i++) {
      Assert.assertTrue(result.hasNext());
      OResult next = result.next();
      Assert.assertNotNull(next);
    }
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test public void testFetchFromIndexWithoutConditions2() {
    String className = "testFetchFromIndexWithoutConditions2";
    OClass clazz = db.getMetadata().getSchema().createClass(className);
    clazz.createProperty("name", OType.STRING);
    clazz.createIndex(className + ".name", OClass.INDEX_TYPE.NOTUNIQUE_HASH_INDEX, "name");

    ODocument doc = db.newInstance(className);
    doc.setProperty("name", "name1");
    doc.save();

    try {
      db.query("select from index:" + className + ".name");
      Assert.fail();
    } catch (OCommandExecutionException ex) {

    } catch (Exception x) {
      Assert.fail();
    }
  }

  @Test public void testFetchFromIndexValues() {
    String className = "testFetchFromIndexValues";
    OClass clazz = db.getMetadata().getSchema().createClass(className);
    clazz.createProperty("name", OType.STRING);
    clazz.createIndex(className + ".name", OClass.INDEX_TYPE.NOTUNIQUE, "name");

    for (int i = 0; i < 10; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("name", "name" + i % 3);
      doc.save();
    }

    OTodoResultSet result = db.query("select from indexvalues:" + className + ".name");
    printExecutionPlan(result);
    Set<String> expectedValues = new HashSet<>();
    expectedValues.add("name0");
    expectedValues.add("name1");
    expectedValues.add("name2");
    Object lastValue = null;
    for (int i = 0; i < 10; i++) {
      Assert.assertTrue(result.hasNext());
      OResult next = result.next();
      Assert.assertNotNull(next);
      String currentValue = next.getProperty("name");
      Assert.assertNotNull(currentValue);
      if (lastValue != null) {
        Assert.assertTrue(currentValue.compareTo(lastValue.toString()) >= 0);
      }
      expectedValues.remove(lastValue);
      lastValue = currentValue;
    }
    Assert.assertTrue(expectedValues.isEmpty());
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test public void testFetchFromIndexValuesAsc() {
    String className = "testFetchFromIndexValuesAsc";
    OClass clazz = db.getMetadata().getSchema().createClass(className);
    clazz.createProperty("name", OType.STRING);
    clazz.createIndex(className + ".name", OClass.INDEX_TYPE.NOTUNIQUE, "name");

    for (int i = 0; i < 10; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("name", "name" + i % 3);
      doc.save();
    }

    OTodoResultSet result = db.query("select from indexvaluesasc:" + className + ".name");
    printExecutionPlan(result);
    Set<String> expectedValues = new HashSet<>();
    expectedValues.add("name0");
    expectedValues.add("name1");
    expectedValues.add("name2");
    Object lastValue = null;
    for (int i = 0; i < 10; i++) {
      Assert.assertTrue(result.hasNext());
      OResult next = result.next();
      Assert.assertNotNull(next);
      String currentValue = next.getProperty("name");
      Assert.assertNotNull(currentValue);
      if (lastValue != null) {
        Assert.assertTrue(currentValue.compareTo(lastValue.toString()) >= 0);
      }
      expectedValues.remove(lastValue);
      lastValue = currentValue;
    }
    Assert.assertTrue(expectedValues.isEmpty());
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test public void testFetchFromIndexValuesDesc() {
    String className = "testFetchFromIndexValuesDesc";
    OClass clazz = db.getMetadata().getSchema().createClass(className);
    clazz.createProperty("name", OType.STRING);
    clazz.createIndex(className + ".name", OClass.INDEX_TYPE.NOTUNIQUE, "name");

    for (int i = 0; i < 10; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("name", "name" + i % 3);
      doc.save();
    }

    OTodoResultSet result = db.query("select from indexvaluesdesc:" + className + ".name");
    printExecutionPlan(result);
    Set<String> expectedValues = new HashSet<>();
    expectedValues.add("name0");
    expectedValues.add("name1");
    expectedValues.add("name2");
    Object lastValue = null;
    for (int i = 0; i < 10; i++) {
      Assert.assertTrue(result.hasNext());
      OResult next = result.next();
      Assert.assertNotNull(next);
      String currentValue = next.getProperty("name");
      Assert.assertNotNull(currentValue);
      if (lastValue != null) {
        Assert.assertTrue(currentValue.compareTo(lastValue.toString()) <= 0);
      }
      expectedValues.remove(lastValue);
      lastValue = currentValue;
    }
    Assert.assertTrue(expectedValues.isEmpty());
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test public void testFetchFromIndexValuesDescWithCondition() {
    String className = "testFetchFromIndexValuesDescWithCondition";
    OClass clazz = db.getMetadata().getSchema().createClass(className);
    clazz.createProperty("name", OType.STRING);
    clazz.createIndex(className + ".name", OClass.INDEX_TYPE.NOTUNIQUE, "name");

    for (int i = 0; i < 10; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("name", "name" + i % 3);
      doc.save();
    }

    OTodoResultSet result = db.query("select from indexvaluesdesc:" + className + ".name where name = 'name0'");
    printExecutionPlan(result);
    for (int i = 0; i < 4; i++) {
      Assert.assertTrue(result.hasNext());
      OResult next = result.next();
      Assert.assertNotNull(next);
      String currentValue = next.getProperty("name");
      Assert.assertNotNull(currentValue);
      Assert.assertEquals("name0", currentValue);
    }
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test public void testFetchFromClassWithIndex() {
    String className = "testFetchFromClassWithIndex";
    OClass clazz = db.getMetadata().getSchema().createClass(className);
    clazz.createProperty("name", OType.STRING);
    clazz.createIndex(className + ".name", OClass.INDEX_TYPE.NOTUNIQUE, "name");

    for (int i = 0; i < 10; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("name", "name" + i);
      doc.save();
    }

    OTodoResultSet result = db.query("select from " + className + " where name = 'name2'");
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
    Assert.assertEquals(1, plan.getSteps().size());
    Assert.assertEquals(FetchFromIndexStep.class, plan.getSteps().get(0).getClass());
    result.close();
  }

  @Test public void testFetchFromClassWithIndexes() {
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

    OTodoResultSet result = db.query("select from " + className + " where name = 'name2' or surname = 'surname3'");
    printExecutionPlan(result);

    Assert.assertTrue(result.hasNext());
    for (int i = 0; i < 2; i++) {
      OResult next = result.next();
      Assert.assertNotNull(next);
      Assert.assertTrue("name2".equals(next.getProperty("name")) || ("surname3".equals(next.getProperty("surname"))));
    }

    Assert.assertFalse(result.hasNext());

    Optional<OExecutionPlan> p = result.getExecutionPlan();
    Assert.assertTrue(p.isPresent());
    OExecutionPlan p2 = p.get();
    Assert.assertTrue(p2 instanceof OSelectExecutionPlan);
    OSelectExecutionPlan plan = (OSelectExecutionPlan) p2;
    Assert.assertEquals(1, plan.getSteps().size());
    Assert.assertEquals(ParallelExecStep.class, plan.getSteps().get(0).getClass());
    ParallelExecStep parallel = (ParallelExecStep) plan.getSteps().get(0);
    Assert.assertEquals(2, parallel.getSubExecutionPlans().size());
    result.close();
  }

  @Test public void testFetchFromClassWithIndexes2() {
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

    OTodoResultSet result = db
        .query("select from " + className + " where foo is not null and (name = 'name2' or surname = 'surname3')");
    printExecutionPlan(result);

    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test public void testFetchFromClassWithIndexes3() {
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

    OTodoResultSet result = db.query("select from " + className + " where foo < 100 and (name = 'name2' or surname = 'surname3')");
    printExecutionPlan(result);

    Assert.assertTrue(result.hasNext());
    for (int i = 0; i < 2; i++) {
      OResult next = result.next();
      Assert.assertNotNull(next);
      Assert.assertTrue("name2".equals(next.getProperty("name")) || ("surname3".equals(next.getProperty("surname"))));
    }

    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test public void testFetchFromClassWithIndexes4() {
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

    OTodoResultSet result = db.query("select from " + className
        + " where foo < 100 and ((name = 'name2' and foo < 20) or surname = 'surname3') and ( 4<5 and foo < 50)");
    printExecutionPlan(result);

    Assert.assertTrue(result.hasNext());
    for (int i = 0; i < 2; i++) {
      OResult next = result.next();
      Assert.assertNotNull(next);
      Assert.assertTrue("name2".equals(next.getProperty("name")) || ("surname3".equals(next.getProperty("surname"))));
    }

    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test public void testFetchFromClassWithIndexes5() {
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

    OTodoResultSet result = db.query("select from " + className + " where name = 'name3' and surname >= 'surname1'");
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

  @Test public void testFetchFromClassWithIndexes6() {
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

    OTodoResultSet result = db.query("select from " + className + " where name = 'name3' and surname > 'surname3'");
    printExecutionPlan(result);

    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test public void testFetchFromClassWithIndexes7() {
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

    OTodoResultSet result = db.query("select from " + className + " where name = 'name3' and surname >= 'surname3'");
    printExecutionPlan(result);
    for (int i = 0; i < 1; i++) {
      OResult next = result.next();
      Assert.assertNotNull(next);
      Assert.assertEquals("name3", next.getProperty("name"));
    }
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test public void testFetchFromClassWithIndexes8() {
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

    OTodoResultSet result = db.query("select from " + className + " where name = 'name3' and surname < 'surname3'");
    printExecutionPlan(result);

    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test public void testFetchFromClassWithIndexes9() {
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

    OTodoResultSet result = db.query("select from " + className + " where name = 'name3' and surname <= 'surname3'");
    printExecutionPlan(result);
    for (int i = 0; i < 1; i++) {
      OResult next = result.next();
      Assert.assertNotNull(next);
      Assert.assertEquals("name3", next.getProperty("name"));
    }
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test public void testFetchFromClassWithIndexes10() {
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

    OTodoResultSet result = db.query("select from " + className + " where name > 'name3' ");
    printExecutionPlan(result);
    for (int i = 0; i < 6; i++) {
      Assert.assertTrue(result.hasNext());
      OResult next = result.next();
      Assert.assertNotNull(next);
    }
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test public void testFetchFromClassWithIndexes11() {
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

    OTodoResultSet result = db.query("select from " + className + " where name >= 'name3' ");
    printExecutionPlan(result);
    for (int i = 0; i < 7; i++) {
      OResult next = result.next();
      Assert.assertNotNull(next);
    }
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test public void testFetchFromClassWithIndexes12() {
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

    OTodoResultSet result = db.query("select from " + className + " where name < 'name3' ");
    printExecutionPlan(result);
    for (int i = 0; i < 3; i++) {
      OResult next = result.next();
      Assert.assertNotNull(next);
    }
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test public void testFetchFromClassWithIndexes13() {
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

    OTodoResultSet result = db.query("select from " + className + " where name <= 'name3' ");
    printExecutionPlan(result);
    for (int i = 0; i < 4; i++) {
      OResult next = result.next();
      Assert.assertNotNull(next);
    }
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test public void testFetchFromClassWithIndexes14() {
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

    OTodoResultSet result = db.query("select from " + className + " where name > 'name3' and name < 'name5'");
    printExecutionPlan(result);
    for (int i = 0; i < 1; i++) {
      OResult next = result.next();
      Assert.assertNotNull(next);
    }
    Assert.assertFalse(result.hasNext());
    OSelectExecutionPlan plan = (OSelectExecutionPlan) result.getExecutionPlan().get();
    Assert.assertEquals(1, plan.getSteps().size());
    result.close();
  }

  @Test public void testFetchFromClassWithIndexes15() {
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

    OTodoResultSet result = db.query(
        "select from " + className + " where name > 'name6' and name = 'name3' and surname > 'surname2' and surname < 'surname5' ");
    printExecutionPlan(result);
    Assert.assertFalse(result.hasNext());
    OSelectExecutionPlan plan = (OSelectExecutionPlan) result.getExecutionPlan().get();
    Assert.assertEquals(2, plan.getSteps().size());
    result.close();
  }

  @Test public void testFetchFromClassWithHashIndexes1() {
    String className = "testFetchFromClassWithHashIndexes1";
    OClass clazz = db.getMetadata().getSchema().createClass(className);
    clazz.createProperty("name", OType.STRING);
    clazz.createProperty("surname", OType.STRING);
    clazz.createIndex(className + ".name_surname", OClass.INDEX_TYPE.NOTUNIQUE_HASH_INDEX, "name", "surname");

    for (int i = 0; i < 10; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("name", "name" + i);
      doc.setProperty("surname", "surname" + i);
      doc.setProperty("foo", i);
      doc.save();
    }

    OTodoResultSet result = db.query("select from " + className + " where name = 'name6' and surname = 'surname6' ");
    printExecutionPlan(result);

    for (int i = 0; i < 1; i++) {
      Assert.assertTrue(result.hasNext());
      OResult next = result.next();
      Assert.assertNotNull(next);
    }
    Assert.assertFalse(result.hasNext());
    OSelectExecutionPlan plan = (OSelectExecutionPlan) result.getExecutionPlan().get();
    Assert.assertEquals(1, plan.getSteps().size());
    result.close();
  }

  @Test public void testFetchFromClassWithHashIndexes2() {
    String className = "testFetchFromClassWithHashIndexes2";
    OClass clazz = db.getMetadata().getSchema().createClass(className);
    clazz.createProperty("name", OType.STRING);
    clazz.createProperty("surname", OType.STRING);
    clazz.createIndex(className + ".name_surname", OClass.INDEX_TYPE.NOTUNIQUE_HASH_INDEX, "name", "surname");

    for (int i = 0; i < 10; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("name", "name" + i);
      doc.setProperty("surname", "surname" + i);
      doc.setProperty("foo", i);
      doc.save();
    }

    OTodoResultSet result = db.query("select from " + className + " where name = 'name6' and surname >= 'surname6' ");
    printExecutionPlan(result);

    for (int i = 0; i < 1; i++) {
      Assert.assertTrue(result.hasNext());
      OResult next = result.next();
      Assert.assertNotNull(next);
    }
    Assert.assertFalse(result.hasNext());
    OSelectExecutionPlan plan = (OSelectExecutionPlan) result.getExecutionPlan().get();
    Assert.assertEquals(2, plan.getSteps().size());
    Assert.assertEquals(FetchFromClassExecutionStep.class, plan.getSteps().get(0).getClass());//index not used
    result.close();
  }

  @Test public void testExpand1() {
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

    OTodoResultSet result = db.query("select expand(linked) from " + parentClassName);
    printExecutionPlan(result);

    for (int i = 0; i < count; i++) {
      Assert.assertTrue(result.hasNext());
      OResult next = result.next();
      Assert.assertNotNull(next);
    }
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test public void testExpand2() {
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

    OTodoResultSet result = db.query("select expand(linked) from " + parentClassName);
    printExecutionPlan(result);

    for (int i = 0; i < count * collSize; i++) {
      Assert.assertTrue(result.hasNext());
      OResult next = result.next();
      Assert.assertNotNull(next);
    }
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test public void testExpand3() {
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

    OTodoResultSet result = db.query("select expand(linked) from " + parentClassName + " order by name");
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

  @Test public void testDistinct1() {
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

    OTodoResultSet result = db.query("select distinct name, surname from " + className);
    printExecutionPlan(result);

    for (int i = 0; i < 10; i++) {
      Assert.assertTrue(result.hasNext());
      OResult next = result.next();
      Assert.assertNotNull(next);
    }
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test public void testLet1() {
    OTodoResultSet result = db.query("select $a as one, $b as two let $a = 1, $b = 1+1");
    Assert.assertTrue(result.hasNext());
    OResult item = result.next();
    Assert.assertNotNull(item);
    Assert.assertEquals(1L, item.<Object>getProperty("one"));
    Assert.assertEquals(2L, item.<Object>getProperty("two"));
    printExecutionPlan(result);
    result.close();
  }

  @Test public void testLet2() {
    OTodoResultSet result = db.query("select $a as one let $a = (select 1 as a)");
    printExecutionPlan(result);
    Assert.assertTrue(result.hasNext());
    OResult item = result.next();
    Assert.assertNotNull(item);
    Object one = item.<Object>getProperty("one");
    Assert.assertTrue(one instanceof List);
    Assert.assertEquals(1, ((List) one).size());
    Object x = ((List) one).get(0);
    Assert.assertTrue(x instanceof OResult);
    Assert.assertEquals(1L, (Object) ((OResult) x).getProperty("a"));
    result.close();
  }

  @Test public void testLet3() {
    OTodoResultSet result = db.query("select $a[0].foo as one let $a = (select 1 as foo)");
    printExecutionPlan(result);
    Assert.assertTrue(result.hasNext());
    OResult item = result.next();
    Assert.assertNotNull(item);
    Object one = item.<Object>getProperty("one");
    Assert.assertEquals(1l, one);
    result.close();
  }

  @Test public void testLet4() {
    String className = "testLet4";
    db.getMetadata().getSchema().createClass(className);

    for (int i = 0; i < 10; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("name", "name" + i);
      doc.setProperty("surname", "surname" + i);
      doc.save();
    }

    OTodoResultSet result = db.query(
        "select name, surname, $nameAndSurname as fullname from " + className + " let $nameAndSurname = name + ' ' + surname");
    printExecutionPlan(result);
    for (int i = 0; i < 10; i++) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Assert.assertNotNull(item);
      Assert.assertEquals(item.getProperty("fullname"), item.getProperty("name") + " " + item.getProperty("surname"));
    }
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test public void testLet5() {
    String className = "testLet5";
    db.getMetadata().getSchema().createClass(className);

    for (int i = 0; i < 10; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("name", "name" + i);
      doc.setProperty("surname", "surname" + i);
      doc.save();
    }

    OTodoResultSet result = db
        .query("select from " + className + " where name in (select name from " + className + " where name = 'name1')");
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

  @Test public void testLet6() {
    String className = "testLet6";
    db.getMetadata().getSchema().createClass(className);

    for (int i = 0; i < 10; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("name", "name" + i);
      doc.setProperty("surname", "surname" + i);
      doc.save();
    }

    OTodoResultSet result = db.query("select $foo as name from " + className + " let $foo = (select name from " + className
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

  @Test public void testLet7() {
    String className = "testLet7";
    db.getMetadata().getSchema().createClass(className);

    for (int i = 0; i < 10; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("name", "name" + i);
      doc.setProperty("surname", "surname" + i);
      doc.save();
    }

    OTodoResultSet result = db.query("select $bar as name from " + className + " " + "let $foo = (select name from " + className
        + " where name = $parent.$current.name)," + "$bar = $foo[0].name");
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

  @Test public void testUnwind1() {
    String className = "testUnwind1";
    db.getMetadata().getSchema().createClass(className);

    for (int i = 0; i < 10; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("i", i);
      doc.setProperty("iSeq", new int[] { i, 2 * i, 4 * i });
      doc.save();
    }

    OTodoResultSet result = db.query("select i, iSeq from " + className + " unwind iSeq");
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

  @Test public void testUnwind2() {
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

    OTodoResultSet result = db.query("select i, iSeq from " + className + " unwind iSeq");
    printExecutionPlan(result);
    for (int i = 0; i < 30; i++) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Assert.assertNotNull(item);
      Assert.assertNotNull(item.getProperty("i"));
      Assert.assertNotNull(item.getProperty("iSeq"));
      Integer first = (Integer) item.getProperty("i");
      Integer second = (Integer) item.getProperty("iSeq");
      Assert.assertTrue(first + second == 0 || second.intValue() % first.intValue() == 0);
    }
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test public void testFetchFromSubclassIndexes1() {
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

    OTodoResultSet result = db.query("select from " + parent + " where name = 'name1'");
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

  @Test public void testFetchFromSubclassIndexes2() {
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

    OTodoResultSet result = db.query("select from " + parent + " where name = 'name1' and surname = 'surname1'");
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

  @Test public void testFetchFromSubclassIndexes3() {
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

    OTodoResultSet result = db.query("select from " + parent + " where name = 'name1' and surname = 'surname1'");
    printExecutionPlan(result);
    OInternalExecutionPlan plan = (OInternalExecutionPlan) result.getExecutionPlan().get();
    Assert.assertTrue(plan.getSteps().get(0) instanceof FetchFromClassExecutionStep);//no index used
    for (int i = 0; i < 2; i++) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Assert.assertNotNull(item);
    }
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test public void testFetchFromSubclassIndexes4() {
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

    OTodoResultSet result = db.query("select from " + parent + " where name = 'name1' and surname = 'surname1'");
    printExecutionPlan(result);
    OInternalExecutionPlan plan = (OInternalExecutionPlan) result.getExecutionPlan().get();
    Assert
        .assertTrue(plan.getSteps().get(0) instanceof FetchFromClassExecutionStep); //no index, because the superclass is not empty
    for (int i = 0; i < 2; i++) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Assert.assertNotNull(item);
    }
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test public void testFetchFromSubSubclassIndexes() {
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

    OTodoResultSet result = db.query("select from " + parent + " where name = 'name1' and surname = 'surname1'");
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

  @Test public void testFetchFromSubSubclassIndexesWithDiamond() {
    String parent = "testFetchFromSubSubclassIndexesWithDiamond_parent";
    String child1 = "testFetchFromSubSubclassIndexesWithDiamond_child1";
    String child2 = "testFetchFromSubSubclassIndexesWithDiamond_child2";
    String child12 = "testFetchFromSubSubclassIndexesWithDiamond_child12";

    OClass parentClass = db.getMetadata().getSchema().createClass(parent);
    OClass childClass1 = db.getMetadata().getSchema().createClass(child1, parentClass);
    OClass childClass2 = db.getMetadata().getSchema().createClass(child2, parentClass);
    OClass childClass12 = db.getMetadata().getSchema().createClass(child12, childClass1, childClass2);

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

    OTodoResultSet result = db.query("select from " + parent + " where name = 'name1' and surname = 'surname1'");
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

  @Test public void testIndexPlusSort1() {
    String className = "testIndexPlusSort1";
    OClass clazz = db.getMetadata().getSchema().createClass(className);
    clazz.createProperty("name", OType.STRING);
    clazz.createProperty("surname", OType.STRING);
    db.command(new OCommandSQL("create index " + className + ".name_surname on " + className + " (name, surname) NOTUNIQUE"))
        .execute();

    for (int i = 0; i < 10; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("name", "name" + i % 3);
      doc.setProperty("surname", "surname" + i);
      doc.save();
    }

    OTodoResultSet result = db.query("select from " + className + " where name = 'name1' order by surname ASC");
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
    Assert.assertEquals(1, result.getExecutionPlan().get().getSteps().size());//index used, no ORDER BY step
    result.close();
  }

  @Test public void testIndexPlusSort2() {
    String className = "testIndexPlusSort2";
    OClass clazz = db.getMetadata().getSchema().createClass(className);
    clazz.createProperty("name", OType.STRING);
    clazz.createProperty("surname", OType.STRING);
    db.command(new OCommandSQL("create index " + className + ".name_surname on " + className + " (name, surname) NOTUNIQUE"))
        .execute();

    for (int i = 0; i < 10; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("name", "name" + i % 3);
      doc.setProperty("surname", "surname" + i);
      doc.save();
    }

    OTodoResultSet result = db.query("select from " + className + " where name = 'name1' order by surname DESC");
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
    Assert.assertEquals(1, result.getExecutionPlan().get().getSteps().size());//index used, no ORDER BY step
    result.close();
  }

  @Test public void testIndexPlusSort3() {
    String className = "testIndexPlusSort3";
    OClass clazz = db.getMetadata().getSchema().createClass(className);
    clazz.createProperty("name", OType.STRING);
    clazz.createProperty("surname", OType.STRING);
    db.command(new OCommandSQL("create index " + className + ".name_surname on " + className + " (name, surname) NOTUNIQUE"))
        .execute();

    for (int i = 0; i < 10; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("name", "name" + i % 3);
      doc.setProperty("surname", "surname" + i);
      doc.save();
    }

    OTodoResultSet result = db.query("select from " + className + " where name = 'name1' order by name DESC, surname DESC");
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
    Assert.assertEquals(1, result.getExecutionPlan().get().getSteps().size());//index used, no ORDER BY step
    result.close();
  }

  @Test public void testIndexPlusSort4() {
    String className = "testIndexPlusSort4";
    OClass clazz = db.getMetadata().getSchema().createClass(className);
    clazz.createProperty("name", OType.STRING);
    clazz.createProperty("surname", OType.STRING);
    db.command(new OCommandSQL("create index " + className + ".name_surname on " + className + " (name, surname) NOTUNIQUE"))
        .execute();

    for (int i = 0; i < 10; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("name", "name" + i % 3);
      doc.setProperty("surname", "surname" + i);
      doc.save();
    }

    OTodoResultSet result = db.query("select from " + className + " where name = 'name1' order by name ASC, surname ASC");
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
    Assert.assertEquals(1, result.getExecutionPlan().get().getSteps().size());//index used, no ORDER BY step
    result.close();
  }

  @Test public void testIndexPlusSort5() {
    String className = "testIndexPlusSort5";
    OClass clazz = db.getMetadata().getSchema().createClass(className);
    clazz.createProperty("name", OType.STRING);
    clazz.createProperty("surname", OType.STRING);
    clazz.createProperty("address", OType.STRING);
    db.command(
        new OCommandSQL("create index " + className + ".name_surname on " + className + " (name, surname, address) NOTUNIQUE"))
        .execute();

    for (int i = 0; i < 10; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("name", "name" + i % 3);
      doc.setProperty("surname", "surname" + i);
      doc.save();
    }

    OTodoResultSet result = db.query("select from " + className + " where name = 'name1' order by surname ASC");
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
    Assert.assertEquals(1, result.getExecutionPlan().get().getSteps().size());//index used, no ORDER BY step
    result.close();
  }

  @Test public void testIndexPlusSort6() {
    String className = "testIndexPlusSort6";
    OClass clazz = db.getMetadata().getSchema().createClass(className);
    clazz.createProperty("name", OType.STRING);
    clazz.createProperty("surname", OType.STRING);
    clazz.createProperty("address", OType.STRING);
    db.command(
        new OCommandSQL("create index " + className + ".name_surname on " + className + " (name, surname, address) NOTUNIQUE"))
        .execute();

    for (int i = 0; i < 10; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("name", "name" + i % 3);
      doc.setProperty("surname", "surname" + i);
      doc.save();
    }

    OTodoResultSet result = db.query("select from " + className + " where name = 'name1' order by surname DESC");
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
    Assert.assertEquals(1, result.getExecutionPlan().get().getSteps().size());//index used, no ORDER BY step
    result.close();
  }

  @Test public void testIndexPlusSort7() {
    String className = "testIndexPlusSort7";
    OClass clazz = db.getMetadata().getSchema().createClass(className);
    clazz.createProperty("name", OType.STRING);
    clazz.createProperty("surname", OType.STRING);
    clazz.createProperty("address", OType.STRING);
    db.command(
        new OCommandSQL("create index " + className + ".name_surname on " + className + " (name, surname, address) NOTUNIQUE"))
        .execute();

    for (int i = 0; i < 10; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("name", "name" + i % 3);
      doc.setProperty("surname", "surname" + i);
      doc.save();
    }

    OTodoResultSet result = db.query("select from " + className + " where name = 'name1' order by address DESC");
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

  @Test public void testIndexPlusSort8() {
    String className = "testIndexPlusSort8";
    OClass clazz = db.getMetadata().getSchema().createClass(className);
    clazz.createProperty("name", OType.STRING);
    clazz.createProperty("surname", OType.STRING);
    db.command(new OCommandSQL("create index " + className + ".name_surname on " + className + " (name, surname) NOTUNIQUE"))
        .execute();

    for (int i = 0; i < 10; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("name", "name" + i % 3);
      doc.setProperty("surname", "surname" + i);
      doc.save();
    }

    OTodoResultSet result = db.query("select from " + className + " where name = 'name1' order by name ASC, surname DESC");
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

  @Test public void testIndexPlusSort9() {
    String className = "testIndexPlusSort9";
    OClass clazz = db.getMetadata().getSchema().createClass(className);
    clazz.createProperty("name", OType.STRING);
    clazz.createProperty("surname", OType.STRING);
    db.command(new OCommandSQL("create index " + className + ".name_surname on " + className + " (name, surname) NOTUNIQUE"))
        .execute();

    for (int i = 0; i < 10; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("name", "name" + i % 3);
      doc.setProperty("surname", "surname" + i);
      doc.save();
    }

    OTodoResultSet result = db.query("select from " + className + " order by name , surname ASC");
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

  @Test public void testIndexPlusSort10() {
    String className = "testIndexPlusSort10";
    OClass clazz = db.getMetadata().getSchema().createClass(className);
    clazz.createProperty("name", OType.STRING);
    clazz.createProperty("surname", OType.STRING);
    db.command(new OCommandSQL("create index " + className + ".name_surname on " + className + " (name, surname) NOTUNIQUE"))
        .execute();

    for (int i = 0; i < 10; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("name", "name" + i % 3);
      doc.setProperty("surname", "surname" + i);
      doc.save();
    }

    OTodoResultSet result = db.query("select from " + className + " order by name desc, surname desc");
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

  @Test public void testIndexPlusSort11() {
    String className = "testIndexPlusSort11";
    OClass clazz = db.getMetadata().getSchema().createClass(className);
    clazz.createProperty("name", OType.STRING);
    clazz.createProperty("surname", OType.STRING);
    db.command(new OCommandSQL("create index " + className + ".name_surname on " + className + " (name, surname) NOTUNIQUE"))
        .execute();

    for (int i = 0; i < 10; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("name", "name" + i % 3);
      doc.setProperty("surname", "surname" + i);
      doc.save();
    }

    OTodoResultSet result = db.query("select from " + className + " order by name asc, surname desc");
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

  @Test public void testIndexPlusSort12() {
    String className = "testIndexPlusSort12";
    OClass clazz = db.getMetadata().getSchema().createClass(className);
    clazz.createProperty("name", OType.STRING);
    clazz.createProperty("surname", OType.STRING);
    db.command(new OCommandSQL("create index " + className + ".name_surname on " + className + " (name, surname) NOTUNIQUE"))
        .execute();

    for (int i = 0; i < 10; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("name", "name" + i % 3);
      doc.setProperty("surname", "surname" + i);
      doc.save();
    }

    OTodoResultSet result = db.query("select from " + className + " order by name");
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

  public void stressTestNew() {
    String className = "stressTestNew";
    db.getMetadata().getSchema().createClass(className);
    for (int i = 0; i < 1000000; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("name", "name" + i);
      doc.setProperty("surname", "surname" + i);
      doc.save();
    }

    for (int run = 0; run < 5; run++) {
      long begin = System.nanoTime();
      OTodoResultSet result = db.query("select name from " + className + " where name <> 'name1' ");
      for (int i = 0; i < 999999; i++) {
        //        Assert.assertTrue(result.hasNext());
        OResult item = result.next();
        //        Assert.assertNotNull(item);
        Object name = item.getProperty("name");
        Assert.assertFalse("name1".equals(name));
      }
      Assert.assertFalse(result.hasNext());
      result.close();
      long end = System.nanoTime();
      System.out.println("new: " + ((end - begin) / 1000000));
    }
  }

  public void stressTestOld() {
    String className = "stressTestOld";
    db.getMetadata().getSchema().createClass(className);
    for (int i = 0; i < 1000000; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("name", "name" + i);
      doc.setProperty("surname", "surname" + i);
      doc.save();
    }
    for (int run = 0; run < 5; run++) {
      long begin = System.nanoTime();
      List<ODocument> r = db.query(new OSQLSynchQuery<ODocument>("select name from " + className + " where name <> 'name1' "));
      //      Iterator<ODocument> result = r.iterator();
      for (int i = 0; i < 999999; i++) {
        //        Assert.assertTrue(result.hasNext());
        //        ODocument item = result.next();
        ODocument item = r.get(i);

        //        Assert.assertNotNull(item);
        Object name = item.getProperty("name");
        Assert.assertFalse("name1".equals(name));
      }
      //      Assert.assertFalse(result.hasNext());
      long end = System.nanoTime();
      System.out.println("old: " + ((end - begin) / 1000000));
    }
  }

  private void printExecutionPlan(OTodoResultSet result) {
    printExecutionPlan(null, result);
  }

  private void printExecutionPlan(String query, OTodoResultSet result) {
    if (query != null) {
      System.out.println(query);
    }
    result.getExecutionPlan().ifPresent(x -> System.out.println(x.prettyPrint(0, 3)));
    System.out.println();
  }

}
