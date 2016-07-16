package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

/**
 * Created by luigidellaquila on 07/07/16.
 */
public class OSelectStatementExecutionTest {
  static ODatabaseDocumentTx db;

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
    OTodoResultSet result = db.query("select name from " + className );
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
    }catch(Exception e){
      Assert.fail();
    }
  }

  @Test public void testAggretateMixedWithNonAggregate() {
    String className = "testAggretateMixedWithNonAggregate";
    db.getMetadata().getSchema().createClass(className);

    try {
      db.query("select max(a) + max(b) + pippo + pluto as foo, max(d) + max(e), f from " + className);
      Assert.fail();
    }catch(OCommandExecutionException x){

    }catch(Exception e){
      Assert.fail();
    }
  }

  @Test public void testAggretateMixedWithNonAggregateInCollection() {
    String className = "testAggretateMixedWithNonAggregateInCollection";
    db.getMetadata().getSchema().createClass(className);

    try {
      db.query("select [max(a), max(b), foo] from " + className);
      Assert.fail();
    }catch(OCommandExecutionException x){

    }catch(Exception e){
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
    }catch(Exception x){
      Assert.fail();
    }
  }

  @Test public void testAggretateMixedWithNonAggregateConstants() {
    String className = "testAggretateMixedWithNonAggregateConstants";
    db.getMetadata().getSchema().createClass(className);

    try {
      OTodoResultSet result = db.query("select max(a + b) + (max(b + c * 2) + 1 + 2) * 3 as foo, max(d) + max(e), f from " + className);
      printExecutionPlan(result);
    }catch(Exception e){
      e.printStackTrace();
      Assert.fail();
    }
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
    if(query!=null){
      System.out.println(query);
    }
    result.getExecutionPlan().ifPresent(x -> System.out.println(x.prettyPrint(3)));
  }

}
