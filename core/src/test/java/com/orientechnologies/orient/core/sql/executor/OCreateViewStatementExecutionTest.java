package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.viewmanager.ViewCreationListener;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OView;
import com.orientechnologies.orient.core.metadata.schema.OViewConfig;
import com.orientechnologies.orient.core.record.OElement;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/** @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com) */
public class OCreateViewStatementExecutionTest {
  static ODatabaseDocument db;

  @BeforeClass
  public static void beforeClass() {
    db = new ODatabaseDocumentTx("memory:OCreateViewStatementExecutionTest");
    db.create();
  }

  @AfterClass
  public static void afterClass() {
    db.close();
  }

  @Test
  public void testPlain() {
    String className = "testPlain";
    OResultSet result = db.command("create view " + className + "  FROM (SELECT FROM V)");
    OSchema schema = db.getMetadata().getSchema();
    OView view = schema.getView(className);
    Assert.assertNotNull(view);
    Assert.assertEquals(className, view.getName());
    result.close();
  }

  @Test
  public void testOriginField() throws InterruptedException {
    String className = "testOriginFieldClass";
    String viewName = "testOriginFieldView";
    db.createClass(className);

    OElement elem = db.newElement(className);
    elem.setProperty("name", "foo");
    elem.save();

    OViewConfig cfg = new OViewConfig(viewName, "SELECT FROM " + className);
    cfg.setOriginRidField("origin");
    CountDownLatch latch = new CountDownLatch(1);
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

    OResultSet rs = db.query("SELECT FROM " + viewName);
    Assert.assertTrue(rs.hasNext());
    OResult item = rs.next();
    Assert.assertEquals(elem.getIdentity(), item.getProperty("origin"));
    Assert.assertEquals("foo", item.getProperty("name"));
    Assert.assertFalse(rs.hasNext());
    rs.close();
  }

  @Test
  public void testMetadata() throws InterruptedException {
    String className = "testMetadataClass";
    String viewName = "testMetadata";
    db.createClass(className);

    String statement =
        "CREATE VIEW " + viewName + " FROM (SELECT FROM " + className + ") METADATA {";
    statement += "updatable:true, ";
    //    statement+="indexes...";
    statement += "updateStrategy: '" + OViewConfig.UPDATE_STRATEGY_LIVE + "', ";
    statement += "watchClasses:['foo', 'bar'], ";
    statement += "nodes:['baz','xx'], ";
    statement += "updateIntervalSeconds:100, ";
    statement += "originRidField:'pp' ";
    statement += "}";

    db.command(statement);

    OView view = db.getMetadata().getSchema().getView(viewName);
    Assert.assertTrue(view.isUpdatable());
    //    Assert.assertEquals(OViewConfig.UPDATE_STRATEGY_LIVE, view.get());
    Assert.assertTrue(view.getWatchClasses().contains("foo"));
    Assert.assertTrue(view.getWatchClasses().contains("bar"));
    Assert.assertEquals(2, view.getWatchClasses().size());
    Assert.assertTrue(view.getNodes().contains("baz"));
    Assert.assertTrue(view.getNodes().contains("xx"));
    Assert.assertEquals(2, view.getNodes().size());
    Assert.assertEquals(100, view.getUpdateIntervalSeconds());
    Assert.assertEquals("pp", view.getOriginRidField());
  }

  @Test
  public void testIndexes() throws InterruptedException {
    String className = "testIndexesClass";
    String viewName = "testIndexes";
    db.createClass(className);

    for (int i = 0; i < 10; i++) {
      OElement elem = db.newElement(className);
      elem.setProperty("name", "name" + i);
      elem.setProperty("surname", "surname" + i);
      elem.save();
    }

    String statement =
        "CREATE VIEW " + viewName + " FROM (SELECT FROM " + className + ") METADATA {";
    statement += "indexes: [{type:'NOTUNIQUE', properties:{name:'STRING', surname:'STRING'}}]";
    statement += "}";

    db.command(statement);

    Thread.sleep(1000);

    OResultSet result = db.query("SELECT FROM " + viewName + " WHERE name = 'name4'");
    Assert.assertTrue(
        result.getExecutionPlan().get().getSteps().stream()
            .anyMatch(x -> x instanceof FetchFromIndexStep));
    Assert.assertTrue(result.hasNext());
    result.next();
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testCollectionIndexes() throws InterruptedException {
    String className = "testCollectionIndexesClass";
    String viewName = "testCollectionIndexes";
    db.createClass(className);

    for (int i = 0; i < 10; i++) {
      OElement elem = db.newElement(className);
      elem.setProperty("name", "name" + i);
      elem.setProperty("data", Arrays.asList(new Integer[] {20 + i, 40 + i, 50 + i}));
      elem.save();
    }

    String statement =
        "CREATE VIEW " + viewName + " FROM (SELECT FROM " + className + ") METADATA {";
    statement +=
        "indexes: [{type:'NOTUNIQUE', properties:{name:'STRING'}},{type:'NOTUNIQUE', properties:{data:{type:'EMBEDDEDLIST',linkedType:'INTEGER'}}}]";
    statement += "}";

    db.command(statement);

    Thread.sleep(1000);

    OResultSet result = db.query("SELECT FROM " + viewName + " WHERE data = 22");
    Assert.assertTrue(
        result.getExecutionPlan().get().getSteps().stream()
            .anyMatch(x -> x instanceof FetchFromIndexStep));
    Assert.assertTrue(result.hasNext());
    result.next();
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testLiveUpdate() throws InterruptedException {
    String className = "testLiveUpdateClass";
    String viewName = "testLiveUpdate";
    db.createClass(className);

    for (int i = 0; i < 10; i++) {
      OElement elem = db.newElement(className);
      elem.setProperty("name", "name" + i);
      elem.setProperty("surname", "surname" + i);
      elem.save();
    }

    String statement =
        "CREATE VIEW " + viewName + " FROM (SELECT FROM " + className + ") METADATA {";
    statement += "updateStrategy:\"live\",";
    statement += "originRidField:\"origin\"";
    statement += "}";

    db.command(statement);

    Thread.sleep(1000);

    db.command("UPDATE " + className + " SET surname = 'changed' WHERE name = 'name3'");

    Thread.sleep(1000);

    OResultSet result = db.query("SELECT FROM " + viewName);
    for (int i = 0; i < 10; i++) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      if (item.getProperty("name").equals("name3")) {
        Assert.assertEquals("changed", item.getProperty("surname"));
      } else {
        Assert.assertEquals("sur" + item.getProperty("name"), item.getProperty("surname"));
      }
    }
    result.close();
  }

  @Test
  public void testLiveUpdateDelete() throws InterruptedException {
    String className = "testLiveUpdateDeleteClass";
    String viewName = "testLiveUpdateDelete";
    db.createClass(className);

    for (int i = 0; i < 10; i++) {
      OElement elem = db.newElement(className);
      elem.setProperty("name", "name" + i);
      elem.setProperty("surname", "surname" + i);
      elem.save();
    }

    String statement =
        "CREATE VIEW " + viewName + " FROM (SELECT FROM " + className + ") METADATA {";
    statement += "updateStrategy:\"live\",";
    statement += "originRidField:\"origin\"";
    statement += "}";

    db.command(statement);

    Thread.sleep(1000);

    OResultSet result = db.query("SELECT FROM " + viewName);
    Assert.assertEquals(10, result.stream().count());
    result.close();

    db.command("DELETE FROM " + className + " WHERE name = 'name3'");

    Thread.sleep(1000);
    result = db.query("SELECT FROM " + viewName);
    for (int i = 0; i < 9; i++) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Assert.assertNotEquals("name3", item.getProperty("name"));
    }
    result.close();
  }
}
