package com.orientechnologies.orient.core.sql.executor;

import static com.orientechnologies.orient.core.sql.executor.ExecutionPlanPrintUtils.printExecutionPlan;

import com.orientechnologies.orient.core.OCreateDatabaseUtil;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.viewmanager.ViewCreationListener;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.metadata.schema.OViewConfig;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

/** @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com) */
public class OUpdateStatementExecutionTest {
  @Rule public TestName name = new TestName();

  private ODatabaseDocument db;

  private String className;
  private OrientDB orientDB;

  @Before
  public void before() {
    orientDB =
        OCreateDatabaseUtil.createDatabase(
            name.getMethodName(), "embedded:", OCreateDatabaseUtil.TYPE_MEMORY);
    db = orientDB.open(name.getMethodName(), "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);

    className = name.getMethodName();
    OClass clazz = db.getMetadata().getSchema().createClass(className);

    for (int i = 0; i < 10; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("name", "name" + i);
      doc.setProperty("surname", "surname" + i);
      doc.setProperty("number", 4L);

      List<String> tagsList = new ArrayList<>();
      tagsList.add("foo");
      tagsList.add("bar");
      tagsList.add("baz");
      doc.setProperty("tagsList", tagsList);

      Map<String, String> tagsMap = new HashMap<>();
      tagsMap.put("foo", "foo");
      tagsMap.put("bar", "bar");
      tagsMap.put("baz", "baz");
      doc.setProperty("tagsMap", tagsMap);

      doc.save();
    }
  }

  @After
  public void after() {
    db.close();

    orientDB.close();
  }

  @Test
  public void testSetString() {
    OResultSet result = db.command("update " + className + " set surname = 'foo'");
    printExecutionPlan(result);
    Assert.assertTrue(result.hasNext());
    OResult item = result.next();
    Assert.assertNotNull(item);
    Assert.assertEquals((Object) 10L, item.getProperty("count"));

    Assert.assertFalse(result.hasNext());
    result.close();

    result = db.query("select from " + className);
    for (int i = 0; i < 10; i++) {
      Assert.assertTrue(result.hasNext());
      item = result.next();
      Assert.assertNotNull(item);
      Assert.assertEquals("foo", item.getProperty("surname"));
    }
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testCopyField() {
    OResultSet result = db.command("update " + className + " set surname = name");

    Assert.assertTrue(result.hasNext());
    OResult item = result.next();
    Assert.assertNotNull(item);
    Assert.assertEquals((Object) 10L, item.getProperty("count"));
    Assert.assertFalse(result.hasNext());
    result.close();

    result = db.query("select from " + className);
    for (int i = 0; i < 10; i++) {
      Assert.assertTrue(result.hasNext());
      item = result.next();
      Assert.assertNotNull(item);
      Assert.assertEquals((Object) item.getProperty("name"), item.getProperty("surname"));
    }
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testSetExpression() {
    OResultSet result = db.command("update " + className + " set surname = 'foo'+name ");
    Assert.assertTrue(result.hasNext());
    OResult item = result.next();
    Assert.assertNotNull(item);
    Assert.assertEquals((Object) 10L, item.getProperty("count"));
    Assert.assertFalse(result.hasNext());
    result.close();

    result = db.query("select from " + className);
    for (int i = 0; i < 10; i++) {
      Assert.assertTrue(result.hasNext());
      item = result.next();
      Assert.assertNotNull(item);
      Assert.assertEquals("foo" + item.getProperty("name"), item.getProperty("surname"));
    }
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testConditionalSet() {
    OResultSet result =
        db.command("update " + className + " set surname = 'foo' where name = 'name3'");
    Assert.assertTrue(result.hasNext());
    OResult item = result.next();
    Assert.assertNotNull(item);
    Assert.assertEquals((Object) 1L, item.getProperty("count"));
    Assert.assertFalse(result.hasNext());
    result.close();

    result = db.query("select from " + className);
    boolean found = false;
    for (int i = 0; i < 10; i++) {
      Assert.assertTrue(result.hasNext());
      item = result.next();
      Assert.assertNotNull(item);
      if ("name3".equals(item.getProperty("name"))) {
        Assert.assertEquals("foo", item.getProperty("surname"));
        found = true;
      }
    }
    Assert.assertTrue(found);
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testSetOnList() {
    OResultSet result =
        db.command("update " + className + " set tagsList[0] = 'abc' where name = 'name3'");
    printExecutionPlan(result);
    Assert.assertTrue(result.hasNext());
    OResult item = result.next();
    Assert.assertNotNull(item);
    Assert.assertEquals((Object) 1L, item.getProperty("count"));
    Assert.assertFalse(result.hasNext());
    result.close();

    result = db.query("select from " + className);
    boolean found = false;
    for (int i = 0; i < 10; i++) {
      Assert.assertTrue(result.hasNext());
      item = result.next();
      Assert.assertNotNull(item);
      if ("name3".equals(item.getProperty("name"))) {
        List<String> tags = new ArrayList<>();
        tags.add("abc");
        tags.add("bar");
        tags.add("baz");
        Assert.assertEquals(tags, item.getProperty("tagsList"));
        found = true;
      }
    }
    Assert.assertTrue(found);
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testSetOnList2() {
    OResultSet result =
        db.command("update " + className + " set tagsList[6] = 'abc' where name = 'name3'");
    printExecutionPlan(result);
    Assert.assertTrue(result.hasNext());
    OResult item = result.next();
    Assert.assertNotNull(item);
    Assert.assertEquals((Object) 1L, item.getProperty("count"));
    Assert.assertFalse(result.hasNext());
    result.close();

    result = db.query("select from " + className);
    boolean found = false;
    for (int i = 0; i < 10; i++) {
      Assert.assertTrue(result.hasNext());
      item = result.next();
      Assert.assertNotNull(item);
      if ("name3".equals(item.getProperty("name"))) {
        List<String> tags = new ArrayList<>();
        tags.add("foo");
        tags.add("bar");
        tags.add("baz");
        tags.add(null);
        tags.add(null);
        tags.add(null);
        tags.add("abc");
        Assert.assertEquals(tags, item.getProperty("tagsList"));
        found = true;
      }
    }
    Assert.assertTrue(found);
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testSetOnMap() {
    OResultSet result =
        db.command("update " + className + " set tagsMap['foo'] = 'abc' where name = 'name3'");
    Assert.assertTrue(result.hasNext());
    OResult item = result.next();
    Assert.assertNotNull(item);
    Assert.assertEquals((Object) 1L, item.getProperty("count"));
    Assert.assertFalse(result.hasNext());
    result.close();

    result = db.query("select from " + className);
    boolean found = false;
    for (int i = 0; i < 10; i++) {
      Assert.assertTrue(result.hasNext());
      item = result.next();
      Assert.assertNotNull(item);
      if ("name3".equals(item.getProperty("name"))) {
        Map<String, String> tags = new HashMap<>();
        tags.put("foo", "abc");
        tags.put("bar", "bar");
        tags.put("baz", "baz");
        Assert.assertEquals(tags, item.getProperty("tagsMap"));
        found = true;
      } else {
        Map<String, String> tags = new HashMap<>();
        tags.put("foo", "foo");
        tags.put("bar", "bar");
        tags.put("baz", "baz");
        Assert.assertEquals(tags, item.getProperty("tagsMap"));
      }
    }
    Assert.assertTrue(found);
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testPlusAssign() {
    OResultSet result =
        db.command("update " + className + " set name += 'foo', newField += 'bar', number += 5");
    Assert.assertTrue(result.hasNext());
    OResult item = result.next();
    Assert.assertNotNull(item);
    Assert.assertEquals((Object) 10L, item.getProperty("count"));
    Assert.assertFalse(result.hasNext());
    result.close();

    result = db.query("select from " + className);
    for (int i = 0; i < 10; i++) {
      Assert.assertTrue(result.hasNext());
      item = result.next();
      Assert.assertNotNull(item);
      Assert.assertTrue(
          item.getProperty("name").toString().endsWith("foo")); // test concatenate string to string
      Assert.assertEquals(8, item.getProperty("name").toString().length());
      Assert.assertEquals("bar", item.getProperty("newField")); // test concatenate null to string
      Assert.assertEquals((Object) 9L, item.getProperty("number")); // test sum numbers
    }
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testMinusAssign() {
    OResultSet result = db.command("update " + className + " set number -= 5");
    printExecutionPlan(result);
    Assert.assertTrue(result.hasNext());
    OResult item = result.next();
    Assert.assertNotNull(item);
    Assert.assertEquals((Object) 10L, item.getProperty("count"));
    Assert.assertFalse(result.hasNext());
    result.close();

    result = db.query("select from " + className);
    for (int i = 0; i < 10; i++) {
      Assert.assertTrue(result.hasNext());
      item = result.next();
      Assert.assertNotNull(item);
      Assert.assertEquals((Object) (-1L), item.getProperty("number"));
    }
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testStarAssign() {
    OResultSet result = db.command("update " + className + " set number *= 5");
    Assert.assertTrue(result.hasNext());
    OResult item = result.next();
    Assert.assertNotNull(item);
    Assert.assertEquals((Object) 10L, item.getProperty("count"));
    Assert.assertFalse(result.hasNext());
    result.close();

    result = db.query("select from " + className);
    for (int i = 0; i < 10; i++) {
      Assert.assertTrue(result.hasNext());
      item = result.next();
      Assert.assertNotNull(item);
      Assert.assertEquals((Object) 20L, item.getProperty("number"));
    }
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testSlashAssign() {
    OResultSet result = db.command("update " + className + " set number /= 2");
    Assert.assertTrue(result.hasNext());
    OResult item = result.next();
    Assert.assertNotNull(item);
    Assert.assertEquals((Object) 10L, item.getProperty("count"));
    Assert.assertFalse(result.hasNext());
    result.close();

    result = db.query("select from " + className);
    for (int i = 0; i < 10; i++) {
      Assert.assertTrue(result.hasNext());
      item = result.next();
      Assert.assertNotNull(item);
      Assert.assertEquals((Object) 2L, item.getProperty("number"));
    }
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testRemove() {
    OResultSet result = db.query("select from " + className);
    for (int i = 0; i < 10; i++) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Assert.assertNotNull(item);
      Assert.assertNotNull(item.getProperty("surname"));
    }

    result.close();
    result = db.command("update " + className + " remove surname");
    for (int i = 0; i < 1; i++) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Assert.assertNotNull(item);
      Assert.assertEquals((Object) 10L, item.getProperty("count"));
    }
    Assert.assertFalse(result.hasNext());
    result.close();

    result = db.query("select from " + className);
    for (int i = 0; i < 10; i++) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Assert.assertNotNull(item);
      Assert.assertNull(item.getProperty("surname"));
    }
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testContent() {

    OResultSet result =
        db.command("update " + className + " content {'name': 'foo', 'secondName': 'bar'}");
    Assert.assertTrue(result.hasNext());
    OResult item = result.next();
    Assert.assertNotNull(item);
    Assert.assertEquals((Object) 10L, item.getProperty("count"));
    Assert.assertFalse(result.hasNext());
    result.close();

    result = db.query("select from " + className);
    for (int i = 0; i < 10; i++) {
      Assert.assertTrue(result.hasNext());
      item = result.next();
      Assert.assertNotNull(item);
      Assert.assertEquals("foo", item.getProperty("name"));
      Assert.assertEquals("bar", item.getProperty("secondName"));
      Assert.assertNull(item.getProperty("surname"));
    }
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testMerge() {

    OResultSet result =
        db.command("update " + className + " merge {'name': 'foo', 'secondName': 'bar'}");
    Assert.assertTrue(result.hasNext());
    OResult item = result.next();
    Assert.assertNotNull(item);
    Assert.assertEquals((Object) 10L, item.getProperty("count"));
    Assert.assertFalse(result.hasNext());
    result.close();

    result = db.query("select from " + className);
    for (int i = 0; i < 10; i++) {
      Assert.assertTrue(result.hasNext());
      item = result.next();
      Assert.assertNotNull(item);
      Assert.assertEquals("foo", item.getProperty("name"));
      Assert.assertEquals("bar", item.getProperty("secondName"));
      Assert.assertTrue(item.getProperty("surname").toString().startsWith("surname"));
    }
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testUpsert1() {

    OResultSet result =
        db.command("update " + className + " set foo = 'bar' upsert where name = 'name1'");
    Assert.assertTrue(result.hasNext());
    OResult item = result.next();
    Assert.assertNotNull(item);
    Assert.assertEquals((Object) 1L, item.getProperty("count"));
    Assert.assertFalse(result.hasNext());
    result.close();

    result = db.query("select from " + className);
    for (int i = 0; i < 10; i++) {
      Assert.assertTrue(result.hasNext());
      item = result.next();
      Assert.assertNotNull(item);
      String name = item.getProperty("name");
      Assert.assertNotNull(name);
      if ("name1".equals(name)) {
        Assert.assertEquals("bar", item.getProperty("foo"));
      } else {
        Assert.assertNull(item.getProperty("foo"));
      }
    }
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testUpsertAndReturn() {

    OResultSet result =
        db.command(
            "update " + className + " set foo = 'bar' upsert  return after  where name = 'name1' ");

    Assert.assertTrue(result.hasNext());
    OResult item = result.next();
    Assert.assertNotNull(item);
    Assert.assertEquals("bar", item.getProperty("foo"));
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testUpsert2() {

    OResultSet result =
        db.command("update " + className + " set foo = 'bar' upsert where name = 'name11'");
    Assert.assertTrue(result.hasNext());
    OResult item = result.next();
    Assert.assertNotNull(item);
    Assert.assertEquals((Object) 1L, item.getProperty("count"));
    Assert.assertFalse(result.hasNext());
    result.close();

    result = db.query("select from " + className);
    for (int i = 0; i < 11; i++) {
      Assert.assertTrue(result.hasNext());
      item = result.next();
      Assert.assertNotNull(item);
      String name = item.getProperty("name");
      Assert.assertNotNull(name);
      if ("name11".equals(name)) {
        Assert.assertEquals("bar", item.getProperty("foo"));
      } else {
        Assert.assertNull(item.getProperty("foo"));
      }
    }
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testRemove1() {
    String className = "overridden" + this.className;

    OClass clazz = db.getMetadata().getSchema().createClass(className);
    clazz.createProperty("theProperty", OType.EMBEDDEDLIST);

    ODocument doc = db.newInstance(className);
    List theList = new ArrayList();
    for (int i = 0; i < 10; i++) {
      theList.add("n" + i);
    }
    doc.setProperty("theProperty", theList);

    doc.save();

    OResultSet result = db.command("update " + className + " remove theProperty[0]");
    printExecutionPlan(result);
    Assert.assertTrue(result.hasNext());
    OResult item = result.next();
    Assert.assertNotNull(item);
    Assert.assertFalse(result.hasNext());
    result.close();

    result = db.query("select from " + className);
    Assert.assertTrue(result.hasNext());
    item = result.next();
    Assert.assertNotNull(item);
    List ls = item.getProperty("theProperty");
    Assert.assertNotNull(ls);
    Assert.assertEquals(9, ls.size());
    Assert.assertFalse(ls.contains("n0"));
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testRemove2() {
    String className = "overridden" + this.className;
    OClass clazz = db.getMetadata().getSchema().createClass(className);
    clazz.createProperty("theProperty", OType.EMBEDDEDLIST);

    ODocument doc = db.newInstance(className);
    List theList = new ArrayList();
    for (int i = 0; i < 10; i++) {
      theList.add("n" + i);
    }
    doc.setProperty("theProperty", theList);

    doc.save();

    OResultSet result = db.command("update " + className + " remove theProperty[0, 1, 3]");
    printExecutionPlan(result);
    Assert.assertTrue(result.hasNext());
    OResult item = result.next();
    Assert.assertNotNull(item);
    Assert.assertFalse(result.hasNext());
    result.close();

    result = db.query("select from " + className);
    Assert.assertTrue(result.hasNext());
    item = result.next();
    Assert.assertNotNull(item);
    List ls = item.getProperty("theProperty");

    Assertions.assertThat(ls)
        .isNotNull()
        .hasSize(7)
        .doesNotContain("n0")
        .doesNotContain("n1")
        .contains("n2")
        .doesNotContain("n3")
        .contains("n4");

    //    Assert.assertNotNull(ls);
    //    Assert.assertEquals(7, ls.size());
    //    Assert.assertFalse(ls.contains("n0"));
    //    Assert.assertFalse(ls.contains("n1"));
    //    Assert.assertTrue(ls.contains("n2"));
    //    Assert.assertFalse(ls.contains("n3"));
    //    Assert.assertTrue(ls.contains("n4"));

    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testRemove3() {
    String className = "overriden" + this.className;
    OClass clazz = db.getMetadata().getSchema().createClass(className);
    clazz.createProperty("theProperty", OType.EMBEDDED);

    ODocument doc = db.newInstance(className);
    ODocument emb = new ODocument();
    emb.setProperty("sub", "foo");
    emb.setProperty("aaa", "bar");
    doc.setProperty("theProperty", emb);

    doc.save();

    OResultSet result = db.command("update " + className + " remove theProperty.sub");
    printExecutionPlan(result);
    Assert.assertTrue(result.hasNext());
    OResult item = result.next();
    Assert.assertNotNull(item);
    Assert.assertFalse(result.hasNext());
    result.close();

    result = db.query("select from " + className);
    Assert.assertTrue(result.hasNext());
    item = result.next();
    Assert.assertNotNull(item);
    OResult ls = item.getProperty("theProperty");
    Assert.assertNotNull(ls);
    Assert.assertFalse(ls.getPropertyNames().contains("sub"));
    Assert.assertEquals("bar", ls.getProperty("aaa"));
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testRemoveFromMapSquare() {

    db.command("UPDATE " + className + " REMOVE tagsMap[\"bar\"]").close();

    OResultSet result = db.query("SELECT tagsMap FROM " + className);
    printExecutionPlan(result);
    for (int i = 0; i < 10; i++) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Assert.assertNotNull(item);
      Assert.assertEquals(2, ((Map) item.getProperty("tagsMap")).size());
      Assert.assertFalse(((Map) item.getProperty("tagsMap")).containsKey("bar"));
    }
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testRemoveFromMapEquals() {

    db.command("UPDATE " + className + " REMOVE tagsMap = \"bar\"").close();

    OResultSet result = db.query("SELECT tagsMap FROM " + className);
    printExecutionPlan(result);
    for (int i = 0; i < 10; i++) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Assert.assertNotNull(item);
      Assert.assertEquals(2, ((Map) item.getProperty("tagsMap")).size());
      Assert.assertFalse(((Map) item.getProperty("tagsMap")).containsKey("bar"));
    }
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testUpdateView() throws InterruptedException {

    String viewName = "testUpdateViewView";
    OViewConfig cfg = new OViewConfig(viewName, "SELECT FROM " + className);
    cfg.setUpdatable(true);
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

    db.command("UPDATE " + viewName + " SET aNewProp = \"newPropValue\"").close();

    OResultSet result = db.query("SELECT aNewProp FROM " + viewName);
    for (int i = 0; i < 10; i++) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Assert.assertNotNull(item);
      Assert.assertEquals("newPropValue", item.getProperty("aNewProp"));
    }
    Assert.assertFalse(result.hasNext());
    result.close();

    result = db.query("SELECT aNewProp FROM " + className);
    for (int i = 0; i < 10; i++) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Assert.assertNotNull(item);
      Assert.assertEquals("newPropValue", item.getProperty("aNewProp"));
    }
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testReturnBefore() {
    OResultSet result =
        db.command("update " + className + " set name = 'foo' RETURN BEFORE where name = 'name1'");
    printExecutionPlan(result);
    Assert.assertTrue(result.hasNext());
    OResult item = result.next();
    Assert.assertNotNull(item);
    Assert.assertEquals("name1", item.getProperty("name"));

    Assert.assertFalse(result.hasNext());
    result.close();
  }
}
