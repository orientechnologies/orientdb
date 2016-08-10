package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.impl.ODocument;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Luigi Dell'Aquila
 */
public class OUpdateStatementExecutionTest {
  static ODatabaseDocument db;

  @BeforeClass public static void beforeClass() {

    db = new ODatabaseDocumentTx("memory:OSelectStatementExecutionTest");
    db.create();
  }

  @AfterClass public static void afterClass() {
    db.close();
  }

  @Test public void testSetString() {
    String className = "testSetString";
    db.getMetadata().getSchema().createClass(className);
    for (int i = 0; i < 10; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("name", "name" + i);
      doc.save();
    }
    OTodoResultSet result = db.command("update " + className + " set surname = 'foo'");
    printExecutionPlan(result);
    for (int i = 0; i < 1; i++) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Assert.assertNotNull(item);
      Assert.assertEquals((Object) 10L, item.getProperty("count"));
    }
    Assert.assertFalse(result.hasNext());

    result = db.query("select from " + className);
    for (int i = 0; i < 10; i++) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Assert.assertNotNull(item);
      Assert.assertEquals("foo", item.getProperty("surname"));
    }
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test public void testCopyField() {
    String className = "testCopyField";
    db.getMetadata().getSchema().createClass(className);
    for (int i = 0; i < 10; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("name", "name" + i);
      doc.save();
    }
    OTodoResultSet result = db.command("update " + className + " set surname = name");
    printExecutionPlan(result);
    for (int i = 0; i < 1; i++) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Assert.assertNotNull(item);
      Assert.assertEquals((Object) 10L, item.getProperty("count"));
    }
    Assert.assertFalse(result.hasNext());

    result = db.query("select from " + className);
    for (int i = 0; i < 10; i++) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Assert.assertNotNull(item);
      Assert.assertEquals((Object) item.getProperty("name"), item.getProperty("surname"));
    }
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test public void testSetExpression() {
    String className = "testSetExpression";
    db.getMetadata().getSchema().createClass(className);
    for (int i = 0; i < 10; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("name", "name" + i);
      doc.save();
    }
    OTodoResultSet result = db.command("update " + className + " set surname = 'foo'+name ");
    printExecutionPlan(result);
    for (int i = 0; i < 1; i++) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Assert.assertNotNull(item);
      Assert.assertEquals((Object) 10L, item.getProperty("count"));
    }
    Assert.assertFalse(result.hasNext());

    result = db.query("select from " + className);
    for (int i = 0; i < 10; i++) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Assert.assertNotNull(item);
      Assert.assertEquals("foo" + item.getProperty("name"), item.getProperty("surname"));
    }
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test public void testConditionalSet() {
    String className = "testConditionalSet";
    db.getMetadata().getSchema().createClass(className);
    for (int i = 0; i < 10; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("name", "name" + i);
      doc.save();
    }
    OTodoResultSet result = db.command("update " + className + " set surname = 'foo' where name = 'name3'");
    printExecutionPlan(result);
    for (int i = 0; i < 1; i++) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Assert.assertNotNull(item);
      Assert.assertEquals((Object) 1L, item.getProperty("count"));
    }
    Assert.assertFalse(result.hasNext());

    result = db.query("select from " + className);
    boolean found = false;
    for (int i = 0; i < 10; i++) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
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

  @Test public void testSetOnList() {
    String className = "testSetOnList";
    db.getMetadata().getSchema().createClass(className);
    for (int i = 0; i < 10; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("name", "name" + i);
      List<String> tags = new ArrayList<>();
      tags.add("foo");
      tags.add("bar");
      tags.add("baz");
      doc.setProperty("tags", tags);
      doc.save();
    }
    OTodoResultSet result = db.command("update " + className + " set tags[0] = 'abc' where name = 'name3'");
    printExecutionPlan(result);
    for (int i = 0; i < 1; i++) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Assert.assertNotNull(item);
      Assert.assertEquals((Object) 1L, item.getProperty("count"));
    }
    Assert.assertFalse(result.hasNext());

    result = db.query("select from " + className);
    boolean found = false;
    for (int i = 0; i < 10; i++) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Assert.assertNotNull(item);
      if ("name3".equals(item.getProperty("name"))) {
        List<String> tags = new ArrayList<>();
        tags.add("abc");
        tags.add("bar");
        tags.add("baz");
        Assert.assertEquals(tags, item.getProperty("tags"));
        found = true;
      }
    }
    Assert.assertTrue(found);
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test public void testSetOnList2() {
    String className = "testSetOnList2";
    db.getMetadata().getSchema().createClass(className);
    for (int i = 0; i < 10; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("name", "name" + i);
      List<String> tags = new ArrayList<>();
      tags.add("foo");
      tags.add("bar");
      tags.add("baz");
      doc.setProperty("tags", tags);
      doc.save();
    }
    OTodoResultSet result = db.command("update " + className + " set tags[6] = 'abc' where name = 'name3'");
    printExecutionPlan(result);
    for (int i = 0; i < 1; i++) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Assert.assertNotNull(item);
      Assert.assertEquals((Object) 1L, item.getProperty("count"));
    }
    Assert.assertFalse(result.hasNext());

    result = db.query("select from " + className);
    boolean found = false;
    for (int i = 0; i < 10; i++) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
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
        Assert.assertEquals(tags, item.getProperty("tags"));
        found = true;
      }
    }
    Assert.assertTrue(found);
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test public void testSetOnMap() {
    String className = "testSetOnMap";
    db.getMetadata().getSchema().createClass(className);
    for (int i = 0; i < 10; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("name", "name" + i);
      Map<String, String> tags = new HashMap<>();
      tags.put("foo", "foo");
      tags.put("bar", "bar");
      tags.put("baz", "baz");
      doc.setProperty("tags", tags);
      doc.save();
    }
    OTodoResultSet result = db.command("update " + className + " set tags['foo'] = 'abc' where name = 'name3'");
    printExecutionPlan(result);
    for (int i = 0; i < 1; i++) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Assert.assertNotNull(item);
      Assert.assertEquals((Object) 1L, item.getProperty("count"));
    }
    Assert.assertFalse(result.hasNext());

    result = db.query("select from " + className);
    boolean found = false;
    for (int i = 0; i < 10; i++) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Assert.assertNotNull(item);
      if ("name3".equals(item.getProperty("name"))) {
        Map<String, String> tags = new HashMap<>();
        tags.put("foo", "abc");
        tags.put("bar", "bar");
        tags.put("baz", "baz");
        Assert.assertEquals(tags, item.getProperty("tags"));
        found = true;
      } else {
        Map<String, String> tags = new HashMap<>();
        tags.put("foo", "foo");
        tags.put("bar", "bar");
        tags.put("baz", "baz");
        Assert.assertEquals(tags, item.getProperty("tags"));
      }
    }
    Assert.assertTrue(found);
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test public void testPlusAssign() {
    String className = "testPlusAssign";
    db.getMetadata().getSchema().createClass(className);
    for (int i = 0; i < 10; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("name", "name" + i);
      doc.setProperty("number", 4L);
      doc.save();
    }
    OTodoResultSet result = db.command("update " + className + " set name += 'foo', surname += 'bar', number += 5");
    printExecutionPlan(result);
    for (int i = 0; i < 1; i++) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Assert.assertNotNull(item);
      Assert.assertEquals((Object) 10L, item.getProperty("count"));
    }
    Assert.assertFalse(result.hasNext());

    result = db.query("select from " + className);
    for (int i = 0; i < 10; i++) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Assert.assertNotNull(item);
      Assert.assertTrue(item.getProperty("name").toString().endsWith("foo")); //test concatenate string to string
      Assert.assertEquals(8, item.getProperty("name").toString().length());
      Assert.assertEquals("bar", item.getProperty("surname")); //test concatenate null to string
      Assert.assertEquals((Object)9L, item.getProperty("number")); //test sum numbers
    }
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test public void testMinusAssign() {
    String className = "testMinusAssign";
    db.getMetadata().getSchema().createClass(className);
    for (int i = 0; i < 10; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("name", "name" + i);
      doc.setProperty("number", 4L);
      doc.save();
    }
    OTodoResultSet result = db.command("update " + className + " set number -= 5");
    printExecutionPlan(result);
    for (int i = 0; i < 1; i++) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Assert.assertNotNull(item);
      Assert.assertEquals((Object) 10L, item.getProperty("count"));
    }
    Assert.assertFalse(result.hasNext());

    result = db.query("select from " + className);
    for (int i = 0; i < 10; i++) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Assert.assertNotNull(item);
      Assert.assertEquals((Object)(-1L), item.getProperty("number"));
    }
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test public void testStarAssign() {
    String className = "testStarAssign";
    db.getMetadata().getSchema().createClass(className);
    for (int i = 0; i < 10; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("name", "name" + i);
      doc.setProperty("number", 4L);
      doc.save();
    }
    OTodoResultSet result = db.command("update " + className + " set number *= 5");
    printExecutionPlan(result);
    for (int i = 0; i < 1; i++) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Assert.assertNotNull(item);
      Assert.assertEquals((Object) 10L, item.getProperty("count"));
    }
    Assert.assertFalse(result.hasNext());

    result = db.query("select from " + className);
    for (int i = 0; i < 10; i++) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Assert.assertNotNull(item);
      Assert.assertEquals((Object)20L, item.getProperty("number"));
    }
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test public void testSlashAssign() {
    String className = "testSlashAssign";
    db.getMetadata().getSchema().createClass(className);
    for (int i = 0; i < 10; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("name", "name" + i);
      doc.setProperty("number", 4L);
      doc.save();
    }
    OTodoResultSet result = db.command("update " + className + " set number /= 2");
    printExecutionPlan(result);
    for (int i = 0; i < 1; i++) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Assert.assertNotNull(item);
      Assert.assertEquals((Object) 10L, item.getProperty("count"));
    }
    Assert.assertFalse(result.hasNext());

    result = db.query("select from " + className);
    for (int i = 0; i < 10; i++) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Assert.assertNotNull(item);
      Assert.assertEquals((Object)2L, item.getProperty("number"));
    }
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test public void testRemove() {
    String className = "testRemove";
    db.getMetadata().getSchema().createClass(className);
    for (int i = 0; i < 10; i++) {
      ODocument doc = db.newInstance(className);
      doc.setProperty("name", "name" + i);
      doc.setProperty("surname", "surname" + i);
      doc.save();
    }

    OTodoResultSet result = db.query("select from " + className);
    for (int i = 0; i < 10; i++) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Assert.assertNotNull(item);
      Assert.assertNotNull(item.getProperty("surname"));
    }

    result = db.command("update " + className + " remove surname");
    printExecutionPlan(result);
    for (int i = 0; i < 1; i++) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Assert.assertNotNull(item);
      Assert.assertEquals((Object) 10L, item.getProperty("count"));
    }
    Assert.assertFalse(result.hasNext());

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
