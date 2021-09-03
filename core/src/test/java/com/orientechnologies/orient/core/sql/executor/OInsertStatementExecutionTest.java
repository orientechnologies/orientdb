package com.orientechnologies.orient.core.sql.executor;

import static com.orientechnologies.orient.core.sql.executor.ExecutionPlanPrintUtils.printExecutionPlan;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/** @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com) */
public class OInsertStatementExecutionTest {
  static ODatabaseDocument db;

  @BeforeClass
  public static void beforeClass() {
    db = new ODatabaseDocumentTx("memory:OInsertStatementExecutionTest");
    db.create();
  }

  @AfterClass
  public static void afterClass() {
    db.close();
  }

  @Test
  public void testInsertSet() {
    String className = "testInsertSet";
    db.getMetadata().getSchema().createClass(className);

    OResultSet result = db.command("insert into " + className + " set name = 'name1'");
    printExecutionPlan(result);
    for (int i = 0; i < 1; i++) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Assert.assertNotNull(item);
      Assert.assertEquals("name1", item.getProperty("name"));
    }
    Assert.assertFalse(result.hasNext());

    result = db.query("select from " + className);
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
  public void testInsertValue() {
    String className = "testInsertValue";
    db.getMetadata().getSchema().createClass(className);

    OResultSet result =
        db.command("insert into " + className + "  (name, surname) values ('name1', 'surname1')");
    printExecutionPlan(result);
    for (int i = 0; i < 1; i++) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Assert.assertNotNull(item);
      Assert.assertEquals("name1", item.getProperty("name"));
      Assert.assertEquals("surname1", item.getProperty("surname"));
    }
    Assert.assertFalse(result.hasNext());

    result = db.query("select from " + className);
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
  public void testInsertValue2() {
    String className = "testInsertValue2";
    db.getMetadata().getSchema().createClass(className);

    OResultSet result =
        db.command(
            "insert into "
                + className
                + "  (name, surname) values ('name1', 'surname1'), ('name2', 'surname2')");
    printExecutionPlan(result);

    for (int i = 0; i < 2; i++) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Assert.assertNotNull(item);
      Assert.assertEquals("name" + (i + 1), item.getProperty("name"));
      Assert.assertEquals("surname" + (i + 1), item.getProperty("surname"));
    }
    Assert.assertFalse(result.hasNext());

    Set<String> names = new HashSet<>();
    names.add("name1");
    names.add("name2");
    result = db.query("select from " + className);
    for (int i = 0; i < 2; i++) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Assert.assertNotNull(item);
      Assert.assertNotNull(item.getProperty("name"));
      names.remove(item.getProperty("name"));
      Assert.assertNotNull(item.getProperty("surname"));
    }
    Assert.assertFalse(result.hasNext());
    Assert.assertTrue(names.isEmpty());
    result.close();
  }

  @Test
  public void testInsertFromSelect1() {
    String className1 = "testInsertFromSelect1";
    db.getMetadata().getSchema().createClass(className1);

    String className2 = "testInsertFromSelect1_1";
    db.getMetadata().getSchema().createClass(className2);
    for (int i = 0; i < 10; i++) {
      ODocument doc = db.newInstance(className1);
      doc.setProperty("name", "name" + i);
      doc.setProperty("surname", "surname" + i);
      doc.save();
    }
    OResultSet result = db.command("insert into " + className2 + " from select from " + className1);
    printExecutionPlan(result);

    for (int i = 0; i < 10; i++) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Assert.assertNotNull(item);
      Assert.assertNotNull(item.getProperty("name"));
      Assert.assertNotNull(item.getProperty("surname"));
    }
    Assert.assertFalse(result.hasNext());

    Set<String> names = new HashSet<>();
    for (int i = 0; i < 10; i++) {
      names.add("name" + i);
    }
    result = db.query("select from " + className2);
    for (int i = 0; i < 10; i++) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Assert.assertNotNull(item);
      Assert.assertNotNull(item.getProperty("name"));
      names.remove(item.getProperty("name"));
      Assert.assertNotNull(item.getProperty("surname"));
    }
    Assert.assertFalse(result.hasNext());
    Assert.assertTrue(names.isEmpty());
    result.close();
  }

  @Test
  public void testInsertFromSelect2() {
    String className1 = "testInsertFromSelect2";
    db.getMetadata().getSchema().createClass(className1);

    String className2 = "testInsertFromSelect2_1";
    db.getMetadata().getSchema().createClass(className2);
    for (int i = 0; i < 10; i++) {
      ODocument doc = db.newInstance(className1);
      doc.setProperty("name", "name" + i);
      doc.setProperty("surname", "surname" + i);
      doc.save();
    }
    OResultSet result =
        db.command("insert into " + className2 + " ( select from " + className1 + ")");
    printExecutionPlan(result);

    for (int i = 0; i < 10; i++) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Assert.assertNotNull(item);
      Assert.assertNotNull(item.getProperty("name"));
      Assert.assertNotNull(item.getProperty("surname"));
    }
    Assert.assertFalse(result.hasNext());

    Set<String> names = new HashSet<>();
    for (int i = 0; i < 10; i++) {
      names.add("name" + i);
    }
    result = db.query("select from " + className2);
    for (int i = 0; i < 10; i++) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Assert.assertNotNull(item);
      Assert.assertNotNull(item.getProperty("name"));
      names.remove(item.getProperty("name"));
      Assert.assertNotNull(item.getProperty("surname"));
    }
    Assert.assertFalse(result.hasNext());
    Assert.assertTrue(names.isEmpty());
    result.close();
  }

  @Test
  public void testContent() {
    String className = "testContent";
    db.getMetadata().getSchema().createClass(className);

    OResultSet result =
        db.command("insert into " + className + " content {'name':'name1', 'surname':'surname1'}");
    printExecutionPlan(result);
    for (int i = 0; i < 1; i++) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Assert.assertNotNull(item);
      Assert.assertEquals("name1", item.getProperty("name"));
    }
    Assert.assertFalse(result.hasNext());

    result = db.query("select from " + className);
    for (int i = 0; i < 1; i++) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Assert.assertNotNull(item);
      Assert.assertEquals("name1", item.getProperty("name"));
      Assert.assertEquals("surname1", item.getProperty("surname"));
    }
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testContentWithParam() {
    String className = "testContentWithParam";
    db.getMetadata().getSchema().createClass(className);

    Map<String, Object> theContent = new HashMap<>();
    theContent.put("name", "name1");
    theContent.put("surname", "surname1");
    Map<String, Object> params = new HashMap<>();
    params.put("theContent", theContent);
    OResultSet result = db.command("insert into " + className + " content :theContent", params);
    printExecutionPlan(result);
    for (int i = 0; i < 1; i++) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Assert.assertNotNull(item);
      Assert.assertEquals("name1", item.getProperty("name"));
    }
    Assert.assertFalse(result.hasNext());

    result = db.query("select from " + className);
    for (int i = 0; i < 1; i++) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Assert.assertNotNull(item);
      Assert.assertEquals("name1", item.getProperty("name"));
      Assert.assertEquals("surname1", item.getProperty("surname"));
    }
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testLinkConversion() {
    String className1 = "testLinkConversion1";
    String className2 = "testLinkConversion2";

    db.command("CREATE CLASS " + className1).close();
    db.command("INSERT INTO " + className1 + " SET name='Active';").close();
    db.command("INSERT INTO " + className1 + " SET name='Inactive';").close();

    db.command("CREATE CLASS " + className2 + ";").close();
    db.command("CREATE PROPERTY " + className2 + ".processingType LINK " + className1 + ";")
        .close();

    db.command(
            "INSERT INTO "
                + className2
                + " SET name='Active', processingType = (SELECT FROM "
                + className1
                + " WHERE name = 'Active') ;")
        .close();
    db.command(
            "INSERT INTO "
                + className2
                + " SET name='Inactive', processingType = (SELECT FROM "
                + className1
                + " WHERE name = 'Inactive') ;")
        .close();

    OResultSet result = db.query("SELECT FROM " + className2);
    for (int i = 0; i < 2; i++) {
      Assert.assertTrue(result.hasNext());
      OResult row = result.next();
      Object val = row.getProperty("processingType");
      Assert.assertNotNull(val);
      Assert.assertTrue(val instanceof OIdentifiable);
    }
    result.close();
  }

  @Test
  public void testEmbeddedlistConversion() {
    String className1 = "testEmbeddedlistConversion1";
    String className2 = "testEmbeddedlistConversion2";

    db.command("CREATE CLASS " + className1).close();

    db.command("CREATE CLASS " + className2 + ";").close();
    db.command("CREATE PROPERTY " + className2 + ".sub EMBEDDEDLIST " + className1 + ";").close();

    db.command("INSERT INTO " + className2 + " SET name='Active', sub = [{'name':'foo'}];").close();

    OResultSet result = db.query("SELECT FROM " + className2);
    for (int i = 0; i < 1; i++) {
      Assert.assertTrue(result.hasNext());
      OResult row = result.next();
      Object list = row.getProperty("sub");
      Assert.assertNotNull(list);
      Assert.assertTrue(list instanceof List);
      Assert.assertEquals(1, ((List) list).size());

      Object o = ((List) list).get(0);
      Assert.assertTrue(o instanceof OResult);
      Assert.assertEquals("foo", ((OResult) o).getProperty("name"));
      Assert.assertEquals(className1, ((OResult) o).toElement().getSchemaType().get().getName());
    }
    result.close();
  }

  @Test
  public void testEmbeddedlistConversion2() {
    String className1 = "testEmbeddedlistConversion21";
    String className2 = "testEmbeddedlistConversion22";

    db.command("CREATE CLASS " + className1).close();

    db.command("CREATE CLASS " + className2 + ";").close();
    db.command("CREATE PROPERTY " + className2 + ".sub EMBEDDEDLIST " + className1 + ";").close();

    db.command("INSERT INTO " + className2 + " (name, sub) values ('Active', [{'name':'foo'}]);")
        .close();

    OResultSet result = db.query("SELECT FROM " + className2);
    for (int i = 0; i < 1; i++) {
      Assert.assertTrue(result.hasNext());
      OResult row = result.next();
      Object list = row.getProperty("sub");
      Assert.assertNotNull(list);
      Assert.assertTrue(list instanceof List);
      Assert.assertEquals(1, ((List) list).size());

      Object o = ((List) list).get(0);
      Assert.assertTrue(o instanceof OResult);
      Assert.assertEquals("foo", ((OResult) o).getProperty("name"));
      Assert.assertEquals(className1, ((OResult) o).toElement().getSchemaType().get().getName());
    }
    result.close();
  }

  @Test
  public void testInsertReturn() {
    String className = "testInsertReturn";
    db.getMetadata().getSchema().createClass(className);

    OResultSet result =
        db.command("insert into " + className + " set name = 'name1' RETURN 'OK' as result");
    printExecutionPlan(result);
    for (int i = 0; i < 1; i++) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Assert.assertNotNull(item);
      Assert.assertEquals("OK", item.getProperty("result"));
    }
    Assert.assertFalse(result.hasNext());

    result = db.query("select from " + className);
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
  public void testNestedInsert() {
    String className = "testNestedInsert";
    db.getMetadata().getSchema().createClass(className);

    OResultSet result =
        db.command(
            "insert into "
                + className
                + " set name = 'parent', children = (INSERT INTO "
                + className
                + " SET name = 'child')");

    result.close();

    result = db.query("SELECT FROM " + className);

    for (int i = 0; i < 2; i++) {
      OResult item = result.next();
      if (item.getProperty("name").equals("parent")) {
        Assert.assertTrue(item.getProperty("children") instanceof Collection);
        Assert.assertEquals(1, ((Collection) item.getProperty("children")).size());
      }
    }
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testLinkMapWithSubqueries() {
    String className = "testLinkMapWithSubqueries";
    String itemclassName = "testLinkMapWithSubqueriesTheItem";

    db.command("CREATE CLASS " + className);
    db.command("CREATE CLASS " + itemclassName);
    db.command("CREATE PROPERTY " + className + ".mymap LINKMAP " + itemclassName);

    db.command("INSERT INTO " + itemclassName + " (name) VALUES ('test')");
    db.command(
        "INSERT INTO "
            + className
            + " (mymap) VALUES ({'A-1': (SELECT FROM "
            + itemclassName
            + " WHERE name = 'test')})");

    OResultSet result = db.query("SELECT FROM " + className);

    OResult item = result.next();
    Map theMap = item.getProperty("mymap");
    Assert.assertEquals(1, theMap.size());
    Assert.assertNotNull(theMap.get("A-1"));

    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testQuotedCharactersInJson() {
    String className = "testQuotedCharactersInJson";

    db.command("CREATE CLASS " + className);

    db.command(
        "INSERT INTO "
            + className
            + " CONTENT { name: \"jack\", memo: \"this is a \\n multi line text\" }");

    OResultSet result = db.query("SELECT FROM " + className);

    OResult item = result.next();
    String memo = item.getProperty("memo");
    Assert.assertEquals("this is a \n multi line text", memo);

    Assert.assertFalse(result.hasNext());
    result.close();
  }
}
