package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexCursor;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.*;

/**
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
public class OTruncateClassStatementExecutionTest {
  static ODatabaseDocument database;

  @BeforeClass public static void beforeClass() {
    database = new ODatabaseDocumentTx("memory:OTruncateClassStatementExecutionTest");
    database.create();
  }

  @AfterClass public static void afterClass() {
    database.close();
  }

  @SuppressWarnings("unchecked") @Test public void testTruncateClass() {

    OSchema schema = database.getMetadata().getSchema();
    OClass testClass = getOrCreateClass(schema);

    final OIndex<?> index = getOrCreateIndex(testClass);
    schema.save();

    database.command("truncate class test_class");

    database.save(new ODocument(testClass).field("name", "x").field("data", Arrays.asList(1, 2)));
    database.save(new ODocument(testClass).field("name", "y").field("data", Arrays.asList(3, 0)));

    database.command(new OCommandSQL("truncate class test_class")).execute();

    database.save(new ODocument(testClass).field("name", "x").field("data", Arrays.asList(5, 6, 7)));
    database.save(new ODocument(testClass).field("name", "y").field("data", Arrays.asList(8, 9, -1)));

    OResultSet result = database.query("select from test_class");
    //    Assert.assertEquals(result.size(), 2);

    Set<Integer> set = new HashSet<Integer>();
    while (result.hasNext()) {
      set.addAll((Collection<Integer>) result.next().getProperty("data"));
    }
    result.close();
    Assert.assertTrue(set.containsAll(Arrays.asList(5, 6, 7, 8, 9, -1)));

    Assert.assertEquals(index.getSize(), 6);

    OIndexCursor cursor = index.cursor();
    Map.Entry<Object, OIdentifiable> entry = cursor.nextEntry();
    while (entry != null) {
      Assert.assertTrue(set.contains((Integer) entry.getKey()));
      entry = cursor.nextEntry();
    }

    schema.dropClass("test_class");
  }

  @Test public void testTruncateVertexClass() {
    database.command("create class TestTruncateVertexClass extends V");
    database.command("create vertex TestTruncateVertexClass set name = 'foo'");

    try {
      database.command("truncate class TestTruncateVertexClass");
      Assert.fail();
    } catch (Exception e) {
    }
    OResultSet result = database.query("select from TestTruncateVertexClass");
    Assert.assertTrue(result.hasNext());
    result.close();

    database.command("truncate class TestTruncateVertexClass unsafe");
    result = database.query("select from TestTruncateVertexClass");
    Assert.assertFalse(result.hasNext());
    result.close();

  }

  @Test public void testTruncateVertexClassSubclasses() {

    database.command("create class TestTruncateVertexClassSuperclass");
    database.command("create class TestTruncateVertexClassSubclass extends TestTruncateVertexClassSuperclass");

    database.command("insert into TestTruncateVertexClassSuperclass set name = 'foo'");
    database.command("insert into TestTruncateVertexClassSubclass set name = 'bar'");

    OResultSet result = database.query("select from TestTruncateVertexClassSuperclass");
    for (int i = 0; i < 2; i++) {
      Assert.assertTrue(result.hasNext());
      result.next();
    }
    Assert.assertFalse(result.hasNext());
    result.close();

    database.command("truncate class TestTruncateVertexClassSuperclass ");
    result = database.query("select from TestTruncateVertexClassSubclass");
    Assert.assertTrue(result.hasNext());
    result.next();
    Assert.assertFalse(result.hasNext());
    result.close();

    database.command("truncate class TestTruncateVertexClassSuperclass polymorphic");
    result = database.query("select from TestTruncateVertexClassSubclass");
    Assert.assertFalse(result.hasNext());
    result.close();

  }

  @Test public void testTruncateVertexClassSubclassesWithIndex() {

    database.command("create class TestTruncateVertexClassSuperclassWithIndex");
    database.command("create property TestTruncateVertexClassSuperclassWithIndex.name STRING");
    database.command(
        "create index TestTruncateVertexClassSuperclassWithIndex_index on TestTruncateVertexClassSuperclassWithIndex (name) NOTUNIQUE");

    database.command("create class TestTruncateVertexClassSubclassWithIndex extends TestTruncateVertexClassSuperclassWithIndex");

    database.command("insert into TestTruncateVertexClassSuperclassWithIndex set name = 'foo'");
    database.command("insert into TestTruncateVertexClassSubclassWithIndex set name = 'bar'");

    OResultSet result = database.query("select from index:TestTruncateVertexClassSuperclassWithIndex_index");
    Assert.assertEquals(toList(result).size(), 2);
    result.close();

    database.command("truncate class TestTruncateVertexClassSubclassWithIndex");
    result = database.query("select from index:TestTruncateVertexClassSuperclassWithIndex_index");
    Assert.assertEquals(toList(result).size(), 1);
    result.close();

    database.command("truncate class TestTruncateVertexClassSuperclassWithIndex polymorphic");
    result = database.query("select from index:TestTruncateVertexClassSuperclassWithIndex_index");
    Assert.assertEquals(toList(result).size(), 0);
    result.close();

  }

  private List<OResult> toList(OResultSet input) {
    List<OResult> result = new ArrayList<>();
    while (input.hasNext()) {
      result.add(input.next());
    }
    return result;
  }

  private OIndex<?> getOrCreateIndex(OClass testClass) {
    OIndex<?> index = database.getMetadata().getIndexManager().getIndex("test_class_by_data");
    if (index == null) {
      testClass.createProperty("data", OType.EMBEDDEDLIST, OType.INTEGER);
      index = testClass.createIndex("test_class_by_data", OClass.INDEX_TYPE.UNIQUE, "data");
    }
    return index;
  }

  private OClass getOrCreateClass(OSchema schema) {
    OClass testClass;
    if (schema.existsClass("test_class")) {
      testClass = schema.getClass("test_class");
    } else {
      testClass = schema.createClass("test_class");
    }
    schema.save();
    return testClass;
  }

  @SuppressWarnings("unchecked") @Test public void testTruncateClassWithCommandCache() {

    OSchema schema = database.getMetadata().getSchema();
    OClass testClass = getOrCreateClass(schema);

    boolean ccWasEnabled = database.getMetadata().getCommandCache().isEnabled();
    database.getMetadata().getCommandCache().enable();

    database.command("truncate class test_class");

    database.save(new ODocument(testClass).field("name", "x").field("data", Arrays.asList(1, 2)));
    database.save(new ODocument(testClass).field("name", "y").field("data", Arrays.asList(3, 0)));

    OResultSet result = database.query("select from test_class");
    Assert.assertEquals(toList(result).size(), 2);

    database.command("truncate class test_class");

    result = database.query("select from test_class");
    Assert.assertEquals(toList(result).size(), 0);

    schema.dropClass("test_class");
    if (!ccWasEnabled) {
      database.getMetadata().getCommandCache().disable();
    }
  }

}
