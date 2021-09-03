package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseInternal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexManagerAbstract;
import com.orientechnologies.orient.core.metadata.OMetadataInternal;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/** @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com) */
public class OTruncateClassStatementExecutionTest {
  static ODatabaseDocumentInternal database;

  @BeforeClass
  public static void beforeClass() {
    database = new ODatabaseDocumentTx("memory:OTruncateClassStatementExecutionTest");
    database.create();
  }

  @AfterClass
  public static void afterClass() {
    database.close();
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testTruncateClass() {

    OSchema schema = database.getMetadata().getSchema();
    OClass testClass = getOrCreateClass(schema);

    final OIndex index = getOrCreateIndex(testClass);

    database.command("truncate class test_class");

    database.save(new ODocument(testClass).field("name", "x").field("data", Arrays.asList(1, 2)));
    database.save(new ODocument(testClass).field("name", "y").field("data", Arrays.asList(3, 0)));

    database.command(new OCommandSQL("truncate class test_class")).execute();

    database.save(
        new ODocument(testClass).field("name", "x").field("data", Arrays.asList(5, 6, 7)));
    database.save(
        new ODocument(testClass).field("name", "y").field("data", Arrays.asList(8, 9, -1)));

    OResultSet result = database.query("select from test_class");
    //    Assert.assertEquals(result.size(), 2);

    Set<Integer> set = new HashSet<Integer>();
    while (result.hasNext()) {
      set.addAll((Collection<Integer>) result.next().getProperty("data"));
    }
    result.close();
    Assert.assertTrue(set.containsAll(Arrays.asList(5, 6, 7, 8, 9, -1)));

    Assert.assertEquals(index.getInternal().size(), 6);

    try (Stream<ORawPair<Object, ORID>> stream = index.getInternal().stream()) {
      stream.forEach(
          (entry) -> {
            Assert.assertTrue(set.contains((Integer) entry.first));
          });
    }

    schema.dropClass("test_class");
  }

  @Test
  public void testTruncateVertexClass() {
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

  @Test
  public void testTruncateVertexClassSubclasses() {

    database.command("create class TestTruncateVertexClassSuperclass");
    database.command(
        "create class TestTruncateVertexClassSubclass extends TestTruncateVertexClassSuperclass");

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

  @Test
  public void testTruncateVertexClassSubclassesWithIndex() {

    database.command("create class TestTruncateVertexClassSuperclassWithIndex");
    database.command("create property TestTruncateVertexClassSuperclassWithIndex.name STRING");
    database.command(
        "create index TestTruncateVertexClassSuperclassWithIndex_index on TestTruncateVertexClassSuperclassWithIndex (name) NOTUNIQUE");

    database.command(
        "create class TestTruncateVertexClassSubclassWithIndex extends TestTruncateVertexClassSuperclassWithIndex");

    database.command("insert into TestTruncateVertexClassSuperclassWithIndex set name = 'foo'");
    database.command("insert into TestTruncateVertexClassSubclassWithIndex set name = 'bar'");

    if (!((ODatabaseInternal) database).getStorage().isRemote()) {
      final OIndexManagerAbstract indexManager =
          ((OMetadataInternal) database.getMetadata()).getIndexManagerInternal();
      final OIndex indexOne =
          indexManager.getIndex(
              (ODatabaseDocumentInternal) database,
              "TestTruncateVertexClassSuperclassWithIndex_index");
      Assert.assertEquals(2, indexOne.getInternal().size());

      database.command("truncate class TestTruncateVertexClassSubclassWithIndex");
      Assert.assertEquals(1, indexOne.getInternal().size());

      database.command("truncate class TestTruncateVertexClassSuperclassWithIndex polymorphic");
      Assert.assertEquals(0, indexOne.getInternal().size());
    }
  }

  private List<OResult> toList(OResultSet input) {
    List<OResult> result = new ArrayList<>();
    while (input.hasNext()) {
      result.add(input.next());
    }
    return result;
  }

  private OIndex getOrCreateIndex(OClass testClass) {
    OIndex index =
        database.getMetadata().getIndexManagerInternal().getIndex(database, "test_class_by_data");
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
    return testClass;
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testTruncateClassWithCommandCache() {

    OSchema schema = database.getMetadata().getSchema();
    OClass testClass = getOrCreateClass(schema);

    database.command("truncate class test_class");

    database.save(new ODocument(testClass).field("name", "x").field("data", Arrays.asList(1, 2)));
    database.save(new ODocument(testClass).field("name", "y").field("data", Arrays.asList(3, 0)));

    OResultSet result = database.query("select from test_class");
    Assert.assertEquals(toList(result).size(), 2);

    result.close();
    database.command("truncate class test_class");

    result = database.query("select from test_class");
    Assert.assertEquals(toList(result).size(), 0);
    result.close();

    schema.dropClass("test_class");
  }
}
