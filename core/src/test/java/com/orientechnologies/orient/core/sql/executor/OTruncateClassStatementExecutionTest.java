package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.BaseMemoryInternalDatabase;
import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseInternal;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexManagerAbstract;
import com.orientechnologies.orient.core.metadata.OMetadataInternal;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
public class OTruncateClassStatementExecutionTest extends BaseMemoryInternalDatabase {

  @SuppressWarnings("unchecked")
  @Test
  public void testTruncateClass() {

    OSchema schema = db.getMetadata().getSchema();
    OClass testClass = getOrCreateClass(schema);

    final OIndex index = getOrCreateIndex(testClass);

    db.command("truncate class test_class");

    db.save(new ODocument(testClass).field("name", "x").field("data", Arrays.asList(1, 2)));
    db.save(new ODocument(testClass).field("name", "y").field("data", Arrays.asList(3, 0)));

    db.command("truncate class test_class").close();

    db.save(new ODocument(testClass).field("name", "x").field("data", Arrays.asList(5, 6, 7)));
    db.save(new ODocument(testClass).field("name", "y").field("data", Arrays.asList(8, 9, -1)));

    OResultSet result = db.query("select from test_class");
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
    db.command("create class TestTruncateVertexClass extends V");
    db.command("create vertex TestTruncateVertexClass set name = 'foo'");

    try {
      db.command("truncate class TestTruncateVertexClass");
      Assert.fail();
    } catch (Exception e) {
    }
    OResultSet result = db.query("select from TestTruncateVertexClass");
    Assert.assertTrue(result.hasNext());
    result.close();

    db.command("truncate class TestTruncateVertexClass unsafe");
    result = db.query("select from TestTruncateVertexClass");
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testTruncateVertexClassSubclasses() {

    db.command("create class TestTruncateVertexClassSuperclass");
    db.command(
        "create class TestTruncateVertexClassSubclass extends TestTruncateVertexClassSuperclass");

    db.command("insert into TestTruncateVertexClassSuperclass set name = 'foo'");
    db.command("insert into TestTruncateVertexClassSubclass set name = 'bar'");

    OResultSet result = db.query("select from TestTruncateVertexClassSuperclass");
    for (int i = 0; i < 2; i++) {
      Assert.assertTrue(result.hasNext());
      result.next();
    }
    Assert.assertFalse(result.hasNext());
    result.close();

    db.command("truncate class TestTruncateVertexClassSuperclass ");
    result = db.query("select from TestTruncateVertexClassSubclass");
    Assert.assertTrue(result.hasNext());
    result.next();
    Assert.assertFalse(result.hasNext());
    result.close();

    db.command("truncate class TestTruncateVertexClassSuperclass polymorphic");
    result = db.query("select from TestTruncateVertexClassSubclass");
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testTruncateVertexClassSubclassesWithIndex() {

    db.command("create class TestTruncateVertexClassSuperclassWithIndex");
    db.command("create property TestTruncateVertexClassSuperclassWithIndex.name STRING");
    db.command(
        "create index TestTruncateVertexClassSuperclassWithIndex_index on"
            + " TestTruncateVertexClassSuperclassWithIndex (name) NOTUNIQUE");

    db.command(
        "create class TestTruncateVertexClassSubclassWithIndex extends"
            + " TestTruncateVertexClassSuperclassWithIndex");

    db.command("insert into TestTruncateVertexClassSuperclassWithIndex set name = 'foo'");
    db.command("insert into TestTruncateVertexClassSubclassWithIndex set name = 'bar'");

    if (!((ODatabaseInternal) db).getStorage().isRemote()) {
      final OIndexManagerAbstract indexManager =
          ((OMetadataInternal) db.getMetadata()).getIndexManagerInternal();
      final OIndex indexOne =
          indexManager.getIndex(
              (ODatabaseDocumentInternal) db, "TestTruncateVertexClassSuperclassWithIndex_index");
      Assert.assertEquals(2, indexOne.getInternal().size());

      db.command("truncate class TestTruncateVertexClassSubclassWithIndex");
      Assert.assertEquals(1, indexOne.getInternal().size());

      db.command("truncate class TestTruncateVertexClassSuperclassWithIndex polymorphic");
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
    OIndex index = db.getMetadata().getIndexManagerInternal().getIndex(db, "test_class_by_data");
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

    OSchema schema = db.getMetadata().getSchema();
    OClass testClass = getOrCreateClass(schema);

    db.command("truncate class test_class");

    db.save(new ODocument(testClass).field("name", "x").field("data", Arrays.asList(1, 2)));
    db.save(new ODocument(testClass).field("name", "y").field("data", Arrays.asList(3, 0)));

    OResultSet result = db.query("select from test_class");
    Assert.assertEquals(toList(result).size(), 2);

    result.close();
    db.command("truncate class test_class");

    result = db.query("select from test_class");
    Assert.assertEquals(toList(result).size(), 0);
    result.close();

    schema.dropClass("test_class");
  }
}
