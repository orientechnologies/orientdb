package com.orientechnologies.orient.test.database.auto;

import java.util.List;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;

/**
 * There is a possibility to create an automatic index for class without creation property in schema.
 * 
 * @since OrientDB v1.1.0
 * @see com.orientechnologies.orient.core.index.OIndexManager
 * @see com.orientechnologies.orient.core.metadata.schema.OClass
 * 
 * @author Artem Orobets
 */
@Test(groups = { "index" })
public class SQLIndexWithoutSchemaTest extends AbstractIndexReuseTest {
  public static final String TEST_CLASS = "sqlIndexWithoutSchemaTest";

  @Parameters("url")
  public SQLIndexWithoutSchemaTest(final String iURL) {
    super(iURL);
  }

  @BeforeClass
  public void setUp() throws Exception {
    super.setUp();

    if (database.isClosed())
      database.open("admin", "admin");

    final OSchema schema = database.getMetadata().getSchema();
    schema.createClass(TEST_CLASS);
    schema.save();

    database.close();
  }

  @AfterClass
  public void tearDown() {
    if (database.isClosed())
      database.open("admin", "admin");

    database.getMetadata().getSchema().dropClass(TEST_CLASS);

    database.close();
  }

  @Test
  public void testCreateIndex() {
    database.command(new OCommandSQL("CREATE INDEX indexWithoutSchema ON " + TEST_CLASS + " (prop2) NOTUNIQUE INTEGER")).execute();

    database.getMetadata().getIndexManager().reload();

    final OIndex<?> index = database.getMetadata().getSchema().getClass(TEST_CLASS).getClassIndex("indexWithoutSchema");

    Assert.assertNotNull(index);
    Assert.assertEquals(index.getType(), OClass.INDEX_TYPE.NOTUNIQUE.name());

    final OIndexDefinition definition = index.getDefinition();
    Assert.assertEquals(definition.getFields().size(), 1);
    Assert.assertEquals(definition.getFields().get(0).toLowerCase(), "prop2");
    Assert.assertEquals(definition.getTypes()[0], OType.INTEGER);
  }

  @Test(dependsOnMethods = "testCreateIndex")
  public void testPutInIndex() {
    { // Create document without field2
      final ODocument doc = database.newInstance(TEST_CLASS);
      doc.field("prop1", 0);
      doc.save();

      final List<?> result = database.query(new OSQLSynchQuery<Integer>("SELECT FROM index:indexWithoutSchema"));
      Assert.assertEquals(result.size(), 0);
    }

    for (int i = 1; i <= 10; i++) {
      final ODocument doc = database.newInstance(TEST_CLASS);
      doc.field("prop1", i);
      doc.field("prop2", i * i);
      doc.save();
    }

    final List<ODocument> results = database.query(new OSQLSynchQuery<Integer>("SELECT FROM index:indexWithoutSchema"));
    Assert.assertEquals(results.size(), 10);

    for (int i = 0, resultsSize = results.size(); i < resultsSize; i++) {
      ODocument result = results.get(i).field("rid");

      Assert.assertEquals(result.field("prop1"), i + 1);
    }
  }

  @Test(dependsOnMethods = "testPutInIndex")
  public void testDeleteFromIndex() {
    database.command(new OCommandSQL("DELETE FROM " + TEST_CLASS + " WHERE prop1 < 6")).execute();

    final List<ODocument> results = database.query(new OSQLSynchQuery<Integer>("SELECT FROM index:indexWithoutSchema"));
    Assert.assertEquals(results.size(), 5);

    for (int i = 0, resultsSize = results.size(); i < resultsSize; i++) {
      ODocument result = results.get(i).field("rid");

      Assert.assertEquals(result.field("prop1"), i + 6);
    }
  }

  @Test(dependsOnMethods = "testDeleteFromIndex")
  public void testReuseIndexInSelect() {
    final long oldIndexUsage = Math.max(profiler.getCounter("Query.indexUsage"), 0);

    final List<ODocument> result = database.query(new OSQLSynchQuery<ODocument>("SELECT FROM " + TEST_CLASS + " WHERE prop2 < 50"));

    Assert.assertEquals(result.size(), 2);

    Assert.assertEquals(profiler.getCounter("Query.indexUsage"), oldIndexUsage + 1);
  }

  @Test(dependsOnMethods = { "testCreateIndex", "testPutInIndex", "testDeleteFromIndex", "testReuseIndexInSelect" })
  public void testDropIndex() {
    database.command(new OCommandSQL("DROP INDEX indexWithoutSchema")).execute();

    database.getMetadata().getIndexManager().reload();

    final OIndex<?> index = database.getMetadata().getSchema().getClass(TEST_CLASS).getClassIndex("indexWithoutSchema");

    Assert.assertNull(index);
  }

  @Test
  public void testCreateCompositeIndex() {
    database.command(
        new OCommandSQL("CREATE INDEX compositeIndexWithoutSchema ON " + TEST_CLASS + " (cp2, cp3) NOTUNIQUE INTEGER, INTEGER"))
        .execute();

    database.getMetadata().getIndexManager().reload();

    final OIndex<?> index = database.getMetadata().getSchema().getClass(TEST_CLASS).getClassIndex("compositeIndexWithoutSchema");

    Assert.assertNotNull(index);
    Assert.assertEquals(index.getType(), OClass.INDEX_TYPE.NOTUNIQUE.name());

    final OIndexDefinition definition = index.getDefinition();
    Assert.assertEquals(definition.getFields().size(), 2);
    Assert.assertEquals(definition.getFields().get(0).toLowerCase(), "cp2");
    Assert.assertEquals(definition.getFields().get(1).toLowerCase(), "cp3");
    Assert.assertEquals(definition.getTypes()[0], OType.INTEGER);
    Assert.assertEquals(definition.getTypes()[1], OType.INTEGER);
  }

  @Test(dependsOnMethods = "testCreateCompositeIndex")
  public void testPutInCompositeIndex() {
    { // Create document without cp2, cp3
      final ODocument doc = database.newInstance(TEST_CLASS);
      doc.field("cp1", 0);
      doc.save();

      final List<?> result = database.query(new OSQLSynchQuery<Integer>("SELECT FROM index:compositeIndexWithoutSchema"));
      Assert.assertEquals(result.size(), 0);
    }

    for (int i = 1; i <= 10; i++) {
      final ODocument doc = database.newInstance(TEST_CLASS);
      doc.field("cp1", i);
      doc.field("cp2", 10 * i);
      doc.field("cp3", 100 * i);
      doc.save();
    }

    final List<ODocument> results = database.query(new OSQLSynchQuery<Integer>("SELECT FROM index:compositeIndexWithoutSchema"));
    Assert.assertEquals(results.size(), 10);

    for (int i = 0, resultsSize = results.size(); i < resultsSize; i++) {
      ODocument result = results.get(i).field("rid");

      Assert.assertEquals(result.field("cp1"), i + 1);
    }
  }

  @Test(dependsOnMethods = "testPutInCompositeIndex")
  public void testDeleteFromCompositeIndex() {
    database.command(new OCommandSQL("DELETE FROM " + TEST_CLASS + " WHERE cp1 < 6")).execute();

    final List<ODocument> results = database.query(new OSQLSynchQuery<Integer>("SELECT FROM index:compositeIndexWithoutSchema"));
    Assert.assertEquals(results.size(), 5);

    for (int i = 0, resultsSize = results.size(); i < resultsSize; i++) {
      ODocument result = results.get(i).field("rid");

      Assert.assertEquals(result.field("cp1"), i + 6);
    }
  }

  @Test(dependsOnMethods = "testDeleteFromCompositeIndex")
  public void testReuseCompositeIndexInSelect() {
    final long oldIndexUsage = Math.max(profiler.getCounter("Query.indexUsage"), 0);
    final long oldCompositeIndexUsage = Math.max(profiler.getCounter("Query.compositeIndexUsage"), 0);
    final long oldCompositeIndexUsage2 = Math.max(profiler.getCounter("Query.compositeIndexUsage.2"), 0);

    final List<ODocument> result = database.query(new OSQLSynchQuery<ODocument>("SELECT FROM " + TEST_CLASS
        + " WHERE cp2 = 70 and cp3 = 700"));

    Assert.assertEquals(result.size(), 1);
    Assert.assertEquals(result.get(0).field("cp1"), 7);

    Assert.assertEquals(profiler.getCounter("Query.indexUsage"), oldIndexUsage + 1);
    Assert.assertEquals(profiler.getCounter("Query.compositeIndexUsage"), oldCompositeIndexUsage + 1);
    Assert.assertEquals(profiler.getCounter("Query.compositeIndexUsage.2"), oldCompositeIndexUsage2 + 1);
  }

  @Test(dependsOnMethods = { "testCreateCompositeIndex", "testPutInCompositeIndex", "testDeleteFromCompositeIndex",
      "testReuseCompositeIndexInSelect" })
  public void testDropCompositeIndex() {
    database.command(new OCommandSQL("DROP INDEX compositeIndexWithoutSchema")).execute();

    database.getMetadata().getIndexManager().reload();

    final OIndex<?> index = database.getMetadata().getSchema().getClass(TEST_CLASS).getClassIndex("compositeIndexWithoutSchema");

    Assert.assertNull(index);
  }
}
