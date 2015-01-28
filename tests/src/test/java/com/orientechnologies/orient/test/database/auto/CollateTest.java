package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.core.collate.OCaseInsensitiveCollate;
import com.orientechnologies.orient.core.collate.ODefaultCollate;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.index.OCompositeKey;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import org.testng.Assert;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Set;

@Test
public class CollateTest extends DocumentDBBaseTest {

  @Parameters(value = "url")
  public CollateTest(@Optional String url) {
    super(url);
  }

  public void testQuery() {
    final OSchema schema = database.getMetadata().getSchema();
    OClass clazz = schema.createClass("collateTest");

    OProperty csp = clazz.createProperty("csp", OType.STRING);
    csp.setCollate(ODefaultCollate.NAME);

    OProperty cip = clazz.createProperty("cip", OType.STRING);
    cip.setCollate(OCaseInsensitiveCollate.NAME);

    for (int i = 0; i < 10; i++) {
      ODocument document = new ODocument("collateTest");

      if (i % 2 == 0) {
        document.field("csp", "VAL");
        document.field("cip", "VAL");
      } else {
        document.field("csp", "val");
        document.field("cip", "val");
      }

      document.save();
    }

    List<ODocument> result = database.query(new OSQLSynchQuery<ODocument>("select from collateTest where csp = 'VAL'"));
    Assert.assertEquals(result.size(), 5);

    for (ODocument document : result)
      Assert.assertEquals(document.field("csp"), "VAL");

    result = database.query(new OSQLSynchQuery<ODocument>("select from collateTest where cip = 'VaL'"));
    Assert.assertEquals(result.size(), 10);

    for (ODocument document : result)
      Assert.assertEquals((document.<String> field("cip")).toUpperCase(), "VAL");
  }

  public void testQueryNotNullCi() {
    final OSchema schema = database.getMetadata().getSchema();
    OClass clazz = schema.createClass("collateTestNotNull");

    OProperty csp = clazz.createProperty("bar", OType.STRING);
    csp.setCollate(OCaseInsensitiveCollate.NAME);

    ODocument document = new ODocument("collateTestNotNull");
    document.field("bar", "baz");
    document.save();

    document = new ODocument("collateTestNotNull");
    document.field("nobar", true);
    document.save();

    List<ODocument> result = database.query(new OSQLSynchQuery<ODocument>("select from collateTestNotNull where bar is null"));
    Assert.assertEquals(result.size(), 1);

    result = database.query(new OSQLSynchQuery<ODocument>("select from collateTestNotNull where bar is not null"));
    Assert.assertEquals(result.size(), 1);

  }

  public void testIndexQuery() {
    final OSchema schema = database.getMetadata().getSchema();
    OClass clazz = schema.createClass("collateIndexTest");

    OProperty csp = clazz.createProperty("csp", OType.STRING);
    csp.setCollate(ODefaultCollate.NAME);

    OProperty cip = clazz.createProperty("cip", OType.STRING);
    cip.setCollate(OCaseInsensitiveCollate.NAME);

    clazz.createIndex("collateIndexCSP", OClass.INDEX_TYPE.NOTUNIQUE, "csp");
    clazz.createIndex("collateIndexCIP", OClass.INDEX_TYPE.NOTUNIQUE, "cip");

    for (int i = 0; i < 10; i++) {
      ODocument document = new ODocument("collateIndexTest");

      if (i % 2 == 0) {
        document.field("csp", "VAL");
        document.field("cip", "VAL");
      } else {
        document.field("csp", "val");
        document.field("cip", "val");
      }

      document.save();
    }

    String query = "select from collateIndexTest where csp = 'VAL'";
    List<ODocument> result = database.query(new OSQLSynchQuery<ODocument>(query));
    Assert.assertEquals(result.size(), 5);

    for (ODocument document : result)
      Assert.assertEquals(document.field("csp"), "VAL");

    ODocument explain = database.command(new OCommandSQL("explain " + query)).execute();
    Assert.assertTrue(explain.<Set<String>> field("involvedIndexes").contains("collateIndexCSP"));

    query = "select from collateIndexTest where cip = 'VaL'";
    result = database.query(new OSQLSynchQuery<ODocument>(query));
    Assert.assertEquals(result.size(), 10);

    for (ODocument document : result)
      Assert.assertEquals((document.<String> field("cip")).toUpperCase(), "VAL");

    explain = database.command(new OCommandSQL("explain " + query)).execute();
    Assert.assertTrue(explain.<Set<String>> field("involvedIndexes").contains("collateIndexCIP"));
  }

  public void testIndexQueryCollateWasChanged() {
    final OSchema schema = database.getMetadata().getSchema();
    OClass clazz = schema.createClass("collateWasChangedIndexTest");

    OProperty cp = clazz.createProperty("cp", OType.STRING);
    cp.setCollate(ODefaultCollate.NAME);

    clazz.createIndex("collateWasChangedIndex", OClass.INDEX_TYPE.NOTUNIQUE, "cp");

    for (int i = 0; i < 10; i++) {
      ODocument document = new ODocument("collateWasChangedIndexTest");

      if (i % 2 == 0)
        document.field("cp", "VAL");
      else
        document.field("cp", "val");

      document.save();
    }

    String query = "select from collateWasChangedIndexTest where cp = 'VAL'";
    List<ODocument> result = database.query(new OSQLSynchQuery<ODocument>(query));
    Assert.assertEquals(result.size(), 5);

    for (ODocument document : result)
      Assert.assertEquals(document.field("cp"), "VAL");

    ODocument explain = database.command(new OCommandSQL("explain " + query)).execute();
    Assert.assertTrue(explain.<Set<String>> field("involvedIndexes").contains("collateWasChangedIndex"));

    cp = clazz.getProperty("cp");
    cp.setCollate(OCaseInsensitiveCollate.NAME);

    query = "select from collateWasChangedIndexTest where cp = 'VaL'";
    result = database.query(new OSQLSynchQuery<ODocument>(query));
    Assert.assertEquals(result.size(), 10);

    for (ODocument document : result)
      Assert.assertEquals((document.<String> field("cp")).toUpperCase(), "VAL");

    explain = database.command(new OCommandSQL("explain " + query)).execute();
    Assert.assertTrue(explain.<Set<String>> field("involvedIndexes").contains("collateWasChangedIndex"));
  }

  public void testCompositeIndexQueryCS() {
    final OSchema schema = database.getMetadata().getSchema();
    OClass clazz = schema.createClass("CompositeIndexQueryCSTest");

    OProperty csp = clazz.createProperty("csp", OType.STRING);
    csp.setCollate(ODefaultCollate.NAME);

    OProperty cip = clazz.createProperty("cip", OType.STRING);
    cip.setCollate(OCaseInsensitiveCollate.NAME);

    clazz.createIndex("collateCompositeIndexCS", OClass.INDEX_TYPE.NOTUNIQUE, "csp", "cip");

    for (int i = 0; i < 10; i++) {
      ODocument document = new ODocument("CompositeIndexQueryCSTest");

      if (i % 2 == 0) {
        document.field("csp", "VAL");
        document.field("cip", "VAL");
      } else {
        document.field("csp", "val");
        document.field("cip", "val");
      }

      document.save();
    }

    String query = "select from CompositeIndexQueryCSTest where csp = 'VAL'";
    List<ODocument> result = database.query(new OSQLSynchQuery<ODocument>(query));
    Assert.assertEquals(result.size(), 5);

    for (ODocument document : result)
      Assert.assertEquals(document.field("csp"), "VAL");

    ODocument explain = database.command(new OCommandSQL("explain " + query)).execute();
    Assert.assertTrue(explain.<Set<String>> field("involvedIndexes").contains("collateCompositeIndexCS"));

    query = "select from CompositeIndexQueryCSTest where csp = 'VAL' and cip = 'VaL'";
    result = database.query(new OSQLSynchQuery<ODocument>(query));
    Assert.assertEquals(result.size(), 5);

    for (ODocument document : result) {
      Assert.assertEquals(document.field("csp"), "VAL");
      Assert.assertEquals((document.<String> field("cip")).toUpperCase(), "VAL");
    }

    explain = database.command(new OCommandSQL("explain " + query)).execute();
    Assert.assertTrue(explain.<Set<String>> field("involvedIndexes").contains("collateCompositeIndexCS"));

    result = database.query(new OSQLSynchQuery<ODocument>("select from index:collateCompositeIndexCS where key = ['VAL', 'VaL']"));

    Assert.assertEquals(result.size(), 5);
    for (ODocument document : result) {
      final OCompositeKey key = document.field("key");
      final List keys = key.getKeys();
      Assert.assertEquals(keys.get(0), "VAL");
      Assert.assertTrue("val".compareToIgnoreCase((String) keys.get(1)) == 0);

      final ODocument record = document.<OIdentifiable> field("rid").getRecord();
      Assert.assertEquals(record.field("csp"), "VAL");
      Assert.assertEquals((record.<String> field("cip")).toUpperCase(), "VAL");
    }
  }

  public void testCompositeIndexQueryCollateWasChanged() {
    final OSchema schema = database.getMetadata().getSchema();
    OClass clazz = schema.createClass("CompositeIndexQueryCollateWasChangedTest");

    OProperty csp = clazz.createProperty("csp", OType.STRING);
    csp.setCollate(ODefaultCollate.NAME);

    clazz.createProperty("cip", OType.STRING);

    clazz.createIndex("collateCompositeIndexCollateWasChanged", OClass.INDEX_TYPE.NOTUNIQUE, "csp", "cip");

    for (int i = 0; i < 10; i++) {
      ODocument document = new ODocument("CompositeIndexQueryCollateWasChangedTest");
      if (i % 2 == 0) {
        document.field("csp", "VAL");
        document.field("cip", "VAL");
      } else {
        document.field("csp", "val");
        document.field("cip", "val");
      }

      document.save();
    }

    String query = "select from CompositeIndexQueryCollateWasChangedTest where csp = 'VAL'";
    List<ODocument> result = database.query(new OSQLSynchQuery<ODocument>(query));
    Assert.assertEquals(result.size(), 5);

    for (ODocument document : result)
      Assert.assertEquals(document.field("csp"), "VAL");

    ODocument explain = database.command(new OCommandSQL("explain " + query)).execute();
    Assert.assertTrue(explain.<Set<String>> field("involvedIndexes").contains("collateCompositeIndexCollateWasChanged"));

    csp = clazz.getProperty("csp");
    csp.setCollate(OCaseInsensitiveCollate.NAME);

    query = "select from CompositeIndexQueryCollateWasChangedTest where csp = 'VaL'";
    result = database.query(new OSQLSynchQuery<ODocument>(query));
    Assert.assertEquals(result.size(), 10);

    for (ODocument document : result)
      Assert.assertEquals(document.<String> field("csp").toUpperCase(), "VAL");

    explain = database.command(new OCommandSQL("explain " + query)).execute();
    Assert.assertTrue(explain.<Set<String>> field("involvedIndexes").contains("collateCompositeIndexCollateWasChanged"));
  }

  public void collateThroughSQL() {
    final OSchema schema = database.getMetadata().getSchema();
    OClass clazz = schema.createClass("collateTestViaSQL");

    OProperty csp = clazz.createProperty("csp", OType.STRING);
    OProperty cip = clazz.createProperty("cip", OType.STRING);

    database.command(new OCommandSQL("create index collateTestViaSQL.index on collateTestViaSQL (cip COLLATE CI) NOTUNIQUE")).execute();

    for (int i = 0; i < 10; i++) {
      ODocument document = new ODocument("collateTestViaSQL");

      if (i % 2 == 0) {
        document.field("csp", "VAL");
        document.field("cip", "VAL");
      } else {
        document.field("csp", "val");
        document.field("cip", "val");
      }

      document.save();
    }

    List<ODocument> result = database.query(new OSQLSynchQuery<ODocument>("select from collateTestViaSQL where csp = 'VAL'"));
    Assert.assertEquals(result.size(), 5);

    for (ODocument document : result)
      Assert.assertEquals(document.field("csp"), "VAL");

    result = database.query(new OSQLSynchQuery<ODocument>("select from collateTestViaSQL where cip = 'VaL'"));
    Assert.assertEquals(result.size(), 10);

    for (ODocument document : result)
      Assert.assertEquals((document.<String> field("cip")).toUpperCase(), "VAL");
  }
}
