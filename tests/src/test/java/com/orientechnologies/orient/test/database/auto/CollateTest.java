package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.core.collate.OCaseInsensitiveCollate;
import com.orientechnologies.orient.core.collate.ODefaultCollate;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OCompositeKey;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexManagerAbstract;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.testng.Assert;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

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

      database.save(document);
    }

    List<OResult> result =
        database.query("select from collateTest where csp = 'VAL'").stream().toList();
    Assert.assertEquals(result.size(), 5);

    for (OResult document : result) Assert.assertEquals(document.getProperty("csp"), "VAL");

    result = database.query("select from collateTest where cip = 'VaL'").stream().toList();
    Assert.assertEquals(result.size(), 10);

    for (OResult document : result)
      Assert.assertEquals((document.<String>getProperty("cip")).toUpperCase(Locale.ENGLISH), "VAL");
  }

  public void testQueryNotNullCi() {
    final OSchema schema = database.getMetadata().getSchema();
    OClass clazz = schema.createClass("collateTestNotNull");

    OProperty csp = clazz.createProperty("bar", OType.STRING);
    csp.setCollate(OCaseInsensitiveCollate.NAME);

    ODocument document = new ODocument("collateTestNotNull");
    document.field("bar", "baz");
    database.save(document);

    document = new ODocument("collateTestNotNull");
    document.field("nobar", true);
    database.save(document);

    List<OResult> result =
        database.query("select from collateTestNotNull where bar is null").stream().toList();
    Assert.assertEquals(result.size(), 1);

    result =
        database.query("select from collateTestNotNull where bar is not null").stream().toList();
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

      database.save(document);
    }

    String query = "select from collateIndexTest where csp = 'VAL'";
    List<OResult> result = database.query(query).stream().toList();
    Assert.assertEquals(result.size(), 5);

    for (OResult document : result) Assert.assertEquals(document.getProperty("csp"), "VAL");

    OResultSet explain = database.command("explain " + query);
    Assert.assertTrue(explain.getExecutionPlan().get().getIndexes().contains("collateIndexCSP"));

    query = "select from collateIndexTest where cip = 'VaL'";
    result = database.query(query).stream().toList();
    Assert.assertEquals(result.size(), 10);

    for (OResult document : result)
      Assert.assertEquals((document.<String>getProperty("cip")).toUpperCase(Locale.ENGLISH), "VAL");

    explain = database.command("explain " + query);
    Assert.assertTrue(explain.getExecutionPlan().get().getIndexes().contains("collateIndexCIP"));
  }

  public void testIndexQueryCollateWasChanged() {
    final OSchema schema = database.getMetadata().getSchema();
    OClass clazz = schema.createClass("collateWasChangedIndexTest");

    OProperty cp = clazz.createProperty("cp", OType.STRING);
    cp.setCollate(ODefaultCollate.NAME);

    clazz.createIndex("collateWasChangedIndex", OClass.INDEX_TYPE.NOTUNIQUE, "cp");

    for (int i = 0; i < 10; i++) {
      ODocument document = new ODocument("collateWasChangedIndexTest");

      if (i % 2 == 0) document.field("cp", "VAL");
      else document.field("cp", "val");

      database.save(document);
    }

    String query = "select from collateWasChangedIndexTest where cp = 'VAL'";
    List<OResult> result = database.query(query).stream().toList();
    Assert.assertEquals(result.size(), 5);

    for (OResult document : result) Assert.assertEquals(document.getProperty("cp"), "VAL");

    OResultSet explain = database.command("explain " + query);
    Assert.assertTrue(
        explain.getExecutionPlan().get().getIndexes().contains("collateWasChangedIndex"));

    cp = clazz.getProperty("cp");
    cp.setCollate(OCaseInsensitiveCollate.NAME);

    query = "select from collateWasChangedIndexTest where cp = 'VaL'";
    result = database.query(query).stream().toList();
    Assert.assertEquals(result.size(), 10);

    for (OResult document : result)
      Assert.assertEquals((document.<String>getProperty("cp")).toUpperCase(Locale.ENGLISH), "VAL");

    explain = database.command("explain " + query);
    Assert.assertTrue(
        explain.getExecutionPlan().get().getIndexes().contains("collateWasChangedIndex"));
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
      database.save(document);
    }

    String query = "select from CompositeIndexQueryCSTest where csp = 'VAL'";
    List<OResult> result = database.query(query).stream().toList();
    Assert.assertEquals(result.size(), 5);

    for (OResult document : result) Assert.assertEquals(document.getProperty("csp"), "VAL");

    OResultSet explain = database.command("explain " + query);
    Assert.assertTrue(
        explain.getExecutionPlan().get().getIndexes().contains("collateCompositeIndexCS"));

    query = "select from CompositeIndexQueryCSTest where csp = 'VAL' and cip = 'VaL'";
    result = database.query(query).stream().toList();
    Assert.assertEquals(result.size(), 5);

    for (OResult document : result) {
      Assert.assertEquals(document.getProperty("csp"), "VAL");
      Assert.assertEquals((document.<String>getProperty("cip")).toUpperCase(Locale.ENGLISH), "VAL");
    }

    explain = database.command("explain " + query);
    Assert.assertTrue(
        explain.getExecutionPlan().get().getIndexes().contains("collateCompositeIndexCS"));

    if (!database.getStorage().isRemote()) {
      final OIndexManagerAbstract indexManager = database.getMetadata().getIndexManagerInternal();
      final OIndex index = indexManager.getIndex(database, "collateCompositeIndexCS");

      final Collection<ORID> value;
      try (Stream<ORID> stream = index.getInternal().getRids(new OCompositeKey("VAL", "VaL"))) {
        value = stream.collect(Collectors.toList());
      }

      Assert.assertEquals(value.size(), 5);
      for (ORID identifiable : value) {
        final ODocument record = identifiable.getRecord();
        Assert.assertEquals(record.field("csp"), "VAL");
        Assert.assertEquals((record.<String>field("cip")).toUpperCase(Locale.ENGLISH), "VAL");
      }
    }
  }

  public void testCompositeIndexQueryCollateWasChanged() {
    final OSchema schema = database.getMetadata().getSchema();
    OClass clazz = schema.createClass("CompositeIndexQueryCollateWasChangedTest");

    OProperty csp = clazz.createProperty("csp", OType.STRING);
    csp.setCollate(ODefaultCollate.NAME);

    clazz.createProperty("cip", OType.STRING);

    clazz.createIndex(
        "collateCompositeIndexCollateWasChanged", OClass.INDEX_TYPE.NOTUNIQUE, "csp", "cip");

    for (int i = 0; i < 10; i++) {
      ODocument document = new ODocument("CompositeIndexQueryCollateWasChangedTest");
      if (i % 2 == 0) {
        document.field("csp", "VAL");
        document.field("cip", "VAL");
      } else {
        document.field("csp", "val");
        document.field("cip", "val");
      }

      database.save(document);
    }

    String query = "select from CompositeIndexQueryCollateWasChangedTest where csp = 'VAL'";
    List<OResult> result = database.query(query).stream().toList();
    Assert.assertEquals(result.size(), 5);

    for (OResult document : result) Assert.assertEquals(document.getProperty("csp"), "VAL");

    OResultSet explain = database.command("explain " + query);
    Assert.assertTrue(
        explain
            .getExecutionPlan()
            .get()
            .getIndexes()
            .contains("collateCompositeIndexCollateWasChanged"));
    explain.close();
    csp = clazz.getProperty("csp");
    csp.setCollate(OCaseInsensitiveCollate.NAME);

    query = "select from CompositeIndexQueryCollateWasChangedTest where csp = 'VaL'";
    result = database.query(query).stream().toList();
    Assert.assertEquals(result.size(), 10);

    for (OResult document : result)
      Assert.assertEquals(document.<String>getProperty("csp").toUpperCase(Locale.ENGLISH), "VAL");

    explain = database.command("explain " + query);
    Assert.assertTrue(
        explain
            .getExecutionPlan()
            .get()
            .getIndexes()
            .contains("collateCompositeIndexCollateWasChanged"));
  }

  public void collateThroughSQL() {
    final OSchema schema = database.getMetadata().getSchema();
    OClass clazz = schema.createClass("collateTestViaSQL");

    clazz.createProperty("csp", OType.STRING);
    clazz.createProperty("cip", OType.STRING);

    database
        .command(
            "create index collateTestViaSQL.index on collateTestViaSQL (cip COLLATE CI)"
                + " NOTUNIQUE")
        .close();

    for (int i = 0; i < 10; i++) {
      ODocument document = new ODocument("collateTestViaSQL");

      if (i % 2 == 0) {
        document.field("csp", "VAL");
        document.field("cip", "VAL");
      } else {
        document.field("csp", "val");
        document.field("cip", "val");
      }

      database.save(document);
    }

    List<OResult> result =
        database.query("select from collateTestViaSQL where csp = 'VAL'").stream().toList();
    Assert.assertEquals(result.size(), 5);

    for (OResult document : result) Assert.assertEquals(document.getProperty("csp"), "VAL");

    result = database.query("select from collateTestViaSQL where cip = 'VaL'").stream().toList();
    Assert.assertEquals(result.size(), 10);

    for (OResult document : result)
      Assert.assertEquals((document.<String>getProperty("cip")).toUpperCase(Locale.ENGLISH), "VAL");
  }
}
