package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import org.testng.Assert;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;

import java.util.List;
import java.util.Set;

/**
 * @author Andrey Lomakin <a href="mailto:lomakin.andrey@gmail.com">Andrey Lomakin</a>
 * @since 4/11/14
 */
public class CompositeIndexWithNullTest extends BaseTest {
  @Parameters(value = "url")
  public CompositeIndexWithNullTest(@Optional String url) {
    super(url);
  }

  public void testPointQuery() {
    final OSchema schema = database.getMetadata().getSchema();
    OClass clazz = schema.createClass("compositeIndexNullPointQueryClass");
    clazz.createProperty("prop1", OType.INTEGER);
    clazz.createProperty("prop2", OType.INTEGER);
    clazz.createProperty("prop3", OType.INTEGER);

    final ODocument metadata = new ODocument();
    metadata.field("ignoreNullValues", false);

    clazz.createIndex("compositeIndexNullPointQueryIndex", OClass.INDEX_TYPE.NOTUNIQUE.toString(), null, metadata, new String[] {
        "prop1", "prop2", "prop3" });

    for (int i = 0; i < 20; i++) {
      ODocument document = new ODocument("compositeIndexNullPointQueryClass");
      document.field("prop1", i / 10);
      document.field("prop2", i / 5);

      if (i % 2 == 0)
        document.field("prop3", i);

      document.save();
    }

    final String query = "select from compositeIndexNullPointQueryClass where prop1 = 1 and prop2 = 2";
    List<ODocument> result = database.query(new OSQLSynchQuery<ODocument>(query));
    Assert.assertEquals(result.size(), 5);
    for (int k = 0; k < 5; k++) {
      ODocument document = result.get(k);
      Assert.assertEquals(document.field("prop1"), 1);
      Assert.assertEquals(document.field("prop2"), 2);
    }

    ODocument explain = database.command(new OCommandSQL("explain " + query)).execute();
    Assert.assertTrue(explain.<Set<String>> field("involvedIndexes").contains("compositeIndexNullPointQueryIndex"));
  }

  public void testRangeQuery() {
    final OSchema schema = database.getMetadata().getSchema();
    OClass clazz = schema.createClass("compositeIndexNullRangeQueryClass");
    clazz.createProperty("prop1", OType.INTEGER);
    clazz.createProperty("prop2", OType.INTEGER);
    clazz.createProperty("prop3", OType.INTEGER);

    final ODocument metadata = new ODocument();
    metadata.field("ignoreNullValues", false);

    clazz.createIndex("compositeIndexNullRangeQueryIndex", OClass.INDEX_TYPE.NOTUNIQUE.toString(), null, metadata, null,
        new String[] { "prop1", "prop2", "prop3" });

    for (int i = 0; i < 20; i++) {
      ODocument document = new ODocument("compositeIndexNullRangeQueryClass");
      document.field("prop1", i / 10);
      document.field("prop2", i / 5);

      if (i % 2 == 0)
        document.field("prop3", i);

      document.save();
    }

    String query = "select from compositeIndexNullRangeQueryClass where prop1 = 1 and prop2 > 2";
    List<ODocument> result = database.query(new OSQLSynchQuery<ODocument>(query));

    Assert.assertEquals(result.size(), 5);
    for (int k = 0; k < 5; k++) {
      ODocument document = result.get(k);
      Assert.assertEquals(document.field("prop1"), 1);
      Assert.assertTrue(document.<Integer> field("prop2") > 2);
    }

    ODocument explain = database.command(new OCommandSQL("explain " + query)).execute();
    Assert.assertTrue(explain.<Set<String>> field("involvedIndexes").contains("compositeIndexNullRangeQueryIndex"));

    query = "select from compositeIndexNullRangeQueryClass where prop1 > 0";
    result = database.query(new OSQLSynchQuery<ODocument>(query));

    Assert.assertEquals(result.size(), 10);
    for (int k = 0; k < 10; k++) {
      ODocument document = result.get(k);
      Assert.assertTrue(document.<Integer> field("prop1") > 0);
    }
  }

  public void testPointQueryNullInTheMiddle() {
    final OSchema schema = database.getMetadata().getSchema();
    OClass clazz = schema.createClass("compositeIndexNullPointQueryNullInTheMiddleClass");
    clazz.createProperty("prop1", OType.INTEGER);
    clazz.createProperty("prop2", OType.INTEGER);
    clazz.createProperty("prop3", OType.INTEGER);

    final ODocument metadata = new ODocument();
    metadata.field("ignoreNullValues", false);

    clazz.createIndex("compositeIndexNullPointQueryNullInTheMiddleIndex", OClass.INDEX_TYPE.NOTUNIQUE.toString(), null, metadata,
        null, new String[] { "prop1", "prop2", "prop3" });

    for (int i = 0; i < 20; i++) {
      ODocument document = new ODocument("compositeIndexNullPointQueryNullInTheMiddleClass");
      document.field("prop1", i / 10);

      if (i % 2 == 0)
        document.field("prop2", i);

      document.field("prop3", i);

      document.save();
    }

    final String query = "select from compositeIndexNullPointQueryNullInTheMiddleClass where prop1 = 1";
    List<ODocument> result = database.query(new OSQLSynchQuery<ODocument>(query));
    Assert.assertEquals(result.size(), 10);
    for (int k = 0; k < 10; k++) {
      ODocument document = result.get(k);
      Assert.assertEquals(document.field("prop1"), 1);
    }

    ODocument explain = database.command(new OCommandSQL("explain " + query)).execute();
    Assert.assertTrue(explain.<Set<String>> field("involvedIndexes").contains("compositeIndexNullPointQueryNullInTheMiddleIndex"));
  }

  public void testRangeQueryNullInTheMiddle() {
    final OSchema schema = database.getMetadata().getSchema();
    OClass clazz = schema.createClass("compositeIndexNullRangeQueryNullInTheMiddleClass");
    clazz.createProperty("prop1", OType.INTEGER);
    clazz.createProperty("prop2", OType.INTEGER);
    clazz.createProperty("prop3", OType.INTEGER);

    final ODocument metadata = new ODocument();
    metadata.field("ignoreNullValues", false);

    clazz.createIndex("compositeIndexNullRangeQueryNullInTheMiddleIndex", OClass.INDEX_TYPE.NOTUNIQUE.toString(), null, metadata,
        new String[] { "prop1", "prop2", "prop3" });

    for (int i = 0; i < 20; i++) {
      ODocument document = new ODocument("compositeIndexNullRangeQueryNullInTheMiddleClass");
      document.field("prop1", i / 10);

      if (i % 2 == 0)
        document.field("prop2", i);

      document.field("prop3", i);

      document.save();
    }

    final String query = "select from compositeIndexNullRangeQueryNullInTheMiddleClass where prop1 > 0";
    List<ODocument> result = database.query(new OSQLSynchQuery<ODocument>(query));
    Assert.assertEquals(result.size(), 10);
    for (int k = 0; k < 10; k++) {
      ODocument document = result.get(k);
      Assert.assertEquals(document.field("prop1"), 1);
    }

    ODocument explain = database.command(new OCommandSQL("explain " + query)).execute();
    Assert.assertTrue(explain.<Set<String>> field("involvedIndexes").contains("compositeIndexNullRangeQueryNullInTheMiddleIndex"));
  }
}
