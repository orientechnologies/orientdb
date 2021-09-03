package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 9/12/14
 */
@Test
public class BetweenConversionTest extends DocumentDBBaseTest {

  @Parameters(value = "url")
  public BetweenConversionTest(@Optional String url) {
    super(url);
  }

  @BeforeClass
  @Override
  public void beforeClass() throws Exception {
    super.beforeClass();

    final OSchema schema = database.getMetadata().getSchema();
    final OClass clazz = schema.createClass("BetweenConversionTest");
    clazz.createProperty("a", OType.INTEGER);
    clazz.createProperty("ai", OType.INTEGER);

    clazz.createIndex("BetweenConversionTestIndex", OClass.INDEX_TYPE.NOTUNIQUE, "ai");

    for (int i = 0; i < 10; i++) {
      ODocument document = new ODocument("BetweenConversionTest");
      document.field("a", i);
      document.field("ai", i);

      if (i < 5) document.field("vl", "v1");
      else document.field("vl", "v2");

      ODocument ed = new ODocument();
      ed.field("a", i);

      document.field("d", ed);

      document.save();
    }
  }

  public void testBetweenRightLeftIncluded() {
    final String query = "select from BetweenConversionTest where a >= 1 and a <= 3";
    final List<ODocument> result = database.query(new OSQLSynchQuery<ODocument>(query));

    Assert.assertEquals(result.size(), 3);
    List<Integer> values = new ArrayList<Integer>(Arrays.asList(1, 2, 3));

    for (ODocument document : result) {
      Assert.assertTrue(values.remove((Integer) document.field("a")));
    }

    Assert.assertTrue(values.isEmpty());

    ODocument explain = database.command(new OCommandSQL("explain " + query)).execute();

    Assert.assertEquals(explain.<Object>field("rangeQueryConvertedInBetween"), 1);
  }

  public void testBetweenRightLeftIncludedReverseOrder() {
    final String query = "select from BetweenConversionTest where a <= 3 and a >= 1";
    final List<ODocument> result = database.query(new OSQLSynchQuery<ODocument>(query));

    Assert.assertEquals(result.size(), 3);
    List<Integer> values = new ArrayList<Integer>(Arrays.asList(1, 2, 3));

    for (ODocument document : result) {
      Assert.assertTrue(values.remove((Integer) document.field("a")));
    }

    Assert.assertTrue(values.isEmpty());

    ODocument explain = database.command(new OCommandSQL("explain " + query)).execute();

    Assert.assertEquals(explain.<Object>field("rangeQueryConvertedInBetween"), 1);
  }

  public void testBetweenRightIncluded() {
    final String query = "select from BetweenConversionTest where a > 1 and a <= 3";
    final List<ODocument> result = database.query(new OSQLSynchQuery<ODocument>(query));

    Assert.assertEquals(result.size(), 2);
    List<Integer> values = new ArrayList<Integer>(Arrays.asList(2, 3));

    for (ODocument document : result) {
      Assert.assertTrue(values.remove((Integer) document.field("a")));
    }

    Assert.assertTrue(values.isEmpty());

    ODocument explain = database.command(new OCommandSQL("explain " + query)).execute();

    Assert.assertEquals(explain.<Object>field("rangeQueryConvertedInBetween"), 1);
  }

  public void testBetweenRightIncludedReverse() {
    final String query = "select from BetweenConversionTest where a <= 3 and a > 1";
    final List<ODocument> result = database.query(new OSQLSynchQuery<ODocument>(query));

    Assert.assertEquals(result.size(), 2);
    List<Integer> values = new ArrayList<Integer>(Arrays.asList(2, 3));

    for (ODocument document : result) {
      Assert.assertTrue(values.remove((Integer) document.field("a")));
    }

    Assert.assertTrue(values.isEmpty());

    ODocument explain = database.command(new OCommandSQL("explain " + query)).execute();

    Assert.assertEquals(explain.<Object>field("rangeQueryConvertedInBetween"), 1);
  }

  public void testBetweenLeftIncluded() {
    final String query = "select from BetweenConversionTest where a >= 1 and a < 3";
    final List<ODocument> result = database.query(new OSQLSynchQuery<ODocument>(query));

    Assert.assertEquals(result.size(), 2);
    List<Integer> values = new ArrayList<Integer>(Arrays.asList(1, 2));

    for (ODocument document : result) {
      Assert.assertTrue(values.remove((Integer) document.field("a")));
    }

    Assert.assertTrue(values.isEmpty());

    ODocument explain = database.command(new OCommandSQL("explain " + query)).execute();

    Assert.assertEquals(explain.<Object>field("rangeQueryConvertedInBetween"), 1);
  }

  public void testBetweenLeftIncludedReverseOrder() {
    final String query = "select from BetweenConversionTest where  a < 3 and a >= 1";
    final List<ODocument> result = database.query(new OSQLSynchQuery<ODocument>(query));

    Assert.assertEquals(result.size(), 2);
    List<Integer> values = new ArrayList<Integer>(Arrays.asList(1, 2));

    for (ODocument document : result) {
      Assert.assertTrue(values.remove((Integer) document.field("a")));
    }

    Assert.assertTrue(values.isEmpty());

    ODocument explain = database.command(new OCommandSQL("explain " + query)).execute();

    Assert.assertEquals(explain.<Object>field("rangeQueryConvertedInBetween"), 1);
  }

  public void testBetween() {
    final String query = "select from BetweenConversionTest where a > 1 and a < 3";
    final List<ODocument> result = database.query(new OSQLSynchQuery<ODocument>(query));

    Assert.assertEquals(result.size(), 1);
    List<Integer> values = new ArrayList<Integer>(Arrays.asList(2));

    for (ODocument document : result) {
      Assert.assertTrue(values.remove((Integer) document.field("a")));
    }

    Assert.assertTrue(values.isEmpty());

    ODocument explain = database.command(new OCommandSQL("explain " + query)).execute();

    Assert.assertEquals(explain.<Object>field("rangeQueryConvertedInBetween"), 1);
  }

  public void testBetweenRightLeftIncludedIndex() {
    final String query = "select from BetweenConversionTest where ai >= 1 and ai <= 3";
    final List<ODocument> result = database.query(new OSQLSynchQuery<ODocument>(query));

    Assert.assertEquals(result.size(), 3);
    List<Integer> values = new ArrayList<Integer>(Arrays.asList(1, 2, 3));

    for (ODocument document : result) {
      Assert.assertTrue(values.remove((Integer) document.field("ai")));
    }

    Assert.assertTrue(values.isEmpty());

    ODocument explain = database.command(new OCommandSQL("explain " + query)).execute();

    Assert.assertEquals(explain.<Object>field("rangeQueryConvertedInBetween"), 1);
    Assert.assertTrue(
        ((Set<String>) explain.field("involvedIndexes")).contains("BetweenConversionTestIndex"));
  }

  public void testBetweenRightLeftIncludedReverseOrderIndex() {
    final String query = "select from BetweenConversionTest where ai <= 3 and ai >= 1";
    final List<ODocument> result = database.query(new OSQLSynchQuery<ODocument>(query));

    Assert.assertEquals(result.size(), 3);
    List<Integer> values = new ArrayList<Integer>(Arrays.asList(1, 2, 3));

    for (ODocument document : result) {
      Assert.assertTrue(values.remove((Integer) document.field("ai")));
    }

    Assert.assertTrue(values.isEmpty());

    ODocument explain = database.command(new OCommandSQL("explain " + query)).execute();

    Assert.assertEquals(explain.<Object>field("rangeQueryConvertedInBetween"), 1);
    Assert.assertTrue(
        ((Set<String>) explain.field("involvedIndexes")).contains("BetweenConversionTestIndex"));
  }

  public void testBetweenRightIncludedIndex() {
    final String query = "select from BetweenConversionTest where ai > 1 and ai <= 3";
    final List<ODocument> result = database.query(new OSQLSynchQuery<ODocument>(query));

    Assert.assertEquals(result.size(), 2);
    List<Integer> values = new ArrayList<Integer>(Arrays.asList(2, 3));

    for (ODocument document : result) {
      Assert.assertTrue(values.remove((Integer) document.field("ai")));
    }

    Assert.assertTrue(values.isEmpty());

    ODocument explain = database.command(new OCommandSQL("explain " + query)).execute();

    Assert.assertEquals(explain.<Object>field("rangeQueryConvertedInBetween"), 1);
    Assert.assertTrue(
        ((Set<String>) explain.field("involvedIndexes")).contains("BetweenConversionTestIndex"));
  }

  public void testBetweenRightIncludedReverseOrderIndex() {
    final String query = "select from BetweenConversionTest where ai <= 3 and ai > 1";
    final List<ODocument> result = database.query(new OSQLSynchQuery<ODocument>(query));

    Assert.assertEquals(result.size(), 2);
    List<Integer> values = new ArrayList<Integer>(Arrays.asList(2, 3));

    for (ODocument document : result) {
      Assert.assertTrue(values.remove((Integer) document.field("ai")));
    }

    Assert.assertTrue(values.isEmpty());

    ODocument explain = database.command(new OCommandSQL("explain " + query)).execute();

    Assert.assertEquals(explain.<Object>field("rangeQueryConvertedInBetween"), 1);
    Assert.assertTrue(
        ((Set<String>) explain.field("involvedIndexes")).contains("BetweenConversionTestIndex"));
  }

  public void testBetweenLeftIncludedIndex() {
    final String query = "select from BetweenConversionTest where ai >= 1 and ai < 3";
    final List<ODocument> result = database.query(new OSQLSynchQuery<ODocument>(query));

    Assert.assertEquals(result.size(), 2);
    List<Integer> values = new ArrayList<Integer>(Arrays.asList(1, 2));

    for (ODocument document : result) {
      Assert.assertTrue(values.remove((Integer) document.field("ai")));
    }

    Assert.assertTrue(values.isEmpty());

    ODocument explain = database.command(new OCommandSQL("explain " + query)).execute();

    Assert.assertEquals(explain.<Object>field("rangeQueryConvertedInBetween"), 1);
    Assert.assertTrue(
        ((Set<String>) explain.field("involvedIndexes")).contains("BetweenConversionTestIndex"));
  }

  public void testBetweenLeftIncludedReverseOrderIndex() {
    final String query = "select from BetweenConversionTest where  ai < 3 and ai >= 1";
    final List<ODocument> result = database.query(new OSQLSynchQuery<ODocument>(query));

    Assert.assertEquals(result.size(), 2);
    List<Integer> values = new ArrayList<Integer>(Arrays.asList(1, 2));

    for (ODocument document : result) {
      Assert.assertTrue(values.remove((Integer) document.field("ai")));
    }

    Assert.assertTrue(values.isEmpty());

    ODocument explain = database.command(new OCommandSQL("explain " + query)).execute();

    Assert.assertEquals(explain.<Object>field("rangeQueryConvertedInBetween"), 1);
    Assert.assertTrue(
        ((Set<String>) explain.field("involvedIndexes")).contains("BetweenConversionTestIndex"));
  }

  public void testBetweenIndex() {
    final String query = "select from BetweenConversionTest where ai > 1 and ai < 3";
    final List<ODocument> result = database.query(new OSQLSynchQuery<ODocument>(query));

    Assert.assertEquals(result.size(), 1);
    List<Integer> values = new ArrayList<Integer>(Arrays.asList(2));

    for (ODocument document : result) {
      Assert.assertTrue(values.remove((Integer) document.field("ai")));
    }

    Assert.assertTrue(values.isEmpty());

    ODocument explain = database.command(new OCommandSQL("explain " + query)).execute();

    Assert.assertEquals(explain.<Object>field("rangeQueryConvertedInBetween"), 1);
    Assert.assertTrue(
        ((Set<String>) explain.field("involvedIndexes")).contains("BetweenConversionTestIndex"));
  }

  public void testBetweenRightLeftIncludedDeepQuery() {
    final String query =
        "select from BetweenConversionTest where (vl = 'v1' and (vl <> 'v3' and (vl <> 'v2' and ((a >= 1 and a <= 7) and vl = 'v1'))) and vl <> 'v4')";
    final List<ODocument> result = database.query(new OSQLSynchQuery<ODocument>(query));

    Assert.assertEquals(result.size(), 4);
    List<Integer> values = new ArrayList<Integer>(Arrays.asList(1, 2, 3, 4));

    for (ODocument document : result) {
      Assert.assertTrue(values.remove((Integer) document.field("a")));
    }

    Assert.assertTrue(values.isEmpty());

    ODocument explain = database.command(new OCommandSQL("explain " + query)).execute();

    Assert.assertEquals(explain.<Object>field("rangeQueryConvertedInBetween"), 1);
  }

  public void testBetweenRightLeftIncludedDeepQueryIndex() {
    final String query =
        "select from BetweenConversionTest where (vl = 'v1' and (vl <> 'v3' and (vl <> 'v2' and ((ai >= 1 and ai <= 7) and vl = 'v1'))) and vl <> 'v4')";
    final List<ODocument> result = database.query(new OSQLSynchQuery<ODocument>(query));

    Assert.assertEquals(result.size(), 4);
    List<Integer> values = new ArrayList<Integer>(Arrays.asList(1, 2, 3, 4));

    for (ODocument document : result) {
      Assert.assertTrue(values.remove((Integer) document.field("ai")));
    }

    Assert.assertTrue(values.isEmpty());

    ODocument explain = database.command(new OCommandSQL("explain " + query)).execute();

    Assert.assertEquals(explain.<Object>field("rangeQueryConvertedInBetween"), 1);
    Assert.assertTrue(
        ((Set<String>) explain.field("involvedIndexes")).contains("BetweenConversionTestIndex"));
  }

  public void testBetweenRightLeftIncludedDifferentFields() {
    final String query = "select from BetweenConversionTest where a >= 1 and ai <= 3";
    final List<ODocument> result = database.query(new OSQLSynchQuery<ODocument>(query));

    Assert.assertEquals(result.size(), 3);
    List<Integer> values = new ArrayList<Integer>(Arrays.asList(1, 2, 3));

    for (ODocument document : result) {
      Assert.assertTrue(values.remove((Integer) document.field("a")));
    }

    Assert.assertTrue(values.isEmpty());

    ODocument explain = database.command(new OCommandSQL("explain " + query)).execute();

    Assert.assertNull(explain.field("rangeQueryConvertedInBetween"));
  }

  public void testBetweenNotRangeQueryRight() {
    final String query = "select from BetweenConversionTest where a >= 1 and a = 3";
    final List<ODocument> result = database.query(new OSQLSynchQuery<ODocument>(query));

    Assert.assertEquals(result.size(), 1);
    List<Integer> values = new ArrayList<Integer>(Arrays.asList(3));

    for (ODocument document : result) {
      Assert.assertTrue(values.remove((Integer) document.field("a")));
    }

    Assert.assertTrue(values.isEmpty());

    ODocument explain = database.command(new OCommandSQL("explain " + query)).execute();

    Assert.assertNull(explain.field("rangeQueryConvertedInBetween"));
  }

  public void testBetweenNotRangeQueryLeft() {
    final String query = "select from BetweenConversionTest where a = 1 and a <= 3";
    final List<ODocument> result = database.query(new OSQLSynchQuery<ODocument>(query));

    Assert.assertEquals(result.size(), 1);
    List<Integer> values = new ArrayList<Integer>(Arrays.asList(1));

    for (ODocument document : result) {
      Assert.assertTrue(values.remove((Integer) document.field("a")));
    }

    Assert.assertTrue(values.isEmpty());

    ODocument explain = database.command(new OCommandSQL("explain " + query)).execute();

    Assert.assertNull(explain.field("rangeQueryConvertedInBetween"));
  }

  public void testBetweenRightLeftIncludedBothFieldsLeft() {
    final String query = "select from BetweenConversionTest where a >= ai and a <= 3";
    final List<ODocument> result = database.query(new OSQLSynchQuery<ODocument>(query));

    Assert.assertEquals(result.size(), 4);
    List<Integer> values = new ArrayList<Integer>(Arrays.asList(0, 1, 2, 3));

    for (ODocument document : result) {
      Assert.assertTrue(values.remove((Integer) document.field("a")));
    }

    Assert.assertTrue(values.isEmpty());

    ODocument explain = database.command(new OCommandSQL("explain " + query)).execute();

    Assert.assertNull(explain.field("rangeQueryConvertedInBetween"));
  }

  public void testBetweenRightLeftIncludedBothFieldsRight() {
    final String query = "select from BetweenConversionTest where a >= 1 and a <= ai";
    final List<ODocument> result = database.query(new OSQLSynchQuery<ODocument>(query));

    Assert.assertEquals(result.size(), 9);
    List<Integer> values = new ArrayList<Integer>(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9));

    for (ODocument document : result) {
      Assert.assertTrue(values.remove((Integer) document.field("a")));
    }

    Assert.assertTrue(values.isEmpty());

    ODocument explain = database.command(new OCommandSQL("explain " + query)).execute();

    Assert.assertNull(explain.field("rangeQueryConvertedInBetween"));
  }

  public void testBetweenRightLeftIncludedFieldChainLeft() {
    final String query = "select from BetweenConversionTest where d.a >= 1 and a <= 3";
    final List<ODocument> result = database.query(new OSQLSynchQuery<ODocument>(query));

    Assert.assertEquals(result.size(), 3);
    List<Integer> values = new ArrayList<Integer>(Arrays.asList(1, 2, 3));

    for (ODocument document : result) {
      Assert.assertTrue(values.remove((Integer) document.field("a")));
    }

    Assert.assertTrue(values.isEmpty());

    ODocument explain = database.command(new OCommandSQL("explain " + query)).execute();

    Assert.assertNull(explain.field("rangeQueryConvertedInBetween"));
  }

  public void testBetweenRightLeftIncludedFieldChainRight() {
    final String query = "select from BetweenConversionTest where a >= 1 and d.a <= 3";
    final List<ODocument> result = database.query(new OSQLSynchQuery<ODocument>(query));

    Assert.assertEquals(result.size(), 3);
    List<Integer> values = new ArrayList<Integer>(Arrays.asList(1, 2, 3));

    for (ODocument document : result) {
      Assert.assertTrue(values.remove((Integer) document.field("a")));
    }

    Assert.assertTrue(values.isEmpty());

    ODocument explain = database.command(new OCommandSQL("explain " + query)).execute();

    Assert.assertNull(explain.field("rangeQueryConvertedInBetween"));
  }
}
