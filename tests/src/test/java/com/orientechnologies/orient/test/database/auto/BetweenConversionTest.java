package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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
      document.setProperty("a", i);
      document.setProperty("ai", i);

      if (i < 5) document.setProperty("vl", "v1");
      else document.setProperty("vl", "v2");

      ODocument ed = new ODocument();
      ed.setProperty("a", i);

      document.setProperty("d", ed);

      database.save(document);
    }
  }

  public void testBetweenRightLeftIncluded() {
    final String query = "select from BetweenConversionTest where a >= 1 and a <= 3";
    final List<OResult> result = database.query(query).stream().toList();

    Assert.assertEquals(result.size(), 3);
    List<Integer> values = new ArrayList<Integer>(Arrays.asList(1, 2, 3));

    for (OResult document : result) {
      Assert.assertTrue(values.remove((Integer) document.getProperty("a")));
    }

    Assert.assertTrue(values.isEmpty());
  }

  public void testBetweenRightLeftIncludedReverseOrder() {
    final String query = "select from BetweenConversionTest where a <= 3 and a >= 1";
    final List<OResult> result = database.query(query).stream().toList();

    Assert.assertEquals(result.size(), 3);
    List<Integer> values = new ArrayList<Integer>(Arrays.asList(1, 2, 3));

    for (OResult document : result) {
      Assert.assertTrue(values.remove((Integer) document.getProperty("a")));
    }

    Assert.assertTrue(values.isEmpty());
  }

  public void testBetweenRightIncluded() {
    final String query = "select from BetweenConversionTest where a > 1 and a <= 3";
    final List<OResult> result = database.query(query).stream().toList();

    Assert.assertEquals(result.size(), 2);
    List<Integer> values = new ArrayList<Integer>(Arrays.asList(2, 3));

    for (OResult document : result) {
      Assert.assertTrue(values.remove((Integer) document.getProperty("a")));
    }

    Assert.assertTrue(values.isEmpty());
  }

  public void testBetweenRightIncludedReverse() {
    final String query = "select from BetweenConversionTest where a <= 3 and a > 1";
    final List<OResult> result = database.query(query).stream().toList();

    Assert.assertEquals(result.size(), 2);
    List<Integer> values = new ArrayList<Integer>(Arrays.asList(2, 3));

    for (OResult document : result) {
      Assert.assertTrue(values.remove((Integer) document.getProperty("a")));
    }

    Assert.assertTrue(values.isEmpty());
  }

  public void testBetweenLeftIncluded() {
    final String query = "select from BetweenConversionTest where a >= 1 and a < 3";
    final List<OResult> result = database.query(query).stream().toList();

    Assert.assertEquals(result.size(), 2);
    List<Integer> values = new ArrayList<Integer>(Arrays.asList(1, 2));

    for (OResult document : result) {
      Assert.assertTrue(values.remove((Integer) document.getProperty("a")));
    }

    Assert.assertTrue(values.isEmpty());
  }

  public void testBetweenLeftIncludedReverseOrder() {
    final String query = "select from BetweenConversionTest where  a < 3 and a >= 1";
    final List<OResult> result = database.query(query).stream().toList();

    Assert.assertEquals(result.size(), 2);
    List<Integer> values = new ArrayList<Integer>(Arrays.asList(1, 2));

    for (OResult document : result) {
      Assert.assertTrue(values.remove((Integer) document.getProperty("a")));
    }

    Assert.assertTrue(values.isEmpty());
  }

  public void testBetween() {
    final String query = "select from BetweenConversionTest where a > 1 and a < 3";
    final List<OResult> result = database.query(query).stream().toList();

    Assert.assertEquals(result.size(), 1);
    List<Integer> values = new ArrayList<Integer>(Arrays.asList(2));

    for (OResult document : result) {
      Assert.assertTrue(values.remove((Integer) document.getProperty("a")));
    }

    Assert.assertTrue(values.isEmpty());
  }

  public void testBetweenRightLeftIncludedIndex() {
    final String query = "select from BetweenConversionTest where ai >= 1 and ai <= 3";
    final List<OResult> result = database.query(query).stream().toList();

    Assert.assertEquals(result.size(), 3);
    List<Integer> values = new ArrayList<Integer>(Arrays.asList(1, 2, 3));

    for (OResult document : result) {
      Assert.assertTrue(values.remove((Integer) document.getProperty("ai")));
    }

    Assert.assertTrue(values.isEmpty());

    OResultSet explainResult = database.command("explain " + query);

    Assert.assertTrue(
        explainResult.getExecutionPlan().get().getIndexes().contains("BetweenConversionTestIndex"));
  }

  public void testBetweenRightLeftIncludedReverseOrderIndex() {
    final String query = "select from BetweenConversionTest where ai <= 3 and ai >= 1";
    final List<OResult> result = database.query(query).stream().toList();

    Assert.assertEquals(result.size(), 3);
    List<Integer> values = new ArrayList<Integer>(Arrays.asList(1, 2, 3));

    for (OResult document : result) {
      Assert.assertTrue(values.remove((Integer) document.getProperty("ai")));
    }

    Assert.assertTrue(values.isEmpty());

    OResultSet explainResult = database.command("explain " + query);

    Assert.assertTrue(
        explainResult.getExecutionPlan().get().getIndexes().contains("BetweenConversionTestIndex"));
  }

  public void testBetweenRightIncludedIndex() {
    final String query = "select from BetweenConversionTest where ai > 1 and ai <= 3";
    final List<OResult> result = database.query(query).stream().toList();

    Assert.assertEquals(result.size(), 2);
    List<Integer> values = new ArrayList<Integer>(Arrays.asList(2, 3));

    for (OResult document : result) {
      Assert.assertTrue(values.remove((Integer) document.getProperty("ai")));
    }

    Assert.assertTrue(values.isEmpty());

    OResultSet explainResult = database.command("explain " + query);

    Assert.assertTrue(
        explainResult.getExecutionPlan().get().getIndexes().contains("BetweenConversionTestIndex"));
  }

  public void testBetweenRightIncludedReverseOrderIndex() {
    final String query = "select from BetweenConversionTest where ai <= 3 and ai > 1";
    final List<OResult> result = database.query(query).stream().toList();

    Assert.assertEquals(result.size(), 2);
    List<Integer> values = new ArrayList<Integer>(Arrays.asList(2, 3));

    for (OResult document : result) {
      Assert.assertTrue(values.remove((Integer) document.getProperty("ai")));
    }

    Assert.assertTrue(values.isEmpty());

    OResultSet explainResult = database.command("explain " + query);

    Assert.assertTrue(
        explainResult.getExecutionPlan().get().getIndexes().contains("BetweenConversionTestIndex"));
  }

  public void testBetweenLeftIncludedIndex() {
    final String query = "select from BetweenConversionTest where ai >= 1 and ai < 3";
    final List<OResult> result = database.query(query).stream().toList();

    Assert.assertEquals(result.size(), 2);
    List<Integer> values = new ArrayList<Integer>(Arrays.asList(1, 2));

    for (OResult document : result) {
      Assert.assertTrue(values.remove((Integer) document.getProperty("ai")));
    }

    Assert.assertTrue(values.isEmpty());

    OResultSet explainResult = database.command("explain " + query);

    Assert.assertTrue(
        explainResult.getExecutionPlan().get().getIndexes().contains("BetweenConversionTestIndex"));
  }

  public void testBetweenLeftIncludedReverseOrderIndex() {
    final String query = "select from BetweenConversionTest where  ai < 3 and ai >= 1";
    final List<OResult> result = database.query(query).stream().toList();

    Assert.assertEquals(result.size(), 2);
    List<Integer> values = new ArrayList<Integer>(Arrays.asList(1, 2));

    for (OResult document : result) {
      Assert.assertTrue(values.remove((Integer) document.getProperty("ai")));
    }

    Assert.assertTrue(values.isEmpty());

    OResultSet explainResult = database.command("explain " + query);

    Assert.assertTrue(
        explainResult.getExecutionPlan().get().getIndexes().contains("BetweenConversionTestIndex"));
  }

  public void testBetweenIndex() {
    final String query = "select from BetweenConversionTest where ai > 1 and ai < 3";
    final List<OResult> result = database.query(query).stream().toList();

    Assert.assertEquals(result.size(), 1);
    List<Integer> values = new ArrayList<Integer>(Arrays.asList(2));

    for (OResult document : result) {
      Assert.assertTrue(values.remove((Integer) document.getProperty("ai")));
    }

    Assert.assertTrue(values.isEmpty());

    OResultSet explainResult = database.command("explain " + query);

    Assert.assertTrue(
        explainResult.getExecutionPlan().get().getIndexes().contains("BetweenConversionTestIndex"));
  }

  public void testBetweenRightLeftIncludedDeepQuery() {
    final String query =
        "select from BetweenConversionTest where (vl = 'v1' and (vl <> 'v3' and (vl <> 'v2' and ((a"
            + " >= 1 and a <= 7) and vl = 'v1'))) and vl <> 'v4')";
    final List<OResult> result = database.query(query).stream().toList();

    Assert.assertEquals(result.size(), 4);
    List<Integer> values = new ArrayList<Integer>(Arrays.asList(1, 2, 3, 4));

    for (OResult document : result) {
      Assert.assertTrue(values.remove((Integer) document.getProperty("a")));
    }

    Assert.assertTrue(values.isEmpty());
  }

  public void testBetweenRightLeftIncludedDeepQueryIndex() {
    final String query =
        "select from BetweenConversionTest where (vl = 'v1' and (vl <> 'v3' and (vl <> 'v2' and"
            + " ((ai >= 1 and ai <= 7) and vl = 'v1'))) and vl <> 'v4')";
    final List<OResult> result = database.query(query).stream().toList();

    Assert.assertEquals(result.size(), 4);
    List<Integer> values = new ArrayList<Integer>(Arrays.asList(1, 2, 3, 4));

    for (OResult document : result) {
      Assert.assertTrue(values.remove((Integer) document.getProperty("ai")));
    }

    Assert.assertTrue(values.isEmpty());

    OResultSet explainResult = database.command("explain " + query);

    Assert.assertTrue(
        explainResult.getExecutionPlan().get().getIndexes().contains("BetweenConversionTestIndex"));
  }

  public void testBetweenRightLeftIncludedDifferentFields() {
    final String query = "select from BetweenConversionTest where a >= 1 and ai <= 3";
    final List<OResult> result = database.query(query).stream().toList();

    Assert.assertEquals(result.size(), 3);
    List<Integer> values = new ArrayList<Integer>(Arrays.asList(1, 2, 3));

    for (OResult document : result) {
      Assert.assertTrue(values.remove((Integer) document.getProperty("a")));
    }

    Assert.assertTrue(values.isEmpty());
  }

  public void testBetweenNotRangeQueryRight() {
    final String query = "select from BetweenConversionTest where a >= 1 and a = 3";
    final List<OResult> result = database.query(query).stream().toList();

    Assert.assertEquals(result.size(), 1);
    List<Integer> values = new ArrayList<Integer>(Arrays.asList(3));

    for (OResult document : result) {
      Assert.assertTrue(values.remove((Integer) document.getProperty("a")));
    }

    Assert.assertTrue(values.isEmpty());
  }

  public void testBetweenNotRangeQueryLeft() {
    final String query = "select from BetweenConversionTest where a = 1 and a <= 3";
    final List<OResult> result = database.query(query).stream().toList();

    Assert.assertEquals(result.size(), 1);
    List<Integer> values = new ArrayList<Integer>(Arrays.asList(1));

    for (OResult document : result) {
      Assert.assertTrue(values.remove((Integer) document.getProperty("a")));
    }

    Assert.assertTrue(values.isEmpty());
  }

  public void testBetweenRightLeftIncludedBothFieldsLeft() {
    final String query = "select from BetweenConversionTest where a >= ai and a <= 3";
    final List<OResult> result = database.query(query).stream().toList();

    Assert.assertEquals(result.size(), 4);
    List<Integer> values = new ArrayList<Integer>(Arrays.asList(0, 1, 2, 3));

    for (OResult document : result) {
      Assert.assertTrue(values.remove((Integer) document.getProperty("a")));
    }

    Assert.assertTrue(values.isEmpty());
  }

  public void testBetweenRightLeftIncludedBothFieldsRight() {
    final String query = "select from BetweenConversionTest where a >= 1 and a <= ai";
    final List<OResult> result = database.query(query).stream().toList();

    Assert.assertEquals(result.size(), 9);
    List<Integer> values = new ArrayList<Integer>(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9));

    for (OResult document : result) {
      Assert.assertTrue(values.remove((Integer) document.getProperty("a")));
    }

    Assert.assertTrue(values.isEmpty());
  }

  public void testBetweenRightLeftIncludedFieldChainLeft() {
    final String query = "select from BetweenConversionTest where d.a >= 1 and a <= 3";
    final List<OResult> result = database.query(query).stream().toList();

    Assert.assertEquals(result.size(), 3);
    List<Integer> values = new ArrayList<Integer>(Arrays.asList(1, 2, 3));

    for (OResult document : result) {
      Assert.assertTrue(values.remove((Integer) document.getProperty("a")));
    }

    Assert.assertTrue(values.isEmpty());
  }

  public void testBetweenRightLeftIncludedFieldChainRight() {
    final String query = "select from BetweenConversionTest where a >= 1 and d.a <= 3";
    final List<OResult> result = database.query(query).stream().toList();

    Assert.assertEquals(result.size(), 3);
    List<Integer> values = new ArrayList<Integer>(Arrays.asList(1, 2, 3));

    for (OResult document : result) {
      Assert.assertTrue(values.remove((Integer) document.getProperty("a")));
    }

    Assert.assertTrue(values.isEmpty());
  }
}
