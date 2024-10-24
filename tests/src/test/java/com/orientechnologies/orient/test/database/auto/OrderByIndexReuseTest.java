package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 2/11/14
 */
@Test
public class OrderByIndexReuseTest extends DocumentDBBaseTest {

  @Parameters(value = "url")
  public OrderByIndexReuseTest(@Optional String url) {
    super(url);
  }

  @Override
  @BeforeClass
  public void beforeClass() throws Exception {
    super.beforeClass();

    final OSchema schema = database.getMetadata().getSchema();

    final OClass orderByIndexReuse = schema.createClass("OrderByIndexReuse", 1, null);

    orderByIndexReuse.createProperty("firstProp", OType.INTEGER);
    orderByIndexReuse.createProperty("secondProp", OType.INTEGER);
    orderByIndexReuse.createProperty("thirdProp", OType.STRING);
    orderByIndexReuse.createProperty("prop4", OType.STRING);

    orderByIndexReuse.createIndex(
        "OrderByIndexReuseIndexSecondThirdProp",
        OClass.INDEX_TYPE.UNIQUE,
        "secondProp",
        "thirdProp");
    orderByIndexReuse.createIndex(
        "OrderByIndexReuseIndexFirstPropNotUnique", OClass.INDEX_TYPE.NOTUNIQUE, "firstProp");

    for (int i = 0; i < 100; i++) {
      ODocument document = new ODocument("OrderByIndexReuse");
      document.setProperty("firstProp", (101 - i) / 2);
      document.setProperty("secondProp", (101 - i) / 2);

      document.setProperty("thirdProp", "prop" + (101 - i));
      document.setProperty("prop4", "prop" + (101 - i));

      database.save(document);
    }
  }

  public void testGreaterThanOrderByAscFirstProperty() {
    String query = "select from OrderByIndexReuse where firstProp > 5 order by firstProp limit 5";
    List<OResult> result = database.query(query).stream().toList();

    Assert.assertEquals(result.size(), 5);
    for (int i = 0; i < 5; i++) {
      OResult document = result.get(i);
      Assert.assertEquals((int) document.<Integer>getProperty("firstProp"), i / 2 + 6);
    }

    final OResultSet explainResult = database.command("explain " + query);

    Assert.assertEquals(
        explainResult.getExecutionPlan().get().getIndexes().toArray(),
        new String[] {"OrderByIndexReuseIndexFirstPropNotUnique"});
  }

  public void testGreaterThanOrderByAscSecondAscThirdProperty() {
    String query =
        "select from OrderByIndexReuse where secondProp > 5 order by secondProp asc, thirdProp asc"
            + " limit 5";
    List<OResult> result = database.query(query).stream().toList();

    Assert.assertEquals(result.size(), 5);
    for (int i = 0; i < 5; i++) {
      OResult document = result.get(i);
      Assert.assertEquals((int) document.<Integer>getProperty("secondProp"), i / 2 + 6);
      Assert.assertEquals(document.getProperty("thirdProp"), "prop" + (i + 12));
    }

    final OResultSet explainResult = database.command("explain " + query);

    Assert.assertEquals(
        explainResult.getExecutionPlan().get().getIndexes().toArray(),
        new String[] {"OrderByIndexReuseIndexSecondThirdProp"});
  }

  public void testGreaterThanOrderByDescSecondDescThirdProperty() {
    String query =
        "select from OrderByIndexReuse where secondProp > 5 order by secondProp desc, thirdProp"
            + " desc limit 5";
    List<OResult> result = database.query(query).stream().toList();

    Assert.assertEquals(result.size(), 5);
    for (int i = 0; i < 5; i++) {
      OResult document = result.get(i);
      Assert.assertEquals((int) document.<Integer>getProperty("secondProp"), 50 - i / 2);
      Assert.assertEquals(document.getProperty("thirdProp"), "prop" + (101 - i));
    }

    final OResultSet explainResult = database.command("explain " + query);

    Assert.assertEquals(
        explainResult.getExecutionPlan().get().getIndexes().toArray(),
        new String[] {"OrderByIndexReuseIndexSecondThirdProp"});
  }

  public void testGreaterThanOrderByAscSecondDescThirdProperty() {
    String query =
        "select from OrderByIndexReuse where secondProp > 5 order by secondProp asc, thirdProp desc"
            + " limit 5";
    List<OResult> result = database.query(query).stream().toList();

    Assert.assertEquals(result.size(), 5);
    for (int i = 0; i < 5; i++) {
      OResult document = result.get(i);
      Assert.assertEquals((int) document.<Integer>getProperty("secondProp"), i / 2 + 6);
      int thirdPropertyIndex;
      if (i % 2 == 0) thirdPropertyIndex = document.<Integer>getProperty("secondProp") * 2 + 1;
      else thirdPropertyIndex = document.<Integer>getProperty("secondProp") * 2;

      Assert.assertEquals(document.getProperty("thirdProp"), "prop" + thirdPropertyIndex);
    }

    final OResultSet explainResult = database.command("explain " + query);

    Assert.assertEquals(
        explainResult.getExecutionPlan().get().getIndexes().toArray(),
        new String[] {"OrderByIndexReuseIndexSecondThirdProp"});
  }

  public void testGreaterThanOrderByDescFirstProperty() {
    final String query =
        "select from OrderByIndexReuse where firstProp > 5 order by firstProp desc limit 5";
    List<OResult> result = database.query(query).stream().toList();

    Assert.assertEquals(result.size(), 5);
    for (int i = 0; i < 5; i++) {
      OResult document = result.get(i);
      Assert.assertEquals((int) document.<Integer>getProperty("firstProp"), 50 - i / 2);
    }

    final OResultSet explainResult = database.command("explain " + query);

    Assert.assertEquals(
        explainResult.getExecutionPlan().get().getIndexes().toArray(),
        new String[] {"OrderByIndexReuseIndexFirstPropNotUnique"});
  }

  public void testGTEOrderByAscFirstProperty() {
    final String query =
        "select from OrderByIndexReuse where firstProp >= 5 order by firstProp limit 5";
    List<OResult> result = database.query(query).stream().toList();

    Assert.assertEquals(result.size(), 5);
    for (int i = 0; i < 5; i++) {
      OResult document = result.get(i);
      Assert.assertEquals((int) document.<Integer>getProperty("firstProp"), i / 2 + 5);
    }

    final OResultSet explainResult = database.command("explain " + query);

    Assert.assertEquals(
        explainResult.getExecutionPlan().get().getIndexes().toArray(),
        new String[] {"OrderByIndexReuseIndexFirstPropNotUnique"});
  }

  public void testGTEOrderByAscSecondPropertyAscThirdProperty() {
    final String query =
        "select from OrderByIndexReuse where secondProp >= 5 order by secondProp asc, thirdProp asc"
            + " limit 5";
    List<OResult> result = database.query(query).stream().toList();

    Assert.assertEquals(result.size(), 5);
    for (int i = 0; i < 5; i++) {
      OResult document = result.get(i);
      Assert.assertEquals((int) document.<Integer>getProperty("secondProp"), i / 2 + 5);
      int thirdPropertyIndex;
      if (i % 2 == 0) thirdPropertyIndex = document.<Integer>getProperty("secondProp") * 2;
      else thirdPropertyIndex = document.<Integer>getProperty("secondProp") * 2 + 1;

      Assert.assertEquals(document.getProperty("thirdProp"), "prop" + thirdPropertyIndex);
    }

    final OResultSet explainResult = database.command("explain " + query);

    Assert.assertEquals(
        explainResult.getExecutionPlan().get().getIndexes().toArray(),
        new String[] {"OrderByIndexReuseIndexSecondThirdProp"});
  }

  public void testGTEOrderByDescSecondPropertyDescThirdProperty() {
    final String query =
        "select from OrderByIndexReuse where secondProp >= 5 order by secondProp desc, thirdProp"
            + " desc limit 5";
    List<OResult> result = database.query(query).stream().toList();

    Assert.assertEquals(result.size(), 5);
    for (int i = 0; i < 5; i++) {
      OResult document = result.get(i);
      Assert.assertEquals((int) document.<Integer>getProperty("secondProp"), 50 - i / 2);
      int thirdPropertyIndex;
      if (i % 2 == 0) thirdPropertyIndex = document.<Integer>getProperty("secondProp") * 2 + 1;
      else thirdPropertyIndex = document.<Integer>getProperty("secondProp") * 2;

      Assert.assertEquals(document.getProperty("thirdProp"), "prop" + thirdPropertyIndex);
    }

    final OResultSet explainResult = database.command("explain " + query);

    Assert.assertEquals(
        explainResult.getExecutionPlan().get().getIndexes().toArray(),
        new String[] {"OrderByIndexReuseIndexSecondThirdProp"});
  }

  public void testGTEOrderByAscSecondPropertyDescThirdProperty() {
    final String query =
        "select from OrderByIndexReuse where secondProp >= 5 order by secondProp asc, thirdProp"
            + " desc limit 5";
    List<OResult> result = database.query(query).stream().toList();

    Assert.assertEquals(result.size(), 5);
    for (int i = 0; i < 5; i++) {
      OResult document = result.get(i);
      Assert.assertEquals((int) document.<Integer>getProperty("secondProp"), i / 2 + 5);
      int thirdPropertyIndex;
      if (i % 2 == 0) thirdPropertyIndex = document.<Integer>getProperty("secondProp") * 2 + 1;
      else thirdPropertyIndex = document.<Integer>getProperty("secondProp") * 2;

      Assert.assertEquals(document.getProperty("thirdProp"), "prop" + thirdPropertyIndex);
    }

    final OResultSet explainResult = database.command("explain " + query);

    Assert.assertEquals(
        explainResult.getExecutionPlan().get().getIndexes().toArray(),
        new String[] {"OrderByIndexReuseIndexSecondThirdProp"});
  }

  public void testGTEOrderByDescFirstProperty() {
    final String query =
        "select from OrderByIndexReuse where firstProp >= 5 order by firstProp desc limit 5";
    List<OResult> result = database.query(query).stream().toList();

    Assert.assertEquals(result.size(), 5);
    for (int i = 0; i < 5; i++) {
      OResult document = result.get(i);
      Assert.assertEquals((int) document.<Integer>getProperty("firstProp"), 50 - i / 2);
    }

    final OResultSet explainResult = database.command("explain " + query);

    Assert.assertEquals(
        explainResult.getExecutionPlan().get().getIndexes().toArray(),
        new String[] {"OrderByIndexReuseIndexFirstPropNotUnique"});
  }

  public void testLTOrderByAscFirstProperty() {
    final String query =
        "select from OrderByIndexReuse where firstProp < 5 order by firstProp limit 3";
    List<OResult> result = database.query(query).stream().toList();

    Assert.assertEquals(result.size(), 3);
    for (int i = 0; i < 3; i++) {
      OResult document = result.get(i);
      Assert.assertEquals((int) document.<Integer>getProperty("firstProp"), i / 2 + 1);
    }

    final OResultSet explainResult = database.command("explain " + query);

    Assert.assertEquals(
        explainResult.getExecutionPlan().get().getIndexes().toArray(),
        new String[] {"OrderByIndexReuseIndexFirstPropNotUnique"});
  }

  public void testLTOrderByAscSecondAscThirdProperty() {
    final String query =
        "select from OrderByIndexReuse where secondProp < 5 order by secondProp asc, thirdProp asc"
            + " limit 3";
    List<OResult> result = database.query(query).stream().toList();

    Assert.assertEquals(result.size(), 3);
    for (int i = 0; i < 3; i++) {
      OResult document = result.get(i);
      Assert.assertEquals((int) document.<Integer>getProperty("secondProp"), i / 2 + 1);

      int thirdPropertyIndex;
      if (i % 2 == 0) thirdPropertyIndex = document.<Integer>getProperty("secondProp") * 2;
      else thirdPropertyIndex = document.<Integer>getProperty("secondProp") * 2 + 1;

      Assert.assertEquals(document.getProperty("thirdProp"), "prop" + thirdPropertyIndex);
    }

    final OResultSet explainResult = database.command("explain " + query);

    Assert.assertEquals(
        explainResult.getExecutionPlan().get().getIndexes().toArray(),
        new String[] {"OrderByIndexReuseIndexSecondThirdProp"});
  }

  public void testLTOrderByDescSecondDescThirdProperty() {
    final String query =
        "select from OrderByIndexReuse where secondProp < 5 order by secondProp desc, thirdProp"
            + " desc limit 3";
    List<OResult> result = database.query(query).stream().toList();

    Assert.assertEquals(result.size(), 3);
    for (int i = 0; i < 3; i++) {
      OResult document = result.get(i);
      Assert.assertEquals((int) document.<Integer>getProperty("secondProp"), 4 - i / 2);

      int thirdPropertyIndex;
      if (i % 2 == 0) thirdPropertyIndex = document.<Integer>getProperty("secondProp") * 2 + 1;
      else thirdPropertyIndex = document.<Integer>getProperty("secondProp") * 2;

      Assert.assertEquals(document.getProperty("thirdProp"), "prop" + thirdPropertyIndex);
    }

    final OResultSet explainResult = database.command("explain " + query);

    Assert.assertEquals(
        explainResult.getExecutionPlan().get().getIndexes().toArray(),
        new String[] {"OrderByIndexReuseIndexSecondThirdProp"});
  }

  public void testLTOrderByAscSecondDescThirdProperty() {
    final String query =
        "select from OrderByIndexReuse where secondProp < 5 order by secondProp asc, thirdProp desc"
            + " limit 3";
    List<OResult> result = database.query(query).stream().toList();

    Assert.assertEquals(result.size(), 3);
    for (int i = 0; i < 3; i++) {
      OResult document = result.get(i);
      Assert.assertEquals((int) document.<Integer>getProperty("secondProp"), i / 2 + 1);

      int thirdPropertyIndex;
      if (i % 2 == 0) thirdPropertyIndex = document.<Integer>getProperty("secondProp") * 2 + 1;
      else thirdPropertyIndex = document.<Integer>getProperty("secondProp") * 2;

      Assert.assertEquals(document.getProperty("thirdProp"), "prop" + thirdPropertyIndex);
    }

    final OResultSet explainResult = database.command("explain " + query);

    Assert.assertEquals(
        explainResult.getExecutionPlan().get().getIndexes().toArray(),
        new String[] {"OrderByIndexReuseIndexSecondThirdProp"});
  }

  public void testLTOrderByDescFirstProperty() {
    final String query =
        "select from OrderByIndexReuse where firstProp < 5 order by firstProp desc limit 3";
    List<OResult> result = database.query(query).stream().toList();

    Assert.assertEquals(result.size(), 3);
    for (int i = 0; i < 3; i++) {
      OResult document = result.get(i);
      Assert.assertEquals((int) document.<Integer>getProperty("firstProp"), 4 - i / 2);
    }

    final OResultSet explainResult = database.command("explain " + query);

    Assert.assertEquals(
        explainResult.getExecutionPlan().get().getIndexes().toArray(),
        new String[] {"OrderByIndexReuseIndexFirstPropNotUnique"});
  }

  public void testLTEOrderByAscFirstProperty() {
    final String query =
        "select from OrderByIndexReuse where firstProp <= 5 order by firstProp limit 3";
    List<OResult> result = database.query(query).stream().toList();

    Assert.assertEquals(result.size(), 3);
    for (int i = 0; i < 3; i++) {
      OResult document = result.get(i);
      Assert.assertEquals((int) document.<Integer>getProperty("firstProp"), i / 2 + 1);
    }

    final OResultSet explainResult = database.command("explain " + query);

    Assert.assertEquals(
        explainResult.getExecutionPlan().get().getIndexes().toArray(),
        new String[] {"OrderByIndexReuseIndexFirstPropNotUnique"});
  }

  public void testLTEOrderByAscSecondAscThirdProperty() {
    final String query =
        "select from OrderByIndexReuse where secondProp <= 5 order by secondProp asc, thirdProp asc"
            + " limit 3";
    List<OResult> result = database.query(query).stream().toList();

    Assert.assertEquals(result.size(), 3);
    for (int i = 0; i < 3; i++) {
      OResult document = result.get(i);
      Assert.assertEquals((int) document.<Integer>getProperty("secondProp"), i / 2 + 1);

      int thirdPropertyIndex;
      if (i % 2 == 0) thirdPropertyIndex = document.<Integer>getProperty("secondProp") * 2;
      else thirdPropertyIndex = document.<Integer>getProperty("secondProp") * 2 + 1;

      Assert.assertEquals(document.getProperty("thirdProp"), "prop" + thirdPropertyIndex);
    }

    final OResultSet explainResult = database.command("explain " + query);

    Assert.assertEquals(
        explainResult.getExecutionPlan().get().getIndexes().toArray(),
        new String[] {"OrderByIndexReuseIndexSecondThirdProp"});
  }

  public void testLTEOrderByDescSecondDescThirdProperty() {
    final String query =
        "select from OrderByIndexReuse where secondProp <= 5 order by secondProp desc, thirdProp"
            + " desc limit 3";
    List<OResult> result = database.query(query).stream().toList();

    Assert.assertEquals(result.size(), 3);
    for (int i = 0; i < 3; i++) {
      OResult document = result.get(i);
      Assert.assertEquals((int) document.<Integer>getProperty("secondProp"), 5 - i / 2);

      int thirdPropertyIndex;
      if (i % 2 == 0) thirdPropertyIndex = document.<Integer>getProperty("secondProp") * 2 + 1;
      else thirdPropertyIndex = document.<Integer>getProperty("secondProp") * 2;

      Assert.assertEquals(document.getProperty("thirdProp"), "prop" + thirdPropertyIndex);
    }

    final OResultSet explainResult = database.command("explain " + query);

    Assert.assertEquals(
        explainResult.getExecutionPlan().get().getIndexes().toArray(),
        new String[] {"OrderByIndexReuseIndexSecondThirdProp"});
  }

  public void testLTEOrderByAscSecondDescThirdProperty() {
    final String query =
        "select from OrderByIndexReuse where secondProp <= 5 order by secondProp asc, thirdProp"
            + " desc limit 3";
    List<OResult> result = database.query(query).stream().toList();

    Assert.assertEquals(result.size(), 3);
    for (int i = 0; i < 3; i++) {
      OResult document = result.get(i);
      Assert.assertEquals((int) document.<Integer>getProperty("secondProp"), i / 2 + 1);

      int thirdPropertyIndex;
      if (i % 2 == 0) thirdPropertyIndex = document.<Integer>getProperty("secondProp") * 2 + 1;
      else thirdPropertyIndex = document.<Integer>getProperty("secondProp") * 2;

      Assert.assertEquals(document.getProperty("thirdProp"), "prop" + thirdPropertyIndex);
    }

    final OResultSet explainResult = database.command("explain " + query);

    Assert.assertEquals(
        explainResult.getExecutionPlan().get().getIndexes().toArray(),
        new String[] {"OrderByIndexReuseIndexSecondThirdProp"});
  }

  public void testLTEOrderByDescFirstProperty() {
    final String query =
        "select from OrderByIndexReuse where firstProp <= 5 order by firstProp desc limit 3";
    List<OResult> result = database.query(query).stream().toList();

    Assert.assertEquals(result.size(), 3);
    for (int i = 0; i < 3; i++) {
      OResult document = result.get(i);
      Assert.assertEquals((int) document.<Integer>getProperty("firstProp"), 5 - i / 2);
    }

    final OResultSet explainResult = database.command("explain " + query);

    Assert.assertEquals(
        explainResult.getExecutionPlan().get().getIndexes().toArray(),
        new String[] {"OrderByIndexReuseIndexFirstPropNotUnique"});
  }

  public void testBetweenOrderByAscFirstProperty() {
    final String query =
        "select from OrderByIndexReuse where firstProp between 5 and 15 order by firstProp limit 5";
    List<OResult> result = database.query(query).stream().toList();

    Assert.assertEquals(result.size(), 5);
    for (int i = 0; i < 5; i++) {
      OResult document = result.get(i);
      Assert.assertEquals((int) document.<Integer>getProperty("firstProp"), i / 2 + 5);
    }

    final OResultSet explainResult = database.command("explain " + query);

    Assert.assertEquals(
        explainResult.getExecutionPlan().get().getIndexes().toArray(),
        new String[] {"OrderByIndexReuseIndexFirstPropNotUnique"});
  }

  public void testBetweenOrderByAscSecondAscThirdProperty() {
    final String query =
        "select from OrderByIndexReuse where secondProp between 5 and 15 order by secondProp asc,"
            + " thirdProp asc limit 5";
    List<OResult> result = database.query(query).stream().toList();

    Assert.assertEquals(result.size(), 5);
    for (int i = 0; i < 5; i++) {
      OResult document = result.get(i);
      Assert.assertEquals((int) document.<Integer>getProperty("secondProp"), i / 2 + 5);

      int thirdPropertyIndex;
      if (i % 2 == 0) thirdPropertyIndex = document.<Integer>getProperty("secondProp") * 2;
      else thirdPropertyIndex = document.<Integer>getProperty("secondProp") * 2 + 1;

      Assert.assertEquals(document.getProperty("thirdProp"), "prop" + thirdPropertyIndex);
    }

    final OResultSet explainResult = database.command("explain " + query);

    Assert.assertEquals(
        explainResult.getExecutionPlan().get().getIndexes().toArray(),
        new String[] {"OrderByIndexReuseIndexSecondThirdProp"});
  }

  public void testBetweenOrderByDescSecondDescThirdProperty() {
    final String query =
        "select from OrderByIndexReuse where secondProp between 5 and 15 order by secondProp desc,"
            + " thirdProp desc limit 5";
    List<OResult> result = database.query(query).stream().toList();

    Assert.assertEquals(result.size(), 5);
    for (int i = 0; i < 5; i++) {
      OResult document = result.get(i);
      Assert.assertEquals((int) document.<Integer>getProperty("secondProp"), 15 - i / 2);

      int thirdPropertyIndex;
      if (i % 2 == 0) thirdPropertyIndex = document.<Integer>getProperty("secondProp") * 2 + 1;
      else thirdPropertyIndex = document.<Integer>getProperty("secondProp") * 2;

      Assert.assertEquals(document.getProperty("thirdProp"), "prop" + thirdPropertyIndex);
    }

    final OResultSet explainResult = database.command("explain " + query);

    Assert.assertEquals(
        explainResult.getExecutionPlan().get().getIndexes().toArray(),
        new String[] {"OrderByIndexReuseIndexSecondThirdProp"});
  }

  public void testBetweenOrderByAscSecondDescThirdProperty() {
    final String query =
        "select from OrderByIndexReuse where secondProp between 5 and 15 order by secondProp asc,"
            + " thirdProp desc limit 5";
    List<OResult> result = database.query(query).stream().toList();

    Assert.assertEquals(result.size(), 5);
    for (int i = 0; i < 5; i++) {
      OResult document = result.get(i);
      Assert.assertEquals((int) document.<Integer>getProperty("secondProp"), i / 2 + 5);

      int thirdPropertyIndex;
      if (i % 2 == 0) thirdPropertyIndex = document.<Integer>getProperty("secondProp") * 2 + 1;
      else thirdPropertyIndex = document.<Integer>getProperty("secondProp") * 2;

      Assert.assertEquals(document.getProperty("thirdProp"), "prop" + thirdPropertyIndex);
    }

    final OResultSet explainResult = database.command("explain " + query);

    Assert.assertEquals(
        explainResult.getExecutionPlan().get().getIndexes().toArray(),
        new String[] {"OrderByIndexReuseIndexSecondThirdProp"});
  }

  public void testBetweenOrderByDescFirstProperty() {
    final String query =
        "select from OrderByIndexReuse where firstProp between 5 and 15 order by firstProp desc"
            + " limit 5";
    List<OResult> result = database.query(query).stream().toList();

    Assert.assertEquals(result.size(), 5);
    for (int i = 0; i < 5; i++) {
      OResult document = result.get(i);
      Assert.assertEquals((int) document.<Integer>getProperty("firstProp"), 15 - i / 2);
    }

    final OResultSet explainResult = database.command("explain " + query);

    Assert.assertEquals(
        explainResult.getExecutionPlan().get().getIndexes().toArray(),
        new String[] {"OrderByIndexReuseIndexFirstPropNotUnique"});
  }

  public void testInOrderByAscFirstProperty() {
    final String query =
        "select from OrderByIndexReuse where firstProp in [10, 2, 43, 21, 45, 47, 11, 12] order by"
            + " firstProp limit 3";
    List<OResult> result = database.query(query).stream().toList();

    Assert.assertEquals(result.size(), 3);

    OResult document = result.get(0);
    Assert.assertEquals((int) document.<Integer>getProperty("firstProp"), 2);

    document = result.get(1);
    Assert.assertEquals((int) document.<Integer>getProperty("firstProp"), 2);

    document = result.get(2);
    Assert.assertEquals((int) document.<Integer>getProperty("firstProp"), 10);

    final OResultSet explainResult = database.command("explain " + query);

    Assert.assertEquals(
        explainResult.getExecutionPlan().get().getIndexes().toArray(),
        new String[] {"OrderByIndexReuseIndexFirstPropNotUnique"});
  }

  public void testInOrderByDescFirstProperty() {
    final String query =
        "select from OrderByIndexReuse where firstProp in [10, 2, 43, 21, 45, 47, 11, 12] order by"
            + " firstProp desc limit 3";
    List<OResult> result = database.query(query).stream().toList();

    Assert.assertEquals(result.size(), 3);

    OResult document = result.get(0);
    Assert.assertEquals((int) document.<Integer>getProperty("firstProp"), 47);

    document = result.get(1);
    Assert.assertEquals((int) document.<Integer>getProperty("firstProp"), 47);

    document = result.get(2);
    Assert.assertEquals((int) document.<Integer>getProperty("firstProp"), 45);

    final OResultSet explainResult = database.command("explain " + query);

    Assert.assertEquals(
        explainResult.getExecutionPlan().get().getIndexes().toArray(),
        new String[] {"OrderByIndexReuseIndexFirstPropNotUnique"});
  }

  public void testGreaterThanOrderByAscFirstAscFourthProperty() {
    String query =
        "select from OrderByIndexReuse where firstProp > 5 order by firstProp asc, prop4 asc limit"
            + " 5";
    List<OResult> result = database.query(query).stream().toList();

    Assert.assertEquals(result.size(), 5);
    for (int i = 0; i < 5; i++) {
      OResult document = result.get(i);
      Assert.assertEquals((int) document.<Integer>getProperty("firstProp"), i / 2 + 6);
      Assert.assertEquals(document.<String>getProperty("prop4"), "prop" + (i + 12));
    }

    final OResultSet explainResult = database.command("explain " + query);

    Assert.assertEquals(
        explainResult.getExecutionPlan().get().getIndexes().toArray(),
        new String[] {"OrderByIndexReuseIndexFirstPropNotUnique"});
  }

  public void testGreaterThanOrderByDescFirstPropertyAscFourthProperty() {
    final String query =
        "select from OrderByIndexReuse where firstProp > 5 order by firstProp desc, prop4 asc limit"
            + " 5";
    List<OResult> result = database.query(query).stream().toList();

    Assert.assertEquals(result.size(), 5);
    for (int i = 0; i < 5; i++) {
      OResult document = result.get(i);
      Assert.assertEquals((int) document.<Integer>getProperty("firstProp"), 50 - i / 2);
      int property4Index;
      if (i % 2 == 0) property4Index = document.<Integer>getProperty("firstProp") * 2;
      else property4Index = document.<Integer>getProperty("firstProp") * 2 + 1;

      Assert.assertEquals(document.getProperty("prop4"), "prop" + property4Index);
    }

    final OResultSet explainResult = database.command("explain " + query);

    Assert.assertEquals(
        explainResult.getExecutionPlan().get().getIndexes().toArray(),
        new String[] {"OrderByIndexReuseIndexFirstPropNotUnique"});
  }

  public void testGTEOrderByAscFirstPropertyAscFourthProperty() {
    final String query =
        "select from OrderByIndexReuse where firstProp >= 5 order by firstProp asc, prop4 asc limit"
            + " 5";
    List<OResult> result = database.query(query).stream().toList();

    Assert.assertEquals(result.size(), 5);
    for (int i = 0; i < 5; i++) {
      OResult document = result.get(i);
      Assert.assertEquals((int) document.<Integer>getProperty("firstProp"), i / 2 + 5);

      int property4Index;
      if (i % 2 == 0) property4Index = document.<Integer>getProperty("firstProp") * 2;
      else property4Index = document.<Integer>getProperty("firstProp") * 2 + 1;

      Assert.assertEquals(document.getProperty("prop4"), "prop" + property4Index);
    }

    final OResultSet explainResult = database.command("explain " + query);

    Assert.assertEquals(
        explainResult.getExecutionPlan().get().getIndexes().toArray(),
        new String[] {"OrderByIndexReuseIndexFirstPropNotUnique"});
  }

  public void testGTEOrderByDescFirstPropertyAscFourthProperty() {
    final String query =
        "select from OrderByIndexReuse where firstProp >= 5 order by firstProp desc, prop4 asc"
            + " limit 5";
    List<OResult> result = database.query(query).stream().toList();

    Assert.assertEquals(result.size(), 5);
    for (int i = 0; i < 5; i++) {
      OResult document = result.get(i);
      Assert.assertEquals((int) document.<Integer>getProperty("firstProp"), 50 - i / 2);

      int property4Index;
      if (i % 2 == 0) property4Index = document.<Integer>getProperty("firstProp") * 2;
      else property4Index = document.<Integer>getProperty("firstProp") * 2 + 1;

      Assert.assertEquals(document.getProperty("prop4"), "prop" + property4Index);
    }

    final OResultSet explainResult = database.command("explain " + query);

    Assert.assertEquals(
        explainResult.getExecutionPlan().get().getIndexes().toArray(),
        new String[] {"OrderByIndexReuseIndexFirstPropNotUnique"});
  }

  public void testLTOrderByAscFirstPropertyAscFourthProperty() {
    final String query =
        "select from OrderByIndexReuse where firstProp < 5 order by firstProp asc, prop4 asc limit"
            + " 3";
    List<OResult> result = database.query(query).stream().toList();

    Assert.assertEquals(result.size(), 3);
    for (int i = 0; i < 3; i++) {
      OResult document = result.get(i);
      Assert.assertEquals((int) document.<Integer>getProperty("firstProp"), i / 2 + 1);

      int property4Index;
      if (i % 2 == 0) property4Index = document.<Integer>getProperty("firstProp") * 2;
      else property4Index = document.<Integer>getProperty("firstProp") * 2 + 1;

      Assert.assertEquals(document.getProperty("prop4"), "prop" + property4Index);
    }

    final OResultSet explainResult = database.command("explain " + query);

    Assert.assertEquals(
        explainResult.getExecutionPlan().get().getIndexes().toArray(),
        new String[] {"OrderByIndexReuseIndexFirstPropNotUnique"});
  }

  public void testLTOrderByDescFirstPropertyAscFourthProperty() {
    final String query =
        "select from OrderByIndexReuse where firstProp < 5 order by firstProp desc, prop4 asc limit"
            + " 3";
    List<OResult> result = database.query(query).stream().toList();

    Assert.assertEquals(result.size(), 3);
    for (int i = 0; i < 3; i++) {
      OResult document = result.get(i);
      Assert.assertEquals((int) document.<Integer>getProperty("firstProp"), 4 - i / 2);

      int property4Index;
      if (i % 2 == 0) property4Index = document.<Integer>getProperty("firstProp") * 2;
      else property4Index = document.<Integer>getProperty("firstProp") * 2 + 1;

      Assert.assertEquals(document.getProperty("prop4"), "prop" + property4Index);
    }

    final OResultSet explainResult = database.command("explain " + query);

    Assert.assertEquals(
        explainResult.getExecutionPlan().get().getIndexes().toArray(),
        new String[] {"OrderByIndexReuseIndexFirstPropNotUnique"});
  }

  public void testLTEOrderByAscFirstPropertyAscFourthProperty() {
    final String query =
        "select from OrderByIndexReuse where firstProp <= 5 order by firstProp asc, prop4 asc limit"
            + " 3";
    List<OResult> result = database.query(query).stream().toList();

    Assert.assertEquals(result.size(), 3);
    for (int i = 0; i < 3; i++) {
      OResult document = result.get(i);
      Assert.assertEquals((int) document.<Integer>getProperty("firstProp"), i / 2 + 1);

      int property4Index;
      if (i % 2 == 0) property4Index = document.<Integer>getProperty("firstProp") * 2;
      else property4Index = document.<Integer>getProperty("firstProp") * 2 + 1;

      Assert.assertEquals(document.getProperty("prop4"), "prop" + property4Index);
    }

    final OResultSet explainResult = database.command("explain " + query);

    Assert.assertEquals(
        explainResult.getExecutionPlan().get().getIndexes().toArray(),
        new String[] {"OrderByIndexReuseIndexFirstPropNotUnique"});
  }

  public void testLTEOrderByDescFirstPropertyAscFourthProperty() {
    final String query =
        "select from OrderByIndexReuse where firstProp <= 5 order by firstProp desc, prop4 asc"
            + " limit 3";
    List<OResult> result = database.query(query).stream().toList();

    Assert.assertEquals(result.size(), 3);
    for (int i = 0; i < 3; i++) {
      OResult document = result.get(i);
      Assert.assertEquals((int) document.<Integer>getProperty("firstProp"), 5 - i / 2);

      int property4Index;
      if (i % 2 == 0) property4Index = document.<Integer>getProperty("firstProp") * 2;
      else property4Index = document.<Integer>getProperty("firstProp") * 2 + 1;

      Assert.assertEquals(document.getProperty("prop4"), "prop" + property4Index);
    }

    final OResultSet explainResult = database.command("explain " + query);

    Assert.assertEquals(
        explainResult.getExecutionPlan().get().getIndexes().toArray(),
        new String[] {"OrderByIndexReuseIndexFirstPropNotUnique"});
  }

  public void testBetweenOrderByAscFirstPropertyAscFourthProperty() {
    final String query =
        "select from OrderByIndexReuse where firstProp between 5 and 15 order by firstProp asc,"
            + " prop4 asc limit 5";
    List<OResult> result = database.query(query).stream().toList();

    Assert.assertEquals(result.size(), 5);
    for (int i = 0; i < 5; i++) {
      OResult document = result.get(i);
      Assert.assertEquals((int) document.<Integer>getProperty("firstProp"), i / 2 + 5);

      int property4Index;
      if (i % 2 == 0) property4Index = document.<Integer>getProperty("firstProp") * 2;
      else property4Index = document.<Integer>getProperty("firstProp") * 2 + 1;

      Assert.assertEquals(document.getProperty("prop4"), "prop" + property4Index);
    }

    final OResultSet explainResult = database.command("explain " + query);

    Assert.assertEquals(
        explainResult.getExecutionPlan().get().getIndexes().toArray(),
        new String[] {"OrderByIndexReuseIndexFirstPropNotUnique"});
  }

  public void testBetweenOrderByDescFirstPropertyAscFourthProperty() {
    final String query =
        "select from OrderByIndexReuse where firstProp between 5 and 15 order by firstProp desc,"
            + " prop4 asc limit 5";
    List<OResult> result = database.query(query).stream().toList();

    Assert.assertEquals(result.size(), 5);
    for (int i = 0; i < 5; i++) {
      OResult document = result.get(i);
      Assert.assertEquals((int) document.<Integer>getProperty("firstProp"), 15 - i / 2);

      int property4Index;
      if (i % 2 == 0) property4Index = document.<Integer>getProperty("firstProp") * 2;
      else property4Index = document.<Integer>getProperty("firstProp") * 2 + 1;

      Assert.assertEquals(document.getProperty("prop4"), "prop" + property4Index);
    }

    final OResultSet explainResult = database.command("explain " + query);

    Assert.assertEquals(
        explainResult.getExecutionPlan().get().getIndexes().toArray(),
        new String[] {"OrderByIndexReuseIndexFirstPropNotUnique"});
  }

  public void testInOrderByAscFirstPropertyAscFourthProperty() {
    final String query =
        "select from OrderByIndexReuse where firstProp in [10, 2, 43, 21, 45, 47, 11, 12] order by"
            + " firstProp asc, prop4 asc limit 3";
    List<OResult> result = database.query(query).stream().toList();

    Assert.assertEquals(result.size(), 3);

    OResult document = result.get(0);
    Assert.assertEquals((int) document.<Integer>getProperty("firstProp"), 2);
    Assert.assertEquals(document.<String>getProperty("prop4"), "prop4");

    document = result.get(1);
    Assert.assertEquals((int) document.<Integer>getProperty("firstProp"), 2);
    Assert.assertEquals(document.<String>getProperty("prop4"), "prop5");

    document = result.get(2);
    Assert.assertEquals((int) document.<Integer>getProperty("firstProp"), 10);
    Assert.assertEquals(document.<String>getProperty("prop4"), "prop20");

    final OResultSet explainResult = database.command("explain " + query);

    Assert.assertEquals(
        explainResult.getExecutionPlan().get().getIndexes().toArray(),
        new String[] {"OrderByIndexReuseIndexFirstPropNotUnique"});
  }

  public void testInOrderByDescFirstPropertyAscFourthProperty() {
    final String query =
        "select from OrderByIndexReuse where firstProp in [10, 2, 43, 21, 45, 47, 11, 12] order by"
            + " firstProp desc, prop4 asc limit 3";
    List<OResult> result = database.query(query).stream().toList();

    Assert.assertEquals(result.size(), 3);

    OResult document = result.get(0);
    Assert.assertEquals((int) document.<Integer>getProperty("firstProp"), 47);
    Assert.assertEquals(document.<String>getProperty("prop4"), "prop94");

    document = result.get(1);
    Assert.assertEquals((int) document.<Integer>getProperty("firstProp"), 47);
    Assert.assertEquals(document.<String>getProperty("prop4"), "prop95");

    document = result.get(2);
    Assert.assertEquals((int) document.<Integer>getProperty("firstProp"), 45);
    Assert.assertEquals(document.<String>getProperty("prop4"), "prop90");

    final OResultSet explainResult = database.command("explain " + query);

    Assert.assertEquals(
        explainResult.getExecutionPlan().get().getIndexes().toArray(),
        new String[] {"OrderByIndexReuseIndexFirstPropNotUnique"});
  }

  public void testOrderByFirstPropWithLimitAsc() {
    final String query = "select from OrderByIndexReuse order by firstProp offset 10 limit 4";

    List<OResult> result = database.query(query).stream().toList();

    Assert.assertEquals(result.size(), 4);

    for (int i = 0; i < 4; i++) {
      final OResult document = result.get(i);

      Assert.assertEquals(document.<Object>getProperty("firstProp"), 6 + i / 2);
    }

    final OResultSet explainResult = database.command("explain " + query);

    Assert.assertEquals(
        explainResult.getExecutionPlan().get().getIndexes().toArray(),
        new String[] {"OrderByIndexReuseIndexFirstPropNotUnique"});
  }

  public void testOrderByFirstPropWithLimitDesc() {
    final String query = "select from OrderByIndexReuse order by firstProp desc offset 10 limit 4";

    List<OResult> result = database.query(query).stream().toList();

    Assert.assertEquals(result.size(), 4);

    for (int i = 0; i < 4; i++) {
      final OResult document = result.get(i);

      Assert.assertEquals(document.<Object>getProperty("firstProp"), 45 - i / 2);
    }

    final OResultSet explainResult = database.command("explain " + query);

    Assert.assertEquals(
        explainResult.getExecutionPlan().get().getIndexes().toArray(),
        new String[] {"OrderByIndexReuseIndexFirstPropNotUnique"});
  }

  public void testOrderBySecondThirdPropWithLimitAsc() {
    final String query =
        "select from OrderByIndexReuse order by secondProp asc, thirdProp asc offset 10 limit 4";

    List<OResult> result = database.query(query).stream().toList();

    Assert.assertEquals(result.size(), 4);

    for (int i = 0; i < 4; i++) {
      final OResult document = result.get(i);

      Assert.assertEquals(document.<Object>getProperty("secondProp"), 6 + i / 2);

      int thirdPropertyIndex;
      if (i % 2 == 0) thirdPropertyIndex = document.<Integer>getProperty("secondProp") * 2;
      else thirdPropertyIndex = document.<Integer>getProperty("secondProp") * 2 + 1;

      Assert.assertEquals(document.getProperty("thirdProp"), "prop" + thirdPropertyIndex);
    }

    final OResultSet explainResult = database.command("explain " + query);

    Assert.assertEquals(
        explainResult.getExecutionPlan().get().getIndexes().toArray(),
        new String[] {"OrderByIndexReuseIndexSecondThirdProp"});
  }

  public void testOrderBySecondThirdPropWithLimitDesc() {
    final String query =
        "select from OrderByIndexReuse order by secondProp desc, thirdProp desc offset 10 limit 4";

    List<OResult> result = database.query(query).stream().toList();

    Assert.assertEquals(result.size(), 4);

    for (int i = 0; i < 4; i++) {
      final OResult document = result.get(i);

      Assert.assertEquals(document.<Object>getProperty("secondProp"), 45 - i / 2);

      int thirdPropertyIndex;
      if (i % 2 == 0) thirdPropertyIndex = document.<Integer>getProperty("secondProp") * 2 + 1;
      else thirdPropertyIndex = document.<Integer>getProperty("secondProp") * 2;

      Assert.assertEquals(document.getProperty("thirdProp"), "prop" + thirdPropertyIndex);
    }

    final OResultSet explainResult = database.command("explain " + query);

    Assert.assertEquals(
        explainResult.getExecutionPlan().get().getIndexes().toArray(),
        new String[] {"OrderByIndexReuseIndexSecondThirdProp"});
  }

  public void testOrderBySecondThirdPropWithLimitAscDesc() {
    final String query =
        "select from OrderByIndexReuse order by secondProp asc, thirdProp desc offset 10 limit 4";

    List<OResult> result = database.query(query).stream().toList();

    Assert.assertEquals(result.size(), 4);

    for (int i = 0; i < 4; i++) {
      final OResult document = result.get(i);

      Assert.assertEquals(document.<Object>getProperty("secondProp"), 6 + i / 2);

      int thirdPropertyIndex;
      if (i % 2 == 0) thirdPropertyIndex = document.<Integer>getProperty("secondProp") * 2 + 1;
      else thirdPropertyIndex = document.<Integer>getProperty("secondProp") * 2;

      Assert.assertEquals(document.getProperty("thirdProp"), "prop" + thirdPropertyIndex);
    }

    final OResultSet explainResult = database.command("explain " + query);

    Assert.assertTrue(explainResult.getExecutionPlan().get().getIndexes().isEmpty());
  }

  public void testOrderBySecondThirdPropWithLimitDescAsc() {
    final String query =
        "select from OrderByIndexReuse order by secondProp desc, thirdProp asc offset 10 limit 4";

    List<OResult> result = database.query(query).stream().toList();

    Assert.assertEquals(result.size(), 4);

    for (int i = 0; i < 4; i++) {
      final OResult document = result.get(i);

      Assert.assertEquals(document.<Object>getProperty("secondProp"), 45 - i / 2);

      int thirdPropertyIndex;
      if (i % 2 == 0) thirdPropertyIndex = document.<Integer>getProperty("secondProp") * 2;
      else thirdPropertyIndex = document.<Integer>getProperty("secondProp") * 2 + 1;

      Assert.assertEquals(document.getProperty("thirdProp"), "prop" + thirdPropertyIndex);
    }

    final OResultSet explainResult = database.command("explain " + query);

    Assert.assertTrue(explainResult.getExecutionPlan().get().getIndexes().isEmpty());
  }
}
