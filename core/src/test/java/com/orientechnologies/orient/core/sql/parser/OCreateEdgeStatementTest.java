package com.orientechnologies.orient.core.sql.parser;

import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import org.junit.Test;

public class OCreateEdgeStatementTest {

  protected SimpleNode checkRightSyntax(String query) {
    SimpleNode result = checkSyntax(query, true);
    return checkSyntax(result.toString(), true);
  }

  protected SimpleNode checkWrongSyntax(String query) {
    return checkSyntax(query, false);
  }

  protected SimpleNode checkSyntax(String query, boolean isCorrect) {
    OrientSql osql = getParserFor(query);
    try {
      SimpleNode result = osql.parse();
      if (!isCorrect) {
        fail();
      }
      return result;
    } catch (Exception e) {
      if (isCorrect) {
        e.printStackTrace();
        fail();
      }
    }
    return null;
  }

  @Test
  public void testSimpleCreate() {
    checkRightSyntax("create edge Foo from (Select from a) to (Select from b)");
  }

  @Test
  public void testCreateFromRid() {
    checkRightSyntax("create edge Foo from #11:0 to #11:1");
  }

  @Test
  public void testCreateFromRidArray() {
    checkRightSyntax("create edge Foo from [#11:0, #11:3] to [#11:1, #12:0]");
  }

  @Test
  public void testRetry() {
    checkRightSyntax("create edge Foo from [#11:0, #11:3] to [#11:1, #12:0] retry 3 wait 20");
  }

  @Test
  public void testCreateFromRidSet() {
    checkRightSyntax("create edge Foo from #11:0 to #11:1 set foo='bar', bar=2");
  }

  @Test
  public void testCreateFromRidArraySet() {
    checkRightSyntax("create edge Foo from [#11:0, #11:3] to [#11:1, #12:0] set foo='bar', bar=2");
  }

  @Test
  public void testRetrySet() {
    checkRightSyntax(
        "create edge Foo from [#11:0, #11:3] to [#11:1, #12:0] set foo='bar', bar=2 retry 3 wait 20");
  }

  @Test
  public void testBatch() {
    checkRightSyntax(
        "create edge Foo from [#11:0, #11:3] to [#11:1, #12:0] set foo='bar', bar=2 retry 3 wait 20 batch 10");
  }

  public void testInputVariables() {
    checkRightSyntax("create edge Foo from ? to ?");
    checkRightSyntax("create edge Foo from :a to :b");
    checkRightSyntax("create edge Foo from [:a, :b] to [:b, :c]");
  }

  public void testSubStatements() {
    checkRightSyntax("create edge Foo from (select from Foo) to (select from bar)");
    checkRightSyntax("create edge Foo from (traverse out() from #12:0) to (select from bar)");
    checkRightSyntax(
        "create edge Foo from (MATCH {class:Person, as:A} return $elements) to (select from bar)");
  }

  private void printTree(String s) {
    OrientSql osql = getParserFor(s);
    try {
      SimpleNode n = osql.parse();
    } catch (ParseException e) {
      e.printStackTrace();
    }
  }

  protected OrientSql getParserFor(String string) {
    InputStream is = new ByteArrayInputStream(string.getBytes());
    OrientSql osql = new OrientSql(is);
    return osql;
  }
}
