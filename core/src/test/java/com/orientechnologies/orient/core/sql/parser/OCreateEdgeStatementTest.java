package com.orientechnologies.orient.core.sql.parser;

import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

import static org.testng.Assert.fail;

@Test
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

  public void testSimpleCreate() {
    checkRightSyntax("create edge Foo from (Select from a) to (Select from b)");
  }


  public void testCreateFromRid() {
    checkRightSyntax("create edge Foo from #11:0 to #11:1");

  }

  public void testCreateFromRidArray() {
    checkRightSyntax("create edge Foo from [#11:0, #11:3] to [#11:1, #12:0]");

  }

  public void testRetry() {
    checkRightSyntax("create edge Foo from [#11:0, #11:3] to [#11:1, #12:0] retry 3 wait 20");

  }

  public void testCreateFromRidSet() {
    checkRightSyntax("create edge Foo from #11:0 to #11:1 set foo='bar', bar=2");

  }

  public void testCreateFromRidArraySet() {
    checkRightSyntax("create edge Foo from [#11:0, #11:3] to [#11:1, #12:0] set foo='bar', bar=2");

  }

  public void testRetrySet() {
    checkRightSyntax("create edge Foo from [#11:0, #11:3] to [#11:1, #12:0] set foo='bar', bar=2 retry 3 wait 20");

  }

  public void testBatch() {
    checkRightSyntax("create edge Foo from [#11:0, #11:3] to [#11:1, #12:0] set foo='bar', bar=2 retry 3 wait 20 batch 10");

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
