package com.orientechnologies.orient.core.sql.parser;

import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import org.junit.Test;

public class OUpdateEdgeStatementTest {

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
  public void testSimpleInsert() {
    checkRightSyntax("update edge Foo set a = b");
    checkRightSyntax("update edge Foo set a = 'b'");
    checkRightSyntax("update edge Foo set a = 1");
    checkRightSyntax("update edge Foo set a = 1+1");
    checkRightSyntax("update edge Foo set a = a.b.toLowerCase()");

    checkRightSyntax("update edge Foo set a = b, b=c");
    checkRightSyntax("update edge Foo set a = 'b', b=1");
    checkRightSyntax("update edge Foo set a = 1, c=k");
    checkRightSyntax("update edge Foo set a = 1+1, c=foo, d='bar'");
    checkRightSyntax("update edge Foo set a = a.b.toLowerCase(), b=out('pippo')[0]");
    printTree("update edge Foo set a = a.b.toLowerCase(), b=out('pippo')[0]");
    checkRightSyntax("UPDATE EDGE E1 SET out = #10:0, in = #21:0 WHERE @rid = #24:0");
  }

  @Test
  public void testCollections() {
    checkRightSyntax("update edge Foo add a = b");
    checkWrongSyntax("update edge Foo add 'a' = b");
    checkRightSyntax("update edge Foo add a = 'a'");
    checkWrongSyntax("update edge Foo put a = b");
    checkRightSyntax("update edge Foo put a = b, c");
    checkRightSyntax("update edge Foo put a = 'b', 1.34");
    checkRightSyntax("update edge Foo put a = 'b', 'c'");
  }

  @Test
  public void testJson() {
    checkRightSyntax("update edge Foo merge {'a':'b', 'c':{'d':'e'}} where name = 'foo'");
    checkRightSyntax(
        "update edge Foo content {'a':'b', 'c':{'d':'e', 'f': ['a', 'b', 4]}} where name = 'foo'");
  }

  @Test
  public void testIncrementOld() {
    checkRightSyntax("update edge Foo increment a = 2");
  }

  @Test
  public void testIncrement() {
    checkRightSyntax("update edge Foo set a += 2");
    printTree("update edge Foo set a += 2");
  }

  @Test
  public void testDecrement() {
    checkRightSyntax("update edge Foo set a -= 2");
  }

  @Test
  public void testQuotedJson() {
    checkRightSyntax(
        "update edge E SET key = \"test\", value = {\"f12\":\"test\\\\\"} UPSERT WHERE key = \"test\"");
  }

  @Test
  public void testTargetQuery() {
    // issue #4415
    checkRightSyntax(
        "update edge (select from (traverse References from ( select from Node WHERE Email = 'julia@local'  ) ) WHERE @class = 'Node' and $depth <= 1 and Active = true ) set Points = 0 RETURN BEFORE $current.Points");
  }

  @Test
  public void testTargetMultipleRids() {
    checkRightSyntax("update EDGE [#9:0, #9:1] set foo = 'bar'");
  }

  private void printTree(String s) {
    OrientSql osql = getParserFor(s);
    try {
      SimpleNode result = osql.parse();

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
