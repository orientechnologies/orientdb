package com.orientechnologies.orient.core.sql.parser;

import static org.testng.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

import org.testng.annotations.Test;

@Test
public class OUpdateStatementTest {

  protected SimpleNode checkRightSyntax(String query) {
    return checkSyntax(query, true);
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

  public void testSimpleInsert() {
    checkRightSyntax("update  Foo set a = b");
    checkRightSyntax("update  Foo set a = 'b'");
    checkRightSyntax("update  Foo set a = 1");
    checkRightSyntax("update  Foo set a = 1+1");
    checkRightSyntax("update  Foo set a = a.b.toLowerCase()");

    checkRightSyntax("update  Foo set a = b, b=c");
    checkRightSyntax("update  Foo set a = 'b', b=1");
    checkRightSyntax("update  Foo set a = 1, c=k");
    checkRightSyntax("update  Foo set a = 1+1, c=foo, d='bar'");
    checkRightSyntax("update  Foo set a = a.b.toLowerCase(), b=out('pippo')[0]");
    printTree("update  Foo set a = a.b.toLowerCase(), b=out('pippo')[0]");
  }

  public void testIncrementOld() {
    checkRightSyntax("update  Foo increment a = 2");
  }

  public void testIncrement() {
    checkRightSyntax("update  Foo set a += 2");
    printTree("update  Foo set a += 2");
  }

  public void testDecrement() {
    checkRightSyntax("update  Foo set a -= 2");
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
