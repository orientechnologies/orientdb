package com.orientechnologies.orient.core.sql.parser;

import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

import static org.testng.Assert.fail;

@Test
public class OCreateVertexStatementTest {

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

  public void testSimpleCreate() {
    checkRightSyntax("create vertex");
    checkRightSyntax("create vertex V");
    checkRightSyntax("create vertex x cluster t");
    checkWrongSyntax("create vertex V foo");
    checkRightSyntax("create vertex Foo (a) values (1)");
    checkRightSyntax("create vertex Foo (a) values ('1')");
    checkRightSyntax("create vertex Foo (a) values (\"1\")");

    checkRightSyntax("create vertex Foo (a,b) values (1, 2)");
    checkRightSyntax("create vertex Foo (a,b) values ('1', '2')");
    checkRightSyntax("create vertex (a,b) values (\"1\", \"2\")");

    printTree("create vertex (a,b) values (\"1\", \"2\")");
  }

  public void testSimpleCreateSet() {
    checkRightSyntax("create vertex Foo set a = 1");
    checkRightSyntax("create vertex Foo set a = '1'");
    checkRightSyntax("create vertex Foo set a = \"1\"");

    checkRightSyntax("create vertex Foo set a = 1, b = 2");

  }


  public void testEmptyArrayCreate() {
    checkRightSyntax("create vertex Foo set a = 'foo'");
    checkRightSyntax("create vertex Foo set a = []");
//    checkRightSyntax("create vertex Foo set a = [ ]");
  }


  public void testEmptyMapCreate() {
    checkRightSyntax("create vertex Foo set a = {}");
    checkRightSyntax("create vertex Foo SET a = { }");
  }


  public void testInsertIntoCluster() {
    checkRightSyntax("create vertex cluster:default (equaledges, name, list) values ('yes', 'square', ['bottom', 'top','left','right'] )");

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
