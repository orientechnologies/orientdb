package com.orientechnologies.orient.core.sql.parser;

import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

import static org.testng.Assert.fail;

@Test
public class OMatchStatementTest {

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
  public void testWrongFilterKey() {
    checkWrongSyntax("MATCH {clasx: 'V'} RETURN foo");
  }

  @Test
  public void testBasicMatch() {
    checkRightSyntax("MATCH {class: 'V', as: foo} RETURN foo");
  }

  @Test
  public void testNoReturn() {
    checkWrongSyntax("MATCH {class: 'V', as: foo}");
  }

  @Test
  public void testSingleMethod() {
    checkRightSyntax("MATCH {class: 'V', as: foo}.out() RETURN foo");
  }

  @Test
  public void testSingleMethodAndFilter() {
    checkRightSyntax("MATCH {class: 'V', as: foo}.out(){class: 'V', as: bar} RETURN foo");
  }

  @Test
  public void testLongPath() {
    checkRightSyntax("MATCH {class: 'V', as: foo}.out().in('foo').both('bar').out(){as: bar} RETURN foo");
  }

  @Test
  public void testLongPath2() {
    checkRightSyntax("MATCH {class: 'V', as: foo}.out().in('foo'){}.both('bar'){CLASS: 'bar'}.out(){as: bar} RETURN foo");
  }

  @Test
  public void testFilterTypes() {
    StringBuilder query = new StringBuilder();
    query.append("MATCH {");
    query.append("   class: 'v', ");
    query.append("   as: foo, ");
    query.append("   where: (name = 'foo' and surname = 'bar' or aaa in [1,2,3]), ");
    query.append("   maxDepth: 10 ");
    query.append("} return foo");
    System.out.println(query);
    checkRightSyntax(query.toString());
  }

  @Test
  public void testFilterTypes2() {
    StringBuilder query = new StringBuilder();
    query.append("MATCH {");
    query.append("   classes: ['V', 'E'], ");
    query.append("   as: foo, ");
    query.append("   where: (name = 'foo' and surname = 'bar' or aaa in [1,2,3]), ");
    query.append("   maxDepth: 10 ");
    query.append("} return foo");
    System.out.println(query);
    checkRightSyntax(query.toString());
  }

  @Test
  public void testMultiPath() {
    StringBuilder query = new StringBuilder();
    query.append("MATCH {}");
    query.append("  .(out().in(){class:'v'}.both('Foo')){maxDepth: 3}.out() return foo");
    System.out.println(query);
    checkRightSyntax(query.toString());
  }

  @Test
  public void testMultipleMatches() {
    String query = "MATCH {class: 'V', as: foo}.out(){class: 'V', as: bar}, ";
    query += " {class: 'V', as: foo}.out(){class: 'V', as: bar},";
    query += " {class: 'V', as: foo}.out(){class: 'V', as: bar} RETURN foo";
    checkRightSyntax(query);
  }


  @Test
  public void testWhile() {
    checkRightSyntax("MATCH {class: 'V', as: foo}.out(){while:($depth<4), as:bar} RETURN bar ");
  }

  @Test
  public void testLimit() {
    checkRightSyntax("MATCH {class: 'V'} RETURN foo limit 10");
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
