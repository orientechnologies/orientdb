package com.orientechnologies.orient.core.sql.parser;

import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

import static org.testng.Assert.*;

@Test
public class OSelectStatementTest {

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
      System.out.println(query);
      System.out.println("->");
      System.out.println(result.toString());
      System.out.println("............");
      return result;
    } catch (Exception e) {
      if (isCorrect) {
        e.printStackTrace();
        fail();
      }
    }
    return null;
  }

  public void testParserSimpleSelect1() {
    SimpleNode stm = checkRightSyntax("select from Foo");
    assertTrue(stm instanceof OSelectStatement);
    OSelectStatement select = (OSelectStatement) stm;
    assertTrue(select.getProjection() == null);
    assertTrue(select.getTarget() != null);
    assertTrue(!Boolean.TRUE.equals(select.getLockRecord()));
    assertTrue(select.getWhereClause() == null);
  }

  public void testParserSimpleSelect2() {
    SimpleNode stm = checkRightSyntax("select bar from Foo");
    assertTrue(stm instanceof OSelectStatement);
    OSelectStatement select = (OSelectStatement) stm;
    assertTrue(select.getProjection() != null);
    assertTrue(select.getProjection().getItems() != null);
    assertEquals(select.getProjection().getItems().size(), 1);
    assertTrue(select.getTarget() != null);
    assertTrue(!Boolean.TRUE.equals(select.getLockRecord()));
    assertTrue(select.getWhereClause() == null);
  }

  public void testSimpleSelect() {
    checkRightSyntax("select from Foo");
    checkRightSyntax("select * from Foo");

    checkWrongSyntax("select from Foo bar");
    checkWrongSyntax("select * from Foo bar");

    checkWrongSyntax("select * Foo");

  }

  public void testSubSelect() {
    checkRightSyntax("select from (select from Foo)");

    checkWrongSyntax("select from select from foo");
  }

  public void testSimpleSelectWhere() {
    checkRightSyntax("select from Foo where name = 'foo'");
    checkRightSyntax("select * from Foo where name = 'foo'");

    checkRightSyntax("select from Foo where name = 'foo' and surname = \"bar\"");
    checkRightSyntax("select * from Foo where name = 'foo' and surname = \"bar\"");

    checkWrongSyntax("select * from Foo name = 'foo'");
    checkWrongSyntax("select from Foo bar where name = 'foo'");
    checkWrongSyntax("select * from Foo bar where name = 'foo'");
    checkWrongSyntax("select Foo where name = 'foo'");
    checkWrongSyntax("select * Foo where name = 'foo'");

  }

  public void testIn() {
    SimpleNode result = checkRightSyntax("select count(*) from OFunction where name in [\"a\"]");
    // result.dump("    ");
    assertTrue(result instanceof OSelectStatement);
    OSelectStatement select = (OSelectStatement) result;

  }

  public void testNotIn() {
    SimpleNode result = checkRightSyntax("select count(*) from OFunction where name not in [\"a\"]");
    // result.dump("    ");
    assertTrue(result instanceof OStatement);
    OStatement stm = (OStatement) result;

  }

  public void testMath1() {
    SimpleNode result = checkRightSyntax("" + "select * from sqlSelectIndexReuseTestClass where prop1 = 1 + 1");
    // result.dump("    ");
    assertTrue(result instanceof OSelectStatement);
    OSelectStatement select = (OSelectStatement) result;

  }

  public void testMath2() {
    SimpleNode result = checkRightSyntax("" + "select * from sqlSelectIndexReuseTestClass where prop1 = foo + 1");
    // result.dump("    ");
    assertTrue(result instanceof OSelectStatement);
    OSelectStatement select = (OSelectStatement) result;

  }

  public void testIndex1() {
    SimpleNode result = checkRightSyntax("select from index:collateCompositeIndexCS where key = ['VAL', 'VaL']");
    // result.dump("    ");
    assertTrue(result instanceof OSelectStatement);
    OSelectStatement select = (OSelectStatement) result;

  }

  public void testMath5() {
    SimpleNode result = checkRightSyntax("" + "select * from sqlSelectIndexReuseTestClass where prop1 = foo + 1 * bar - 5");

    assertTrue(result instanceof OSelectStatement);
    OSelectStatement select = (OSelectStatement) result;

  }

  public void testFetchPlan1() {
    SimpleNode result = checkRightSyntax("" + "select 'Ay' as a , 'bEE' as b from Foo fetchplan *:1");

    assertTrue(result instanceof OSelectStatement);
    OSelectStatement select = (OSelectStatement) result;

  }

  public void testFetchPlan2() {
    SimpleNode result = checkRightSyntax("" + "select 'Ay' as a , 'bEE' as b fetchplan *:1");

    assertTrue(result instanceof OSelectWithoutTargetStatement);
    OSelectWithoutTargetStatement select = (OSelectWithoutTargetStatement) result;

  }

  public void testContainsWithCondition() {
    SimpleNode result = checkRightSyntax("select from Profile where customReferences.values() CONTAINS 'a'");

    assertTrue(result instanceof OSelectStatement);
    OSelectStatement select = (OSelectStatement) result;

  }

  public void testNamedParam() {
    SimpleNode result = checkRightSyntax("select from JavaComplexTestClass where enumField = :enumItem");

    assertTrue(result instanceof OSelectStatement);
    OSelectStatement select = (OSelectStatement) result;

  }

  public void testBoolean() {
    SimpleNode result = checkRightSyntax("select from Foo where bar = true");
    // result.dump("    ");
    assertTrue(result instanceof OSelectStatement);
    OSelectStatement select = (OSelectStatement) result;

  }

  public void testDottedAtField() {
    SimpleNode result = checkRightSyntax("select from City where country.@class = 'Country'");
    // result.dump("    ");
    assertTrue(result instanceof OSelectStatement);
    OSelectStatement select = (OSelectStatement) result;

  }

  public void testLongDotted() {
    SimpleNode result = checkRightSyntax("select from Profile where location.city.country.name = 'Spain'");
    // result.dump("    ");
    assertTrue(result instanceof OSelectStatement);
    OSelectStatement select = (OSelectStatement) result;

  }

  public void testInIsNotAReservedWord() {
    SimpleNode result = checkRightSyntax("select count(*) from TRVertex where in.type() not in [\"LINKSET\"] ");
    // result.dump("    ");
    assertTrue(result instanceof OSelectStatement);
    OSelectStatement select = (OSelectStatement) result;

  }

  public void testSelectFunction() {
    SimpleNode result = checkRightSyntax("select max(1,2,7,0,-2,3), 'pluto'");
    // result.dump("    ");
    assertTrue(result instanceof OSelectWithoutTargetStatement);
    OSelectWithoutTargetStatement select = (OSelectWithoutTargetStatement) result;

  }

  public void testEscape1() {
    SimpleNode result = checkRightSyntax("select from cluster:internal where \"\\u005C\\u005C\" = \"\\u005C\\u005C\" ");
    assertTrue(result instanceof OSelectStatement);

  }

  public void testEscape2() {
    try {
      SimpleNode result = checkWrongSyntax("select from cluster:internal where \"\\u005C\" = \"\\u005C\" ");
      fail();
    } catch (Error e) {

    }
  }

  public void testSubConditions() {
    checkRightSyntax("SELECT @rid as rid, localName FROM Person WHERE ( 'milano' IN out('lives').localName OR 'roma' IN out('lives').localName ) ORDER BY age ASC");
  }

  // issue #3718
  public void testComplexTarget1() {
    checkRightSyntax("SELECT $e FROM [#1:1,#1:2] LET $e = (SELECT FROM $current.prop1)");
    checkRightSyntax("SELECT $e FROM [#1:1,#1:2] let $e = (SELECT FROM (SELECT FROM $parent.$current))");
  }

  @Test(enabled = false)
  public void testSlashInQuery() {
    checkRightSyntax("insert into test content {\"node_id\": \"MFmqvmht//sYYWB8=\"}");
    checkRightSyntax("insert into test content { \"node_id\": \"MFmqvmht\\/\\/GYsYYWB8=\"}");

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
