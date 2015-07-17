package com.orientechnologies.orient.core.sql.parser;

import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

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

  @Test
  public void testParserSimpleSelect1() {
    SimpleNode stm = checkRightSyntax("select from Foo");
    assertTrue(stm instanceof OSelectStatement);
    OSelectStatement select = (OSelectStatement) stm;
    assertTrue(select.getProjection() == null);
    assertTrue(select.getTarget() != null);
    assertTrue(!Boolean.TRUE.equals(select.getLockRecord()));
    assertTrue(select.getWhereClause() == null);
  }

  @Test
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

  @Test
  public void testSimpleSelect() {
    checkRightSyntax("select from Foo");
    checkRightSyntax("select * from Foo");

    checkWrongSyntax("select from Foo bar");
    checkWrongSyntax("select * from Foo bar");

    checkWrongSyntax("select * Foo");

  }

  @Test
  public void testUnwind() {
    checkRightSyntax("select from Foo unwind foo");
    checkRightSyntax("select from Foo unwind foo, bar");
    checkRightSyntax("select from Foo where foo = 1 unwind foo, bar");
    checkRightSyntax("select from Foo where foo = 1 order by foo unwind foo, bar");
    checkRightSyntax("select from Foo where foo = 1 group by bar order by foo unwind foo, bar");
  }

  @Test
  public void testSubSelect() {
    checkRightSyntax("select from (select from Foo)");

    checkWrongSyntax("select from select from foo");
  }

  @Test
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

  @Test
  public void testIn() {
    SimpleNode result = checkRightSyntax("select count(*) from OFunction where name in [\"a\"]");
    // result.dump("    ");
    assertTrue(result instanceof OSelectStatement);
    OSelectStatement select = (OSelectStatement) result;

  }

  @Test
  public void testNotIn() {
    SimpleNode result = checkRightSyntax("select count(*) from OFunction where name not in [\"a\"]");
    // result.dump("    ");
    assertTrue(result instanceof OStatement);
    OStatement stm = (OStatement) result;

  }

  @Test
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

  @Test
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

  @Test
  public void testFetchPlan1() {
    SimpleNode result = checkRightSyntax("" + "select 'Ay' as a , 'bEE' as b from Foo fetchplan *:1");

    assertTrue(result instanceof OSelectStatement);
    OSelectStatement select = (OSelectStatement) result;

  }

  @Test
  public void testFetchPlan2() {
    SimpleNode result = checkRightSyntax("" + "select 'Ay' as a , 'bEE' as b fetchplan *:1");

    assertTrue(result instanceof OSelectWithoutTargetStatement);
    OSelectWithoutTargetStatement select = (OSelectWithoutTargetStatement) result;

  }

  @Test
  public void testContainsWithCondition() {
    SimpleNode result = checkRightSyntax("select from Profile where customReferences.values() CONTAINS 'a'");

    assertTrue(result instanceof OSelectStatement);
    OSelectStatement select = (OSelectStatement) result;

  }

  @Test
  public void testNamedParam() {
    SimpleNode result = checkRightSyntax("select from JavaComplexTestClass where enumField = :enumItem");

    assertTrue(result instanceof OSelectStatement);
    OSelectStatement select = (OSelectStatement) result;

  }

  @Test
  public void testBoolean() {
    SimpleNode result = checkRightSyntax("select from Foo where bar = true");
    // result.dump("    ");
    assertTrue(result instanceof OSelectStatement);
    OSelectStatement select = (OSelectStatement) result;

  }

  @Test
  public void testDottedAtField() {
    SimpleNode result = checkRightSyntax("select from City where country.@class = 'Country'");
    // result.dump("    ");
    assertTrue(result instanceof OSelectStatement);
    OSelectStatement select = (OSelectStatement) result;

  }

  @Test
  public void testQuotedFieldNameFrom() {
    SimpleNode result = checkRightSyntax("select `from` from City where country.@class = 'Country'");
    assertTrue(result instanceof OSelectStatement);
    OSelectStatement select = (OSelectStatement) result;

  }

  @Test
  public void testQuotedTargetName() {
    checkRightSyntax("select from `edge`");
    checkRightSyntax("select from `from`");
    checkRightSyntax("select from `vertex`");
    checkRightSyntax("select from `select`");
  }

  @Test
  public void testQuotedFieldName() {
    checkRightSyntax("select `foo` from City where country.@class = 'Country'");

  }

  @Test
  public void testLongDotted() {
    SimpleNode result = checkRightSyntax("select from Profile where location.city.country.name = 'Spain'");
    // result.dump("    ");
    assertTrue(result instanceof OSelectStatement);
    OSelectStatement select = (OSelectStatement) result;

  }

  @Test
  public void testInIsNotAReservedWord() {
    SimpleNode result = checkRightSyntax("select count(*) from TRVertex where in.type() not in [\"LINKSET\"] ");
    // result.dump("    ");
    assertTrue(result instanceof OSelectStatement);
    OSelectStatement select = (OSelectStatement) result;

  }

  @Test
  public void testSelectFunction() {
    SimpleNode result = checkRightSyntax("select max(1,2,7,0,-2,3), 'pluto'");
    // result.dump("    ");
    assertTrue(result instanceof OSelectWithoutTargetStatement);
    OSelectWithoutTargetStatement select = (OSelectWithoutTargetStatement) result;

  }

  @Test
  public void testEscape1() {
    SimpleNode result = checkRightSyntax("select from cluster:internal where \"\\u005C\\u005C\" = \"\\u005C\\u005C\" ");
    assertTrue(result instanceof OSelectStatement);

  }

  @Test
  public void testWildcardSuffix() {
    checkRightSyntax("select foo.* from bar ");
  }

  @Test
  public void testEmptyCollection() {
    String query = "select from bar where name not in :param1";
    OrientSql osql = getParserFor(query);
    try {
      SimpleNode result = osql.parse();
      OSelectStatement stm = (OSelectStatement) result;
      Map<Object, Object> params = new HashMap<Object, Object>();
      params.put("param1", new HashSet<Object>());
      ((OSelectStatement) result).replaceParameters(params);
      assertEquals(stm.toString(), "SELECT FROM bar WHERE name NOT IN []");
    } catch (Exception e) {
      fail();

    }

  }

  @Test
  public void testEscape2() {
    try {
      SimpleNode result = checkWrongSyntax("select from cluster:internal where \"\\u005C\" = \"\\u005C\" ");
      fail();
    } catch (Error e) {

    }
  }

  @Test
  public void testSpatial() {

    checkRightSyntax("select *,$distance from Place where [latitude,longitude,$spatial] NEAR [41.893056,12.482778,{\"maxDistance\": 0.5}]");
    checkRightSyntax("select * from Place where [latitude,longitude] WITHIN [[51.507222,-0.1275],[55.507222,-0.1275]]");

  }

  @Test
  public void testSubConditions() {
    checkRightSyntax("SELECT @rid as rid, localName FROM Person WHERE ( 'milano' IN out('lives').localName OR 'roma' IN out('lives').localName ) ORDER BY age ASC");
  }

  @Test
  public void testRecordAttributes() {
    // issue #4430
    checkRightSyntax("SELECT @this, @rid, @rid_id, @rid_pos, @version, @class, @type, @size, @fields, @raw from V");
    checkRightSyntax("SELECT @THIS, @RID, @RID_ID, @RID_POS, @VERSION, @CLASS, @TYPE, @SIZE, @FIELDS, @RAW from V");
  }

  @Test
  public void testDoubleEquals() {
    // issue #4413
    checkRightSyntax("SELECT from V where name = 'foo'");
    checkRightSyntax("SELECT from V where name == 'foo'");
  }

  @Test
  public void testMatches() {

    checkRightSyntax("select from Person where name matches 'a'");

    checkRightSyntax("select from Person where name matches '(?i)(^\\\\Qname1\\\\E$)|(^\\\\Qname2\\\\E$)|(^\\\\Qname3\\\\E$)' and age=30");
  }

  @Test
  // issue #3718
  public void testComplexTarget1() {
    checkRightSyntax("SELECT $e FROM [#1:1,#1:2] LET $e = (SELECT FROM $current.prop1)");
    checkRightSyntax("SELECT $e FROM [#1:1,#1:2] let $e = (SELECT FROM (SELECT FROM $parent.$current))");
  }

  @Test
  public void testEval() {
    checkRightSyntax("  select  sum(weight) , f.name as name from (\n"
        + "      select weight, if(eval(\"out.name = 'one'\"),out,in) as f  from (\n" + "      select expand(bothE('E')) from V\n"
        + "  )\n" + "      ) group by name\n");

  }

  @Test
  public void testNewLine() {
    checkRightSyntax("INSERT INTO Country SET name=\"one\\ntwo\" RETURN @rid");

  }

  @Test
  public void testJsonWithUrl() {
    checkRightSyntax("insert into V content { \"url\": \"http://www.google.com\" } ");
  }

  @Test
  public void testGroupBy() {
    // issue #4245
    checkRightSyntax("select in.name from (  \n" + "select expand(outE()) from V\n" + ")\n" + "group by in.name");

  }

  @Test
  public void testInputParams() {

    checkRightSyntax("select from foo where name like  '%'+ :param1 + '%'");

    checkRightSyntax("select from foo where name like  'aaa'+ :param1 + 'a'");

  }

  @Test(enabled = false)
  public void testSlashInQuery() {
    checkRightSyntax("insert into test content {\"node_id\": \"MFmqvmht//sYYWB8=\"}");
    checkRightSyntax("insert into test content { \"node_id\": \"MFmqvmht\\/\\/GYsYYWB8=\"}");
  }

  @Test()
  public void testClusterList() {
    checkRightSyntax("select from cluster:[foo,bar]");
  }

  @Test
  public void checkOrderBySyntax() {
    checkRightSyntax("select from test order by something ");
    checkRightSyntax("select from test order by something, somethingElse ");
    checkRightSyntax("select from test order by something asc, somethingElse desc");
    checkRightSyntax("select from test order by something asc, somethingElse ");
    checkRightSyntax("select from test order by something, somethingElse asc");
    checkRightSyntax("select from test order by something asc");
    checkRightSyntax("select from test order by something desc");
    checkRightSyntax("select from test order by (something desc)");
    checkRightSyntax("select from test order by (something asc)");
    checkRightSyntax("select from test order by (something asc),somethingElse");
    checkRightSyntax("select from test order by (something),(somethingElse)");
    checkRightSyntax("select from test order by something,(somethingElse)");
    checkRightSyntax("select from test order by (something asc),(somethingElse desc)");
  }

  @Test()
  public void testMultipleLucene() {
    checkRightSyntax("select from Foo where a lucene 'a'");
    checkWrongSyntax("select from Foo where a lucene 'a' and b lucene 'a'");

    checkWrongSyntax(
        "select union($a, $b) let $a = (select from Foo where a lucene 'a' and b lucene 'b'), $b = (select from Foo where b lucene 'b')");
    checkRightSyntax("select union($a, $b) let $a = (select from Foo where a lucene 'a'), $b = (select from Foo where b lucene 'b')");
    checkWrongSyntax("select from (select from Foo) where a lucene 'a'");

    checkWrongSyntax(
        "select from Foo where (a=2 and b=3 and (a = 4 and (b=5 and d lucene 'foo')))) or select from Foo where (a=2 and b=3 and (a = 4 and (b=5 and d lucene 'foo'))))");

    checkWrongSyntax("select from cluster:foo where a lucene 'b'");
    checkWrongSyntax("select from index:foo where a lucene 'b'");
    checkWrongSyntax("select from #12:0 where a lucene 'b'");
    checkWrongSyntax("select from [#12:0, #12:1] where a lucene 'b'");

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
