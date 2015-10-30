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
  public void testComments() {
    checkRightSyntax("select from Foo");

    checkRightSyntax("select /* aaa bbb ccc*/from Foo");
    checkRightSyntax("select /* aaa bbb \nccc*/from Foo");
    checkRightSyntax("select /** aaa bbb ccc**/from Foo");
    checkRightSyntax("select /** aaa bbb ccc*/from Foo");

    checkRightSyntax("/* aaa bbb ccc*/select from Foo");
    checkRightSyntax("select from Foo/* aaa bbb ccc*/");
    checkRightSyntax("/* aaa bbb ccc*/select from Foo/* aaa bbb ccc*/");

    checkWrongSyntax("select /** aaa bbb */ccc*/from Foo");

    checkWrongSyntax("select /**  /*aaa bbb */ccc*/from Foo");
    checkWrongSyntax("*/ select from Foo");
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
    assertTrue(result instanceof OSelectStatement);
    OSelectStatement select = (OSelectStatement) result;

  }

  @Test
  public void testIndex2() {
    SimpleNode result = checkRightSyntax("select from index:collateCompositeIndexCS where key between ['VAL', 'VaL'] and ['zz', 'zz']");
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

    checkWrongSyntax("select union($a, $b) let $a = (select from Foo where a lucene 'a' and b lucene 'b'), $b = (select from Foo where b lucene 'b')");
    checkRightSyntax("select union($a, $b) let $a = (select from Foo where a lucene 'a'), $b = (select from Foo where b lucene 'b')");
    checkWrongSyntax("select from (select from Foo) where a lucene 'a'");

    checkWrongSyntax("select from Foo where (a=2 and b=3 and (a = 4 and (b=5 and d lucene 'foo')))) or select from Foo where (a=2 and b=3 and (a = 4 and (b=5 and d lucene 'foo'))))");

    checkWrongSyntax("select from cluster:foo where a lucene 'b'");
    checkRightSyntax("select from index:foo where a lucene 'b'");
    checkWrongSyntax("select from #12:0 where a lucene 'b'");
    checkWrongSyntax("select from [#12:0, #12:1] where a lucene 'b'");

  }

  public void testBacktick() {
    checkRightSyntax("select `foo` from foo where `foo` = 'bar'");
    checkRightSyntax("select `SELECT` from foo where `SELECT` = 'bar'");
    checkRightSyntax("select `TRAVERSE` from foo where `TRAVERSE` = 'bar'");
    checkRightSyntax("select `INSERT` from foo where `INSERT` = 'bar'");
    checkRightSyntax("select `CREATE` from foo where `CREATE` = 'bar'");
    checkRightSyntax("select `DELETE` from foo where `DELETE` = 'bar'");
    checkRightSyntax("select `VERTEX` from foo where `VERTEX` = 'bar'");
    checkRightSyntax("select `EDGE` from foo where `EDGE` = 'bar'");
    checkRightSyntax("select `UPDATE` from foo where `UPDATE` = 'bar'");
    checkRightSyntax("select `UPSERT` from foo where `UPSERT` = 'bar'");
    checkRightSyntax("select `FROM` from foo where `FROM` = 'bar'");
    checkRightSyntax("select `TO` from foo where `TO` = 'bar'");
    checkRightSyntax("select `WHERE` from foo where `WHERE` = 'bar'");
    checkRightSyntax("select `WHILE` from foo where `WHILE` = 'bar'");
    checkRightSyntax("select `INTO` from foo where `INTO` = 'bar'");
    checkRightSyntax("select `VALUES` from foo where `VALUES` = 'bar'");
    checkRightSyntax("select `SET` from foo where `SET` = 'bar'");
    checkRightSyntax("select `ADD` from foo where `ADD` = 'bar'");
    checkRightSyntax("select `PUT` from foo where `PUT` = 'bar'");
    checkRightSyntax("select `MERGE` from foo where `MERGE` = 'bar'");
    checkRightSyntax("select `CONTENT` from foo where `CONTENT` = 'bar'");
    checkRightSyntax("select `REMOVE` from foo where `REMOVE` = 'bar'");
    checkRightSyntax("select `INCREMENT` from foo where `INCREMENT` = 'bar'");
    checkRightSyntax("select `AND` from foo where `AND` = 'bar'");
    checkRightSyntax("select `OR` from foo where `OR` = 'bar'");
    checkRightSyntax("select `NULL` from foo where `NULL` = 'bar'");
    checkRightSyntax("select `DEFINED` from foo where `DEFINED` = 'bar'");
    checkRightSyntax("select `ORDER` from foo where `ORDER` = 'bar'");
    checkRightSyntax("select `GROUP` from foo where `GROUP` = 'bar'");
    checkRightSyntax("select `BY` from foo where `BY` = 'bar'");
    checkRightSyntax("select `LIMIT` from foo where `LIMIT` = 'bar'");
    checkRightSyntax("select `SKIP2` from foo where `SKIP2` = 'bar'");
    checkRightSyntax("select `OFFSET` from foo where `OFFSET` = 'bar'");
    checkRightSyntax("select `TIMEOUT` from foo where `TIMEOUT` = 'bar'");
    checkRightSyntax("select `ASC` from foo where `ASC` = 'bar'");
    checkRightSyntax("select `AS` from foo where `AS` = 'bar'");
    checkRightSyntax("select `DESC` from foo where `DESC` = 'bar'");
    checkRightSyntax("select `FETCHPLAN` from foo where `FETCHPLAN` = 'bar'");
    checkRightSyntax("select `RETURN` from foo where `RETURN` = 'bar'");
    checkRightSyntax("select `BEFORE` from foo where `BEFORE` = 'bar'");
    checkRightSyntax("select `AFTER` from foo where `AFTER` = 'bar'");
    checkRightSyntax("select `LOCK` from foo where `LOCK` = 'bar'");
    checkRightSyntax("select `RECORD` from foo where `RECORD` = 'bar'");
    checkRightSyntax("select `WAIT` from foo where `WAIT` = 'bar'");

    checkRightSyntax("select `identifier` from foo where `identifier` = 'bar'");
    checkRightSyntax("select `select` from foo where `select` = 'bar'");
    checkRightSyntax("select `traverse` from foo where `traverse` = 'bar'");
    checkRightSyntax("select `insert` from foo where `insert` = 'bar'");
    checkRightSyntax("select `create` from foo where `create` = 'bar'");
    checkRightSyntax("select `delete` from foo where `delete` = 'bar'");
    checkRightSyntax("select `vertex` from foo where `vertex` = 'bar'");
    checkRightSyntax("select `edge` from foo where `edge` = 'bar'");
    checkRightSyntax("select `update` from foo where `update` = 'bar'");
    checkRightSyntax("select `upsert` from foo where `upsert` = 'bar'");
    checkRightSyntax("select `from` from foo where `from` = 'bar'");
    checkRightSyntax("select `to` from foo where `to` = 'bar'");
    checkRightSyntax("select `where` from foo where `where` = 'bar'");
    checkRightSyntax("select `while` from foo where `while` = 'bar'");
    checkRightSyntax("select `into` from foo where `into` = 'bar'");
    checkRightSyntax("select `values` from foo where `values` = 'bar'");
    checkRightSyntax("select `set` from foo where `set` = 'bar'");
    checkRightSyntax("select `add` from foo where `add` = 'bar'");
    checkRightSyntax("select `put` from foo where `put` = 'bar'");
    checkRightSyntax("select `merge` from foo where `merge` = 'bar'");
    checkRightSyntax("select `content` from foo where `content` = 'bar'");
    checkRightSyntax("select `remove` from foo where `remove` = 'bar'");
    checkRightSyntax("select `increment` from foo where `increment` = 'bar'");
    checkRightSyntax("select `and` from foo where `and` = 'bar'");
    checkRightSyntax("select `or` from foo where `or` = 'bar'");
    checkRightSyntax("select `null` from foo where `null` = 'bar'");
    checkRightSyntax("select `defined` from foo where `defined` = 'bar'");
    checkRightSyntax("select `order` from foo where `order` = 'bar'");
    checkRightSyntax("select `group` from foo where `group` = 'bar'");
    checkRightSyntax("select `by` from foo where `by` = 'bar'");
    checkRightSyntax("select `limit` from foo where `limit` = 'bar'");
    checkRightSyntax("select `skip2` from foo where `skip2` = 'bar'");
    checkRightSyntax("select `offset` from foo where `offset` = 'bar'");
    checkRightSyntax("select `timeout` from foo where `timeout` = 'bar'");
    checkRightSyntax("select `asc` from foo where `asc` = 'bar'");
    checkRightSyntax("select `as` from foo where `as` = 'bar'");
    checkRightSyntax("select `desc` from foo where `desc` = 'bar'");
    checkRightSyntax("select `fetchplan` from foo where `fetchplan` = 'bar'");
    checkRightSyntax("select `return` from foo where `return` = 'bar'");
    checkRightSyntax("select `before` from foo where `before` = 'bar'");
    checkRightSyntax("select `after` from foo where `after` = 'bar'");
    checkRightSyntax("select `lock` from foo where `lock` = 'bar'");
    checkRightSyntax("select `record` from foo where `record` = 'bar'");
    checkRightSyntax("select `wait` from foo where `wait` = 'bar'");
    checkRightSyntax("select `retry` from foo where `retry` = 'bar'");
    checkRightSyntax("select `let` from foo where `let` = 'bar'");
    checkRightSyntax("select `nocache` from foo where `nocache` = 'bar'");
    checkRightSyntax("select `unsafe` from foo where `unsafe` = 'bar'");
    checkRightSyntax("select `parallel` from foo where `parallel` = 'bar'");
    checkRightSyntax("select `strategy` from foo where `strategy` = 'bar'");
    checkRightSyntax("select `depth_first` from foo where `depth_first` = 'bar'");
    checkRightSyntax("select `breadth_first` from foo where `breadth_first` = 'bar'");
    checkRightSyntax("select `lucene` from foo where `lucene` = 'bar'");
    checkRightSyntax("select `near` from foo where `near` = 'bar'");
    checkRightSyntax("select `within` from foo where `within` = 'bar'");
    checkRightSyntax("select `unwind` from foo where `unwind` = 'bar'");
    checkRightSyntax("select `maxdepth` from foo where `maxdepth` = 'bar'");
    checkRightSyntax("select `not` from foo where `not` = 'bar'");
    checkRightSyntax("select `in` from foo where `in` = 'bar'");
    checkRightSyntax("select `like` from foo where `like` = 'bar'");
    checkRightSyntax("select `is` from foo where `is` = 'bar'");
    checkRightSyntax("select `between` from foo where `between` = 'bar'");
    checkRightSyntax("select `contains` from foo where `contains` = 'bar'");
    checkRightSyntax("select `containsall` from foo where `containsall` = 'bar'");
    checkRightSyntax("select `containskey` from foo where `containskey` = 'bar'");
    checkRightSyntax("select `containsvalue` from foo where `containsvalue` = 'bar'");
    checkRightSyntax("select `containstext` from foo where `containstext` = 'bar'");
    checkRightSyntax("select `matches` from foo where `matches` = 'bar'");
    checkRightSyntax("select `key` from foo where `key` = 'bar'");
    checkRightSyntax("select `instanceof` from foo where `instanceof` = 'bar'");
    checkRightSyntax("select `cluster` from foo where `cluster` = 'bar'");

    checkRightSyntax("select `foo-bar` from foo where `cluster` = 'bar'");

    checkWrongSyntax("select `cluster from foo where `cluster` = 'bar'");
    checkWrongSyntax("select `cluster from foo where cluster` = 'bar'");

  }

  @Test
  public void testReturn() {
    checkRightSyntax("select from ouser timeout 1 exception");
    checkRightSyntax("select from ouser timeout 1 return");

  }


  @Test
  public void testDefined() {
    checkRightSyntax("select from foo where bar is defined");
    checkRightSyntax("select from foo where bar is not defined");

  }

  @Test
  public void testRecordAttributeAsAlias() {
    checkRightSyntax("select @rid as @rid from foo ");
  }

  @Test
  public void testParamWithMatches() {
    //issue #5229
    checkRightSyntax("select from Person where name matches :param1");
  }

  @Test
  public void testInstanceOfE(){
    //issue #5212
    checkRightSyntax("select from Friend where @class instanceof 'E'");
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
