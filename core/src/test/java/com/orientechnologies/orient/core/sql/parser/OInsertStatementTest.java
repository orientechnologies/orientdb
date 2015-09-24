package com.orientechnologies.orient.core.sql.parser;

import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

import static org.testng.Assert.fail;

@Test
public class OInsertStatementTest {

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

  public void testSimpleInsert() {
    checkRightSyntax("insert into Foo (a) values (1)");
    checkRightSyntax("insert into Foo (a) values ('1')");
    checkRightSyntax("insert into Foo (a) values (\"1\")");

    checkRightSyntax("insert into Foo (a,b) values (1, 2)");
    checkRightSyntax("insert into Foo (a,b) values ('1', '2')");
    checkRightSyntax("insert into Foo (a,b) values (\"1\", \"2\")");

  }


  public void testInsertIntoCluster() {
    checkRightSyntax("insert into cluster:default (equaledges, name, list) values ('yes', 'square', ['bottom', 'top','left','right'] )");
    checkRightSyntax("insert into CLUSTER:default (equaledges, name, list) values ('yes', 'square', ['bottom', 'top','left','right'] )");

    checkRightSyntax("insert into Foo cluster foo1 (equaledges, name, list) values ('yes', 'square', ['bottom', 'top','left','right'] )");
    checkRightSyntax("insert into Foo CLUSTER foo1 (equaledges, name, list) values ('yes', 'square', ['bottom', 'top','left','right'] )");

  }

  public void testInsertSelectTimeout() {
    checkRightSyntax("insert into foo return foo select from bar TIMEOUT 10 ");
    checkRightSyntax("insert into foo return foo select from bar TIMEOUT 10 return");
    checkRightSyntax("insert into foo return foo select from bar TIMEOUT 10 exception");
  }

  public void testInsertInsert() {
    checkRightSyntax("insert into foo set bar = (insert into foo set a = 'foo') ");
//    checkRightSyntax("insert into foo set bar = (select from foo) ");
  }

    public void testInsertEmbeddedDocs() {
    checkRightSyntax("INSERT INTO Activity SET user = #14:1, story = #18:2, `like` = { \n"
        + "      count: 0, \n"
        + "      latest: [], \n"
        + "      '@type': 'document', \n"
        + "      '@class': 'Like'\n"
        + "    }");

    checkRightSyntax("INSERT INTO Activity SET user = #14:1, story = #18:2, `like` = { \n"
        + "      count: 0, \n"
        + "      latest: [], \n"
        + "      '@type': 'document', \n"
        + "      '@class': 'Like'\n"
        + "    }");
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
