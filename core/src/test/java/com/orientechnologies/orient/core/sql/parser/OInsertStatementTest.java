package com.orientechnologies.orient.core.sql.parser;

import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import org.junit.Test;

public class OInsertStatementTest {

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

  @Test
  public void testSimpleInsert() {
    checkRightSyntax("insert into Foo (a) values (1)");
    checkRightSyntax("insert into Foo (a) values ('1')");
    checkRightSyntax("insert into Foo (a) values (\"1\")");

    checkRightSyntax("insert into Foo (a,b) values (1, 2)");
    checkRightSyntax("insert into Foo (a,b) values ('1', '2')");
    checkRightSyntax("insert into Foo (a,b) values (\"1\", \"2\")");
  }

  @Test
  public void testInsertIntoCluster() {
    checkRightSyntax(
        "insert into cluster:default (equaledges, name, list) values ('yes', 'square', ['bottom', 'top','left','right'] )");
    checkRightSyntax(
        "insert into CLUSTER:default (equaledges, name, list) values ('yes', 'square', ['bottom', 'top','left','right'] )");

    checkRightSyntax(
        "insert into Foo cluster foo1 (equaledges, name, list) values ('yes', 'square', ['bottom', 'top','left','right'] )");
    checkRightSyntax(
        "insert into Foo CLUSTER foo1 (equaledges, name, list) values ('yes', 'square', ['bottom', 'top','left','right'] )");
  }

  @Test
  public void testInsertSelectTimeout() {
    checkRightSyntax("insert into foo return foo select from bar TIMEOUT 10 ");
    checkRightSyntax("insert into foo return foo select from bar TIMEOUT 10 return");
    checkRightSyntax("insert into foo return foo select from bar TIMEOUT 10 exception");
  }

  @Test
  public void testInsertInsert() {
    checkRightSyntax("insert into foo set bar = (insert into foo set a = 'foo') ");
  }

  @Test
  public void testInsertEmbeddedDocs() {
    checkRightSyntax(
        "INSERT INTO Activity SET user = #14:1, story = #18:2, `like` = { \n"
            + "      count: 0, \n"
            + "      latest: [], \n"
            + "      '@type': 'document', \n"
            + "      '@class': 'Like'\n"
            + "    }");

    checkRightSyntax(
        "INSERT INTO Activity SET user = #14:1, story = #18:2, `like` = { \n"
            + "      count: 0, \n"
            + "      latest: [], \n"
            + "      '@type': 'document', \n"
            + "      '@class': 'Like'\n"
            + "    }");
  }

  @Test
  public void testJsonEscaping() {
    // issue #5911
    checkRightSyntax(
        "insert into Bookmark content {\"data\""
            + ":\"DPl62xXzEqG3tIPv7jYYWK34IG4bwTUNk0UUnhYHOluUdPiMQOLSz3V\\/GraBuzbEbjDARS6X1wUh53Dh3\\/hFpSXVy74iw4K7\\/WvwtyvdDJ51\\/6qg8RgPyL8qByNXnqxLviMaZk+UZCNmJ+wPJ+\\/Jphtb\\/cNPw5HmbTIA2VxOq"
            + "1OybZIuJaTRVD5tO8sVpMqJTa4IFjMb69vlIYpWctEYByp7gtBCRQOsBeLydnoW+DUOeG1jDyrMmA4hi5M+ctwdn9Vb5wqTjWw=\",\"isRead\":\"N\",\"id\":\"52013784-4e32-4e9b-9676-1814ca1256fb\",\"isPrivate\":\"F\",\"is"
            + "Shared\":0}");
  }

  @Test
  public void testInsertSelectNoTarget() {
    checkRightSyntax("insert into Bookmark from select #12:0");
    checkRightSyntax("insert into Bookmark from select expand($foo)");
    checkRightSyntax("insert into Bookmark from (select #12:0)");
    checkRightSyntax("insert into Bookmark from (select expand($foo))");
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
