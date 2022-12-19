package com.orientechnologies.orient.core.sql.parser;

import static org.junit.Assert.fail;

import com.orientechnologies.BaseMemoryDatabase;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

public class ODeleteStatementTest extends BaseMemoryDatabase {

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
  public void deleteFromSubqueryWithWhereTest() {

    db.command("create class Foo").close();
    db.command("create class Bar").close();
    final ODocument doc1 = new ODocument("Foo").field("k", "key1");
    final ODocument doc2 = new ODocument("Foo").field("k", "key2");
    final ODocument doc3 = new ODocument("Foo").field("k", "key3");

    doc1.save();
    doc2.save();
    doc3.save();

    List<ODocument> list = new ArrayList<ODocument>();
    list.add(doc1);
    list.add(doc2);
    list.add(doc3);
    final ODocument bar = new ODocument("Bar").field("arr", list);
    bar.save();

    db.command("delete from (select expand(arr) from Bar) where k = 'key2'").close();

    try (OResultSet result = db.query("select from Foo")) {
      Assert.assertNotNull(result);
      int count = 0;
      while (result.hasNext()) {
        OResult doc = result.next();
        Assert.assertNotEquals(doc.getProperty("k"), "key2");
        count += 1;
      }
      Assert.assertEquals(count, 2);
    }
  }

  protected OrientSql getParserFor(String string) {
    InputStream is = new ByteArrayInputStream(string.getBytes());
    OrientSql osql = new OrientSql(is);
    return osql;
  }
}
