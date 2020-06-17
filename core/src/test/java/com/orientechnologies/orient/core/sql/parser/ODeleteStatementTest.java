package com.orientechnologies.orient.core.sql.parser;

import static org.junit.Assert.fail;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

public class ODeleteStatementTest {

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

    ODatabaseDocument database =
        new ODatabaseDocumentTx("memory:ODeleteStatementTestFromSubqueryWithWhereTest");
    database.create();

    try {
      database.command(new OCommandSQL("create class Foo")).execute();
      database.command(new OCommandSQL("create class Bar")).execute();
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

      database
          .command(new OCommandSQL("delete from (select expand(arr) from Bar) where k = 'key2'"))
          .execute();

      List<ODocument> result = database.query(new OSQLSynchQuery<ODocument>("select from Foo"));
      Assert.assertNotNull(result);
      Assert.assertEquals(result.size(), 2);
      for (ODocument doc : result) {
        Assert.assertNotEquals(doc.field("k"), "key2");
      }
    } finally {
      database.close();
    }
  }

  protected OrientSql getParserFor(String string) {
    InputStream is = new ByteArrayInputStream(string.getBytes());
    OrientSql osql = new OrientSql(is);
    return osql;
  }
}
