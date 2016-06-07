package com.orientechnologies.orient.core.sql.parser;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexFactory;
import com.orientechnologies.orient.core.index.OIndexes;
import com.orientechnologies.orient.core.index.OSimpleKeyIndexDefinition;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import static org.testng.Assert.fail;

@Test public class ODeleteStatementTest {

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

  public void testDeleteFromIndexBinary() {

    ODatabaseDocument database = new ODatabaseDocumentTx("memory:ODeleteStatementTestDeleteFromIndexBinary");
    database.create();

    OIndexFactory factory = OIndexes.getFactory("NOTUNIQUE", null);
    database.getMetadata().getIndexManager().createIndex("byte-array-manualIndex-notunique", "NOTUNIQUE",
        new OSimpleKeyIndexDefinition(factory.getLastVersion(), OType.BINARY), null, null, null);

    OIndex<?> index = database.getMetadata().getIndexManager().getIndex("byte-array-manualIndex-notunique");

    byte[] key1 = new byte[] { 0, 1, 2, 3 };
    byte[] key2 = new byte[] { 4, 5, 6, 7 };

    final ODocument doc1 = new ODocument().field("k", "key1");
    final ODocument doc2 = new ODocument().field("k", "key1");
    final ODocument doc3 = new ODocument().field("k", "key2");
    final ODocument doc4 = new ODocument().field("k", "key2");

    doc1.save();
    doc2.save();
    doc3.save();
    doc4.save();

    index.put(key1, doc1);
    index.put(key1, doc2);
    index.put(key2, doc3);
    index.put(key2, doc4);

    Assert.assertTrue(index.remove(key1, doc2));
    database.command(new OCommandSQL("delete from index:byte-array-manualIndex-notunique where key = ? and rid = ?"))
        .execute(key1, doc1);

    // Assert.assertEquals(((Collection<?>) index.get(key1)).size(), 1);
    // Assert.assertEquals(((Collection<?>) index.get(key2)).size(), 2);
    database.close();
  }

  public void deleteFromSubqueryWithWhereTest() {

    ODatabaseDocument database = new ODatabaseDocumentTx("memory:ODeleteStatementTestFromSubqueryWithWhereTest");
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

      database.command(new OCommandSQL("delete from (select expand(arr) from Bar) where k = 'key2'")).execute();

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
