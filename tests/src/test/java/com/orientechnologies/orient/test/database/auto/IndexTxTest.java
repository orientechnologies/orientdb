package com.orientechnologies.orient.test.database.auto;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;

/**
 * @author <a href="mailto:enisher@gmail.com">Artem Orobets</a>
 */
public class IndexTxTest {
  private ODatabaseDocumentTx database;

  @Parameters(value = "url")
  public IndexTxTest(final String iURL) {
    this.database = new ODatabaseDocumentTx(iURL);
  }

  @BeforeClass
  public void beforeClass() {
    database.open("admin", "admin");

    database.command(new OCommandSQL("create class IndexTxTestClass")).execute();
    database.command(new OCommandSQL("create property IndexTxTestClass.name string")).execute();
    database.command(new OCommandSQL("create index IndexTxTestIndex on IndexTxTestClass (name) unique")).execute();
    database.close();
  }

  @BeforeMethod
  public void beforeMethod() throws IOException {
    database.open("admin", "admin");
    final OSchema schema = database.getMetadata().getSchema();
    schema.reload();
    schema.getClass("IndexTxTestClass").truncate();
  }

  @AfterMethod
  public void afterMethod() {
    database.close();
  }

  @Test
  public void testIndexCrossReferencedDocuments() throws Exception {
    database.begin();

    final ODocument doc1 = new ODocument("IndexTxTestClass");
    final ODocument doc2 = new ODocument("IndexTxTestClass");

    doc1.save();
    doc2.save();

    doc1.field("ref", doc2.getIdentity().copy());
    doc1.field("name", "doc1");
    doc2.field("ref", doc1.getIdentity().copy());
    doc2.field("name", "doc2");

    doc1.save();
    doc2.save();

    database.commit();

    Map<String, ORID> expectedResult = new HashMap<String, ORID>();
    expectedResult.put("doc1", doc1.getIdentity());
    expectedResult.put("doc2", doc2.getIdentity());

    final List<ODocument> result = database.query(new OSQLSynchQuery<Object>("select from index:IndexTxTestIndex"));
    for (ODocument o : result) {
      final String key = o.rawField("key");
      final ORID expectedValue = expectedResult.get(key);
      final ORID value = o.rawField("rid");
      Assert.assertTrue(value.isPersistent());
      Assert.assertEquals(value, expectedValue);
    }
  }
}
